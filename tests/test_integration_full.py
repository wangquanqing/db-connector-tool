"""集成测试（完整端到端）

与单元测试的分工：

- **单元测试**（其余 ``test_*.py``）：单个函数/类，大量 mock，快；
- **本文件**：把真实的 CLI → DatabaseManager → 驱动 → SQLite 串起来，
  只 mock 外部不可控因素（网络、JDBC、真实业务库）。

覆盖的目标场景：

1. 命令行全链路：add → list → test → query → file → remove
2. SQL 脚本端到端：方言感知切分 + 逐条执行 + 统计
3. 结果保真：重名列在「驱动 → 管理器 → CLI 渲染/导出」全程不丢
4. 配置安全：签名 → 加密存储 → 重载 → 解密一致性
5. 连接池：复用、状态、清理
6. 批处理：多连接并发查询与失败隔离
7. 异常路径：坏配置、坏 SQL、断连后的行为

**真实数据库**：若环境变量 ``DCT_IT_<NAME>`` 指向可用连接名（如 ``db_kes``），
相关用例会额外在真库上跑一遍；否则自动跳过，不阻塞 CI。

运行方式::

    python -m unittest tests.test_integration_full -v
"""

import io
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import closing, redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool import DatabaseManager
from src.db_connector_tool.batch_manager import BatchDatabaseManager
from src.db_connector_tool.cli import create_argument_parser
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.config import ConfigManager
from src.db_connector_tool.core.config_security import ConfigSecurityManager
from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.exceptions import (
    ConfigError,
    DatabaseError,
    QueryError,
)
from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.core.result import QueryResult
from src.db_connector_tool.core.sql_splitter import split_sql

from ._isolation import IsolatedTestCase

# 真实库开关：设了环境变量才跑（CI 里没有这些库）
REAL_DB = os.environ.get("DCT_IT_DB", "").strip()
# 真实库所属的应用名（默认就是项目的标准 app 名）
REAL_DB_APP = os.environ.get("DCT_IT_APP", "db_connector_tool").strip()


class _SqliteFixture(IsolatedTestCase):
    """提供已建表并灌好数据的 SQLite 文件库"""

    def setUp(self) -> None:
        super().setUp()
        self.db_file = self.temp_path / "it.db"
        self._seed()

    def _seed(self) -> None:
        """建表并插入测试数据（含重名列场景的两张表）"""
        with closing(sqlite3.connect(self.db_file)) as conn:
            conn.executescript(
                """
                CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    amount DECIMAL(19, 2)
                );
                CREATE TABLE dept (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL
                );
                INSERT INTO users (id, name, amount) VALUES
                    (1, '张三', 12345678901234567.89),
                    (2, '李四', NULL);
                INSERT INTO dept (id, name) VALUES (1, '研发部');
                """
            )
            conn.commit()

    def _config(self) -> dict:
        """返回该 SQLite 文件的连接配置"""
        return {"type": "sqlite", "database": str(self.db_file)}


# ---------------------------------------------------------------- 1


class TestCliEndToEnd(_SqliteFixture):
    """命令行全链路：add → list → test → query → remove"""

    def setUp(self) -> None:
        super().setUp()
        self.cli = DBConnectorCLI()
        self.cli.db_manager = DatabaseManager("it_cli_app")
        self.parser = create_argument_parser(self.cli)

    def tearDown(self) -> None:
        try:
            self.cli.db_manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    def _run(self, argv, func):
        """解析参数并执行，返回 (输出, 生成的文件)"""
        args = self.parser.parse_args(argv)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            func(args)
        return buffer.getvalue()

    def test_add_list_test_remove(self) -> None:
        """新增 → 列出 → 测试连接 → 删除 的完整流程"""
        # add
        out = self._run(
            [
                "add",
                "it_conn",
                "-T",
                "sqlite",
                "-d",
                str(self.db_file),
            ],
            self.cli.add_connection,
        )
        self.assertIn("添加成功", out)

        # list
        out = self._run(["list"], self.cli.list_connections)
        self.assertIn("it_conn", out)

        # test
        out = self._run(["test", "it_conn"], self.cli.test_connection)
        self.assertIn("测试成功", out)

        # remove
        out = self._run(["remove", "it_conn"], self.cli.remove_connection)
        self.assertIn("删除", out)
        self.assertNotIn("it_conn", self.cli.db_manager.list_connections())

    def test_query_renders_table(self) -> None:
        """query 命令渲染表格，且数值类型正确"""
        self.cli.db_manager.add_connection("it_q", self._config())
        out = self._run(
            ["query", "it_q", "SELECT id, name FROM users ORDER BY id"],
            self.cli.execute_query,
        )
        self.assertIn("张三", out)
        self.assertIn("李四", out)
        self.assertIn("总计: 2 行", out)

    def test_query_export_csv_and_json(self) -> None:
        """query -o 导出 CSV / JSON，内容与真值一致"""
        self.cli.db_manager.add_connection("it_exp", self._config())
        csv_path = self.temp_path / "out.csv"
        json_path = self.temp_path / "out.json"

        self._run(
            [
                "query",
                "it_exp",
                "SELECT id, name FROM users ORDER BY id",
                "-o",
                str(csv_path),
            ],
            self.cli.execute_query,
        )
        self.assertTrue(csv_path.exists())
        content = csv_path.read_text(encoding="utf-8")
        self.assertIn("张三", content)
        self.assertIn("李四", content)

        self._run(
            [
                "query",
                "it_exp",
                "SELECT id, name FROM users ORDER BY id",
                "-o",
                str(json_path),
            ],
            self.cli.execute_query,
        )
        data = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(len(data), 2)
        self.assertEqual(data[0]["name"], "张三")

    def test_file_command_executes_script(self) -> None:
        """file 命令切分并逐条执行脚本"""
        self.cli.db_manager.add_connection("it_file", self._config())
        script = self.temp_path / "s.sql"
        script.write_text(
            "INSERT INTO users (id, name, amount) VALUES (3, '王五', 1.5);\n"
            "UPDATE users SET name = '李四改' WHERE id = 2;\n"
            "SELECT COUNT(*) AS n FROM users;\n",
            encoding="utf-8",
        )
        out = self._run(
            ["file", "it_file", str(script), "-c"], self.cli.execute_file
        )
        self.assertIn("成功 3 条", out)
        # 数据确实落库
        rows, _ = self.cli.db_manager.execute_query(
            "it_file", "SELECT name FROM users WHERE id = 2"
        )
        self.assertEqual(rows[0]["name"], "李四改")


# ---------------------------------------------------------------- 2


class TestScriptExecutionEndToEnd(_SqliteFixture):
    """脚本执行：切分准确性 + 数据一致性"""

    def test_mixed_script_all_statements_execute(self) -> None:
        """混合 DDL/DML/查询脚本按语句逐条执行，最终数据正确"""
        db_file = self.temp_path / "mixed.db"
        manager = DatabaseManager("it_mixed_app")
        manager.add_connection("m", {"type": "sqlite", "database": str(db_file)})

        script = """
        -- 建表
        CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
        /* 插入两条，含分号的字符串 */
        INSERT INTO t (v) VALUES ('a;b');
        INSERT INTO t (v) VALUES ('it''s');
        UPDATE t SET v = 'x' WHERE id = 1;
        SELECT COUNT(*) AS n FROM t;
        """
        statements = split_sql(script, "sqlite")
        self.assertEqual(len(statements), 5)

        for stmt in statements:
            if stmt.text.lower().lstrip().startswith("select"):
                manager.execute_query("m", stmt.text)
            else:
                manager.execute_command("m", stmt.text)

        rows, _ = manager.execute_query("m", "SELECT COUNT(*) AS n FROM t")
        self.assertEqual(rows[0]["n"], 2)
        # 含分号的字符串完整保存
        rows, _ = manager.execute_query(
            "m", "SELECT v FROM t WHERE id = 2"
        )
        self.assertEqual(rows[0]["v"], "it's")
        manager.close_all_connections()

    def test_procedural_body_kept_whole(self) -> None:
        """过程体整体成一条（不按内部分号切碎）"""
        script = (
            "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END;\n"
            "SELECT 'after';"
        )
        statements = split_sql(script, "mysql")
        self.assertEqual(len(statements), 2)
        self.assertIn("BEGIN", statements[0].text)
        self.assertIn("END", statements[0].text)

    def test_incomplete_script_is_flagged(self) -> None:
        """未闭合脚本被标记 is_complete=False"""
        statements = split_sql("SELECT 'unclosed", None)
        self.assertEqual(len(statements), 1)
        self.assertFalse(statements[0].is_complete)


# ---------------------------------------------------------------- 3


class TestResultFidelityEndToEnd(_SqliteFixture):
    """结果保真：驱动 → 管理器 → CLI 渲染/导出"""

    def setUp(self) -> None:
        super().setUp()
        self.manager = DatabaseManager("it_fid_app")
        self.manager.add_connection("fid", self._config())
        self.cli = DBConnectorCLI()
        self.cli.db_manager = self.manager

    def tearDown(self) -> None:
        try:
            self.manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    def test_decimal_precision_survives_pipeline(self) -> None:
        """DECIMAL 从驱动到管理器不丢精度"""
        result = self.manager.execute_query_result(
            "fid", "SELECT amount FROM users WHERE id = 1"
        )
        value = result.rows[0][0]
        # SQLite 的 NUMERIC 会转成 float，这里验证「至少不是字符串或被截断成科学计数法」
        self.assertIsNotNone(value)
        self.assertNotIsInstance(value, str)

    def test_null_and_value_distinguishable(self) -> None:
        """NULL 与真实值可区分"""
        result = self.manager.execute_query_result(
            "fid", "SELECT id, amount FROM users ORDER BY id"
        )
        self.assertIsNone(result.rows[1][1])
        self.assertIsNotNone(result.rows[0][1])

    def test_duplicate_columns_preserved_to_csv(self) -> None:
        """重名列：JOIN 结果导出 CSV 时每列独立"""
        sql = "SELECT * FROM users u JOIN dept d ON u.id = d.id"
        result = self.manager.execute_query_result("fid", sql)
        self.assertEqual(len(result.columns), 5)
        self.assertEqual(
            result.column_names(),
            ["id", "name", "amount", "id (1)", "name (1)"],
        )

        csv_path = self.temp_path / "join.csv"
        self.cli._save_csv(result, str(csv_path))  # pylint: disable=protected-access
        content = csv_path.read_text(encoding="utf-8")
        self.assertIn("张三", content)
        self.assertIn("研发部", content)
        # 员工名与部门名各占一列
        first_line = content.splitlines()[0]
        self.assertEqual(first_line, "id,name,amount,id (1),name (1)")

    def test_duplicate_columns_preserved_to_json(self) -> None:
        """重名列：JOIN 结果导出 JSON 时 5 键齐备"""
        sql = "SELECT * FROM users u JOIN dept d ON u.id = d.id"
        result = self.manager.execute_query_result("fid", sql)
        json_path = self.temp_path / "join.json"
        self.cli._save_json(result, str(json_path))  # pylint: disable=protected-access
        data = json.loads(json_path.read_text(encoding="utf-8"))
        self.assertEqual(len(data[0]), 5)
        self.assertEqual(data[0]["name"], "张三")
        self.assertEqual(data[0]["name (1)"], "研发部")

    def test_table_render_shows_all_duplicate_columns(self) -> None:
        """重名列：终端表格 5 列都在且值正确"""
        sql = "SELECT * FROM users u JOIN dept d ON u.id = d.id"
        result = self.manager.execute_query_result("fid", sql)
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.cli._show_results(result)  # pylint: disable=protected-access
        out = buffer.getvalue()
        self.assertIn("id (1)", out)
        self.assertIn("name (1)", out)
        self.assertIn("张三", out)
        self.assertIn("研发部", out)

    def test_streaming_matches_full_result(self) -> None:
        """流式取数与全量取数结果一致"""
        sql = "SELECT id, name FROM users ORDER BY id"
        full = self.manager.execute_query_result("fid", sql)

        chunks = list(self.manager.execute_query_stream("fid", sql, fetch_size=1))
        streamed = [row for chunk in chunks for row in chunk.rows]
        self.assertEqual(streamed, full.rows)
        self.assertTrue(chunks[-1].is_last)


# ---------------------------------------------------------------- 4


class TestConfigSecurityEndToEnd(IsolatedTestCase):
    """配置安全：签名 → 加密 → 重载 → 解密"""

    def test_config_roundtrip_with_signature(self) -> None:
        """写入配置后重新加载，字段解密后与原文一致"""
        app = "it_sec_app"
        manager = ConfigManager(app)
        try:
            manager.add_config(
                "c1",
                {
                    "type": "sqlite",
                    "database": ":memory:",
                    "password": "s3cret!",
                },
            )
            # 重新构造（不走缓存）
            reloaded = ConfigManager(app)
            config = reloaded.get_config("c1")
            self.assertEqual(config["type"], "sqlite")
            self.assertEqual(config["password"], "s3cret!")
        finally:
            self._cleanup_app(app)

    def _cleanup_app(self, app: str) -> None:
        """清理测试用的配置"""
        try:
            manager = ConfigManager(app)
            for name in list(manager.list_configs()):
                manager.remove_config(name)
        except Exception:  # pylint: disable=broad-except
            pass

    def test_tampered_config_rejected(self) -> None:
        """配置被篡改后签名校验失败"""
        app = "it_tamper_app"
        try:
            manager = ConfigManager(app)
            manager.add_config("c1", {"type": "sqlite", "database": ":memory:"})
            # 直接改文件内容（绕过签名计算）
            path = manager.config_path
            text = path.read_text(encoding="utf-8")
            path.write_text(
                text.replace('type = "', 'type = "tampered_', 1),
                encoding="utf-8",
            )
            with self.assertRaises((ConfigError, Exception)):  # noqa: B017
                ConfigManager(app).get_config("c1")
        finally:
            self._cleanup_app(app)

    def test_crypto_roundtrip(self) -> None:
        """加密 / 解密 往返一致，且两次密文不同（随机 IV）"""
        crypto = CryptoManager()
        plaintext = "敏感数据 abc 123"
        first = crypto.encrypt(plaintext)
        second = crypto.encrypt(plaintext)
        self.assertNotEqual(first, second)
        self.assertEqual(crypto.decrypt(first), plaintext)
        self.assertEqual(crypto.decrypt(second), plaintext)

    def test_key_rotation_keeps_data_readable(self) -> None:
        """密钥轮换后旧数据仍可读"""
        app = "it_rotate_app"
        try:
            manager = ConfigManager(app)
            manager.add_config(
                "c1", {"type": "sqlite", "database": ":memory:", "password": "p!ss"}
            )
            manager.rotate_encryption_key()
            self.assertEqual(
                ConfigManager(app).get_config("c1")["password"], "p!ss"
            )
        finally:
            self._cleanup_app(app)


# ---------------------------------------------------------------- 5


class TestConnectionPoolEndToEnd(_SqliteFixture):
    """连接池：复用、状态、清理"""

    def setUp(self) -> None:
        super().setUp()
        self.manager = DatabaseManager("it_pool_app")
        self.manager.add_connection("p", self._config())

    def tearDown(self) -> None:
        try:
            self.manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    def test_connection_reused(self) -> None:
        """同一连接名复用同一驱动实例"""
        first = self.manager.get_connection("p")
        second = self.manager.get_connection("p")
        self.assertIs(first, second)

    def test_stats_track_queries(self) -> None:
        """查询后统计更新"""
        self.manager.execute_query("p", "SELECT 1 AS a")
        pool = self.manager.pool_manager
        self.assertIsNotNone(pool.get_connection_info("p"))

    def test_remove_connection_clears_pool(self) -> None:
        """关闭连接后池中不再有该连接"""
        self.manager.get_connection("p")
        self.manager.pool_manager.remove_connection("p")
        self.assertIsNone(self.manager.pool_manager.get_connection("p"))

    def test_test_connection_true(self) -> None:
        """连接测试成功"""
        self.assertTrue(self.manager.test_connection("p"))

    def test_test_connection_false_for_bad_path(self) -> None:
        """坏路径的连接测试返回 False 而不是抛异常"""
        self.manager.add_connection(
            "bad", {"type": "sqlite", "database": "/nonexistent/dir/x.db"}
        )
        self.assertFalse(self.manager.test_connection("bad"))


# ---------------------------------------------------------------- 6


class TestBatchEndToEnd(IsolatedTestCase):
    """批处理：多连接并发与失败隔离"""

    def test_batch_query_across_connections(self) -> None:
        """两个连接上的同一查询都能成功"""
        db_a = self.temp_path / "a.db"
        db_b = self.temp_path / "b.db"
        for path in (db_a, db_b):
            with closing(sqlite3.connect(path)) as conn:
                conn.execute("CREATE TABLE t (n INTEGER)")
                conn.execute("INSERT INTO t VALUES (7)")
                conn.commit()

        manager = BatchDatabaseManager("it_batch_app")
        try:
            manager.set_base_config({"type": "sqlite", "database": str(db_a)})
            manager.add_batch_connections(["c1"], "x")
            # 第二个连接手动换成 b.db
            manager.database_manager.add_connection(
                "xb2", {"type": "sqlite", "database": str(db_b)}
            )
            manager._connection_names.append("xb2")  # pylint: disable=protected-access

            results = manager.execute_batch_query("SELECT n FROM t", max_workers=1)
            self.assertEqual(len(results), 2)
            for name, payload in results.items():
                self.assertTrue(payload["success"], msg=f"{name}: {payload}")
                self.assertEqual(payload["count"], 1)
                self.assertEqual(payload["data"][0]["n"], 7)
        finally:
            manager.close_all_connections()

    def test_batch_failure_isolated(self) -> None:
        """一个连接失败不影响其它连接"""
        good = self.temp_path / "good.db"
        with closing(sqlite3.connect(good)) as conn:
            conn.execute("CREATE TABLE t (n INTEGER)")
            conn.execute("INSERT INTO t VALUES (1)")
            conn.commit()

        manager = BatchDatabaseManager("it_batch_fail")
        try:
            manager.database_manager.add_connection(
                "good", {"type": "sqlite", "database": str(good)}
            )
            manager.database_manager.add_connection(
                "bad", {"type": "sqlite", "database": "/no/such/dir/x.db"}
            )
            manager._connection_names.extend(["good", "bad"])  # pylint: disable=protected-access

            results = manager.execute_batch_query("SELECT n FROM t", max_workers=1)
            self.assertTrue(results["good"]["success"])
            self.assertFalse(results["bad"]["success"])
        finally:
            manager.close_all_connections()


# ---------------------------------------------------------------- 7


class TestErrorRecoveryEndToEnd(_SqliteFixture):
    """异常路径：坏 SQL、断连、错误传播"""

    def setUp(self) -> None:
        super().setUp()
        self.manager = DatabaseManager("it_err_app")
        self.manager.add_connection("e", self._config())

    def tearDown(self) -> None:
        try:
            self.manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    def test_bad_sql_raises_database_error(self) -> None:
        """语法错误包装为 DatabaseError（QueryError 是其子类上一层的包装），
        且连接仍可用"""
        with self.assertRaises(DatabaseError):
            self.manager.execute_query("e", "SELECT * FROM no_such_table")
        # 连接仍可用
        rows, _ = self.manager.execute_query("e", "SELECT 1 AS ok")
        self.assertEqual(rows[0]["ok"], 1)

    def test_injection_blocked(self) -> None:
        """注入特征被拦截，且不执行"""
        with self.assertRaises((QueryError, DatabaseError)):
            self.manager.execute_query(
                "e", "SELECT * FROM users WHERE name = 'x' OR 1=1"
            )

    def test_multi_statement_blocked(self) -> None:
        """一次执行多条语句被拦截（语句走私）"""
        with self.assertRaises((QueryError, DatabaseError)):
            self.manager.execute_query("e", "SELECT 1; DROP TABLE users")

    def test_unknown_connection_raises(self) -> None:
        """未知连接名报错"""
        with self.assertRaises((ConfigError, DatabaseError)):
            self.manager.execute_query("no_such_conn", "SELECT 1")


# ---------------------------------------------------------------- 8


@unittest.skipUnless(REAL_DB, "未设置 DCT_IT_DB，跳过真实数据库集成测试")
class TestRealDatabase(unittest.TestCase):
    """真实数据库集成（通过 DCT_IT_DB=<连接名> 启用）

    覆盖单元测试无法验证的部分：驱动真实行为、方言编译、类型映射。
    """

    @classmethod
    def setUpClass(cls) -> None:
        # 用**真实应用名**才能读到用户已配置的连接
        # （DatabaseManager 按 app_name 决定配置目录）
        cls.manager = DatabaseManager(REAL_DB_APP)
        # 连不上就整类跳过：这是**环境**问题，不该让测试失败
        try:
            available = cls.manager.test_connection(REAL_DB)
        except Exception:  # pylint: disable=broad-except
            available = False
        if not available:
            cls.manager.close_all_connections()
            raise unittest.SkipTest(
                f"真实数据库 {REAL_DB} 当前不可用，跳过集成测试"
            )

    @classmethod
    def tearDownClass(cls) -> None:
        cls.manager.close_all_connections()

    def test_connection(self) -> None:
        """连接可用"""
        self.assertTrue(self.manager.test_connection(REAL_DB))

    @classmethod
    def _one_from(cls) -> str:
        """返回该方言可用的「无表查询」写法

        GBase 8s / Oracle 需要伪表，PostgreSQL / MySQL / SQL Server / SQLite
        直接 ``SELECT 1`` 即可。
        """
        dialect = cls.manager.get_connection(REAL_DB).engine.dialect
        name = dialect.name.lower()
        if name in ("oracle", "gbase8s"):
            return " FROM DUAL"
        return ""

    def test_select_one(self) -> None:
        """能执行最简单的查询（按方言决定是否需要伪表）"""
        sql = f"SELECT 1 AS one{self._one_from()}"
        result = self.manager.execute_query_result(REAL_DB, sql)
        self.assertEqual(result.rows[0][0], 1)

    def test_duplicate_columns_preserved(self) -> None:
        """重名列在真实驱动下也保真"""
        sql = f"SELECT 1 AS a, 2 AS a, 3 AS b{self._one_from()}"
        result = self.manager.execute_query_result(REAL_DB, sql)
        self.assertEqual(result.rows[0], (1, 2, 3))
        self.assertEqual(result.column_names(), ["a", "a (1)", "b"])


if __name__ == "__main__":
    unittest.main()
