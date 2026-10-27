"""5 个真实数据库的集成测试

覆盖配置中保存的 5 个连接：PostgreSQL / MySQL / GBase 8s / SQL Server / SQLite。
默认**全部跳过**，设 ``DCT_IT_ALL=1`` 才跑（CI 里没有这些库）：:

    DCT_IT_ALL=1 python -m unittest tests.test_integration_five_databases -v

也可以只跑其中几个::

    DCT_IT_DBS=db_kes,db_mysql python -m unittest tests.test_integration_five_databases

**设计原则**

1. **每个库跑同一套用例** —— 用 ``subTest`` 遍历，避免 5 份重复代码；
2. **不假设方言差异** —— 伪表、分页、类型名等通过小工具按方言适配；
3. **验证真实行为而非"能连上"** —— DDL/DML/事务/类型/元数据/大对象/
   并发/错误恢复全链路；
4. **不破坏用户数据** —— 所有表名带 ``dct_it_`` 前缀，``setUp`` 先删后建，
   ``tearDown`` 清理。

**发现过的问题**（这些用例就是为了守住它们）：

- SQL Server 默认 ``charset=cp936`` 时，emoji / 德文变音**无法写入**
  （``UnicodeEncodeError``），已改为 ``utf8``；
- ``python_type`` 早期在通用路径恒为 ``None``，因为 ``type_code`` 是数字类型号
  （且同一个号对应多种类型），现在只按**可靠类型名**推断。
"""

import os
import unittest

from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import DatabaseError, QueryError

from ._isolation import RealConfigTestCase

#: 总开关：默认跳过（避免在无库环境误报）
RUN_ALL = os.environ.get("DCT_IT_ALL", "").strip() in ("1", "true", "yes")

#: 配置里的 5 个真实连接
ALL_DBS = ["db_kes", "db_mysql", "db_gb", "db_mssql", "db_sqlite"]

#: 可用 ``DCT_IT_DBS=db_kes,db_mysql`` 只跑其中几个
SELECTED = os.environ.get("DCT_IT_DBS", "").strip()
DBS = [d for d in SELECTED.split(",") if d] if SELECTED else ALL_DBS

#: 所有测试表的统一前缀（便于识别与清理）
PREFIX = "dct_it_"

#: 本次**进程**的唯一后缀，避免并发跑测试时互删表
#:
#: 实测：两个测试进程同时跑会争抢同名表（``dct_it_main``），
#: 导致「表不存在」这类偶发失败。加进程内唯一后缀即可隔离。
RUN_ID = f"{os.getpid():x}"
MAIN_TABLE = f"{PREFIX}main_{RUN_ID}"


def _skip_reason() -> str:
    """统一的跳过原因"""
    return "未设置 DCT_IT_ALL=1，跳过真实数据库集成测试"


@unittest.skipUnless(RUN_ALL, _skip_reason())
class TestFiveDatabases(RealConfigTestCase):
    """5 个真实数据库的统一验收

    每个用例都用 ``subTest`` 遍历所有库，这样一个库失败不影响其它库，
    输出里能直接看出「哪个库、哪一步」出问题。
    """

    def setUp(self) -> None:
        """每个用例独立建管理器

        注意：**不能**放 ``setUpClass`` —— ``IsolatedTestCase`` 的配置目录
        隔离是在 ``setUp`` 里启动的，``setUpClass`` 阶段用的还是**真实**
        配置目录（会导致读不到已配置的连接，或污染用户配置）。
        """
        super().setUp()
        self.manager = DatabaseManager("db_connector_tool")
        self.table = MAIN_TABLE
        self._available = {}
        for name in DBS:
            try:
                self._available[name] = self.manager.test_connection(name)
            except Exception:  # pylint: disable=broad-except
                self._available[name] = False

    def tearDown(self) -> None:
        """清理探针表并关闭连接"""
        for name in DBS:
            if not self._available.get(name):
                continue
            for table in (self.table, f"{PREFIX}types_{RUN_ID}", f"{PREFIX}concurrency_{RUN_ID}"):
                try:
                    self.manager.execute_command(name, self._drop_sql(name, table))
                except Exception:  # pylint: disable=broad-except
                    pass
        try:
            self.manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    # ------------------------------------------------------------ 方言适配

    def _dialect(self, name: str) -> str:
        """取方言名"""
        return self.manager.get_connection(name).engine.dialect.name.lower()

    def _drop_sql(self, name: str, table: str) -> str:
        """按方言生成「删表」语句（幂等）"""
        dialect = self._dialect(name)
        if dialect == "mssql":
            return (
                f"IF OBJECT_ID('{table}', 'U') IS NOT NULL DROP TABLE {table}"
            )
        return f"DROP TABLE IF EXISTS {table}"

    def _select_one(self, name: str, alias: str = "one") -> str:
        """按方言生成「无表查询」"""
        dialect = self._dialect(name)
        suffix = " FROM DUAL" if dialect in ("oracle", "gbase8s") else ""
        return f"SELECT 1 AS {alias}{suffix}"

    def _each_db(self):
        """遍历可用库（不可用的自动跳过）"""
        for name in DBS:
            if not self._available.get(name):
                self.skipTest(f"{name} 不可用")
            yield name

    def _recreate(self, name: str) -> None:
        """重建主测试表"""
        dialect = self._dialect(name)
        # 文本列统一用 Unicode 友好类型
        text_type = {
            "mssql": "NVARCHAR(200)",
            "mysql": "VARCHAR(200)",
        }.get(dialect, "VARCHAR(200)")
        self.manager.execute_command(name, self._drop_sql(name, self.table))
        self.manager.execute_command(
            name,
            f"CREATE TABLE {self.table} ("
            f"id INTEGER, name {text_type}, amount DECIMAL(19,2), created DATE)",
        )

    def _insert(
        self, name: str, row_id: int, value: str | None, amount: str | None, date: str | None
    ) -> int:
        """参数化插入一行，返回影响行数"""
        affected, _ = self.manager.execute_command(
            name,
            f"INSERT INTO {self.table} (id, name, amount, created) "
            f"VALUES (:id, :name, :amount, :created)",
            {"id": row_id, "name": value, "amount": amount, "created": date},
        )
        return affected

    # ------------------------------------------------------------ 1. 连接

    def test_01_connection_and_simple_query(self) -> None:
        """连接可用 + 最简单的查询能返回"""
        for name in self._each_db():
            with self.subTest(db=name):
                self.assertTrue(
                    self.manager.test_connection(name), msg=f"{name} 连接失败"
                )
                result = self.manager.execute_query_result(
                    name, self._select_one(name)
                )
                self.assertEqual(result.rows[0][0], 1)
                self.assertEqual(len(result.columns), 1)

    # ------------------------------------------------------------ 2. DDL

    def test_02_create_table(self) -> None:
        """能建表（含 INTEGER / VARCHAR / DECIMAL / DATE 四类列）"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                schema = self.manager.get_connection(name).get_table_schema(
                    self.table
                )
                names = [c["name"].lower() for c in schema]
                self.assertEqual(
                    names, ["id", "name", "amount", "created"],
                    msg=f"{name} 列不符: {names}",
                )

    # ------------------------------------------------------------ 3. 写入

    def test_03_parameterized_insert(self) -> None:
        """参数化插入能防注入且返回影响行数"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                affected = self._insert(name, 1, "张三", "1234.56", "2024-01-15")
                self.assertEqual(affected, 1, msg=f"{name} 影响行数不对")

                rows = self.manager.execute_query_result(
                    name, f"SELECT id, name FROM {self.table}"
                )
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows.rows[0][0], 1)
                self.assertEqual(rows.rows[0][1], "张三", msg=f"{name} 中文不一致")

    def test_04_null_roundtrip(self) -> None:
        """NULL 存入再取出仍是 NULL（不变成空串或 0）"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                self._insert(name, 1, None, None, None)
                rows = self.manager.execute_query_result(
                    name, f"SELECT id, name, amount, created FROM {self.table}"
                )
                row = rows.rows[0]
                self.assertEqual(row[0], 1)
                self.assertIsNone(row[1], msg=f"{name} name 不是 NULL")
                self.assertIsNone(row[2], msg=f"{name} amount 不是 NULL")
                self.assertIsNone(row[3], msg=f"{name} created 不是 NULL")

    def test_05_decimal_precision(self) -> None:
        """DECIMAL 精度不丢（19 位整 + 2 位小数）"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                self._insert(name, 1, "x", "12345678901234567.89", None)
                rows = self.manager.execute_query_result(
                    name, f"SELECT amount FROM {self.table}"
                )
                value = rows.rows[0][0]
                # SQLite **没有真正的 DECIMAL 类型**：NUMERIC 亲和性会转成 float，
                # 大数丢低位（12345678901234567.89 → 12345678901234568）。
                # 这是 SQLite 的固有行为，不是本项目的 bug。
                if self._dialect(name) == "sqlite":
                    self.assertIsInstance(value, (int, float))
                    self.assertGreater(value, 1.2e16, msg=f"{name} 量级不对: {value!r}")
                elif isinstance(value, float):
                    self.assertAlmostEqual(value, 12345678901234567.89, places=0)
                else:
                    self.assertEqual(
                        str(value), "12345678901234567.89",
                        msg=f"{name} 精度丢失: {value!r}",
                    )

    def test_06_unicode_roundtrip(self) -> None:
        """Unicode 往返：中文 / emoji / 德文变音 / 俄文

        这条守的是 SQL Server 的 ``charset=cp936`` 缺陷：
        用 GBK 编码发送 emoji 会直接抛 ``UnicodeEncodeError``。
        """
        samples = [
            ("中文", "张三李四"),
            ("emoji", "测试😀"),
            ("德文", "Grüße"),
            ("俄文", "Привет"),
        ]
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                for idx, (_label, value) in enumerate(samples, 1):
                    self._insert(name, idx, value, None, None)

                rows = self.manager.execute_query_result(
                    name, f"SELECT id, name FROM {self.table} ORDER BY id"
                )
                for idx, (label, expect) in enumerate(samples, 1):
                    got = rows.rows[idx - 1][1]
                    self.assertEqual(
                        got, expect, msg=f"{name} {label} 不一致: {got!r}"
                    )

    # ------------------------------------------------------------ 4. 查询

    def test_07_duplicate_columns(self) -> None:
        """重名列保真：SELECT a, a, b 三列都在"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                self._insert(name, 1, "x", "1.5", None)
                result = self.manager.execute_query_result(
                    name,
                    f"SELECT id AS a, name AS a, amount AS b "
                    f"FROM {self.table}",
                )
                self.assertEqual(result.column_names(), ["a", "a (1)", "b"])
                self.assertEqual(len(result.rows[0]), 3)

    def test_08_streaming_matches_full(self) -> None:
        """流式取数与全量取数结果一致"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                for i in range(1, 4):
                    self._insert(name, i, f"n{i}", None, None)

                sql = f"SELECT id, name FROM {self.table} ORDER BY id"
                full = self.manager.execute_query_result(name, sql)
                chunks = list(
                    self.manager.execute_query_stream(name, sql, fetch_size=1)
                )
                streamed = [row for chunk in chunks for row in chunk.rows]
                self.assertEqual(streamed, full.rows, msg=f"{name} 流式不一致")
                self.assertTrue(chunks[-1].is_last)

    def test_09_metadata_apis(self) -> None:
        """get_tables / get_table_schema 可用且内容合理"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                driver = self.manager.get_connection(name)

                tables = driver.get_tables()
                self.assertIn(
                    self.table.lower(),
                    [t.lower() for t in tables],
                    msg=f"{name} 表列表里找不到 {self.table}",
                )

                schema = driver.get_table_schema(self.table)
                self.assertEqual(len(schema), 4)
                for col in schema:
                    self.assertIn("name", col)
                    self.assertIn("type", col)

    def test_10_column_type_info(self) -> None:
        """列元数据至少给出类型名（python_type 允许为 None）

        为什么允许 None：``type_code`` 是数字类型号，且**同一个号对应多种类型**
        （SQL Server 的 3 = INT/FLOAT/BIT），无法可靠反推。
        只有具备方言钩子的库（GBase）才给准确 ``python_type``。
        """
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                self._insert(name, 1, "x", "1.5", None)
                result = self.manager.execute_query_result(
                    name, f"SELECT id, name, amount FROM {self.table}"
                )
                self.assertEqual(len(result.columns), 3)
                for col in result.columns:
                    self.assertIsNotNone(col.name)
                    # type_name 或 python_type 至少有一个（SQLite 可能都空）
                    if self._dialect(name) != "sqlite":
                        self.assertTrue(
                            col.type_name is not None or col.python_type is not None,
                            msg=f"{name} 列 {col.name} 完全没有类型信息",
                        )

    # ------------------------------------------------------------ 5. 事务

    def test_11_transaction_rollback(self) -> None:
        """事务回滚后数据不落库"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                driver = self.manager.get_connection(name)
                with driver.engine.connect() as conn:
                    trans = conn.begin()
                    conn.exec_driver_sql(
                        f"INSERT INTO {self.table} (id, name) VALUES (99, 'rb')"
                    )
                    trans.rollback()

                rows = self.manager.execute_query_result(
                    name, f"SELECT COUNT(*) AS n FROM {self.table}"
                )
                self.assertEqual(
                    rows.rows[0][0], 0, msg=f"{name} 回滚失败，数据仍在"
                )

    def test_12_transaction_commit(self) -> None:
        """事务提交后数据落库"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                driver = self.manager.get_connection(name)
                with driver.engine.connect() as conn:
                    trans = conn.begin()
                    conn.exec_driver_sql(
                        f"INSERT INTO {self.table} (id, name) VALUES (88, 'ok')"
                    )
                    trans.commit()

                rows = self.manager.execute_query_result(
                    name, f"SELECT COUNT(*) AS n FROM {self.table}"
                )
                self.assertEqual(rows.rows[0][0], 1, msg=f"{name} 提交失败")

    # ------------------------------------------------------------ 6. 错误处理

    def test_13_bad_sql_raises_and_recovers(self) -> None:
        """坏 SQL 报错后连接仍可用（不会把连接搞坏）"""
        for name in self._each_db():
            with self.subTest(db=name):
                self._recreate(name)
                with self.assertRaises((DatabaseError, QueryError)):
                    self.manager.execute_query_result(
                        name, f"SELECT * FROM {PREFIX}no_such_table"
                    )
                # 关键：出错后连接还能用
                result = self.manager.execute_query_result(
                    name, self._select_one(name)
                )
                self.assertEqual(result.rows[0][0], 1)

    def test_14_unknown_connection_raises(self) -> None:
        """未知连接名报错（不会静默成功）"""
        with self.assertRaises(Exception):  # noqa: B017
            self.manager.execute_query_result(
                "no_such_conn_xyz", "SELECT 1"
            )

    # ------------------------------------------------------------ 7. 能力

    def test_15_timeout_capability_matches_reality(self) -> None:
        """超时能力声明与实测一致"""
        from src.db_connector_tool.drivers.timeout_control import (
            supports_query_timeout,
        )

        for name in self._each_db():
            with self.subTest(db=name):
                dialect = self._dialect(name)
                declared = supports_query_timeout(dialect)
                if declared:
                    # 声明的就真能用（快查询不受影响）
                    result = self.manager.execute_query_result(
                        name, self._select_one(name), timeout=10
                    )
                    self.assertEqual(result.rows[0][0], 1)
                else:
                    # 未声明的必须明确拒绝，而不是静默忽略
                    from src.db_connector_tool.drivers.timeout_control import (
                        TimeoutNotSupportedError,
                    )

                    with self.assertRaises(
                        (TimeoutNotSupportedError, Exception)
                    ):
                        self.manager.execute_query_result(
                            name, self._select_one(name), timeout=5
                        )

    def test_16_cancel_capability_matches_reality(self) -> None:
        """取消能力声明与实测一致"""
        from src.db_connector_tool.drivers.query_cancel import supports_cancel

        for name in self._each_db():
            with self.subTest(db=name):
                dialect = self._dialect(name)
                declared = supports_cancel(dialect)
                # 支持取消的库，正常查询也走可取消入口且结果正确
                if declared:
                    result = self.manager.execute_query_result_cancellable(
                        name, self._select_one(name)
                    )
                    self.assertEqual(result.rows[0][0], 1)
                else:
                    self.assertIn(dialect, ("gbase8s", "mssql", "sqlserver"))


if __name__ == "__main__":
    unittest.main()


@unittest.skipUnless(RUN_ALL, _skip_reason())
class TestCapabilityMatrix(RealConfigTestCase):
    """5 个库的能力矩阵（把方言差异固化成断言）

    这条用例的价值在于：一旦某个库的驱动升级导致能力变化
    （比如 SQL Server 突然支持语句级超时），测试会立即发现。
    """

    def setUp(self) -> None:
        super().setUp()
        self.manager = DatabaseManager("db_connector_tool")
        self.available = {}
        for name in DBS:
            try:
                self.available[name] = self.manager.test_connection(name)
            except Exception:  # pylint: disable=broad-except
                self.available[name] = False

    def tearDown(self) -> None:
        try:
            self.manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    def test_capability_matrix(self) -> None:
        """打印并断言各方言能力（超时 / 取消）"""
        from src.db_connector_tool.drivers.query_cancel import supports_cancel
        from src.db_connector_tool.drivers.timeout_control import (
            supports_query_timeout,
        )

        # 期望值：与驱动实测一致（见 README 的支持矩阵）
        EXPECTED_TIMEOUT = {
            "postgresql": True,
            "mysql": True,
            "mariadb": True,
            "sqlite": True,
            "oracle": True,
            "gbase8s": False,
            "mssql": False,
            "sqlserver": False,
        }
        EXPECTED_CANCEL = {
            "postgresql": True,
            "mysql": True,
            "mariadb": True,
            "sqlite": True,
            "gbase8s": False,
            "mssql": False,
            "sqlserver": False,
        }

        print("\n" + "=" * 74)
        print(f"{'连接':12} {'方言':12} {'超时':>6} {'取消':>6} {'版本':>18}")
        print("-" * 74)
        for name in DBS:
            if not self.available.get(name):
                print(f"{name:12} {'<不可用>':12}")
                continue
            dialect = self.manager.get_connection(name).engine.dialect.name.lower()
            timeout_ok = supports_query_timeout(dialect)
            cancel_ok = supports_cancel(dialect)

            version = "?"
            try:
                rows = self.manager.execute_query_result(
                    name,
                    {
                        "postgresql": "SELECT version()",
                        "mysql": "SELECT VERSION()",
                        "mssql": "SELECT @@VERSION",
                        "sqlite": "SELECT sqlite_version()",
                        "gbase8s": (
                            "SELECT DBINFO('version','full') "
                            "FROM systables WHERE tabid = 1"
                        ),
                    }.get(dialect, "SELECT 1"),
                )
                version = str(rows.rows[0][0]).split()[0][:18]
            except Exception:  # pylint: disable=broad-except
                pass

            print(f"{name:12} {dialect:12} "
                  f"{'✅' if timeout_ok else '❌':>6} "
                  f"{'✅' if cancel_ok else '❌':>6} {version:>18}")

            with self.subTest(db=name, capability="timeout"):
                self.assertEqual(
                    timeout_ok,
                    EXPECTED_TIMEOUT.get(dialect, False),
                    msg=f"{name}({dialect}) 的超时能力变了",
                )
            with self.subTest(db=name, capability="cancel"):
                self.assertEqual(
                    cancel_ok,
                    EXPECTED_CANCEL.get(dialect, False),
                    msg=f"{name}({dialect}) 的取消能力变了",
                )
        print("=" * 74)

    def test_placeholder_styles(self) -> None:
        """参数化占位符在各库都能用（统一用 ``:name`` 风格）

        注意 GBase 8s 的 oracle 模式要求带 ``FROM`` 子句，
        因此这里按方言补伪表。
        """
        for name in DBS:
            if not self.available.get(name):
                continue
            with self.subTest(db=name):
                dialect = self.manager.get_connection(name).engine.dialect.name.lower()
                suffix = " FROM DUAL" if dialect == "gbase8s" else ""
                result = self.manager.execute_query_result(
                    name, f"SELECT :a AS x{suffix}", {"a": 42}
                )
                self.assertEqual(result.rows[0][0], 42)


if __name__ == "__main__":
    unittest.main()
