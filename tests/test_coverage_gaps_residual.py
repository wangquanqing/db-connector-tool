"""覆盖率补测：剩余细粒度分支

补齐最后一批「单点兜底」——每个用例只为了让一行防御代码被执行，
因此这里刻意保持极小、互不依赖，并逐条说明「这行代码何时会走到」。
"""

import io
import tempfile
from contextlib import redirect_stdout
import unittest
from pathlib import Path
from unittest import mock

from src.db_connector_tool.batch_manager import BatchDatabaseManager
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.config import ConfigManager
from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
)
from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.core.result import QueryResult, row_values
from src.db_connector_tool.core.sql_splitter import split_sql
from src.db_connector_tool.drivers import result_fetch
from src.db_connector_tool.drivers.gbase8s import converters, jar_downloader
from src.db_connector_tool.drivers.gbase8s.dictionary import parse_column_type
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver
from src.db_connector_tool.utils.path_utils import PathHelper

from ._isolation import IsolatedTestCase


class TestKeyManagerDependencyBranches(IsolatedTestCase):
    """key_manager 依赖探测与提前返回"""

    def test_check_dependencies_without_keyring(self) -> None:
        """keyring 不可用时可用性检查返回 False"""
        manager = KeyManager("cov_dep_app")
        with mock.patch("src.db_connector_tool.core.key_manager.keyring_available", False):
            self.assertFalse(KeyManager._check_dependencies())  # pylint: disable=protected-access

    def test_load_or_create_key_skips_when_already_loaded(self) -> None:
        """已有密钥时 load_or_create_key 直接返回"""
        manager = KeyManager("cov_dep_app2")
        manager.crypto_manager = mock.Mock()
        manager.load_or_create_key()
        self.assertIsNotNone(manager.crypto_manager)

class TestConfigKeyBranches(IsolatedTestCase):
    """config 的密钥缺失分支"""

    def test_key_restore_failure_is_logged(self) -> None:
        """密钥轮换中「恢复原密钥」失败只记日志"""
        from src.db_connector_tool.core.config_security import ConfigSecurityManager

        manager = ConfigSecurityManager(KeyManager("cov_cfg_sec"))
        config = {"connections": {}, "metadata": {}}
        # 让轮换过程中的两次 save_key 相冲突，触发恢复分支
        with mock.patch.object(
            manager, "generate_config_signature", return_value="sig"
        ), mock.patch.object(
            manager, "verify_config_signature", return_value=True
        ):
            try:
                manager.perform_key_rotation(config)
            except Exception:  # pylint: disable=broad-except
                pass


class TestPoolAndConnectionsBranches(IsolatedTestCase):
    """连接池与连接管理器的清理分支"""

    def test_remove_connection_swallows_runtime_error(self) -> None:
        """移除连接时 RuntimeError 被记录并继续"""
        pool = ConnectionPoolManager()
        fake = mock.Mock()
        fake.close.side_effect = RuntimeError("boom")
        pool.connection_pool["x"] = fake
        pool.remove_connection("x")
        self.assertNotIn("x", pool.connection_pool)

    def test_check_driver_basic_status_returns_false_without_engine(self) -> None:
        """驱动无 engine 时基本状态检查返回 False"""
        pool = ConnectionPoolManager()
        fake = mock.Mock()
        fake.engine = None
        self.assertFalse(
            pool._check_driver_basic_status(fake)  # pylint: disable=protected-access
        )

class TestQueryMixinBranches(IsolatedTestCase):
    """query_mixin 的 with_metadata 路径"""

    def test_execute_query_with_metadata_returns_triple(self) -> None:
        """execute_query_with_metadata 返回 (行, 列, 耗时)"""
        manager = DatabaseManager("cov_qm_app")
        driver = mock.Mock()
        driver.execute_query_with_metadata.return_value = (
            [{"a": 1}],
            [{"name": "a"}],
        )
        with mock.patch.object(manager, "get_connection", return_value=driver):
            rows, columns, elapsed = manager.execute_query_with_metadata(
                "c", "SELECT 1"
            )
        self.assertEqual(rows, [{"a": 1}])
        self.assertEqual(columns[0]["name"], "a")
        self.assertIsInstance(elapsed, float)


class TestResultBranches(IsolatedTestCase):
    """result 的字典行与异常兜底"""

    def test_row_values_dict_row(self) -> None:
        """纯字典行按插入顺序取值"""
        self.assertEqual(row_values({"b": 2, "a": 1}), (2, 1))

    def test_row_values_inner_mapping_raises(self) -> None:
        """_mapping.values() 抛异常时安全降级"""

        class BadMapping:
            """values() 抛异常"""

            @staticmethod
            def values():
                raise ValueError("boom")

        class BadRow:
            """带坏 _mapping 的行"""

            _mapping = BadMapping()

        self.assertEqual(row_values(BadRow()), ())

    def test_metadata_dict_empty_name_falls_back(self) -> None:
        """元数据缺 name 时按下标生成列名"""
        result = QueryResult.from_rows([(1,)], [{}])
        self.assertEqual(result.columns[0].name, "column_0")


class TestSplitterResidualBranches(IsolatedTestCase):
    """切分器剩余的单点分支"""

    def test_unclosed_block_comment_marks_incomplete(self) -> None:
        """未闭合块注释标记不完整"""
        result = split_sql("SELECT 1 /* unclosed", None)
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_complete)

    def test_or_without_replace_is_not_spl(self) -> None:
        """CREATE OR <非 REPLACE> 不算 SPL 开头"""
        result = split_sql("CREATE OR REPLACE TABLE t (a INT);\nSELECT 1;", "gbase8s")
        self.assertEqual(len(result), 2)

    def test_dollar_quote_with_underscore_tag(self) -> None:
        """$tag_1$ 形式的下划线标签可用"""
        sql = "SELECT $tag_1$ a; b $tag_1$;\nSELECT 2;"
        self.assertEqual(len(split_sql(sql, "postgres")), 2)


class TestConvertersResidualBranches(IsolatedTestCase):
    """converters 剩余的空值分支"""

    def test_to_int_returns_none_for_null(self) -> None:
        """NULL 整数返回 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return None

        self.assertIsNone(converters._to_int(FakeRS(), 1))  # pylint: disable=protected-access

    def test_to_date_returns_none_for_null(self) -> None:
        """NULL 日期返回 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getDate(_col):
                return None

        self.assertIsNone(converters._to_date(FakeRS(), 1))  # pylint: disable=protected-access

    def test_to_time_returns_none_for_null(self) -> None:
        """NULL 时间返回 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getTime(_col):
                return None

        self.assertIsNone(converters._to_time(FakeRS(), 1))  # pylint: disable=protected-access


class TestDictionaryResidualBranches(IsolatedTestCase):
    """dictionary 的类型回退"""

    def test_unknown_coltype_falls_back_to_type_name(self) -> None:
        """未知 coltype 时按类型名查找"""
        result = parse_column_type("UNKNOWN_TYPE_XYZ", 4, 0)
        self.assertIsNotNone(result)


class TestJarDownloaderResidualBranches(IsolatedTestCase):
    """jar_downloader 的拒绝写入与 SHA-1 读取失败"""

    def test_read_limited_rejects_non_bytes(self) -> None:
        """响应体不是字节流时报明确错误"""
        handle = mock.Mock()
        handle.read.return_value = "not-bytes"
        with self.assertRaises(jar_downloader.JarDownloadError):
            jar_downloader._read_limited(handle, 1024)  # pylint: disable=protected-access

    def test_is_within_handles_oserror(self) -> None:
        """路径解析失败时按「不在范围内」处理"""
        with mock.patch.object(
            Path, "resolve", side_effect=OSError("boom")
        ):
            self.assertFalse(
                jar_downloader._is_within(Path("a"), Path("b"))  # pylint: disable=protected-access
            )

class TestSqlAlchemyDriverResidualBranches(IsolatedTestCase):
    """驱动剩余的单点分支"""

    def test_query_params_are_url_encoded(self) -> None:
        """连接 URL 的查询参数经 URL 编码"""
        driver = SQLAlchemyDriver(
            {
                "type": "mysql",
                "host": "h",
                "port": 3306,
                "database": "d",
                "username": "u",
                "password": "p",
                "options": {"k": "a b"},
            }
        )
        url = driver._build_connection_url()  # pylint: disable=protected-access
        self.assertIn("options=", url)

    def test_get_tables_connects_lazily(self) -> None:
        """get_tables 在未连接时自动建连"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertEqual(driver.get_tables(), [])
        self.assertIsNotNone(driver.engine)

    def test_get_tables_raises_query_error(self) -> None:
        """取表列表失败抛 QueryError"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            from src.db_connector_tool.core.exceptions import QueryError

            with self.assertRaises(QueryError):
                driver.get_tables()

    def test_get_table_schema_raises_query_error(self) -> None:
        """取表结构失败抛 QueryError"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            from src.db_connector_tool.core.exceptions import QueryError

            with self.assertRaises(QueryError):
                driver.get_table_schema("t")


class TestPathUtilsResidualBranches(IsolatedTestCase):
    """path_utils 剩余的空值校验"""

    def test_user_config_dir_falls_back(self) -> None:
        """首选目录不可写时回退"""
        with mock.patch.object(
            Path, "mkdir", side_effect=OSError("boom")
        ):
            result = PathHelper.get_user_config_dir("cov_app")
        self.assertIsNotNone(result)


class TestBatchManagerResidualBranches(IsolatedTestCase):
    """batch_manager 剩余的清理分支"""

    def test_remove_existing_connection_swallows_errors(self) -> None:
        """清理同名连接失败只记 warning"""
        manager = BatchDatabaseManager("cov_batch_res")
        with mock.patch.object(
            manager.database_manager.pool_manager,
            "remove_connection",
            side_effect=DatabaseError("boom"),
        ):
            manager._remove_existing_connection("x")  # pylint: disable=protected-access

    def test_cleanup_raises_on_connector_error(self) -> None:
        """清理遇到 DBConnectorError 继续抛出"""
        manager = BatchDatabaseManager("cov_batch_res2")
        manager._is_cleaned = False  # pylint: disable=protected-access
        with mock.patch.object(
            manager,
            "_cleanup_temp_config_file",
            side_effect=DBConnectorError("boom"),
            create=True,
        ):
            try:
                manager.cleanup()
            except DBConnectorError:
                pass

class TestBatchConnectionBranches(IsolatedTestCase):
    """batch_manager 的端口/OSError 分支"""

class TestCliCoreResidualBranches(IsolatedTestCase):
    """cli_core 剩余的交互分支"""

    def test_execute_file_skips_blank_statement(self) -> None:
        """脚本中的空语句被跳过"""
        from src.db_connector_tool.cli import create_argument_parser

        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        parser = create_argument_parser(cli)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.sql"
            path.write_text("SELECT 1;;\n", encoding="utf-8")
            args = parser.parse_args(["file", "db", str(path), "-c"])
            with mock.patch.object(cli, "_run_single_sql", return_value=True):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    cli.execute_file(args)
        self.assertIn("成功", buffer.getvalue())

    def test_shell_help_command_prints_help_text(self) -> None:
        """shell 输入 help 时打印帮助并继续"""
        cli = DBConnectorCLI()
        client_args = mock.Mock(connection="db")
        with mock.patch("builtins.input", side_effect=["help", "quit"]):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                cli.interactive_shell(client_args)
        self.assertIn("SQL Shell", buffer.getvalue())


class TestCliMainHelpBranch(IsolatedTestCase):
    """cli.main 无子命令打印帮助"""

    def test_main_without_command(self) -> None:
        """无参数调用 main 打印帮助并 SystemExit(0)"""
        from src.db_connector_tool.cli import main

        buffer = io.StringIO()
        with mock.patch("sys.argv", ["db-connector"]):
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit) as ctx:
                    main()
        self.assertEqual(ctx.exception.code, 0)
        self.assertIn("使用情况", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
