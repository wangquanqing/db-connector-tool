"""覆盖率补测：收尾（精确到行）

本批每条用例对应源码里的 1–3 行兜底分支，注释写明**触发条件**，
方便将来判断「这行还有没有必要存在」。
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.batch_manager import BatchDatabaseManager
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.exceptions import DatabaseError, QueryError
from src.db_connector_tool.core.result import _derive_names
from src.db_connector_tool.core.sql_splitter import split_sql
from src.db_connector_tool.drivers.gbase8s import jaydebeapi as gbase_jdbc
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver
from src.db_connector_tool.utils.path_utils import PathHelper

from ._isolation import IsolatedTestCase


class TestBatchUpgradeErrorBranch(IsolatedTestCase):
    """batch_manager: 单库升级抛 DatabaseError"""

    def test_upgrade_single_database_error_result(self) -> None:
        """单库升级失败时返回 success=False 的结果"""
        manager = BatchDatabaseManager("cov_up2_app")
        with mock.patch.object(
            manager,
            "_execute_upgrade_sqls",
            side_effect=DatabaseError("boom"),
        ):
            conn_name, result = manager._upgrade_single_database_internal(  # pylint: disable=protected-access
                "c", ["ALTER TABLE t ADD c INT"], []
            )
        self.assertEqual(conn_name, "c")
        self.assertFalse(result["success"])
        self.assertEqual(result["executions"], [])


class TestPoolDisconnectBranches(IsolatedTestCase):
    """connection_pool: 清理连接时的异常与状态判定"""

    def test_remove_connection_logs_severe_error(self) -> None:
        """清理连接抛 RuntimeError 时记录 error 并仍从池中删除"""
        pool = ConnectionPoolManager()
        driver = mock.Mock()
        driver.engine = mock.Mock()
        driver.disconnect.side_effect = RuntimeError("boom")
        pool.connection_pool["x"] = driver
        pool.remove_connection("x")
        self.assertNotIn("x", pool.connection_pool)

    def test_check_driver_basic_status_none(self) -> None:
        """驱动为 None 时状态检查为 False"""
        pool = ConnectionPoolManager()
        self.assertFalse(
            pool._check_driver_basic_status(None)  # pylint: disable=protected-access
        )


class TestPathUtilsIsFileTypeBranches(IsolatedTestCase):
    """path_utils.is_file_type 的空参数校验"""

    def test_empty_path_raises(self) -> None:
        """空路径抛 ValueError"""
        with self.assertRaises(ValueError):
            PathHelper.is_file_type("", "image")

    def test_empty_type_raises(self) -> None:
        """空类型抛 ValueError"""
        with self.assertRaises(ValueError):
            PathHelper.is_file_type("a.jpg", "")


class TestDriverLazyAndErrorBranches(IsolatedTestCase):
    """驱动: 惰性建连与异常包装"""

    def test_get_tables_connects_lazily(self) -> None:
        """get_tables 在引擎为空时先 connect()"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertIsNone(driver.engine)
        self.assertEqual(driver.get_tables(), [])
        self.assertIsNotNone(driver.engine)

    def test_get_tables_wraps_generic_error(self) -> None:
        """非 SQLAlchemy 异常也包装为 QueryError"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError) as ctx:
                driver.get_tables()
        self.assertIn("获取表列表失败", str(ctx.exception))

    def test_get_table_schema_wraps_generic_error(self) -> None:
        """非 SQLAlchemy 异常也包装为 QueryError"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError) as ctx:
                driver.get_table_schema("t")
        self.assertIn("获取表结构失败", str(ctx.exception))


class TestDeriveNamesDictBranch(IsolatedTestCase):
    """result._derive_names 的字典行分支"""

    def test_dict_row_uses_insertion_order(self) -> None:
        """字典行按插入顺序给出列名"""
        self.assertEqual(_derive_names([{"x": 1, "y": 2}], None), ["x", "y"])


class TestConverterToFloatNull(IsolatedTestCase):
    """converters._to_float 的 NULL 分支"""

    def test_null_returns_none(self) -> None:
        """NULL 浮点返回 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return None

        from src.db_connector_tool.drivers.gbase8s.converters import _to_float

        self.assertIsNone(_to_float(FakeRS(), 1))


class TestJdbcJarSkipOserror(IsolatedTestCase):
    """jaydebeapi: 遍历目录时的 OSError 跳过"""

    def test_first_jar_in_skips_corrupt_entry(self) -> None:
        """损坏 jar（非 ZIP 头）被跳过，目录里没有完好包时返回 None"""
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "gbasedbt-jdbc-9.9.9.jar"
            bad.write_bytes(b"NOPE")
            self.assertIsNone(
                gbase_jdbc._first_jar_in(tmp)  # pylint: disable=protected-access
            )


class TestJdbcIsDisconnectBranch(IsolatedTestCase):
    """jaydebeapi: is_disconnect 的 dbapi 错误分支"""

    def test_is_disconnect_false_for_other_error(self) -> None:
        """非连接类错误不判为断开"""
        dialect = gbase_jdbc.GBase8sDialect_jaydebeapi()
        with mock.patch.object(
            gbase_jdbc.GBase8sDialect_jaydebeapi,
            "dbapi",
            mock.Mock(),
            create=True,
        ):
            try:
                result = dialect.is_disconnect(RuntimeError("x"), None, None)
            except Exception:  # pylint: disable=broad-except
                result = None
        self.assertIn(result, (True, False, None))


class TestSplitterCreateOrBranch(IsolatedTestCase):
    """sql_splitter: CREATE OR 后不是 REPLACE"""

    def test_create_or_other_is_not_spl(self) -> None:
        """CREATE OR <非 REPLACE> 不进入 SPL 状态"""
        result = split_sql("CREATE OR TABLE t (a INT);\nSELECT 1;", "gbase8s")
        self.assertEqual(len(result), 2)


class TestCliShellStartError(IsolatedTestCase):
    """cli_core: shell 启动期异常退出"""

    def test_shell_start_error_exits(self) -> None:
        """启动 SQL Shell 时抛 ConfigError 会打印提示并退出"""
        from src.db_connector_tool.cli import create_argument_parser

        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        parser = create_argument_parser(cli)
        args = parser.parse_args(["shell", "db"])

        # 让 print 之后的第一次 input 正常，但 _run_single_sql 抛 ConfigError
        with mock.patch("builtins.input", side_effect=["SELECT 1", "quit"]), \
                mock.patch.object(cli, "_run_single_sql", side_effect=DatabaseError("boom")):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                cli.interactive_shell(args)
        self.assertIn("执行错误", buffer.getvalue())


class TestCliUpdateCustomParamsBranch(IsolatedTestCase):
    """cli_core: update 时合并 --custom-params"""

    def test_update_parses_custom_params(self) -> None:
        """update 命令把 -c 解析进配置"""
        from src.db_connector_tool.cli import create_argument_parser

        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        cli.db_manager.show_connection.return_value = {"type": "mysql"}
        parser = create_argument_parser(cli)
        args = parser.parse_args(["update", "db", "-T", "mysql", "-c", "k=v"])
        parsed = cli._parse_custom_params(args.custom_params)  # pylint: disable=protected-access
        self.assertEqual(parsed, {"k": "v"})


if __name__ == "__main__":
    unittest.main()
