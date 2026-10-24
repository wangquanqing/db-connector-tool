"""覆盖率补测：最后一批可精确复现的分支

到这里剩下的都是「只有在很具体的时序/参数组合下才会走到」的单行分支。
本文件逐条给出触发条件，便于将来维护者判断是否还有意义。
"""

import atexit
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.batch_manager import BatchDatabaseManager
from src.db_connector_tool.cli import create_argument_parser, main
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import DatabaseError
from src.db_connector_tool.core.result import QueryResult, _derive_names
from src.db_connector_tool.core.sql_splitter import split_sql
from src.db_connector_tool.drivers.gbase8s import converters
from src.db_connector_tool.drivers.result_fetch import ResultFetchMixin

from ._isolation import IsolatedTestCase


class TestBatchCloseBranch(IsolatedTestCase):
    """batch_manager 关闭连接时单条失败"""

    def test_close_all_connections_logs_individual_failure(self) -> None:
        """批量关闭时某条失败只记 warning"""
        manager = BatchDatabaseManager("cov_close_app")
        manager._connection_names.extend(["a"])  # pylint: disable=protected-access
        with mock.patch.object(
            manager.database_manager.pool_manager,
            "remove_connection",
            side_effect=DatabaseError("boom"),
        ):
            manager.close_all_connections()  # 不应抛异常

    def test_cleanup_ignores_temp_file_oserror(self) -> None:
        """删除临时配置抛 OSError 时只记 warning"""
        manager = BatchDatabaseManager("cov_clean_app")
        manager._is_cleaned = False  # pylint: disable=protected-access
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "connections_x.toml").write_text("x", encoding="utf-8")
            with mock.patch(
                "src.db_connector_tool.batch_manager.PathHelper.get_user_config_dir",
                return_value=Path(tmp),
            ), mock.patch.object(Path, "unlink", side_effect=OSError("boom")):
                manager.cleanup()


class TestMainHelpBranch(IsolatedTestCase):
    """cli.main 无子命令"""

    def test_main_prints_help(self) -> None:
        """无参数调用打印帮助并退出 0"""
        buffer = io.StringIO()
        with mock.patch("sys.argv", ["db-connector"]):
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit) as ctx:
                    main()
        self.assertEqual(ctx.exception.code, 0)
        self.assertIn("使用情况", buffer.getvalue())


class TestCliUpdateCustomParams(IsolatedTestCase):
    """cli_core 更新连接时合并自定义参数"""

    def test_update_connection_merges_custom_params(self) -> None:
        """--custom-params 合并进更新后的配置"""
        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        cli.db_manager.show_connection.return_value = {"type": "mysql"}
        parser = create_argument_parser(cli)
        args = parser.parse_args(
            ["update", "db", "-T", "mysql", "-c", "k=v"]
        )
        with mock.patch.object(
            cli,
            "_build_updated_conn_config",
            return_value={"type": "mysql", "host": "h", "port": 1, "database": "d"},
        ):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                cli.update_connection(args)
        cli.db_manager.update_connection.assert_called_once()


class TestConnectionsTempBranch(IsolatedTestCase):
    """connections 注册的临时连接清理回调"""

class TestResultDeriveNames(IsolatedTestCase):
    """result 的字典行推导列名"""

    def test_derive_names_from_dict(self) -> None:
        """字典行按插入顺序推导"""
        self.assertEqual(_derive_names([{"b": 1, "a": 2}], None), ["b", "a"])


class TestSplitterNonSplCreate(IsolatedTestCase):
    """切分器：CREATE OR <非 REPLACE>"""

    def test_create_or_without_replace(self) -> None:
        """不是 CREATE OR REPLACE 时不进入 SPL 状态"""
        result = split_sql("CREATE OR TABLE t (a INT);\nSELECT 1;", "gbase8s")
        self.assertEqual(len(result), 2)


class TestConvertersNullFloat(IsolatedTestCase):
    """converters 的 NULL 浮点"""

    def test_to_float_null_returns_none(self) -> None:
        """NULL 浮点返回 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return None

        self.assertIsNone(converters._to_float(FakeRS(), 1))  # pylint: disable=protected-access


class TestResultFetchLazyConnect(IsolatedTestCase):
    """result_fetch 的惰性建连与参数分支"""

    def test_ensure_engine_connects(self) -> None:
        """引擎为空时触发 connect()"""

        class FakeHost(ResultFetchMixin):
            """最小宿主"""

            def __init__(self) -> None:
                self.engine = None
                self.connected = False

            def connect(self) -> None:
                """模拟建连"""
                self.connected = True
                self.engine = mock.Mock()

        host = FakeHost()
        host._ensure_engine()  # pylint: disable=protected-access
        self.assertTrue(host.connected)

    def test_query_context_with_parameters(self) -> None:
        """带参数时走参数化执行"""
        execute = ResultFetchMixin._query_context("SELECT :a", {"a": 1})  # pylint: disable=protected-access
        connection = mock.Mock()
        execute(connection)
        connection.execute.assert_called_once()

    def test_query_context_without_parameters(self) -> None:
        """无参数时直接执行"""
        execute = ResultFetchMixin._query_context("SELECT 1", None)  # pylint: disable=protected-access
        connection = mock.Mock()
        execute(connection)
        connection.execute.assert_called_once()

    def test_row_values_delegates(self) -> None:
        """_row_values 转发到 row_values"""
        self.assertEqual(
            ResultFetchMixin._row_values((1, 2, 3)), (1, 2, 3)  # pylint: disable=protected-access
        )


class TestCliCoreSkipBranch(IsolatedTestCase):
    """cli_core 执行脚本时跳过空白片段"""

    def test_execute_file_skips_whitespace_statement(self) -> None:
        """切分结果中的纯空白片段被跳过（不计数）"""
        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        parser = create_argument_parser(cli)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.sql"
            path.write_text("SELECT 1;\nSELECT 2;\n", encoding="utf-8")
            args = parser.parse_args(["file", "db", str(path), "-c"])
            with mock.patch.object(
                cli, "_run_single_sql", return_value=True
            ), mock.patch.object(
                cli,
                "_parse_sql_file",
                return_value=[
                    # 直接构造一个只有空白 text 的语句，覆盖 continue 分支
                    mock.Mock(text="   ", is_complete=True),
                    mock.Mock(text="SELECT 1", is_complete=True),
                ],
            ):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    cli.execute_file(args)
        self.assertIn("成功 1 条", buffer.getvalue())


if __name__ == "__main__":
    unittest.main()
