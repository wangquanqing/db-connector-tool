"""覆盖率补测：batch_manager / cli / cli_core / jar_downloader / GBase 方言

这里覆盖的是**编排层与下载器**的错误路径：批量操作中的局部失败、
命令行交互中的异常分支、网络下载的各种失败形态。

这些路径的共同点是「不常发生但必须有兜底」—— 比如批量建连时某个 IP 失败，
不能让整批崩掉；下载 jar 时网络抖动要有明确错误而不是裸 traceback。
"""

import contextlib
import io
import tempfile
import unittest
import urllib.error
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.batch_manager import (
    BatchDatabaseManager,
    cleanup_temp_configs,
)
from src.db_connector_tool.cli import main
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
)
from src.db_connector_tool.drivers.gbase8s import jar_downloader

from ._isolation import IsolatedTestCase


class TestBatchManagerEdgeBranches(IsolatedTestCase):
    """batch_manager 的局部失败与清理路径"""

    def setUp(self) -> None:
        super().setUp()
        self.manager = BatchDatabaseManager("cov_batch_app")

    def tearDown(self) -> None:
        try:
            self.manager.close_all_connections()
        except Exception:  # pylint: disable=broad-except
            pass
        super().tearDown()

    def test_set_base_config_strips_host(self) -> None:
        """基础配置模板会去掉 host（由每个 IP 各自提供）"""
        self.manager.set_base_config(
            {"host": "ignored", "port": 5432, "username": "u", "database": "d"}
        )
        self.assertNotIn("host", self.manager.base_config)
        self.assertEqual(self.manager.base_config["port"], 5432)

    def test_add_batch_connections_records_partial_failure(self) -> None:
        """批量建连时单个失败不影响其它 IP，结果里标记 False"""
        self.manager.set_base_config({"port": 1, "username": "u", "database": "d"})
        with mock.patch.object(
            self.manager.database_manager,
            "add_connection",
            side_effect=[None, DatabaseError("boom"), None],
        ):
            results = self.manager.add_batch_connections(
                ["10.0.0.1", "10.0.0.2", "10.0.0.3"], "db"
            )
        self.assertTrue(results["10.0.0.1"])
        self.assertFalse(results["10.0.0.2"])
        self.assertTrue(results["10.0.0.3"])

    def test_test_batch_connections_marks_unreachable(self) -> None:
        """批量测试连接时异常记为 False"""
        self.manager._connection_names.extend(["a", "b"])  # pylint: disable=protected-access
        with mock.patch.object(
            self.manager.database_manager,
            "test_connection",
            side_effect=[True, DatabaseError("boom")],
        ):
            results = self.manager.test_batch_connections()
        self.assertTrue(results["a"])
        self.assertFalse(results["b"])

    def test_test_batch_connections_without_connections(self) -> None:
        """没有连接时返回空字典"""
        self.assertEqual(self.manager.test_batch_connections(), {})

    def test_cleanup_handles_missing_temp_config(self) -> None:
        """临时配置文件不存在时正常结束"""
        self.manager._is_cleaned = False  # pylint: disable=protected-access
        self.manager.cleanup()
        self.assertTrue(self.manager._is_cleaned)  # pylint: disable=protected-access

    def test_cleanup_ignores_unlink_oserror(self) -> None:
        """删除临时配置失败只记 warning"""
        self.manager._is_cleaned = False  # pylint: disable=protected-access
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "connections_x.toml"
            target.write_text("x", encoding="utf-8")
            with mock.patch(
                "src.db_connector_tool.batch_manager.PathHelper.get_user_config_dir",
                return_value=Path(tmp),
            ), mock.patch.object(Path, "unlink", side_effect=OSError("boom")):
                self.manager.cleanup()

    def test_execute_batch_query_records_failure(self) -> None:
        """批量查询时驱动异常记为失败项"""
        with mock.patch.object(
            self.manager, "_get_all_connection_names", return_value=["c"]
        ), mock.patch.object(
            self.manager.database_manager,
            "execute_query",
            side_effect=DatabaseError("boom"),
        ):
            results = self.manager.execute_batch_query("SELECT 1")
        self.assertFalse(results["c"]["success"])

    def test_execute_batch_query_without_connections(self) -> None:
        """没有连接时返回空字典"""
        with mock.patch.object(
            self.manager, "_get_all_connection_names", return_value=[]
        ):
            self.assertEqual(self.manager.execute_batch_query("SELECT 1"), {})

    def test_try_rollback_logs_failure(self) -> None:
        """回滚失败只记日志，不打断升级流程"""
        with mock.patch.object(
            self.manager, "_execute_rollback", side_effect=DatabaseError("boom")
        ):
            self.manager._try_rollback("c", ["x"])  # pylint: disable=protected-access

    def test_execute_rollback_swallows_statement_errors(self) -> None:
        """回滚里单条语句失败只记 warning"""
        with mock.patch.object(
            self.manager, "_execute_statement", side_effect=DatabaseError("boom")
        ):
            self.manager._execute_rollback("c", ["a", "b"])  # pylint: disable=protected-access

    def test_connection_stats_records_error(self) -> None:
        """取连接状态失败时记录错误而不是整体失败"""
        with mock.patch.object(
            self.manager, "_get_all_connection_names", return_value=["a"]
        ), mock.patch.object(
            self.manager.database_manager,
            "get_connection_info",
            side_effect=DatabaseError("boom"),
        ):
            stats = self.manager.get_connection_stats()
        self.assertIn("error", stats["a"])

    def test_cleanup_temp_configs_ignores_oserror(self) -> None:
        """清理临时文件失败只记 warning"""
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "connections_x.toml").write_text("x", encoding="utf-8")
            with mock.patch(
                "src.db_connector_tool.batch_manager.PathHelper.get_user_config_dir",
                return_value=Path(tmp),
            ), mock.patch.object(Path, "unlink", side_effect=OSError("boom")):
                cleanup_temp_configs("cov_batch_app")


class TestCliEntryEdgeBranches(IsolatedTestCase):
    """cli.py 入口的分发与帮助"""

    def test_no_command_prints_help(self) -> None:
        """无子命令时打印帮助而不是报错"""
        buffer = io.StringIO()
        with mock.patch("sys.argv", ["db-connector"]):
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit):
                    main()
        self.assertIn("使用情况", buffer.getvalue())


class TestCliCoreEdgeBranches(IsolatedTestCase):
    """cli_core 的自定义参数展示与交互式 shell"""

    def setUp(self) -> None:
        super().setUp()
        self.cli = DBConnectorCLI()
        self.cli.db_manager = mock.Mock()

    def test_print_custom_params_lists_keys(self) -> None:
        """展示自定义参数时列出键名（隐藏值）"""
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.cli._print_custom_params(  # pylint: disable=protected-access
                {"options": {"a": 1}, "port": 1}
            )
        self.assertIn("自定义参数", buffer.getvalue())

    def test_show_conn_details_prints_custom_params(self) -> None:
        """连接详情展示自定义参数"""
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.cli._show_conn_details(  # pylint: disable=protected-access
                {
                    "type": "mysql",
                    "host": "h",
                    "port": 1,
                    "database": "d",
                    "options": {"a": 1},
                }
            )
        self.assertIn("自定义参数", buffer.getvalue())

    def test_shorten_sql_appends_ellipsis(self) -> None:
        """超长 SQL 被截断并加省略号"""
        short = self.cli._shorten_sql("a" * 100, max_length=10)  # pylint: disable=protected-access
        self.assertEqual(short, "aaaaaaaaaa...")

    def test_shell_skips_blank_and_quit(self) -> None:
        """交互式 shell 跳过空行、quit 退出"""
        args = mock.Mock(connection="db")
        with mock.patch("builtins.input", side_effect=["", "quit"]):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                self.cli.interactive_shell(args)
        self.assertIn("启动SQL Shell", buffer.getvalue())

    def test_shell_handles_statement_error(self) -> None:
        """shell 中单条语句失败不退出循环"""
        args = mock.Mock(connection="db")
        with mock.patch("builtins.input", side_effect=["bad sql", "quit"]), mock.patch.object(
            self.cli, "_run_single_sql", side_effect=DatabaseError("boom")
        ):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                self.cli.interactive_shell(args)
        self.assertIn("执行错误", buffer.getvalue())

    def test_shell_reports_statement_error_and_continues(self) -> None:
        """shell 中语句执行失败会提示，但循环继续（quit 才退出）

        失败由循环内部的 ``except (DatabaseError, ConfigError)`` 处理，
        因此不会中断会话 —— 这正是「交互式 shell 要能容错」的设计。
        """
        args = mock.Mock(connection="db")
        with mock.patch(
            "builtins.input", side_effect=["SELECT 1", "quit"]
        ), mock.patch.object(
            self.cli, "_run_single_sql", side_effect=ConfigError("boom")
        ):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                self.cli.interactive_shell(args)
        self.assertIn("执行错误", buffer.getvalue())

    def test_execute_file_reports_incomplete_sql(self) -> None:
        """脚本含未闭合语句时给出提示"""
        from src.db_connector_tool.cli import create_argument_parser  # noqa: E402

        parser = create_argument_parser(self.cli)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "bad.sql"
            path.write_text("SELECT 'unclosed", encoding="utf-8")
            args = parser.parse_args(["file", "db", str(path), "-c"])
            with mock.patch.object(self.cli, "_run_single_sql", return_value=True):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    self.cli.execute_file(args)
        self.assertIn("不完整", buffer.getvalue())

    def test_dialect_of_returns_config_type(self) -> None:
        """能从连接配置读到方言名"""
        self.cli.db_manager.config_manager.get_config.return_value = {
            "type": "gbase8s"
        }
        self.assertEqual(
            self.cli._dialect_of("db"), "gbase8s"  # pylint: disable=protected-access
        )

    def test_dialect_of_returns_none_on_error(self) -> None:
        """读配置失败时返回 None（切分器退回通用档）"""
        self.cli.db_manager.config_manager.get_config.side_effect = ConfigError("boom")
        self.assertIsNone(
            self.cli._dialect_of("db")  # pylint: disable=protected-access
        )

    def test_dialect_of_returns_none_for_non_dict(self) -> None:
        """配置不是字典时返回 None"""
        self.cli.db_manager.config_manager.get_config.return_value = "not-a-dict"
        self.assertIsNone(
            self.cli._dialect_of("db")  # pylint: disable=protected-access
        )


class TestJarDownloaderEdgeBranches(IsolatedTestCase):
    """jar_downloader 的网络错误与文件系统兜底"""

    def test_open_url_network_error(self) -> None:
        """网络错误原样抛出（由上层转换为 JarDownloadError）"""
        with mock.patch(
            "urllib.request.urlopen", side_effect=urllib.error.URLError("boom")
        ):
            with self.assertRaises(urllib.error.URLError):
                jar_downloader._open_url(  # pylint: disable=protected-access
                    "https://example.invalid/x", timeout=1
                )

    def test_open_url_reads_local_file(self) -> None:
        """本地路径（含 file://）按文件读取"""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "meta.xml"
            target.write_bytes(b"<metadata/>")
            with jar_downloader._open_url(str(target), 1) as handle:  # pylint: disable=protected-access
                self.assertEqual(handle.read(), b"<metadata/>")

    def test_fetch_metadata_missing_versioning(self) -> None:
        """元数据缺 versioning 节点时报明确错误"""

        @contextlib.contextmanager
        def fake_open(_url, _timeout):
            """每次返回全新的 BytesIO（复用同一个会在第二次读时为空）"""
            yield io.BytesIO(b"<metadata><groupId>x</groupId></metadata>")

        with mock.patch.object(jar_downloader, "_open_url", fake_open):
            with self.assertRaises(jar_downloader.JarDownloadError) as ctx:
                jar_downloader._fetch_metadata(  # pylint: disable=protected-access
                    "https://repo/x", timeout=1, max_bytes=65536
                )
        self.assertIn("versioning", str(ctx.exception))

    def test_fetch_sha1_returns_none_on_network_error(self) -> None:
        """SHA-1 读取失败时返回 None（跳过校验）"""
        with mock.patch.object(
            jar_downloader,
            "_open_url",
            side_effect=urllib.error.URLError("boom"),
        ):
            result = jar_downloader._fetch_sha1(  # pylint: disable=protected-access
                "https://repo/x", "1.0", timeout=1
            )
        self.assertIsNone(result)

    def test_atomic_write_cleans_temp_on_error(self) -> None:
        """写文件失败时清理临时文件"""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "out.jar"
            with mock.patch(
                "os.replace", side_effect=OSError("boom")
            ):
                with self.assertRaises(OSError):
                    jar_downloader._atomic_write(target, b"data")  # pylint: disable=protected-access
            self.assertFalse(list(Path(tmp).glob("*.part*")))

    def test_is_within_rejects_outside(self) -> None:
        """路径越界判定正确"""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "jars"
            base.mkdir()
            self.assertTrue(
                jar_downloader._is_within(base / "a.jar", base)  # pylint: disable=protected-access
            )
            self.assertFalse(
                jar_downloader._is_within(Path(tmp) / "evil.jar", base)  # pylint: disable=protected-access
            )

    def test_read_limited_rejects_oversized(self) -> None:
        """超过上限的响应被拒绝"""
        handle = mock.MagicMock()
        handle.read.side_effect = [b"x" * 10, b"y" * 10, b""]
        with self.assertRaises(jar_downloader.JarDownloadError):
            jar_downloader._read_limited(handle, max_bytes=15)  # pylint: disable=protected-access


class TestGbaseDialectEdgeBranches(IsolatedTestCase):
    """gbase8s/base.py 的编译与隔离级别兜底"""

    def test_get_view_names_delegates(self) -> None:
        """get_view_names 走统一的对象名查询（类型 V）"""
        from src.db_connector_tool.drivers.gbase8s.base import (  # noqa: E402
            GBase8sDialect,
        )

        dialect = GBase8sDialect()
        with mock.patch.object(
            GBase8sDialect, "_get_object_names", return_value=["v1"]
        ) as mocked:
            self.assertEqual(dialect.get_view_names(mock.Mock(), "s"), ["v1"])
        mocked.assert_called_once()

    def test_get_sequence_names_delegates(self) -> None:
        """get_sequence_names 走统一的对象名查询（类型 Q）"""
        from src.db_connector_tool.drivers.gbase8s.base import (  # noqa: E402
            GBase8sDialect,
        )

        dialect = GBase8sDialect()
        with mock.patch.object(
            GBase8sDialect, "_get_object_names", return_value=["s1"]
        ) as mocked:
            self.assertEqual(dialect.get_sequence_names(mock.Mock(), "s"), ["s1"])
        mocked.assert_called_once()

    def test_set_isolation_level_falls_back_to_sql(self) -> None:
        """无对应 JDBC 常量的级别退回 SET ISOLATION 语句"""
        import src.db_connector_tool.drivers.gbase8s.base as base_mod

        dialect = base_mod.GBase8sDialect()
        conn = mock.Mock()
        with mock.patch.dict(base_mod.JDBC_ISOLATION_CONSTANTS, clear=False):
            base_mod.JDBC_ISOLATION_CONSTANTS.pop("REPEATABLE READ", None)
            dialect.set_isolation_level(conn, "REPEATABLE READ")
        conn.cursor.return_value.execute.assert_called_once()
        self.assertIn(
            "SET ISOLATION", conn.cursor.return_value.execute.call_args[0][0]
        )

    def test_get_isolation_level_maps_constants(self) -> None:
        """get_isolation_level 映射 JDBC 常量"""
        from src.db_connector_tool.drivers.gbase8s.base import (  # noqa: E402
            GBase8sDialect,
        )

        dialect = GBase8sDialect()
        for constant, expected in (
            (1, "READ UNCOMMITTED"),
            (2, "READ COMMITTED"),
            (4, "REPEATABLE READ"),
            (8, "SERIALIZABLE"),
        ):
            with self.subTest(constant=constant):
                conn = mock.Mock()
                conn.jconn.getTransactionIsolation.return_value = constant
                self.assertEqual(dialect.get_isolation_level(conn), expected)


if __name__ == "__main__":
    unittest.main()
