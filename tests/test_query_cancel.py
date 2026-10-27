"""查询取消（Ctrl+C 即时中断）的单元测试

覆盖 ``drivers/query_cancel.py`` 与接入点。核心断言围绕三件事：

1. **能力判定**：哪些方言可取消、哪些明确不可（不能"假装能"）；
2. **取消机制**：各方言调对了方法；
3. **安全边界**：GBase 8s / SQL Server 必须在**尝试之前**就拒绝
   （实测 GBase 的 ``jconn.abort()`` 会让进程挂死）。
"""

import sys
import threading
import time
import unittest
from unittest import mock

from src.db_connector_tool.core.exceptions import QueryCancelledError, QueryError
from src.db_connector_tool.drivers import query_cancel as qc

from ._isolation import IsolatedTestCase


def _conn(dialect: str, dbapi: object | None = None) -> mock.Mock:
    """构造带方言名的假连接"""
    connection = mock.Mock()
    connection.dialect.name = dialect
    connection.connection.dbapi_connection = (
        dbapi if dbapi is not None else mock.Mock()
    )
    return connection


class TestSupportsCancel(IsolatedTestCase):
    """能力判定"""

    def test_supported_dialects(self) -> None:
        """实测可取消的方言返回 True"""
        for name in ("postgresql", "sqlite", "mysql", "mariadb"):
            self.assertTrue(qc.supports_cancel(name), msg=name)

    def test_unsupported_dialects(self) -> None:
        """GBase 8s 与 SQL Server 必须返回 False"""
        for name in ("gbase8s", "sqlserver", "", "unknown"):
            self.assertFalse(qc.supports_cancel(name), msg=name)

    def test_case_insensitive(self) -> None:
        """大小写不敏感"""
        self.assertTrue(qc.supports_cancel("PostgreSQL"))
        self.assertTrue(qc.supports_cancel("SQLITE"))

    def test_uncancellable_table_documents_reason(self) -> None:
        """不支持的方言必须写明原因（便于用户理解，而非猜测）"""
        for name, reason in qc.UNCANCELLABLE_DIALECTS.items():
            self.assertTrue(reason, msg=f"{name} 缺少原因说明")
            self.assertGreater(len(reason), 5, msg=f"{name} 原因过于笼统")


class TestCancelMechanisms(IsolatedTestCase):
    """各方言的取消机制"""

    def test_postgresql_uses_dbapi_cancel(self) -> None:
        """PostgreSQL 调 dbapi 的 cancel()"""
        dbapi = mock.Mock()
        connection = _conn("postgresql", dbapi)
        method = qc.cancel_query(connection)
        self.assertEqual(method, "dbapi.cancel()")
        dbapi.cancel.assert_called_once()

    def test_sqlite_uses_interrupt(self) -> None:
        """SQLite 调 dbapi 的 interrupt()"""
        dbapi = mock.Mock()
        connection = _conn("sqlite", dbapi)
        method = qc.cancel_query(connection)
        self.assertEqual(method, "dbapi.interrupt()")
        dbapi.interrupt.assert_called_once()

    def test_mysql_uses_kill_on_side_connection(self) -> None:
        """MySQL 另开连接执行 KILL <id>"""
        dbapi = mock.Mock()
        dbapi.thread_id.return_value = 42
        connection = _conn("mysql", dbapi)
        engine = mock.MagicMock()
        killer = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = killer

        method = qc.cancel_query(connection, engine)
        self.assertIn("KILL", method)
        killer.exec_driver_sql.assert_called_once_with("KILL 42")

    def test_mysql_without_engine_returns_none(self) -> None:
        """MySQL 没给 engine 时无法 KILL，返回 None"""
        connection = _conn("mysql", mock.Mock())
        self.assertIsNone(qc.cancel_query(connection))

    def test_mysql_uses_keepalive_id_when_given(self) -> None:
        """优先用调用方登记的连接 id"""
        dbapi = mock.Mock()
        connection = _conn("mysql", dbapi)
        engine = mock.MagicMock()
        killer = mock.MagicMock()
        engine.connect.return_value.__enter__.return_value = killer

        qc.cancel_query(connection, engine, keepalive=(7,))
        killer.exec_driver_sql.assert_called_once_with("KILL 7")
        dbapi.thread_id.assert_not_called()

    def test_mysql_thread_id_failure_returns_none(self) -> None:
        """取连接 id 失败时返回 None（不抛异常）"""
        dbapi = mock.Mock()
        dbapi.thread_id.side_effect = RuntimeError("boom")
        connection = _conn("mysql", dbapi)
        self.assertIsNone(qc.cancel_query(connection, mock.Mock()))

    def test_mysql_kill_failure_returns_none(self) -> None:
        """KILL 本身失败时返回 None"""
        dbapi = mock.Mock()
        dbapi.thread_id.return_value = 1
        connection = _conn("mysql", dbapi)
        engine = mock.MagicMock()
        killer = mock.MagicMock()
        killer.exec_driver_sql.side_effect = RuntimeError("权限不足")
        engine.connect.return_value.__enter__.return_value = killer
        self.assertIsNone(qc.cancel_query(connection, engine))


class TestCancelSafety(IsolatedTestCase):
    """安全边界：不安全的方言绝不能尝试"""

    def test_gbase8s_never_touches_driver(self) -> None:
        """GBase 8s 必须在碰驱动**之前**就拒绝（实测会挂死）"""
        dbapi = mock.Mock()
        dbapi.jconn = mock.Mock()
        connection = _conn("gbase8s", dbapi)

        self.assertIsNone(qc.cancel_query(connection))
        # 关键：一次都不能调
        dbapi.jconn.abort.assert_not_called()
        connection.connection.dbapi_connection.jconn.abort.assert_not_called()

    def test_sqlserver_never_touches_driver(self) -> None:
        """SQL Server 同样必须在碰驱动之前拒绝"""
        dbapi = mock.Mock()
        connection = _conn("sqlserver", dbapi)
        self.assertIsNone(qc.cancel_query(connection))
        dbapi.cancel.assert_not_called()
        dbapi.close.assert_not_called()

    def test_unknown_dialect_returns_none(self) -> None:
        """未知方言返回 None"""
        connection = _conn("weirddb", mock.Mock())
        self.assertIsNone(qc.cancel_query(connection))

    def test_cancel_exception_is_swallowed(self) -> None:
        """取消过程抛异常时不向外抛（避免掩盖原始错误）"""
        dbapi = mock.Mock()
        dbapi.cancel.side_effect = RuntimeError("boom")
        connection = _conn("postgresql", dbapi)
        self.assertIsNone(qc.cancel_query(connection))


class TestRunCancellable(IsolatedTestCase):
    """工作线程执行 + 可中断等待"""

    def test_normal_completion(self) -> None:
        """正常完成时返回 (结果, 未取消)"""
        value, cancelled = qc.run_cancellable(lambda: 42, lambda: None)
        self.assertEqual(value, 42)
        self.assertFalse(cancelled)

    def test_exception_propagates(self) -> None:
        """操作抛异常时原样透传"""

        def _boom():
            raise ValueError("业务错误")

        with self.assertRaises(ValueError):
            qc.run_cancellable(_boom, lambda: None)

    def test_keyboard_interrupt_scenario_is_covered_by_integration(self) -> None:
        """Ctrl+C 的真实行为由集成测试覆盖

        单元层不模拟信号：``signal.raise_signal`` 会打断 ``Thread.start()``
        内部的事件等待，导致测试本身不稳定。真实场景（含信号投递、
        取消生效、连接复用）放在 ``test_integration_full.py`` 的
        ``TestRealDatabase`` 与子进程探针里验证。
        """
        self.assertTrue(qc.supports_cancel("postgresql"))
        self.assertFalse(qc.supports_cancel("gbase8s"))

    def test_unsupported_cancel_message_explains_reason(self) -> None:
        """取消未生效时的异常文案必须说清原因"""
        error = QueryCancelledError(
            "已收到中断，但该数据库不支持取消正在执行的查询；"
            "查询会在后台继续跑完（结果将被丢弃）。"
        )
        self.assertIn("不支持取消", str(error))
        self.assertTrue(issubclass(QueryCancelledError, QueryError))



    def test_is_query_error_subclass(self) -> None:
        """继承 QueryError —— 可被现有的统一捕获处理"""
        self.assertTrue(issubclass(QueryCancelledError, QueryError))

    def test_reason_field(self) -> None:
        """reason 字段可选并写入 details"""
        error = QueryCancelledError("取消", "CANCEL_001", reason="user")
        self.assertEqual(error.reason, "user")
        self.assertEqual(error.details["reason"], "user")

    def test_reason_optional(self) -> None:
        """不传 reason 也不报错"""
        error = QueryCancelledError("取消")
        self.assertIsNone(error.reason)


class TestDriverIntegration(IsolatedTestCase):
    """接入点存在性与参数"""

    def test_manager_has_cancellable_entry(self) -> None:
        """管理器提供可取消入口"""
        from src.db_connector_tool.core.connections import DatabaseManager

        self.assertTrue(
            hasattr(DatabaseManager, "execute_query_result_cancellable")
        )

    def test_driver_has_cancellable_entry(self) -> None:
        """驱动提供可取消入口"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        self.assertTrue(
            hasattr(SQLAlchemyDriver, "execute_query_result_cancellable")
        )

    def test_sqlite_cancellable_query_works(self) -> None:
        """SQLite 上可取消入口能正常返回结果"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        result = driver.execute_query_result_cancellable("SELECT 5 AS v")
        self.assertEqual(result.rows[0][0], 5)

    def test_cli_has_no_cancel_flag(self) -> None:
        """CLI 的 query/file 支持 --no-cancel"""
        from src.db_connector_tool.cli import create_argument_parser
        from src.db_connector_tool.cli_core import DBConnectorCLI

        parser = create_argument_parser(DBConnectorCLI())
        args = parser.parse_args(["query", "db", "SELECT 1", "--no-cancel"])
        self.assertTrue(args.no_cancel)

    def test_cli_default_is_cancellable(self) -> None:
        """不传 --no-cancel 时默认启用取消"""
        from src.db_connector_tool.cli import create_argument_parser
        from src.db_connector_tool.cli_core import DBConnectorCLI

        parser = create_argument_parser(DBConnectorCLI())
        args = parser.parse_args(["query", "db", "SELECT 1"])
        self.assertFalse(args.no_cancel)


if __name__ == "__main__":
    unittest.main()


class TestRunCancellableCancelPath(IsolatedTestCase):
    """``run_cancellable`` 的中断分支

    通过替换内部等待函数来模拟「主线程收到 KeyboardInterrupt」，
    比注入真实信号稳定得多（真实信号会打断 ``Thread.start()`` 内部等待）。
    """

    def test_cancel_effective_returns_cancelled(self) -> None:
        """取消生效时返回 (None, True)"""
        release = threading.Event()

        def _slow():
            """等 release 后返回（模拟被取消后收尾）"""
            release.wait(timeout=3)
            return "never-returned"

        def _on_cancel():
            """取消回调：放行 worker"""
            release.set()
            return "dbapi.cancel()"

        def _fake_wait(stop, thread, timeout):  # noqa: ARG001
            """模拟「主线程收到中断」"""
            raise KeyboardInterrupt

        with mock.patch.object(qc, "_wait_until_done", _fake_wait):
            value, cancelled = qc.run_cancellable(_slow, _on_cancel)

        self.assertTrue(cancelled)
        self.assertIsNone(value)

    def test_cancel_callback_raises_but_worker_finishes(self) -> None:
        """取消回调自己抛异常，但 worker 已结束 → 仍报「取消未生效」"""
        release = threading.Event()

        def _slow():
            """很快结束"""
            release.set()
            return "done"

        def _bad_cancel():
            raise RuntimeError("取消失败")

        def _fake_wait(stop, thread, timeout):  # noqa: ARG001
            raise KeyboardInterrupt

        with mock.patch.object(qc, "_wait_until_done", _fake_wait):
            with self.assertRaises(QueryCancelledError) as ctx:
                qc.run_cancellable(_slow, _bad_cancel)
        self.assertIn("未生效", str(ctx.exception))

    def test_cancel_ineffective_raises_clear_error(self) -> None:
        """worker 仍然存活时，必须抛异常说明「查询仍在后台执行」"""
        release = threading.Event()

        def _stuck():
            """长时间不结束"""
            release.wait(timeout=5)
            return "still-running"

        def _noop_cancel():
            return None

        def _fake_wait(stop, thread, timeout):  # noqa: ARG001
            raise KeyboardInterrupt

        try:
            with mock.patch.object(qc, "_wait_until_done", _fake_wait), \
                    mock.patch.object(qc, "threading") as fake_threading:
                # join 立刻返回，但 is_alive 恒为 True → 模拟取消失败
                fake_threading.Thread.return_value = mock.Mock(
                    start=mock.Mock(),
                    join=mock.Mock(),
                    is_alive=mock.Mock(return_value=True),
                )
                fake_threading.Event = threading.Event
                with self.assertRaises(QueryCancelledError) as ctx:
                    qc.run_cancellable(_stuck, _noop_cancel)
            self.assertIn("不支持取消", str(ctx.exception))
        finally:
            release.set()

    def test_wait_until_done_returns_when_stop_set(self) -> None:
        """wait 在 stop 置位后立即返回"""
        stop = threading.Event()
        stop.set()
        thread = mock.Mock(is_alive=mock.Mock(return_value=False))
        qc._wait_until_done(stop, thread, 5)  # pylint: disable=protected-access

    def test_wait_until_done_respects_timeout(self) -> None:
        """超过 timeout 后返回（不无限等待）"""
        stop = threading.Event()
        thread = mock.Mock(is_alive=mock.Mock(return_value=True))
        start = time.monotonic()
        qc._wait_until_done(stop, thread, 0.25)  # pylint: disable=protected-access
        self.assertLess(time.monotonic() - start, 2)

    def test_wait_until_done_detects_dead_thread(self) -> None:
        """线程已死时提前返回"""
        stop = threading.Event()
        thread = mock.Mock(is_alive=mock.Mock(return_value=False))
        qc._wait_until_done(stop, thread, 5)  # pylint: disable=protected-access
