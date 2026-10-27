"""覆盖率补测：``timeout_control`` 各 applier 与异常分支

这些分支都是「设置超时失败」的兜底 —— 真实环境很少触发，
但必须保证失败时**不崩溃、不误判**，而是安静地退回上层的处理逻辑。
"""

import time
import unittest
from unittest import mock

from src.db_connector_tool.drivers import timeout_control as tc

from ._isolation import IsolatedTestCase


def _conn(dialect: str, dbapi: object | None = None) -> mock.Mock:
    """构造一个带方言名的假连接"""
    connection = mock.Mock()
    connection.dialect.name = dialect
    connection.connection.dbapi_connection = dbapi if dbapi is not None else mock.Mock()
    return connection


class TestApplySqlTimeout(IsolatedTestCase):
    """PostgreSQL / MySQL 的 SQL 设置路径"""

    def test_success_returns_dialect(self) -> None:
        """设置成功返回方言名"""
        connection = _conn("postgresql")
        self.assertEqual(tc.apply_server_timeout(connection, 2), "postgresql")
        connection.exec_driver_sql.assert_called_once()

    def test_failure_returns_none(self) -> None:
        """SQL 执行失败时返回 None（不抛异常）"""
        connection = _conn("postgresql")
        connection.exec_driver_sql.side_effect = RuntimeError("权限不足")
        self.assertIsNone(tc.apply_server_timeout(connection, 2))

    def test_unsupported_dialect_returns_none(self) -> None:
        """无机制方言返回 None"""
        connection = _conn("sqlserver")
        self.assertIsNone(tc.apply_server_timeout(connection, 2))

    def test_none_timeout_returns_none(self) -> None:
        """timeout 为 None 直接返回 None"""
        connection = _conn("postgresql")
        self.assertIsNone(tc.apply_server_timeout(connection, None))
        connection.exec_driver_sql.assert_not_called()


class TestApplyOracleTimeout(IsolatedTestCase):
    """Oracle 的 call_timeout 路径"""

    def test_success_sets_milliseconds(self) -> None:
        """成功时设置毫秒值"""
        dbapi = mock.Mock()
        connection = _conn("oracle", dbapi)
        self.assertEqual(tc.apply_server_timeout(connection, 1.5), "oracle")
        self.assertEqual(dbapi.call_timeout, 1500)

    def test_failure_returns_none(self) -> None:
        """设置失败返回 None"""

        class ReadOnly:
            """禁止设置属性，模拟只读对象"""

            @property
            def call_timeout(self):
                """只读"""
                return 0

        connection = _conn("oracle", ReadOnly())
        self.assertIsNone(tc.apply_server_timeout(connection, 1))

    def test_missing_dbapi_returns_none(self) -> None:
        """取不到 dbapi 连接时返回 None"""
        connection = mock.Mock()
        connection.dialect.name = "oracle"
        connection.connection.dbapi_connection = None
        # None 上设置属性会抛 AttributeError
        self.assertIsNone(tc.apply_server_timeout(connection, 1))


class TestApplySqliteTimeout(IsolatedTestCase):
    """SQLite 的 progress_handler 路径"""

    def test_success_installs_handler(self) -> None:
        """成功安装 handler"""
        dbapi = mock.Mock()
        connection = _conn("sqlite", dbapi)
        self.assertEqual(tc.apply_server_timeout(connection, 1), "sqlite")
        dbapi.set_progress_handler.assert_called_once()

    def test_missing_dbapi_returns_none(self) -> None:
        """取不到 dbapi 时返回 None"""
        connection = mock.Mock()
        connection.dialect.name = "sqlite"
        connection.connection.dbapi_connection = None
        self.assertIsNone(tc.apply_server_timeout(connection, 1))

    def test_handler_install_failure_returns_none(self) -> None:
        """安装 handler 失败返回 None"""
        dbapi = mock.Mock()
        dbapi.set_progress_handler.side_effect = RuntimeError("不支持")
        connection = _conn("sqlite", dbapi)
        self.assertIsNone(tc.apply_server_timeout(connection, 1))


class TestRestoreServerTimeout(IsolatedTestCase):
    """会话级超时的复位"""

    def test_mysql_restores(self) -> None:
        """MySQL 会执行复位语句"""
        connection = _conn("mysql")
        tc.restore_server_timeout(connection, "mysql")
        connection.exec_driver_sql.assert_called_once()
        self.assertIn("= 0", connection.exec_driver_sql.call_args[0][0])

    def test_mariadb_restores(self) -> None:
        """MariaDB 同样复位"""
        connection = _conn("mariadb")
        tc.restore_server_timeout(connection, "mariadb")
        connection.exec_driver_sql.assert_called_once()

    def test_postgresql_needs_no_restore(self) -> None:
        """PostgreSQL 用 SET LOCAL，无需复位"""
        connection = _conn("postgresql")
        tc.restore_server_timeout(connection, "postgresql")
        connection.exec_driver_sql.assert_not_called()

    def test_none_dialect_is_noop(self) -> None:
        """方言为 None 时什么都不做"""
        connection = _conn("postgresql")
        tc.restore_server_timeout(connection, None)
        connection.exec_driver_sql.assert_not_called()

    def test_restore_failure_is_swallowed(self) -> None:
        """复位失败只记日志（连接可能已失效）"""
        connection = _conn("mysql")
        connection.exec_driver_sql.side_effect = RuntimeError("连接已断")
        # 不应抛异常
        tc.restore_server_timeout(connection, "mysql")


class TestDeadline(IsolatedTestCase):
    """SQLite 的截止时刻对象"""

    def test_not_expired_initially(self) -> None:
        """刚创建时未到期，handler 返回 0"""
        deadline = tc._Deadline(10)  # pylint: disable=protected-access
        self.assertFalse(deadline.expired)
        self.assertEqual(deadline.handler(), 0)
        self.assertGreater(deadline.remaining, 0)

    def test_expires_after_timeout(self) -> None:
        """超时后 handler 返回 1"""
        deadline = tc._Deadline(0.05)  # pylint: disable=protected-access
        time.sleep(0.1)
        self.assertTrue(deadline.expired)
        self.assertEqual(deadline.handler(), 1)
        self.assertLess(deadline.remaining, 0)


class TestWatchdogState(IsolatedTestCase):
    """看门狗状态对象"""

    def test_initial_state(self) -> None:
        """初始未触发、未结束"""
        state = tc.WatchdogState()
        self.assertFalse(state.timed_out)
        self.assertFalse(state.finished.is_set())

    def test_mark_finished(self) -> None:
        """mark_finished 置位结束事件"""
        state = tc.WatchdogState()
        state.mark_finished()
        self.assertTrue(state.finished.is_set())

    def test_wait_finished_returns_true_when_set(self) -> None:
        """已结束时 wait_finished 立即返回 True"""
        state = tc.WatchdogState()
        state.mark_finished()
        self.assertTrue(state.wait_finished(1))

    def test_wait_finished_times_out(self) -> None:
        """未结束时等到超时返回 False"""
        state = tc.WatchdogState()
        self.assertFalse(state.wait_finished(0.05))

    def test_timed_out_reflects_fired(self) -> None:
        """timed_out 反映 fired 标志"""
        state = tc.WatchdogState()
        state.fired.set()
        self.assertTrue(state.timed_out)


if __name__ == "__main__":
    unittest.main()
