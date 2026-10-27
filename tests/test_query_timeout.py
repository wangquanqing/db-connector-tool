"""查询超时的单元测试（``drivers/timeout_control.py`` 与接入点）

覆盖点：

1. :func:`normalize_timeout` 的入参校验（含 ``True`` 这种易错值）；
2. :func:`server_timeout_for` 的方言映射（含毫秒/秒两种单位）；
3. :func:`is_timeout_error` 对各驱动文案的识别；
4. :func:`to_timeout_error` 的字段完整性；
5. :func:`watchdog` 对**不安全方言**必须拒绝（实测 GBase 会挂死）；
6. 接入点：``execute_*`` 能传超时，且 ``timeout=None`` 时行为不变。
"""

import threading
import time
import unittest
from unittest import mock

from src.db_connector_tool.core.exceptions import DBTimeoutError, QueryError
from src.db_connector_tool.drivers import timeout_control as tc

from ._isolation import IsolatedTestCase


class TestNormalizeTimeout(IsolatedTestCase):
    """入参规范化与校验"""

    def test_none_means_no_limit(self) -> None:
        """None 表示不限制"""
        self.assertIsNone(tc.normalize_timeout(None))

    def test_zero_and_negative_mean_no_limit(self) -> None:
        """0 与负数都表示不限制"""
        self.assertIsNone(tc.normalize_timeout(0))
        self.assertIsNone(tc.normalize_timeout(-5))

    def test_positive_values_pass_through(self) -> None:
        """正数原样返回（转成 float）"""
        self.assertEqual(tc.normalize_timeout(3), 3.0)
        self.assertEqual(tc.normalize_timeout(1.5), 1.5)
        self.assertIsInstance(tc.normalize_timeout(2), float)

    def test_non_numeric_raises(self) -> None:
        """非数值抛 ValueError"""
        for bad in ("abc", [], {}, object()):
            with self.assertRaises(ValueError):
                tc.normalize_timeout(bad)

    def test_bool_rejected(self) -> None:
        """布尔值被拒绝（避免 True 被当成 1 秒）"""
        for flag in (True, False):
            with self.assertRaises(ValueError):
                tc.normalize_timeout(flag)


class TestServerTimeoutFor(IsolatedTestCase):
    """各方言的超时语句"""

    def test_postgresql_uses_milliseconds(self) -> None:
        """PostgreSQL 用毫秒，且是 SET LOCAL"""
        sql = tc.server_timeout_for("postgresql", 1.5)
        self.assertEqual(sql, "SET LOCAL statement_timeout = 1500")
        self.assertIn("LOCAL", sql)

    def test_mysql_uses_session_and_milliseconds(self) -> None:
        """MySQL 用 SESSION 级 + 毫秒"""
        sql = tc.server_timeout_for("mysql", 2)
        self.assertEqual(sql, "SET SESSION max_execution_time = 2000")

    def test_mariadb_uses_seconds(self) -> None:
        """MariaDB 的 max_statement_time 单位是**秒**"""
        sql = tc.server_timeout_for("mariadb", 2.5)
        self.assertEqual(sql, "SET SESSION max_statement_time = 2")

    def test_minimum_one_millisecond(self) -> None:
        """极小的超时也至少 1 毫秒（不能是 0，0 表示不限制）"""
        self.assertIn("= 1", tc.server_timeout_for("postgresql", 0.0001))

    def test_unsupported_dialects_return_none(self) -> None:
        """无服务端机制的方言返回 None"""
        for name in ("sqlserver", "sqlite", "oracle", "gbase8s", "未知方言"):
            self.assertIsNone(tc.server_timeout_for(name, 5), msg=name)


class TestIsTimeoutError(IsolatedTestCase):
    """驱动超时异常的识别"""

    def test_recognizes_common_messages(self) -> None:
        """常见驱动的超时文案都能识别"""
        samples = [
            "canceling statement due to statement timeout",  # psycopg
            "(3024, 'Query execution was interrupted')",     # MySQL
            "ORA-01013: user requested cancel",              # Oracle
            "QueryTimeout expired",                          # pyodbc/JDBC
            "interrupted",                                   # sqlite
        ]
        for text in samples:
            self.assertTrue(tc.is_timeout_error(RuntimeError(text)), msg=text)

    def test_rejects_unrelated_errors(self) -> None:
        """普通错误不误判为超时"""
        samples = ["表不存在", "connection refused", "syntax error", "权限不足"]
        for text in samples:
            self.assertFalse(tc.is_timeout_error(RuntimeError(text)), msg=text)

    def test_db_timeout_error_itself(self) -> None:
        """DBTimeoutError 自身被识别"""
        self.assertTrue(tc.is_timeout_error(DBTimeoutError("超时", timeout_seconds=1)))


class TestToTimeoutError(IsolatedTestCase):
    """异常包装"""

    def test_fields_populated(self) -> None:
        """timeout_seconds / operation 正确写入"""
        error = tc.to_timeout_error(RuntimeError("boom"), 3.0, "query")
        self.assertIsInstance(error, DBTimeoutError)
        self.assertEqual(error.timeout_seconds, 3.0)
        self.assertEqual(error.operation, "query")
        self.assertEqual(error.error_code, "TIMEOUT_001")
        self.assertIn("3", error.message)

    def test_message_keeps_first_line_only(self) -> None:
        """多行报错只取首行（避免刷屏）"""
        error = tc.to_timeout_error(RuntimeError("首行\n第二行\n第三行"), 2.0)
        self.assertIn("首行", error.message)
        self.assertNotIn("第三行", error.message)

    def test_none_timeout_does_not_crash(self) -> None:
        """timeout 为 None 时也能生成异常"""
        error = tc.to_timeout_error(RuntimeError("boom"), None)
        self.assertIsInstance(error, DBTimeoutError)


class TestWatchdogSafety(IsolatedTestCase):
    """看门狗的方言安全闸门（本模块最关键的防线）"""

    def test_rejects_unsafe_dialect(self) -> None:
        """不安全的方言必须直接拒绝（实测 GBase 会让进程挂死）"""
        connection = mock.Mock()
        connection.dialect.name = "gbase8s"
        with self.assertRaises(tc.TimeoutNotSupportedError) as ctx:
            with tc.watchdog(connection, 1):
                pass
        self.assertIn("gbase8s", str(ctx.exception))
        connection.invalidate.assert_not_called()

    def test_rejects_sqlserver(self) -> None:
        """SQL Server 同样拒绝（无语句级机制）"""
        connection = mock.Mock()
        connection.dialect.name = "sqlserver"
        with self.assertRaises(tc.TimeoutNotSupportedError):
            with tc.watchdog(connection, 1):
                pass

    def test_none_timeout_is_passthrough(self) -> None:
        """timeout 为 None 时不启用看门狗（也不检查方言）"""
        connection = mock.Mock()
        connection.dialect.name = "gbase8s"
        with tc.watchdog(connection, None) as state:
            pass
        self.assertFalse(state.timed_out)
        connection.invalidate.assert_not_called()

    def test_safe_dialect_normal_completion(self) -> None:
        """安全方言正常完成时不触发 invalidate"""
        connection = mock.Mock()
        connection.dialect.name = "postgresql"
        with tc.watchdog(connection, 5) as state:
            time.sleep(0.05)
        self.assertFalse(state.timed_out)
        connection.invalidate.assert_not_called()

    def test_safe_dialect_fires_on_timeout(self) -> None:
        """安全方言超时后触发 invalidate 并置 timed_out"""
        connection = mock.Mock()
        connection.dialect.name = "postgresql"
        with tc.watchdog(connection, 0.1) as state:
            # 模拟"查询一直不返回"
            time.sleep(0.3)
        self.assertTrue(state.timed_out)
        connection.invalidate.assert_called_once()


class TestSupportsQueryTimeout(IsolatedTestCase):
    """能力查询"""

    def test_supported_dialects(self) -> None:
        """有机制的方言返回 True"""
        for name in ("postgresql", "mysql", "mariadb", "sqlite", "oracle"):
            self.assertTrue(tc.supports_query_timeout(name), msg=name)

    def test_unsupported_dialects(self) -> None:
        """明确不支持的方言返回 False"""
        for name in ("sqlserver", "gbase8s", "", "unknown"):
            self.assertFalse(tc.supports_query_timeout(name), msg=name)


class TestExceptionHierarchy(IsolatedTestCase):
    """异常继承关系（决定能否被上层统一捕获）"""

    def test_timeout_not_supported_is_query_error(self) -> None:
        """TimeoutNotSupportedError 继承 QueryError（可被统一捕获）"""
        self.assertTrue(
            issubclass(tc.TimeoutNotSupportedError, QueryError)
        )

    def test_db_timeout_error_is_not_query_error(self) -> None:
        """DBTimeoutError 不是 QueryError —— 上层需分别处理"""
        self.assertFalse(issubclass(DBTimeoutError, QueryError))


class TestQueryPathsAcceptTimeout(IsolatedTestCase):
    """接入点：查询入口能接收 timeout 参数"""

    def test_driver_execute_query_result_accepts_timeout(self) -> None:
        """驱动层 execute_query_result 接受 timeout 且 None 时行为不变"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        result = driver.execute_query_result("SELECT 1 AS a", timeout=None)
        self.assertEqual(result.rows[0][0], 1)

    def test_driver_execute_query_accepts_timeout(self) -> None:
        """驱动层 execute_query 接受 timeout"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        rows = driver.execute_query("SELECT 2 AS b", timeout=None)
        self.assertEqual(len(rows), 1)

    def test_driver_execute_command_accepts_timeout(self) -> None:
        """驱动层 execute_command 接受 timeout"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.execute_command("CREATE TABLE t (a INT)", timeout=None)
        affected = driver.execute_command("INSERT INTO t VALUES (1)", timeout=None)
        self.assertEqual(affected, 1)

    def test_sqlite_timeout_actually_interrupts(self) -> None:
        """SQLite 的 progress_handler 真的能中断长查询"""
        from src.db_connector_tool.core.exceptions import DBTimeoutError as TimeoutErr
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        long_sql = (
            "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL "
            "SELECT x+1 FROM c WHERE x < 30000000) SELECT count(*) FROM c"
        )
        with self.assertRaises((TimeoutErr, QueryError)):
            driver.execute_query_result(long_sql, timeout=0.3)


class TestRunSqlPassthrough(IsolatedTestCase):
    """run_sql 对超时异常必须原样透传（否则丢失 timeout_seconds）"""

    def test_db_timeout_error_passes_through(self) -> None:
        """DBTimeoutError 不被包成 QueryError"""
        from src.db_connector_tool.drivers.result_fetch import run_sql

        original = DBTimeoutError("超时了", timeout_seconds=2, operation="query")

        def _boom():
            raise original

        with self.assertRaises(DBTimeoutError) as ctx:
            run_sql(_boom)
        self.assertIs(ctx.exception, original)
        self.assertEqual(ctx.exception.timeout_seconds, 2)


if __name__ == "__main__":
    unittest.main()
