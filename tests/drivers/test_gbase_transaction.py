"""GBase 8s「不支持事务」的降级测试

**守的问题**（真实故障，2026-11 用户重装 WSL 版 GBase 后连不上）：

新版 GBase 12.10 的数据库可能以**不记日志（No Logging）**模式创建。
这类库没有事务概念，对以下调用统一抛
``java.sql.SQLException: 不支持事务``：

- ``setAutoCommit(False)``
- ``commit()`` / ``rollback()``
- ``setTransactionIsolation()``

而本项目原先在「建连」「写操作」「设隔离级别」三处**无条件调用**它们，
导致：

1. 连接一建立就失败 → 用户看到「连不上」；
2. 即使绕过，``CREATE TABLE`` / ``INSERT`` 全部失败 → 数据库不可用。

修法：把「不支持事务」识别为**可降级条件**，保持自动提交继续工作。
写操作在自动提交下已立即生效，主用途不受影响。
"""

import unittest
from unittest import mock

from src.db_connector_tool.drivers.gbase8s.base import (
    is_transaction_unsupported,
)
from src.db_connector_tool.drivers.gbase8s.jaydebeapi import (
    disable_autocommit,
)

from .._isolation import IsolatedTestCase


class TestIsTransactionUnsupported(IsolatedTestCase):
    """异常识别"""

    def test_recognizes_chinese_message(self) -> None:
        """识别 GBase 的中文报错"""
        samples = [
            "java.sql.SQLException: 不支持事务",
            "SQLException: 不记日志的数据库上没有事务隔离",
        ]
        for text in samples:
            self.assertTrue(
                is_transaction_unsupported(RuntimeError(text)), msg=text
            )

    def test_recognizes_english_message(self) -> None:
        """识别英文等价消息"""
        samples = [
            "Transaction not supported",
            "transactions are not supported by this server",
            "does not support transactions",
        ]
        for text in samples:
            self.assertTrue(
                is_transaction_unsupported(RuntimeError(text)), msg=text
            )

    def test_rejects_unrelated_errors(self) -> None:
        """普通错误不能被误判"""
        samples = [
            "语法错误",
            "connection refused",
            "表不存在",
            "权限不足",
        ]
        for text in samples:
            self.assertFalse(
                is_transaction_unsupported(RuntimeError(text)), msg=text
            )


class TestDisableAutocommit(IsolatedTestCase):
    """``disable_autocommit`` 的降级行为"""

    def _conn(self, side_effect=None) -> mock.Mock:
        """构造带 jconn 的假连接"""
        conn = mock.Mock()
        conn.jconn = mock.Mock()
        if side_effect is not None:
            conn.jconn.setAutoCommit.side_effect = side_effect
        return conn

    def test_success_returns_true(self) -> None:
        """支持事务的服务器：成功关闭自动提交"""
        conn = self._conn()
        self.assertTrue(disable_autocommit(conn))
        conn.jconn.setAutoCommit.assert_called_once_with(False)

    def test_unsupported_returns_false_without_raising(self) -> None:
        """不支持事务时返回 False，**不抛异常**（关键）"""
        conn = self._conn(side_effect=RuntimeError("java.sql.SQLException: 不支持事务"))
        # 关键断言：不抛异常
        self.assertFalse(disable_autocommit(conn))

    def test_unsupported_restores_autocommit(self) -> None:
        """降级后要把连接置回自动提交（确保写操作能生效）"""
        conn = self._conn(side_effect=RuntimeError("不支持事务"))
        disable_autocommit(conn)
        # 最后一次调用应为 True
        self.assertEqual(conn.jconn.setAutoCommit.call_args_list[-1], mock.call(True))

    def test_other_errors_also_degrade(self) -> None:
        """其它异常同样不该阻断连接"""
        conn = self._conn(side_effect=RuntimeError("网络抖动"))
        self.assertFalse(disable_autocommit(conn))

    def test_missing_jconn_is_safe(self) -> None:
        """连接没有 jconn 属性时安全返回"""
        conn = mock.Mock(spec=[])  # 无任何属性
        self.assertFalse(disable_autocommit(conn))

    def test_restore_failure_is_silent(self) -> None:
        """恢复自动提交失败也不抛异常"""
        conn = self._conn()

        def _side_effect(value):  # noqa: ARG001
            raise RuntimeError("不支持事务")

        conn.jconn.setAutoCommit.side_effect = _side_effect
        # 不应抛异常
        self.assertFalse(disable_autocommit(conn))


class TestDialectCommitRollback(IsolatedTestCase):
    """方言的 ``do_commit`` / ``do_rollback`` 降级"""

    def _dialect(self):
        """构造方言实例"""
        from src.db_connector_tool.drivers.gbase8s.jaydebeapi import (
            GBase8sDialect_jaydebeapi,
        )

        return GBase8sDialect_jaydebeapi()

    def test_do_commit_normal(self) -> None:
        """正常 commit 会被调用"""
        dialect = self._dialect()
        conn = mock.Mock()
        conn._closed = False  # pylint: disable=protected-access
        dialect.do_commit(conn)
        conn.commit.assert_called_once()

    def test_do_commit_unsupported_is_swallowed(self) -> None:
        """不支持事务时 commit 静默跳过（否则所有写操作都失败）"""
        dialect = self._dialect()
        conn = mock.Mock()
        conn._closed = False  # pylint: disable=protected-access
        conn.commit.side_effect = RuntimeError("java.sql.SQLException: 不支持事务")
        # 关键：不抛异常
        dialect.do_commit(conn)

    def test_do_commit_other_error_raised(self) -> None:
        """其它 commit 错误仍要抛出（不掩盖真实问题）"""
        dialect = self._dialect()
        conn = mock.Mock()
        conn._closed = False  # pylint: disable=protected-access
        conn.commit.side_effect = RuntimeError("磁盘写满")
        with self.assertRaises(RuntimeError):
            dialect.do_commit(conn)

    def test_do_commit_skips_closed(self) -> None:
        """连接已关闭时不调用 commit"""
        dialect = self._dialect()
        conn = mock.Mock()
        conn._closed = True  # pylint: disable=protected-access
        dialect.do_commit(conn)
        conn.commit.assert_not_called()

    def test_do_rollback_unsupported_is_swallowed(self) -> None:
        """不支持事务时 rollback 静默跳过"""
        dialect = self._dialect()
        conn = mock.Mock()
        conn._closed = False  # pylint: disable=protected-access
        conn.rollback.side_effect = RuntimeError("不支持事务")
        dialect.do_rollback(conn)  # 不应抛

    def test_do_rollback_skips_closed(self) -> None:
        """连接已关闭时不调用 rollback"""
        dialect = self._dialect()
        conn = mock.Mock()
        conn._closed = True  # pylint: disable=protected-access
        dialect.do_rollback(conn)
        conn.rollback.assert_not_called()


class TestDialectIsolationLevel(IsolatedTestCase):
    """方言的 ``set_isolation_level`` 降级"""

    def _dialect(self):
        from src.db_connector_tool.drivers.gbase8s.jaydebeapi import (
            GBase8sDialect_jaydebeapi,
        )

        return GBase8sDialect_jaydebeapi()

    def _conn(self, unsupported: bool = False) -> mock.Mock:
        """构造假 dbapi 连接"""
        conn = mock.Mock()
        conn.jconn = mock.Mock()
        if unsupported:
            conn.jconn.setAutoCommit.side_effect = RuntimeError("不支持事务")
            conn.jconn.setTransactionIsolation.side_effect = RuntimeError("不支持事务")
        return conn

    def test_autocommit_always_works(self) -> None:
        """AUTOCOMMIT 级别始终可用"""
        dialect = self._dialect()
        conn = self._conn()
        dialect.set_isolation_level(conn, "AUTOCOMMIT")
        conn.jconn.setAutoCommit.assert_called_with(True)

    def test_normal_level_sets_isolation(self) -> None:
        """支持事务时能正常设置隔离级别"""
        dialect = self._dialect()
        conn = self._conn()
        dialect.set_isolation_level(conn, "READ COMMITTED")
        conn.jconn.setAutoCommit.assert_called_once_with(False)
        conn.jconn.setTransactionIsolation.assert_called_once()

    def test_unsupported_level_degrades(self) -> None:
        """不支持事务时降级为自动提交（不抛异常）"""
        dialect = self._dialect()
        conn = self._conn(unsupported=True)
        # 关键：不抛异常
        dialect.set_isolation_level(conn, "READ COMMITTED")

    def test_invalid_level_still_raises(self) -> None:
        """无效的级别名仍要抛 ArgumentError（不吞用法错误）"""
        from sqlalchemy import exc

        dialect = self._dialect()
        conn = self._conn()
        with self.assertRaises(exc.ArgumentError):
            dialect.set_isolation_level(conn, "NO_SUCH_LEVEL")


if __name__ == "__main__":
    unittest.main()


class TestCancellableEntryAvoidsWorkerThread(IsolatedTestCase):
    """可取消入口对**不支持取消的方言**必须走主线程

    **守的是一个会让进程挂死的严重问题**（真实故障：2026-11 重装 WSL 版
    GBase 后，CLI 的 `query` 命令输出正确但进程永远退不出去）。

    实测因果链（子进程验证）：

    ==========================  ===========  ==========
    场景                        耗时          结果
    ==========================  ===========  ==========
    线程内只 import             1.15s        ✅ 正常退出
    **线程内建连**（已 close）  **>30s**     ❌ 挂死
    **管理器走工作线程**        **>30s**     ❌ 挂死
    主线程建连                  2.96s        ✅ 正常退出
    ==========================  ===========  ==========

    JPype 的 JVM 与创建连接的线程绑定，工作线程里建连后
    ``atexit`` 阶段永远等不到线程结束 —— 连 ``shutdownJVM()`` 都会阻塞。

    因此这类方言必须走主线程；反正它们本来也不支持取消。
    """

    def test_gbase_does_not_use_worker_thread(self) -> None:
        """GBase 走主线程（不创建 dct-query-worker）"""
        import threading as threading_module

        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        # 伪造方言名为 gbase8s，验证分支选择（不真的连 JDBC）
        before = {t.name for t in threading_module.enumerate()}

        called = {"reached": False}

        def _fake_connect():
            """模拟主线程直连路径"""
            called["reached"] = True
            raise RuntimeError("到此为止（只验证分支）")

        driver.engine = mock.Mock()
        driver.engine.dialect.name = "gbase8s"
        driver.engine.connect.side_effect = _fake_connect

        with self.assertRaises(RuntimeError):
            driver.execute_query_result_cancellable("SELECT 1")

        self.assertTrue(called["reached"], msg="应走主线程分支")
        after = {t.name for t in threading_module.enumerate()}
        self.assertEqual(
            before, after, msg="不应创建任何工作线程"
        )

    def test_gbase_is_not_cancellable(self) -> None:
        """GBase 仍在「不支持取消」名单里（前提条件）"""
        from src.db_connector_tool.drivers.query_cancel import supports_cancel

        self.assertFalse(supports_cancel("gbase8s"))

    def test_supported_dialect_still_uses_worker(self) -> None:
        """支持取消的方言仍走工作线程（不能一刀切改回主线程）"""
        from src.db_connector_tool.drivers.query_cancel import supports_cancel

        self.assertTrue(supports_cancel("postgresql"))
        self.assertTrue(supports_cancel("sqlite"))

    def test_sqlserver_also_avoids_worker_thread(self) -> None:
        """SQL Server 同样不支持取消，也应走主线程"""
        from src.db_connector_tool.drivers.query_cancel import supports_cancel

        self.assertFalse(supports_cancel("sqlserver"))
