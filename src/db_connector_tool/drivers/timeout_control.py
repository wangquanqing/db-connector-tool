# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""查询超时控制

给一次查询加**时间上限**，超时就中断并抛
:class:`~db_connector_tool.core.exceptions.DBTimeoutError`。

**为什么不能只靠客户端计时**

用户按下 Ctrl+C 或超时后想「立刻停下」，但底层 DBAPI 调用是同步阻塞的：
Python 的信号处理器要等该调用返回才会执行。更关键的是，实测
``connection.invalidate()``（最通用的「作废连接」手段）**只对部分方言安全**：

============  =========================  ===================
方言          ``invalidate()`` 实测结果   结论
============  =========================  ===================
PostgreSQL    ✅ 1.50s 内中断            可用作兜底
SQLite        ✅ 3.88s 内中断            可用作兜底
MySQL         ⚠️ **等满 8s 才返回**      无效（但有服务端机制）
SQL Server    ⚠️ 结果不确定              不敢用
**GBase 8s**  ❌ **进程直接挂死**         **绝不能用**
============  =========================  ===================

因此本模块采用**三档策略**：

1. **服务端超时优先** —— 让数据库自己杀掉语句，客户端一定能返回；
2. **客户端看门狗** —— 仅对白名单方言启用（实测不会挂死）；
3. **都不支持则明确报错** —— 抛 :class:`ValueError` 说明该方言不支持，
   **绝不静默忽略，也绝不冒挂死的风险**。

各方言的支持情况（均实测）：

============  ==================================  ==============
方言          机制                                查询级超时
============  ==================================  ==============
PostgreSQL    ``SET LOCAL statement_timeout``      ✅ 支持
MySQL         ``SET SESSION max_execution_time``   ✅ 支持
MariaDB       ``SET SESSION max_statement_time``   ✅ 支持
SQLite        ``set_progress_handler``             ✅ 支持
Oracle        ``connection.call_timeout``          ✅ 支持
GBase 8s      JDBC ``Statement.setQueryTimeout``   ⚠️ 未接入，明确报错
SQL Server    **无语句级机制**                      ❌ 不支持，明确报错
============  ==================================  ==============

.. warning::
    MySQL 的 ``max_execution_time`` **不覆盖** ``SLEEP()`` 一类函数
    （MySQL 文档明示），这类语句只能靠客户端兜底，而 MySQL 的
    看门狗又无效 —— 属于已知局限。

.. note::
    SQL Server 可在**连接级**配置超时（pymssql 的 ``connect_args={"timeout": N}``，
    实测有效），但那作用于所有查询，不是单条语句。

.. warning::
    MySQL 的 ``max_execution_time`` **不覆盖** ``SLEEP()`` 一类函数
    （MySQL 文档明示），这类语句只能靠客户端兜底。

Example:
    >>> from db_connector_tool.drivers.timeout_control import normalize_timeout
    >>> normalize_timeout(3)
    3.0
    >>> normalize_timeout(None) is None
    True
"""

import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator, Optional

from ..core.exceptions import DBTimeoutError, QueryError
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

__all__ = [
    "TIMEOUT_STATEMENTS",
    "SESSION_SCOPED_DIALECTS",
    "WATCHDOG_SAFE_DIALECTS",
    "NO_QUERY_TIMEOUT_DIALECTS",
    "normalize_timeout",
    "server_timeout_for",
    "apply_server_timeout",
    "restore_server_timeout",
    "WatchdogState",
    "watchdog",
    "is_timeout_error",
    "to_timeout_error",
    "supports_query_timeout",
    "TimeoutNotSupportedError",
]


#: 各方言设置「语句级超时」的 SQL 模板
#:
#: ``None`` 表示该方言**没有**可用的服务端机制，只能靠客户端看门狗。
TIMEOUT_STATEMENTS = {
    "postgresql": "SET LOCAL statement_timeout = {ms}",
    "mysql": "SET SESSION max_execution_time = {ms}",
    "mariadb": "SET SESSION max_statement_time = {seconds}",
}

#: Oracle / GBase 走驱动 API（不是 SQL），单独处理
_API_DIALECTS = {"oracle", "gbase8s", "sqlite"}

#: 这些方言的超时是 **SESSION 级**，用完必须复位，否则会「泄漏」到
#: 同一连接上的后续查询（实测 MySQL 会一直保持 1200ms）。
SESSION_SCOPED_DIALECTS = {
    "mysql": "SET SESSION max_execution_time = 0",
    "mariadb": "SET SESSION max_statement_time = 0",
}

#: **可以安全使用**「看门狗 + invalidate()」的方言白名单
#:
#: 只有实测证明「不会挂死、且能真正打断」的方言才列入。实测数据：
#:   - postgresql: 1.50s 打断 ✅
#:   - sqlite    : 3.88s 打断 ✅
#:   - mysql     : 等满 8s，**无效**     ❌
#:   - sqlserver : 结果不确定            ❌
#:   - gbase8s  : **进程挂死**（JDBC 不可重入）❌
WATCHDOG_SAFE_DIALECTS = frozenset({"postgresql", "sqlite"})

#: 不支持「查询级超时」的方言（调用方应给用户明确提示，而不是静默忽略）
NO_QUERY_TIMEOUT_DIALECTS = frozenset({"gbase8s", "sqlserver"})

#: 各库超时错误的特征串（用于把驱动异常识别为超时）
_TIMEOUT_MARKERS = (
    "statement timeout",          # PostgreSQL
    "query execution was interrupted",  # MySQL
    "querycanceled",              # psycopg
    "query timeout",              # ODBC / JDBC
    "ora-01013",                  # Oracle: user requested cancel
    "ora-12170",                  # Oracle: TNS 超时
    "timeout expired",            # pyodbc
    "canceling statement",        # psycopg 文案
    "interrupted",                # sqlite progress_handler
    "statement timeout expired",  # 通用
)


class TimeoutNotSupportedError(QueryError):
    """该方言不支持查询级超时

    刻意继承 :class:`~db_connector_tool.core.exceptions.QueryError`：
    它确实是一次「查询没能执行」，且能被上层的 ``except QueryError`` 统一处理；
    同时在文案里明确告诉用户**为什么**不支持、该怎么做。

    实测不支持的原因（不是"没实现"，而是"做不到"）：

    * **GBase 8s**：JDBC 驱动从其他线程 ``invalidate()`` 会让**整个进程挂死**
      （JVM 调用不可重入）；
    * **SQL Server**：pymssql 只有**连接级** ``timeout``，没有语句级机制，
      且 ``invalidate()`` 行为不确定。
    """


def normalize_timeout(timeout: Optional[float]) -> Optional[float]:
    """校验并规范化超时秒数

    Args:
        timeout: 超时秒数；``None`` 或 ``<=0`` 表示不设超时

    Returns:
        Optional[float]: 规范化后的秒数；不设超时时返回 ``None``

    Raises:
        ValueError: 传入非数值（布尔值也拒绝，避免 ``True`` 被当成 1 秒）

    Example:
        >>> normalize_timeout(None) is None
        True
        >>> normalize_timeout(0) is None
        True
        >>> normalize_timeout(2.5)
        2.5
    """
    if timeout is None:
        return None
    if isinstance(timeout, bool):
        raise ValueError(f"超时必须是数值，收到 {timeout!r}")
    try:
        value = float(timeout)
    except (TypeError, ValueError) as error:
        raise ValueError(f"超时必须是数值，收到 {timeout!r}") from error
    return value if value > 0 else None


def supports_query_timeout(dialect_name: str) -> bool:
    """该方言是否支持「查询级」超时

    Args:
        dialect_name: 方言名（小写）

    Returns:
        bool: 支持返回 ``True``

    Example:
        >>> supports_query_timeout("postgresql")
        True
        >>> supports_query_timeout("sqlserver")
        False
    """
    name = (dialect_name or "").lower()
    if name in NO_QUERY_TIMEOUT_DIALECTS:
        return False
    return name in TIMEOUT_STATEMENTS or name in {"oracle", "sqlite"}


def _dialect_name(connection: Any) -> str:
    """取出连接的方言名（小写）"""
    return str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()


def server_timeout_for(dialect_name: str, timeout: float) -> Optional[str]:
    """给定方言与超时，返回要执行的 SQL（无服务端机制时返回 ``None``）

    Args:
        dialect_name: 方言名（小写）
        timeout: 超时秒数

    Returns:
        Optional[str]: SQL 语句；该方言不支持时 ``None``

    Example:
        >>> server_timeout_for("postgresql", 1.5)
        'SET LOCAL statement_timeout = 1500'
        >>> server_timeout_for("sqlserver", 1.5) is None
        True
    """
    template = TIMEOUT_STATEMENTS.get(dialect_name)
    if not template:
        return None
    milliseconds = max(1, int(timeout * 1000))
    seconds = max(1, int(timeout))
    return template.format(ms=milliseconds, seconds=seconds)


def _apply_sql_timeout(connection: Any, name: str, timeout: float) -> bool:
    """通过 SQL 设置服务端超时（PostgreSQL / MySQL / MariaDB）"""
    sql = server_timeout_for(name, timeout)
    if not sql:
        return False
    try:
        connection.exec_driver_sql(sql)
        logger.debug("已为 %s 设置查询超时 %sms", name, int(timeout * 1000))
        return True
    except Exception as error:  # pylint: disable=broad-except
        logger.debug("%s 设置服务端超时失败: %s", name, error)
        return False


def _apply_oracle_timeout(connection: Any, timeout: float) -> bool:
    """Oracle：走 python-oracledb 的 ``call_timeout``（毫秒）"""
    try:
        dbapi = connection.connection.dbapi_connection
        dbapi.call_timeout = max(1, int(timeout * 1000))
        logger.debug("已为 oracle 设置 call_timeout")
        return True
    except Exception as error:  # pylint: disable=broad-except
        logger.debug("oracle 设置 call_timeout 失败: %s", error)
        return False


def _apply_sqlite_timeout(connection: Any, timeout: float) -> bool:
    """SQLite：安装 progress_handler，按指令数回调并在到点时中断"""
    try:
        handler = connection.connection.dbapi_connection.set_progress_handler
    except Exception as error:  # pylint: disable=broad-except
        logger.debug("sqlite 取不到底层连接: %s", error)
        return False
    try:
        handler(_Deadline(timeout).handler, 1000)
        logger.debug("已为 sqlite 安装 progress_handler")
        return True
    except Exception as error:  # pylint: disable=broad-except
        logger.debug("sqlite 安装 progress_handler 失败: %s", error)
        return False


def apply_server_timeout(connection: Any, timeout: float) -> Optional[str]:
    """在连接上设置服务端超时

    支持情况见模块文档的对照表。GBase 8s 需要在执行语句时设置
    （JDBC ``Statement.setQueryTimeout``），此处**不做处理** ——
    它最终会落到「不支持」的明确报错上。

    Args:
        connection: SQLAlchemy 连接
        timeout: 超时秒数

    Returns:
        Optional[str]: 生效的方言名；不支持或失败时 ``None``
    """
    if timeout is None:
        return None

    name = _dialect_name(connection)
    if name == "oracle":
        return name if _apply_oracle_timeout(connection, timeout) else None
    if name == "sqlite":
        return name if _apply_sqlite_timeout(connection, timeout) else None
    return name if _apply_sql_timeout(connection, name, timeout) else None


def restore_server_timeout(connection: Any, dialect_name: Optional[str]) -> None:
    """复位**会话级**超时，避免泄漏到同一连接的后续查询

    实测：MySQL 的 ``SET SESSION max_execution_time`` 会一直留在连接上，
    不复位则后续查询会被莫名其妙地打断。

    Args:
        connection: SQLAlchemy 连接
        dialect_name: :func:`apply_server_timeout` 的返回值
    """
    if not dialect_name:
        return
    reset_sql = SESSION_SCOPED_DIALECTS.get(dialect_name)
    if not reset_sql:
        return
    try:
        connection.exec_driver_sql(reset_sql)
        logger.debug("已复位 %s 的会话级超时", dialect_name)
    except Exception as error:  # pylint: disable=broad-except
        logger.debug("复位会话超时失败（连接可能已失效）: %s", error)


class _Deadline:
    """记录截止时刻，供 SQLite 的 progress_handler 轮询"""

    __slots__ = ("_deadline",)

    def __init__(self, timeout: float) -> None:
        self._deadline = time.monotonic() + timeout

    def handler(self) -> int:
        """SQLite 每 N 条虚拟机指令回调一次；返回非 0 表示中断"""
        return 1 if self.expired else 0

    @property
    def expired(self) -> bool:
        """是否已到截止时刻（便于测试与日志）"""
        return time.monotonic() > self._deadline

    @property
    def remaining(self) -> float:
        """剩余秒数（已超时则为负）"""
        return self._deadline - time.monotonic()


class WatchdogState:
    """看门狗的运行状态

    调用方据此判断「这次异常是不是超时引起的」—— 只看异常文案不可靠，
    因为各驱动文案不一，而且连接被作废后的报错往往看不出"超时"。

    Attributes:
        finished: 「查询已正常结束」事件
        fired:    「看门狗已动手中断连接」事件
    """

    __slots__ = ("finished", "fired")

    def __init__(self) -> None:
        self.finished = threading.Event()
        self.fired = threading.Event()

    def mark_finished(self) -> None:
        """标记「查询已正常结束」，让看门狗不必动手"""
        self.finished.set()

    def wait_finished(self, timeout: float) -> bool:
        """等待查询结束（返回是否在超时前结束）"""
        return self.finished.wait(timeout)

    @property
    def timed_out(self) -> bool:
        """看门狗是否已触发（即判定为超时）"""
        return self.fired.is_set()


@contextmanager
def watchdog(connection: Any, timeout: Optional[float]) -> Iterator[WatchdogState]:
    """客户端看门狗：到点就 ``invalidate()`` 连接，让阻塞中的调用返回

    **不在此处抛异常** —— 由调用方在捕获到驱动异常后，用
    :attr:`WatchdogState.timed_out` 或 :func:`is_timeout_error` 归类。
    这样既不会吞掉原始异常，也能正确区分「超时」与「真的出错」。

    Args:
        connection: SQLAlchemy 连接
        timeout: 超时秒数；``None`` 表示不启用

    Yields:
        WatchdogState: 含 ``finished`` / ``fired`` / ``timed_out``

    Example:
        >>> from sqlalchemy import create_engine, text
        >>> engine = create_engine("sqlite://")
        >>> with engine.connect() as conn:              # doctest: +SKIP
        ...     with watchdog(conn, 5) as state:
        ...         conn.execute(text("SELECT 1"))
        ...     print(state.timed_out)
        False
    """
    seconds = normalize_timeout(timeout)
    state = WatchdogState()

    if seconds is None:
        yield state
        return

    name = _dialect_name(connection)
    if name not in WATCHDOG_SAFE_DIALECTS:
        # 绝不在不安全的方言上启动看门狗 —— 实测 GBase 8s（JDBC）会让整个
        # 进程挂死，MySQL 则完全无效（等满查询时间才返回）。
        raise TimeoutNotSupportedError(
            f"该连接（{name or '未知方言'}）不支持查询级超时："
            f"既无服务端超时机制，客户端中断又对该驱动不安全"
            f"（实测会导致挂起或完全无效）。"
            f"如需限制查询时间，请在连接配置里设置（例如 SQL Server 的 "
            f"connect_args.timeout），或改用支持超时的数据库。"
        )

    def _watch() -> None:
        """计时到点后作废连接"""
        if state.wait_finished(seconds):
            return
        state.fired.set()
        logger.warning("查询超时（%.1fs），正在中断连接", seconds)
        try:
            connection.invalidate()
        except Exception as error:  # pylint: disable=broad-except
            logger.debug("中断连接失败: %s", error)

    thread = threading.Thread(target=_watch, daemon=True, name="dct-timeout-watchdog")
    thread.start()
    try:
        yield state
    finally:
        state.mark_finished()
        thread.join(timeout=1)


def is_timeout_error(error: BaseException) -> bool:
    """判断异常是否属于「查询超时」

    各驱动文案不一（且部分不带明确的类型），故按特征串匹配。

    Args:
        error: 捕获到的异常

    Returns:
        bool: 是超时类错误返回 ``True``

    Example:
        >>> is_timeout_error(RuntimeError("canceling statement due to statement timeout"))
        True
        >>> is_timeout_error(RuntimeError("表不存在"))
        False
    """
    if isinstance(error, DBTimeoutError):
        return True
    text_of_error = str(error).lower()
    return any(marker in text_of_error for marker in _TIMEOUT_MARKERS)


def to_timeout_error(
    error: BaseException,
    timeout: Optional[float],
    operation: str = "query",
) -> DBTimeoutError:
    """把驱动超时异常包装成 :class:`DBTimeoutError`

    Args:
        error: 原始异常
        timeout: 设置的超时秒数
        operation: 操作名

    Returns:
        DBTimeoutError: 带 ``timeout_seconds`` / ``operation`` 的异常
    """
    detail = str(error).splitlines()[0][:200]
    return DBTimeoutError(
        f"查询超时（{timeout:g} 秒）：{detail}" if timeout else f"查询超时：{detail}",
        "TIMEOUT_001",
        timeout_seconds=timeout,
        operation=operation,
    )
