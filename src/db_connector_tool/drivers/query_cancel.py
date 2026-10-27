# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""查询取消（Ctrl+C 即时中断）

让用户在查询跑着的时候按 Ctrl+C 就能**立刻**停下，而不是干等它跑完。

**为什么这件事比看起来难**

Python 的信号处理器只在「字节码边界」执行。驱动在 C 扩展里阻塞时：

1. 若该扩展**释放 GIL**（psycopg 会），信号能及时送达；
2. 若**不释放 GIL**（pymysql / pymssql / sqlite3 大多如此），
   信号会被推迟到查询返回之后 —— 此时再取消已经毫无意义。

实测（主线程直接执行查询，1.5s 时发 SIGINT）：

============  ======================
方言          handler 执行时刻
============  ======================
PostgreSQL    1.51s  ✅ 及时
SQLite        **8.30s** ❌ 被推迟到查询结束
============  ======================

因此本模块把查询放到**工作线程**，主线程只做可被打断的等待：

============  ======================  ======================
方言          handler（主线程模式）    handler（工作线程模式）
============  ======================  ======================
PostgreSQL    1.51s                   1.90s ✅
SQLite        **8.30s** ❌            1.90s ✅
============  ======================  ======================

**各方言的取消机制**（均实测）

============  ==========================  ==================  ========
方言          机制                        耗时                连接复用
============  ==========================  ==================  ========
PostgreSQL    ``dbapi.cancel()``          2.90s               ✅
SQLite        ``dbapi.interrupt()``       2.62s               ✅
MySQL/MariaDB 另开连接 ``KILL <id>``      1.70s               ✅
GBase 8s      —（JDBC 不可重入）          **挂死**            ❌
SQL Server    —（KILL 被拒/close 挂死）   —                   ❌
============  ==========================  ==================  ========

.. warning::
    **GBase 8s 绝不可尝试取消**：``jconn.abort()`` 会让整个进程挂死。
    **SQL Server 同理**：``KILL <spid>`` 被服务端拒绝（错误 6115），
    ``close()`` 会挂死。两者都走「明确不支持」的路径。

Example:
    >>> from db_connector_tool import DatabaseManager
    >>> db = DatabaseManager("my_app")
    >>> # Ctrl+C 会即时中断（支持取消的方言）
    >>> result = db.execute_query_result_cancellable("db_kes", "SELECT ...")  # doctest: +SKIP
"""

import threading
import time
from typing import Any, Callable, Dict, Optional, Tuple

from ..core.exceptions import QueryCancelledError
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

__all__ = [
    "CANCELLABLE_DIALECTS",
    "cancel_query",
    "supports_cancel",
    "run_cancellable",
]


#: 支持「查询取消」的方言 → 取消机制说明
#:
#: 只有实测证明「能真正打断、且连接还能继续用」的方言才列入。
CANCELLABLE_DIALECTS: Dict[str, str] = {
    "postgresql": "dbapi.cancel()",
    "sqlite": "dbapi.interrupt()",
    "mysql": "KILL <connection_id>（另开连接）",
    "mariadb": "KILL <connection_id>（另开连接）",
}

#: 明确**不可**取消的方言及原因（写清原因，便于用户理解而非猜测）
UNCANCELLABLE_DIALECTS: Dict[str, str] = {
    "gbase8s": "JDBC 调用不可重入，中断会让进程挂死",
    "sqlserver": "KILL 被服务端拒绝(6115)，close 会挂死",
}


def supports_cancel(dialect_name: str) -> bool:
    """该方言是否支持查询取消

    Args:
        dialect_name: 方言名（小写）

    Returns:
        bool: 支持返回 ``True``

    Example:
        >>> supports_cancel("postgresql")
        True
        >>> supports_cancel("gbase8s")
        False
    """
    return (dialect_name or "").lower() in CANCELLABLE_DIALECTS


def cancel_query(
    connection: Any,
    engine: Any = None,
    keepalive: Tuple[Any, ...] = (),
) -> Optional[str]:
    """取消连接上正在执行的查询

    按方言调用对应机制。**调用前请确认** :func:`supports_cancel` 为真 ——
    本函数对不支持的方言会直接返回 ``None``（不尝试，避免挂死）。

    Args:
        connection: 正在执行查询的 SQLAlchemy 连接
        engine: 引擎（MySQL 的 KILL 需要另开连接）
        keepalive: 额外需要保活的参数（如 MySQL 的连接 id）

    Returns:
        Optional[str]: 生效的机制名；不支持或失败时 ``None``

    Example:
        >>> method = cancel_query(conn, engine)   # doctest: +SKIP
        >>> print(method)
        'psycopg.cancel'
    """
    dialect = str(
        getattr(getattr(connection, "dialect", None), "name", "") or ""
    ).lower()

    if dialect in UNCANCELLABLE_DIALECTS:
        logger.warning(
            "%s 不支持查询取消：%s", dialect, UNCANCELLABLE_DIALECTS[dialect]
        )
        return None

    if not supports_cancel(dialect):
        logger.debug("%s 无取消机制", dialect)
        return None

    try:
        if dialect == "postgresql":
            connection.connection.dbapi_connection.cancel()
        elif dialect == "sqlite":
            connection.connection.dbapi_connection.interrupt()
        else:
            # 只剩 MySQL / MariaDB：需要另开连接 KILL
            return _kill_mysql(connection, engine, keepalive)
    except Exception as error:  # pylint: disable=broad-except
        logger.warning("取消 %s 查询失败: %s", dialect, error)
        return None

    logger.info("已取消 %s 查询", dialect)
    return CANCELLABLE_DIALECTS[dialect]


def _kill_mysql(
    connection: Any, engine: Any, keepalive: Tuple[Any, ...]
) -> Optional[str]:
    """MySQL：另开连接执行 ``KILL <connection_id>``

    MySQL 没有连接级 cancel，标准做法是查目标会话的 id 再 KILL。
    连接 id 优先从 ``keepalive`` 取（由调用方在执行前记录），
    取不到时现查（但目标连接正忙，可能查不出来）。
    """
    if engine is None:
        logger.debug("MySQL 取消需要 engine（用于另开连接）")
        return None

    connection_id = keepalive[0] if keepalive else None
    if connection_id is None:
        # pymysql 的 thread_id() 是**普通方法**，调用时不与服务器通信，
        # 因此即便连接正忙也能安全取到 id。
        try:
            connection_id = connection.connection.dbapi_connection.thread_id()
        except Exception as error:  # pylint: disable=broad-except
            logger.debug("取 MySQL 连接 id 失败: %s", error)
            return None

    try:
        # 必须用独立连接执行 KILL，不能用正在忙的目标连接
        with engine.connect() as killer:
            killer.exec_driver_sql(f"KILL {int(connection_id)}")
        logger.info("已 KILL MySQL 连接 %s", connection_id)
        return CANCELLABLE_DIALECTS["mysql"]
    except Exception as error:  # pylint: disable=broad-except
        logger.warning("KILL MySQL 连接 %s 失败: %s", connection_id, error)
        return None


def _wait_until_done(
    stop: threading.Event, thread: threading.Thread, timeout: Optional[float]
) -> None:
    """在主线程里等待工作线程结束（保持可被信号打断）

    用 **短间隔轮询** 而不是单次长等待：这样每次 ``wait()`` 都返回解释器，
    Python 才有机会处理信号。实测这一改动让 SQLite 的 Ctrl+C 从
    「等满 8 秒」变成「1.9 秒响应」。

    Args:
        stop: 工作线程完成时置位的事件
        thread: 工作线程
        timeout: 最长等待秒数；``None`` 表示不限

    Raises:
        KeyboardInterrupt: 用户按了 Ctrl+C（由调用方处理）
    """
    deadline = None if timeout is None else time.monotonic() + timeout
    while not stop.wait(0.1):
        if deadline is not None and time.monotonic() > deadline:
            break
        if not thread.is_alive():
            break


def run_cancellable(
    operation: Callable[[], Any],
    on_cancel: Callable[[], Optional[str]],
    timeout: Optional[float] = None,
) -> Tuple[Any, bool]:
    """在工作线程执行操作，主线程保持可被打断

    主线程只做短间隔轮询（``join(0.1)``），因此**信号能及时送达**。
    收到 :class:`KeyboardInterrupt` 后调用 ``on_cancel`` 取消底层查询，
    再等工作线程收尾。

    Args:
        operation: 无参可调用对象，在工作线程里执行
        on_cancel: 取消回调，返回生效的机制名（用于判断是否真的取消了）
        timeout: 最长等待秒数；``None`` 表示不限

    Returns:
        Tuple[Any, bool]: ``(返回值, 是否被取消)``

    Raises:
        KeyboardInterrupt: 取消失败时向上抛（让用户看到 Ctrl+C 生效）
        Exception: ``operation`` 抛出的异常原样透传

    Example:
        >>> result, cancelled = run_cancellable(          # doctest: +SKIP
        ...     lambda: run_query(),
        ...     lambda: cancel_query(conn, engine),
        ... )
    """
    box: Dict[str, Any] = {"done": False, "value": None, "error": None}
    stop = threading.Event()

    def _worker() -> None:
        """工作线程：执行操作，把结果放进 box"""
        try:
            box["value"] = operation()
        except BaseException as error:  # pylint: disable=broad-except
            box["error"] = error
        finally:
            box["done"] = True
            stop.set()

    thread = threading.Thread(target=_worker, daemon=True, name="dct-query-worker")
    thread.start()

    cancelled = False
    try:
        _wait_until_done(stop, thread, timeout)
    except KeyboardInterrupt:
        cancelled = True
        logger.warning("用户中断查询，正在尝试取消")
        method = None
        try:
            method = on_cancel()
        except Exception as error:  # pylint: disable=broad-except
            logger.debug("取消回调本身失败: %s", error)

        # 等 worker 收尾（取消成功时它很快会返回）
        thread.join(timeout=10)

        if thread.is_alive():
            # 取消没生效：不能假装成功，必须让用户知道
            logger.error("取消未生效，查询仍在执行（后台线程已放弃等待）")
            raise QueryCancelledError(
                "已收到中断，但该数据库不支持取消正在执行的查询；"
                "查询会在后台继续跑完（结果将被丢弃）。"
            ) from None

        if method is None:
            raise QueryCancelledError(
                "已收到中断，但取消操作未生效（该方言可能不支持取消）。"
            ) from None

    if cancelled:
        # 取消成功：worker 抛的异常是「被取消」的正常表现，不再上抛
        logger.info("查询已取消")
        return (None, True)

    error = box["error"]
    if isinstance(error, BaseException):
        raise error
    return (box["value"], False)
