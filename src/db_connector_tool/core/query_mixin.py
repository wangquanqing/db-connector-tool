# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""查询执行与结果获取（DatabaseManager 的 mixin）

把「取驱动 → 计时 → 更新连接池统计 → 包装异常」这套重复骨架，以及四个查询入口
（``execute_query`` / ``execute_query_with_metadata`` / ``execute_query_result`` /
``execute_query_stream``）从 ``connections`` 模块里拆出来。

**保真的关键**：新增的 :meth:`QueryMixin.execute_query_result` 返回按位置存放的
:class:`~db_connector_tool.core.result.QueryResult`，列名重复也不会互相覆盖 ——
这是 ``SELECT * FROM a JOIN b`` 场景的正确形态；旧的 ``execute_query`` 因返回
``RowMapping`` 列表而在重名列下会丢值，故标注为兼容保留。

约定宿主类提供：

- ``get_connection(name)``：取（或建）驱动实例
- ``pool_manager``：连接池管理器（计时与错误计数）

Example:
    >>> from db_connector_tool import DatabaseManager
    >>> db = DatabaseManager("my_app")
    >>> result = db.execute_query_result("mysql_db", "SELECT 1 AS a")
    >>> result.rows[0][0]
    1
"""

import time
from typing import Any, Dict, Iterator, List, Optional, Tuple

from ..utils.logging_utils import get_logger
from .exceptions import DatabaseError
from .result import QueryChunk, QueryResult

logger = get_logger(__name__)


class QueryMixin:
    """查询执行（由 DatabaseManager 混入）

    Attributes:
        pool_manager: 由宿主类提供，用于更新查询统计与错误计数
    """

    # 由宿主类赋值
    pool_manager: Any

    def get_connection(self, name: str) -> Any:
        """取（或建）驱动实例（由宿主类实现）"""
        raise NotImplementedError

    def _run_query(self, connection_name: str, operation) -> Any:
        """查询执行的公共骨架：计时 + 统计 + 异常包装

        Args:
            connection_name: 连接名称
            operation: 接收驱动实例并返回结果的可调用对象

        Returns:
            Any: ``operation`` 的返回值

        Raises:
            DatabaseError: 查询失败时（同时登记一次连接错误）
        """
        try:
            driver = self.get_connection(connection_name)
            start_time = time.time()
            result = operation(driver)
            response_time = time.time() - start_time
            self.pool_manager.update_query_metadata(connection_name, response_time)
            return result, response_time
        except (OSError, DatabaseError) as error:
            self.pool_manager.record_connection_error(connection_name, error)
            logger.error("查询执行失败 %s: %s", connection_name, error)
            raise DatabaseError(f"查询执行失败: {str(error)}") from error

    def execute_query(
        self,
        connection_name: str,
        query: str,
        params: Dict[str, Any] | None = None,
        timeout: Optional[float] = None,
    ) -> Tuple[List[Any], float]:
        """执行SQL查询语句

        .. note::
            行元素是 SQLAlchemy 的 ``RowMapping``（Mapping 接口，**不是** ``dict``
            子类）。**列名重复时按键消费会丢值甚至取到错值**（``SELECT * FROM a
            JOIN b`` 的常态），因此新代码请优先用 :meth:`execute_query_result`。

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选
            timeout: 超时秒数；``None``/``<=0`` 表示不限制

        Returns:
            Tuple[List[Any], float]: 查询结果列表（每行 ``RowMapping``）和执行时间

        Raises:
            DatabaseError: 当查询执行失败时
            ConfigError: 当连接配置不存在时
            DBTimeoutError: 超过 ``timeout`` 秒仍未返回

        Example:
            >>> results, elapsed = db_manager.execute_query(
            ...     "mysql_db", "SELECT * FROM users"
            ... )
            >>> for row in results:
            ...     print(row["name"])
        """
        return self._run_query(
            connection_name,
            lambda driver: driver.execute_query(query, params, timeout=timeout),
        )

    def execute_query_result(
        self,
        connection_name: str,
        query: str,
        params: Dict[str, Any] | None = None,
        timeout: Optional[float] = None,
    ) -> QueryResult:
        """执行查询并返回**保真**的结构化结果（推荐入口）

        相比 :meth:`execute_query` 返回的行对象列表，这里返回
        :class:`~db_connector_tool.core.result.QueryResult`：行是**按位置的元组**，
        列名重复时不会互相覆盖 —— 这对 ``SELECT * FROM a JOIN b`` 至关重要，
        字典化会把同名列合并、静默丢值。

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选
            timeout: 超时秒数；``None``/``<=0`` 表示不限制

        Returns:
            QueryResult: 列元数据 + 按位置的行数据 + 耗时

        Raises:
            DatabaseError: 当查询执行失败时
            ConfigError: 当连接配置不存在时
            DBTimeoutError: 超过 ``timeout`` 秒仍未返回

        Example:
            >>> result = db_manager.execute_query_result("mysql_db", "SELECT 1 AS a")
            >>> result.rows[0][0]
            1
            >>> result.columns[0].display_name
            'a'
        """
        result, response_time = self._run_query(
            connection_name,
            lambda driver: driver.execute_query_result(query, params, timeout=timeout),
        )
        result.elapsed = response_time
        return result

    def execute_query_with_metadata(
        self, connection_name: str, query: str, params: Dict[str, Any] | None = None
    ) -> Tuple[List[Any], List[Dict[str, Any]], float]:
        """执行SQL查询并同时返回列元数据

        相比 :meth:`execute_query` 多返回一份列类型信息，供结果网格渲染、
        列宽计算、编辑器选择等场景使用（模拟 DBeaver 类客户端所需）。

        .. note::
            行仍是 ``RowMapping``（兼容保留）；列元数据每项带 ``ordinal`` 与
            ``display_name``，可配合 ``list(row.values())`` 按位置安全取值。
            需要彻底保真请用 :meth:`execute_query_result`。

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选

        Returns:
            Tuple[List[Any], List[Dict[str, Any]], float]:
            ``(行列表, 列元数据, 执行时间)``；列元数据每项含 ``name`` /
            ``ordinal`` / ``display_name`` / ``type_name`` / ``python_type`` /
            ``precision`` / ``scale`` / ``nullable`` 等键

        Raises:
            DatabaseError: 当查询执行失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> rows, columns, cost = db_manager.execute_query_with_metadata(
            ...     "mysql_db", "SELECT id FROM users"
            ... )
            >>> columns[0]["ordinal"]        # 取值按序号，重名列也安全
            0
        """
        (rows, columns), response_time = self._run_query(
            connection_name,
            lambda driver: driver.execute_query_with_metadata(query, params),
        )
        return rows, columns, response_time

    def execute_query_stream(
        self,
        connection_name: str,
        query: str,
        params: Dict[str, Any] | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[QueryChunk]:
        """流式执行查询，按批返回结果（大表不爆内存）

        每次取 ``fetch_size`` 行交出一批，适合大表浏览与结果网格的分页加载
        （DBeaver 类客户端的「取前 N 行」）。

        .. warning::
            这是生成器：底层连接在生成器结束时才归还连接池。
            请**完整消费**（或显式 ``close()``）后再做其他数据库操作，
            不要长时间持有未消费完的生成器。

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选
            fetch_size: 每批行数

        Yields:
            QueryChunk: 每批数据；末批 ``is_last=True``

        Raises:
            DatabaseError: 当查询执行失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> for chunk in db_manager.execute_query_stream("db", "SELECT * FROM big"):
            ...     print(len(chunk.rows))
        """
        try:
            driver = self.get_connection(connection_name)
            yield from driver.execute_query_stream(query, params, fetch_size)
        except (OSError, DatabaseError) as error:
            self.pool_manager.record_connection_error(connection_name, error)
            logger.error("查询执行失败 %s: %s", connection_name, error)
            raise DatabaseError(f"查询执行失败: {str(error)}") from error

    def execute_command(
        self,
        connection_name: str,
        command: str,
        params: Dict[str, Any] | None = None,
        timeout: Optional[float] = None,
    ) -> Tuple[int, float]:
        """执行非查询SQL命令（INSERT/UPDATE/DELETE等）

        Args:
            connection_name: 连接名称
            command: SQL命令语句
            params: 命令参数字典，可选
            timeout: 超时秒数；``None``/``<=0`` 表示不限制

        Returns:
            Tuple[int, float]: 受影响的行数和执行时间

        Raises:
            DatabaseError: 当命令执行失败时
            ConfigError: 当连接配置不存在时
            DBTimeoutError: 超过 ``timeout`` 秒仍未返回

        Example:
            >>> affected, elapsed = db_manager.execute_command(
            ...     "mysql_db", "UPDATE users SET status = 'active' WHERE id = :id",
            ...     {"id": 1}
            ... )
            >>> print(f"更新了 {affected} 行")
        """
        affected, response_time = self._run_query(
            connection_name,
            lambda driver: driver.execute_command(command, params, timeout=timeout),
        )
        return affected, response_time
