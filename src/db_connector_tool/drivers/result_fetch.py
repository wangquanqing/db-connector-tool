# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""查询结果获取与列元数据（SQLAlchemyDriver 的 mixin）

把「执行查询 → 读列元数据 → 按位置取出保真的行」这一组职责从
``sqlalchemy_driver`` 里拆出来，原因有二：

1. 驱动模块本已接近 pylint 的 1000 行上限；
2. 这块逻辑与「连接/方言/URL 构建」是两件事，独立后更好测。

**保真的关键**：行数据一律经 :func:`~db_connector_tool.core.result.row_values`
按**位置**取出，绝不用 ``dict(row)`` / ``row.items()`` —— 后者在列名重复时会
互相覆盖（``SELECT * FROM a JOIN b`` 的常态），属静默数据损坏。

Example:
    >>> from db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver
    >>> driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
    >>> driver.connect()
    >>> result = driver.execute_query_result("SELECT 1 AS a, 2 AS a")
    >>> result.column_names()
    ['a', 'a (1)']
"""

from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from ..core.exceptions import QueryError
from ..core.result import (
    ColumnInfo,
    QueryChunk,
    QueryResult,
    assign_display_names,
    row_values,
)
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)


def column_metadata_from_description(cursor: Any) -> List[Dict[str, Any]]:
    """从 DBAPI 游标的 ``description`` 提取列元数据（通用回退）

    ``description`` 的 7 元组遵循 PEP 249：
    ``(name, type_code, display_size, internal_size, precision, scale, null_ok)``。
    各驱动的 ``type_code`` 形态不一（可能是类型对象，也可能是数字类型号），
    这里统一转成字符串，取不到类型名时置 ``None``。

    Args:
        cursor: DBAPI 游标

    Returns:
        List[Dict[str, Any]]: 每列一项；游标无 ``description`` 时返回空列表

    Example:
        >>> column_metadata_from_description(cursor)[0]["name"]
        'id'
    """
    columns: List[Dict[str, Any]] = []
    for description in getattr(cursor, "description", None) or ():
        type_code = description[1]
        # jaydebeapi 的 DBAPITypeObject 带 values 元组（JDBC 类型名）
        names = getattr(type_code, "values", None)
        if names:
            type_name: Optional[str] = str(names[0]).upper()
        elif type_code is None:
            type_name = None
        else:
            type_name = str(type_code)
        columns.append(
            {
                "name": str(description[0]),
                "type_name": type_name,
                "jdbc_type": None,
                "python_type": None,
                "precision": description[4],
                "scale": description[5],
                "nullable": None if description[6] is None else bool(description[6]),
                "java_class": None,
            }
        )
    return columns


def enrich_metadata(
    metadata: Sequence[Dict[str, Any]], rows: Sequence[Any]
) -> List[Dict[str, Any]]:
    """给列元数据补上 ``ordinal`` / ``display_name``（保留原有键）

    ``ordinal`` 是列的**身份**（0 基，稳定唯一），``display_name`` 是去重后的
    显示名。有了它们，调用方即使面对重名列也能按序号取值、或按唯一的显示名
    建字典 —— 这是「列身份与显示名分离」的落地。

    若方言取不到 description（元数据为空）而实际有行，则按行宽兜底生成列名，
    避免下游按索引取值时越界。

    Args:
        metadata: 原始列元数据（来自方言钩子或 DBAPI description）
        rows: 实际的行（用于在元数据缺失时兜底推断列数）

    Returns:
        List[Dict[str, Any]]: 补充后的列元数据
    """
    entries = [dict(meta) for meta in metadata]
    if not entries and rows:
        width = len(row_values(rows[0]))
        entries = [{"name": f"column_{i}"} for i in range(width)]
    if not entries:
        return []

    names = [str(meta.get("name", f"column_{i}")) for i, meta in enumerate(entries)]
    display_names = assign_display_names(names)
    for index, meta in enumerate(entries):
        meta["name"] = names[index]
        meta["ordinal"] = index
        meta["display_name"] = display_names[index]
    return entries


def columns_from_metadata(
    metadata: Sequence[Dict[str, Any]],
) -> Tuple[ColumnInfo, ...]:
    """由列元数据构造 :class:`ColumnInfo` 元组（含去重显示名）

    Args:
        metadata: 列元数据字典序列

    Returns:
        Tuple[ColumnInfo, ...]: 列信息元组
    """
    names = [str(meta.get("name", f"column_{i}")) for i, meta in enumerate(metadata)]
    display_names = assign_display_names(names)
    return tuple(
        ColumnInfo.from_mapping(index, display_names[index], metadata[index])
        for index in range(len(metadata))
    )


def run_sql(operation: Callable[[], Any]) -> Any:
    """执行一段 SQL 操作，并把各类异常统一翻译成 :class:`QueryError`

    各查询入口的异常映射完全一致，集中于此以免重复：

    - ``SQLAlchemyError`` → 「数据库错误」
    - ``ValueError``      → 「验证错误」
    - 其它                → 原样带出（保留驱动细节）

    Args:
        operation: 无参可调用对象，内部完成建连与执行

    Returns:
        Any: ``operation`` 的返回值

    Raises:
        QueryError: 执行失败时

    Example:
        >>> run_sql(lambda: "ok")
        'ok'
    """
    try:
        return operation()
    except SQLAlchemyError as error:
        raise QueryError(f"SQL执行失败: 数据库错误 - {str(error)}") from error
    except ValueError as error:
        raise QueryError(f"SQL执行失败: 验证错误 - {str(error)}") from error
    except Exception as error:  # pylint: disable=broad-except
        raise QueryError(f"SQL执行失败: {str(error)}") from error


class ResultFetchMixin:
    """查询执行与结果获取（由 SQLAlchemyDriver 混入）

    约定宿主类提供：

    - ``engine``：SQLAlchemy ``Engine``（可为 ``None``）
    - ``connect()``：惰性建连
    - ``_validate_sql_query(sql)``：SQL 校验闸门

    Attributes:
        engine: 由宿主类提供
    """

    # 由宿主类赋值；这里声明类型便于检查
    engine: Any

    # ------------------------------------------------------------ 工具
    def _read_column_metadata(self, cursor: Any) -> List[Dict[str, Any]]:
        """读取列元数据（**必须在取值之前调用**）

        元数据只在结果集打开期间有效，取完最后一行后游标会关闭结果集，
        ``cursor.description`` 随之失效。

        优先交给方言实现（GBase 8s 的 JDBC 元数据含类型名/精度/可空/Java 类名），
        方言未提供时退回 DBAPI 的 ``cursor.description``。

        Args:
            cursor: DBAPI 游标

        Returns:
            List[Dict[str, Any]]: 每列一项；取不到信息时返回空列表

        Example:
            >>> metadata = driver._read_column_metadata(result.cursor)
            >>> metadata[0]["name"]
            'id'
        """
        # 方言可选实现；用 getattr 探测，避免要求所有方言都提供该钩子
        hook = getattr(
            getattr(self.engine, "dialect", None), "get_column_metadata", None
        )
        if hook is not None:
            try:
                # pylint: disable=not-callable  # 运行期才确定的方言方法
                return hook(cursor)
            except Exception as error:  # pylint: disable=broad-except
                logger.debug("方言列元数据读取失败，回退 DBAPI description: %s", error)
        return column_metadata_from_description(cursor)

    @staticmethod
    def _row_values(row: Any) -> Tuple[Any, ...]:
        """按位置取出整行的值（重复列名也保真）

        转发到 :func:`db_connector_tool.core.result.row_values`，保证驱动层与
        CLI 用的是同一套取值语义。

        Args:
            row: 行对象

        Returns:
            Tuple[Any, ...]: 该行的值元组
        """
        return row_values(row)

    def _ensure_engine(self) -> None:
        """确保引擎可用（未建连时先建）"""
        if not self.engine:
            self.connect()
        assert self.engine is not None, "数据库引擎应该已经初始化，执行查询"

    @staticmethod
    def _query_context(query: str, parameters: Dict[str, Any] | None) -> Callable:
        """返回「在给定连接上执行语句」的闭包，供各入口复用

        Args:
            query: SQL 语句
            parameters: 参数（可为 ``None``）

        Returns:
            Callable: 接收连接、返回 ``Result`` 的闭包
        """

        def _execute(connection):
            if parameters:
                return connection.execute(text(query), parameters)
            return connection.execute(text(query))

        return _execute

    # ------------------------------------------------------------ 查询
    def execute_query_result(
        self, query: str, parameters: Dict[str, Any] | None = None
    ) -> QueryResult:
        """执行查询并返回**保真**的结构化结果（推荐入口）

        与 :meth:`execute_query` 的区别：返回 :class:`~db_connector_tool.core.result.QueryResult`，
        其中的行是**按位置的元组**，列名重复时也不会互相覆盖。

        这一点对 ``SELECT * FROM a JOIN b`` 这类查询是必须的：
        ``mappings()`` 的字典化会把同名列合并，实测

        .. code-block:: text

            SQL      : SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id
            真值     : (1, '张三', 10, 10, '研发部')
            dict 化后: {'id': 10, 'name': '研发部', 'dept_id': 10}   ← 张三 丢失

        而 :class:`QueryResult` 保留全部 5 列，并给重复列生成显示名
        ``id (1)`` / ``name (1)`` 供展示与序列化使用。

        Args:
            query: SQL查询语句
            parameters: 查询参数字典，用于参数化查询

        Returns:
            QueryResult: 列元数据 + 按位置的行数据

        Raises:
            QueryError: 当查询执行失败时

        Example:
            >>> result = driver.execute_query_result("SELECT 1 AS a, 2 AS a")
            >>> result.rows[0]              # 两个 a 各自保留
            (1, 2)
            >>> result.column_names()       # 显示名唯一，可直接做表头
            ['a', 'a (1)']
            >>> result.as_dicts()[0]
            {'a': 1, 'a (1)': 2}
        """
        self._ensure_engine()
        self._validate_sql_query(query)
        execute = self._query_context(query, parameters)

        def _operation() -> QueryResult:
            with self.engine.connect() as connection:
                sql_result = execute(connection)
                # 列元数据必须先读（取完最后一行后 cursor.description 失效）
                metadata = self._read_column_metadata(sql_result.cursor)
                # 用 Row（非 mappings）取行：按位置保真
                raw_rows = sql_result.all()
                return QueryResult.from_rows(raw_rows, metadata)

        return run_sql(_operation)

    def execute_query_stream(
        self,
        query: str,
        parameters: Dict[str, Any] | None = None,
        fetch_size: int = 1000,
    ) -> Iterator[QueryChunk]:
        """流式执行查询，按批返回结果（大表不爆内存）

        与 :meth:`execute_query_result` 的区别：不把全部行物化到内存，
        而是每次取 ``fetch_size`` 行交出一批。适合大表浏览、
        结果网格的分页加载（DBeaver 类客户端的「取前 N 行」）。

        .. warning::
            本方法使用生成器：**必须在生成器被完整消费（或显式 ``close()``）
            之前保持迭代**，底层连接在生成器结束时才归还连接池。
            换言之，不要跨线程/跨请求持有未消费完的生成器。

        Args:
            query: SQL查询语句
            parameters: 查询参数字典
            fetch_size: 每批行数（须为正整数）

        Yields:
            QueryChunk: 每批数据；末批 ``is_last=True``

        Raises:
            QueryError: 当查询执行失败时
            ValueError: ``fetch_size`` 非正数时

        Example:
            >>> for chunk in driver.execute_query_stream("SELECT * FROM big_table"):
            ...     for row in chunk.rows:
            ...         print(row)          # 按位置取值，重名列安全
        """
        if fetch_size <= 0:
            raise ValueError(f"fetch_size 必须为正整数，收到 {fetch_size}")
        self._ensure_engine()
        self._validate_sql_query(query)
        execute = self._query_context(query, parameters)

        def _batches() -> Iterator[QueryChunk]:
            with self.engine.connect() as connection:
                sql_result = execute(connection)
                metadata = self._read_column_metadata(sql_result.cursor)
                columns = columns_from_metadata(metadata)
                stream = sql_result.mappings()

                batch = stream.fetchmany(fetch_size)
                if not batch:
                    # 空结果也交出一批空的（携带列元数据），便于渲染表头
                    yield QueryChunk(columns=columns, rows=[], is_last=True)
                    return

                index = 0
                while batch:
                    rows = [row_values(row) for row in batch]
                    # 预读下一批：这样当前批是否为末批是确定的
                    next_batch = stream.fetchmany(fetch_size)
                    yield QueryChunk(
                        columns=columns,
                        rows=rows,
                        is_last=not next_batch,
                        chunk_index=index,
                    )
                    batch = next_batch
                    index += 1

        # 生成器内部的异常也要翻译成 QueryError
        yield from run_sql(_batches)

    def execute_query_with_metadata(
        self, query: str, parameters: Dict[str, Any] | None = None
    ) -> Tuple[List[Any], List[Dict[str, Any]]]:
        """执行查询并同时返回列元数据

        与 :meth:`execute_query` 的区别在于额外返回列类型信息，供结果网格渲染、
        列宽计算、编辑器选择等场景使用（模拟 DBeaver 类客户端所需）。

        .. note::
            与 :meth:`execute_query` 同样是**兼容保留**的接口：返回的行仍是
            ``RowMapping``，按键消费在重名列下会丢值。列元数据的每项里带有
            ``ordinal`` 与 ``display_name``，可配合 ``list(row.values())``
            按位置安全取值；也可以直接用 :meth:`execute_query_result`。

        Args:
            query: SQL查询语句
            parameters: 查询参数字典，用于参数化查询

        Returns:
            Tuple[List[Any], List[Dict[str, Any]]]:
            ``(行列表, 列元数据)``；列元数据每项含 ``name`` / ``ordinal`` /
            ``display_name`` / ``type_name`` / ``python_type`` / ``precision`` /
            ``scale`` / ``nullable`` 等键

        Raises:
            QueryError: 当SQL执行失败时

        Example:
            >>> rows, columns = driver.execute_query_with_metadata("SELECT id FROM t")
            >>> columns[0]["name"], columns[0]["python_type"]
            ('id', 'int')
            >>> columns[0]["ordinal"]          # 取值按序号，重名列也安全
            0
        """
        self._ensure_engine()
        self._validate_sql_query(query)
        execute = self._query_context(query, parameters)

        def _operation() -> Tuple[List[Any], List[Dict[str, Any]]]:
            with self.engine.connect() as connection:
                sql_result = execute(connection)
                # 元数据必须先于取值读取（取完即失效）
                columns = self._read_column_metadata(sql_result.cursor)
                rows = sql_result.mappings().all()
                return rows, enrich_metadata(columns, rows)

        return run_sql(_operation)
