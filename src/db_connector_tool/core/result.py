# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""查询结果的结构化载体（列元数据 + 按位置保真的行数据）

**为什么需要这个模块**

原先查询结果直接以 ``sql_result.mappings().all()`` 的形式在内部流转，
调用方普遍用 ``dict(row)`` / ``row.items()`` / ``row.get(名字)`` 消费。
这在**列名重复**时会静默产出错误数据 —— 而列名重复恰恰是
``SELECT * FROM a JOIN b`` 的常态：

.. code-block:: text

    SQL      : SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id
    真实数据 : emp(id=1, name='张三', dept_id=10) + dept(id=10, name='研发部')
    keys()   : ['id', 'name', 'dept_id', 'id', 'name']      5 列
    values() : [1, '张三', 10, 10, '研发部']                 5 个真值 ✅
    dict(row): {'id': 10, 'name': '研发部', 'dept_id': 10}   塌成 3 个键 ❌

``RowMapping`` 内部其实**按位置保真**（``values()`` 是对的），
丢失发生在 ``dict()`` 转换这一步：``_key_to_index`` 每个列名只记一个索引
（最后出现者），于是同名列全部取到同一个值，连 ``items()`` 给出的**值都是错的**。

**本模块的对策**：把「列的身份」与「显示名」分开。

- ``ColumnInfo.ordinal``  —— 列身份，稳定唯一（0 基），取值一律按它；
- ``ColumnInfo.name``     —— 原始列名，**允许重复**；
- ``ColumnInfo.display_name`` —— 去重后的显示名（``id`` / ``id (1)``），仅供展示与
  序列化用作出路键，不参与取值。

``QueryResult.rows`` 因此是**元组列表**（按位置），而非字典列表。
需要字典形态时用 :meth:`QueryResult.as_dicts`，它按 ``display_name`` 建键，
天然无冲突。

Example:
    >>> from db_connector_tool.core.result import ColumnInfo, QueryResult
    >>> columns = (ColumnInfo(0, "id", "id"), ColumnInfo(1, "id", "id (1)"))
    >>> result = QueryResult(columns=columns, rows=[(1, 2)], elapsed=0.01)
    >>> result.rows[0][1]          # 按位置取值，永不塌陷
    2
    >>> result.as_dicts()[0]
    {'id': 1, 'id (1)': 2}
"""

from dataclasses import dataclass, replace
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

__all__ = [
    "ColumnInfo",
    "QueryResult",
    "QueryChunk",
    "assign_display_names",
    "row_values",
]


def row_values(row: Any) -> Tuple[Any, ...]:
    """按**位置**取出整行的值（重复列名也保真）

    这是全项目取单元格值的**唯一**正确姿势，取代 ``dict(row)`` /
    ``row.items()`` / ``row.get(名字)`` —— 后者在重名列下会互相覆盖。

    各形态的来源（实测）：

    ==================  ==================================================
    行对象              位置来源
    ==================  ==================================================
    ``tuple`` / ``list`` 本身
    ``dict``            ``values()``（按插入顺序）
    ``RowMapping``      ``values()`` —— 正确；``list(row)`` 得到的是**键**
    ``Row``             无 ``values()``，走 ``_mapping.values()``
    ==================  ==================================================

    Args:
        row: 行对象

    Returns:
        Tuple[Any, ...]: 该行的值元组；完全取不到时返回空元组

    Example:
        >>> import sqlalchemy
        >>> from sqlalchemy import create_engine, text
        >>> engine = create_engine("sqlite://")
        >>> with engine.connect() as conn:
        ...     row = conn.execute(text("SELECT 1 AS a, 2 AS a")).mappings().first()
        >>> row_values(row)          # 两个 a 各自保留
        (1, 2)
    """
    if isinstance(row, (tuple, list)):
        return tuple(row)
    if isinstance(row, dict):
        return tuple(row.values())

    getter = getattr(row, "values", None)
    if callable(getter):
        try:
            return tuple(getter())
        except (TypeError, ValueError):
            pass

    # Row 没有 values()，但 _mapping.values() 同样按位置有序
    # （RowMapping 访问 _mapping 会抛 AttributeError，由默认值兜住）
    inner = getattr(row, "_mapping", None)
    inner_getter = getattr(inner, "values", None)
    if callable(inner_getter):
        try:
            return tuple(inner_getter())
        except (TypeError, ValueError):
            pass
    return ()


def assign_display_names(names: Sequence[str]) -> List[str]:
    """为可能重复的列名生成唯一的显示名

    规则与常见数据库客户端一致：**首次出现保留原名**，
    其后每次重复追加 `` (n)``，``n`` 从 1 起递增：

    >>> assign_display_names(["id", "name", "id", "name"])
    ['id', 'name', 'id (1)', 'name (1)']

    若追加后缀后仍与既有名字冲突（例如查询里本就有一列叫 ``id (1)``），
    则继续递增 ``n`` 直到唯一，保证结果内**绝不出现重名**。

    Args:
        names: 原始列名序列，允许重复

    Returns:
        List[str]: 与输入等长、且两两互不相同的显示名列表

    Example:
        >>> assign_display_names(["a", "a", "a"])
        ['a', 'a (1)', 'a (2)']
        >>> assign_display_names(["a", "a (1)", "a"])
        ['a', 'a (1)', 'a (2)']
    """
    used: set = set()
    # 先登记“只会出现一次”的名字，避免后缀撞上后面才出现的真实列名
    counts: Dict[str, int] = {}
    for name in names:
        counts[name] = counts.get(name, 0) + 1

    display: List[str] = []
    taken: Dict[str, int] = {}
    for name in names:
        if counts[name] == 1 and name not in taken:
            display.append(name)
            taken[name] = 0
            used.add(name)
            continue
        # 重名（或与已用名冲突）：从 1 开始找一个没被占用的后缀
        index = taken.get(name, 0)
        candidate = f"{name} ({index})" if index else name
        while candidate in used:
            index += 1
            candidate = f"{name} ({index})"
        taken[name] = index + 1 if index else 1
        used.add(candidate)
        display.append(candidate)
    return display


@dataclass(frozen=True)
# 数据类：字段多是有意的（列身份、显示名、类型、精度、可空、表名、时区、BLOB），
# 拆小反而会让调用方到处拼接，故整体豁免该检查。
# pylint: disable=too-many-instance-attributes
class ColumnInfo:
    """单列的元数据

    Attributes:
        ordinal: 列序号（0 基）。**列身份**，取值一律按它，重名也不受影响。
        name: 原始列名，允许重复。
        display_name: 去重后的显示名（如 ``id (1)``），供表格渲染与序列化建键。
        type_name: 原生类型名（如 ``numeric(10,2)`` / ``DECIMAL``），取不到为 ``None``。
        python_type: 归一化后的 Python 类型名（如 ``int`` / ``Decimal``）。
        precision: 数值精度；取不到为 ``None``。
        scale: 小数位数；取不到为 ``None``。
        nullable: 是否可空；未知为 ``None``。
        java_class: JDBC 的 Java 类名（仅 GBase 8s 等 JDBC 方言提供）。
        table: 归属表名；当前恒为 ``None``，为多表 JOIN 场景预留。
        timezone_aware: 时间类型是否带时区；为跨库一致性预留。
        is_binary: 是否为二进制/大对象列（客户端可显示 ``<BLOB>``）。
    """

    ordinal: int
    name: str
    display_name: str
    type_name: Optional[str] = None
    python_type: Optional[str] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    nullable: Optional[bool] = None
    java_class: Optional[str] = None
    table: Optional[str] = None
    timezone_aware: Optional[bool] = None
    is_binary: bool = False

    @classmethod
    def from_mapping(
        cls, ordinal: int, display_name: str, meta: Dict[str, Any]
    ) -> "ColumnInfo":
        """由列元数据字典构造（兼容既有 ``column_metadata()`` 的输出）

        Args:
            ordinal: 列序号（0 基）
            display_name: 去重后的显示名
            meta: 含 ``name`` / ``type_name`` / ``python_type`` / ``precision`` /
                ``scale`` / ``nullable`` / ``java_class`` 等键的字典

        Returns:
            ColumnInfo: 构造出的列信息

        Example:
            >>> ColumnInfo.from_mapping(0, "id", {"name": "id", "python_type": "int"})
            ColumnInfo(ordinal=0, name='id', display_name='id', ...)
        """
        return cls(
            ordinal=ordinal,
            name=str(meta.get("name", f"column_{ordinal}")),
            display_name=display_name,
            type_name=meta.get("type_name"),
            python_type=meta.get("python_type"),
            precision=meta.get("precision"),
            scale=meta.get("scale"),
            nullable=meta.get("nullable"),
            java_class=meta.get("java_class"),
            table=meta.get("table"),
        )

    def to_mapping(self) -> Dict[str, Any]:
        """转回字典形态（保留既有键名，并新增 ordinal / display_name）

        Returns:
            Dict[str, Any]: 含全部字段的字典

        Example:
            >>> ColumnInfo(0, "id", "id").to_mapping()["ordinal"]
            0
        """
        return {
            "name": self.name,
            "ordinal": self.ordinal,
            "display_name": self.display_name,
            "type_name": self.type_name,
            "jdbc_type": None,
            "python_type": self.python_type,
            "precision": self.precision,
            "scale": self.scale,
            "nullable": self.nullable,
            "java_class": self.java_class,
            "table": self.table,
            "timezone_aware": self.timezone_aware,
            "is_binary": self.is_binary,
        }


class QueryResult:
    """一次查询的完整结果：列元数据 + 按位置保真的行数据

    ``rows`` 是**元组列表**而非字典列表 —— 这是刻意的设计：字典按“名字”建键，
    一旦列名重复就会互相覆盖（见模块文档）。按位置存放则永不塌陷。

    Attributes:
        columns: 列元数据，顺序即结果集的列顺序
        rows: 行数据，每行是与 ``columns`` 等长的元组
        elapsed: 执行耗时（秒）
        rowcount: 受影响/返回的行数（与 ``len(rows)`` 一致）

    Example:
        >>> from db_connector_tool.core.result import ColumnInfo, QueryResult
        >>> result = QueryResult(
        ...     columns=(ColumnInfo(0, "a", "a"), ColumnInfo(1, "a", "a (1)")),
        ...     rows=[(1, 2)],
        ... )
        >>> len(result)
        1
        >>> result.column_names()
        ['a', 'a (1)']
        >>> result.as_dicts()
        [{'a': 1, 'a (1)': 2}]
    """

    __slots__ = ("columns", "rows", "elapsed")

    def __init__(
        self,
        columns: Sequence[ColumnInfo],
        rows: Sequence[Tuple[Any, ...]],
        elapsed: float = 0.0,
    ) -> None:
        """初始化查询结果

        Args:
            columns: 列元数据序列
            rows: 行数据（每行与 columns 等长的元组）
            elapsed: 执行耗时（秒）
        """
        self.columns: Tuple[ColumnInfo, ...] = tuple(columns)
        self.rows: List[Tuple[Any, ...]] = [tuple(row) for row in rows]
        self.elapsed = elapsed

    def __len__(self) -> int:
        """返回行数"""
        return len(self.rows)

    def __iter__(self) -> Iterator[Tuple[Any, ...]]:
        """按位置迭代行（每行一个元组）"""
        return iter(self.rows)

    def __repr__(self) -> str:
        """返回简短表示"""
        return (
            f"QueryResult(columns={len(self.columns)}, "
            f"rows={len(self.rows)}, elapsed={self.elapsed:.3f}s)"
        )

    def __eq__(self, other: object) -> bool:
        """按列与行内容比较（便于测试）"""
        if not isinstance(other, QueryResult):
            return NotImplemented
        return (
            self.columns == other.columns
            and self.rows == other.rows
            and self.elapsed == other.elapsed
        )

    # ---------------------------------------------------------------- 查询
    @property
    def rowcount(self) -> int:
        """受影响/返回的行数"""
        return len(self.rows)

    def column_names(self) -> List[str]:
        """返回**显示名**列表（去重，可直接做表头）

        Returns:
            List[str]: 显示名列表

        Example:
            >>> QueryResult((ColumnInfo(0, "a", "a"),), [(1,)]).column_names()
            ['a']
        """
        return [column.display_name for column in self.columns]

    def raw_column_names(self) -> List[str]:
        """返回**原始列名**列表（可能重复）

        Returns:
            List[str]: 原始列名列表
        """
        return [column.name for column in self.columns]

    def find_column(self, name: str) -> Optional[ColumnInfo]:
        """按显示名或原始名查找列（重名时返回第一个）

        Args:
            name: 显示名或原始列名

        Returns:
            Optional[ColumnInfo]: 找到的列；未找到返回 ``None``

        Example:
            >>> QueryResult((ColumnInfo(0, "a", "a"),), [(1,)]).find_column("a").ordinal
            0
        """
        for column in self.columns:
            if name in (column.display_name, column.name):
                return column
        return None

    # ---------------------------------------------------------------- 取值
    def value_at(self, row_index: int, column_index: int) -> Any:
        """按位置取值（重名列也安全）

        Args:
            row_index: 行下标（0 基）
            column_index: 列下标（0 基）

        Returns:
            Any: 该单元格的值

        Raises:
            IndexError: 行或列下标越界

        Example:
            >>> QueryResult((ColumnInfo(0, "a", "a"),), [(7,)]).value_at(0, 0)
            7
        """
        return self.rows[row_index][column_index]

    def as_dicts(self) -> List[Dict[str, Any]]:
        """转为字典列表，键为**去重后的显示名**

        因为显示名两两不同，转换过程**不丢任何列**。

        Returns:
            List[Dict[str, Any]]: 每行一个字典

        Example:
            >>> result = QueryResult(
            ...     (ColumnInfo(0, "a", "a"), ColumnInfo(1, "a", "a (1)")), [(1, 2)]
            ... )
            >>> result.as_dicts()
            [{'a': 1, 'a (1)': 2}]
        """
        keys = self.column_names()
        return [dict(zip(keys, row)) for row in self.rows]

    def iter_rows(self) -> Iterator[Tuple[Any, ...]]:
        """按位置迭代行（每行一个元组）

        Returns:
            Iterator[Tuple[Any, ...]]: 行迭代器
        """
        return iter(self.rows)

    def with_columns_metadata(
        self, metadata: Sequence[Dict[str, Any]]
    ) -> "QueryResult":
        """用新的列元数据补充/替换列的属性（行数据不变）

        用于把 DBAPI ``cursor.description`` 的类型信息补进已有结果。

        Args:
            metadata: 与 ``columns`` 等长的列元数据字典序列

        Returns:
            QueryResult: 新的结果对象（不改动原对象）

        Example:
            >>> r = QueryResult((ColumnInfo(0, "a", "a"),), [(1,)])
            >>> r.with_columns_metadata([{"name": "a", "python_type": "int"}]) \\
            ...     .columns[0].python_type
            'int'
        """
        updated: List[ColumnInfo] = []
        for index, column in enumerate(self.columns):
            if index < len(metadata):
                meta = metadata[index]
                updated.append(
                    replace(
                        column,
                        name=str(meta.get("name", column.name)),
                        type_name=meta.get("type_name", column.type_name),
                        python_type=meta.get("python_type", column.python_type),
                        precision=meta.get("precision", column.precision),
                        scale=meta.get("scale", column.scale),
                        nullable=meta.get("nullable", column.nullable),
                        java_class=meta.get("java_class", column.java_class),
                    )
                )
            else:
                updated.append(column)
        return QueryResult(updated, self.rows, self.elapsed)

    @classmethod
    def from_rows(
        cls,
        rows: Sequence[Any],
        metadata: Sequence[Dict[str, Any]] | None = None,
        elapsed: float = 0.0,
    ) -> "QueryResult":
        """由行对象（``Row`` / ``RowMapping`` / 元组 / 字典）构造结果

        这是把驱动层的原始结果**收口到保真形态**的入口：优先按位置读取，
        因此对重名列天然安全。

        Args:
            rows: 行序列；元素可为 ``Row``（按位置）、``RowMapping``（混合）、
                元组/列表，或字典（按插入顺序取值）
            metadata: 列元数据字典序列；缺省时从行对象的 ``keys()`` 推导列名
            elapsed: 执行耗时（秒）

        Returns:
            QueryResult: 构造出的结果

        Example:
            >>> QueryResult.from_rows([(1, "x")], [{"name": "id"}, {"name": "n"}]) \\
            ...     .as_dicts()
            [{'id': 1, 'n': 'x'}]
        """
        materialized = list(rows)
        names = _derive_names(materialized, metadata)

        if metadata:
            metas = list(metadata)
            # 元数据数量可能与实际列数不符（某些方言取不到），以两者较小值为准
            if len(metas) < len(names):
                metas.extend(
                    {"name": name} for name in names[len(metas):]
                )
            elif len(metas) > len(names):
                names = [
                    str(meta.get("name", f"column_{i}"))
                    for i, meta in enumerate(metas)
                ]
        else:
            metas = [{"name": name} for name in names]

        if not names:
            return cls((), [], elapsed)

        display_names = assign_display_names(names)
        columns = [
            ColumnInfo.from_mapping(index, display_names[index], metas[index])
            for index in range(len(display_names))
        ]
        position_rows = [_row_to_tuple(row, len(columns)) for row in materialized]
        return cls(columns, position_rows, elapsed)


@dataclass
class QueryChunk:
    """流式取数的一批数据

    Attributes:
        columns: 列元数据（首批即确定，后续批次复用）
        rows: 本批的行（元组列表）
        is_last: 是否为最后一批
        chunk_index: 批次序号（0 基）
    """

    columns: Tuple[ColumnInfo, ...]
    rows: List[Tuple[Any, ...]]
    is_last: bool = False
    chunk_index: int = 0

    def __len__(self) -> int:
        """返回本批行数"""
        return len(self.rows)

    def as_dicts(self) -> List[Dict[str, Any]]:
        """本批转为字典列表（键为去重后的显示名）

        Returns:
            List[Dict[str, Any]]: 每行一个字典
        """
        keys = [column.display_name for column in self.columns]
        return [dict(zip(keys, row)) for row in self.rows]


def _derive_names(
    rows: Sequence[Any], metadata: Sequence[Dict[str, Any]] | None
) -> List[str]:
    """推导原始列名列表（可能重复）

    行对象的取键来源（实测）：

    - ``RowMapping``：有 ``keys()``；
    - ``Row``：**没有** ``keys()``，但 ``row._mapping.keys()`` 可用；
    - ``dict`` / ``tuple`` / ``list``：直接可得。

    Args:
        rows: 行序列
        metadata: 列元数据；提供时优先采用

    Returns:
        List[str]: 原始列名列表；无从得知时返回空列表
    """
    if metadata:
        return [str(meta.get("name", f"column_{i}")) for i, meta in enumerate(metadata)]
    for row in rows:
        keys = getattr(row, "keys", None)
        if callable(keys):
            return [str(key) for key in keys()]
        if isinstance(row, dict):
            return [str(key) for key in row]
        # Row：键在 _mapping 上（访问 RowMapping 的 _mapping 会抛 AttributeError，
        # 但那条分支已被上面的 keys() 覆盖）
        inner = getattr(row, "_mapping", None)
        inner_keys = getattr(inner, "keys", None)
        if callable(inner_keys):
            return [str(key) for key in inner_keys()]
        if isinstance(row, (tuple, list)):
            return [f"column_{i}" for i in range(len(row))]
    return []


def _row_to_tuple(row: Any, width: int) -> Tuple[Any, ...]:
    """把任意行对象按**位置**转成元组

    优先使用位置信息，只有在拿不到时才退回按名取值 —— 这样重名列也不会塌陷。

    各形态的位置来源（实测）:

    ==================  ==================================================
    行对象              位置来源
    ==================  ==================================================
    ``dict``            ``values()``（按插入顺序）
    ``tuple`` / ``list`` 本身
    ``RowMapping``      ``values()`` —— **正确**；``list(row)`` 得到的是键
    ``Row``             无 ``values()``，须走 ``_mapping.values()``
    ==================  ==================================================

    Args:
        row: 行对象
        width: 期望的列数

    Returns:
        Tuple[Any, ...]: 定长元组，不足处补 ``None``
    """
    values = row_values(row)
    if len(values) < width:
        values = values + (None,) * (width - len(values))
    return tuple(values[:width])
