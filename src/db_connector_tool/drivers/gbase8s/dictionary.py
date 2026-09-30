"""GBase 8s 数据字典（系统目录）查询与列类型解析

该模块集中存放反射所需的系统目录 SQL 以及 ``syscolumns`` 列类型解析逻辑。
为兼容一库三模（oracle / gbase / mysql），所有反射 SQL 均满足以下约束：

* 只使用 ANSI 连接语法（``JOIN ... ON``），不使用 Informix 的 ``OUTER`` 旧语法；
* 只使用 ``?`` 位置占位符，配合 ``exec_driver_sql`` 执行；
* 不使用 ``FIRST``/``SKIP`` 等仅某一模式可用的分页关键字。

对于可能不存在的可选系统目录（如 ``sysdefaults``），调用方需按“取不到即空”处理。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence, Type, cast

from sqlalchemy import types as sqltypes

from .types import ischema_names

# 单个索引最多包含 16 个列（sysindexes.part1 ~ part16）
_INDEX_PART_COLUMNS = ", ".join(f"i.part{index}" for index in range(1, 17))

# GBase 8s 中 CLOB / BLOB / BOOLEAN 共用 coltype=41，需借助 syscolumns.extended_id 区分
_LARGE_OBJECT_COLTYPE = 41
_EXTENDED_TYPE_MAP: Dict[int, Type[sqltypes.TypeEngine]] = {
    5: sqltypes.Boolean,  # BOOLEAN
    10: sqltypes.LargeBinary,  # BLOB
    11: sqltypes.Text,  # CLOB
}

# 外键查询：sysindexes 的 part 列带别名，避免本地/远程表列名冲突
_LOCAL_PART_ALIASED = ", ".join(
    f"li.part{index} AS l_part{index}" for index in range(1, 17)
)
_REMOTE_PART_ALIASED = ", ".join(
    f"ri.part{index} AS r_part{index}" for index in range(1, 17)
)

# 查询当前连接的用户名，其结果同时作为默认 schema（owner）
CURRENT_USER_QUERY = "SELECT USER FROM systables WHERE tabid = 1"

# 查询所有 schema（GBase 8s 以对象 owner 作为 schema）
SCHEMA_NAMES_QUERY = (
    "SELECT DISTINCT owner FROM systables WHERE tabid >= 100 ORDER BY owner"
)

# 按 owner 与对象类型（T=表，V=视图，Q=序列）查询对象名
OBJECT_NAMES_QUERY = (
    "SELECT tabname FROM systables "
    "WHERE owner = ? AND tabtype = ? AND tabid >= 100 "
    "ORDER BY tabname"
)

# 判断表/视图是否存在
HAS_OBJECT_QUERY = (
    "SELECT COUNT(*) FROM systables "
    "WHERE tabname = ? AND owner = ? AND tabtype IN ('T', 'V')"
)

# 查询表的列定义（colattr 高 8 位表示是否非空，低 8 位为类型号）
COLUMNS_QUERY = (
    "SELECT c.colname, c.coltype, c.collength, c.colno, c.extended_id "
    "FROM syscolumns c JOIN systables t ON c.tabid = t.tabid "
    "WHERE t.tabname = ? AND t.owner = ? "
    "ORDER BY c.colno"
)

# 查询表的主键约束（sysconstraints.constrtype = 'P'）
PRIMARY_KEY_QUERY = (
    f"SELECT c.constrname, {_INDEX_PART_COLUMNS} "  # noqa: S608 - 仅拼接固定的 part 列名
    "FROM sysconstraints c "
    "JOIN systables t ON c.tabid = t.tabid "
    "JOIN sysindexes i ON i.tabid = c.tabid AND i.idxname = c.idxname "
    "WHERE t.tabname = ? AND t.owner = ? AND c.constrtype = 'P'"
)

# 查询索引（含唯一索引与约束名）
INDEXES_QUERY = (
    f"SELECT i.idxname, i.idxtype, c.constrtype, c.constrname, {_INDEX_PART_COLUMNS} "
    "FROM sysindexes i "
    "JOIN systables t ON i.tabid = t.tabid "
    "LEFT JOIN sysconstraints c ON c.tabid = i.tabid AND c.idxname = i.idxname "
    "WHERE t.tabname = ? AND t.owner = ?"
)

# 查询表的外键约束。直接选取 sysindexes 的 part 列号，
# 在 Python 端通过 column_name_map 解析列名，避免 syscolumns IN(...)
# 对多列外键产生笛卡尔积导致列配对顺序错误。
FOREIGN_KEYS_QUERY = (
    f"SELECT c.constrname, {_LOCAL_PART_ALIASED}, rt.tabname, rt.owner, "
    f"{_REMOTE_PART_ALIASED}, r.delrule "
    "FROM sysconstraints c "
    "JOIN systables t ON c.tabid = t.tabid "
    "JOIN sysreferences r ON r.constrid = c.constrid "
    "LEFT JOIN systables rt ON rt.tabid = r.ptabid "
    "JOIN sysconstraints pc ON pc.constrid = r.primary "
    "JOIN sysindexes ri ON ri.tabid = pc.tabid AND ri.idxname = pc.idxname "
    "JOIN sysindexes li ON li.tabid = c.tabid AND li.idxname = c.idxname "
    "WHERE t.tabname = ? AND t.owner = ? AND c.constrtype = 'R'"
)

# FOREIGN_KEYS_QUERY 结果列位置。与 SELECT 列顺序严格绑定，修改查询时需同步更新。
FK_CONSTRNAME = 0
FK_LOCAL_PARTS_START = 1
FK_LOCAL_PARTS_END = 17  # 不含
FK_REMOTE_TABLE = 17
FK_REMOTE_OWNER = 18
FK_REMOTE_PARTS_START = 19
FK_REMOTE_PARTS_END = 35  # 不含
FK_DELRULE = 35

# 查询检查约束（syschecks.type = 'T' 表示约束文本）
CHECK_CONSTRAINTS_QUERY = (
    "SELECT c.constrname, k.checktext "
    "FROM sysconstraints c "
    "JOIN systables t ON c.tabid = t.tabid "
    "JOIN syschecks k ON k.constrid = c.constrid "
    "WHERE t.tabname = ? AND t.owner = ? AND c.constrtype = 'C' AND k.type = 'T' "
    "ORDER BY c.constrname, k.seqno"
)

# 查询视图定义
VIEW_DEFINITION_QUERY = (
    "SELECT v.viewtext "
    "FROM sysviews v JOIN systables t ON v.tabid = t.tabid "
    "WHERE t.tabname = ? AND t.owner = ? "
    "ORDER BY v.seqno"
)

# 查询表的所有列号与其列名（用于把索引/约束中的列号还原为列名）
COLUMN_NAME_MAP_QUERY = (
    "SELECT c.colno, c.colname "
    "FROM syscolumns c JOIN systables t ON c.tabid = t.tabid "
    "WHERE t.tabname = ? AND t.owner = ?"
)

# 查询列默认值。sysdefaults 为可选目录，且其列名包含保留字，
# 因此使用 SELECT * 并通过结果集列名定位，避免不同 sqlmode 下的标识符引用问题。
COLUMN_DEFAULTS_QUERY = (
    "SELECT d.* FROM sysdefaults d "
    "JOIN systables t ON d.tabid = t.tabid "
    "WHERE t.tabname = ? AND t.owner = ?"
)

# 查询当前会话隔离级别（部分版本/模式不支持该 DBINFO 用法）
ISOLATION_LEVEL_QUERY = (
    "SELECT DBINFO('session', 'isolation') FROM systables WHERE tabid = 1"
)


def get_index_part_columns(row: Any) -> List[int]:
    """从 sysindexes 结果行中提取有效的列号列表

    Args:
        row: 包含 ``part1`` ~ ``part16`` 字段的结果行

    Returns:
        list: 按索引列顺序排列的列号列表，已过滤空值并取绝对值

    Example:
        >>> get_index_part_columns(row)
        [1, 2]
    """
    part_numbers = [getattr(row, f"part{index}", None) for index in range(1, 17)]
    return [abs(int(number)) for number in part_numbers if number]


def parse_column_type(
    coltype: int, collength: int, extended_id: int = 0
) -> sqltypes.TypeEngine:
    """把 syscolumns 中的类型号与长度解析为 SQLAlchemy 类型对象

    Args:
        coltype: ``syscolumns.coltype`` 的低 8 位类型号
        collength: ``syscolumns.collength``，其含义随类型变化
        extended_id: ``syscolumns.extended_id``，仅 ``coltype`` 为 41 时用于
            区分 CLOB(11) / BLOB(10) / BOOLEAN(5)

    Returns:
        sqltypes.TypeEngine: 对应的 SQLAlchemy 类型实例；无法识别时为 ``NULLTYPE``

    Example:
        >>> parse_column_type(13, 64)
        VARCHAR(64)
        >>> parse_column_type(41, 1, 5)
        BOOLEAN
    """
    if coltype in (0, 13):  # CHAR / VARCHAR：collength 即为字符长度
        type_cls = cast(Type[sqltypes.String], ischema_names[coltype])
        return type_cls(collength)
    if coltype == _LARGE_OBJECT_COLTYPE:  # CLOB / BLOB / BOOLEAN 共用类型号
        extended_type = _EXTENDED_TYPE_MAP.get(extended_id)
        return extended_type() if extended_type else ischema_names[coltype]()
    if coltype == 5:  # DECIMAL：高 8 位为精度，低 8 位为小数位
        precision, scale = (collength & 0xFF00) >> 8, collength & 0xFF
        return sqltypes.Numeric(precision, 0 if scale == 255 else scale)
    if coltype == 14:  # INTERVAL：collength 为编码限定符，此处不展开
        return ischema_names[coltype]()
    type_class = ischema_names.get(coltype)
    if type_class is None:
        return sqltypes.NULLTYPE
    return type_class()


def parse_column_defaults(
    result_keys: Sequence[str], rows: List[Sequence[Any]]
) -> Dict[int, str]:
    """把 sysdefaults 结果集解析为 ``{列号: 默认值文本}``

    Args:
        result_keys: 结果集的列名序列，用于定位 ``colno`` 与 ``default`` 列
        rows: sysdefaults 的结果行列表

    Returns:
        Dict[int, str]: 列号到默认值文本的映射，空值不会出现在结果中

    Example:
        >>> parse_column_defaults(["colno", "default"], [(1, "0")])
        {1: '0'}
    """
    keys = [str(key).lower() for key in result_keys]
    try:
        colno_index = keys.index("colno")
        default_index = keys.index("default")
    except ValueError:
        return {}
    defaults: Dict[int, str] = {}
    for row in rows:
        value: Optional[str] = row[default_index]
        if value is None or not str(value).strip():
            continue
        defaults[int(row[colno_index])] = str(value).strip()
    return defaults
