"""GBase 8s 系统目录字典 (GBase8s Dictionary)

集中存放反射所使用的系统目录 SQL 常量与类型码解析逻辑：

- 所有系统目录均来自**当前数据库**，表名前缀占位符 ``{p}`` 由 ``schema`` 决定
  （``schema`` 可为 ``dbname`` 或 ``dbname:owner`` 形式的跨库限定串，为 ``None`` 时不加前缀）；
- 过滤系统目录：``tabid > 99``（Informix 血统中 tabid < 100 为系统目录）；
- 类型码解析：``syscolumns.coltype`` 低字节为类型码，``coltype // 256 == 1`` 表示 NOT NULL。

Example:
>>> from db_connector_tool.drivers.gbase8s.dictionary import columns_sql
>>> columns_sql("mydb").startswith("SELECT t1.colname")
True
"""

from typing import Any

from sqlalchemy.sql import sqltypes

from ...utils.logging_utils import get_logger
from .types import AUTOINCREMENT_TYPE_CODES, ischema_names

logger = get_logger(__name__)

# 系统目录起始 tabid（小于该值的为系统表）
SYSTEM_TABLE_MAX_TABID = 99

# 数据类型：coltype 低字节类型码
CHAR = 0
SMALLINT = 1
INTEGER = 2
SMALLFLOAT = 4
DECIMAL = 5
SERIAL = 6
DATE = 7
MONEY = 8
DATETIME = 10
BYTE = 11
TEXT = 12
VARCHAR_TYPE = 13
NCHAR = 15
NVARCHAR = 16
INT8 = 17
SERIAL8 = 18
LVARCHAR_TYPE = 43
BIGINT = 52
TIMESTAMP = 65

# 主键/外键/索引的 16 个列号字段（Informix 血统固定 16 个 part 字段）
MAX_INDEX_COLUMNS = 16


def part_fields(alias: str) -> str:
    """生成索引列号字段列表（``alias.part1 ... alias.part16``）

    Args:
        alias: 表别名

    Returns:
        逗号分隔的字段列表字符串
    """
    return ", ".join(
        "%s.part%d" % (alias, no) for no in range(1, MAX_INDEX_COLUMNS + 1)
    )


PARTS_T1 = part_fields("t1")
PARTS_T2 = part_fields("t2")

HAS_TABLE = (
    "select count(*) from {p}systables where tabname = ? "
    "and tabtype in ('T', 'V')"
)

HAS_SEQUENCE = (
    "select count(*) from {p}systables where tabname = ? and tabtype = 'Q'"
)

TABLE_NAMES = (
    "SELECT tabname FROM {p}systables WHERE tabtype = ? "
    "AND tabid > %d ORDER BY tabname" % SYSTEM_TABLE_MAX_TABID
)

# oracle 兼容模式下的临时表 flags 实测值
TEMP_TABLE_NAMES = (
    "SELECT tabname FROM {p}systables WHERE tabtype = ? "
    "AND flags = 28672 ORDER BY tabname"
)

SEQUENCE_NAMES = (
    "SELECT tabname FROM {p}systables WHERE tabtype = 'Q' "
    "AND tabid > %d ORDER BY tabname" % SYSTEM_TABLE_MAX_TABID
)

SCHEMA_NAMES = "SELECT DBS_DBSNAME FROM SYSMASTER.SYSDBSLOCALE"

COLUMNS = (
    "SELECT t1.colname, t1.coltype, t1.collength, t1.colno\n"
    "FROM {p}syscolumns AS t1, {p}systables AS t2\n"
    "WHERE t1.tabid = t2.tabid AND t2.tabname = ?\n"
    "ORDER BY t1.colno"
)

COLUMN_TABID = (
    "SELECT t2.tabid FROM {p}systables AS t2 WHERE t2.tabname = ? "
    "AND t2.tabtype in ('T', 'V')"
)

# sysdefaultsexpr 在部分版本/GBase 分支才存在，缺失时降级为无默认值
COLUMN_DEFAULTS = (
    "SELECT t3.colno, t3.default\n"
    "FROM {p}sysdefaultsexpr AS t3, {p}systables AS t2\n"
    "WHERE t3.tabid = t2.tabid AND t2.tabname = ? AND t3.type = 'T'"
)

COLUMN_COMMENTS = (
    "select colname, comments from {p}syscolcomments where tabname = ?"
)

COLUMN_NAMES_BY_NO = (
    "SELECT colno, colname FROM {p}syscolumns "
    "WHERE tabid = ? AND colno IN ({colnos})"
)

PK_CONSTRAINT = (
    "SELECT t1.tabid, t2.idxname, %s\n"
    "FROM {p}systables AS t1, {p}sysindexes AS t2, {p}sysconstraints AS t3\n"
    "WHERE t1.tabid = t2.tabid AND t1.tabname = ?\n"
    "  AND t2.idxname = t3.idxname AND t3.constrtype = 'P'" % PARTS_T2
)

INDEXES = (
    "SELECT t1.idxname, t1.idxtype, t1.clustered, %s,\n"
    "       t2.constrtype, t2.constrname\n"
    "FROM {p}sysindexes AS t1\n"
    "LEFT JOIN {p}sysconstraints AS t2 "
    "ON (t1.tabid = t2.tabid AND t1.idxname = t2.idxname)\n"
    "WHERE t1.tabid = (SELECT tabid FROM {p}systables WHERE tabname = ?)\n"
    "ORDER BY t1.idxname ASC" % PARTS_T1
)

FOREIGN_KEYS_LOCAL = (
    "SELECT t1.tabid, t2.idxname, t3.constrname, t3.constrid, %s\n"
    "FROM {p}systables AS t1, {p}sysindexes AS t2, {p}sysconstraints AS t3\n"
    "WHERE t1.tabid = t2.tabid AND t1.tabname = ?\n"
    "  AND t2.idxname = t3.idxname AND t3.constrtype = 'R'" % PARTS_T2
)

FOREIGN_KEYS_REFERRED = (
    "SELECT t4.tabname, t4.tabid, t1.delrule, %s\n"
    "FROM {p}sysreferences AS t1, {p}sysindexes AS t2, "
    "{p}sysconstraints AS t3, {p}systables AS t4\n"
    "WHERE t1.constrid = ? AND t3.constrid = t1.primary\n"
    "  AND t2.idxname = t3.idxname AND t4.tabid = t1.ptabid" % PARTS_T2
)

CHECK_CONSTRAINTS = (
    "SELECT t1.constrname, t2.checktext, t2.seqno\n"
    "FROM {p}sysconstraints AS t1, {p}syschecks AS t2\n"
    "WHERE t1.tabid = (SELECT tabid FROM {p}systables WHERE tabname = ?)\n"
    "  AND t1.constrid = t2.constrid AND t2.type = 'T' AND t1.constrtype = 'C'\n"
    "ORDER BY t1.constrname, t2.seqno"
)

TABLE_COMMENT = "select comments from {p}syscomments where tabname = ?"

VIEW_DEFINITION = (
    "SELECT t1.viewtext\n"
    "FROM {p}sysviews AS t1, {p}systables AS t2\n"
    "WHERE t1.tabid = t2.tabid AND t2.tabname = ?\n"
    "ORDER BY seqno"
)

ISOLATION_LEVEL = "select scs_isolationlevel from sysmaster:syssqlcurses"

# 外键删除动作编码：delrule
DELETE_RULE_MAP = {"C": "CASCADE", "N": "SET NULL"}

# 隔离级别白名单（SET ISOLATION 不支持绑定参数，白名单即注入防护）
ISOLATION_LEVELS = (
    "DIRTY READ",
    "COMMITTED READ",
    "COMMITTED READ LAST COMMITTED",
    "CURSOR STABILITY",
    "REPEATABLE READ",
)


def format_sql(template: str, schema: str | None) -> str:
    """把 ``{p}`` 占位符替换为 schema 前缀

    Args:
        template: 含 ``{p}`` 占位符的 SQL 模板
        schema: 跨库限定串（``dbname`` 或 ``dbname:owner``），为 None 时不加前缀

    Returns:
        可执行的 SQL 文本
    """
    prefix = "%s." % schema if schema else ""
    return template.format(p=prefix)


def parse_column_type(coltype: int, collength: int) -> tuple[Any, bool]:
    """解析 ``syscolumns.coltype`` 与 ``collength``

    Args:
        coltype: 系统目录 coltype，低字节为类型码，``coltype // 256 == 1`` 表示 NOT NULL
        collength: 系统目录 collength，按类型码编码长度/精度

    Returns:
        (SQLAlchemy 类型实例, 是否 NOT NULL)
    """
    not_null, code = divmod(int(coltype), 256)
    type_class = ischema_names.get(code)

    if type_class is None:
        logger.warning("未识别的 GBase 8s 类型码 %s，按 NULLTYPE 处理", code)
        return sqltypes.NullType(), bool(not_null)

    if code in (CHAR, NCHAR):
        return type_class(int(collength)), bool(not_null)
    if code in (VARCHAR_TYPE, NVARCHAR):
        return type_class(int(collength) // 256), bool(not_null)
    if code == LVARCHAR_TYPE:
        length = int(collength) // 256 if int(collength) > 255 else int(collength)
        return type_class(length), bool(not_null)
    if code == DECIMAL:
        precision = (int(collength) & 0xFF00) >> 8
        scale = int(collength) & 0xFF
        if scale == 255:
            return sqltypes.INTEGER(), bool(not_null)
        return sqltypes.Numeric(precision, scale), bool(not_null)

    return type_class(), bool(not_null)


def is_autoincrement_type(coltype: int) -> bool:
    """判断类型码对应列是否为自增列

    Args:
        coltype: 系统目录 coltype

    Returns:
        是否为 SERIAL / SERIAL8 自增类型
    """
    _, code = divmod(int(coltype), 256)
    return code in AUTOINCREMENT_TYPE_CODES