"""GBase 8s 方言（DBAPI 无关层）

对齐 SQLAlchemy 官方 Oracle 方言的组织方式：本模块**不感知任何 DBAPI**，
只承载类型编译、SQL/DDL 编译、能力矩阵、版本/schema/隔离级别与系统表反射。
连接建立与取值转换见 ``jaydebeapi.py``。

反射实现按职责拆分为 5 个 mixin（表名/列/约束/外键/服务信息），
以满足「单个类不超过 100 行」的编码约束；最终由 :class:`GBase8sDialect` 组合。

Example:
>>> from db_connector_tool.drivers.gbase8s.base import GBase8sDialect
>>> GBase8sDialect.name
'gbasedbt-sqli'
"""

import re
from typing import Any

from sqlalchemy import exc, sql
from sqlalchemy.dialects.oracle.base import OracleDialect, OracleTypeCompiler
from sqlalchemy.engine import default, reflection

from ...utils.logging_utils import get_logger
from . import dictionary
from .types import colspecs, ischema_names

logger = get_logger(__name__)


class GBase8sTypeCompiler(OracleTypeCompiler):
    """GBase 8s 类型编译器

    在 Oracle 类型编译规则之上补充 GBase 8s 专属类型
    （``NCHAR`` 等通用类型沿用 SQLAlchemy 基类实现，无需重复定义）。
    """

    def visit_LVARCHAR(self, type_: Any, **kw: Any) -> str:
        """渲染 LVARCHAR 类型"""
        if type_.length:
            return "LVARCHAR(%s)" % type_.length
        return "LVARCHAR"

    def visit_SERIAL(self, type_: Any, **kw: Any) -> str:
        """渲染 SERIAL 类型"""
        return "SERIAL"

    def visit_SERIAL8(self, type_: Any, **kw: Any) -> str:
        """渲染 SERIAL8 类型"""
        return "SERIAL8"


def _text(value: Any) -> str:
    """把系统目录取值规范化为去空白的字符串

    Args:
        value: 系统目录返回的值

    Returns:
        去空白后的字符串；None 转为空串
    """
    return str(value).strip() if value is not None else ""


def _execute(
    connection: Any,
    template: str,
    schema: str | None,
    params: tuple | None = None,
    optional: bool = False,
) -> list:
    """执行系统目录查询并返回全部行

    Args:
        connection: SQLAlchemy Connection
        template: 含 ``{p}`` 占位符的 SQL 模板
        schema: 跨库限定串，为 None 时不加前缀
        params: 绑定参数
        optional: 为 True 时查询失败（目录视图缺失）降级为空结果

    Returns:
        行列表

    Raises:
        DBAPIError: optional 为 False 且查询失败时原样抛出
    """
    statement = dictionary.format_sql(template, schema)
    try:
        return list(connection.exec_driver_sql(statement, params).fetchall())
    except exc.DBAPIError:
        if not optional:
            raise
        logger.warning("GBase 8s 系统目录查询失败，已降级为空结果: %s", statement)
        return []


def _part_column_numbers(values: tuple) -> list[int]:
    """从 part1..part16 字段值中提取有效列号

    Args:
        values: part 字段值序列

    Returns:
        升序的列号列表（仅保留大于 0 的项）
    """
    numbers = []
    for value in values:
        if value is None:
            continue
        number = int(value)
        if number > 0:
            numbers.append(number)
    return numbers


def _column_names(
    connection: Any, schema: str | None, tabid: int, colnos: list[int]
) -> list[str]:
    """把列号批量转换为列名

    Args:
        connection: SQLAlchemy Connection
        schema: 跨库限定串
        tabid: 表 id
        colnos: 列号列表

    Returns:
        与 colnos 顺序一致的列名列表
    """
    if not colnos:
        return []
    placeholders = ", ".join("?" for _ in colnos)
    template = dictionary.COLUMN_NAMES_BY_NO.replace("{colnos}", placeholders)
    rows = _execute(connection, template, schema, (tabid, *colnos))
    mapping = {int(row[0]): _text(row[1]) for row in rows}
    return [mapping[no] for no in colnos if no in mapping]


def _table_tabid(connection: Any, schema: str | None, table_name: str) -> int | None:
    """查询表的 tabid

    Args:
        connection: SQLAlchemy Connection
        schema: 跨库限定串
        table_name: 表名

    Returns:
        表 id；表不存在时返回 None
    """
    rows = _execute(connection, dictionary.COLUMN_TABID, schema, (table_name,))
    return int(rows[0][0]) if rows else None


class _GBase8sServerInfo:
    """服务信息：服务端版本与默认 schema"""

    def get_default_schema_name(self, connection: Any) -> str | None:
        """获取默认 schema 名称

        Args:
            connection: 数据库连接对象

        Returns:
            大写的用户名作为默认 schema，用户名不存在时返回 None
        """
        username = connection.engine.url.username
        return username.upper() if username else None

    def _get_default_schema_name(self, connection: Any) -> str | None:
        """内部方法：获取默认 schema 名称

        Args:
            connection: 数据库连接对象

        Returns:
            默认 schema 名称
        """
        return self.get_default_schema_name(connection)

    def _get_server_version_info(self, connection: Any) -> tuple[int, ...] | None:
        """获取服务端版本信息

        依次尝试 ``dbinfo('version_gbase','full')`` 与 ``dbinfo('version','full')``，
        并用正则解析版本号。

        Args:
            connection: 数据库连接对象

        Returns:
            版本号元组（如 ``(8, 8, 3, 6, 2)``），无法解析时返回 None
        """
        try:
            version_queries = [
                "select dbinfo('version_gbase','full') from dual",
                "select dbinfo('version','full') from dual",
            ]

            banner = None
            for query in version_queries:
                try:
                    version_sql = sql.text(query)
                    banner = connection.execute(version_sql).scalar()
                    if banner:
                        break
                except exc.DBAPIError:
                    continue

            if isinstance(banner, str):
                version_pattern = (
                    r"GBase8sV?([\d.]+)"
                    r"(?:_TL_([\d.]+))?"
                    r"(?:_(\d+X\d+))?"
                    r"(?:_(\d+))?"
                    r"(?:_([a-f0-9]+))?"
                )
                match = re.search(version_pattern, banner)

                if match:
                    version_str = match.group(1)
                    return tuple(int(part) for part in version_str.split("."))
                simple_match = re.search(r"(\d+)\.(\d+)", banner)
                if simple_match:
                    return (int(simple_match.group(1)), int(simple_match.group(2)))

            return None

        except exc.DBAPIError:
            return None
        except (ValueError, re.error, AttributeError):
            return None


class _GBase8sNameReflection:
    """表/视图/序列/schema 名称反射"""

    @reflection.cache
    def has_table(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> bool:
        """判断表或视图是否存在"""
        rows = _execute(connection, dictionary.HAS_TABLE, schema, (table_name,))
        return bool(rows and rows[0][0])

    @reflection.cache
    def has_sequence(
        self, connection: Any, sequence_name: str, schema: str | None = None, **kw: Any
    ) -> bool:
        """判断序列是否存在"""
        rows = _execute(connection, dictionary.HAS_SEQUENCE, schema, (sequence_name,))
        return bool(rows and rows[0][0])

    @reflection.cache
    def get_table_names(
        self, connection: Any, schema: str | None = None, **kw: Any
    ) -> list[str]:
        """获取表名列表"""
        rows = _execute(connection, dictionary.TABLE_NAMES, schema, ("T",))
        return [_text(row[0]) for row in rows]

    @reflection.cache
    def get_temp_table_names(
        self, connection: Any, schema: str | None = None, **kw: Any
    ) -> list[str]:
        """获取临时表名列表"""
        rows = _execute(
            connection, dictionary.TEMP_TABLE_NAMES, schema, ("T",), optional=True
        )
        return [_text(row[0]) for row in rows]

    @reflection.cache
    def get_view_names(
        self, connection: Any, schema: str | None = None, **kw: Any
    ) -> list[str]:
        """获取视图名列表"""
        rows = _execute(connection, dictionary.TABLE_NAMES, schema, ("V",))
        return [_text(row[0]) for row in rows]

    @reflection.cache
    def get_sequence_names(
        self, connection: Any, schema: str | None = None, **kw: Any
    ) -> list[str]:
        """获取序列名列表"""
        rows = _execute(connection, dictionary.SEQUENCE_NAMES, schema, optional=True)
        return [_text(row[0]) for row in rows]

    @reflection.cache
    def get_schema_names(self, connection: Any, **kw: Any) -> list[str]:
        """获取数据库（schema）名列表"""
        rows = _execute(connection, dictionary.SCHEMA_NAMES, None, optional=True)
        return [_text(row[0]) for row in rows]


class _GBase8sColumnReflection:
    """列、默认值、列注释与视图定义反射"""

    @reflection.cache
    def get_columns(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[dict]:
        """获取列定义

        Raises:
            NoSuchTableError: 表不存在
        """
        rows = _execute(connection, dictionary.COLUMNS, schema, (table_name,))
        if not rows:
            raise exc.NoSuchTableError(table_name)

        defaults = self._column_defaults(connection, table_name, schema)
        comments = self._column_comments(connection, table_name, schema)

        columns = []
        for name, coltype, collength, colno in rows:
            column_type, not_null = dictionary.parse_column_type(coltype, collength)
            column_name = _text(name)
            columns.append(
                {
                    "name": column_name,
                    "type": column_type,
                    "nullable": not not_null,
                    "default": defaults.get(int(colno)),
                    "autoincrement": dictionary.is_autoincrement_type(coltype),
                    "comment": comments.get(column_name),
                }
            )
        return columns

    def _column_defaults(
        self, connection: Any, table_name: str, schema: str | None
    ) -> dict[int, str]:
        """获取列默认值（sysdefaultsexpr 缺失时降级为空）"""
        rows = _execute(
            connection, dictionary.COLUMN_DEFAULTS, schema, (table_name,), optional=True
        )
        defaults = {}
        for colno, default_expr in rows:
            if default_expr is None:
                continue
            value = str(default_expr).rstrip("\x00")
            defaults[int(colno)] = value or None
        return defaults

    def _column_comments(
        self, connection: Any, table_name: str, schema: str | None
    ) -> dict[str, str]:
        """获取列注释（syscolcomments 缺失时降级为空）"""
        rows = _execute(
            connection, dictionary.COLUMN_COMMENTS, schema, (table_name,), optional=True
        )
        return {_text(row[0]): _text(row[1]) for row in rows if row[1] is not None}

    @reflection.cache
    def get_table_comment(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> dict:
        """获取表注释"""
        rows = _execute(
            connection, dictionary.TABLE_COMMENT, schema, (table_name,), optional=True
        )
        if not rows or rows[0][0] is None:
            return reflection.ReflectionDefaults.table_comment()
        return {"text": str(rows[0][0]).rstrip()}

    @reflection.cache
    def get_view_definition(
        self, connection: Any, view_name: str, schema: str | None = None, **kw: Any
    ) -> str | None:
        """获取视图定义

        按 seqno 拼接全部 viewtext 行（厂商实现仅取首行）。
        视图文本不做去空白处理，否则会丢失行间的空白与换行。

        Raises:
            NoSuchTableError: 视图不存在
        """
        rows = _execute(connection, dictionary.VIEW_DEFINITION, schema, (view_name,))
        if not rows:
            raise exc.NoSuchTableError(view_name)
        return "".join(str(row[0]) for row in rows if row[0] is not None)


class _GBase8sConstraintReflection:
    """主键、索引、唯一、外键与检查约束反射"""

    @reflection.cache
    def get_pk_constraint(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> dict:
        """获取主键约束"""
        rows = _execute(connection, dictionary.PK_CONSTRAINT, schema, (table_name,))
        if not rows:
            return reflection.ReflectionDefaults.pk_constraint()

        tabid, idxname = rows[0][0], rows[0][1]
        colnos = _part_column_numbers(tuple(rows[0][2:]))
        return {
            "name": _text(idxname),
            "constrained_columns": _column_names(connection, schema, int(tabid), colnos),
        }

    @reflection.cache
    def get_indexes(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[dict]:
        """获取索引（跳过主键/外键背后由约束创建的索引）"""
        rows = _execute(connection, dictionary.INDEXES, schema, (table_name,))
        if not rows:
            return []

        tabid = _table_tabid(connection, schema, table_name)
        part_end = 3 + dictionary.MAX_INDEX_COLUMNS
        indexes = []
        for row in rows:
            constrtype = _text(row[part_end])
            if constrtype in ("P", "R"):
                continue
            idxtype = _text(row[1])
            colnos = _part_column_numbers(tuple(row[3:part_end]))
            indexes.append(
                {
                    "name": _text(row[0]),
                    "column_names": (
                        _column_names(connection, schema, tabid, colnos)
                        if tabid is not None
                        else []
                    ),
                    "unique": idxtype.upper().startswith("U"),
                    "dialect_options": {},
                }
            )
        return indexes

    @reflection.cache
    def get_unique_constraints(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[dict]:
        """获取唯一约束（由唯一索引表达）"""
        return [
            {"name": index["name"], "column_names": index["column_names"]}
            for index in self.get_indexes(connection, table_name, schema)
            if index["unique"]
        ]


class _GBase8sForeignKeyReflection:
    """外键与检查约束反射"""

    @reflection.cache
    def get_foreign_keys(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[dict]:
        """获取外键约束"""
        rows = _execute(connection, dictionary.FOREIGN_KEYS_LOCAL, schema, (table_name,))
        foreign_keys = []
        for row in rows:
            referred = _execute(
                connection, dictionary.FOREIGN_KEYS_REFERRED, schema, (int(row[3]),)
            )
            if not referred:
                continue
            item = self._build_foreign_key(connection, schema, row, referred[0])
            foreign_keys.append(item)
        return foreign_keys

    def _build_foreign_key(
        self, connection: Any, schema: str | None, row: tuple, referred: tuple
    ) -> dict:
        """组装单条外键定义"""
        part_end = 4 + dictionary.MAX_INDEX_COLUMNS
        local_columns = _column_names(
            connection, schema, int(row[0]), _part_column_numbers(tuple(row[4:part_end]))
        )
        referred_columns = _column_names(
            connection,
            schema,
            int(referred[1]),
            _part_column_numbers(tuple(referred[3:])),
        )
        return {
            "name": _text(row[2]),
            "constrained_columns": local_columns,
            "referred_schema": schema,
            "referred_table": _text(referred[0]),
            "referred_columns": referred_columns,
            "options": {"ondelete": dictionary.DELETE_RULE_MAP.get(_text(referred[2]))},
        }

    @reflection.cache
    def get_check_constraints(
        self, connection: Any, table_name: str, schema: str | None = None, **kw: Any
    ) -> list[dict]:
        """获取检查约束

        按 ``constrname`` 分组后把同组 ``checktext`` 按 ``seqno`` 拼接；
        检查文本不做去空白处理，否则会丢失片段之间的空格。
        """
        rows = _execute(
            connection, dictionary.CHECK_CONSTRAINTS, schema, (table_name,), optional=True
        )
        grouped: dict[str, list[tuple[int, str]]] = {}
        for name, checktext, seqno in rows:
            grouped.setdefault(_text(name), []).append(
                (int(seqno), "" if checktext is None else str(checktext))
            )

        return [
            {
                "name": name,
                "sqltext": "".join(text for _, text in sorted(parts)),
            }
            for name, parts in grouped.items()
        ]


class _GBase8sIsolationLevel:
    """事务隔离级别

    GBase 8s 的自动提交由 JDBC ``setAutoCommit`` 控制，不通过 ``SET ISOLATION`` 表达，
    因此隔离级别白名单中不含 ``AUTOCOMMIT``。
    """

    def get_isolation_level_values(self, dbapi_connection: Any) -> tuple[str, ...]:
        """返回允许设置的隔离级别名称"""
        return dictionary.ISOLATION_LEVELS

    def get_default_isolation_level(self, dbapi_conn: Any) -> str:
        """返回默认隔离级别

        Returns:
            当前连接隔离级别，读取失败时返回 GBase 8s 默认的 ``COMMITTED READ``
        """
        return self.get_isolation_level(dbapi_conn) or "COMMITTED READ"

    def get_isolation_level(self, dbapi_connection: Any) -> str | None:
        """读取当前隔离级别

        Returns:
            隔离级别字符串；查询失败时返回 None
        """
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(dictionary.ISOLATION_LEVEL)
            row = cursor.fetchone()
        except Exception:  # pylint: disable=broad-except
            logger.warning("读取 GBase 8s 隔离级别失败，返回 None")
            return None
        finally:
            cursor.close()
        return _text(row[0]) if row else None

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        """设置隔离级别

        ``SET ISOLATION`` 不支持绑定参数，故以白名单校验代替参数化，防止 SQL 注入。

        Raises:
            ArgumentError: level 不在白名单内
        """
        normalized = str(level).upper()
        if normalized not in dictionary.ISOLATION_LEVELS:
            raise exc.ArgumentError(
                "不支持的 GBase 8s 隔离级别 %r，可选值: %s"
                % (level, ", ".join(dictionary.ISOLATION_LEVELS))
            )
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("SET ISOLATION TO %s" % normalized)
        finally:
            cursor.close()


# pylint: disable=too-many-ancestors
class GBase8sDialect(
    _GBase8sServerInfo,
    _GBase8sNameReflection,
    _GBase8sColumnReflection,
    _GBase8sConstraintReflection,
    _GBase8sForeignKeyReflection,
    _GBase8sIsolationLevel,
    OracleDialect,
):
    """GBase 8s 方言（DBAPI 无关层）

    继承 Oracle 基类以复用 oracle 兼容模式的 SQL 编译规则
    （``ROWNUM`` 分页、``||`` 连接、``FROM DUAL`` 等），
    并按 GBase 8s 的实际能力覆盖差异项。

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine("jdbcgbase8s://user:pwd@host:9088/db")
    """

    name = "gbasedbt-sqli"
    driver = "com.gbasedbt.jdbc.Driver"
    default_paramstyle = "qmark"

    colspecs = colspecs
    ischema_names = ischema_names
    type_compiler_cls = GBase8sTypeCompiler

    # jaydebeapi 无 cursor.var()，无法绑定 RETURNING 的 out 参数
    insert_returning = False
    update_returning = False
    delete_returning = False

    # GBase 8s 不支持多行 VALUES 语法
    use_insertmanyvalues = False
    use_insertmanyvalues_wo_returning = False

    # jaydebeapi 无 lastrowid，不引入 dbinfo('sqlca.sqlerrd1') 额外查询
    postfetch_lastrowid = False
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    supports_statement_cache = True
    description_encoding = None
    supports_sequences = True

    def __init__(self, **kwargs: Any) -> None:
        """初始化方言

        GBase 8s 不支持 Oracle 12+ 的 ``OFFSET/FETCH`` 语法，分页固定走基类的
        ``ROWNUM`` 包装路径，故关闭 ``enable_offset_fetch``。
        """
        kwargs.setdefault("enable_offset_fetch", False)
        super().__init__(**kwargs)

    def initialize(self, connection: Any) -> None:
        """初始化方言

        不走 Oracle 基类的版本分支（GBase 8s 无 IDENTITY 列，且不支持 OFFSET/FETCH），
        同时避免版本号无法解析（None）时与 Oracle 版本元组比较引发 TypeError。
        """
        default.DefaultDialect.initialize(self, connection)
        self.supports_identity_columns = False
        self._supports_offset_fetch = False

    @property
    def _is_oracle_8(self) -> bool:
        """GBase 8s 并非 Oracle 8，禁用基类的 Oracle 8 降级分支"""
        return False

    def _check_max_identifier_length(self, connection: Any) -> None:
        """继承 Oracle 基类的 128 字节标识符上限，不做运行时探测"""
        return None