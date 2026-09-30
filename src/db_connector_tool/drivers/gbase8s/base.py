"""GBase 8s SQLAlchemy 方言核心实现（与具体 DBAPI 无关）

文件组织参考 SQLAlchemy Oracle 方言：``base.py`` 只负责类型编译、SQL 编译、
DDL 编译以及系统目录反射；JayDeBeApi/JDBC 相关的连接细节放在 :mod:`jaydebeapi`。

**一库三模**：GBase 8s 支持同一实例按会话参数 ``sqlmode`` 在
``oracle`` / ``gbase`` / ``mysql`` 三种兼容模式间切换。本模块据此生成
伪表、分页与布尔字面量等差异化 SQL：

======================  ====================  =============================  ==================
能力                    oracle 模式           gbase 模式                     mysql 模式
======================  ====================  =============================  ==================
无 FROM 时的伪表        ``FROM DUAL``         ``FROM sysmaster:sysdual``     ``FROM sysmaster:sysdual``
分页                    ``ROWNUM`` 双层子查询 ``SKIP n FIRST m``             ``LIMIT m OFFSET n``
布尔字面量              ``'t'`` / ``'f'``     ``'t'`` / ``'f'``              ``'t'`` / ``'f'``
======================  ====================  =============================  ==================

其中伪表差异经真机验证：oracle 模式允许省略 FROM，但不能使用 ``库:表`` 的
跨库限定语法（故用 ``DUAL``）；gbase 与 mysql 模式必须给出 FROM，且 mysql
模式下 ``DUAL`` 不存在，只能用 ``sysmaster:sysdual``。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import exc
from sqlalchemy import schema as sa_schema
from sqlalchemy import sql
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default as default_dialect
from sqlalchemy.engine import reflection
from sqlalchemy.sql import compiler
from sqlalchemy.sql.elements import literal_column

from ...utils.logging_utils import get_logger
from . import dictionary
from .types import colspecs, ischema_names

logger = get_logger(__name__)

# pylint: disable=abstract-method
# 本方言只实现 GBase 8s 用得到的 Dialect 接口，其余抽象方法交由
# ``DefaultDialect`` 的默认实现（抛 NotImplementedError）处理。
# pylint: disable=unused-argument,invalid-name
# ``visit_XXX`` 的方法名与参数签名由 SQLAlchemy 编译器的访问者约定决定。
# pylint: disable=protected-access,no-member
# 编译器需读取 ``Select`` 的受保护属性；反射 mixin 需访问组合成方言后才存在的成员。
# pylint: disable=too-few-public-methods
# 反射 mixin 只按职责聚合方法，不携带实例状态。

# 一库三模的会话兼容模式
SQLMODE_ORACLE = "oracle"
SQLMODE_GBASE = "gbase"
SQLMODE_MYSQL = "mysql"
SUPPORTED_SQLMODES: Tuple[str, ...] = (SQLMODE_ORACLE, SQLMODE_GBASE, SQLMODE_MYSQL)
DEFAULT_SQLMODE = SQLMODE_ORACLE

# 各模式在“无 FROM 子句”时使用的伪表。oracle 模式允许省略 FROM 但也接受 DUAL；
# gbase/mysql 模式必须给出 FROM，其中 mysql 模式没有 DUAL，只能用 sysmaster:sysdual。
PSEUDO_TABLES: Dict[str, str] = {
    SQLMODE_ORACLE: " FROM DUAL",
    SQLMODE_GBASE: " FROM sysmaster:sysdual",
    SQLMODE_MYSQL: " FROM sysmaster:sysdual",
}

# ORACLE 模式下 ROWNUM 包装查询使用的别名
_ROWNUM_LABEL = "gbase8s_rn"

# mysql 模式在只有 OFFSET 没有 LIMIT 时使用的行数上限。
# GBase 8s 不接受 MySQL 的 ``LIMIT -1``，也不接受超出 BIGINT 范围的
# ``18446744073709551615``（真机验证均报语法错误），故取 BIGINT 最大值。
_MYSQL_MAX_ROW_COUNT = "9223372036854775807"

# 会话隔离级别到 GBase 8s 语法的映射
_READ_UNCOMMITTED = "READ UNCOMMITTED"
_READ_COMMITTED = "READ COMMITTED"
_REPEATABLE_READ = "REPEATABLE READ"
ISOLATION_LEVELS: Dict[str, str] = {
    _READ_UNCOMMITTED: "DIRTY READ",
    _READ_COMMITTED: "COMMITTED READ",
    _REPEATABLE_READ: "REPEATABLE READ",
    "SERIALIZABLE": "SERIALIZABLE",
}


def normalize_sqlmode(value: Optional[str]) -> str:
    """校验并规范化 sqlmode 取值

    Args:
        value: 用户传入的兼容模式，大小写不敏感；``None`` 或空串取默认值

    Returns:
        str: 规范化后的模式名（``oracle`` / ``gbase`` / ``mysql``）

    Raises:
        ValueError: 当取值不在支持范围内时

    Example:
        >>> normalize_sqlmode("MySQL")
        'mysql'
    """
    mode = (value or "").strip().lower()
    if not mode:
        return DEFAULT_SQLMODE
    if mode not in SUPPORTED_SQLMODES:
        raise ValueError(
            f"不支持的 sqlmode: {value}，可选值为 {'/'.join(SUPPORTED_SQLMODES)}"
        )
    return mode


def column_name_map(connection: Any, table_name: str, schema: str) -> Dict[int, str]:
    """查询 ``{列号: 列名}`` 映射，用于还原索引与约束中的列

    结果缓存在 ``connection.info`` 中，避免同一次反射中重复查库。

    Args:
        connection: SQLAlchemy 连接对象
        table_name: 表名
        schema: schema（GBase 8s 中的 owner）

    Returns:
        Dict[int, str]: 列号到列名的映射

    Example:
        >>> column_name_map(conn, "orders", "cvicse")
        {1: 'order_id', 2: 'amount'}
    """
    cache = connection.info.setdefault("gbase8s_column_name_map", {})
    cache_key = (table_name, schema)
    if cache_key not in cache:
        rows = connection.exec_driver_sql(
            dictionary.COLUMN_NAME_MAP_QUERY, (table_name, schema)
        ).fetchall()
        cache[cache_key] = {int(row[0]): str(row[1]).strip() for row in rows}
    return cache[cache_key]


class GBase8sTypeCompiler(compiler.GenericTypeCompiler):
    """GBase 8s 类型编译器

    GBase 8s 需要为部分通用类型提供方言写法，例如 ``TEXT`` 应渲染为
    ``CLOB``、``BIGINT`` 应渲染为 ``INT8``。
    """

    def visit_TEXT(self, type_: sqltypes.Text, **kw: Any) -> str:
        return "CLOB"

    def visit_BIGINT(self, type_: sqltypes.BigInteger, **kw: Any) -> str:
        return "INT8"

    def visit_DATETIME(self, type_: sqltypes.DateTime, **kw: Any) -> str:
        return "DATETIME YEAR TO FRACTION(5)"

    def visit_TIME(self, type_: sqltypes.Time, **kw: Any) -> str:
        return "DATETIME HOUR TO FRACTION(5)"

    def visit_INTERVAL(self, type_: sqltypes.Interval, **kw: Any) -> str:
        """INTERVAL 统一按 DAY TO FRACTION(5) 精度渲染"""
        return "INTERVAL DAY TO FRACTION(5)"

    def visit_TIMESTAMP(self, type_: sqltypes.TIMESTAMP, **kw: Any) -> str:
        """GBase 8s 无独立 TIMESTAMP 类型，渲染为 DATETIME YEAR TO FRACTION(5)"""
        return "DATETIME YEAR TO FRACTION(5)"

    def visit_LVARCHAR(self, type_: Any, **kw: Any) -> str:
        """LVARCHAR 按可选长度渲染"""
        length = getattr(type_, "length", None)
        return f"LVARCHAR({length})" if length else "LVARCHAR"

    def visit_MONEY(self, type_: Any, **kw: Any) -> str:
        """MONEY 为 GBase 8s 内置货币类型，无需长度"""
        return "MONEY"

    def visit_SERIAL(self, type_: Any, **kw: Any) -> str:
        """SERIAL 为 GBase 8s 自增整型"""
        return "SERIAL"

    def visit_SERIAL8(self, type_: Any, **kw: Any) -> str:
        """SERIAL8 为 GBase 8s 自增大整型"""
        return "SERIAL8"


class GBase8sCompiler(compiler.SQLCompiler):
    """GBase 8s 语句编译器，按 ``sqlmode`` 生成差异化 SQL"""

    dialect: "GBase8sDialect"

    def default_from(self) -> str:
        return PSEUDO_TABLES[self.dialect.sqlmode]

    def visit_empty_set_expr(self, element_types: Any, **kw: Any) -> str:
        return f"SELECT 1{PSEUDO_TABLES[self.dialect.sqlmode]} WHERE 1!=1"

    def visit_true(self, expr: Any, **kw: Any) -> str:
        return "'t'"

    def visit_false(self, expr: Any, **kw: Any) -> str:
        return "'f'"

    def visit_sequence(self, sequence: Any, **kw: Any) -> str:
        return f"{self.preparer.format_sequence(sequence)}.NEXTVAL"

    def limit_clause(self, select: Any, **kw: Any) -> str:
        """仅 mysql 模式使用标准 ``LIMIT ... OFFSET`` 子句

        真机验证：GBase 8s 既不接受 ``LIMIT -1``，也不接受超出 BIGINT 范围的
        ``18446744073709551615``，故仅 OFFSET 时取 BIGINT 最大值；且 LIMIT / OFFSET
        值必须以字面量内联，否则 ``prepareStatement`` 会报语法错误。
        """
        if self.dialect.sqlmode != SQLMODE_MYSQL:
            return ""
        limit_clause = select._limit_clause
        offset_clause = select._offset_clause
        if limit_clause is None and offset_clause is None:
            return ""
        parts = []
        if limit_clause is not None:
            if select._simple_int_clause(limit_clause):
                limit_clause = limit_clause.render_literal_execute()
            parts.append("LIMIT " + self.process(limit_clause, **kw))
        elif offset_clause is not None:
            parts.append(f"LIMIT {_MYSQL_MAX_ROW_COUNT}")
        if offset_clause is not None:
            if select._simple_int_clause(offset_clause):
                offset_clause = offset_clause.render_literal_execute()
            parts.append("OFFSET " + self.process(offset_clause, **kw))
        return "\n " + " ".join(parts)

    def get_select_precolumns(self, select: Any, **kw: Any) -> str:
        """gbase 模式的 ``FIRST n SKIP m`` 需紧跟在 SELECT 之后

        GBase 8s 语法顺序为 ``SELECT [DISTINCT] FIRST n SKIP m ...``，
        FIRST/SKIP 在 DISTINCT 之后、列列表之前。
        """
        text = super().get_select_precolumns(select, **kw)
        if self.dialect.sqlmode != SQLMODE_GBASE or not select._has_row_limiting_clause:
            return text
        return text + self._gbase_row_limit_prefix(select)

    @staticmethod
    def _gbase_row_limit_prefix(select: Any) -> str:
        """生成 gbase 模式的 ``FIRST n SKIP m`` 前缀"""
        prefix = ""
        if select._limit is not None:
            prefix += f"FIRST {select._limit} "
        if select._offset:
            prefix += f"SKIP {select._offset} "
        return prefix

    def translate_select_structure(self, select_stmt: Any, **kwargs: Any) -> Any:
        """oracle 模式下把 LIMIT/OFFSET 改写为 ROWNUM 双层子查询"""
        select = select_stmt
        if self.dialect.sqlmode != SQLMODE_ORACLE:
            return select
        if getattr(select, "_gbase8s_rownum", False):
            return select
        if not select._has_row_limiting_clause:
            return select
        limit_clause, offset_clause = self._rownum_limit_clauses(select)
        if limit_clause is None and offset_clause is None:
            return select
        return self._build_rownum_select(select, limit_clause, offset_clause)

    @staticmethod
    def _rownum_limit_clauses(select: Any) -> Tuple[Any, Any]:
        """把 LIMIT/OFFSET 子句转为可参与 ROWNUM 比较的表达式"""
        limit_clause = select._limit_clause
        offset_clause = select._offset_clause
        if limit_clause is not None and select._simple_int_clause(limit_clause):
            limit_clause = limit_clause.render_literal_execute()
        if offset_clause is not None and select._simple_int_clause(offset_clause):
            offset_clause = offset_clause.render_literal_execute()
        return limit_clause, offset_clause

    def _build_rownum_select(
        self, orig_select: Any, limit_clause: Any, offset_clause: Any
    ) -> Any:
        """按 ROWNUM 限制结果集大小，并在需要时再包一层实现 OFFSET"""
        select = orig_select._generate()
        select._gbase8s_rownum = True  # type: ignore[attr-defined]
        inner_subquery = select.alias()
        limitselect = sql.select(*self._matching_columns(inner_subquery, orig_select))
        limitselect._gbase8s_rownum = True  # type: ignore[attr-defined]
        if limit_clause is not None:
            max_row = limit_clause
            if offset_clause is not None:
                max_row = max_row + offset_clause
            limitselect = limitselect.where(literal_column("ROWNUM") <= max_row)
        if offset_clause is None:
            return limitselect
        return self._wrap_rownum_offset(limitselect, orig_select, offset_clause)

    def _wrap_rownum_offset(
        self, limitselect: Any, orig_select: Any, offset_clause: Any
    ) -> Any:
        """在外层通过 ``ROWNUM`` 别名过滤 OFFSET"""
        limitselect = limitselect.add_columns(
            literal_column("ROWNUM").label(_ROWNUM_LABEL)
        )
        limit_subquery = limitselect.alias()
        offsetselect = sql.select(*self._matching_columns(limit_subquery, orig_select))
        offsetselect._gbase8s_rownum = True  # type: ignore[attr-defined]
        return offsetselect.where(literal_column(_ROWNUM_LABEL) > offset_clause)

    @staticmethod
    def _matching_columns(subquery: Any, orig_select: Any) -> List[Any]:
        """挑出与原查询结果列对应的子查询列（过滤掉 ROWNUM 包装列）

        按原查询 ``selected_columns`` 中各列的键名匹配子查询列，
        避免 ``corresponding_column`` 对匿名表达式列匹配失败导致丢列。
        """
        orig_keys = {col.key for col in orig_select.selected_columns}
        return [col for col in subquery.c if col.key in orig_keys]


class GBase8sDDLCompiler(compiler.DDLCompiler):
    """GBase 8s DDL 编译器

    自增主键在 GBase 8s 中必须写成 ``SERIAL`` / ``SERIAL8`` 列类型，
    且不能同时声明 ``NOT NULL``。
    """

    def get_column_specification(self, column: Any, **kwargs: Any) -> str:
        colspec = self.preparer.format_column(column)
        if self._is_serial_column(column):
            serial_type = (
                " SERIAL8"
                if isinstance(column.type, sqltypes.BigInteger)
                else " SERIAL"
            )
            return colspec + serial_type
        colspec += " " + self.dialect.type_compiler.process(
            column.type, type_expression=column
        )
        default = self.get_column_default_string(column)
        if default is not None:
            colspec += " DEFAULT " + default
        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    @staticmethod
    def _is_serial_column(column: Any) -> bool:
        """判断列是否为 GBase 8s 的自增列（SERIAL / SERIAL8）"""
        table = column.table
        if not column.primary_key or column is not table._autoincrement_column:
            return False
        return column.default is None or (
            isinstance(column.default, sa_schema.Sequence) and column.default.optional
        )


class _GBase8sSchemaReflection:
    """schema、表/视图/序列、视图定义等对象级反射"""

    default_schema_name: Optional[str]

    @reflection.cache
    def get_schema_names(self, connection: Any, **kw: Any) -> List[str]:
        """返回所有 schema（GBase 8s 以对象 owner 作为 schema）

        ``systables.owner`` 为空格填充的 CHAR 列，需去掉填充；空 owner
        表示无属主的系统对象，不作为 schema 暴露。
        """
        rows = connection.exec_driver_sql(dictionary.SCHEMA_NAMES_QUERY).fetchall()
        return [name for name in (str(row[0]).strip() for row in rows) if name]

    def _get_default_schema_name(self, connection: Any) -> Optional[str]:
        """返回当前连接用户（``USER`` 为空格填充的 CHAR 列，需去填充）"""
        name = connection.exec_driver_sql(dictionary.CURRENT_USER_QUERY).scalar()
        return str(name).strip() if name is not None else None

    @reflection.cache
    def has_table(
        self, connection: Any, table_name: str, schema: Optional[str] = None, **kw: Any
    ) -> bool:
        """判断表或视图是否存在"""
        schema = schema or self.default_schema_name
        count = connection.exec_driver_sql(
            dictionary.HAS_OBJECT_QUERY, (table_name, schema)
        ).scalar()
        return bool(count)

    @reflection.cache
    def get_table_names(
        self, connection: Any, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """返回 schema 下的所有表名"""
        return self._get_object_names(connection, schema, "T")

    @reflection.cache
    def get_view_names(
        self, connection: Any, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """返回 schema 下的所有视图名"""
        return self._get_object_names(connection, schema, "V")

    @reflection.cache
    def get_sequence_names(
        self, connection: Any, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """返回 schema 下的所有序列名"""
        return self._get_object_names(connection, schema, "Q")

    def _get_object_names(
        self, connection: Any, schema: Optional[str], object_type: str
    ) -> List[str]:
        """按对象类型（T/V/Q）查询对象名"""
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            dictionary.OBJECT_NAMES_QUERY, (schema, object_type)
        ).fetchall()
        return [str(row[0]).strip() for row in rows]

    def get_temp_table_names(
        self, connection: Any, schema: Optional[str] = None, **kw: Any
    ) -> List[str]:
        """返回临时表名

        GBase 8s 中 ``CREATE TEMP TABLE`` 创建的临时表在 ``systables`` 中
        同样以 ``tabtype = 'T'`` 出现且仅当前会话可见，无法通过静态系统目录
        查询与持久表可靠区分，因此始终返回空列表。

        需要临时表反射的场景建议在应用层通过命名约定（如 ``tmp_`` 前缀）自行管理。
        """
        return []

    def get_table_comment(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> Dict[str, Optional[str]]:
        """返回表注释

        GBase 8s 系统目录中不存在可查询的表注释存储，因此始终返回空注释。
        """
        return {"text": None}

    @reflection.cache
    def get_view_definition(
        self,
        connection: Any,
        view_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> str:
        """返回视图定义 SQL 文本"""
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            dictionary.VIEW_DEFINITION_QUERY, (view_name, schema)
        ).fetchall()
        return "".join(row[0] for row in rows)


class _GBase8sColumnReflection:
    """列级反射"""

    default_schema_name: Optional[str]
    get_pk_constraint: Any  # mixin from _GBase8sConstraintReflection

    @reflection.cache
    def get_columns(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:  # type: ignore[override]
        """返回表的列定义（名称、类型、可空、默认值、是否自增、是否主键）"""
        schema = schema or self.default_schema_name
        assert schema is not None
        rows = connection.exec_driver_sql(
            dictionary.COLUMNS_QUERY, (table_name, schema)
        ).fetchall()
        defaults = self._get_column_defaults(connection, table_name, schema)
        primary_keys = set(
            self.get_pk_constraint(connection, table_name, schema, **kw)[
                "constrained_columns"
            ]
        )
        return [self._build_column_info(row, defaults, primary_keys) for row in rows]

    def _get_column_defaults(
        self, connection: Any, table_name: str, schema: str
    ) -> Dict[int, str]:
        """读取列默认值；sysdefaults 为可选目录，缺失时返回空字典"""
        try:
            result = connection.exec_driver_sql(
                dictionary.COLUMN_DEFAULTS_QUERY, (table_name, schema)
            )
            return dictionary.parse_column_defaults(result.keys(), result.fetchall())
        except exc.DBAPIError as error:
            logger.debug("读取 sysdefaults 失败，忽略列默认值: %s", error)
            return {}

    @staticmethod
    def _build_column_info(
        row: Any, defaults: Dict[int, str], primary_keys: set
    ) -> Dict[str, Any]:
        """把单行列定义转换为 SQLAlchemy 反射结构"""
        name, colattr, collength, colno = row[0], int(row[1]), int(row[2]), int(row[3])
        extended_id = int(row[4]) if len(row) > 4 else 0
        not_nullable, coltype = divmod(colattr, 256)
        column_type = dictionary.parse_column_type(coltype, collength, extended_id)
        default = defaults.get(colno)
        if default is not None and coltype not in (0, 13, 15, 16):
            default = default.split()[-1]
        elif default is not None:
            default = f"'{default}'"
        return {
            "name": name,
            "type": column_type,
            "nullable": not not_nullable,
            "default": default,
            "autoincrement": coltype in (6, 18, 53),
            "primary_key": name in primary_keys,
            "comment": None,
        }


class _GBase8sConstraintReflection:
    """主键、外键、唯一约束与检查约束反射"""

    default_schema_name: Optional[str]
    get_indexes: Any  # mixin from _GBase8sIndexReflection

    @reflection.cache
    def get_pk_constraint(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> Dict[str, Any]:
        """返回主键约束名与主键列（按索引列顺序排列）"""
        schema = schema or self.default_schema_name
        assert schema is not None
        rows = connection.exec_driver_sql(
            dictionary.PRIMARY_KEY_QUERY, (table_name, schema)
        ).fetchall()
        if not rows:
            return {"constrained_columns": [], "name": None}
        column_names = column_name_map(connection, table_name, schema)
        parts = dictionary.get_index_part_columns(rows[0])
        return {
            "constrained_columns": [
                column_names[colno] for colno in parts if colno in column_names
            ],
            "name": str(rows[0][0]).strip(),
        }

    @reflection.cache
    def get_foreign_keys(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """返回外键约束列表"""
        schema = schema or self.default_schema_name
        assert schema is not None
        rows = connection.exec_driver_sql(
            dictionary.FOREIGN_KEYS_QUERY, (table_name, schema)
        ).fetchall()
        result: List[Dict[str, Any]] = []
        local_colnames = column_name_map(connection, table_name, schema)
        for row in rows:
            remote_table = row[dictionary.FK_REMOTE_TABLE]
            remote_owner = row[dictionary.FK_REMOTE_OWNER]

            if not remote_table or not remote_owner:
                continue

            constrname = str(row[dictionary.FK_CONSTRNAME]).strip()
            remote_table = str(remote_table).strip()
            remote_owner = str(remote_owner).strip()
            delrule = str(row[dictionary.FK_DELRULE]).strip()

            local_colnos = [
                abs(int(val))
                for val in (
                    row[i]
                    for i in range(
                        dictionary.FK_LOCAL_PARTS_START,
                        dictionary.FK_LOCAL_PARTS_END,
                    )
                )
                if val
            ]
            remote_colnos = [
                abs(int(val))
                for val in (
                    row[i]
                    for i in range(
                        dictionary.FK_REMOTE_PARTS_START,
                        dictionary.FK_REMOTE_PARTS_END,
                    )
                )
                if val
            ]

            constrained_columns = [
                local_colnames[cn] for cn in local_colnos if cn in local_colnames
            ]

            remote_colnames = column_name_map(connection, remote_table, remote_owner)
            referred_columns = [
                remote_colnames[cn] for cn in remote_colnos if cn in remote_colnames
            ]
            referred_schema = (
                remote_owner if remote_owner.upper() != schema.upper() else None
            )

            options: Dict[str, str] = {}
            if delrule == "C":
                options["ondelete"] = "CASCADE"

            result.append(
                {
                    "name": constrname,
                    "constrained_columns": constrained_columns,
                    "referred_schema": referred_schema,
                    "referred_table": remote_table,
                    "referred_columns": referred_columns,
                    "options": options,
                }
            )
        return result

    @reflection.cache
    def get_unique_constraints(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """返回唯一约束列表（由唯一索引派生）"""
        constraints = []
        for index in self.get_indexes(connection, table_name, schema, **kw):
            if index.get("unique"):
                constraints.append(
                    {
                        "name": index.get("name"),
                        "column_names": index.get("column_names", []),
                    }
                )
        return constraints

    @reflection.cache
    def get_check_constraints(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """返回检查约束列表"""
        schema = schema or self.default_schema_name
        rows = connection.exec_driver_sql(
            dictionary.CHECK_CONSTRAINTS_QUERY, (table_name, schema)
        ).fetchall()
        texts: Dict[str, str] = {}
        for constrname, checktext in rows:
            texts[constrname] = texts.get(constrname, "") + checktext
        return [
            {"name": name, "sqltext": text.rstrip()} for name, text in texts.items()
        ]


class _GBase8sIndexReflection:
    """索引反射"""

    default_schema_name: Optional[str]

    @reflection.cache
    def get_indexes(
        self,
        connection: Any,
        table_name: str,
        schema: Optional[str] = None,
        **kw: Any,
    ) -> List[Dict[str, Any]]:
        """返回索引列表（主键索引不入结果，其信息由主键约束提供）"""
        schema = schema or self.default_schema_name
        assert schema is not None
        rows = connection.exec_driver_sql(
            dictionary.INDEXES_QUERY, (table_name, schema)
        ).fetchall()
        column_names = column_name_map(connection, table_name, schema)
        indexes = []
        for row in rows:
            if str(row.constrtype).strip() == "P":
                continue
            parts = dictionary.get_index_part_columns(row)
            constraint_name = row.constrname
            indexes.append(
                {
                    "name": str(row.idxname).strip(),
                    "unique": str(row.idxtype).strip().lower() == "u",
                    "column_names": [
                        column_names[colno] for colno in parts if colno in column_names
                    ],
                    "dialect_options": {
                        "gbase8s_constraint_name": (
                            str(constraint_name).strip() if constraint_name else None
                        )
                    },
                }
            )
        return indexes


# DBINFO('session','isolation') 的可能返回值 -> SQLAlchemy 隔离级别名
_ISOLATION_CODES: Dict[str, str] = {
    "0": _READ_UNCOMMITTED,
    "1": _READ_COMMITTED,
    "2": _READ_COMMITTED,
    "3": _REPEATABLE_READ,
    "DIRTY READ": _READ_UNCOMMITTED,
    "COMMITTED READ": _READ_COMMITTED,
    "CURSOR STABILITY": _READ_COMMITTED,
    _REPEATABLE_READ: _REPEATABLE_READ,
}


class GBase8sDialect(  # type: ignore[override]
    _GBase8sSchemaReflection,
    _GBase8sColumnReflection,
    _GBase8sConstraintReflection,
    _GBase8sIndexReflection,
    default_dialect.DefaultDialect,
):
    """GBase 8s 方言（与具体 DBAPI 无关）

    通过 :attr:`sqlmode` 控制一库三模的 SQL 生成差异；该值可来自
    构造函数参数，也可由 :mod:`jaydebeapi` 从连接 URL 的 ``sqlmode`` 查询参数解析。
    """

    name = "gbase8s"

    # === 能力矩阵 ===
    supports_statement_cache = True
    supports_native_decimal = True
    supports_native_boolean = True
    supports_sequences = True
    supports_alter = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = False
    supports_empty_insert = False
    postfetch_lastrowid = False
    insert_returning = update_returning = delete_returning = False
    use_insertmanyvalues = False
    use_insertmanyvalues_wo_returning = False

    default_paramstyle = "qmark"
    max_identifier_length = 128

    ischema_names = ischema_names
    colspecs = colspecs

    statement_compiler = GBase8sCompiler
    ddl_compiler = GBase8sDDLCompiler
    type_compiler_cls = GBase8sTypeCompiler

    # 默认兼容模式；实例化时可由 sqlmode 参数或连接 URL 覆盖
    sqlmode = DEFAULT_SQLMODE

    def __init__(self, sqlmode: Optional[str] = None, **kwargs: Any) -> None:
        """初始化方言

        Args:
            sqlmode: 会话兼容模式（``oracle`` / ``gbase`` / ``mysql``），
                缺省时沿用类默认值（``oracle``）
            **kwargs: 传递给 :class:`DefaultDialect` 的标准方言参数

        Raises:
            ValueError: 当 sqlmode 取值不受支持时
        """
        super().__init__(**kwargs)
        self.sqlmode = normalize_sqlmode(sqlmode or self.sqlmode)

    def get_isolation_level_values(self, dbapi_conn: Any) -> List[str]:  # type: ignore[override]
        """返回方言支持的隔离级别名称（参数名与 SQLAlchemy 接口定义保持一致）"""
        return ["AUTOCOMMIT"] + list(ISOLATION_LEVELS)

    def set_isolation_level(self, dbapi_connection: Any, level: str) -> None:
        """设置会话隔离级别，``AUTOCOMMIT`` 通过 JDBC 自动提交开关实现"""
        normalized = level.upper()
        valid_levels = {"AUTOCOMMIT"} | set(ISOLATION_LEVELS)
        if normalized not in valid_levels:
            raise exc.ArgumentError(
                f"无效的事务隔离级别: '{level}'，可选值为 "
                + ", ".join(sorted(valid_levels))
            )
        if normalized == "AUTOCOMMIT":
            dbapi_connection.jconn.setAutoCommit(True)
            return
        dbapi_connection.jconn.setAutoCommit(False)
        statement = f"SET ISOLATION TO {ISOLATION_LEVELS[normalized]}"
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute(statement)
        finally:
            cursor.close()

    def get_isolation_level(self, dbapi_connection: Any) -> str:  # type: ignore[override]
        """返回当前会话隔离级别；部分版本/模式不支持查询时回退为 READ COMMITTED"""
        try:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(dictionary.ISOLATION_LEVEL_QUERY)
                row = cursor.fetchone()
            finally:
                cursor.close()
        except Exception as error:  # pylint: disable=broad-except
            logger.debug("查询会话隔离级别失败，回退为 READ COMMITTED: %s", error)
            return _READ_COMMITTED
        value = str(row[0]).strip().upper() if row else ""
        return _ISOLATION_CODES.get(value, _READ_COMMITTED)
