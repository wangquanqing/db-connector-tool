"""GBase 8s（JayDeBeApi + JDBC）方言测试模块

覆盖 `docs/gbase8s-dialect-design.md` 第 11 节验收清单的全部条目：
游标、时间戳装饰器、类型体系、collength 解析、能力矩阵、连接参数、
版本与 schema、隔离级别、系统表反射、环境自检与 SQL 编译快照。

全部用例均为离线用例（不连接真实数据库、不启动 JVM）。
为满足「单个类不超过 100 行」的编码约束，验收清单中的大类按职责拆分为多个小类。
"""

import os
import sys
import unittest
from datetime import datetime
from pathlib import Path
from unittest import TestCase, mock

from sqlalchemy import Column, MetaData, String, Table, create_engine, exc, select
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import sqltypes

from src.db_connector_tool.drivers.gbase8s import (
    LVARCHAR,
    SERIAL,
    SERIAL8,
    GBase8sCursor,
    GBase8sDialect,
    GBase8sDialect_jaydebeapi,
    ObTimestamp,
)
from src.db_connector_tool.drivers.gbase8s import base as gbase8s_base
from src.db_connector_tool.drivers.gbase8s import dictionary, provision


def _src_dir() -> Path:
    """返回项目 ``src`` 目录的绝对路径

    Returns:
        ``<repo>/src`` 路径对象
    """
    return Path(__file__).resolve().parents[2] / "src"


def _ensure_src_on_path() -> None:
    """把 ``src`` 加入 ``sys.path``

    使 ``pyproject.toml`` 声明的入口点字符串 ``db_connector_tool.drivers.gbase8s``
    能够被 ``create_engine`` 解析（该项目包未安装到运行环境时同样成立）。
    """
    src = str(_src_dir())
    if src not in sys.path:
        sys.path.insert(0, src)


def _patch_execute(case: TestCase, routes: dict) -> list:
    """把 ``base._execute`` 替换为按 SQL 关键字路由的假实现

    Args:
        case: TestCase 实例，用于注册清理动作
        routes: ``{SQL 关键字: 行列表}``，按插入顺序匹配

    Returns:
        记录 ``(template, schema, params)`` 的调用列表
    """
    calls = []

    def fake_execute(
        connection: object,
        template: str,
        schema: str | None,
        params: tuple | None = None,
        optional: bool = False,
    ) -> list:
        calls.append((template, schema, params))
        for keyword, rows in routes.items():
            if keyword in template:
                return [tuple(row) for row in rows]
        return []

    patcher = mock.patch.object(gbase8s_base, "_execute", side_effect=fake_execute)
    patcher.start()
    case.addCleanup(patcher.stop)
    return calls


class _FakeCursor:
    """假的 DBAPI 游标"""

    def __init__(self, row: tuple | None = None, error: Exception | None = None) -> None:
        self._row = row
        self._error = error
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        """记录语句并可选地抛出预置异常"""
        self.statements.append(statement)
        if self._error is not None:
            raise self._error

    def fetchone(self) -> tuple | None:
        """返回预置行"""
        return self._row

    def close(self) -> None:
        """关闭游标（空实现）"""


class _FakeDBAPIConnection:
    """假的 DBAPI 连接，仅提供 cursor()"""

    def __init__(self, row: tuple | None = None, error: Exception | None = None) -> None:
        self.cursor_obj = _FakeCursor(row, error)

    def cursor(self) -> _FakeCursor:
        """返回假游标"""
        return self.cursor_obj


# 索引/外键 SQL 中的 16 个 part 字段，测试数据用它补齐长度
_ZEROS = (0,) * 14

# get_columns 的假查询结果
_COLUMN_ROUTES = {
    "SELECT t1.colname, t1.coltype": [
        ("ID", 256 + 6, 4, 1),
        ("NAME", 13, 2560, 2),
        ("BIRTH", 10, 4, 3),
    ],
    "sysdefaultsexpr": [(2, "guest")],
    "syscolcomments": [("NAME", "名称")],
}


class TestGBase8sCursor(TestCase):
    """测试 GBase 8s 游标类"""

    def test_init(self):
        """测试游标初始化"""
        mock_connection = mock.Mock()
        mock_converters = {}
        with mock.patch("jaydebeapi.Cursor.__init__", return_value=None):
            cursor = GBase8sCursor(mock_connection, mock_converters)
            self.assertIsNotNone(cursor)


class TestObTimestamp(TestCase):
    """测试 GBase 8s 时间戳类型装饰器"""

    def test_process_bind_param_none(self):
        """测试 None 值的绑定参数处理"""
        timestamp = ObTimestamp()
        result = timestamp.process_bind_param(None, None)
        self.assertIsNone(result)

    def test_process_bind_param_non_datetime(self):
        """测试非 datetime 类型的绑定参数处理"""
        timestamp = ObTimestamp()
        result = timestamp.process_bind_param("not a datetime", None)
        self.assertEqual(result, "not a datetime")

    def test_process_bind_param_datetime(self):
        """测试 datetime 对象的绑定参数处理"""
        timestamp = ObTimestamp()
        test_datetime = datetime(2024, 1, 1, 12, 0, 0)
        with mock.patch("jpype.JClass") as mock_jclass:
            mock_timestamp_class = mock.Mock()
            mock_jclass.return_value = mock_timestamp_class
            mock_timestamp_class.valueOf.return_value = "java_timestamp"
            result = timestamp.process_bind_param(test_datetime, None)
            self.assertEqual(result, "java_timestamp")

    def test_process_result_value(self):
        """测试结果值处理"""
        timestamp = ObTimestamp()

        result = timestamp.process_result_value(None, None)
        self.assertIsNone(result)

        test_str = "2024-01-01 12:00:00"
        result = timestamp.process_result_value(test_str, None)
        self.assertIsInstance(result, datetime)

    def test_process_literal_param(self):
        """测试字面量参数处理"""
        timestamp = ObTimestamp()
        self.assertEqual(timestamp.process_literal_param(None, None), "NULL")
        self.assertEqual(timestamp.process_literal_param("x", None), "x")
        literal = timestamp.process_literal_param(datetime(2024, 1, 1, 0, 0, 0), None)
        self.assertTrue(literal.startswith("TIMESTAMP '2024-01-01 00:00:00"))


class TestGBase8sTypes(TestCase):
    """测试 GBase 8s 专属类型与类型码表"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()
        self.compiler = self.dialect.type_compiler

    def test_visit_lvarchar(self):
        """测试 LVARCHAR 带长度与不带长度的渲染"""
        self.assertEqual(self.compiler.process(LVARCHAR(64)), "LVARCHAR(64)")
        self.assertEqual(self.compiler.process(LVARCHAR()), "LVARCHAR")

    def test_visit_serial_types(self):
        """测试 SERIAL / SERIAL8 渲染"""
        self.assertEqual(self.compiler.process(SERIAL()), "SERIAL")
        self.assertEqual(self.compiler.process(SERIAL8()), "SERIAL8")

    def test_visit_nchar(self):
        """测试 NCHAR 渲染（沿用 SQLAlchemy 基类实现）"""
        self.assertEqual(self.compiler.process(sqltypes.NCHAR(10)), "NCHAR(10)")

    def test_ischema_names_codes(self):
        """测试类型码表覆盖设计文档约定的全部类型码"""
        expected = {
            0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 43, 52, 65,
        }
        self.assertEqual(set(GBase8sDialect.ischema_names), expected)

    def test_ischema_names_special_types(self):
        """测试自增与 LVARCHAR 类型码指向专属类型"""
        names = GBase8sDialect.ischema_names
        self.assertIs(names[6], SERIAL)
        self.assertIs(names[18], SERIAL8)
        self.assertIs(names[43], LVARCHAR)


class TestColumnTypeParsing(TestCase):
    """测试 syscolumns collength 解析"""

    def test_char_length(self):
        """测试 CHAR 长度取 collength"""
        column_type, not_null = dictionary.parse_column_type(0, 10)
        self.assertIsInstance(column_type, sqltypes.CHAR)
        self.assertEqual(column_type.length, 10)
        self.assertFalse(not_null)

    def test_not_null_flag(self):
        """测试 coltype // 256 == 1 表示 NOT NULL"""
        _, not_null = dictionary.parse_column_type(256 + 2, 4)
        self.assertTrue(not_null)

    def test_varchar_length(self):
        """测试 VARCHAR 长度取 collength // 256"""
        column_type, _ = dictionary.parse_column_type(13, 2560)
        self.assertIsInstance(column_type, sqltypes.VARCHAR)
        self.assertEqual(column_type.length, 10)

    def test_nvarchar_length(self):
        """测试 NVARCHAR 长度取 collength // 256"""
        column_type, _ = dictionary.parse_column_type(16, 512)
        self.assertIsInstance(column_type, sqltypes.NVARCHAR)
        self.assertEqual(column_type.length, 2)

    def test_lvarchar_small_length(self):
        """测试 collength <= 255 时 LVARCHAR 长度取原值"""
        column_type, _ = dictionary.parse_column_type(43, 100)
        self.assertIsInstance(column_type, LVARCHAR)
        self.assertEqual(column_type.length, 100)

    def test_lvarchar_large_length(self):
        """测试 collength > 255 时 LVARCHAR 长度取高字节"""
        column_type, _ = dictionary.parse_column_type(43, 512)
        self.assertEqual(column_type.length, 2)

    def test_decimal_precision_scale(self):
        """测试 DECIMAL 精度与小数位解析"""
        column_type, _ = dictionary.parse_column_type(5, (10 << 8) | 2)
        self.assertIsInstance(column_type, sqltypes.Numeric)
        self.assertEqual((column_type.precision, column_type.scale), (10, 2))

    def test_decimal_scale_255_is_integer(self):
        """测试小数位为 255 时按 INTEGER 处理"""
        column_type, _ = dictionary.parse_column_type(5, (10 << 8) | 255)
        self.assertIsInstance(column_type, sqltypes.INTEGER)

    def test_serial_is_autoincrement(self):
        """测试 SERIAL / SERIAL8 判定为自增"""
        column_type, _ = dictionary.parse_column_type(6, 4)
        self.assertIsInstance(column_type, SERIAL)
        self.assertTrue(dictionary.is_autoincrement_type(6))
        self.assertTrue(dictionary.is_autoincrement_type(18))
        self.assertFalse(dictionary.is_autoincrement_type(2))

    def test_unknown_type_code_warns(self):
        """测试未识别类型码告警并降级为 NullType"""
        with self.assertLogs(dictionary.logger.name, level="WARNING"):
            column_type, _ = dictionary.parse_column_type(200, 4)
        self.assertIsInstance(column_type, sqltypes.NullType)


class TestGBase8sDialectCapability(TestCase):
    """测试方言能力矩阵（防止回归）"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_identity(self):
        """测试方言名称、驱动类与参数风格"""
        self.assertEqual(self.dialect.name, "gbasedbt-sqli")
        self.assertEqual(self.dialect.driver, "com.gbasedbt.jdbc.Driver")
        self.assertEqual(self.dialect.default_paramstyle, "qmark")
        self.assertEqual(self.dialect.paramstyle, "qmark")

    def test_returning_disabled(self):
        """测试 RETURNING 全部关闭（jaydebeapi 无 cursor.var()）"""
        self.assertFalse(self.dialect.insert_returning)
        self.assertFalse(self.dialect.update_returning)
        self.assertFalse(self.dialect.delete_returning)

    def test_insertmanyvalues_disabled(self):
        """测试多行 VALUES 关闭（GBase 8s 不支持该语法）"""
        self.assertFalse(self.dialect.use_insertmanyvalues)
        self.assertFalse(self.dialect.use_insertmanyvalues_wo_returning)

    def test_other_capabilities(self):
        """测试其余能力开关"""
        self.assertFalse(self.dialect.postfetch_lastrowid)
        self.assertFalse(self.dialect.supports_sane_rowcount)
        self.assertFalse(self.dialect.supports_sane_multi_rowcount)
        self.assertTrue(self.dialect.supports_unicode_binds)
        self.assertTrue(self.dialect.supports_statement_cache)
        self.assertIsNone(self.dialect.description_encoding)
        self.assertTrue(self.dialect.supports_sequences)

    def test_oracle_inherited_attributes(self):
        """测试继承自 Oracle 基类的标识符规格"""
        self.assertEqual(self.dialect.max_identifier_length, 128)
        self.assertTrue(self.dialect.requires_name_normalize)

    def test_is_oracle_8_false(self):
        """测试禁用 Oracle 8 降级分支"""
        self.assertFalse(self.dialect._is_oracle_8)

    def test_offset_fetch_disabled(self):
        """测试关闭 OFFSET/FETCH（分页固定走 ROWNUM）"""
        self.assertFalse(self.dialect._supports_offset_fetch)


class TestGBase8sJdbcUrl(TestCase):
    """测试 JDBC URL 构建"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_full(self):
        """测试完整的 JDBC URL 构建"""
        mock_url_obj = mock.Mock(host="localhost", port=9088, database="testdb")
        result = self.dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost:9088/testdb")

    def test_no_port(self):
        """测试没有端口的 JDBC URL 构建"""
        mock_url_obj = mock.Mock(host="localhost", port=None, database="testdb")
        result = self.dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost/testdb")

    def test_no_database(self):
        """测试没有数据库的 JDBC URL 构建"""
        mock_url_obj = mock.Mock(host="localhost", port=9088, database=None)
        result = self.dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost:9088")

    def test_composite_database(self):
        """测试复合 database（含 GBASEDBTSERVER）的 JDBC URL 构建"""
        mock_url_obj = mock.Mock(
            host="localhost", port=9088, database="testdb:GBASEDBTSERVER=srv"
        )
        result = self.dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(
            result, "jdbc:gbasedbt-sqli://localhost:9088/testdb:GBASEDBTSERVER=srv"
        )


class TestGBase8sConnectArgs(TestCase):
    """测试连接参数字典构建"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_full(self):
        """测试完整的连接参数构建"""
        mock_url_obj = mock.Mock(
            username="user", password="pass", query={"param1": "value1"}
        )
        result = self.dialect._build_connect_args(mock_url_obj)
        self.assertEqual(result["user"], "user")
        self.assertEqual(result["password"], "pass")
        self.assertEqual(result["param1"], "value1")
        self.assertEqual(result["rewriteBatchedStatements"], "true")

    def test_minimal(self):
        """测试最小连接参数构建"""
        mock_url_obj = mock.Mock(username=None, password=None, query=None)
        result = self.dialect._build_connect_args(mock_url_obj)
        self.assertNotIn("user", result)
        self.assertNotIn("password", result)
        self.assertEqual(result["rewriteBatchedStatements"], "true")

    def test_non_string_query(self):
        """测试非字符串查询参数"""
        mock_url_obj = mock.Mock(username=None, password=None, query={"param1": 123})
        result = self.dialect._build_connect_args(mock_url_obj)
        self.assertEqual(result["param1"], "123")

    def test_with_rewrite(self):
        """测试已存在 rewriteBatchedStatements 参数"""
        mock_url_obj = mock.Mock(
            username=None, password=None, query={"rewriteBatchedStatements": "false"}
        )
        result = self.dialect._build_connect_args(mock_url_obj)
        self.assertEqual(result["rewriteBatchedStatements"], "false")

    def test_create_connect_args(self):
        """测试 create_connect_args 方法"""
        mock_url = mock.Mock()

        with mock.patch("sqlalchemy.engine.url.make_url", return_value=mock_url):
            with mock.patch.object(
                self.dialect, "_build_jdbc_url", return_value="jdbc:url"
            ):
                with mock.patch.object(
                    self.dialect, "_build_connect_args", return_value={"user": "test"}
                ):
                    with mock.patch.object(
                        self.dialect, "_handle_jar_path"
                    ) as mock_handle:
                        args, kwargs = self.dialect.create_connect_args(mock_url)
                        self.assertEqual(args, ())
                        self.assertEqual(kwargs["jclassname"], "com.gbasedbt.jdbc.Driver")
                        self.assertEqual(kwargs["url"], "jdbc:url")
                        self.assertEqual(kwargs["driver_args"], {"user": "test"})
                        mock_handle.assert_called_once()


class TestGBase8sJarPath(TestCase):
    """测试 JDBC 驱动 JAR 处理"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_handle_jar_path_found(self):
        """测试找到 JAR 时写入 jars 参数"""
        kwargs = {}
        with mock.patch.object(provision, "find_jdbc_jar", return_value="C:/jars/g.db"):
            self.dialect._handle_jar_path(kwargs)
        self.assertEqual(kwargs["jars"], "C:/jars/g.db")

    def test_handle_jar_path_missing_warns(self):
        """测试未找到 JAR 时告警并把 jars 置为 None"""
        kwargs = {}
        with mock.patch.object(provision, "find_jdbc_jar", return_value=None):
            with self.assertWarns(UserWarning):
                self.dialect._handle_jar_path(kwargs)
        self.assertIsNone(kwargs["jars"])


class TestGBase8sConnectionHooks(TestCase):
    """测试 DBAPI 导入、回滚与断连识别"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_import_dbapi(self):
        """测试 import_dbapi 方法"""
        self.assertIsNotNone(self.dialect.import_dbapi())

    def test_do_rollback_calls_dbapi(self):
        """测试 do_rollback 真实调用 DBAPI 的 rollback"""
        dbapi_connection = mock.Mock()
        self.dialect.do_rollback(dbapi_connection)
        dbapi_connection.rollback.assert_called_once_with()

    def test_do_rollback_swallows_error(self):
        """测试回滚失败时降级不抛异常"""
        dbapi_connection = mock.Mock()
        dbapi_connection.rollback.side_effect = RuntimeError("rollback failed")
        self.dialect.do_rollback(dbapi_connection)
        dbapi_connection.rollback.assert_called_once_with()

    def test_is_disconnect(self):
        """测试 is_disconnect 方法"""
        indicators = [
            "Connection closed",
            "Socket closed",
            "Broken pipe",
            "Connection reset",
            "JDBC connection",
            "Network error",
            "java.sql.SQLNonTransientConnectionException",
        ]
        for indicator in indicators:
            self.assertTrue(self.dialect.is_disconnect(Exception(indicator), None, None))

        self.assertFalse(self.dialect.is_disconnect(Exception("Other"), None, None))


class TestGBase8sVersionAndSchema(TestCase):
    """测试服务端版本与默认 schema"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_get_default_schema_name(self):
        """测试 get_default_schema_name 方法"""
        mock_connection = mock.Mock()
        mock_connection.engine.url.username = "test_user"
        self.assertEqual(
            self.dialect.get_default_schema_name(mock_connection), "TEST_USER"
        )

        mock_connection.engine.url.username = None
        self.assertIsNone(self.dialect.get_default_schema_name(mock_connection))

    def test_get_default_schema_name_internal(self):
        """测试 _get_default_schema_name 方法"""
        mock_connection = mock.Mock()
        mock_connection.engine.url.username = "test_user"
        with mock.patch.object(
            self.dialect, "get_default_schema_name", return_value="TEST_USER"
        ) as mock_get:
            result = self.dialect._get_default_schema_name(mock_connection)
            self.assertEqual(result, "TEST_USER")
            mock_get.assert_called_once_with(mock_connection)

    def test_get_server_version_info_success(self):
        """测试成功获取服务器版本信息"""
        mock_connection = mock.Mock()
        mock_result = mock.Mock()
        mock_result.scalar.return_value = "GBase8sV8.6.2_2.0"
        mock_connection.execute.return_value = mock_result
        result = self.dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6, 2))

    def test_get_server_version_info_simple(self):
        """测试简单版本格式"""
        mock_connection = mock.Mock()
        mock_result = mock.Mock()
        mock_result.scalar.return_value = "GBase8s 8.6"
        mock_connection.execute.return_value = mock_result
        result = self.dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6))

    def test_get_server_version_info_no_match(self):
        """测试无法匹配版本格式"""
        mock_connection = mock.Mock()
        mock_result = mock.Mock()
        mock_result.scalar.return_value = "Unknown version string"
        mock_connection.execute.return_value = mock_result
        result = self.dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)

    def test_get_server_version_info_dbapi_error(self):
        """测试查询失败时返回 None"""
        mock_connection = mock.Mock()
        mock_connection.execute.side_effect = exc.DBAPIError(
            "select 1", None, Exception("boom")
        )
        result = self.dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)


class TestGBase8sIsolationLevel(TestCase):
    """测试隔离级别的读取与白名单"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_get_isolation_level_values(self):
        """测试隔离级别白名单（不含 AUTOCOMMIT）"""
        self.assertEqual(
            self.dialect.get_isolation_level_values(None),
            (
                "DIRTY READ",
                "COMMITTED READ",
                "COMMITTED READ LAST COMMITTED",
                "CURSOR STABILITY",
                "REPEATABLE READ",
            ),
        )

    def test_get_isolation_level(self):
        """测试读取隔离级别"""
        connection = _FakeDBAPIConnection(row=("COMMITTED READ",))
        self.assertEqual(self.dialect.get_isolation_level(connection), "COMMITTED READ")
        self.assertEqual(connection.cursor_obj.statements, [dictionary.ISOLATION_LEVEL])

    def test_get_isolation_level_returns_none_on_error(self):
        """测试读取失败时返回 None"""
        connection = _FakeDBAPIConnection(error=RuntimeError("boom"))
        self.assertIsNone(self.dialect.get_isolation_level(connection))

    def test_get_isolation_level_empty_row(self):
        """测试无结果行时返回 None"""
        self.assertIsNone(self.dialect.get_isolation_level(_FakeDBAPIConnection()))

    def test_get_default_isolation_level_reads_value(self):
        """测试默认隔离级别优先取当前值"""
        with mock.patch.object(
            self.dialect, "get_isolation_level", return_value="CURSOR STABILITY"
        ):
            self.assertEqual(
                self.dialect.get_default_isolation_level(None), "CURSOR STABILITY"
            )

    def test_get_default_isolation_level_fallback(self):
        """测试读取失败时回落到 COMMITTED READ"""
        with mock.patch.object(self.dialect, "get_isolation_level", return_value=None):
            self.assertEqual(
                self.dialect.get_default_isolation_level(None), "COMMITTED READ"
            )


class TestGBase8sIsolationLevelWrite(TestCase):
    """测试隔离级别的设置与注入防护"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_set_isolation_level(self):
        """测试设置隔离级别发出的 SQL"""
        connection = _FakeDBAPIConnection()
        self.dialect.set_isolation_level(connection, "committed read")
        self.assertEqual(
            connection.cursor_obj.statements, ["SET ISOLATION TO COMMITTED READ"]
        )

    def test_set_isolation_level_invalid(self):
        """测试非法隔离级别抛出 ArgumentError 且不下发 SQL"""
        connection = _FakeDBAPIConnection()
        with self.assertRaises(exc.ArgumentError):
            self.dialect.set_isolation_level(connection, "READ UNCOMMITTED")
        self.assertEqual(connection.cursor_obj.statements, [])


class TestGBase8sDictionarySql(TestCase):
    """测试系统目录 SQL 常量与 schema 前缀格式化"""

    def test_format_sql_with_schema(self):
        """测试 schema 前缀替换"""
        sql_text = dictionary.format_sql(dictionary.HAS_TABLE, "mydb")
        self.assertIn("from mydb.systables", sql_text)
        self.assertNotIn("{p}", sql_text)

    def test_format_sql_without_schema(self):
        """测试 schema 为 None 时不加前缀"""
        sql_text = dictionary.format_sql(dictionary.HAS_TABLE, None)
        self.assertIn("from systables", sql_text)
        self.assertNotIn("{p}", sql_text)

    def test_table_names_filters_system_tables(self):
        """测试表名/序列/临时表查询的过滤条件"""
        self.assertIn("tabid > 99", dictionary.TABLE_NAMES)
        self.assertIn("tabid > 99", dictionary.SEQUENCE_NAMES)
        self.assertIn("flags = 28672", dictionary.TEMP_TABLE_NAMES)

    def test_index_parts_have_sixteen_columns(self):
        """测试索引/主键 SQL 含 16 个 part 字段"""
        for template in (dictionary.PK_CONSTRAINT, dictionary.INDEXES):
            self.assertIn("part1", template)
            self.assertIn("part16", template)

    def test_isolation_level_sql(self):
        """测试隔离级别查询取自 sysmaster"""
        self.assertEqual(
            dictionary.ISOLATION_LEVEL,
            "select scs_isolationlevel from sysmaster:syssqlcurses",
        )


class TestGBase8sNameReflection(TestCase):
    """测试表/序列名称反射"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_has_table_true(self):
        """测试 has_table 命中"""
        _patch_execute(self, {"count(*)": [[1]]})
        self.assertTrue(self.dialect.has_table(None, "t1"))

    def test_has_table_false(self):
        """测试 has_table 未命中并透传 schema/参数"""
        calls = _patch_execute(self, {"count(*)": [[0]]})
        self.assertFalse(self.dialect.has_table(None, "t1"))
        self.assertIsNone(calls[0][1])
        self.assertEqual(calls[0][2], ("t1",))

    def test_has_sequence(self):
        """测试 has_sequence"""
        calls = _patch_execute(self, {"count(*)": [[1]]})
        self.assertTrue(self.dialect.has_sequence(None, "seq1"))
        self.assertIn("tabtype = 'Q'", calls[0][0])

    def test_get_table_names(self):
        """测试表名列表（去空白 + schema 透传）"""
        calls = _patch_execute(self, {"tabtype = ? AND tabid > 99": [["t1 "], ["t2"]]})
        self.assertEqual(self.dialect.get_table_names(None, "mydb"), ["t1", "t2"])
        self.assertEqual(calls[0][1], "mydb")
        self.assertEqual(calls[0][2], ("T",))


class TestGBase8sTempAndSchemaReflection(TestCase):
    """测试视图/临时表/序列/schema 名称反射"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_get_view_names(self):
        """测试视图名列表"""
        calls = _patch_execute(self, {"tabtype = ? AND tabid > 99": [["v1"]]})
        self.assertEqual(self.dialect.get_view_names(None), ["v1"])
        self.assertEqual(calls[0][2], ("V",))

    def test_get_temp_table_names(self):
        """测试临时表名列表"""
        _patch_execute(self, {"flags = 28672": [["tmp1"]]})
        self.assertEqual(self.dialect.get_temp_table_names(None), ["tmp1"])

    def test_get_sequence_names(self):
        """测试序列名列表"""
        _patch_execute(self, {"tabtype = 'Q' AND tabid > 99": [["seq1"]]})
        self.assertEqual(self.dialect.get_sequence_names(None), ["seq1"])

    def test_get_schema_names(self):
        """测试 schema（数据库）名列表"""
        calls = _patch_execute(self, {"SYSDBSLOCALE": [["db1"]]})
        self.assertEqual(self.dialect.get_schema_names(None), ["db1"])
        self.assertIsNone(calls[0][1])


class TestGBase8sColumnReflection(TestCase):
    """测试列反射"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_get_columns(self):
        """测试列反射的解析结果"""
        _patch_execute(self, _COLUMN_ROUTES)
        columns = self.dialect.get_columns(None, "t1")

        self.assertEqual([c["name"] for c in columns], ["ID", "NAME", "BIRTH"])
        self.assertIsInstance(columns[0]["type"], SERIAL)
        self.assertTrue(columns[0]["autoincrement"])
        self.assertFalse(columns[0]["nullable"])
        self.assertEqual(columns[1]["default"], "guest")
        self.assertEqual(columns[1]["comment"], "名称")
        self.assertEqual(columns[1]["type"].length, 10)
        self.assertIsInstance(columns[2]["type"], sqltypes.TIMESTAMP)

    def test_get_columns_missing_table(self):
        """测试表不存在时抛出 NoSuchTableError"""
        _patch_execute(self, {})
        with self.assertRaises(exc.NoSuchTableError):
            self.dialect.get_columns(None, "missing")

    def test_get_columns_optional_metadata_missing(self):
        """测试可选目录缺失时默认值与注释降级为 None"""
        _patch_execute(self, {"SELECT t1.colname, t1.coltype": [("ID", 2, 4, 1)]})
        columns = self.dialect.get_columns(None, "t1")
        self.assertIsNone(columns[0]["default"])
        self.assertIsNone(columns[0]["comment"])
        self.assertFalse(columns[0]["autoincrement"])
        self.assertTrue(columns[0]["nullable"])


class TestGBase8sColumnMetadata(TestCase):
    """测试表注释与视图定义反射"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_get_table_comment(self):
        """测试表注释反射"""
        _patch_execute(self, {"syscomments": [("订单表  ",)]})
        self.assertEqual(self.dialect.get_table_comment(None, "t1"), {"text": "订单表"})

    def test_get_table_comment_empty(self):
        """测试无注释时返回默认值"""
        _patch_execute(self, {})
        self.assertEqual(self.dialect.get_table_comment(None, "t1"), {"text": None})

    def test_get_view_definition_concatenates_rows(self):
        """测试视图定义按 seqno 拼接全部行且保留空白"""
        _patch_execute(self, {"sysviews": [("SELECT a ",), ("FROM t1",)]})
        self.assertEqual(
            self.dialect.get_view_definition(None, "v1"), "SELECT a FROM t1"
        )

    def test_get_view_definition_missing(self):
        """测试视图不存在时抛出 NoSuchTableError"""
        _patch_execute(self, {})
        with self.assertRaises(exc.NoSuchTableError):
            self.dialect.get_view_definition(None, "v1")


class TestGBase8sPkAndIndexReflection(TestCase):
    """测试主键、索引与唯一约束反射"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def test_get_pk_constraint(self):
        """测试主键约束反射"""
        routes = {
            "t3.constrtype = 'P'": [(7, "pk_t1", 1, 2, 0, *_ZEROS)],
            "colno IN (": [(1, "ID"), (2, "NAME")],
        }
        _patch_execute(self, routes)
        result = self.dialect.get_pk_constraint(None, "t1")
        self.assertEqual(result["name"], "pk_t1")
        self.assertEqual(result["constrained_columns"], ["ID", "NAME"])

    def test_get_pk_constraint_empty(self):
        """测试无主键时返回默认值"""
        _patch_execute(self, {})
        self.assertEqual(
            self.dialect.get_pk_constraint(None, "t1"),
            {"name": None, "constrained_columns": []},
        )

    def test_get_indexes(self):
        """测试索引反射：跳过主键索引、识别唯一索引"""
        routes = {
            "LEFT JOIN": [
                ("idx_plain", "D", 0, 2, 0, *_ZEROS, " ", "-"),
                ("idx_unique", "U", 0, 3, 0, *_ZEROS, "-", "-"),
                ("idx_pk", "U", 1, 1, 0, *_ZEROS, "P", "pk_t1"),
            ],
            "SELECT t2.tabid": [(7,)],
            "colno IN (": [(2, "NAME"), (3, "AGE")],
        }
        _patch_execute(self, routes)
        indexes = self.dialect.get_indexes(None, "t1")

        self.assertEqual([i["name"] for i in indexes], ["idx_plain", "idx_unique"])
        self.assertFalse(indexes[0]["unique"])
        self.assertEqual(indexes[0]["column_names"], ["NAME"])
        self.assertTrue(indexes[1]["unique"])
        self.assertEqual(indexes[1]["column_names"], ["AGE"])
        self.assertEqual(indexes[1]["dialect_options"], {})

    def test_get_indexes_empty(self):
        """测试无索引时返回空列表"""
        _patch_execute(self, {})
        self.assertEqual(self.dialect.get_indexes(None, "t1"), [])

    def test_get_unique_constraints(self):
        """测试唯一约束由唯一索引表达"""
        routes = {
            "LEFT JOIN": [
                ("idx_plain", "D", 0, 2, 0, *_ZEROS, " ", "-"),
                ("idx_unique", "U", 0, 3, 0, *_ZEROS, "-", "-"),
            ],
            "SELECT t2.tabid": [(7,)],
            "colno IN (": [(2, "NAME"), (3, "AGE")],
        }
        _patch_execute(self, routes)
        self.assertEqual(
            self.dialect.get_unique_constraints(None, "t1"),
            [{"name": "idx_unique", "column_names": ["AGE"]}],
        )


class TestGBase8sForeignKeyReflection(TestCase):
    """测试外键与检查约束反射"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()

    def _fk_routes(self, delrule):
        """构造外键反射的假查询结果

        Args:
            delrule: sysreferences.delrule 取值

        Returns:
            路由字典
        """
        return {
            "t3.constrtype = 'R'": [(7, "fk_idx", "fk_t1", 11, 2, 0, *_ZEROS)],
            "sysreferences": [("t2", 9, delrule, 5, 0, *_ZEROS)],
            "colno IN (": [(2, "PID"), (5, "ID")],
        }

    def test_get_foreign_keys(self):
        """测试外键反射与 delrule=C 映射为 CASCADE"""
        _patch_execute(self, self._fk_routes("C"))
        foreign_keys = self.dialect.get_foreign_keys(None, "t1")

        self.assertEqual(len(foreign_keys), 1)
        item = foreign_keys[0]
        self.assertEqual(item["name"], "fk_t1")
        self.assertEqual(item["constrained_columns"], ["PID"])
        self.assertEqual(item["referred_table"], "t2")
        self.assertEqual(item["referred_columns"], ["ID"])
        self.assertEqual(item["options"], {"ondelete": "CASCADE"})

    def test_get_foreign_keys_set_null(self):
        """测试 delrule=N 映射为 SET NULL"""
        _patch_execute(self, self._fk_routes("N"))
        foreign_keys = self.dialect.get_foreign_keys(None, "t1")
        self.assertEqual(foreign_keys[0]["options"], {"ondelete": "SET NULL"})

    def test_get_foreign_keys_unknown_rule(self):
        """测试 delrule 为其他值时 ondelete 为 None"""
        _patch_execute(self, self._fk_routes("X"))
        foreign_keys = self.dialect.get_foreign_keys(None, "t1")
        self.assertEqual(foreign_keys[0]["options"], {"ondelete": None})

    def test_get_foreign_keys_referred_missing(self):
        """测试被引用信息缺失时跳过该外键"""
        _patch_execute(self, {"t3.constrtype = 'R'": [(7, "fk", "fk_t1", 11, 2, 0, *_ZEROS)]})
        self.assertEqual(self.dialect.get_foreign_keys(None, "t1"), [])

    def test_get_check_constraints(self):
        """测试检查约束按 seqno 拼接同组文本"""
        routes = {
            "syschecks": [
                ("ck_age", "age > 0 ", 1),
                ("ck_age", "AND age < 200", 2),
                ("ck_name", "name IS NOT NULL", 1),
            ]
        }
        _patch_execute(self, routes)
        self.assertEqual(
            self.dialect.get_check_constraints(None, "t1"),
            [
                {"name": "ck_age", "sqltext": "age > 0 AND age < 200"},
                {"name": "ck_name", "sqltext": "name IS NOT NULL"},
            ],
        )

    def test_get_check_constraints_missing(self):
        """测试检查约束目录缺失时返回空列表"""
        _patch_execute(self, {})
        self.assertEqual(self.dialect.get_check_constraints(None, "t1"), [])


class TestGBase8sProvision(TestCase):
    """测试环境自检与验收辅助"""

    def test_jar_search_dir(self):
        """测试 JAR 搜索目录位于用户配置目录下"""
        self.assertTrue(str(provision.jar_search_dir()).endswith(provision.JARS_DIR_NAME))

    def test_find_jdbc_jar_from_env(self):
        """测试环境变量指定的 JAR 存在时被采用"""
        with mock.patch.dict(os.environ, {provision.JAR_ENV_VAR: __file__}):
            self.assertEqual(provision.find_jdbc_jar(), __file__)

    def test_find_jdbc_jar_not_found(self):
        """测试未找到 JAR 时返回 None"""
        env = {provision.JAR_ENV_VAR: str(Path(__file__).with_name("no_such.jar"))}
        missing_dir = Path(__file__).with_name("no_such_dir")
        with mock.patch.dict(os.environ, env):
            with mock.patch.object(provision, "jar_search_dir", return_value=missing_dir):
                self.assertIsNone(provision.find_jdbc_jar())

    def test_probe_sql(self):
        """测试探测 SQL"""
        self.assertIn("dbinfo('version','full')", provision.probe_sql())

    def test_create_test_table_sql(self):
        """测试建测试表 DDL"""
        sql_text = provision.create_test_table_sql("dbct_probe")
        self.assertIn("CREATE TABLE dbct_probe", sql_text)
        self.assertIn("SERIAL PRIMARY KEY", sql_text)
        self.assertIn("VARCHAR(64)", sql_text)

    def test_drop_test_table_sql(self):
        """测试删测试表 DDL"""
        self.assertEqual(
            provision.drop_test_table_sql("dbct_probe"), "DROP TABLE dbct_probe"
        )

    def test_invalid_identifier(self):
        """测试非法表名被拒绝"""
        for name in ("t1; DROP TABLE t2", "1table", ""):
            with self.assertRaises(ValueError):
                provision.create_test_table_sql(name)


class TestGBase8sCompileSnapshot(TestCase):
    """测试 SQL 编译快照（create_engine 与 DDL/DML/分页）"""

    def setUp(self):
        self.dialect = GBase8sDialect_jaydebeapi()
        self.table = Table(
            "t1",
            MetaData(),
            Column("id", SERIAL, primary_key=True),
            Column("name", LVARCHAR(64)),
            Column("big", SERIAL8),
            Column("created", ObTimestamp),
            Column("note", String(10)),
        )

    def test_create_engine_resolves_registered_dialect(self):
        """测试 create_engine 能按注册名解析方言（离线，不连库）"""
        _ensure_src_on_path()
        engine = create_engine("jdbcgbase8s://user:pwd@host:9088/testdb")
        self.assertEqual(type(engine.dialect).__name__, "GBase8sDialect_jaydebeapi")
        self.assertEqual(engine.dialect.name, "gbasedbt-sqli")
        self.assertEqual(engine.dialect.paramstyle, "qmark")

    def test_create_table_snapshot(self):
        """测试建表 DDL 快照"""
        sql_text = str(CreateTable(self.table).compile(dialect=self.dialect))
        self.assertIn("id SERIAL NOT NULL", sql_text)
        self.assertIn("name LVARCHAR(64)", sql_text)
        self.assertIn("big SERIAL8", sql_text)
        self.assertIn("created TIMESTAMP", sql_text)
        self.assertIn("PRIMARY KEY (id)", sql_text)

    def test_insert_snapshot_without_returning(self):
        """测试 INSERT 使用 ? 占位符且不含 RETURNING"""
        statement = self.table.insert().values(name="x")
        sql_text = str(statement.compile(dialect=self.dialect))
        self.assertIn("INSERT INTO t1", sql_text)
        self.assertIn("?", sql_text)
        self.assertNotIn("RETURNING", sql_text.upper())

    def test_pagination_uses_rownum(self):
        """测试分页走 ROWNUM 包装而非 OFFSET/FETCH"""
        statement = select(self.table).limit(5).offset(2)
        sql_text = str(statement.compile(dialect=self.dialect))
        self.assertIn("ROWNUM", sql_text)
        self.assertIn("ora_rn", sql_text)
        self.assertNotIn("FETCH FIRST", sql_text.upper())

    def test_limit_only_uses_rownum(self):
        """测试仅 limit 时也走 ROWNUM 包装"""
        sql_text = str(select(self.table).limit(5).compile(dialect=self.dialect))
        self.assertIn("ROWNUM", sql_text)
        self.assertNotIn("FETCH FIRST", sql_text.upper())


if __name__ == "__main__":
    unittest.main()