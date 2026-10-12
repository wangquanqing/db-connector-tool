"""GBase 8s（JayDeBeApi + JDBC）方言单元测试

覆盖四条主线：

1. 一库三模（``oracle`` / ``gbase`` / ``mysql``）的 SQL 生成差异；
2. 连接 URL 到 :func:`jaydebeapi.connect` 参数的翻译与 jar 定位；
3. 数据字典（``syscolumns`` 等）的解析逻辑；
4. 系统目录反射（表 / 列 / 约束 / 索引）在模拟连接下的行为。
"""

# pylint: disable=protected-access

import os
import tempfile
import unittest
from collections import namedtuple
from pathlib import Path
from unittest.mock import MagicMock, patch

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Integer,
    MetaData,
    Sequence,
    String,
    Table,
    Text,
    Time,
    column,
    create_engine,
    exc,
    literal,
    select,
    table,
    true,
)
from sqlalchemy import types as sqltypes
from sqlalchemy.dialects import registry
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateTable
from sqlalchemy.types import NULLTYPE

from src.db_connector_tool.drivers.gbase8s import (
    DEFAULT_SQLMODE,
    SUPPORTED_SQLMODES,
    GBase8sDialect_jaydebeapi,
    dictionary,
    normalize_sqlmode,
)
from src.db_connector_tool.drivers.gbase8s import jar_downloader
from src.db_connector_tool.drivers.gbase8s.jaydebeapi import (
    JDBC_DRIVER_CLASS,
    _first_jar_in,
    _is_gbase_jar,
    _jar_search_dirs,
    auto_download_dir,
    find_jdbc_jar,
)
from src.db_connector_tool.drivers.gbase8s.types import LVARCHAR, MONEY, SERIAL, SERIAL8
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

BASE_URL = (
    "gbase8s://127.0.0.1:9088/db_station:GBASEDBTSERVER=gbase01"
    "?user=cvicse&password=secret"
)


def build_dialect(sqlmode: str) -> GBase8sDialect_jaydebeapi:
    """构造指定兼容模式的方言实例（不建立连接）"""
    return GBase8sDialect_jaydebeapi(sqlmode=sqlmode)


def compile_sql(statement: object, dialect: GBase8sDialect_jaydebeapi) -> str:
    """编译语句并返回 SQL 文本（换行折叠为单空格，便于断言）"""
    text = str(statement.compile(dialect=dialect))
    return " ".join(text.split())


class TestSqlModeNormalization(unittest.TestCase):
    """sqlmode 取值规范化"""

    def test_default_when_empty(self) -> None:
        """未提供 sqlmode 时取默认值 oracle"""
        self.assertEqual(DEFAULT_SQLMODE, "oracle")
        self.assertEqual(normalize_sqlmode(None), DEFAULT_SQLMODE)
        self.assertEqual(normalize_sqlmode("  "), DEFAULT_SQLMODE)

    def test_case_insensitive(self) -> None:
        """sqlmode 大小写不敏感"""
        for mode in SUPPORTED_SQLMODES:
            self.assertEqual(normalize_sqlmode(mode.upper()), mode)

    def test_invalid_raises_value_error(self) -> None:
        """非法 sqlmode 抛出 ValueError，并提示可选值"""
        with self.assertRaises(ValueError) as context:
            normalize_sqlmode("postgres")
        self.assertIn("不支持的 sqlmode", str(context.exception))

    def test_dialect_rejects_invalid_sqlmode(self) -> None:
        """方言构造时即校验 sqlmode"""
        with self.assertRaises(ValueError):
            build_dialect("sqlserver")


class TestThreeSqlModes(unittest.TestCase):
    """一库三模的 SQL 生成差异"""

    def setUp(self) -> None:
        self.table = table("t", column("a", Integer))

    def test_pseudo_table(self) -> None:
        """无 FROM 的查询按模式补伪表

        oracle 模式允许省略 FROM（仍补 DUAL 以保持 Oracle 习惯）；
        gbase / mysql 模式必须给出 FROM，且 mysql 模式没有 DUAL，
        只能用 ``sysmaster:sysdual``（真机验证结论）。
        """
        expected = {
            "oracle": "SELECT ? AS anon_1 FROM DUAL",
            "gbase": "SELECT ? AS anon_1 FROM sysmaster:sysdual",
            "mysql": "SELECT ? AS anon_1 FROM sysmaster:sysdual",
        }
        for mode, sql in expected.items():
            with self.subTest(sqlmode=mode):
                self.assertEqual(
                    compile_sql(select(literal(1)), build_dialect(mode)), sql
                )

    def test_limit_without_offset(self) -> None:
        """:meth:`Select.limit` 在三种模式下生成不同语法

        注意：简单整型 LIMIT/OFFSET 会以 ``render_literal_execute()`` 内联，
        因此普通 ``compile()`` 输出占位符 ``__[POSTCOMPILE_param_N]``，
        带 ``render_postcompile=True`` 才呈现字面量。
        """
        expected = {
            "oracle": "SELECT anon_1.a FROM (SELECT t.a AS a FROM t) AS anon_1 "
            "WHERE ROWNUM <= __[POSTCOMPILE_param_1]",
            "gbase": "SELECT FIRST 10 t.a FROM t",
            "mysql": "SELECT t.a FROM t LIMIT __[POSTCOMPILE_param_1]",
        }
        for mode, sql in expected.items():
            with self.subTest(sqlmode=mode):
                statement = select(self.table.c.a).limit(10)
                self.assertEqual(compile_sql(statement, build_dialect(mode)), sql)

    def test_limit_with_offset(self) -> None:
        """:meth:`Select.offset` 在三种模式下生成不同语法"""
        expected = {
            # 以实现为准：gbase 模式输出 FIRST n SKIP m
            # （见 GBase8sCompiler._gbase_row_limit_prefix 与设计文档第 4 节）
            "gbase": "SELECT FIRST 10 SKIP 5 t.a FROM t",
            "mysql": "SELECT t.a FROM t LIMIT __[POSTCOMPILE_param_1] "
            "OFFSET __[POSTCOMPILE_param_2]",
        }
        for mode, sql in expected.items():
            with self.subTest(sqlmode=mode):
                statement = select(self.table.c.a).limit(10).offset(5)
                self.assertEqual(compile_sql(statement, build_dialect(mode)), sql)

    def test_offset_only_never_uses_negative_limit(self) -> None:
        """仅 OFFSET 时，mysql 模式用 BIGINT 最大值并把 OFFSET 内联为字面量

        GBase 8s 真机既不接受 ``LIMIT -1``，也不接受超过 BIGINT 范围的
        ``18446744073709551615``；并且 ``LIMIT <max> OFFSET ?`` 这种
        “字面量 + 绑定参数”的组合会在 ``prepareStatement`` 阶段报语法错误，
        故 OFFSET 必须内联渲染。
        """
        statement = select(self.table.c.a).offset(5)
        dialect = build_dialect("mysql")
        self.assertIn(
            "LIMIT 9223372036854775807 OFFSET __[POSTCOMPILE_param_1]",
            compile_sql(statement, dialect),
        )
        text = str(
            statement.compile(
                dialect=dialect, compile_kwargs={"render_postcompile": True}
            )
        )
        self.assertIn("LIMIT 9223372036854775807 OFFSET 5", text)
        self.assertNotIn("LIMIT -1", text)

    def test_oracle_limit_offset_renders_literals(self) -> None:
        """oracle 模式的 ROWNUM 上限在最终编译时以内联字面量呈现"""
        statement = select(self.table.c.a).limit(10).offset(5)
        text = str(
            statement.compile(
                dialect=build_dialect("oracle"),
                compile_kwargs={"render_postcompile": True},
            )
        )
        self.assertIn("ROWNUM <= 10 + 5", text)
        self.assertIn("gbase8s_rn > 5", text)

    def test_boolean_literals(self) -> None:
        """三种模式统一使用 't' / 'f' 布尔字面量"""
        for mode in SUPPORTED_SQLMODES:
            with self.subTest(sqlmode=mode):
                self.assertIn(
                    "SELECT 't'", compile_sql(select(true()), build_dialect(mode))
                )

    def test_empty_set_expr(self) -> None:
        """空集合表达式按模式补伪表"""
        expected = {
            "oracle": "SELECT 1 FROM DUAL WHERE 1!=1",
            "gbase": "SELECT 1 FROM sysmaster:sysdual WHERE 1!=1",
            "mysql": "SELECT 1 FROM sysmaster:sysdual WHERE 1!=1",
        }
        for mode, sql in expected.items():
            with self.subTest(sqlmode=mode):
                dialect = build_dialect(mode)
                compiler = dialect.statement_compiler(dialect, select(literal(1)))
                self.assertEqual(compiler.visit_empty_set_expr([]), sql)

    def test_sequence_nextval(self) -> None:
        """序列取值渲染为 ``<seq>.NEXTVAL``"""
        dialect = build_dialect("gbase")
        compiler = dialect.statement_compiler(dialect, select(literal(1)))
        self.assertEqual(
            compiler.visit_sequence(Sequence("seq_orders")), "seq_orders.NEXTVAL"
        )


class TestTypeAndDdlCompilation(unittest.TestCase):
    """类型与 DDL 编译"""

    def test_type_aliases(self) -> None:
        """GBase 8s 专用类型渲染为自身写法"""
        compiler = build_dialect("oracle").type_compiler
        self.assertEqual(compiler.process(LVARCHAR(10)), "LVARCHAR(10)")
        self.assertEqual(compiler.process(LVARCHAR()), "LVARCHAR")
        self.assertEqual(compiler.process(MONEY()), "MONEY")
        self.assertEqual(compiler.process(SERIAL()), "SERIAL")
        self.assertEqual(compiler.process(SERIAL8()), "SERIAL8")

    def test_builtin_type_mapping(self) -> None:
        """TEXT / BIGINT / DATETIME / TIME 的方言映射"""
        compiler = build_dialect("oracle").type_compiler
        self.assertEqual(compiler.process(Text()), "CLOB")
        self.assertEqual(compiler.process(BigInteger()), "INT8")
        self.assertEqual(compiler.process(DateTime()), "DATETIME YEAR TO FRACTION(5)")
        self.assertEqual(compiler.process(Time()), "DATETIME HOUR TO FRACTION(5)")

    def test_create_table_serial_primary_key(self) -> None:
        """自增主键渲染为 SERIAL / SERIAL8，且不附加 NOT NULL"""
        metadata = MetaData()
        orders = Table(
            "orders",
            metadata,
            Column("id", BigInteger, primary_key=True),
            Column("name", String(30), nullable=False),
            Column("created_at", DateTime),
        )
        ddl = " ".join(
            str(CreateTable(orders).compile(dialect=build_dialect("oracle"))).split()
        )
        self.assertIn("id SERIAL8", ddl)
        self.assertNotIn("id SERIAL8 NOT NULL", ddl)
        self.assertIn("name VARCHAR(30) NOT NULL", ddl)
        self.assertIn("created_at DATETIME YEAR TO FRACTION(5)", ddl)


class TestConnectArgs(unittest.TestCase):
    """连接 URL 到 JayDeBeApi 参数的翻译"""

    def setUp(self) -> None:
        self.dialect = build_dialect("oracle")

    @patch("src.db_connector_tool.drivers.gbase8s.jaydebeapi.find_jdbc_jar")
    def test_create_connect_args(self, find_jar: MagicMock) -> None:
        """URL 翻译为 jdbc:gbasedbt-sqli 连接串与位置参数"""
        find_jar.return_value = "C:/jars/gbasedbt-jdbc.jar"
        url = make_url(BASE_URL + "&sqlmode=gbase&CLIENT_LOCALE=zh_cn.utf8")
        args, kwargs = self.dialect.create_connect_args(url)

        self.assertEqual(kwargs, {})
        self.assertEqual(args[0], JDBC_DRIVER_CLASS)
        self.assertEqual(args[2], ["cvicse", "secret"])
        self.assertEqual(args[3], "C:/jars/gbasedbt-jdbc.jar")
        self.assertTrue(
            args[1].startswith("jdbc:gbasedbt-sqli://127.0.0.1:9088/db_station:")
        )
        for fragment in (
            "GBASEDBTSERVER=gbase01",
            "sqlmode=gbase",
            "CLIENT_LOCALE=zh_cn.utf8",
        ):
            self.assertIn(fragment, args[1])

    @patch("src.db_connector_tool.drivers.gbase8s.jaydebeapi.find_jdbc_jar")
    def test_sqlmode_in_url_overrides_constructor(self, find_jar: MagicMock) -> None:
        """URL 查询参数中的 sqlmode 优先于构造函数取值"""
        find_jar.return_value = None
        self.dialect.create_connect_args(make_url(BASE_URL + "&sqlmode=mysql"))
        self.assertEqual(self.dialect.sqlmode, "mysql")

    @patch("src.db_connector_tool.drivers.gbase8s.jaydebeapi.find_jdbc_jar")
    def test_default_port_and_dialect_keys_excluded(self, find_jar: MagicMock) -> None:
        """缺省端口使用 9088，且方言级查询参数不写入 JDBC 属性"""
        find_jar.return_value = None
        url = make_url("gbase8s://dbhost/db_station?user=u&password=p&sqlmode=oracle")
        args, _ = self.dialect.create_connect_args(url)
        self.assertIn("jdbc:gbasedbt-sqli://dbhost:9088/db_station", args[1])
        for fragment in ("user=", "password="):
            self.assertNotIn(fragment, args[1])

    def test_parse_database(self) -> None:
        """database 段拆分为库名与 JDBC 属性"""
        database, properties = GBase8sDialect_jaydebeapi._parse_database(
            "db_station:GBASEDBTSERVER=gbase01;DB_LOCALE=zh_cn.utf8"
        )
        self.assertEqual(database, "db_station")
        self.assertEqual(properties["GBASEDBTSERVER"], "gbase01")

    def test_parse_database_without_properties(self) -> None:
        """不含 JDBC 属性时返回空字典"""
        self.assertEqual(GBase8sDialect_jaydebeapi._parse_database(""), ("", {}))
        self.assertEqual(GBase8sDialect_jaydebeapi._parse_database("db"), ("db", {}))


class TestJarLookup(unittest.TestCase):
    """JDBC 驱动 jar 定位"""

    def test_explicit_path_wins(self) -> None:
        """显式路径优先"""
        with tempfile.NamedTemporaryFile(suffix=".jar") as jar_file:
            self.assertEqual(
                find_jdbc_jar(os.path.abspath(jar_file.name)),
                os.path.abspath(jar_file.name),
            )

    def test_environment_variable(self) -> None:
        """环境变量 GBASEDBT_JDBC_JAR 次之"""
        with tempfile.NamedTemporaryFile(suffix=".jar") as jar_file:
            path = os.path.abspath(jar_file.name)
            with patch.dict(os.environ, {"GBASEDBT_JDBC_JAR": path}):
                self.assertEqual(find_jdbc_jar(), path)

    @patch("src.db_connector_tool.drivers.gbase8s.jaydebeapi._jar_search_dirs")
    @patch.dict(
        os.environ,
        {"GBASEDBT_JDBC_JAR": "", "GBASE8S_JDBC_JAR": ""},
    )
    def test_returns_none_when_absent(self, search_dirs: MagicMock) -> None:
        """都找不到且自动下载关闭时返回 None，交由 CLASSPATH 决定"""
        search_dirs.return_value = []
        with patch.dict(os.environ, {"GBASEDBT_JDBC_AUTO_DOWNLOAD": "0"}):
            self.assertIsNone(find_jdbc_jar())

    @patch("src.db_connector_tool.drivers.gbase8s.jaydebeapi._jar_search_dirs")
    @patch.dict(
        os.environ,
        {"GBASEDBT_JDBC_JAR": "", "GBASE8S_JDBC_JAR": ""},
    )
    def test_downloads_when_absent(self, search_dirs: MagicMock) -> None:
        """都找不到时尝试自动下载，并把下载结果作为 jar 路径返回"""
        search_dirs.return_value = []
        with patch(
            "src.db_connector_tool.drivers.gbase8s.jaydebeapi.jar_downloader"
            ".download_latest_jar",
            return_value=Path("/tmp/jdbc-3.6.5.21_nobson.jar"),
        ) as download:
            self.assertEqual(find_jdbc_jar(), str(Path("/tmp/jdbc-3.6.5.21_nobson.jar")))
        download.assert_called_once()

    def test_auto_download_dir_is_user_config_jars(self) -> None:
        """自动下载目录为用户配置目录下的 jars"""
        self.assertEqual(auto_download_dir().name, "jars")
        self.assertEqual(auto_download_dir().parent.name, "db_connector_tool")

    def test_search_dirs_are_exactly_two(self) -> None:
        """搜索目录严格只有两个：程序运行目录 jars、用户配置目录 jars"""
        dirs = [os.path.normcase(d) for d in _jar_search_dirs()]

        cwd_jars = os.path.normcase(os.path.join(os.getcwd(), "jars"))
        config_jars = os.path.normcase(str(auto_download_dir()))

        self.assertEqual(dirs, [cwd_jars, config_jars])
        self.assertEqual(auto_download_dir().parent.name, "db_connector_tool")

    def test_search_dirs_exclude_legacy_locations(self) -> None:
        """不再搜索 ~/.sqlalchemy-jdbcapi 等历史目录

        历史变量 ``GBASEDBT_JDBC_SEARCH_DIRS`` 已被移除，此处一并断言它不再生效。
        """
        legacy = os.path.join(os.path.expanduser("~"), ".sqlalchemy-jdbcapi", "drivers")
        extra = os.path.join(os.getcwd(), "extra-jars")
        with patch.dict(os.environ, {"GBASEDBT_JDBC_SEARCH_DIRS": extra}):
            dirs = [os.path.normcase(d) for d in _jar_search_dirs()]

        self.assertNotIn(os.path.normcase(legacy), dirs)
        self.assertNotIn(os.path.normcase(extra), dirs)
        self.assertEqual(len(dirs), 2)

    def test_highest_version_wins_in_directory(self) -> None:
        """同目录多版本时选版本最高的 jar"""
        with tempfile.TemporaryDirectory() as tmp:
            for name in (
                "gbasedbtjdbc_3.6.3_3X2_1_377ee9.jar",
                "gbasedbt-jdbc-3.6.5.21_nobson.jar",
                "gbasedbt-jdbc-2.0.1.26.jar",
            ):
                Path(tmp, name).write_bytes(b"PK\x03\x04")
            self.assertEqual(
                Path(_first_jar_in(tmp)).name, "gbasedbt-jdbc-3.6.5.21_nobson.jar"
            )

    def test_maven_named_jar_is_recognised(self) -> None:
        """Maven 规范名 jdbc-<版本>.jar 也能被识别（下载文件曾被过滤掉的回归）"""
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "jdbc-3.6.5.21_nobson.jar").write_bytes(b"PK\x03\x04")
            self.assertEqual(
                Path(_first_jar_in(tmp)).name, "jdbc-3.6.5.21_nobson.jar"
            )

    def test_downloaded_file_name_is_searchable(self) -> None:
        """自动下载的文件名必须能被搜索逻辑识别"""
        filename = jar_downloader._jar_filename(  # pylint: disable=protected-access
            "3.6.5.21_nobson"
        )
        self.assertTrue(_is_gbase_jar(filename), f"{filename} 无法被搜索逻辑识别")
        self.assertIn("gbasedbt", filename)

    def test_first_jar_in_ignores_non_gbase_and_missing_dir(self) -> None:
        """忽略非 GBase jar；目录不存在时返回 None"""
        with tempfile.TemporaryDirectory() as tmp:
            Path(tmp, "postgresql-42.7.0.jar").write_bytes(b"PK\x03\x04")
            Path(tmp, "readme.txt").write_text("x", encoding="utf-8")
            self.assertIsNone(_first_jar_in(tmp))
            self.assertIsNone(_first_jar_in(os.path.join(tmp, "not-exist")))

    def test_local_jar_preferred_over_download(self) -> None:
        """本地已有 jar 时不应触发下载"""
        with tempfile.TemporaryDirectory() as tmp:
            jar = Path(tmp, "jdbc-3.6.5.21_nobson.jar")
            jar.write_bytes(b"PK\x03\x04")
            with patch(
                "src.db_connector_tool.drivers.gbase8s.jaydebeapi._jar_search_dirs",
                return_value=[tmp],
            ), patch(
                "src.db_connector_tool.drivers.gbase8s.jaydebeapi.jar_downloader"
                ".download_latest_jar"
            ) as download:
                self.assertEqual(find_jdbc_jar(), str(jar))
            download.assert_not_called()

    @patch("src.db_connector_tool.drivers.gbase8s.jaydebeapi._jar_search_dirs")
    @patch.dict(
        os.environ,
        {"GBASEDBT_JDBC_JAR": "", "GBASE8S_JDBC_JAR": ""},
    )
    def test_download_failure_falls_back_to_none(self, search_dirs: MagicMock) -> None:
        """下载失败时仍返回 None，不影响 CLASSPATH 方案"""
        search_dirs.return_value = []
        with patch(
            "src.db_connector_tool.drivers.gbase8s.jaydebeapi.jar_downloader"
            ".download_latest_jar",
            return_value=None,
        ):
            self.assertIsNone(find_jdbc_jar())


class TestDictionaryParsing(unittest.TestCase):
    """数据字典解析"""

    def test_parse_column_type_char_and_varchar(self) -> None:
        """CHAR / VARCHAR 以 collength 作为长度"""
        self.assertEqual(str(dictionary.parse_column_type(0, 8)), "CHAR(8)")
        self.assertEqual(str(dictionary.parse_column_type(13, 64)), "VARCHAR(64)")

    def test_parse_column_type_decimal(self) -> None:
        """DECIMAL 高 8 位为精度、低 8 位为小数位"""
        self.assertEqual(
            str(dictionary.parse_column_type(5, 0x0A02)),
            "NUMERIC(10, 2)",
        )

    def test_parse_column_type_special(self) -> None:
        """LVARCHAR（40/43）与 INT8 等专用类型"""
        self.assertIsInstance(dictionary.parse_column_type(40, 0), LVARCHAR)
        self.assertIsInstance(dictionary.parse_column_type(43, 0), LVARCHAR)
        self.assertIsInstance(dictionary.parse_column_type(17, 0), BigInteger)

    def test_parse_column_type_large_object(self) -> None:
        """coltype=41 按 extended_id 区分 CLOB / BLOB / BOOLEAN"""
        self.assertIsInstance(dictionary.parse_column_type(41, 72, 11), sqltypes.Text)
        self.assertIsInstance(
            dictionary.parse_column_type(41, 72, 10), sqltypes.LargeBinary
        )
        self.assertIsInstance(dictionary.parse_column_type(41, 1, 5), sqltypes.Boolean)
        # extended_id 缺失时回退为 TEXT
        self.assertIsInstance(dictionary.parse_column_type(41, 72), sqltypes.Text)

    def test_parse_column_type_unknown(self) -> None:
        """未知类型号回退为 NULLTYPE"""
        self.assertIsInstance(dictionary.parse_column_type(99, 0), type(NULLTYPE))

    def test_get_index_part_columns(self) -> None:
        """索引列号过滤空值并取绝对值"""
        Row = namedtuple("Row", "part1 part2 part3")
        self.assertEqual(dictionary.get_index_part_columns(Row(1, -2, None)), [1, 2])
        self.assertEqual(dictionary.get_index_part_columns(Row(None, None, None)), [])

    def test_parse_column_defaults(self) -> None:
        """按结果集列名定位 colno 与 default"""
        keys = ["tabid", "colno", "type", "default"]
        rows = [(100, 1, "T", "0"), (100, 2, "T", "  "), (100, 3, "T", "abc")]
        self.assertEqual(
            dictionary.parse_column_defaults(keys, rows), {1: "0", 3: "abc"}
        )

    def test_parse_column_defaults_without_expected_keys(self) -> None:
        """结果集缺少预期列时返回空字典"""
        self.assertEqual(dictionary.parse_column_defaults(["a", "b"], [(1, 2)]), {})


class _FakeResult:
    """模拟 SQLAlchemy CursorResult 的最小接口"""

    def __init__(self, rows: list, keys: list | None = None) -> None:
        """记录预置结果行与结果集列名"""
        self._rows = list(rows)
        self._keys = list(keys or [])

    def fetchall(self) -> list:
        """返回全部结果行"""
        return list(self._rows)

    def scalar(self) -> object:
        """返回首行首列"""
        return self._rows[0][0] if self._rows else None

    def keys(self) -> list:
        """返回结果集列名"""
        return list(self._keys)


class _FakeConnection:  # pylint: disable=too-few-public-methods
    """按 SQL 片段分派预置结果的模拟连接"""

    def __init__(self, responses: dict) -> None:
        """记录 SQL 片段到结果（或待抛异常）的映射"""
        self.responses = responses
        self.calls = []
        # 方言的列名缓存依赖 connection.info（SQLAlchemy 的 Connection.info 字典）
        self.info: dict = {}

    def exec_driver_sql(self, statement: str, parameters: object = None) -> _FakeResult:
        """按片段匹配并返回预置结果

        结果值可以是 :class:`_FakeResult`、待抛异常，或接收 parameters 的
        可调用对象（用于按表名等参数返回不同结果）。
        """
        self.calls.append((statement, parameters))
        for marker, response in self.responses.items():
            if marker in statement:
                if callable(response):
                    response = response(parameters)
                if isinstance(response, BaseException):
                    raise response
                return response
        raise AssertionError(f"未预期的 SQL: {statement}")


class TestReflection(unittest.TestCase):
    """系统目录反射（模拟连接）"""

    def setUp(self) -> None:
        self.dialect = build_dialect("oracle")
        self.column_map = _FakeResult([(1, "order_id"), (2, "user_name")])
        IndexRow = namedtuple(
            "IndexRow", "idxname idxtype constrtype constrname part1 part2"
        )
        self.responses = {
            "SELECT USER FROM systables": _FakeResult([("cvicse",)]),
            "SELECT DISTINCT owner": _FakeResult([("cvicse",), ("gbasedbt",)]),
            "tabtype = ?": _FakeResult([("orders",), ("items",)]),
            "COUNT(*)": _FakeResult([(1,)]),
            "c.colname, c.coltype": _FakeResult(
                [
                    ("order_id", 262, 4, 1, 0),
                    ("user_name", 13, 32, 2, 0),
                    ("note", 41, 72, 3, 11),
                    ("is_active", 41, 1, 4, 5),
                ]
            ),
            "sysdefaults": _FakeResult(
                [(100, 2, "T", "guest")], ["tabid", "colno", "type", "default"]
            ),
            "constrtype = 'P'": _FakeResult(
                [namedtuple("PkRow", "constrname part1 part2")("pk_orders", 1, 2)]
            ),
            "idxname, i.idxtype": _FakeResult(
                [
                    IndexRow("idx_name", "u", "U", "uq_name", 2, None),
                    IndexRow("idx_pk", "u", "P", "pk_orders", 1, None),
                ]
            ),
            "sysreferences": _FakeResult([self._foreign_key_row()]),
            "syschecks": _FakeResult(
                [("ck_amount", "amount > 0"), ("ck_amount", " AND amount < 100")]
            ),
            "sysviews": _FakeResult([("SELECT a",), (" FROM t",)]),
            # 列号->列名的映射按表名返回不同结果（被引用表 customer 的 1 号列是 name）
            "c.colno, c.colname": lambda params: (
                _FakeResult([(1, "name")])
                if params and params[0] == "customer"
                else self.column_map
            ),
        }
        self.connection = _FakeConnection(self.responses)

    @staticmethod
    def _foreign_key_row() -> tuple:
        """构造 FOREIGN_KEYS_QUERY 的结果行

        列顺序与 :mod:`dictionary` 中的 ``FK_*`` 常量严格对应：
        ``constrname, l_part1..l_part16, rt.tabname, rt.owner, r_part1..r_part16, delrule``（共 36 列）。
        """
        local_parts = [2] + [None] * 15
        remote_parts = [1] + [None] * 15
        return tuple(
            ["fk_customer", *local_parts, "customer", "cvicse", *remote_parts, "C"]
        )

    def test_schema_and_object_names(self) -> None:
        """schema、对象名与存在性判断"""
        self.assertEqual(
            self.dialect.get_schema_names(self.connection), ["cvicse", "gbasedbt"]
        )
        self.assertEqual(
            self.dialect._get_default_schema_name(self.connection), "cvicse"
        )
        self.assertEqual(
            self.dialect.get_table_names(self.connection, schema="cvicse"),
            ["orders", "items"],
        )
        self.assertTrue(
            self.dialect.has_table(self.connection, "orders", schema="cvicse")
        )

    def test_view_definition(self) -> None:
        """视图定义按 seqno 顺序拼接"""
        definition = self.dialect.get_view_definition(
            self.connection, "v_orders", schema="cvicse"
        )
        self.assertEqual(definition, "SELECT a FROM t")

    def test_get_columns(self) -> None:
        """列定义包含类型、可空、默认值与自增标记"""
        columns = self.dialect.get_columns(self.connection, "orders", schema="cvicse")
        self.assertEqual(
            [col["name"] for col in columns],
            ["order_id", "user_name", "note", "is_active"],
        )
        order_id, user_name = columns[0], columns[1]
        self.assertIsInstance(order_id["type"], SERIAL)
        self.assertFalse(order_id["nullable"])
        self.assertTrue(order_id["autoincrement"])
        self.assertIsNone(order_id["default"])
        self.assertIsInstance(user_name["type"], String)
        self.assertEqual(user_name["type"].length, 32)
        self.assertEqual(user_name["default"], "'guest'")
        self.assertFalse(user_name["autoincrement"])

    def test_get_columns_distinguishes_extended_types(self) -> None:
        """coltype=41 时按 extended_id 区分 CLOB(11) 与 BOOLEAN(5)"""
        columns = self.dialect.get_columns(self.connection, "orders", schema="cvicse")
        note, is_active = columns[2], columns[3]
        self.assertIsInstance(note["type"], sqltypes.Text)
        self.assertIsInstance(is_active["type"], sqltypes.Boolean)

    def test_get_pk_constraint(self) -> None:
        """主键列按索引列顺序还原为列名"""
        constraint = self.dialect.get_pk_constraint(
            self.connection, "orders", schema="cvicse"
        )
        self.assertEqual(constraint["name"], "pk_orders")
        self.assertEqual(constraint["constrained_columns"], ["order_id", "user_name"])

    def test_get_indexes_skips_primary_key(self) -> None:
        """主键索引不出现在索引列表中，唯一索引带约束名"""
        indexes = self.dialect.get_indexes(self.connection, "orders", schema="cvicse")
        self.assertEqual(len(indexes), 1)
        self.assertEqual(indexes[0]["name"], "idx_name")
        self.assertTrue(indexes[0]["unique"])
        self.assertEqual(indexes[0]["column_names"], ["user_name"])
        self.assertEqual(
            indexes[0]["dialect_options"]["gbase8s_constraint_name"], "uq_name"
        )

    def test_get_foreign_keys(self) -> None:
        """外键包含被约束列、被引用表与级联删除选项

        被引用表与当前 schema 相同时按 SQLAlchemy 约定不重复给出 referred_schema。
        """
        keys = self.dialect.get_foreign_keys(self.connection, "orders", schema="cvicse")
        self.assertEqual(len(keys), 1)
        self.assertEqual(keys[0]["constrained_columns"], ["user_name"])
        self.assertEqual(keys[0]["referred_table"], "customer")
        self.assertIsNone(keys[0]["referred_schema"])
        self.assertEqual(keys[0]["referred_columns"], ["name"])
        self.assertEqual(keys[0]["options"]["ondelete"], "CASCADE")

    def test_get_foreign_keys_cross_schema(self) -> None:
        """被引用表属于其他 owner 时给出 referred_schema"""
        responses = dict(self.responses)
        row = list(self._foreign_key_row())
        row[dictionary.FK_REMOTE_OWNER] = "other_owner"
        responses["sysreferences"] = _FakeResult([tuple(row)])

        keys = self.dialect.get_foreign_keys(
            _FakeConnection(responses), "orders", schema="cvicse"
        )
        self.assertEqual(keys[0]["referred_schema"], "other_owner")

    def test_get_check_constraints(self) -> None:
        """分段的检查约束文本按序合并"""
        constraints = self.dialect.get_check_constraints(
            self.connection, "orders", schema="cvicse"
        )
        self.assertEqual(
            constraints,
            [{"name": "ck_amount", "sqltext": "amount > 0 AND amount < 100"}],
        )

    def test_get_unique_constraints(self) -> None:
        """唯一约束由唯一索引派生"""
        constraints = self.dialect.get_unique_constraints(
            self.connection, "orders", schema="cvicse"
        )
        self.assertEqual(
            constraints, [{"name": "idx_name", "column_names": ["user_name"]}]
        )

    def test_column_defaults_missing_catalog(self) -> None:
        """sysdefaults 不可用时忽略默认值而不报错"""
        connection = _FakeConnection(
            {
                "c.colname, c.coltype": _FakeResult(
                    [("order_id", 262, 4, 1, 0), ("user_name", 13, 32, 2, 0)]
                ),
                "constrtype = 'P'": _FakeResult([]),
                "sysdefaults": exc.DatabaseError(
                    "SELECT d.* FROM sysdefaults", {}, RuntimeError("目录不存在")
                ),
            }
        )
        columns = self.dialect.get_columns(connection, "orders", schema="cvicse")
        self.assertIsNone(columns[1]["default"])

    def test_get_temp_table_names_always_empty(self) -> None:
        """临时表在 GBase 8s 中无法通过系统目录与持久表区分，始终返回空列表"""
        self.assertEqual(
            self.dialect.get_temp_table_names(self.connection, schema="cvicse"),
            [],
        )

    def test_get_table_comment_always_none(self) -> None:
        """GBase 8s 无系统目录存储表注释，始终返回空注释"""
        self.assertEqual(
            self.dialect.get_table_comment(self.connection, "orders", schema="cvicse"),
            {"text": None},
        )


class TestDialectBehaviour(unittest.TestCase):
    """方言能力矩阵与 DBAPI 交互"""

    def setUp(self) -> None:
        self.dialect = create_engine(BASE_URL + "&sqlmode=oracle").dialect

    def test_identity_and_capabilities(self) -> None:
        """方言名、参数风格与关键能力开关"""
        self.assertEqual(self.dialect.name, "gbase8s")
        self.assertEqual(self.dialect.driver, "jaydebeapi")
        self.assertEqual(self.dialect.paramstyle, "qmark")
        self.assertTrue(self.dialect.supports_native_boolean)
        self.assertTrue(self.dialect.supports_statement_cache)
        self.assertFalse(self.dialect.insert_returning)
        self.assertEqual(self.dialect.max_identifier_length, 128)

    def test_isolation_level_values(self) -> None:
        """支持的隔离级别包含 AUTOCOMMIT"""
        values = self.dialect.get_isolation_level_values(None)
        self.assertEqual(values[0], "AUTOCOMMIT")
        self.assertIn("SERIALIZABLE", values)

    def test_set_isolation_level_autocommit(self) -> None:
        """AUTOCOMMIT 通过 JDBC 自动提交开关实现"""
        connection = MagicMock()
        self.dialect.set_isolation_level(connection, "AUTOCOMMIT")
        connection.jconn.setAutoCommit.assert_called_once_with(True)
        connection.cursor.assert_not_called()

    def test_set_isolation_level_transaction(self) -> None:
        """普通隔离级别映射为 SET ISOLATION TO 语句"""
        connection = MagicMock()
        self.dialect.set_isolation_level(connection, "REPEATABLE READ")
        connection.jconn.setAutoCommit.assert_called_once_with(False)
        statement = connection.cursor.return_value.execute.call_args[0][0]
        self.assertEqual(statement, "SET ISOLATION TO REPEATABLE READ")

    def test_set_isolation_level_invalid_raises_argument_error(self) -> None:
        """非白名单隔离级别抛出 ArgumentError 并提示可选值"""
        connection = MagicMock()
        with self.assertRaises(exc.ArgumentError) as ctx:
            self.dialect.set_isolation_level(connection, "READ UNSTABLE")
        self.assertIn("无效的事务隔离级别", str(ctx.exception))
        self.assertIn("READ UNSTABLE", str(ctx.exception))
        for level in ("AUTOCOMMIT", "READ COMMITTED", "SERIALIZABLE"):
            self.assertIn(level, str(ctx.exception))

    def test_get_isolation_level(self) -> None:
        """会话隔离级别由 DBINFO 结果映射"""
        connection = MagicMock()
        connection.cursor.return_value.fetchone.return_value = ("COMMITTED READ",)
        self.assertEqual(self.dialect.get_isolation_level(connection), "READ COMMITTED")

    def test_get_isolation_level_fallback(self) -> None:
        """查询失败时回退为 READ COMMITTED"""
        connection = MagicMock()
        connection.cursor.return_value.execute.side_effect = RuntimeError("boom")
        self.assertEqual(self.dialect.get_isolation_level(connection), "READ COMMITTED")

    def test_on_connect_disables_autocommit(self) -> None:
        """连接建立后关闭 JDBC 自动提交"""
        connection = MagicMock()
        self.dialect.on_connect()(connection)
        connection.jconn.setAutoCommit.assert_called_once_with(False)

    def test_is_disconnect(self) -> None:
        """按异常类型与消息关键字判定连接断开"""
        self.assertTrue(
            self.dialect.is_disconnect(
                self.dialect.dbapi.DatabaseError("connection is closed"), None, None
            )
        )
        self.assertFalse(
            self.dialect.is_disconnect(
                self.dialect.dbapi.DatabaseError("syntax error"), None, None
            )
        )

    def test_do_rollback_skips_closed_connection(self) -> None:
        """连接已关闭时不调用 rollback"""
        closed = MagicMock()
        closed._closed = True
        self.dialect.do_rollback(closed)
        closed.rollback.assert_not_called()

        opened = MagicMock()
        opened._closed = False
        self.dialect.do_rollback(opened)
        opened.rollback.assert_called_once()


class TestRegistration(unittest.TestCase):
    """方言注册与项目接口对接"""

    def test_registry_loads_dialect(self) -> None:
        """gbase8s 已注册到 SQLAlchemy 方言注册表"""
        self.assertIs(
            registry.load("gbase8s"),
            GBase8sDialect_jaydebeapi,
        )

    def test_driver_config_matches_url_template(self) -> None:
        """SQLAlchemyDriver 的 gbase 配置可直接被方言解析"""
        config = {
            "type": "gbase8s",
            "host": "127.0.0.1",
            "port": 9088,
            "database": "db_station",
            "server": "gbase01",
            "username": "cvicse",
            "password": "secret",
            "sqlmode": "gbase",
        }
        driver = SQLAlchemyDriver(config)
        url = make_url(driver._build_connection_url())
        self.assertEqual(url.drivername, "gbase8s")
        self.assertEqual(url.query["sqlmode"], "gbase")
        args, _ = build_dialect("oracle").create_connect_args(url)
        self.assertIn("/db_station:GBASEDBTSERVER=gbase01", args[1])
        self.assertIn("sqlmode=gbase", args[1])
        self.assertEqual(args[2], ["cvicse", "secret"])


if __name__ == "__main__":
    unittest.main()
