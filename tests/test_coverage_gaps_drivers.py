"""覆盖率补测：drivers 层、渲染层、切分器的边界分支

与 `test_coverage_gaps_core.py` 分工一致：这里覆盖「需要 mock 外部依赖
（JDBC/网络/文件系统）」或「人为构造畸形输入」的分支。
"""

import io
import json
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.core.result import ColumnInfo, QueryResult
from src.db_connector_tool.core.result_render import ResultRenderMixin
from src.db_connector_tool.core.sql_splitter import split_sql
from src.db_connector_tool.drivers.gbase8s import converters
from src.db_connector_tool.drivers.result_fetch import (
    column_metadata_from_description,
    enrich_metadata,
    run_sql,
)
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver
from src.db_connector_tool.core.exceptions import QueryError

from ._isolation import IsolatedTestCase


class TestResultRenderBranches(IsolatedTestCase):
    """core/result_render.py 的兜底分支"""

    def setUp(self) -> None:
        super().setUp()
        self.render = ResultRenderMixin()

    def test_save_json_skips_empty_result(self) -> None:
        """空结果不写文件"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.json"
            self.render._save_json([], str(path))  # pylint: disable=protected-access
            self.assertFalse(path.exists())

    def test_save_csv_skips_empty_result(self) -> None:
        """空结果不写文件"""
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "empty.csv"
            self.render._save_csv([], str(path))  # pylint: disable=protected-access
            self.assertFalse(path.exists())

    def test_save_json_wraps_serialization_error(self) -> None:
        """序列化失败时退出而不是抛裸异常"""
        result = QueryResult.from_rows([(1,)], [{"name": "a"}])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.json"
            buffer = io.StringIO()
            with mock.patch.object(
                ResultRenderMixin,
                "_as_query_result",
                return_value=result,
            ), mock.patch("json.dump", side_effect=TypeError("boom")):
                with redirect_stdout(buffer):
                    with self.assertRaises(SystemExit) as ctx:
                        self.render._save_json(result, str(path))  # pylint: disable=protected-access
            self.assertEqual(ctx.exception.code, 1)
            self.assertIn("JSON保存失败", buffer.getvalue())

    def test_to_serializable_uses_non_empty_dict(self) -> None:
        """有内容 __dict__ 的对象转成该字典"""

        class WithAttrs:
            """带 __dict__ 的对象"""

            def __init__(self) -> None:
                self.a = 1

        self.assertEqual(ResultRenderMixin._to_serializable(WithAttrs()), {"a": 1})

    def test_terminal_width_falls_back_on_oserror(self) -> None:
        """取终端宽度失败时回退 120"""
        with mock.patch(
            "shutil.get_terminal_size", side_effect=OSError("no tty")
        ):
            self.assertEqual(self.render._get_term_width(), 120)  # pylint: disable=protected-access

    def test_pick_display_cols_empty_headers(self) -> None:
        """无表头时返回空列表"""
        self.assertEqual(
            self.render._pick_display_cols([], 80, {}), []  # pylint: disable=protected-access
        )

    def test_print_row_accepts_mapping_row(self) -> None:
        """_print_row 接受 Mapping 形态的行（走 _row_values 分支）"""
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.render._print_row(  # pylint: disable=protected-access
                {"a": 1, "b": 2}, ["a", "b"], {"a": 3, "b": 3}
            )
        self.assertIn("1", buffer.getvalue())
        self.assertIn("2", buffer.getvalue())

    def test_print_row_pads_missing_cells(self) -> None:
        """行比表头短时缺的格子留空"""
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.render._print_row(  # pylint: disable=protected-access
                [1], ["a", "b"], {"a": 3, "b": 3}
            )
        self.assertIn("|", buffer.getvalue())

    def test_show_results_narrow_terminal_hides_columns(self) -> None:
        """终端过窄时提示隐藏列数"""
        result = QueryResult.from_rows(
            [("x" * 40, "y" * 40)], [{"name": "c1"}, {"name": "c2"}]
        )
        buffer = io.StringIO()
        # 列宽 40 + 3 分隔符，需 ~46 宽才放得下一列
        with mock.patch(
            "shutil.get_terminal_size",
            return_value=__import__("os").terminal_size((60, 24)),
        ):
            with redirect_stdout(buffer):
                self.render._show_results(result)  # pylint: disable=protected-access
        self.assertIn("终端宽度限制", buffer.getvalue())

    def test_show_results_too_narrow_for_any_column(self) -> None:
        """终端窄到放不下任何列时给出建议"""
        result = QueryResult.from_rows(
            [("x" * 40,)], [{"name": "c1"}]
        )
        buffer = io.StringIO()
        with mock.patch(
            "shutil.get_terminal_size",
            return_value=__import__("os").terminal_size((5, 24)),
        ):
            with redirect_stdout(buffer):
                self.render._show_results(result)  # pylint: disable=protected-access
        self.assertIn("终端宽度过小", buffer.getvalue())

    def test_save_output_rejects_unknown_extension(self) -> None:
        """不支持的扩展名退出"""
        with tempfile.TemporaryDirectory() as tmp:
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit):
                    self.render._save_output(  # pylint: disable=protected-access
                        [{"a": 1}], str(Path(tmp) / "x.xml")
                    )


class TestSqlSplitterBranches(IsolatedTestCase):
    """core/sql_splitter.py 的未闭合/误判分支"""

    def test_unclosed_single_quote(self) -> None:
        """未闭合单引号标记为不完整"""
        result = split_sql("SELECT 'abc", None)
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_complete)

    def test_unclosed_dollar_quote(self) -> None:
        """未闭合 $$ 引用标记为不完整"""
        result = split_sql("SELECT $$ abc", "postgres")
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_complete)

    def test_unclosed_q_quote(self) -> None:
        """未闭合 q'[...]' 标记为不完整"""
        result = split_sql("SELECT q'[abc", "oracle")
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_complete)

    def test_unclosed_bracket_identifier(self) -> None:
        """未闭合方括号标记为不完整"""
        result = split_sql("SELECT [abc", "sqlserver")
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_complete)

    def test_dollar_not_followed_by_tag(self) -> None:
        """$1 之类的占位符不当作引用体"""
        self.assertEqual(len(split_sql("SELECT $1 FROM t;", "postgres")), 1)

    def test_create_or_replace_spl(self) -> None:
        """CREATE OR REPLACE PROCEDURE 也是 SPL 过程体"""
        sql = (
            "CREATE OR REPLACE PROCEDURE p() DEFINE i INT; LET i = 1; "
            "END PROCEDURE;\nSELECT 1 FROM DUAL;"
        )
        self.assertEqual(len(split_sql(sql, "gbase8s")), 2)

    def test_non_spl_create_is_normal(self) -> None:
        """普通 CREATE TABLE 不走 SPL 分支"""
        self.assertEqual(
            len(split_sql("CREATE TABLE t (id INT);\nSELECT 1;", "gbase8s")), 2
        )

    def test_if_without_exists_is_opener(self) -> None:
        """过程体里的 IF ... THEN 计入嵌套"""
        sql = "CREATE PROCEDURE p() BEGIN IF x THEN NULL; END IF; END;\nSELECT 1;"
        self.assertEqual(len(split_sql(sql, "mysql")), 2)


class TestConvertersBranches(IsolatedTestCase):
    """drivers/gbase8s/converters.py 的空值兜底"""

    def test_to_str_returns_none_for_null(self) -> None:
        """JDBC 返回 NULL 时转换器得到 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return None

        self.assertIsNone(converters._to_str(FakeRS(), 1))  # pylint: disable=protected-access

    def test_to_float_falls_back_to_double_value(self) -> None:
        """无法直接 float() 时回退 doubleValue()"""

        class FakeDouble:
            """模拟 java.lang.Double"""

            @staticmethod
            def doubleValue():
                return 1.5

            def __float__(self):
                raise TypeError("no float")

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return FakeDouble()

        self.assertEqual(converters._to_float(FakeRS(), 1), 1.5)  # pylint: disable=protected-access

    def test_to_bool_falls_back_to_boolean_value(self) -> None:
        """无法直接 bool() 时回退 booleanValue()"""

        class FakeBoolean:
            """模拟 java.lang.Boolean"""

            @staticmethod
            def booleanValue():
                return True

            def __bool__(self):
                raise TypeError("no bool")

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return FakeBoolean()

        self.assertIs(converters._to_bool(FakeRS(), 1), True)  # pylint: disable=protected-access

    def test_to_date_returns_none_on_bad_text(self) -> None:
        """日期文本无法解析时返回 None"""

        class FakeDate:
            """模拟 java.sql.Date"""

            def __str__(self):
                return "not-a-date"

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getDate(_col):
                return FakeDate()

        self.assertIsNone(converters._to_date(FakeRS(), 1))  # pylint: disable=protected-access

    def test_to_time_returns_none_on_bad_text(self) -> None:
        """时间文本无法解析时返回 None"""

        class FakeTime:
            """模拟 java.sql.Time"""

            def __str__(self):
                return "bad"

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getTime(_col):
                return FakeTime()

        self.assertIsNone(converters._to_time(FakeRS(), 1))  # pylint: disable=protected-access

    def test_to_object_passthrough(self) -> None:
        """未覆盖类型原样返回 getObject()"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return {"raw": True}

        self.assertEqual(
            converters._to_object(FakeRS(), 1), {"raw": True}  # pylint: disable=protected-access
        )

    def test_column_metadata_empty(self) -> None:
        """游标无 description 时返回空列表"""

        class NoDesc:
            """没有 description"""

            description = None

        self.assertEqual(column_metadata_from_description(NoDesc()), [])


class TestResultFetchBranches(IsolatedTestCase):
    """drivers/result_fetch.py 的降级分支"""

    def test_metadata_type_name_from_number(self) -> None:
        """type_code 是数字时转字符串"""

        class Cursor:
            """模拟 DBAPI 游标"""

            description = [("c", 23, None, None, None, None, None)]

        meta = column_metadata_from_description(Cursor())
        self.assertEqual(meta[0]["type_name"], "23")

    def test_metadata_type_name_from_type_object(self) -> None:
        """type_code 带 values 时取首个 JDBC 类型名"""

        class TypeObj:
            """模拟 DBAPITypeObject"""

            values = ("DECIMAL", "NUMERIC")

        class Cursor:
            """模拟游标"""

            description = [("c", TypeObj(), None, None, 10, 2, None)]

        meta = column_metadata_from_description(Cursor())
        self.assertEqual(meta[0]["type_name"], "DECIMAL")

    def test_enrich_metadata_without_metadata_uses_row_width(self) -> None:
        """元数据为空但已有行时按行宽生成列名"""
        entries = enrich_metadata([], [(1, 2, 3)])
        self.assertEqual(len(entries), 3)
        self.assertEqual([e["name"] for e in entries], ["column_0", "column_1", "column_2"])

    def test_enrich_metadata_empty_rows_and_metadata(self) -> None:
        """元数据与行都为空时返回空列表"""
        self.assertEqual(enrich_metadata([], []), [])

    def test_read_column_metadata_falls_back_when_hook_raises(self) -> None:
        """方言钩子抛异常时回退 DBAPI description"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        driver.engine.dialect.get_column_metadata.side_effect = RuntimeError("boom")

        cursor = mock.Mock()
        cursor.description = [("c", None, None, None, None, None, None)]
        meta = driver._read_column_metadata(cursor)  # pylint: disable=protected-access
        self.assertEqual(meta[0]["name"], "c")

    def test_run_sql_translates_value_error(self) -> None:
        """ValueError 被包装为 QueryError（验证错误）"""
        def _boom():
            raise ValueError("坏参数")

        with self.assertRaises(QueryError) as ctx:
            run_sql(_boom)
        self.assertIn("验证错误", str(ctx.exception))

    def test_run_sql_translates_generic_error(self) -> None:
        """其它异常原样带出"""

        def boom():
            raise RuntimeError("oops")

        with self.assertRaises(QueryError) as ctx:
            run_sql(boom)
        self.assertIn("oops", str(ctx.exception))


class TestSqlAlchemyDriverBranches(IsolatedTestCase):
    """drivers/sqlalchemy_driver.py 的降级分支"""

    def test_connect_args_encode_special_values(self) -> None:
        """自定义参数中的特殊字符被 URL 编码"""
        driver = SQLAlchemyDriver(
            {
                "type": "mysql",
                "host": "h",
                "port": 3306,
                "database": "d",
                "username": "u",
                "password": "p",
                "options": {"sql_mode": "A B&c"},
            }
        )
        url = driver._build_connection_url()  # pylint: disable=protected-access
        # 选项整体经 JSON 编码后再 URL 编码：空格 -> +，& -> %26
        self.assertIn("%26", url)
        self.assertIn("sql_mode", url)

    def test_connection_test_compile_failure_falls_back(self) -> None:
        """方言编译失败时回退到通用 SELECT"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()

        class BadDialect:
            """编译必失败"""

            @staticmethod
            def statement_compiler(*_args, **_kwargs):
                raise TypeError("no compiler")

        driver.engine.dialect = BadDialect()
        conn = mock.MagicMock()
        conn.execute.return_value.fetchone.return_value = (1,)
        driver.engine = mock.MagicMock()
        driver.engine.dialect = BadDialect()
        driver.engine.connect.return_value.__enter__.return_value = conn
        self.assertTrue(driver._perform_connection_test())  # pylint: disable=protected-access

    def test_get_tables_wraps_errors(self) -> None:
        """取表列表失败包装为 QueryError"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch(
            "sqlalchemy.inspect", side_effect=RuntimeError("boom")
        ):
            with self.assertRaises(QueryError):
                driver.get_tables()

    def test_get_table_schema_wraps_errors(self) -> None:
        """取表结构失败包装为 QueryError"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch(
            "sqlalchemy.inspect", side_effect=RuntimeError("boom")
        ):
            with self.assertRaises(QueryError):
                driver.get_table_schema("t")

    def test_lazy_connect_in_query_methods(self) -> None:
        """引擎未初始化时各入口自动建连"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertIsNone(driver.engine)
        self.assertEqual(driver.get_tables(), [])
        self.assertIsNotNone(driver.engine)


if __name__ == "__main__":
    unittest.main()


class TestPythonTypeInference(IsolatedTestCase):
    """``_python_type_from_type_name`` 的映射规则

    这条守的是「**不猜**」原则：``type_code`` 是数字类型号时，
    同一个号可能对应多种类型（SQL Server 的 3 = INT/FLOAT/BIT），
    必须返回 None 而不是瞎猜一个。
    """

    def test_known_type_names(self) -> None:
        """常见类型名能正确映射"""
        from src.db_connector_tool.drivers.result_fetch import (
            _python_type_from_type_name,
        )

        cases = [
            ("VARCHAR", "str"),
            ("NVARCHAR", "str"),
            ("TEXT", "str"),
            ("CHAR", "str"),
            ("INTEGER", "int"),
            ("BIGINT", "int"),
            ("SMALLINT", "int"),
            ("DECIMAL", "Decimal"),
            ("NUMERIC", "Decimal"),
            ("MONEY", "Decimal"),
            ("FLOAT", "float"),
            ("DOUBLE", "float"),
            ("REAL", "float"),
            ("BOOLEAN", "bool"),
            ("BIT", "bool"),
            ("DATE", "date"),
            ("TIME", "time"),
            ("TIMESTAMP", "datetime"),
            ("DATETIME", "datetime"),
            ("BLOB", "bytes"),
            ("BINARY", "bytes"),
            ("INTERVAL", "timedelta"),
        ]
        for type_name, expect in cases:
            with self.subTest(type_name=type_name):
                self.assertEqual(
                    _python_type_from_type_name(type_name), expect
                )

    def test_case_insensitive(self) -> None:
        """大小写不敏感"""
        from src.db_connector_tool.drivers.result_fetch import (
            _python_type_from_type_name,
        )

        self.assertEqual(_python_type_from_type_name("varchar"), "str")
        self.assertEqual(_python_type_from_type_name("Integer"), "int")

    def test_length_suffix_stripped(self) -> None:
        """带长度的类型名先去长度再映射"""
        from src.db_connector_tool.drivers.result_fetch import (
            _python_type_from_type_name,
        )

        self.assertEqual(_python_type_from_type_name("VARCHAR(50)"), "str")
        self.assertEqual(_python_type_from_type_name("DECIMAL(19,2)"), "Decimal")

    def test_numeric_type_codes_not_guessed(self) -> None:
        """纯数字类型号必须返回 None（不猜）

        实测 SQL Server 的 type_code=3 同时对应 INT / FLOAT / BIT，
        无法反推，猜了就是给错信息。
        """
        from src.db_connector_tool.drivers.result_fetch import (
            _python_type_from_type_name,
        )

        for code in ("3", "5", "8", "23", "0", "1700"):
            with self.subTest(code=code):
                self.assertIsNone(
                    _python_type_from_type_name(code),
                    msg=f"类型号 {code} 不该被推断出类型",
                )

    def test_none_and_empty(self) -> None:
        """None / 空串返回 None"""
        from src.db_connector_tool.drivers.result_fetch import (
            _python_type_from_type_name,
        )

        self.assertIsNone(_python_type_from_type_name(None))
        self.assertIsNone(_python_type_from_type_name(""))
        self.assertIsNone(_python_type_from_type_name("   "))

    def test_unknown_name(self) -> None:
        """未知类型名返回 None"""
        from src.db_connector_tool.drivers.result_fetch import (
            _python_type_from_type_name,
        )

        self.assertIsNone(_python_type_from_type_name("NO_SUCH_TYPE_XYZ"))

    def test_description_path_uses_type_name(self) -> None:
        """``column_metadata_from_description`` 走类型名推断"""
        from src.db_connector_tool.drivers.result_fetch import (
            column_metadata_from_description,
        )

        class FakeCursor:
            """模拟游标：第二列填真实类型名，第一列填数字类型号"""

            description = [
                ("a", "VARCHAR", None, None, None, None, None),
                ("b", "23", None, None, None, None, None),
                ("c", None, None, None, None, None, None),
            ]

        meta = column_metadata_from_description(FakeCursor())
        self.assertEqual(meta[0]["python_type"], "str")
        self.assertIsNone(meta[1]["python_type"])  # 数字号不猜
        self.assertIsNone(meta[2]["python_type"])  # None 保持 None
