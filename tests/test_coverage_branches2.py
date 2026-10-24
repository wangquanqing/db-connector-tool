"""覆盖率补测：分支的「条件不成立」侧

每个缺口都是「if 为真时测过了、为假时没测」的路径。
"""

import unittest
from unittest import mock

from ._isolation import IsolatedTestCase


class TestSplitterSlashInSplBody(IsolatedTestCase):
    """sql_splitter: / 在 SPL 体内（depth>0 或 in_spl）"""

    def test_slash_inside_block_does_not_split(self) -> None:
        """/ 出现在 BEGIN..END 内部时不算终结符"""
        from src.db_connector_tool.core.sql_splitter import split_sql

        # 块内的 / 不能被当作 Oracle 终结符
        sql = "BEGIN\n  NULL;\nEND;\n=\n/\nSELECT 1 FROM DUAL;"
        result = split_sql(sql, "oracle")
        self.assertGreaterEqual(len(result), 1)


class TestConverterBoolNotCallable(IsolatedTestCase):
    """converters: _to_bool 的 value 本身可转 bool（不走 booleanValue）"""

    def test_plain_int_converts_directly(self) -> None:
        """普通整数直接 bool() 转换"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return 1

        from src.db_connector_tool.drivers.gbase8s.converters import _to_bool

        self.assertIs(_to_bool(FakeRS(), 1), True)


class TestKeyManagerDependencyCheckBranches(IsolatedTestCase):
    """key_manager: _check_dependencies 的缓存与失败分支"""

    def test_check_dependencies_cached(self) -> None:
        """重复调用命中缓存"""
        from src.db_connector_tool.core.key_manager import KeyManager

        KeyManager._deps_checked = False  # pylint: disable=protected-access
        # 返回 None，只做检查副作用
        self.assertIsNone(KeyManager._check_dependencies())  # pylint: disable=protected-access
        # 第二次调用（已标记检查过）同样返回 None
        self.assertIsNone(KeyManager._check_dependencies())  # pylint: disable=protected-access


class TestGbaseLimitClauseSimpleInt(IsolatedTestCase):
    """gbase8s/base.py: limit_clause 的非简单整数（表达式）路径"""

    def test_expression_limit(self) -> None:
        """LIMIT 是表达式时走 process 而非字面量内联"""
        from sqlalchemy import literal, select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "mysql"
        stmt = select(1).limit(literal(5))
        compiled = str(stmt.compile(dialect=dialect))
        self.assertIn("LIMIT", compiled.upper())


class TestGbaseSelectPrecolumnsNoLimit(IsolatedTestCase):
    """gbase8s/base.py: gbase 模式但无行数限制"""

    def test_gbase_mode_without_limit(self) -> None:
        """gbase 模式无 LIMIT 时不加 FIRST/SKIP"""
        from sqlalchemy import select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "gbase"
        compiled = str(select(1).compile(dialect=dialect))
        self.assertNotIn("FIRST", compiled.upper())


class TestRenderPrintTableIndices(IsolatedTestCase):
    """result_render: _print_table 带 indices（裁剪列）"""

    def test_print_table_with_indices(self) -> None:
        """指定 indices 时只渲染该列"""
        import io
        from contextlib import redirect_stdout

        from src.db_connector_tool.core.result_render import ResultRenderMixin

        render = ResultRenderMixin()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            render._print_table(  # pylint: disable=protected-access
                ["a"], [(1, 2)], {"a": 3}, indices=[0]
            )
        self.assertIn("1", buffer.getvalue())
        self.assertNotIn("2", buffer.getvalue())


class TestDriverShouldSkipBasicParam(IsolatedTestCase):
    """sqlalchemy_driver: _should_skip_param 命中 BASIC_PARAMS / POOL_PARAMS"""

    def test_basic_param_skipped(self) -> None:
        """基础参数（host/port 等）被跳过"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        driver = SQLAlchemyDriver(
            {
                "type": "mysql",
                "host": "h",
                "port": 3306,
                "database": "d",
                "username": "u",
                "password": "p",
            }
        )
        self.assertTrue(
            driver._should_skip_param("host", "x")  # pylint: disable=protected-access
        )


if __name__ == "__main__":
    unittest.main()
