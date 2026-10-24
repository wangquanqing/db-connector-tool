"""覆盖率补测：逐行击穿（收尾）

所有用例都直接调用目标方法或构造精确前置条件，
不依赖间接路径。
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.drivers.gbase8s import jaydebeapi as gbase_jdbc
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

from ._isolation import IsolatedTestCase


class TestDriverExecuteSqlLazyConnect(IsolatedTestCase):
    """驱动: _execute_sql 的惰性建连"""

    def test_execute_sql_connects_when_engine_none(self) -> None:
        """引擎为空时 _execute_sql 先 connect()"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertIsNone(driver.engine)
        driver.execute_command("CREATE TABLE t (a INT)")
        self.assertIsNotNone(driver.engine)


class TestDriverGenericErrorWrap(IsolatedTestCase):
    """驱动: get_tables/get_table_schema 的通用异常包装"""

    def test_get_tables_generic_exception(self) -> None:
        """非 SQLAlchemyError 也包装为 QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError) as ctx:
                driver.get_tables()
        self.assertIn("获取表列表失败", str(ctx.exception))

    def test_get_table_schema_generic_exception(self) -> None:
        """非 SQLAlchemyError 也包装为 QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError) as ctx:
                driver.get_table_schema("t")
        self.assertIn("获取表结构失败", str(ctx.exception))


class TestJdbcJarOserrorSkip(IsolatedTestCase):
    """jaydebeapi: 候选路径 os.path.isfile 抛 OSError 时跳过"""

class TestJdbcColumnMetadata(IsolatedTestCase):
    """jaydebeapi: get_column_metadata 转发"""

    def test_get_column_metadata_delegates(self) -> None:
        """元数据读取转发到 converters.column_metadata"""
        cursor = mock.Mock()
        cursor.description = [("c", None, None, None, 10, 2, True)]
        result = gbase_jdbc.GBase8sDialect_jaydebeapi.get_column_metadata(cursor)
        self.assertEqual(result[0]["name"], "c")


class TestJdbcDoRollback(IsolatedTestCase):
    """jaydebeapi: do_rollback 吞掉 dbapi 错误"""

class TestGbaseTranslateRownum(IsolatedTestCase):
    """gbase8s/base.py: translate_select_structure 的分支"""

    def test_no_limiting_returns_select(self) -> None:
        """无行数限制时返回原 select"""
        from sqlalchemy import select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        compiler = dialect.statement_compiler(dialect, None)
        stmt = select(1)
        self.assertIs(compiler.translate_select_structure(stmt), stmt)


class TestPathUtilsFallbackDir(IsolatedTestCase):
    """path_utils: 配置目录回退"""

    def test_fallback_used_when_primary_fails(self) -> None:
        """首选目录建失败时回退到 cwd"""
        from src.db_connector_tool.utils.path_utils import PathHelper

        real_mkdir = Path.mkdir
        state = {"n": 0}

        def flaky(self, *args, **kwargs):
            state["n"] += 1
            if state["n"] == 1:
                raise OSError("primary failed")
            return real_mkdir(self, *args, **kwargs)

        with mock.patch.object(Path, "mkdir", flaky):
            result = PathHelper.get_user_config_dir("cov_fb2_app")
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
