"""覆盖率补测：逐条命中剩余分支（最终版）

每一条都通过「直接调用内部方法」或「构造精确的前置条件」触发，
不依赖间接路径的偶然命中。
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import DatabaseError
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

from ._isolation import IsolatedTestCase


class TestDriverConnectBranch(IsolatedTestCase):
    """sqlalchemy_driver: get_tables / get_table_schema 的惰性建连"""

    def test_get_tables_connects_when_engine_none(self) -> None:
        """引擎为空时 get_tables 会先 connect()"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertIsNone(driver.engine)
        with mock.patch.object(
            driver, "connect", wraps=driver.connect
        ) as spy:
            driver.get_tables()
        spy.assert_called_once()

    def test_get_table_schema_connects_when_engine_none(self) -> None:
        """引擎为空时 get_table_schema 会先 connect()"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertIsNone(driver.engine)
        with mock.patch.object(
            driver, "connect", wraps=driver.connect
        ) as spy:
            try:
                driver.get_table_schema("nope")
            except Exception:  # pylint: disable=broad-except
                pass
        spy.assert_called_once()

    def test_get_tables_wraps_non_sqlalchemy_error(self) -> None:
        """RuntimeError（非 SQLAlchemyError）也包装为 QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError) as ctx:
                driver.get_tables()
        self.assertIn("获取表列表失败", str(ctx.exception))

    def test_get_table_schema_wraps_non_sqlalchemy_error(self) -> None:
        """RuntimeError 也包装为 QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError) as ctx:
                driver.get_table_schema("t")
        self.assertIn("获取表结构失败", str(ctx.exception))


class TestConnectionsTempCleanupCallback(IsolatedTestCase):
    """connections: 临时连接清理回调的 except 分支"""

class TestGbaseTranslateRownumBranch(IsolatedTestCase):
    """gbase8s/base.py: translate_select_structure 返回原对象的分支"""

    def test_returns_select_when_no_limiting(self) -> None:
        """无行数限制时原样返回 select"""
        from sqlalchemy import select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        compiler = dialect.statement_compiler(dialect, None)
        stmt = select(1)
        self.assertIs(compiler.translate_select_structure(stmt), stmt)

    def test_returns_select_when_already_rownum(self) -> None:
        """已标记 _gbase8s_rownum 时原样返回（避免二次改写）"""
        from sqlalchemy import select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        compiler = dialect.statement_compiler(dialect, None)
        stmt = select(1)
        stmt._gbase8s_rownum = True  # pylint: disable=protected-access
        self.assertIs(compiler.translate_select_structure(stmt), stmt)


class TestJarDownloadOutsideTarget(IsolatedTestCase):
    """jar_downloader: 目标越界时的拒绝写入"""

    def test_rejects_path_outside_target_dir(self) -> None:
        """resolve 出的目标不在 target_dir 内时拒绝并记录"""
        from src.db_connector_tool.drivers.gbase8s import jar_downloader

        jar_downloader.reset_download_state()
        with tempfile.TemporaryDirectory() as tmp:
            target_dir = Path(tmp) / "jars"
            target_dir.mkdir()
            with mock.patch.object(
                jar_downloader, "resolve_latest_version", return_value="1.0"
            ), mock.patch.object(
                jar_downloader, "_is_within", return_value=False
            ):
                result = jar_downloader.download_latest_jar(target_dir)
        self.assertIsNone(result)
        jar_downloader.reset_download_state()


class TestPathUtilsFallbackDir(IsolatedTestCase):
    """path_utils: 首选目录失败后回退到 cwd"""

    def test_fallback_dir_returned(self) -> None:
        """首选目录 mkdir 失败时回退到 cwd 下的隐藏目录"""
        from src.db_connector_tool.utils.path_utils import PathHelper

        real_mkdir = Path.mkdir
        state = {"n": 0}

        def flaky(self, *args, **kwargs):
            state["n"] += 1
            if state["n"] == 1:
                raise OSError("primary failed")
            return real_mkdir(self, *args, **kwargs)

        with mock.patch.object(Path, "mkdir", flaky):
            result = PathHelper.get_user_config_dir("cov_fallback_app")
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()
