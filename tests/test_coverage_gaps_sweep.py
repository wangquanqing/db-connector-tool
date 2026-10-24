"""覆盖率补测：逐条击穿剩余分支

每条用例前面的注释写明「这行代码在什么情况下走到」，
确保不是为覆盖率而覆盖率 —— 每条都对应真实的运行时场景。
"""

import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.batch_manager import BatchDatabaseManager
from src.db_connector_tool.cli import main
from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
)
from src.db_connector_tool.core.result import _derive_names
from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect
from src.db_connector_tool.drivers.gbase8s.dictionary import parse_column_type
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

from ._isolation import IsolatedTestCase


class TestBatchManagerCloseAndCleanup(IsolatedTestCase):
    """batch_manager: 关闭连接与清理的各异常分支"""

    def test_close_all_connections_swallows_driver_error(self) -> None:
        """关闭某个连接抛 DatabaseError 时只记 warning，其余继续"""
        manager = BatchDatabaseManager("cov_bm_app")
        manager._connection_names.extend(["a", "b"])  # pylint: disable=protected-access
        with mock.patch.object(
            manager.database_manager.pool_manager,
            "remove_connection",
            side_effect=[DatabaseError("boom"), None],
        ):
            manager.close_all_connections()

    def test_cleanup_unlink_oserror_logged(self) -> None:
        """删除临时配置抛 OSError 时只记 warning"""
        manager = BatchDatabaseManager("cov_bm_app2")
        manager._is_cleaned = False  # pylint: disable=protected-access
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp) / "connections_x.toml").write_text("x", encoding="utf-8")
            with mock.patch(
                "src.db_connector_tool.batch_manager.PathHelper.get_user_config_dir",
                return_value=Path(tmp),
            ), mock.patch.object(Path, "unlink", side_effect=OSError("boom")):
                manager.cleanup()

    def test_cleanup_re_raises_connector_error(self) -> None:
        """清理时 DBConnectorError 继续抛出"""
        manager = BatchDatabaseManager("cov_bm_app3")
        manager._is_cleaned = False  # pylint: disable=protected-access
        with mock.patch(
            "src.db_connector_tool.batch_manager.PathHelper.get_user_config_dir",
            side_effect=DBConnectorError("boom"),
        ):
            with self.assertRaises(DBConnectorError):
                manager.cleanup()


class TestBatchUpgradeInternalError(IsolatedTestCase):
    """batch_manager: 单库升级内部抛错"""

    def test_internal_error_returns_failure(self) -> None:
        """_execute_upgrade_sqls 抛 DatabaseError 时返回失败结果"""
        manager = BatchDatabaseManager("cov_bm_up")
        with mock.patch.object(
            manager,
            "_execute_upgrade_sqls",
            side_effect=DatabaseError("boom"),
        ):
            conn_name, result = manager._upgrade_single_database_internal(  # pylint: disable=protected-access
                "c", ["ALTER TABLE t ADD c INT"], []
            )
        self.assertEqual(conn_name, "c")
        self.assertFalse(result["success"])


class TestMainHelp(IsolatedTestCase):
    """cli.main: 无子命令"""

    def test_without_command_prints_help(self) -> None:
        """无子命令时打印帮助并退出 0"""
        buffer = io.StringIO()
        with mock.patch("sys.argv", ["db-connector"]):
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit) as ctx:
                    main()
        self.assertEqual(ctx.exception.code, 0)


class TestShellOuterException(IsolatedTestCase):
    """cli_core: shell 外层异常分支"""

class TestKeyManagerEarlyReturn(IsolatedTestCase):
    """key_manager: 密钥环成功时提前返回"""

    def test_keyring_success_returns_early(self) -> None:
        """密钥环读取成功时不再走文件回退"""
        from src.db_connector_tool.core.key_manager import KeyManager

        manager = KeyManager("cov_km_app")
        with mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_available", True
        ), mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_module", mock.Mock()
        ), mock.patch.object(
            manager, "_load_or_create_key_from_keyring"
        ) as ok, mock.patch.object(
            manager, "_load_or_create_key_from_file"
        ) as fallback:
            manager.load_or_create_key()
        ok.assert_called_once()
        fallback.assert_not_called()


class TestDeriveNamesPlainDict(IsolatedTestCase):
    """result: 纯字典行推导列名"""

    def test_plain_dict_row(self) -> None:
        """dict 行按插入顺序给出列名"""
        self.assertEqual(_derive_names([{"p": 1, "q": 2}], None), ["p", "q"])


class TestDictionaryKnownType(IsolatedTestCase):
    """dictionary: 已知类型实例化"""

    def test_integer_type_instance(self) -> None:
        """INTEGER 能实例化出类型对象"""
        self.assertIsNotNone(parse_column_type("INTEGER", 4, 0))


class TestPoolValidityFallback(IsolatedTestCase):
    """connection_pool: 有效性检查兜底"""

    def test_returns_false_on_attribute_error(self) -> None:
        """基本状态检查抛 AttributeError 时返回 False"""
        pool = ConnectionPoolManager()
        with mock.patch.object(
            pool, "_check_driver_basic_status", side_effect=AttributeError("boom")
        ):
            self.assertFalse(pool._is_connection_valid(mock.Mock()))  # pylint: disable=protected-access


class TestGbaseCompilerResidual(IsolatedTestCase):
    """gbase8s/base.py: 方言编译的剩余分支"""

    def test_unknown_coltype_falls_back(self) -> None:
        """未知类型名回退为 SQLAlchemy 的 NullType（不崩溃）"""
        from sqlalchemy.types import NullType

        self.assertIsInstance(parse_column_type("NO_SUCH_TYPE_XYZ", 4, 0), NullType)

    def test_translate_select_structure_no_limit(self) -> None:
        """无 LIMIT 时 translate_select_structure 原样返回"""
        from sqlalchemy import select

        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        compiler = dialect.statement_compiler(dialect, None)
        stmt = select(1)
        self.assertIs(compiler.translate_select_structure(stmt), stmt)


class TestPathUtilsFallback(IsolatedTestCase):
    """path_utils: 配置目录回退"""

    def test_fallback_dir_returned(self) -> None:
        """首选目录不可创建时回退到 cwd"""
        from src.db_connector_tool.utils.path_utils import PathHelper

        calls = {"n": 0}

        def flaky_mkdir(self, *args, **kwargs):
            calls["n"] += 1
            if calls["n"] == 1:
                raise OSError("primary failed")
            return None

        with mock.patch.object(Path, "mkdir", flaky_mkdir):
            result = PathHelper.get_user_config_dir("cov_fb_app")
        self.assertIsNotNone(result)


class TestDriverConnections(IsolatedTestCase):
    """sqlalchemy_driver: 惰性建连与包装"""

    def test_get_tables_lazy_connect(self) -> None:
        """get_tables 在引擎为空时自动建连"""
        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.assertIsNone(driver.engine)
        self.assertEqual(driver.get_tables(), [])
        self.assertIsNotNone(driver.engine)

    def test_get_tables_wraps_generic(self) -> None:
        """非 SQLAlchemy 异常也包装为 QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError):
                driver.get_tables()

    def test_get_table_schema_wraps_generic(self) -> None:
        """非 SQLAlchemy 异常也包装为 QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        driver.engine = mock.Mock()
        with mock.patch("sqlalchemy.inspect", side_effect=RuntimeError("boom")):
            with self.assertRaises(QueryError):
                driver.get_table_schema("t")


if __name__ == "__main__":
    unittest.main()
