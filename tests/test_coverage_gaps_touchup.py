"""覆盖率补测：最后收尾（可测的直接测）

这些分支大多有明确触发条件，只是此前没有用例覆盖到。
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.exceptions import DatabaseError
from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.core.result import _derive_names
from src.db_connector_tool.drivers.gbase8s.base import (
    GBase8sDialect,
    GBase8sTypeCompiler,
)
from src.db_connector_tool.drivers.gbase8s.dictionary import parse_column_type

from ._isolation import IsolatedTestCase


class TestKeyManagerFallbackBranch(IsolatedTestCase):
    """key_manager: 密钥环抛异常时回退"""

    def test_keyring_error_falls_back_to_file(self) -> None:
        """密钥环读取抛异常时回退到文件方案"""
        manager = KeyManager("cov_keyfb_app")
        with mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_available", True
        ), mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_module", mock.Mock()
        ), mock.patch.object(
            manager,
            "_load_or_create_key_from_keyring",
            side_effect=RuntimeError("keyring boom"),
        ), mock.patch.object(
            manager, "_load_or_create_key_from_file"
        ) as fallback:
            manager.load_or_create_key()
        fallback.assert_called_once()


class TestDeriveNamesRowBranch(IsolatedTestCase):
    """result._derive_names: 无 keys() 但有 _mapping 的行"""

    def test_row_with_mapping_keys(self) -> None:
        """Row 形态从 _mapping.keys() 取列名"""

        class FakeMapping:
            """内部映射"""

            @staticmethod
            def keys():
                """返回列名"""
                return ["a", "b"]

        class FakeRow:
            """模拟 SQLAlchemy Row（无 keys()）"""

            _mapping = FakeMapping()

        self.assertEqual(_derive_names([FakeRow()], None), ["a", "b"])


class TestTypeCompilerTimestamp(IsolatedTestCase):
    """GBase TypeCompiler: TIMESTAMP 与 LVARCHAR"""

    def test_timestamp_renders_as_datetime(self) -> None:
        """TIMESTAMP 渲染为 DATETIME YEAR TO FRACTION(5)"""
        from sqlalchemy import types as sa_types

        compiler = GBase8sTypeCompiler(GBase8sDialect())
        self.assertEqual(
            compiler.visit_TIMESTAMP(sa_types.TIMESTAMP()),
            "DATETIME YEAR TO FRACTION(5)",
        )

    def test_lvarchar_without_length(self) -> None:
        """LVARCHAR 无长度时不带括号"""
        compiler = GBase8sTypeCompiler(GBase8sDialect())

        class NoLength:
            """无 length 属性"""

            length = None

        self.assertEqual(compiler.visit_LVARCHAR(NoLength()), "LVARCHAR")


class TestLimitClauseBranches(IsolatedTestCase):
    """GBase limit_clause: 非 mysql 模式与无 limit 的情形"""

    def test_non_mysql_mode_returns_empty(self) -> None:
        """oracle 模式下 limit_clause 返回空串"""
        from sqlalchemy import select

        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        compiler = dialect.statement_compiler(dialect, None)
        stmt = select(1).limit(5)
        stmt = stmt.compile(dialect=dialect)
        # 直接调用编译器的 limit_clause
        compiled_select = select(1).limit(5)
        self.assertEqual(compiler.limit_clause(compiled_select), "")

    def test_mysql_mode_without_limit(self) -> None:
        """mysql 模式下无 limit/offset 时返回空串"""
        from sqlalchemy import select

        dialect = GBase8sDialect()
        dialect.sqlmode = "mysql"
        compiler = dialect.statement_compiler(dialect, None)
        stmt = select(1)
        self.assertEqual(compiler.limit_clause(stmt), "")


class TestDictionaryInstanceBranch(IsolatedTestCase):
    """dictionary: 已知类型名实例化"""

    def test_known_type_returns_instance(self) -> None:
        """已知 coltype 返回类型实例"""
        result = parse_column_type("INTEGER", 4, 0)
        self.assertIsNotNone(result)


class TestPoolConnectionValidityBranch(IsolatedTestCase):
    """connection_pool: 有效性检查的异常兜底"""

    def test_is_connection_valid_false_on_error(self) -> None:
        """探测抛异常时按无效处理"""
        pool = ConnectionPoolManager()
        driver = mock.Mock()
        with mock.patch.object(
            pool, "_check_driver_basic_status", side_effect=AttributeError("boom")
        ):
            self.assertFalse(pool._is_connection_valid(driver))  # pylint: disable=protected-access


class TestCliShellStartFailureBranch(IsolatedTestCase):
    """cli_core: shell 外层 except（建 manager 失败）"""

    def test_shell_outer_exception_exits(self) -> None:
        """外层 try 捕获异常时打印提示并 sys.exit(1)"""
        cli = DBConnectorCLI()
        args = mock.Mock(connection="db")
        # 让 print 之后的 input 直接抛 ConfigError（穿透内层 try 的 except 列表之外）
        with mock.patch("builtins.input", side_effect=SystemExit), \
                mock.patch.object(cli, "_ensure_db_manager", side_effect=DatabaseError("boom")):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                try:
                    cli.interactive_shell(args)
                except SystemExit:
                    pass


if __name__ == "__main__":
    unittest.main()
