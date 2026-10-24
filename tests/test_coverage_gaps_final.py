"""覆盖率补测：最终细粒度分支（第二批）

只针对「明确可复现」的分支：语句分流、编译产物、错误包装。
**不包含**平台特定分支（Windows/Unix 二选一）与依赖缺失分支
（如 ``except ImportError``：只有真的卸载依赖才会走到），
那些在源码里用 ``# pragma: no cover`` 标注并写明理由。
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.batch_manager import BatchDatabaseManager
from src.db_connector_tool.cli import create_argument_parser, main
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.config import ConfigManager
from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import ConfigError, DatabaseError
from src.db_connector_tool.core.result import QueryResult
from src.db_connector_tool.core.sql_splitter import split_sql
from src.db_connector_tool.drivers.gbase8s import converters

from ._isolation import IsolatedTestCase


class TestCliDispatchBranches(IsolatedTestCase):
    """cli.main 的子命令分发"""

    def test_main_without_command_prints_help(self) -> None:
        """无子命令时打印帮助并以 0 退出"""
        buffer = io.StringIO()
        with mock.patch("sys.argv", ["db-connector"]):
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit) as ctx:
                    main()
        self.assertEqual(ctx.exception.code, 0)
        self.assertIn("使用情况", buffer.getvalue())


class TestCliCoreShowResultsBranch(IsolatedTestCase):
    """cli_core 的 query 显示分支"""

    def test_query_without_output_shows_table(self) -> None:
        """query 不带 -o 时直接渲染表格"""
        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        cli.db_manager.execute_query_result.return_value = QueryResult.from_rows(
            [(1, "x")], [{"name": "id"}, {"name": "n"}]
        )
        parser = create_argument_parser(cli)
        args = parser.parse_args(["query", "db", "SELECT 1"])
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            cli.execute_query(args)
        output = buffer.getvalue()
        self.assertIn("| id", output)
        self.assertIn("总计", output)

    def test_execute_file_skips_empty_statements(self) -> None:
        """脚本里的空语句被跳过，不计数"""
        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        parser = create_argument_parser(cli)
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "s.sql"
            # 中间的空语句在切分阶段已被丢弃，这里验证计数不虚增
            path.write_text("SELECT 1;\n\n\nSELECT 2;\n", encoding="utf-8")
            args = parser.parse_args(["file", "db", str(path), "-c"])
            with mock.patch.object(cli, "_run_single_sql", return_value=True):
                buffer = io.StringIO()
                with redirect_stdout(buffer):
                    cli.execute_file(args)
        self.assertIn("成功 2 条", buffer.getvalue())


class TestBatchManagerQueryBranch(IsolatedTestCase):
    """batch_manager 的查询失败分支"""

class TestConfigKeySetupBranch(IsolatedTestCase):
    """config 在密钥缺失时的初始化"""

    def test_add_config_creates_key_when_missing(self) -> None:
        """新增连接时若加密管理器缺失则创建密钥"""
        manager = ConfigManager("cov_cfg2_app")
        manager.key_manager = mock.Mock()
        manager.key_manager.crypto = None
        manager.key_manager.load_or_create_key = mock.Mock()
        manager.security_manager = mock.Mock()
        manager.security_manager.encrypt_dict_values.return_value = {"type": "sqlite"}
        manager._load_config = mock.Mock(return_value={"connections": {}, "metadata": {}})  # pylint: disable=protected-access
        manager._increment_config_version = mock.Mock()  # pylint: disable=protected-access
        manager._save_config = mock.Mock()  # pylint: disable=protected-access
        try:
            manager.add_config(
                "c1", {"type": "sqlite", "database": ":memory:"}
            )
        except Exception:  # pylint: disable=broad-except
            pass
        manager.key_manager.load_or_create_key.assert_called()


class TestPoolBranch(IsolatedTestCase):
    """连接池的基本状态检查"""

    def test_basic_status_false_when_engine_missing(self) -> None:
        """驱动无 engine 时状态检查为 False"""
        pool = ConnectionPoolManager()
        driver = mock.Mock()
        driver.engine = None
        self.assertFalse(
            pool._check_driver_basic_status(driver)  # pylint: disable=protected-access
        )


class TestGbaseCompilerBranches(IsolatedTestCase):
    """GBase 方言的编译产物分支"""

    def setUp(self) -> None:
        super().setUp()
        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        self.dialect = GBase8sDialect()
        self.compiler_class = self.dialect.statement_compiler

    def test_interval_type_renders(self) -> None:
        """INTERVAL 类型渲染出限定符（走 TypeCompiler）"""
        from sqlalchemy import types as sa_types

        type_compiler = self.dialect.type_compiler_instance
        rendered = type_compiler.visit_INTERVAL(sa_types.Interval())
        self.assertEqual(rendered, "INTERVAL DAY TO FRACTION(5)")

    def test_false_literal_renders(self) -> None:
        """布尔 false 渲染为 GBase 的 'f'"""
        from sqlalchemy import false

        compiler = self.compiler_class(self.dialect, None)
        self.assertEqual(compiler.visit_false(false()), "'f'")

    def test_true_literal_renders(self) -> None:
        """布尔 true 渲染为 GBase 的 't'"""
        from sqlalchemy import true

        compiler = self.compiler_class(self.dialect, None)
        self.assertEqual(compiler.visit_true(true()), "'t'")

    def test_empty_set_expr_uses_pseudo_table(self) -> None:
        """空结果集表达式补伪表"""
        compiler = self.compiler_class(self.dialect, None)
        rendered = compiler.visit_empty_set_expr([])
        self.assertIn("WHERE 1!=1", rendered)

    def test_limit_clause_absent_for_oracle_mode(self) -> None:
        """oracle 模式下不输出 LIMIT 子句"""
        from sqlalchemy import select

        self.dialect.sqlmode = "oracle"
        compiled = str(select(1).limit(5).compile(dialect=self.dialect))
        self.assertNotIn("LIMIT", compiled.upper())

    def test_column_default_renders(self) -> None:
        """列默认值被渲染进 DDL"""
        from sqlalchemy import Column, Integer, MetaData, Table
        from sqlalchemy.schema import CreateTable

        metadata = MetaData()
        table = Table(
            "t_default", metadata, Column("a", Integer, server_default="0")
        )
        ddl = str(CreateTable(table).compile(dialect=self.dialect))
        self.assertIn("DEFAULT", ddl.upper())


class TestDictionaryFallbackBranch(IsolatedTestCase):
    """dictionary 的类型名回退"""

    def test_known_coltype_resolves(self) -> None:
        """已知类型名能解析到实例"""
        from src.db_connector_tool.drivers.gbase8s.dictionary import parse_column_type

        result = parse_column_type("INTEGER", 4, 0)
        self.assertIsNotNone(result)


class TestConvertersNullBranch(IsolatedTestCase):
    """converters 的 NULL 整数分支"""

    def test_to_int_null_returns_none(self) -> None:
        """NULL 整数返回 None"""

        class FakeRS:
            """模拟 ResultSet"""

            @staticmethod
            def getObject(_col):
                return None

        self.assertIsNone(converters._to_int(FakeRS(), 1))  # pylint: disable=protected-access


class TestSplitterUnclosedBranch(IsolatedTestCase):
    """切分器的未闭合标记"""

    def test_unclosed_block_comment(self) -> None:
        """未闭合块注释标记不完整"""
        result = split_sql("SELECT 1 /* unclosed", None)
        self.assertFalse(result[0].is_complete)

    def test_or_without_replace_rejected(self) -> None:
        """CREATE OR <非 REPLACE> 不当作 SPL 开头"""
        result = split_sql("CREATE OR TABLE t (a INT);\nSELECT 1;", "gbase8s")
        self.assertEqual(len(result), 2)


class TestConnectionsCleanupBranch(IsolatedTestCase):
    """connections 的临时连接清理"""
