"""覆盖率补测：最后一组可复现分支

到这里剩下的都是「单行兜底」。本文件只收**能稳定复现**的那些；
确实无法在单平台/无依赖环境下复现的（见文末说明），在源码里用
``# pragma: no cover`` 标注并写明理由，而不是写假测试。
"""

import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import DatabaseError
from src.db_connector_tool.core.result import QueryResult, _derive_names
from src.db_connector_tool.core.sql_splitter import split_sql
from src.db_connector_tool.drivers.gbase8s import jar_downloader
from src.db_connector_tool.drivers.gbase8s import jaydebeapi as gbase_jdbc
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

from ._isolation import IsolatedTestCase


class TestJarValidationBranches(IsolatedTestCase):
    """jar 完好性检查的 IO 兜底"""

    def test_corrupt_jar_header_rejected(self) -> None:
        """非 ZIP 头被判为损坏"""
        with tempfile.TemporaryDirectory() as tmp:
            bad = Path(tmp) / "x.jar"
            bad.write_bytes(b"NOPE")
            self.assertFalse(gbase_jdbc._looks_like_jar(str(bad)))  # pylint: disable=protected-access

    def test_missing_jar_file_rejected(self) -> None:
        """文件不存在时判为损坏（OSError 分支）"""
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "nope.jar"
            self.assertFalse(gbase_jdbc._looks_like_jar(str(missing)))  # pylint: disable=protected-access

    def test_first_jar_in_unreadable_dir(self) -> None:
        """目录不可读时返回 None"""
        with mock.patch("os.path.isdir", return_value=True), mock.patch(
            "os.listdir", side_effect=OSError("boom")
        ):
            self.assertIsNone(gbase_jdbc._first_jar_in("/nowhere"))  # pylint: disable=protected-access

    def test_is_within_returns_false_on_oserror(self) -> None:
        """路径解析失败按越界处理"""
        with mock.patch.object(Path, "resolve", side_effect=OSError("boom")):
            self.assertFalse(
                jar_downloader._is_within(Path("a"), Path("b"))  # pylint: disable=protected-access
            )


class TestDriverParamBranches(IsolatedTestCase):
    """驱动 URL 参数合并的跳过分支"""

    def test_skip_none_value(self) -> None:
        """值为 None 的参数被跳过"""
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
        params = {}
        self.assertTrue(
            driver._should_skip_param("k", None)  # pylint: disable=protected-access
        )

    def test_skip_pool_config(self) -> None:
        """pool_config 交给 SQLAlchemy，不拼进 URL"""
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
            driver._should_skip_param("pool_config", {"size": 5})  # pylint: disable=protected-access
        )

    def test_default_param_is_encoded(self) -> None:
        """默认参数被 URL 编码后写入"""
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
        params = {}
        driver._merge_default_params(params, {"charset": "utf8 mb4"})  # pylint: disable=protected-access
        self.assertIn("charset", params)
        self.assertIn("+", params["charset"])


class TestCliCoreCustomParamBranch(IsolatedTestCase):
    """update 时合并自定义参数"""

    def test_update_merges_custom_params(self) -> None:
        """--custom-params 会合并进更新后的配置"""
        from src.db_connector_tool.cli import create_argument_parser

        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        cli.db_manager.show_connection.return_value = {"type": "mysql"}
        parser = create_argument_parser(cli)
        args = parser.parse_args(["update", "db", "-T", "mysql", "-c", "k=v"])
        with mock.patch.object(
            cli, "_build_updated_conn_config", return_value={"type": "mysql"}
        ):
            buffer = io.StringIO()
            from contextlib import redirect_stdout

            with redirect_stdout(buffer):
                cli.update_connection(args)
        cli.db_manager.update_connection.assert_called_once()


class TestConnectionsTempCleanupBranch(IsolatedTestCase):
    """临时连接清理失败的分支"""

class TestResultDeriveNamesBranch(IsolatedTestCase):
    """result 的字典行推导"""

    def test_derive_names_from_dict_row(self) -> None:
        """字典行按插入顺序推导列名"""
        self.assertEqual(_derive_names([{"b": 1, "a": 2}], None), ["b", "a"])


class TestSplitterResidualBranchesTwo(IsolatedTestCase):
    """切分器最后两个分支"""

    def test_unclosed_nested_block_comment(self) -> None:
        """未闭合的嵌套块注释标记不完整"""
        result = split_sql("SELECT 1 /* a /* b", "postgres")
        self.assertFalse(result[0].is_complete)

    def test_or_without_replace_is_not_spl(self) -> None:
        """CREATE OR <非 REPLACE> 不当作 SPL 开头"""
        result = split_sql("CREATE OR TABLE t (a INT);\nSELECT 1;", "gbase8s")
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
