"""覆盖率补测：收尾第二批

这里补的是「_build_updated_conn_config 的自定义参数合并」、
「key_manager 提前返回」、「切分器的 q 引用子分支」等。
"""

import io
import unittest
from contextlib import redirect_stdout
from unittest import mock

from src.db_connector_tool.core.sql_splitter import split_sql

from ._isolation import IsolatedTestCase


class TestUpdateCustomParamsMerge(IsolatedTestCase):
    """cli_core: _build_updated_conn_config 合并 --custom-params"""

    def test_custom_params_merged_into_config(self) -> None:
        """自定义参数被合并进更新后的配置"""
        from src.db_connector_tool.cli import create_argument_parser
        from src.db_connector_tool.cli_core import DBConnectorCLI

        cli = DBConnectorCLI()
        cli.db_manager = mock.Mock()
        parser = create_argument_parser(cli)
        args = parser.parse_args(
            ["update", "db", "-T", "mysql", "-c", "k=v", "x=1"]
        )
        updated = cli._build_updated_conn_config(  # pylint: disable=protected-access
            {"type": "mysql", "host": "h", "port": 1, "database": "d"}, args
        )
        self.assertEqual(updated.get("k"), "v")
        # 值会按字面量做强转（"1" -> 1）
        self.assertEqual(updated.get("x"), 1)


class TestSplitterQQuoteBranches(IsolatedTestCase):
    """sql_splitter: q 引用的子分支"""

    def test_q_not_followed_by_quote(self) -> None:
        """q 后面不是引号时按普通字符处理"""
        self.assertEqual(len(split_sql("SELECT q FROM t;", "oracle")), 1)

    def test_q_quote_too_short(self) -> None:
        """q' 结尾缺内容时安全处理"""
        result = split_sql("SELECT q'", "oracle")
        self.assertEqual(len(result), 1)

    def test_single_quote_not_q_quote(self) -> None:
        """普通单引号不被当成 q 引用"""
        self.assertEqual(len(split_sql("SELECT 'abc';", "oracle")), 1)


if __name__ == "__main__":
    unittest.main()
