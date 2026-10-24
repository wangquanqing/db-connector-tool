"""覆盖率补测：分支（条件为假/空集合路径）

这些分支覆盖率缺口都是「条件为假」的路径 —— 主路径测过了，
反向路径没测。每条用例都构造「不满足条件」的输入。
"""

import unittest
from unittest import mock

from ._isolation import IsolatedTestCase


class TestBatchUpgradeNoRollback(IsolatedTestCase):
    """batch_manager: 升级失败但无回滚语句"""

    def test_failure_without_rollback(self) -> None:
        """无回滚语句时只记录失败、不回滚"""
        from src.db_connector_tool.batch_manager import BatchDatabaseManager
        from src.db_connector_tool.core.exceptions import DatabaseError

        manager = BatchDatabaseManager("cov_br_app")
        with mock.patch.object(
            manager, "_execute_statement", side_effect=DatabaseError("boom")
        ):
            results = manager._execute_upgrade_sqls(  # pylint: disable=protected-access
            "c", ["ALTER TABLE t ADD c INT"], None
        )
        self.assertFalse(results[0]["success"])


class TestDeriveNamesEmptyTupleRow(IsolatedTestCase):
    """result: 空元组行（列数为 0）"""

    def test_empty_tuple_returns_empty_names(self) -> None:
        """空元组推导出空列名"""
        from src.db_connector_tool.core.result import _derive_names

        self.assertEqual(_derive_names([()], None), [])


class TestPickDisplayColsAllFit(IsolatedTestCase):
    """result_render: 所有列都放得下（不 break）"""

    def test_all_columns_fit(self) -> None:
        """全部列都能放下时返回完整列表"""
        from src.db_connector_tool.core.result_render import ResultRenderMixin

        render = ResultRenderMixin()
        headers = ["a", "b"]
        widths = {"a": 3, "b": 3}
        self.assertEqual(
            render._pick_display_cols(headers, 100, widths),  # pylint: disable=protected-access
            ["a", "b"],
        )


class TestSplitterSlashInsideProcessBody(IsolatedTestCase):
    """sql_splitter: / 出现在过程体内（不应终止）"""

    def test_slash_inside_block_not_terminator(self) -> None:
        """/ 在过程体内不当作 Oracle 终结符"""
        from src.db_connector_tool.core.sql_splitter import split_sql

        sql = "CREATE OR REPLACE PROCEDURE p AS\nBEGIN\n  NULL;\nEND;\n/\nSELECT 1 FROM DUAL;"
        # 过程体 + SELECT = 2 条
        self.assertEqual(len(split_sql(sql, "oracle")), 2)


class TestGbaseColumnNameMapCache(IsolatedTestCase):
    """gbase8s/base.py: 列名映射缓存命中"""

    def test_cache_hit_skips_query(self) -> None:
        """第二次调用命中缓存，不再查询"""
        from src.db_connector_tool.drivers.gbase8s.base import column_name_map

        connection = mock.Mock()
        connection.info = {}
        connection.exec_driver_sql.return_value.fetchall.return_value = [
            (1, "order_id")
        ]
        first = column_name_map(connection, "t", "s")
        second = column_name_map(connection, "t", "s")
        self.assertEqual(first, {1: "order_id"})
        self.assertEqual(second, first)
        # 只查了一次
        connection.exec_driver_sql.assert_called_once()


class TestGbaseLimitOnlyOffset(IsolatedTestCase):
    """gbase8s/base.py: mysql 模式只有 OFFSET"""

    def test_offset_only_emits_max_limit(self) -> None:
        """只有 OFFSET 时补一个极大 LIMIT"""
        from sqlalchemy import select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "mysql"
        stmt = select(1).offset(5)
        compiled = str(stmt.compile(dialect=dialect))
        self.assertIn("LIMIT", compiled.upper())
        self.assertIn("OFFSET", compiled.upper())


class TestGbaseSelectPrecolumnsModes(IsolatedTestCase):
    """gbase8s/base.py: get_select_precolumns 的非 gbase 模式"""

    def test_oracle_mode_returns_plain(self) -> None:
        """oracle 模式不加 FIRST/SKIP 前缀"""
        from sqlalchemy import select

        from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        compiled = str(select(1).limit(5).compile(dialect=dialect))
        self.assertNotIn("FIRST", compiled.upper())


if __name__ == "__main__":
    unittest.main()
