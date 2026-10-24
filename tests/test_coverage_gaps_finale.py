"""覆盖率补测：终章

每一行都对应一个明确的运行时条件，注释写明触发方式。
"""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest import mock

from src.db_connector_tool.core.exceptions import DatabaseError
from src.db_connector_tool.core.result import _derive_names
from src.db_connector_tool.drivers.gbase8s import dictionary
from src.db_connector_tool.drivers.gbase8s.base import GBase8sDialect

from ._isolation import IsolatedTestCase


class TestDeriveNamesPlainDictRow(IsolatedTestCase):
    """result: 纯 dict 行（无 keys 方法但 isinstance dict）"""

    def test_plain_dict_returns_keys(self) -> None:
        """dict 行按插入顺序返回列名"""
        self.assertEqual(_derive_names([{"m": 1, "n": 2}], None), ["m", "n"])


class TestDictionaryLargeObjectFallback(IsolatedTestCase):
    """dictionary: 大对象类型号的回退分支"""

    def test_large_object_without_extended_type(self) -> None:
        """extended_id 未知时回退到 ischema_names[coltype]"""
        result = dictionary.parse_column_type(
            dictionary._LARGE_OBJECT_COLTYPE, 16, 9999  # pylint: disable=protected-access
        )
        self.assertIsNotNone(result)

    def test_interval_coltype_branch(self) -> None:
        """INTERVAL 类型号走 ischema_names 分支"""
        result = dictionary.parse_column_type(14, 16, 0)
        self.assertIsNotNone(result)


class TestGbaseTranslateStructureBranches(IsolatedTestCase):
    """gbase8s/base.py: translate_select_structure 的提前返回分支"""

    def _compiler(self):
        """构造 oracle 模式的编译器"""
        dialect = GBase8sDialect()
        dialect.sqlmode = "oracle"
        return dialect.statement_compiler(dialect, None)

    def test_already_gbase_rownum_returns_as_is(self) -> None:
        """已标记 _gbase8s_rownum 的语句原样返回"""
        from sqlalchemy import select

        stmt = select(1)
        stmt._gbase8s_rownum = True  # pylint: disable=protected-access
        compiler = self._compiler()
        self.assertIs(compiler.translate_select_structure(stmt), stmt)

    def test_no_row_limiting_returns_as_is(self) -> None:
        """无 LIMIT/OFFSET 时原样返回"""
        from sqlalchemy import select

        stmt = select(1)
        compiler = self._compiler()
        self.assertIs(compiler.translate_select_structure(stmt), stmt)


class TestGbaseColumnDefaultBranch(IsolatedTestCase):
    """gbase8s/base.py: 列默认值的字符串/表达式分支"""

class TestGbaseForeignKeySkipBranch(IsolatedTestCase):
    """gbase8s/base.py: 外键行缺远端表/属主时跳过"""
