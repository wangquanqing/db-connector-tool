"""公共 API 表面测试

不从文档里“手抄”一份名字清单，而是**直接扫描文档与 docstring** 中出现的
``from db_connector_tool import ...`` 语句，逐个断言其可导入，
从而保证「文档写得出、实际导得进」。
"""

import re
import unittest
from pathlib import Path

import src.db_connector_tool as dct
from src.db_connector_tool import core, utils
from src.db_connector_tool.__about__ import __version__

REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_FILES = ("README.md", "TUTORIAL.md")
ROOT_IMPORT_PATTERN = re.compile(
    r"^from db_connector_tool import ([A-Za-z_][\w]*(?:\s*,\s*[A-Za-z_][\w]*)*)",
    re.MULTILINE,
)


def _documented_root_imports() -> set:
    """收集文档与源码 docstring 中出现的根级导入名"""
    sources = [REPO_ROOT / name for name in DOC_FILES]
    sources.extend(sorted((REPO_ROOT / "src").rglob("*.py")))

    names = set()
    for path in sources:
        text = path.read_text(encoding="utf-8")
        for match in ROOT_IMPORT_PATTERN.findall(text):
            names.update(part.strip() for part in match.split(",") if part.strip())
    return names


class TestPublicApi(unittest.TestCase):
    """模块根导出与文档示例"""

    def test_documented_names_are_exported(self) -> None:
        """文档/docstring 里出现的根级导入名都必须可取到"""
        documented = _documented_root_imports()
        self.assertTrue(documented, "未能从文档中提取到任何根级导入（正则失效？）")

        missing = sorted(name for name in documented if not hasattr(dct, name))
        self.assertEqual(missing, [], f"文档引用了未导出的名字: {missing}")

    def test_all_entries_resolve(self) -> None:
        """__all__ 中的每个名字都真实存在且无重复"""
        self.assertEqual(len(dct.__all__), len(set(dct.__all__)), "__all__ 存在重复项")
        for name in dct.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(dct, name), f"__all__ 中的 {name} 不存在")

    def test_core_exports_are_reexported(self) -> None:
        """core 与 utils 的公共导出在包根同样可用"""
        for name in core.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(dct, name), f"core 导出的 {name} 未在包根导出")
        for name in utils.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(dct, name), f"utils 导出的 {name} 未在包根导出")

    def test_exception_hierarchy_importable(self) -> None:
        """异常继承关系与导出保持一致"""
        self.assertTrue(issubclass(dct.ConfigError, dct.DBConnectorError))
        self.assertTrue(issubclass(dct.DBConnectionError, dct.DatabaseError))
        self.assertTrue(issubclass(dct.QueryError, dct.DatabaseError))
        self.assertTrue(issubclass(dct.DatabaseError, dct.DBConnectorError))

    def test_version_available(self) -> None:
        """版本号可读取"""
        self.assertRegex(__version__, r"^\d+\.\d+\.\d+$")


if __name__ == "__main__":
    unittest.main()
