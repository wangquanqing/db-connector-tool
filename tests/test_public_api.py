"""公共 API 表面测试

确保模块根导出的名字与文档（README / TUTORIAL / docstring）中的用法一致，
避免文档写得出、实际 import 不成立。
"""

import unittest

import src.db_connector_tool as dct


class TestPublicApi(unittest.TestCase):
    """模块根导出与文档示例"""

    # 文档中出现的根级导入名
    DOCUMENTED_NAMES = (
        # 核心管理类
        "BatchDatabaseManager",
        "ConfigManager",
        "CryptoManager",
        "DatabaseManager",
        "KeyManager",
        "SQLAlchemyDriver",
        # 工具函数
        "cleanup_temp_configs",
        "generate_ip_range",
        # 异常体系（README 与 exceptions 模块 docstring 均如此导入）
        "ConfigError",
        "CryptoError",
        "DatabaseError",
        "DBConnectionError",
        "DBConnectorError",
        "DBTimeoutError",
        "DriverError",
        "FileSystemError",
        "QueryError",
        "ValidationError",
        # 验证器
        "ConfigValidator",
        "GenericValidator",
        "PasswordValidator",
        # 工具类与函数
        "LogManager",
        "PathHelper",
        "get_logger",
        "set_log_level",
        "setup_logging",
    )

    def test_documented_names_are_exported(self) -> None:
        """文档中的根级导入名都能取到"""
        for name in self.DOCUMENTED_NAMES:
            with self.subTest(name=name):
                self.assertTrue(
                    hasattr(dct, name), f"db_connector_tool 未导出 {name}"
                )
                self.assertIn(name, dct.__all__)

    def test_all_entries_resolve(self) -> None:
        """__all__ 中的每个名字都真实存在且无重复"""
        self.assertEqual(len(dct.__all__), len(set(dct.__all__)), "__all__ 存在重复项")
        for name in dct.__all__:
            with self.subTest(name=name):
                self.assertTrue(hasattr(dct, name), f"__all__ 中的 {name} 不存在")

    def test_exception_hierarchy_importable(self) -> None:
        """异常继承关系与导出保持一致"""
        self.assertTrue(issubclass(dct.ConfigError, dct.DBConnectorError))
        self.assertTrue(issubclass(dct.DBConnectionError, dct.DatabaseError))
        self.assertTrue(issubclass(dct.QueryError, dct.DatabaseError))
        self.assertTrue(issubclass(dct.DatabaseError, dct.DBConnectorError))

    def test_version_available(self) -> None:
        """版本号可读取"""
        from src.db_connector_tool.__about__ import __version__

        self.assertRegex(__version__, r"^\d+\.\d+\.\d+$")


if __name__ == "__main__":
    unittest.main()
