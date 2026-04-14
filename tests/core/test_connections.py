"""测试数据库连接管理模块

测试 DatabaseManager 类的核心功能，包括上下文管理。
"""

import unittest
from unittest.mock import Mock, patch

from src.db_connector_tool.core.connections import DatabaseManager


class TestDatabaseManager(unittest.TestCase):
    """测试数据库管理器类"""

    def setUp(self):
        """设置测试环境"""
        self.app_name = "test_app"
        self.config_file = "test_connections.toml"

    @patch("db_connector_tool.core.connections.ConfigManager")
    @patch("db_connector_tool.core.connections.ConnectionPoolManager")
    def test_context_manager(self, mock_pool_manager, mock_config_manager):
        """测试上下文管理器功能"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 测试 with 语句
        with DatabaseManager(self.app_name, self.config_file) as db_manager:
            # 验证数据库管理器被正确初始化
            self.assertEqual(db_manager.app_name, self.app_name)
            self.assertEqual(db_manager.config_file, self.config_file)

        # 验证退出上下文时 close_all_connections 被调用
        mock_pool_instance.close_all_connections.assert_called_once()

    @patch("db_connector_tool.core.connections.ConfigManager")
    @patch("db_connector_tool.core.connections.ConnectionPoolManager")
    def test_context_manager_with_exception(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试上下文管理器在异常情况下的行为"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 测试异常情况下的上下文管理器
        with self.assertRaises(ValueError):
            with DatabaseManager(self.app_name, self.config_file) as db_manager:
                # 验证数据库管理器被正确初始化
                self.assertEqual(db_manager.app_name, self.app_name)
                # 抛出异常
                raise ValueError("Test exception")

        # 验证即使发生异常，close_all_connections 也被调用
        mock_pool_instance.close_all_connections.assert_called_once()


if __name__ == "__main__":
    unittest.main()
