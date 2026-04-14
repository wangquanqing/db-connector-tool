"""测试数据库连接管理模块

测试 DatabaseManager 类的核心功能，包括上下文管理、连接操作和异常处理。
"""

import unittest
from unittest.mock import Mock, patch

from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import ConfigError, DatabaseError


class TestDatabaseManager(unittest.TestCase):
    """测试数据库管理器类"""

    def setUp(self):
        """设置测试环境"""
        self.app_name = "test_app"
        self.config_file = "test_connections.toml"

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_context_manager(self, mock_pool_manager, mock_config_manager):
        """测试上下文管理器功能"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        # 设置 close_all_connections 方法返回值
        mock_pool_instance.close_all_connections.return_value = (0, 0)

        # 测试 with 语句
        with DatabaseManager(self.app_name, self.config_file) as db_manager:
            # 验证数据库管理器被正确初始化
            self.assertEqual(db_manager.app_name, self.app_name)
            self.assertEqual(db_manager.config_file, self.config_file)

        # 验证退出上下文时 close_all_connections 被调用
        mock_pool_instance.close_all_connections.assert_called_once()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_context_manager_with_exception(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试上下文管理器在异常情况下的行为"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        # 设置 close_all_connections 方法返回值
        mock_pool_instance.close_all_connections.return_value = (0, 0)

        # 测试异常情况下的上下文管理器
        with self.assertRaises(ValueError):
            with DatabaseManager(self.app_name, self.config_file) as db_manager:
                # 验证数据库管理器被正确初始化
                self.assertEqual(db_manager.app_name, self.app_name)
                # 抛出异常
                raise ValueError("Test exception")

        # 验证即使发生异常，close_all_connections 也被调用
        mock_pool_instance.close_all_connections.assert_called_once()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_add_connection(self, mock_pool_manager, mock_config_manager):
        """测试添加连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = []

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)
        
        # 测试添加连接
        connection_config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "database": "test_db"
        }
        
        db_manager.add_connection("test_db", connection_config)
        
        # 验证配置管理器的 add_config 方法被调用
        mock_config_instance.add_config.assert_called_once_with("test_db", connection_config)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_remove_connection(self, mock_pool_manager, mock_config_manager):
        """测试移除连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)
        
        # 测试移除连接
        db_manager.remove_connection("test_db")
        
        # 验证连接池管理器的 remove_connection 方法被调用
        mock_pool_instance.remove_connection.assert_called_once_with("test_db")
        # 验证配置管理器的 remove_config 方法被调用
        mock_config_instance.remove_config.assert_called_once_with("test_db")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_update_connection(self, mock_pool_manager, mock_config_manager):
        """测试更新连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)
        
        # 测试更新连接
        new_config = {
            "type": "mysql",
            "host": "newhost",
            "port": 3306,
            "username": "root",
            "password": "newpassword",
            "database": "test_db"
        }
        
        db_manager.update_connection("test_db", new_config)
        
        # 验证连接池管理器的 remove_connection 方法被调用
        mock_pool_instance.remove_connection.assert_called_once_with("test_db")
        # 验证配置管理器的 update_config 方法被调用
        mock_config_instance.update_config.assert_called_once_with("test_db", new_config)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_show_connection(self, mock_pool_manager, mock_config_manager):
        """测试显示连接配置"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "database": "test_db"
        }

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)
        
        # 测试显示连接配置
        config = db_manager.show_connection("test_db")
        
        # 验证配置管理器的 get_config 方法被调用
        mock_config_instance.get_config.assert_called_once_with("test_db")
        # 验证返回的配置
        self.assertEqual(config["host"], "localhost")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_list_connections(self, mock_pool_manager, mock_config_manager):
        """测试列出所有连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db1", "test_db2"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)
        
        # 测试列出所有连接
        connections = db_manager.list_connections()
        
        # 验证配置管理器的 list_configs 方法被调用
        mock_config_instance.list_configs.assert_called_once()
        # 验证返回的连接列表
        self.assertEqual(connections, ["test_db1", "test_db2"])

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_execute_query(self, mock_pool_manager, mock_config_manager):
        """测试执行查询"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 模拟驱动实例
        mock_driver = Mock()
        mock_driver.execute_query.return_value = [{"id": 1, "name": "test"}]
        
        # 模拟 get_connection 方法
        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(return_value=mock_driver)
        
        # 测试执行查询
        result = db_manager.execute_query("test_db", "SELECT * FROM users")
        
        # 验证 get_connection 方法被调用
        db_manager.get_connection.assert_called_once_with("test_db")
        # 验证驱动的 execute_query 方法被调用
        mock_driver.execute_query.assert_called_once_with("SELECT * FROM users", None)
        # 验证返回的结果
        self.assertEqual(result, [{"id": 1, "name": "test"}])

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_execute_command(self, mock_pool_manager, mock_config_manager):
        """测试执行命令"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 模拟驱动实例
        mock_driver = Mock()
        mock_driver.execute_command.return_value = 1
        
        # 模拟 get_connection 方法
        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(return_value=mock_driver)
        
        # 测试执行命令
        affected_rows = db_manager.execute_command("test_db", "UPDATE users SET name = 'test' WHERE id = 1")
        
        # 验证 get_connection 方法被调用
        db_manager.get_connection.assert_called_once_with("test_db")
        # 验证驱动的 execute_command 方法被调用
        mock_driver.execute_command.assert_called_once_with("UPDATE users SET name = 'test' WHERE id = 1", None)
        # 验证返回的结果
        self.assertEqual(affected_rows, 1)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_test_connection(self, mock_pool_manager, mock_config_manager):
        """测试测试连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 模拟驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True
        
        # 模拟 get_connection 方法
        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(return_value=mock_driver)
        
        # 测试测试连接
        result = db_manager.test_connection("test_db")
        
        # 验证 get_connection 方法被调用
        db_manager.get_connection.assert_called_once_with("test_db")
        # 验证驱动的 test_connection 方法被调用
        mock_driver.test_connection.assert_called_once()
        # 验证返回的结果
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
