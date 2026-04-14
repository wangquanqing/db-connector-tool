"""测试连接池管理模块

测试 ConnectionPoolManager 类的核心功能，包括连接管理、状态检查和统计信息。
"""

import unittest
from unittest.mock import Mock

from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.connections import DatabaseError


class TestConnectionPoolManager(unittest.TestCase):
    """测试连接池管理器类"""

    def setUp(self):
        """设置测试环境"""
        self.pool_manager = ConnectionPoolManager()

    def test_initialization(self):
        """测试连接池管理器初始化"""
        self.assertIsInstance(self.pool_manager, ConnectionPoolManager)
        stats = self.pool_manager.get_statistics()
        self.assertEqual(stats["connections_created"], 0)
        self.assertEqual(stats["connections_closed"], 0)
        self.assertEqual(stats["connection_errors"], 0)

    def test_add_and_get_connection(self):
        """测试添加和获取连接"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 获取连接
        retrieved_driver = self.pool_manager.get_connection(connection_name)
        self.assertEqual(retrieved_driver, mock_driver)

        # 验证统计信息
        stats = self.pool_manager.get_statistics()
        self.assertEqual(stats["connections_created"], 1)

    def test_remove_connection(self):
        """测试移除连接"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 移除连接
        self.pool_manager.remove_connection(connection_name)

        # 验证连接已被移除
        retrieved_driver = self.pool_manager.get_connection(connection_name)
        self.assertIsNone(retrieved_driver)

        # 验证统计信息
        stats = self.pool_manager.get_statistics()
        self.assertEqual(stats["connections_closed"], 1)

    def test_connection_pool_status(self):
        """测试连接池状态信息"""
        status = self.pool_manager.get_connection_pool_status()
        self.assertIn("active_connections", status)
        self.assertIn("pool_size", status)
        self.assertIn("average_response_time", status)
        self.assertIn("error_rate", status)
        self.assertIn("connection_details", status)
        self.assertIn("statistics", status)

    def test_cleanup_idle_connections(self):
        """测试清理空闲连接"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 清理空闲连接（使用较短的超时时间）
        cleaned_count = self.pool_manager.cleanup_idle_connections(max_idle_time=0)
        self.assertEqual(cleaned_count, 1)

    def test_record_connection_error(self):
        """测试记录连接错误"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 记录错误
        test_error = DatabaseError("Test error")
        self.pool_manager.record_connection_error(connection_name, test_error)

        # 验证错误已记录
        stats = self.pool_manager.get_statistics()
        self.assertEqual(stats["connection_errors"], 1)

    def test_close_all_connections(self):
        """测试关闭所有连接"""
        # 创建模拟的驱动实例
        mock_driver1 = Mock()
        mock_driver1.test_connection.return_value = True
        mock_driver2 = Mock()
        mock_driver2.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db1", mock_driver1)
        self.pool_manager.add_connection("test_db2", mock_driver2)

        # 关闭所有连接
        success_count, error_count = self.pool_manager.close_all_connections()

        # 验证所有连接已关闭
        self.assertEqual(success_count, 2)
        self.assertEqual(error_count, 0)


if __name__ == "__main__":
    unittest.main()
