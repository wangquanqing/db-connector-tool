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


    def test_update_metadata(self):
        """测试更新查询和命令元数据"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 更新查询元数据
        self.pool_manager.update_query_metadata(connection_name, 0.1)
        # 更新命令元数据
        self.pool_manager.update_command_metadata(connection_name, 0.2)

        # 获取连接信息
        info = self.pool_manager.get_connection_info(connection_name)
        self.assertEqual(info["query_count"], 1)
        self.assertEqual(info["transaction_count"], 1)
        self.assertAlmostEqual(info["response_time"], 0.2)

    def test_get_connection_info(self):
        """测试获取连接信息"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 获取连接信息
        info = self.pool_manager.get_connection_info(connection_name)
        self.assertIn("use_count", info)
        self.assertIn("last_used", info)
        self.assertIn("created_at", info)
        self.assertIn("is_active", info)
        self.assertTrue(info["is_active"])

        # 获取不存在连接的信息
        non_existent_info = self.pool_manager.get_connection_info("non_existent")
        self.assertEqual(non_existent_info, {})

    def test_is_connection_valid(self):
        """测试连接有效性检查"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 测试有效的连接
        is_valid = self.pool_manager._is_connection_valid(mock_driver)
        self.assertTrue(is_valid)

        # 测试无效的连接（test_connection 返回 False）
        mock_driver.test_connection.return_value = False
        is_valid = self.pool_manager._is_connection_valid(mock_driver)
        self.assertFalse(is_valid)

        # 测试没有 test_connection 方法但有 engine 属性的驱动（SQLAlchemy 驱动）
        mock_sqlalchemy_driver = Mock()
        mock_sqlalchemy_driver.test_connection = Mock(return_value=True)
        mock_sqlalchemy_driver.engine = Mock()
        mock_sqlalchemy_driver.engine.connect = Mock()
        is_valid = self.pool_manager._is_connection_valid(mock_sqlalchemy_driver)
        self.assertTrue(is_valid)

    def test_add_connection_invalid_driver(self):
        """测试添加无效驱动"""
        # 创建无效的驱动实例（缺少 test_connection 方法）
        invalid_driver = Mock()
        del invalid_driver.test_connection

        # 验证抛出 DatabaseError
        from src.db_connector_tool.core.exceptions import DatabaseError
        with self.assertRaises(DatabaseError):
            self.pool_manager.add_connection("test_db", invalid_driver)

    def test_add_connection_connection_failure(self):
        """测试添加连接时连接失败"""
        # 创建驱动实例，但 test_connection 抛出异常
        mock_driver = Mock()
        mock_driver.test_connection.side_effect = Exception("Connection failed")

        # 验证抛出 DatabaseError
        from src.db_connector_tool.core.exceptions import DatabaseError
        with self.assertRaises(DatabaseError):
            self.pool_manager.add_connection("test_db", mock_driver)

    def test_remove_nonexistent_connection(self):
        """测试移除不存在的连接"""
        # 尝试移除不存在的连接，应该不会抛出异常
        try:
            self.pool_manager.remove_connection("non_existent")
        except Exception as e:
            self.fail(f"remove_connection 应该不会抛出异常，但抛出了: {e}")

    def test_connection_pool_status(self):
        """测试连接池状态信息"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db", mock_driver)

        # 获取连接池状态
        status = self.pool_manager.get_connection_pool_status()
        self.assertIn("current_time", status)
        self.assertIn("pool_size", status)
        self.assertIn("active_connections", status)
        self.assertIn("average_response_time", status)
        self.assertIn("error_rate", status)
        self.assertIn("connection_details", status)
        self.assertIn("statistics", status)

    def test_calculate_statistics(self):
        """测试统计信息计算"""
        # 创建模拟的驱动实例
        mock_driver1 = Mock()
        mock_driver1.test_connection.return_value = True
        mock_driver2 = Mock()
        mock_driver2.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db1", mock_driver1)
        self.pool_manager.add_connection("test_db2", mock_driver2)

        # 更新元数据
        self.pool_manager.update_query_metadata("test_db1", 0.1)
        self.pool_manager.update_command_metadata("test_db2", 0.2)

        # 获取统计信息
        stats = self.pool_manager.get_statistics()
        self.assertEqual(stats["connections_created"], 2)
        self.assertEqual(stats["connection_pool_size"], 2)


if __name__ == "__main__":
    unittest.main()
