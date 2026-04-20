"""测试连接池管理模块

测试 ConnectionPoolManager 类的核心功能，包括连接管理、状态检查和统计信息。
"""

import time
import unittest
from unittest.mock import MagicMock, Mock

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

    def test_remove_nonexistent_connection(self):
        """测试移除不存在的连接"""
        # 尝试移除不存在的连接，应该不会抛出异常
        try:
            self.pool_manager.remove_connection("non_existent")
        except Exception as e:
            self.fail(f"remove_connection 应该不会抛出异常，但抛出了: {e}")

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

    def test_update_metadata_nonexistent_connection(self):
        """测试更新不存在连接的元数据"""
        # 尝试更新不存在连接的元数据，应该不会抛出异常
        try:
            self.pool_manager.update_query_metadata("non_existent", 0.1)
            self.pool_manager.update_command_metadata("non_existent", 0.2)
        except Exception as e:
            self.fail(f"更新不存在连接的元数据应该不会抛出异常，但抛出了: {e}")

    def test_calculate_average_response_time(self):
        """测试计算平均响应时间"""
        # 测试有连接的情况
        avg_time = self.pool_manager._calculate_average_response_time(10.0, 5)
        self.assertAlmostEqual(avg_time, 2.0)

        # 测试空连接池的情况
        avg_time = self.pool_manager._calculate_average_response_time(10.0, 0)
        self.assertEqual(avg_time, 0.0)

    def test_calculate_error_rate(self):
        """测试计算错误率"""
        # 测试有查询的情况
        error_rate = self.pool_manager._calculate_error_rate(5, 100)
        self.assertAlmostEqual(error_rate, 0.05)

        # 测试无查询的情况
        error_rate = self.pool_manager._calculate_error_rate(5, 0)
        self.assertEqual(error_rate, 0.0)

    def test_build_pool_status_response(self):
        """测试构建连接池状态响应"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db", mock_driver)

        # 构建状态响应
        status_data = {
            "current_time": 1234567890.0,
            "pool_size": 1,
            "average_response_time": 0.1,
            "error_rate": 0.0,
            "connection_details": [],
        }
        response = self.pool_manager._build_pool_status_response(status_data)

        # 验证响应包含必要的字段
        self.assertIn("current_time", response)
        self.assertIn("pool_size", response)
        self.assertIn("active_connections", response)
        self.assertIn("average_response_time", response)
        self.assertIn("error_rate", response)
        self.assertIn("connection_details", response)
        self.assertIn("statistics", response)

    def test_cleanup_idle_connections_empty_pool(self):
        """测试清理空连接池的空闲连接"""
        # 清理空连接池的空闲连接
        cleaned_count = self.pool_manager.cleanup_idle_connections()
        self.assertEqual(cleaned_count, 0)

    def test_close_all_connections_empty_pool(self):
        """测试关闭空连接池的所有连接"""
        # 关闭空连接池的所有连接
        success_count, error_count = self.pool_manager.close_all_connections()
        self.assertEqual(success_count, 0)
        self.assertEqual(error_count, 0)

    def test_close_all_connections_with_partial_cleanup(self):
        """测试关闭所有连接时部分清理失败的情况"""
        # 创建模拟的驱动实例
        mock_driver1 = Mock()
        mock_driver1.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db1", mock_driver1)

        # 关闭所有连接
        success_count, error_count = self.pool_manager.close_all_connections()

        # 验证结果
        self.assertEqual(success_count, 1)
        self.assertEqual(error_count, 0)

    def test_process_idle_connections_without_metadata(self):
        """测试处理没有元数据的空闲连接"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 处理空闲连接
        current_time = time.time()
        cleaned_count = self.pool_manager._process_idle_connections(
            [connection_name], current_time, max_idle_time=0
        )
        self.assertEqual(cleaned_count, 1)

    def test_calculate_pool_stats(self):
        """测试计算连接池统计信息"""
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

        # 计算统计信息
        stats = self.pool_manager._calculate_pool_stats()
        self.assertIn("total_use_count", stats)
        self.assertIn("total_query_count", stats)
        self.assertIn("total_transaction_count", stats)
        self.assertIn("total_response_time", stats)
        self.assertIn("total_errors", stats)

    def test_get_connection_details(self):
        """测试获取连接详细信息"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db", mock_driver)

        # 获取连接详细信息
        current_time = time.time()
        details = self.pool_manager._get_connection_details(current_time)
        self.assertIsInstance(details, list)
        self.assertEqual(len(details), 1)
        self.assertIn("name", details[0])
        self.assertIn("idle_time", details[0])
        self.assertIn("use_count", details[0])
        self.assertIn("response_time", details[0])
        self.assertIn("connection_errors", details[0])
        self.assertIn("is_active", details[0])

    def test_close_all_connections_with_remove_connection_error(self):
        """测试关闭所有连接时 remove_connection 抛出 OSError/DatabaseError 的情况"""
        # 创建模拟的驱动实例
        mock_driver1 = Mock()
        mock_driver1.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db1", mock_driver1)

        original_remove = self.pool_manager.remove_connection

        def mock_remove(name):
            raise OSError("Test OS error")

        self.pool_manager.remove_connection = mock_remove

        # 关闭所有连接
        success_count, error_count = self.pool_manager._close_all_connections(
            ["test_db1"]
        )

        # 验证结果
        self.assertEqual(success_count, 0)
        self.assertEqual(error_count, 1)

        # 恢复原方法
        self.pool_manager.remove_connection = original_remove

    def test_remove_connection_from_pool_with_error(self):
        """测试从连接池移除连接时出现异常的情况"""
        # 创建模拟的驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        class ErrorDict(dict):
            def __delitem__(self, key):
                raise DatabaseError("Test error")

        # 替换连接池为我们的错误字典
        original_pool = self.pool_manager.connection_pool
        self.pool_manager.connection_pool = ErrorDict(original_pool)

        # 移除连接（应该不会抛出异常）
        try:
            self.pool_manager._remove_connection_from_pool(connection_name)
        except Exception as e:
            self.fail(f"_remove_connection_from_pool 应该不会抛出异常，但抛出了: {e}")

        # 恢复原连接池
        self.pool_manager.connection_pool = original_pool

    def test_get_connection_invalid_connection(self):
        """测试获取无效连接时的清理"""
        # 创建模拟的驱动实例，先添加成功，然后让连接变为无效
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接（此时 test_connection 返回 True）
        connection_name = "test_db"
        self.pool_manager.add_connection(connection_name, mock_driver)

        # 现在让连接变为无效
        mock_driver.test_connection.return_value = False

        # 重新获取连接，应该返回 None 并清理
        retrieved = self.pool_manager.get_connection(connection_name)
        self.assertIsNone(retrieved)
        self.assertNotIn(connection_name, self.pool_manager.connection_pool)

    def test_record_connection_error_no_metadata(self):
        """测试记录没有元数据的连接的错误"""
        # 记录不存在连接的错误
        test_error = DatabaseError("Test error")
        self.pool_manager.record_connection_error("non_existent", test_error)

        # 验证统计信息仍然被记录
        stats = self.pool_manager.get_statistics()
        self.assertEqual(stats["connection_errors"], 1)

    def test_add_connection_without_test_connection(self):
        """测试添加没有 test_connection 方法的连接"""
        # 创建只有 engine 方法的驱动
        mock_driver = Mock(spec=["engine"])
        mock_driver.engine = Mock()
        # 确保没有 test_connection 方法
        if hasattr(mock_driver, "test_connection"):
            delattr(mock_driver, "test_connection")

        # 这种情况下，add_connection 不会检查 test_connection（因为没有），所以会成功添加
        self.pool_manager.add_connection("test_db", mock_driver)
        # 验证连接被添加
        self.assertIn("test_db", self.pool_manager.connection_pool)

    def test_process_idle_connections_various_metadata_cases(self):
        """测试处理空闲连接时的各种元数据情况"""
        # 创建模拟的驱动实例
        mock_driver1 = Mock()
        mock_driver1.test_connection.return_value = True
        mock_driver2 = Mock()
        mock_driver2.test_connection.return_value = True
        mock_driver3 = Mock()
        mock_driver3.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("db1", mock_driver1)
        self.pool_manager.add_connection("db2", mock_driver2)
        self.pool_manager.add_connection("db3", mock_driver3)

        # 移除 db2 的元数据中的 last_used
        del self.pool_manager._connection_metadata["db2"]["last_used"]
        # 移除 db3 的全部元数据
        del self.pool_manager._connection_metadata["db3"]

        current_time = time.time()
        # 处理空闲连接
        cleaned_count = self.pool_manager._process_idle_connections(
            ["db1", "db2", "db3"],
            current_time,
            max_idle_time=-1,  # 负数表示所有连接都超时
        )

        # 验证所有连接都被清理
        self.assertEqual(cleaned_count, 3)

    def test_build_pool_status_response_no_connections(self):
        """测试构建没有连接的连接池状态响应"""
        # 构建状态响应
        status_data = {
            "current_time": 1234567890.0,
            "pool_size": 0,
            "average_response_time": 0.0,
            "error_rate": 0.0,
            "connection_details": [],
        }
        response = self.pool_manager._build_pool_status_response(status_data)

        # 验证响应包含必要的字段且活跃连接数为 0
        self.assertIn("current_time", response)
        self.assertEqual(response["pool_size"], 0)
        self.assertEqual(response["active_connections"], 0)

    def test_process_idle_connection_not_in_pool(self):
        """测试处理不在连接池中的空闲连接"""
        # 创建模拟的驱动实例并添加
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True
        self.pool_manager.add_connection("test_db", mock_driver)

        # 处理一个不存在的连接
        current_time = time.time()
        cleaned_count = self.pool_manager._process_idle_connections(
            ["non_existent_db"], current_time, max_idle_time=300
        )
        # 应该不会清理任何连接
        self.assertEqual(cleaned_count, 0)

    def test_build_pool_status_response_with_invalid_connection(self):
        """测试构建包含无效连接的连接池状态响应"""
        # 创建模拟的驱动实例，先让 test_connection 返回 True 来成功添加
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 添加连接
        self.pool_manager.add_connection("test_db", mock_driver)

        # 现在让 test_connection 返回 False（确保它被检测为无效）
        mock_driver.test_connection.return_value = False

        # 构建状态响应
        status_data = {
            "current_time": 1234567890.0,
            "pool_size": 1,
            "average_response_time": 0.0,
            "error_rate": 0.0,
            "connection_details": [],
        }
        response = self.pool_manager._build_pool_status_response(status_data)

        # 验证活跃连接数为 0（因为连接无效）
        self.assertEqual(response["active_connections"], 0)


if __name__ == "__main__":
    unittest.main()
