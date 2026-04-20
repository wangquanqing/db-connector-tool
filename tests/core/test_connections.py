"""测试数据库连接管理模块

测试 DatabaseManager 类的核心功能，包括上下文管理、连接操作和异常处理。
"""

import unittest
from unittest.mock import Mock, patch

from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectionError,
)


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
            "database": "test_db",
        }

        db_manager.add_connection("test_db", connection_config)

        # 验证配置管理器的 add_config 方法被调用
        mock_config_instance.add_config.assert_called_once_with(
            "test_db", connection_config
        )

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
            "database": "test_db",
        }

        db_manager.update_connection("test_db", new_config)

        # 验证连接池管理器的 remove_connection 方法被调用
        mock_pool_instance.remove_connection.assert_called_once_with("test_db")
        # 验证配置管理器的 update_config 方法被调用
        mock_config_instance.update_config.assert_called_once_with(
            "test_db", new_config
        )

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
            "database": "test_db",
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
        affected_rows = db_manager.execute_command(
            "test_db", "UPDATE users SET name = 'test' WHERE id = 1"
        )

        # 验证 get_connection 方法被调用
        db_manager.get_connection.assert_called_once_with("test_db")
        # 验证驱动的 execute_command 方法被调用
        mock_driver.execute_command.assert_called_once_with(
            "UPDATE users SET name = 'test' WHERE id = 1", None
        )
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

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_cleanup_idle_connections(self, mock_pool_manager, mock_config_manager):
        """测试清理空闲连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.cleanup_idle_connections.return_value = 2

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试清理空闲连接
        cleaned_count = db_manager.cleanup_idle_connections(600)

        # 验证连接池的 cleanup_idle_connections 被调用
        mock_pool_instance.cleanup_idle_connections.assert_called_once_with(600)
        # 验证返回的结果
        self.assertEqual(cleaned_count, 2)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_get_connection_info(self, mock_pool_manager, mock_config_manager):
        """测试获取连接信息"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {
            "use_count": 10,
            "connection_errors": 0,
        }

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试获取连接信息
        info = db_manager.get_connection_info("test_db")

        # 验证配置管理器的 get_config 被调用
        mock_config_instance.get_config.assert_called_once_with("test_db")
        # 验证连接池的 get_connection_info 被调用
        mock_pool_instance.get_connection_info.assert_called_once_with("test_db")
        # 验证返回的信息
        self.assertEqual(info["type"], "mysql")
        self.assertEqual(info["host"], "localhost")
        self.assertEqual(info["use_count"], 10)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection(self, mock_pool_manager, mock_config_manager):
        """测试诊断连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {}

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 模拟驱动实例
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True

        # 模拟 get_connection 方法
        db_manager.get_connection = Mock(return_value=mock_driver)

        # 测试诊断连接
        diagnosis = db_manager.diagnose_connection("test_db")

        # 验证返回的诊断信息
        self.assertEqual(diagnosis["connection_name"], "test_db")
        self.assertEqual(diagnosis["status"], "healthy")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_add_connection_already_exists(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试添加已存在的连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试添加已存在的连接
        connection_config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "database": "test_db",
        }

        # 验证抛出 ConfigError
        with self.assertRaises(ConfigError):
            db_manager.add_connection("test_db", connection_config)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_remove_connection_not_exists(self, mock_pool_manager, mock_config_manager):
        """测试删除不存在的连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = []

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试删除不存在的连接
        with self.assertRaises(ConfigError):
            db_manager.remove_connection("test_db")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_str_and_repr(self, mock_pool_manager, mock_config_manager):
        """测试 __str__ 和 __repr__ 方法"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db1", "test_db2"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_statistics.return_value = {
            "connection_pool_size": 10,
            "active_connections": 5,
            "connections_created": 20,
            "uptime": 100.0,
        }

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试 __str__ 方法
        str_representation = str(db_manager)
        self.assertIn("test_app", str_representation)
        self.assertIn("2 connections", str_representation)

        # 测试 __repr__ 方法
        repr_representation = repr(db_manager)
        self.assertIn("test_app", repr_representation)
        self.assertIn("test_connections.toml", repr_representation)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_handle_connection_error(self, mock_pool_manager, mock_config_manager):
        """测试处理连接错误"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试不同类型的连接错误
        error_types = [
            ("timeout", "连接超时"),
            ("refused", "连接被拒绝"),
            ("unreachable", "主机不可达"),
            ("permission", "连接失败（权限错误）"),
            ("access denied", "连接失败（权限错误）"),
            ("database not found", "连接失败（数据库不存在）"),
            ("unknown error", "连接失败（未知错误）"),
        ]

        for error_msg, expected_msg in error_types:
            with self.subTest(error_msg=error_msg):
                with self.assertRaises(DBConnectionError) as cm:
                    db_manager._handle_connection_error(Exception(error_msg))
                self.assertIn(expected_msg, str(cm.exception))

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_test_failure(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试诊断连接时的测试失败"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {}

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 模拟驱动测试失败
        mock_driver = Mock()
        mock_driver.test_connection.return_value = False
        db_manager.get_connection = Mock(return_value=mock_driver)

        # 测试诊断连接
        diagnosis = db_manager.diagnose_connection("test_db")
        self.assertEqual(diagnosis["status"], "unhealthy")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_get_connection_from_pool(self, mock_pool_manager, mock_config_manager):
        """测试从连接池获取连接"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试从连接池获取连接
        mock_driver = Mock()
        mock_pool_instance.get_connection.return_value = mock_driver

        result = db_manager._get_connection_from_pool("test_db")
        self.assertEqual(result, mock_driver)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_str_method_with_exception(self, mock_pool_manager, mock_config_manager):
        """测试 __str__ 方法在异常情况下的行为"""
        # 模拟配置管理器和连接池管理器
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        # 模拟抛出 ConfigError 异常
        mock_config_instance.list_configs.side_effect = ConfigError(
            "List configs failed"
        )

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        # 创建数据库管理器
        db_manager = DatabaseManager(self.app_name, self.config_file)

        # 测试 __str__ 方法
        str_representation = str(db_manager)
        self.assertIn("test_app", str_representation)
        self.assertIn("test_connections.toml", str_representation)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_init_exception(self, mock_pool_manager, mock_config_manager):
        """测试 __init__ 方法的异常处理"""
        # 模拟配置管理器抛出异常
        mock_config_manager.side_effect = OSError("Config file error")

        with self.assertRaises(DatabaseError):
            DatabaseManager(self.app_name, self.config_file)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_close_all_connections_with_connections(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 close_all_connections 方法关闭有连接的情况"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.close_all_connections.return_value = (5, 1)

        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.close_all_connections()

        mock_pool_instance.close_all_connections.assert_called_once()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_close_all_connections_exception(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 close_all_connections 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.close_all_connections.side_effect = OSError("Close error")

        db_manager = DatabaseManager(self.app_name, self.config_file)
        with self.assertRaises(DatabaseError):
            db_manager.close_all_connections()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_safe_operation_exception(self, mock_pool_manager, mock_config_manager):
        """测试 _safe_operation 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        db_manager = DatabaseManager(self.app_name, self.config_file)

        def failing_func():
            raise OSError("Test error")

        with self.assertRaises(DatabaseError):
            db_manager._safe_operation("test", "name", failing_func)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_get_connection_exception(self, mock_pool_manager, mock_config_manager):
        """测试 get_connection 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager._validate_connection_exists = Mock()
        db_manager._get_connection_from_pool = Mock(
            side_effect=OSError("Connection error")
        )

        with self.assertRaises(DBConnectionError):
            db_manager.get_connection("test_db")

        mock_pool_instance.record_connection_error.assert_called_once()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_test_connection_failure(self, mock_pool_manager, mock_config_manager):
        """测试 test_connection 方法的失败情况"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        mock_driver = Mock()
        mock_driver.test_connection.return_value = False

        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(return_value=mock_driver)

        result = db_manager.test_connection("test_db")

        self.assertFalse(result)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_test_connection_exception(self, mock_pool_manager, mock_config_manager):
        """测试 test_connection 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(side_effect=OSError("Connection error"))

        result = db_manager.test_connection("test_db")

        self.assertFalse(result)
        mock_pool_instance.record_connection_error.assert_called_once()
        mock_pool_instance.remove_connection.assert_called_once_with("test_db")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_execute_query_exception(self, mock_pool_manager, mock_config_manager):
        """测试 execute_query 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(side_effect=OSError("Query error"))

        with self.assertRaises(DatabaseError):
            db_manager.execute_query("test_db", "SELECT * FROM users")

        mock_pool_instance.record_connection_error.assert_called_once()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_execute_command_exception(self, mock_pool_manager, mock_config_manager):
        """测试 execute_command 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance

        db_manager = DatabaseManager(self.app_name, self.config_file)
        db_manager.get_connection = Mock(side_effect=OSError("Command error"))

        with self.assertRaises(DatabaseError):
            db_manager.execute_command("test_db", "UPDATE users SET name = 'test'")

        mock_pool_instance.record_connection_error.assert_called_once()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_cleanup_idle_connections_exception(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 cleanup_idle_connections 方法的异常处理"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.cleanup_idle_connections.side_effect = OSError(
            "Cleanup error"
        )

        db_manager = DatabaseManager(self.app_name, self.config_file)
        with self.assertRaises(DatabaseError):
            db_manager.cleanup_idle_connections()

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_config_error(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 diagnose_connection 方法的配置错误"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.side_effect = ConfigError("Config error")

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {}

        db_manager = DatabaseManager(self.app_name, self.config_file)
        diagnosis = db_manager.diagnose_connection("test_db")

        self.assertEqual(diagnosis["status"], "error")
        self.assertIn("config", diagnosis["details"])
        self.assertFalse(diagnosis["details"]["config"]["valid"])

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_general_error(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 diagnose_connection 方法的一般错误"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {}

        db_manager = DatabaseManager(self.app_name, self.config_file)

        def mock_diagnose_connection(name, diagnosis):
            raise OSError("General error")

        db_manager._diagnose_config = Mock()
        db_manager._diagnose_connection = Mock(side_effect=OSError("General error"))

        diagnosis = db_manager.diagnose_connection("test_db")

        self.assertEqual(diagnosis["status"], "error")
        self.assertIn("general_error", diagnosis["details"])

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_get_connection_from_pool_creates_new(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 _get_connection_from_pool 方法创建新连接"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.list_configs.return_value = ["test_db"]
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection.return_value = None

        db_manager = DatabaseManager(self.app_name, self.config_file)

        with patch.object(db_manager, "_create_new_connection") as mock_create_new:
            mock_driver = Mock()
            mock_create_new.return_value = mock_driver

            result = db_manager._get_connection_from_pool("test_db")

            mock_create_new.assert_called_once_with("test_db")
            self.assertEqual(result, mock_driver)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_test_exception(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 diagnose_connection 方法的连接测试异常"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {}

        db_manager = DatabaseManager(self.app_name, self.config_file)
        mock_driver = Mock()
        mock_driver.test_connection.side_effect = DBConnectionError("Test error")
        db_manager.get_connection = Mock(return_value=mock_driver)

        diagnosis = db_manager.diagnose_connection("test_db")

        self.assertEqual(diagnosis["status"], "unhealthy")
        self.assertIn("test", diagnosis["details"])
        self.assertFalse(diagnosis["details"]["test"]["success"])

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_with_pool_info(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 diagnose_connection 方法包含连接池信息"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {
            "use_count": 10,
            "connection_errors": 5,
            "last_error": "test error",
            "response_time": 0.5,
        }

        db_manager = DatabaseManager(self.app_name, self.config_file)
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True
        db_manager.get_connection = Mock(return_value=mock_driver)

        diagnosis = db_manager.diagnose_connection("test_db")

        self.assertTrue(diagnosis["details"]["pool_info"]["is_active"])
        self.assertEqual(diagnosis["details"]["pool_info"]["use_count"], 10)
        self.assertEqual(diagnosis["details"]["pool_info"]["connection_errors"], 5)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_without_pool_info(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 diagnose_connection 方法没有连接池信息的情况"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = None

        db_manager = DatabaseManager(self.app_name, self.config_file)
        mock_driver = Mock()
        mock_driver.test_connection.return_value = True
        db_manager.get_connection = Mock(return_value=mock_driver)

        diagnosis = db_manager.diagnose_connection("test_db")

        self.assertFalse(diagnosis["details"]["pool_info"]["is_active"])
        self.assertEqual(diagnosis["details"]["pool_info"]["use_count"], 0)
        self.assertEqual(diagnosis["details"]["pool_info"]["connection_errors"], 0)

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_add_pool_info(self, mock_pool_manager, mock_config_manager):
        """测试 _add_pool_info 方法"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = None

        db_manager = DatabaseManager(self.app_name, self.config_file)
        diagnosis = {"details": {}}
        db_manager._add_pool_info("test_db", diagnosis)

        self.assertIn("pool_info", diagnosis["details"])
        self.assertFalse(diagnosis["details"]["pool_info"]["is_active"])
        mock_pool_instance.get_connection_info.assert_called_once_with("test_db")

    @patch("src.db_connector_tool.core.connections.ConfigManager")
    @patch("src.db_connector_tool.core.connections.ConnectionPoolManager")
    def test_diagnose_connection_exception(
        self, mock_pool_manager, mock_config_manager
    ):
        """测试 _diagnose_connection 方法的异常情况"""
        mock_config_instance = Mock()
        mock_config_manager.return_value = mock_config_instance
        mock_config_instance.get_config.return_value = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
        }
        mock_config_instance.list_configs.return_value = ["test_db"]

        mock_pool_instance = Mock()
        mock_pool_manager.return_value = mock_pool_instance
        mock_pool_instance.get_connection_info.return_value = {}

        db_manager = DatabaseManager(self.app_name, self.config_file)
        diagnosis = {"details": {}}

        db_manager.get_connection = Mock(side_effect=OSError("Connection error"))

        db_manager._diagnose_connection("test_db", diagnosis)

        self.assertEqual(diagnosis["status"], "unhealthy")
        self.assertFalse(diagnosis["details"]["connection"]["established"])
        self.assertIn("error", diagnosis["details"]["connection"])


if __name__ == "__main__":
    unittest.main()
