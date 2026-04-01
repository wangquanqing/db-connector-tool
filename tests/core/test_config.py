import os
import tempfile
import unittest

from src.db_connector_tool.core.config import ConfigManager
from src.db_connector_tool.core.exceptions import ConfigError


class TestConfigManager(unittest.TestCase):
    """测试 ConfigManager 类的功能"""

    def setUp(self) -> None:
        """设置测试环境"""
        # 创建临时目录作为测试配置目录
        self.temp_dir = tempfile.TemporaryDirectory()
        # 为每个测试生成唯一的应用名称，确保测试之间不相互影响
        self.app_name = f"test_app_{id(self)}"
        self.config_file = "test_connections.toml"

        # 保存原始环境变量，以便在测试后恢复
        self.original_env = os.environ.copy()

    def tearDown(self) -> None:
        """清理测试环境"""
        # 恢复原始环境变量
        os.environ.clear()
        os.environ.update(self.original_env)
        # 清理临时目录
        self.temp_dir.cleanup()

    def test_init(self) -> None:
        """测试初始化 ConfigManager"""
        config_manager = ConfigManager(self.app_name, self.config_file)
        self.assertIsInstance(config_manager, ConfigManager)
        self.assertEqual(config_manager.app_name, self.app_name)
        self.assertEqual(config_manager.config_file, self.config_file)

    def test_add_config(self) -> None:
        """测试添加连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 验证配置已添加
            connections = config_manager.list_configs()
            self.assertIn("test_db", connections)

    def test_get_config(self) -> None:
        """测试获取连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 测试获取配置
            retrieved_config = config_manager.get_config("test_db")
            self.assertEqual(retrieved_config["host"], "localhost")
            self.assertEqual(retrieved_config["port"], 5432)
            self.assertEqual(retrieved_config["username"], "admin")
            self.assertEqual(retrieved_config["password"], "secret")

    def test_update_config(self) -> None:
        """测试更新连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 测试更新配置
            new_config = {
                "host": "newhost",
                "port": 5433,
                "username": "newadmin",
                "password": "newsecret",
            }
            config_manager.update_config("test_db", new_config)

            # 验证配置已更新
            retrieved_config = config_manager.get_config("test_db")
            self.assertEqual(retrieved_config["host"], "newhost")
            self.assertEqual(retrieved_config["port"], 5433)
            self.assertEqual(retrieved_config["username"], "newadmin")
            self.assertEqual(retrieved_config["password"], "newsecret")

    def test_remove_config(self) -> None:
        """测试删除连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 验证配置已添加
            connections = config_manager.list_configs()
            self.assertIn("test_db", connections)

            # 测试删除配置
            config_manager.remove_config("test_db")

            # 验证配置已删除
            connections = config_manager.list_configs()
            self.assertNotIn("test_db", connections)

    def test_list_configs(self) -> None:
        """测试列出所有连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加多个配置
            config1 = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config2 = {
                "host": "localhost",
                "port": 3306,
                "username": "root",
                "password": "password",
            }
            config_manager.add_config("postgres", config1)
            config_manager.add_config("mysql", config2)

            # 测试列出配置
            connections = config_manager.list_configs()
            self.assertEqual(len(connections), 2)
            self.assertIn("postgres", connections)
            self.assertIn("mysql", connections)

    def test_get_config_info(self) -> None:
        """测试获取配置信息"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试获取配置信息
            info = config_manager.get_config_info()
            self.assertIsInstance(info, dict)
            self.assertIn("version", info)
            self.assertIn("app_name", info)
            self.assertIn("connection_count", info)
            self.assertIn("created", info)
            self.assertIn("last_modified", info)
            self.assertIn("config_file", info)

    def test_backup_config(self) -> None:
        """测试备份配置文件"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 测试备份配置
            backup_path = config_manager.backup_config()
            self.assertTrue(backup_path.exists())
            self.assertTrue(backup_path.is_file())

    def test_get_key_version(self) -> None:
        """测试获取密钥版本"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试获取密钥版本
            key_version = config_manager.get_key_version()
            self.assertIsInstance(key_version, str)

    def test_rotate_encryption_key(self) -> None:
        """测试轮换加密密钥"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 测试轮换密钥
            old_version = config_manager.get_key_version()
            new_version = config_manager.rotate_encryption_key()
            self.assertNotEqual(old_version, new_version)

            # 验证配置仍然可以获取
            retrieved_config = config_manager.get_config("test_db")
            self.assertEqual(retrieved_config["host"], "localhost")

    def test_get_audit_log(self) -> None:
        """测试获取审计日志"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 测试获取审计日志
            audit_log = config_manager.get_audit_log()
            self.assertIsInstance(audit_log, list)
            self.assertGreater(len(audit_log), 0)

    def test_context_manager(self) -> None:
        """测试上下文管理器功能"""
        # 使用上下文管理器
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 添加配置
            test_config = {
                "host": "localhost",
                "port": 5432,
                "username": "admin",
                "password": "secret",
            }
            config_manager.add_config("test_db", test_config)

            # 验证配置已添加
            connections = config_manager.list_configs()
            self.assertIn("test_db", connections)

        # 上下文管理器退出后，验证配置仍然存在
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            connections = config_manager.list_configs()
            self.assertIn("test_db", connections)

    def test_invalid_connection_name(self) -> None:
        """测试无效的连接名称"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试空连接名称
            with self.assertRaises(ConfigError):
                config_manager.add_config("", {"host": "localhost"})

            # 测试非字符串连接名称
            with self.assertRaises(ConfigError):
                config_manager.add_config(123, {"host": "localhost"})  # type: ignore

    def test_invalid_connection_config(self) -> None:
        """测试无效的连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试空配置
            with self.assertRaises(ConfigError):
                config_manager.add_config("test_db", {})

            # 测试非字典配置
            with self.assertRaises(ConfigError):
                config_manager.add_config("test_db", "not a dict")  # type: ignore

    def test_nonexistent_connection(self) -> None:
        """测试不存在的连接"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试获取不存在的连接
            with self.assertRaises(ConfigError):
                config_manager.get_config("nonexistent")

            # 测试更新不存在的连接
            with self.assertRaises(ConfigError):
                config_manager.update_config("nonexistent", {"host": "localhost"})

            # 测试删除不存在的连接
            with self.assertRaises(ConfigError):
                config_manager.remove_config("nonexistent")


if __name__ == "__main__":
    unittest.main()
