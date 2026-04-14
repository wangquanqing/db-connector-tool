import os
import tempfile
import unittest
from pathlib import Path

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
            with self.assertRaises(ValueError):
                config_manager.add_config("", {"host": "localhost"})

            # 测试非字符串连接名称
            with self.assertRaises(ValueError):
                config_manager.add_config(123, {"host": "localhost"})  # type: ignore

    def test_invalid_connection_config(self) -> None:
        """测试无效的连接配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试空配置
            with self.assertRaises(ValueError):
                config_manager.add_config("test_db", {})

            # 测试非字典配置
            with self.assertRaises(ValueError):
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


class TestConfigManagerAdvanced(unittest.TestCase):
    """测试 ConfigManager 的高级功能和内部方法"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.app_name = f"test_app_advanced_{id(self)}"
        self.config_file = "test_connections.toml"
        self.original_env = os.environ.copy()

    def tearDown(self) -> None:
        """清理测试环境"""
        os.environ.clear()
        os.environ.update(self.original_env)
        self.temp_dir.cleanup()

    def test_str_representation(self) -> None:
        """测试 __str__ 方法"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            str_repr = str(config_manager)
            self.assertIn("ConfigManager", str_repr)
            self.assertIn(self.app_name, str_repr)

    def test_repr_representation(self) -> None:
        """测试 __repr__ 方法"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            repr_repr = repr(config_manager)
            self.assertIn("ConfigManager", repr_repr)
            self.assertIn(self.app_name, repr_repr)
            self.assertIn(self.config_file, repr_repr)

    def test_close_method(self) -> None:
        """测试 close 方法"""
        config_manager = ConfigManager(self.app_name, self.config_file)
        # 确保加密管理器已初始化
        crypto = config_manager.key_manager.get_crypto_manager()
        self.assertIsNotNone(crypto)
        config_manager.close()
        # 验证加密管理器已被清理
        with self.assertRaises(ConfigError):
            config_manager.key_manager.get_crypto_manager()

    def test_validate_connection_name_length(self) -> None:
        """测试连接名称长度验证"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试过长的连接名称
            long_name = "a" * 51
            with self.assertRaises(ValueError):
                config_manager.add_config(long_name, {"host": "localhost"})

    def test_validate_connection_name_characters(self) -> None:
        """测试连接名称字符验证"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试包含特殊字符的连接名称
            with self.assertRaises(ValueError):
                config_manager.add_config("test-name", {"host": "localhost"})

    def test_validate_connection_name_reserved(self) -> None:
        """测试连接名称保留字验证"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试保留字
            reserved_names = ["default", "test", "backup"]
            for name in reserved_names:
                with self.assertRaises(ValueError):
                    config_manager.add_config(name, {"host": "localhost"})

    def test_validate_connection_config_keys(self) -> None:
        """测试连接配置键验证"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 测试非字符串键
            invalid_config = {123: "value"}
            with self.assertRaises(ValueError):
                config_manager.add_config("test_db", invalid_config)  # type: ignore

    def test_parse_version_parts(self) -> None:
        """测试解析版本号各部分"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            major, minor, patch = config_manager._parse_version_parts("1.2.3")
            self.assertEqual(major, 1)
            self.assertEqual(minor, 2)
            self.assertEqual(patch, 3)

    def test_increment_version_parts(self) -> None:
        """测试递增版本号"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 普通递增
            major, minor, patch = config_manager._increment_version_parts(1, 0, 0)
            self.assertEqual((major, minor, patch), (1, 0, 1))

            # 进位测试
            major, minor, patch = config_manager._increment_version_parts(1, 0, 9)
            self.assertEqual((major, minor, patch), (1, 1, 0))

            major, minor, patch = config_manager._increment_version_parts(1, 9, 9)
            self.assertEqual((major, minor, patch), (2, 0, 0))

    def test_increment_config_version(self) -> None:
        """测试递增配置版本号"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            config = config_manager._load_config()
            original_version = config["version"]

            config_manager._increment_config_version(config)
            new_version = config["version"]

            self.assertNotEqual(original_version, new_version)

    def test_backup_config_with_custom_path(self) -> None:
        """测试使用自定义路径备份配置"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 添加一些配置
            config_manager.add_config("test_db", {"host": "localhost"})

            # 使用自定义路径备份
            custom_backup = Path(self.temp_dir.name) / "custom_backup.toml"
            backup_path = config_manager.backup_config(custom_backup)

            self.assertTrue(backup_path.exists())
            self.assertEqual(backup_path, custom_backup)

    def test_get_audit_log(self) -> None:
        """测试获取审计日志"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 先获取初始审计日志长度
            initial_len = len(config_manager.get_audit_log())

            # 执行一些操作
            config_manager.add_config("test_db", {"host": "localhost"})
            config_manager.update_config("test_db", {"host": "newhost"})
            config_manager.remove_config("test_db")

            # 获取审计日志
            audit_log = config_manager.get_audit_log()

            self.assertIsInstance(audit_log, list)
            self.assertEqual(len(audit_log), initial_len + 3)

            # 验证最近的操作类型
            self.assertEqual(audit_log[-3]["operation"], "add")
            self.assertEqual(audit_log[-2]["operation"], "update")
            self.assertEqual(audit_log[-1]["operation"], "remove")

    def test_increment_config_version_invalid_format(self) -> None:
        """测试递增无效格式的版本号"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            config = config_manager._load_config()

            # 设置无效的版本号格式
            config["version"] = "invalid.version"

            # 递增版本号（应该重置为 1.0.0）
            config_manager._increment_config_version(config)

            self.assertEqual(config["version"], "1.0.0")

    def test_increment_config_version_exceptions(self) -> None:
        """测试版本号递增时的异常处理"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            config = config_manager._load_config()

            # 设置会导致异常的版本号
            config["version"] = None  # type: ignore

            # 这应该不会抛出异常，只是记录警告
            config_manager._increment_config_version(config)

    def test_config_cache(self) -> None:
        """测试配置缓存机制"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 添加一个配置
            config_manager.add_config("test_db", {"host": "localhost"})

            # 第一次加载，会从文件读取
            config1 = config_manager._load_config()

            # 第二次加载，应该使用缓存
            config2 = config_manager._load_config()

            self.assertEqual(config1, config2)

    def test_advanced_features(self) -> None:
        """测试高级功能（减少 ConfigManager 实例创建）"""
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 1. 测试密钥轮换功能
            config_manager.add_config("test_db", {"host": "localhost", "port": 5432})
            old_version = config_manager.get_key_version()
            new_version = config_manager.rotate_encryption_key()
            self.assertNotEqual(old_version, new_version)
            self.assertEqual(int(new_version), int(old_version) + 1)
            config = config_manager.get_config("test_db")
            self.assertEqual(config["host"], "localhost")

            # 2. 测试默认路径备份配置
            backup_path = config_manager.backup_config()
            self.assertTrue(backup_path.exists())
            self.assertTrue(".backup." in backup_path.name)

            # 3. 测试版本号递增的主版本边界
            config_data = config_manager._load_config()
            config_data["version"] = "9.9.9"
            with self.assertRaises(ConfigError):
                config_manager._increment_config_version(config_data)

            # 4. 测试确保连接存在
            with self.assertRaises(ConfigError):
                config_manager._ensure_connection_exists(config_data, "nonexistent")

            # 5. 测试解析无效的版本号
            with self.assertRaises(IndexError):
                config_manager._parse_version_parts("1.2")


if __name__ == "__main__":
    unittest.main()
