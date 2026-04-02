import os
import unittest
from pathlib import Path

from src.db_connector_tool.core.config import ConfigManager
from src.db_connector_tool.core.exceptions import ConfigError


class TestConfigManagerCoverage(unittest.TestCase):
    """测试 ConfigManager 的覆盖率"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.app_name = f"test_coverage_{id(self)}"
        self.config_file = "test_coverage.toml"
        self.original_env = os.environ.copy()

    def tearDown(self) -> None:
        """清理测试环境"""
        os.environ.clear()
        os.environ.update(self.original_env)

    def test_all_coverage(self) -> None:
        """综合测试所有覆盖率"""
        # 只创建一个 ConfigManager 实例
        with ConfigManager(self.app_name, self.config_file) as config_manager:
            # 1. 测试字符串表示
            str_repr = str(config_manager)
            self.assertIn("ConfigManager", str_repr)
            self.assertIn(self.app_name, str_repr)
            
            repr_repr = repr(config_manager)
            self.assertIn("ConfigManager", repr_repr)
            self.assertIn(self.app_name, repr_repr)
            
            # 2. 测试配置操作
            config_manager.add_config("test_db", {"host": "localhost", "port": 5432})
            config = config_manager.get_config("test_db")
            self.assertEqual(config["host"], "localhost")
            
            config_manager.update_config("test_db", {"host": "newhost"})
            config = config_manager.get_config("test_db")
            self.assertEqual(config["host"], "newhost")
            
            # 3. 测试密钥轮换
            old_version = config_manager.get_key_version()
            new_version = config_manager.rotate_encryption_key()
            self.assertNotEqual(old_version, new_version)
            
            # 4. 测试备份功能
            backup_path = config_manager.backup_config()
            self.assertTrue(backup_path.exists())
            
            # 5. 测试审计日志
            audit_log = config_manager.get_audit_log()
            self.assertIsInstance(audit_log, list)
            
            # 6. 测试版本号管理
            config_data = config_manager._load_config()
            original_version = config_data["version"]
            config_manager._increment_config_version(config_data)
            self.assertNotEqual(original_version, config_data["version"])
            
            # 7. 测试验证功能
            # 验证必需字段
            data = {"field1": "value1"}
            required_fields = ["field1", "field2"]
            with self.assertRaises(ConfigError):
                config_manager._validate_required_fields(data, required_fields, "测试数据")
            
            # 验证字段类型
            with self.assertRaises(ConfigError):
                config_manager._validate_field_type("string", int, "测试字段")
            
            # 9. 测试连接存在检查
            with self.assertRaises(ConfigError):
                config_manager._ensure_connection_exists(config_data, "nonexistent")
            
            # 8. 测试版本号边界（单独测试，不影响其他操作）
            test_config = config_manager._load_config().copy()
            test_config["version"] = "9.9.9"
            with self.assertRaises(ConfigError):
                config_manager._increment_config_version(test_config)
            
            # 10. 测试版本号解析
            with self.assertRaises(IndexError):
                config_manager._parse_version_parts("1.2")
            
            # 11. 测试版本号格式验证
            self.assertTrue(config_manager._is_valid_version_format("1.0.0"))
            self.assertFalse(config_manager._is_valid_version_format("1.0"))
            
            # 12. 测试 HMAC 密钥
            hmac_key = config_manager._get_secure_hmac_key()
            self.assertIsInstance(hmac_key, bytes)
            self.assertEqual(len(hmac_key), 32)
            
            # 13. 测试序列化和反序列化
            test_value = 42
            serialized = config_manager._serialize_value(test_value)
            deserialized = config_manager._deserialize_value(serialized)
            self.assertEqual(deserialized, test_value)
            
            # 14. 测试删除操作
            config_manager.remove_config("test_db")
            connections = config_manager.list_configs()
            self.assertNotIn("test_db", connections)


if __name__ == "__main__":
    unittest.main()
