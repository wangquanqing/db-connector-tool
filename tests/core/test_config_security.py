import os
import tempfile
import unittest

from src.db_connector_tool.core.config_security import ConfigSecurityManager
from src.db_connector_tool.core.key_manager import KeyManager


class TestConfigSecurityManager(unittest.TestCase):
    """测试 ConfigSecurityManager 类的功能"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.app_name = f"test_app_{id(self)}"
        self.original_env = os.environ.copy()
        # 创建KeyManager实例
        self.key_manager = KeyManager(self.app_name)
        # 加载密钥
        self.key_manager.load_or_create_key()

    def tearDown(self) -> None:
        """清理测试环境"""
        if hasattr(self, "key_manager") and self.key_manager:
            try:
                self.key_manager.close()
            except Exception:
                pass
        os.environ.clear()
        os.environ.update(self.original_env)
        self.temp_dir.cleanup()

    def test_init(self) -> None:
        """测试初始化 ConfigSecurityManager"""
        security_manager = ConfigSecurityManager(self.key_manager)
        self.assertIsInstance(security_manager, ConfigSecurityManager)
        self.assertEqual(security_manager.key_manager, self.key_manager)

    def test_validate_config_signature(self) -> None:
        """测试验证配置签名"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 生成配置数据
        config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
                "audit_log": [],
                "signature": "",
            },
        }

        # 验证没有签名的配置
        result = security_manager.verify_config_signature(config)
        self.assertTrue(result)

    def test_generate_config_signature(self) -> None:
        """测试生成配置签名"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 生成配置数据
        config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
                "audit_log": [],
            },
        }

        # 生成签名
        signature = security_manager.generate_config_signature(config)
        self.assertIsInstance(signature, str)
        self.assertEqual(len(signature), 64)  # SHA-256 哈希长度

    def test_add_audit_log_entry(self) -> None:
        """测试添加审计日志条目"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 生成配置数据
        config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
                "audit_log": [],
            },
        }

        # 添加审计日志条目
        current_time = "2024-01-01T00:00:00"
        security_manager.add_audit_log_entry(config, "test_operation", current_time)
        self.assertEqual(len(config["metadata"]["audit_log"]), 1)
        self.assertEqual(
            config["metadata"]["audit_log"][0]["operation"], "test_operation"
        )

    def test_encrypt_dict_values(self) -> None:
        """测试加密字典值"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 测试数据
        test_data = {
            "host": "localhost",
            "port": 5432,
            "username": "admin",
            "password": "secret",
        }

        # 加密数据
        encrypted = security_manager.encrypt_dict_values(test_data)
        self.assertIsInstance(encrypted, dict)
        for key, value in encrypted.items():
            self.assertIsInstance(value, str)
            self.assertNotEqual(value, test_data[key])

    def test_decrypt_dict_values(self) -> None:
        """测试解密字典值"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 测试数据
        test_data = {
            "host": "localhost",
            "port": 5432,
            "username": "admin",
            "password": "secret",
        }

        # 加密并解密数据
        encrypted = security_manager.encrypt_dict_values(test_data)
        decrypted = security_manager.decrypt_dict_values(encrypted)
        self.assertEqual(decrypted, test_data)

    def test_perform_key_rotation(self) -> None:
        """测试执行密钥轮换"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 生成配置数据
        config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {
                "test_db": {
                    "host": "localhost",
                    "port": 5432,
                    "username": "admin",
                    "password": "secret",
                }
            },
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
                "audit_log": [],
            },
        }

        # 先加密连接配置
        encrypted_connections = security_manager.encrypt_dict_values(
            config["connections"]["test_db"]
        )
        config["connections"]["test_db"] = encrypted_connections

        # 执行密钥轮换
        new_version = security_manager.perform_key_rotation(config)
        self.assertEqual(new_version, "2")
        self.assertEqual(config["metadata"]["key_version"], "2")

    def test_str_representation(self) -> None:
        """测试 __str__ 方法"""
        security_manager = ConfigSecurityManager(self.key_manager)
        str_repr = str(security_manager)
        self.assertIn("ConfigSecurityManager", str_repr)

    def test_repr_representation(self) -> None:
        """测试 __repr__ 方法"""
        security_manager = ConfigSecurityManager(self.key_manager)
        repr_repr = repr(security_manager)
        self.assertIn("ConfigSecurityManager", repr_repr)


if __name__ == "__main__":
    unittest.main()
