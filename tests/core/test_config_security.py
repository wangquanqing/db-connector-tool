import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from src.db_connector_tool.core.config_security import ConfigSecurityManager
from src.db_connector_tool.core.exceptions import ConfigError
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
        security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_valid_signature(self) -> None:
        """测试验证有效的配置签名"""
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
        # 生成签名并添加到配置
        signature = security_manager.generate_config_signature(config)
        config["metadata"]["signature"] = signature
        config["metadata"]["signature_timestamp"] = datetime.now(
            timezone.utc
        ).isoformat()

        # 验证签名
        security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_invalid_signature(self) -> None:
        """测试验证无效的配置签名"""
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
                "signature": "invalid_signature",
                "signature_timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }

        # 验证签名应该失败
        with self.assertRaises(ConfigError):
            security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_no_crypto_manager(self) -> None:
        """测试加密管理器未初始化时验证签名"""
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
                "signature": "some_signature",
            },
        }

        # 模拟加密管理器未初始化
        self.key_manager.crypto = None
        with self.assertRaises(ConfigError):
            security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_expired_timestamp(self) -> None:
        """测试签名时间戳过期时验证签名"""
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
        # 生成签名并添加到配置
        signature = security_manager.generate_config_signature(config)
        config["metadata"]["signature"] = signature
        # 设置过期时间戳（超过1小时）
        expired_time = datetime.now(timezone.utc)
        expired_time = expired_time - timedelta(hours=2)
        config["metadata"]["signature_timestamp"] = expired_time.isoformat()

        # 验证签名应该失败
        with self.assertRaises(ConfigError):
            security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_invalid_timestamp(self) -> None:
        """测试无效签名时间戳时验证签名"""
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
        # 生成签名并添加到配置
        signature = security_manager.generate_config_signature(config)
        config["metadata"]["signature"] = signature
        config["metadata"]["signature_timestamp"] = "invalid_timestamp"

        # 验证签名应该失败
        with self.assertRaises(ConfigError):
            security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_no_timestamp(self) -> None:
        """测试没有签名时间戳时验证签名"""
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
        # 生成签名并添加到配置，但不添加时间戳
        signature = security_manager.generate_config_signature(config)
        config["metadata"]["signature"] = signature

        # 验证签名应该失败
        with self.assertRaises(ConfigError):
            security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_timestamp_no_timezone(self) -> None:
        """测试无时区信息的签名时间戳"""
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
        # 生成签名并添加到配置
        signature = security_manager.generate_config_signature(config)
        config["metadata"]["signature"] = signature
        # 设置无时区信息的时间戳
        config["metadata"]["signature_timestamp"] = datetime.now().isoformat()

        # 验证签名应该成功
        security_manager.verify_config_signature(config)

    def test_verify_config_signature_with_general_exception(self) -> None:
        """测试验证签名时发生一般异常"""
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
                "signature": "some_signature",
                "signature_timestamp": datetime.now(timezone.utc).isoformat(),
            },
        }

        # 模拟 tomli_w.dumps 抛出异常
        with patch(
            "src.db_connector_tool.core.config_security.tomli_w.dumps",
            side_effect=ValueError("模拟异常"),
        ):
            with self.assertRaises(ConfigError):
                security_manager.verify_config_signature(config)

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

    def test_add_audit_log_entry_no_existing_audit_log(self) -> None:
        """测试添加审计日志条目时不存在 audit_log 的情况"""
        security_manager = ConfigSecurityManager(self.key_manager)
        # 生成配置数据，metadata 中没有 audit_log
        config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
            },
        }

        # 添加审计日志条目
        current_time = "2024-01-01T00:00:00"
        security_manager.add_audit_log_entry(config, "test_operation", current_time)
        self.assertEqual(len(config["metadata"]["audit_log"]), 1)
        self.assertEqual(
            config["metadata"]["audit_log"][0]["operation"], "test_operation"
        )

    def test_add_audit_log_entry_over_100_entries(self) -> None:
        """测试添加超过 100 条审计日志条目的情况"""
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

        # 添加 101 条审计日志条目
        for i in range(101):
            current_time = f"2024-01-01T00:00:{i:02d}"
            security_manager.add_audit_log_entry(config, f"operation_{i}", current_time)

        # 应该只保留最后 100 条
        self.assertEqual(len(config["metadata"]["audit_log"]), 100)
        # 第一条应该是 operation_1
        self.assertEqual(config["metadata"]["audit_log"][0]["operation"], "operation_1")
        # 最后一条应该是 operation_100
        self.assertEqual(
            config["metadata"]["audit_log"][-1]["operation"], "operation_100"
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

    def test_deserialize_value_various_types(self) -> None:
        """测试 _deserialize_value 方法支持各种类型"""
        security_manager = ConfigSecurityManager(self.key_manager)

        # 测试 bool 类型
        bool_str = '{"type": "bool", "value": true}'
        result = security_manager._deserialize_value(bool_str)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

        # 测试 float 类型
        float_str = '{"type": "float", "value": 3.14}'
        result = security_manager._deserialize_value(float_str)
        self.assertIsInstance(result, float)
        self.assertEqual(result, 3.14)

        # 测试其他类型
        other_str = '{"type": "list", "value": [1, 2, 3]}'
        result = security_manager._deserialize_value(other_str)
        self.assertEqual(result, [1, 2, 3])

    def test_deserialize_value_with_invalid_json(self) -> None:
        """测试 _deserialize_value 方法处理无效 JSON"""
        security_manager = ConfigSecurityManager(self.key_manager)

        with self.assertRaises(ConfigError):
            security_manager._deserialize_value("invalid json")

    def test_deserialize_value_missing_type_or_value(self) -> None:
        """测试 _deserialize_value 方法处理缺少类型或值的情况"""
        security_manager = ConfigSecurityManager(self.key_manager)

        with self.assertRaises(ConfigError):
            security_manager._deserialize_value('{"type": "int"}')

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

    def test_perform_key_rotation_with_error_rollback(self) -> None:
        """测试密钥轮换失败时的回滚逻辑"""
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

        # 保存原始配置用于比较
        original_config = {
            "connections": config["connections"].copy(),
            "key_version": config["metadata"]["key_version"],
        }

        # 模拟密钥轮换过程中的错误
        with patch.object(
            self.key_manager, "rotate_key", side_effect=Exception("模拟错误")
        ):
            with self.assertRaises(ConfigError):
                security_manager.perform_key_rotation(config)

        # 验证配置已回滚
        self.assertEqual(config["connections"], original_config["connections"])
        self.assertEqual(
            config["metadata"]["key_version"], original_config["key_version"]
        )

    def test_perform_key_rotation_with_key_restore_error(self) -> None:
        """测试密钥轮换失败时恢复密钥也失败的情况"""
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

        # 模拟密钥轮换过程中的错误，并模拟 CryptoManager
        import sys
        from unittest.mock import MagicMock

        # 创建模拟的 CryptoManager
        mock_crypto_module = MagicMock()
        mock_crypto_class = MagicMock()
        mock_crypto_class.from_saved_key.side_effect = Exception("恢复密钥失败")
        mock_crypto_module.CryptoManager = mock_crypto_class

        # 模拟密钥轮换过程中的错误
        with patch.object(
            self.key_manager, "rotate_key", side_effect=Exception("模拟错误")
        ):
            # 临时替换 sys.modules 中的 crypto 模块
            original_crypto = sys.modules.get("src.db_connector_tool.core.crypto")
            sys.modules["src.db_connector_tool.core.crypto"] = mock_crypto_module

            try:
                with self.assertRaises(ConfigError):
                    security_manager.perform_key_rotation(config)
            finally:
                # 恢复原始模块
                if original_crypto:
                    sys.modules["src.db_connector_tool.core.crypto"] = original_crypto
                else:
                    del sys.modules["src.db_connector_tool.core.crypto"]

    def test_perform_key_rotation_without_original_key_info(self) -> None:
        """测试密钥轮换失败时没有原始密钥信息的情况"""
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

        # 先加密连接配置（需要 crypto）
        encrypted_connections = security_manager.encrypt_dict_values(
            config["connections"]["test_db"]
        )
        config["connections"]["test_db"] = encrypted_connections

        # 保存原始的 crypto 对象
        original_crypto = self.key_manager.crypto

        try:
            # 临时设置 key_manager.crypto 为 None
            self.key_manager.crypto = None

            # 同时模拟 _decrypt_all_connections 方法，这样不会因为没有 crypto 而失败
            with patch.object(
                security_manager,
                "_decrypt_all_connections",
                return_value={"test_db": {}},
            ):
                # 模拟密钥轮换过程中的错误
                with patch.object(
                    self.key_manager, "rotate_key", side_effect=Exception("模拟错误")
                ):
                    with self.assertRaises(ConfigError):
                        security_manager.perform_key_rotation(config)
        finally:
            # 恢复原始的 crypto 对象
            self.key_manager.crypto = original_crypto

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
