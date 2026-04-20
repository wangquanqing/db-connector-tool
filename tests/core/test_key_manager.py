import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import tomli_w

import src.db_connector_tool.core.key_manager as km
from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.exceptions import ConfigError, CryptoError
from src.db_connector_tool.core.key_manager import KeyManager


class TestKeyManager(unittest.TestCase):
    """测试 KeyManager 类的功能"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.app_name = f"test_app_{id(self)}"
        self.original_env = os.environ.copy()

    def tearDown(self) -> None:
        """清理测试环境"""
        os.environ.clear()
        os.environ.update(self.original_env)
        self.temp_dir.cleanup()

    def test_init(self) -> None:
        """测试初始化 KeyManager"""
        key_manager = KeyManager(self.app_name)
        self.assertIsInstance(key_manager, KeyManager)
        self.assertEqual(key_manager.app_name, self.app_name)

    def test_get_crypto_manager(self) -> None:
        """测试获取加密管理器"""
        key_manager = KeyManager(self.app_name)
        # 加载密钥
        key_manager.load_or_create_key()
        crypto = key_manager.get_crypto_manager()
        self.assertIsInstance(crypto, CryptoManager)
        key_manager.close()

    def test_rotate_encryption_key(self) -> None:
        """测试轮换加密密钥"""
        key_manager = KeyManager(self.app_name)
        # 加载密钥
        key_manager.load_or_create_key()
        # 轮换密钥
        new_key = key_manager.rotate_key()
        # 验证密钥已创建
        self.assertIsInstance(new_key, dict)
        self.assertIn("password", new_key)
        self.assertIn("salt", new_key)
        key_manager.close()

    def test_get_secure_hmac_key(self) -> None:
        """测试获取安全的HMAC密钥"""
        key_manager = KeyManager(self.app_name)
        # 加载密钥
        key_manager.load_or_create_key()
        hmac_key = key_manager.get_secure_hmac_key()
        self.assertIsInstance(hmac_key, bytes)
        self.assertEqual(len(hmac_key), 32)
        key_manager.close()

    def test_context_manager(self) -> None:
        """测试上下文管理器功能"""
        key_manager = KeyManager(self.app_name)
        self.assertIsInstance(key_manager, KeyManager)
        key_manager.close()

    def test_close_method(self) -> None:
        """测试关闭方法"""
        key_manager = KeyManager(self.app_name)
        # 加载密钥
        key_manager.load_or_create_key()
        # 确保加密管理器已初始化
        crypto = key_manager.get_crypto_manager()
        self.assertIsInstance(crypto, CryptoManager)

        key_manager.close()
        # 验证加密管理器已被清理
        with self.assertRaises(ConfigError):
            key_manager.get_crypto_manager()

    def test_str_representation(self) -> None:
        """测试 __str__ 方法"""
        key_manager = KeyManager(self.app_name)
        str_repr = str(key_manager)
        self.assertIn("KeyManager", str_repr)
        key_manager.close()

    def test_repr_representation(self) -> None:
        """测试 __repr__ 方法"""
        key_manager = KeyManager(self.app_name)
        repr_repr = repr(key_manager)
        self.assertIn("KeyManager", repr_repr)
        key_manager.close()

    def test_load_or_create_key(self) -> None:
        """测试加载或创建密钥"""
        key_manager = KeyManager(self.app_name)
        # 加载或创建密钥
        key_manager.load_or_create_key()
        # 验证加密管理器已初始化
        crypto = key_manager.get_crypto_manager()
        self.assertIsInstance(crypto, CryptoManager)
        key_manager.close()

    def test_load_or_create_key_from_file(self) -> None:
        """测试从文件加载或创建密钥"""
        key_manager = KeyManager(self.app_name)
        # 调用私有方法测试文件存储
        key_manager._load_or_create_key_from_file()
        # 验证加密管理器已初始化
        crypto = key_manager.get_crypto_manager()
        self.assertIsInstance(crypto, CryptoManager)
        key_manager.close()

    def test_load_crypto_from_key_data(self) -> None:
        """测试从密钥数据加载加密管理器"""
        key_manager = KeyManager(self.app_name)
        # 创建一个临时的加密管理器获取密钥数据
        crypto = CryptoManager()
        key_data = crypto.get_key_info()
        crypto.close()

        # 测试加载密钥数据
        key_manager._load_crypto_from_key_data(key_data)
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_create_new_crypto_key(self) -> None:
        """测试创建新的加密密钥"""
        key_manager = KeyManager(self.app_name)
        key_data = key_manager._create_new_crypto_key()
        self.assertIsInstance(key_data, dict)
        self.assertIn("password", key_data)
        self.assertIn("salt", key_data)
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_save_new_key_secure(self) -> None:
        """测试安全保存新密钥"""
        key_manager = KeyManager(self.app_name)
        # 创建新的密钥数据
        key_data = {"password": "test_password", "salt": "test_salt"}

        # 模拟 keyring 不可用，确保使用文件存储
        with mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_available", False
        ):
            # 测试保存新密钥
            key_manager._save_new_key_secure(key_data)
            # 验证密钥文件已创建
            key_file = key_manager.config_dir / "encryption.key"
            self.assertTrue(key_file.exists())

        key_manager.close()

    def test_set_secure_file_permissions(self) -> None:
        """测试设置文件安全权限"""
        key_manager = KeyManager(self.app_name)
        # 创建临时文件
        test_file = Path(self.temp_dir.name) / "test_file.txt"
        test_file.write_text("test")
        # 测试设置权限
        key_manager._set_secure_file_permissions(test_file)
        # 验证文件存在
        self.assertTrue(test_file.exists())
        key_manager.close()

    def test_handle_crypto_error(self) -> None:
        """测试处理加密错误"""
        key_manager = KeyManager(self.app_name)
        # 创建临时密钥文件
        key_file = key_manager.config_dir / "encryption.key"
        key_file.parent.mkdir(parents=True, exist_ok=True)
        key_file.write_text("invalid data")
        # 测试处理加密错误
        with mock.patch(
            "src.db_connector_tool.core.key_manager.CryptoManager.from_saved_key",
            side_effect=CryptoError("Decryption failed"),
        ):
            key_manager._handle_crypto_error(key_file, CryptoError("Decryption failed"))
        # 验证加密管理器已初始化
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_get_secure_hmac_key_no_crypto(self) -> None:
        """测试在加密管理器未初始化时获取HMAC密钥"""
        key_manager = KeyManager(self.app_name)
        # 不加载密钥，直接获取HMAC密钥
        hmac_key = key_manager.get_secure_hmac_key()
        self.assertIsInstance(hmac_key, bytes)
        self.assertEqual(len(hmac_key), 32)
        key_manager.close()

    def test_get_secure_hmac_key_from_env(self) -> None:
        """测试从环境变量获取HMAC密钥"""
        os.environ["DB_CONNECTOR_TOOL_HMAC_KEY"] = "a" * 64  # 32字节的十六进制
        key_manager = KeyManager(self.app_name)
        hmac_key = key_manager.get_secure_hmac_key()
        self.assertIsInstance(hmac_key, bytes)
        self.assertEqual(len(hmac_key), 32)
        key_manager.close()

    def test_load_existing_key(self) -> None:
        """测试加载现有的密钥文件"""
        key_manager = KeyManager(self.app_name)
        # 创建一个有效的密钥文件
        key_file = key_manager.config_dir / "encryption.key"
        key_file.parent.mkdir(parents=True, exist_ok=True)
        # 先创建一个加密管理器获取密钥数据
        crypto = CryptoManager()
        key_data = crypto.get_key_info()
        crypto.close()
        # 写入密钥文件
        key_file.write_bytes(tomli_w.dumps(key_data).encode("utf-8"))
        # 测试加载现有密钥
        key_manager._load_existing_key(key_file)
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_create_new_key(self) -> None:
        """测试创建新的密钥文件"""
        key_manager = KeyManager(self.app_name)
        # 创建新的密钥文件
        key_file = key_manager.config_dir / "encryption.key"
        key_manager._create_new_key(key_file)
        # 验证文件已创建
        self.assertTrue(key_file.exists())
        # 验证加密管理器已初始化
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_check_dependencies(self) -> None:
        """测试检查依赖项"""
        # 重置依赖检查状态
        KeyManager._dependencies_checked = False
        KeyManager._env_key = None
        KeyManager._env_key_available = None
        # 测试依赖检查
        KeyManager._check_dependencies()
        self.assertTrue(KeyManager._dependencies_checked)

    def test_ensure_dependencies_checked(self) -> None:
        """测试确保依赖检查已完成"""
        # 重置依赖检查状态
        KeyManager._dependencies_checked = False
        KeyManager._dependency_check_lock = None
        key_manager = KeyManager(self.app_name)
        # 测试依赖检查
        key_manager._ensure_dependencies_checked()
        self.assertTrue(KeyManager._dependencies_checked)
        key_manager.close()

    def test_load_or_create_key_with_env_key(self) -> None:
        """测试使用环境变量密钥"""
        # 创建一个有效的环境变量密钥
        crypto = CryptoManager()
        key_data = crypto.get_key_info()
        crypto.close()
        os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"] = json.dumps(key_data)
        # 重置依赖检查状态
        KeyManager._dependencies_checked = False
        KeyManager._env_key = None
        KeyManager._env_key_available = None
        # 测试加载密钥
        key_manager = KeyManager(self.app_name)
        key_manager.load_or_create_key()
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_load_or_create_key_with_invalid_env_key(self) -> None:
        """测试使用无效的环境变量密钥"""
        # 设置无效的环境变量密钥
        os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"] = "invalid json"
        # 重置依赖检查状态
        KeyManager._dependencies_checked = False
        KeyManager._env_key = None
        KeyManager._env_key_available = None
        # 测试加载密钥（应该回退到文件存储）
        key_manager = KeyManager(self.app_name)
        key_manager.load_or_create_key()
        self.assertIsInstance(key_manager.crypto, CryptoManager)
        key_manager.close()

    def test_load_or_create_key_from_keyring(self) -> None:
        """测试从keyring加载或创建密钥"""
        # 创建mock keyring模块
        mock_keyring = mock.Mock()
        crypto = CryptoManager()
        key_data = crypto.get_key_info()
        crypto.close()

        with mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_available", True
        ):
            with mock.patch(
                "src.db_connector_tool.core.key_manager.keyring_module", mock_keyring
            ):
                # 测试从keyring创建新密钥（没有现有密钥）
                mock_keyring.get_password.return_value = None
                key_manager = KeyManager(self.app_name)
                key_manager._load_or_create_key_from_keyring()
                self.assertIsInstance(key_manager.crypto, CryptoManager)
                mock_keyring.set_password.assert_called_once()
                key_manager.close()

                # 测试从keyring加载现有密钥
                mock_keyring.get_password.reset_mock()
                mock_keyring.set_password.reset_mock()
                mock_keyring.get_password.return_value = json.dumps(key_data)
                key_manager2 = KeyManager(self.app_name)
                key_manager2._load_or_create_key_from_keyring()
                self.assertIsInstance(key_manager2.crypto, CryptoManager)
                mock_keyring.get_password.assert_called_once()
                mock_keyring.set_password.assert_not_called()
                key_manager2.close()

    def test_load_or_create_key_env_config_error(self) -> None:
        """测试环境变量密钥加载ConfigError的情况"""
        # 创建有效的密钥数据但模拟ConfigError
        crypto = CryptoManager()
        key_data = crypto.get_key_info()
        crypto.close()

        os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"] = json.dumps(key_data)
        # 重置依赖检查状态
        KeyManager._dependencies_checked = False
        KeyManager._env_key = None
        KeyManager._env_key_available = None

        with mock.patch(
            "src.db_connector_tool.core.key_manager.KeyManager._check_dependencies"
        ):
            KeyManager._env_key = os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"]
            KeyManager._env_key_available = True
            with mock.patch(
                "src.db_connector_tool.core.key_manager.keyring_available", False
            ):
                with mock.patch(
                    "src.db_connector_tool.core.key_manager.CryptoManager.from_saved_key",
                    side_effect=ConfigError("Invalid key"),
                ):
                    key_manager = KeyManager(self.app_name)
                    key_manager.load_or_create_key()
                    self.assertIsInstance(key_manager.crypto, CryptoManager)
                    key_manager.close()

    def test_handle_crypto_error_delete_failure(self) -> None:
        """测试处理加密错误时删除文件失败的情况"""
        key_manager = KeyManager(self.app_name)
        key_file = key_manager.config_dir / "encryption.key"
        key_file.parent.mkdir(parents=True, exist_ok=True)
        key_file.write_text("invalid")

        with mock.patch(
            "src.db_connector_tool.core.key_manager.Path.unlink",
            side_effect=OSError("Delete failed"),
        ):
            with self.assertRaises(ConfigError):
                key_manager._handle_crypto_error(
                    key_file, CryptoError("Decryption failed")
                )

        key_manager.close()

    def test_save_new_key_secure_keyring(self) -> None:
        """测试使用keyring安全保存新密钥"""
        key_manager = KeyManager(self.app_name)
        key_data = {"password": "test", "salt": "test_salt"}

        mock_keyring = mock.Mock()
        with mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_available", True
        ):
            with mock.patch(
                "src.db_connector_tool.core.key_manager.keyring_module", mock_keyring
            ):
                key_manager._save_new_key_secure(key_data)
                mock_keyring.set_password.assert_called_once()

        key_manager.close()

    def test_save_new_key_secure_env_available(self) -> None:
        """测试环境变量可用时保存新密钥"""
        key_manager = KeyManager(self.app_name)
        key_data = {"password": "test", "salt": "test_salt"}

        with mock.patch(
            "src.db_connector_tool.core.key_manager.keyring_available", False
        ):
            with mock.patch(
                "src.db_connector_tool.core.key_manager.KeyManager._env_key_available",
                True,
            ):
                key_manager._save_new_key_secure(key_data)

        key_file = key_manager.config_dir / "encryption.key"
        self.assertTrue(key_file.exists())
        key_manager.close()

    def test_check_dependencies_invalid_env_key_format(self) -> None:
        """测试依赖检查时环境变量密钥格式无效的情况"""
        KeyManager._dependencies_checked = False
        KeyManager._env_key = None
        KeyManager._env_key_available = None

        # 测试缺少password或salt的情况
        os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"] = json.dumps(
            {"only_password": "test"}
        )
        KeyManager._check_dependencies()
        self.assertFalse(KeyManager._env_key_available)

        # 重置
        KeyManager._dependencies_checked = False
        KeyManager._env_key = None
        KeyManager._env_key_available = None

        # 测试无效JSON的情况
        os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"] = "not json"
        KeyManager._check_dependencies()
        self.assertFalse(KeyManager._env_key_available)

        # 清理
        del os.environ["DB_CONNECTOR_TOOL_ENCRYPTION_KEY"]

    def test_load_existing_key_crypto_error(self) -> None:
        """测试加载现有密钥时发生CryptoError的情况"""
        key_manager = KeyManager(self.app_name)
        key_file = key_manager.config_dir / "encryption.key"
        key_file.parent.mkdir(parents=True, exist_ok=True)

        crypto = CryptoManager()
        key_data = crypto.get_key_info()
        crypto.close()

        key_file.write_bytes(tomli_w.dumps(key_data).encode("utf-8"))

        with mock.patch(
            "src.db_connector_tool.core.key_manager.CryptoManager.from_saved_key",
            side_effect=CryptoError("Decryption failed"),
        ):
            with mock.patch(
                "src.db_connector_tool.core.key_manager.KeyManager._handle_crypto_error"
            ) as mock_handle:
                key_manager._load_existing_key(key_file)
                mock_handle.assert_called_once()

        key_manager.close()

    def test_load_crypto_from_invalid_key_data(self) -> None:
        """测试加载无效的密钥数据"""
        key_manager = KeyManager(self.app_name)

        with self.assertRaises(ConfigError):
            key_manager._load_crypto_from_key_data({"invalid": "data"})

        key_manager.close()

    def test_handle_config_operation_exceptions(self) -> None:
        """测试handle_config_operation装饰器的各种异常处理"""
        key_manager = KeyManager(self.app_name)

        @KeyManager.handle_config_operation("test_operation")
        def raise_os_error(self):
            raise OSError("OS error")

        @KeyManager.handle_config_operation("test_operation")
        def raise_json_error(self):
            raise json.JSONDecodeError("Invalid JSON", "", 0)

        @KeyManager.handle_config_operation("test_operation")
        def raise_type_error(self):
            raise TypeError("Type error")

        @KeyManager.handle_config_operation("test_operation")
        def raise_value_error(self):
            raise ValueError("Value error")

        @KeyManager.handle_config_operation("test_operation")
        def raise_attribute_error(self):
            raise AttributeError("Attribute error")

        @KeyManager.handle_config_operation("test_operation")
        def raise_runtime_error(self):
            raise RuntimeError("Runtime error")

        @KeyManager.handle_config_operation("test_operation")
        def raise_memory_error(self):
            raise MemoryError("Memory error")

        @KeyManager.handle_config_operation("test_operation")
        def raise_generic_error(self):
            raise Exception("Generic error")

        with self.assertRaises(ConfigError):
            raise_os_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_json_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_type_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_value_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_attribute_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_runtime_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_memory_error(key_manager)

        with self.assertRaises(ConfigError):
            raise_generic_error(key_manager)

        key_manager.close()

    def test_keyring_module_available(self) -> None:
        """测试keyring模块可用的情况"""
        # 这个测试主要是为了覆盖导入keyring的代码
        # 只是确认模块导入正常
        self.assertTrue(hasattr(km, "keyring_available"))

    def test_ensure_dependencies_checked_locks(self) -> None:
        """测试依赖检查的锁机制"""
        KeyManager._dependencies_checked = False
        KeyManager._dependency_check_lock = None

        key_manager1 = KeyManager(self.app_name)
        key_manager1._ensure_dependencies_checked()

        # 再次调用应该不会重新检查
        KeyManager._dependencies_checked = False
        key_manager2 = KeyManager(self.app_name)
        key_manager2._ensure_dependencies_checked()

        key_manager1.close()
        key_manager2.close()


if __name__ == "__main__":
    unittest.main()
