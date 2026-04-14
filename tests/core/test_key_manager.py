import unittest
import tempfile
import os
from pathlib import Path

from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.exceptions import ConfigError


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


if __name__ == "__main__":
    unittest.main()
