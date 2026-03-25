"""
CryptoManager 类的完整单元测试
"""

import base64
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.exceptions import CryptoError


class TestCryptoManager(unittest.TestCase):
    """CryptoManager 类的单元测试"""

    def setUp(self):
        """测试前的准备工作"""
        self.test_password = "My$trongP@ssw0rd123!"
        self.test_salt = b"test_salt_12345678"
        self.test_iterations = 100000
        self.test_data = "这是一个测试数据，包含中文和特殊字符!@#$%"

    def tearDown(self):
        """测试后的清理工作"""
        pass

    def test_initialization_with_default_parameters(self):
        """测试使用默认参数初始化"""
        crypto = CryptoManager()

        # 验证实例已正确创建
        self.assertIsNotNone(crypto)
        self.assertTrue(crypto.is_initialized())

        # 验证默认参数
        self.assertEqual(len(crypto.salt), CryptoManager.DEFAULT_SALT_LENGTH)
        self.assertGreaterEqual(len(crypto.password), 16)
        self.assertGreaterEqual(crypto.iterations, 100000)

    def test_initialization_with_custom_parameters(self):
        """测试使用自定义参数初始化"""
        crypto = CryptoManager(
            password=self.test_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        self.assertIsNotNone(crypto)
        self.assertEqual(crypto.password, self.test_password)
        self.assertEqual(crypto.salt, self.test_salt)
        self.assertEqual(crypto.iterations, self.test_iterations)

    def test_initialization_with_invalid_salt_length(self):
        """测试使用无效盐值长度初始化"""
        with self.assertRaises(ValueError):
            CryptoManager(salt=b"short_salt")

    def test_initialization_with_weak_password(self):
        """测试使用弱密码初始化"""
        weak_password = "weakpassword"
        with self.assertRaises(ValueError):
            CryptoManager(password=weak_password)

    def test_encrypt_decrypt_string(self):
        """测试字符串加密解密功能"""
        crypto = CryptoManager()

        # 加密
        encrypted = crypto.encrypt(self.test_data)
        self.assertIsInstance(encrypted, str)
        self.assertGreater(len(encrypted), 0)

        # 解密
        decrypted = crypto.decrypt(encrypted)
        self.assertEqual(decrypted, self.test_data)

    def test_encrypt_decrypt_bytes(self):
        """测试字节数据加密解密功能"""
        crypto = CryptoManager()
        test_bytes = b"test binary data"

        # 加密
        encrypted_bytes = crypto.encrypt_bytes(test_bytes)
        self.assertIsInstance(encrypted_bytes, bytes)
        self.assertGreater(len(encrypted_bytes), 0)

        # 解密
        decrypted_bytes = crypto.decrypt_bytes(encrypted_bytes)
        self.assertEqual(decrypted_bytes, test_bytes)

    def test_encrypt_empty_string(self):
        """测试加密空字符串"""
        crypto = CryptoManager()

        with self.assertRaises(ValueError):
            crypto.encrypt("")

    def test_decrypt_invalid_data(self):
        """测试解密无效数据"""
        crypto = CryptoManager()

        with self.assertRaises(ValueError):
            crypto.decrypt("")

        with self.assertRaises(CryptoError):
            # 无效的加密数据
            crypto.decrypt("invalid_encrypted_data")

    def test_decrypt_with_wrong_key(self):
        """测试使用错误密钥解密"""
        crypto1 = CryptoManager()
        crypto2 = CryptoManager()  # 不同的实例，不同的密钥

        encrypted = crypto1.encrypt(self.test_data)

        with self.assertRaises(CryptoError):
            crypto2.decrypt(encrypted)

    def test_get_key_info(self):
        """测试获取密钥信息"""
        crypto = CryptoManager(
            password=self.test_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        key_info = crypto.get_key_info()

        self.assertIn("salt", key_info)
        self.assertIn("password", key_info)
        self.assertIn("iterations", key_info)

        # 验证盐值编码正确
        salt_bytes = base64.urlsafe_b64decode(key_info["salt"].encode("utf-8"))
        self.assertEqual(salt_bytes, self.test_salt)

        # 验证密码正确
        self.assertEqual(key_info["password"], self.test_password)

        # 验证迭代次数正确
        self.assertEqual(key_info["iterations"], self.test_iterations)

    def test_from_saved_key(self):
        """测试从保存的密钥创建实例"""
        original_crypto = CryptoManager(
            password=self.test_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        key_info = original_crypto.get_key_info()

        # 从保存的密钥创建新实例
        restored_crypto = CryptoManager.from_saved_key(
            key_info["password"], key_info["salt"], key_info["iterations"]
        )

        # 验证新实例可以正确解密原实例加密的数据
        encrypted = original_crypto.encrypt(self.test_data)
        decrypted = restored_crypto.decrypt(encrypted)
        self.assertEqual(decrypted, self.test_data)

    def test_from_saved_key_invalid_parameters(self):
        """测试使用无效参数从保存的密钥创建实例"""
        with self.assertRaises(ValueError):
            CryptoManager.from_saved_key("", "valid_salt", 100000)

        with self.assertRaises(ValueError):
            CryptoManager.from_saved_key("valid_password", "", 100000)

        with self.assertRaises(CryptoError):
            CryptoManager.from_saved_key("valid_password", "invalid_base64", 100000)

    def test_clear_sensitive_data(self):
        """测试清理敏感数据"""
        crypto = CryptoManager()

        # 验证实例已初始化
        self.assertTrue(crypto.is_initialized())

        # 清理敏感数据
        crypto._clear_sensitive_data()

        # 验证实例不再可用
        self.assertFalse(crypto.is_initialized())

        # 验证无法执行加密操作
        with self.assertRaises(CryptoError):
            crypto.encrypt(self.test_data)

    def test_password_strength_validation(self):
        """测试密码强度验证"""
        # 测试弱密码
        self.assertFalse(CryptoManager.validate_password_strength("weak"))
        self.assertFalse(CryptoManager.validate_password_strength("weakpassword"))
        self.assertFalse(CryptoManager.validate_password_strength("WeakPassword"))
        self.assertFalse(CryptoManager.validate_password_strength("WeakPassword123"))

        # 测试强密码
        self.assertTrue(
            CryptoManager.validate_password_strength("My$trongP@ssw0rd123!")
        )
        self.assertTrue(
            CryptoManager.validate_password_strength(
                "VeryL0ngP@ssw0rdWithSpecialChars!"
            )
        )

    def test_get_password_strength(self):
        """测试获取密码强度等级"""
        # 测试不同强度等级的密码
        self.assertEqual(CryptoManager.get_password_strength("weak"), "weak")
        self.assertEqual(CryptoManager.get_password_strength("WeakPassword"), "medium")
        self.assertEqual(
            CryptoManager.get_password_strength("WeakPassword123"), "strong"
        )
        self.assertEqual(
            CryptoManager.get_password_strength("My$trongP@ssw0rd123!"), "very_strong"
        )

    def test_create_secure_instance(self):
        """测试创建安全实例的便捷方法"""
        # 使用强密码创建实例
        crypto = CryptoManager.create_secure_instance(self.test_password)
        self.assertIsNotNone(crypto)
        self.assertEqual(crypto.password, self.test_password)

        # 使用自动生成密码创建实例
        crypto_auto = CryptoManager.create_secure_instance()
        self.assertIsNotNone(crypto_auto)
        self.assertTrue(CryptoManager.validate_password_strength(crypto_auto.password))

    def test_create_secure_instance_with_weak_password(self):
        """测试使用弱密码创建安全实例"""
        with self.assertRaises(ValueError):
            CryptoManager.create_secure_instance("weakpassword")

    def test_get_security_info(self):
        """测试获取安全信息"""
        crypto = CryptoManager()
        security_info = crypto.get_security_info()

        self.assertIn("salt_length", security_info)
        self.assertIn("password_length", security_info)
        self.assertIn("iterations", security_info)
        self.assertIn("is_initialized", security_info)
        self.assertIn("algorithm", security_info)
        self.assertIn("key_derivation", security_info)

        self.assertEqual(security_info["salt_length"], len(crypto.salt))
        self.assertEqual(security_info["password_length"], len(crypto.password))
        self.assertEqual(security_info["iterations"], crypto.iterations)
        self.assertTrue(security_info["is_initialized"])

    def test_verify_encryption(self):
        """测试验证加密解密功能"""
        crypto = CryptoManager()

        # 验证功能正常
        self.assertTrue(crypto.verify_encryption())

        # 使用自定义测试数据
        self.assertTrue(crypto.verify_encryption("custom_test_data"))

    def test_change_password(self):
        """测试更改密码功能"""
        crypto = CryptoManager(password=self.test_password)

        new_password = "New$trongP@ssw0rd456!"

        # 更改密码
        crypto.change_password(new_password)

        # 验证新密码生效
        self.assertEqual(crypto.password, new_password)

        # 验证加密解密功能仍然正常
        encrypted = crypto.encrypt(self.test_data)
        decrypted = crypto.decrypt(encrypted)
        self.assertEqual(decrypted, self.test_data)

    def test_change_password_with_weak_password(self):
        """测试使用弱密码更改密码"""
        crypto = CryptoManager(password=self.test_password)

        with self.assertRaises(ValueError):
            crypto.change_password("weakpassword")

    def test_change_password_skip_validation(self):
        """测试跳过密码强度验证更改密码"""
        crypto = CryptoManager(password=self.test_password)

        weak_password = "weakpassword"

        # 跳过验证更改密码
        crypto.change_password(weak_password, validate_strength=False)

        # 验证密码已更改
        self.assertEqual(crypto.password, weak_password)

    def test_string_representation(self):
        """测试字符串表示方法"""
        crypto = CryptoManager()

        # 测试 __str__
        str_repr = str(crypto)
        self.assertIn("CryptoManager", str_repr)
        self.assertIn("盐值长度", str_repr)
        self.assertIn("迭代次数", str_repr)

        # 测试 __repr__
        repr_repr = repr(crypto)
        self.assertIn("CryptoManager object", repr_repr)
        self.assertIn("salt_length", repr_repr)
        self.assertIn("password_length", repr_repr)
        self.assertIn("iterations", repr_repr)

    def test_encryption_with_special_characters(self):
        """测试包含特殊字符的加密解密"""
        crypto = CryptoManager()

        test_cases = [
            "普通文本",
            "Text with spaces",
            "Text with\nnewline",
            "Text with tabs\tand more",
            "Text with 中文",
            "Text with emoji 😀",
            "Text with special chars !@#$%^&*()",
            "Very long text " * 100,  # 长文本
        ]

        for test_case in test_cases:
            with self.subTest(test_case=test_case[:20]):  # 限制子测试名称长度
                encrypted = crypto.encrypt(test_case)
                decrypted = crypto.decrypt(encrypted)
                self.assertEqual(decrypted, test_case)

    def test_concurrent_encryption_decryption(self):
        """测试并发加密解密操作"""
        import threading

        crypto = CryptoManager()
        results = []
        errors = []

        def encrypt_decrypt_worker(data, index):
            try:
                encrypted = crypto.encrypt(data)
                decrypted = crypto.decrypt(encrypted)
                results.append((index, decrypted == data))
            except Exception as e:
                errors.append((index, str(e)))

        threads = []
        test_data = "测试数据"

        # 创建多个线程同时进行加密解密
        for i in range(10):
            thread = threading.Thread(
                target=encrypt_decrypt_worker, args=(f"{test_data}_{i}", i)
            )
            threads.append(thread)
            thread.start()

        # 等待所有线程完成
        for thread in threads:
            thread.join()

        # 验证所有操作都成功
        self.assertEqual(len(results), 10)
        self.assertEqual(len(errors), 0)

        for index, success in results:
            self.assertTrue(success, f"线程 {index} 操作失败")

    @patch("src.db_connector_tool.core.crypto.PBKDF2HMAC")
    def test_fernet_creation_failure(self, mock_pbkdf2):
        """测试 Fernet 实例创建失败的情况"""
        mock_pbkdf2.side_effect = Exception("Key derivation failed")

        with self.assertRaises(CryptoError):
            CryptoManager()

    def test_auto_adjust_iterations(self):
        """测试自动调整迭代次数功能"""
        crypto = CryptoManager()

        # 验证迭代次数在合理范围内
        self.assertGreaterEqual(crypto.iterations, 100000)
        self.assertLessEqual(crypto.iterations, 1000000)

        # 验证迭代次数是10000的倍数（根据代码逻辑）
        self.assertEqual(crypto.iterations % 10000, 0)

    def test_encrypt_after_destruction(self):
        """测试对象销毁后的加密操作"""
        crypto = CryptoManager()

        # 模拟对象销毁
        crypto._clear_sensitive_data()

        # 验证无法执行加密操作
        with self.assertRaises(CryptoError):
            crypto.encrypt(self.test_data)

    def test_invalid_token_handling(self):
        """测试无效令牌的处理"""
        crypto = CryptoManager()

        # 创建无效的加密数据
        invalid_data = base64.urlsafe_b64encode(b"invalid_encrypted_data").decode(
            "utf-8"
        )

        with self.assertRaises(CryptoError):
            crypto.decrypt(invalid_data)


if __name__ == "__main__":
    # 运行测试
    unittest.main(verbosity=2)
