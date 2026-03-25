"""
CryptoManager 单元测试模块

为 db_connector_tool.core.crypto 模块提供完整的单元测试覆盖。
测试包括：
- 类初始化和参数验证
- 密码强度验证和评估
- 加密解密功能测试
- 密钥管理和持久化测试
- 错误处理和异常测试
- 安全特性测试

测试设计原则：
- 每个测试方法只测试一个功能点
- 使用适当的测试数据和断言
- 包含边界条件和异常情况测试
- 确保测试的独立性和可重复性
"""

import base64
import unittest
from unittest.mock import patch, MagicMock

from db_connector_tool.core.crypto import CryptoManager
from db_connector_tool.core.exceptions import CryptoError


class TestCryptoManager(unittest.TestCase):
    """CryptoManager 单元测试类"""

    def setUp(self):
        """测试前置设置"""
        # 动态生成测试用的强密码（避免硬编码）
        self.strong_password = self._generate_test_password()
        # 测试用的盐值（16字节）
        self.test_salt = b"test_salt_16bytes"
        # 测试迭代次数
        self.test_iterations = 100000
        # 测试数据
        self.test_data = "这是测试数据，包含中文和特殊字符 🌟"
        self.test_bytes = b"binary_test_data_123"

    def _generate_test_password(self):
        """生成符合强度要求的测试密码"""
        import secrets
        import string

        # 生成基础随机字符串
        length = 20
        characters = string.ascii_letters + string.digits + "!@#$%^&*"

        # 确保包含所有必需字符类型
        while True:
            password = "".join(secrets.choice(characters) for _ in range(length))

            # 验证密码强度
            has_upper = any(c.isupper() for c in password)
            has_lower = any(c.islower() for c in password)
            has_digit = any(c.isdigit() for c in password)
            has_special = any(c in "!@#$%^&*" for c in password)

            if has_upper and has_lower and has_digit and has_special:
                return password

    def tearDown(self):
        """测试后置清理"""
        pass

    # 测试类初始化和参数验证
    def test_default_initialization(self):
        """测试默认初始化"""
        crypto = CryptoManager()

        # 验证默认属性
        self.assertIsNotNone(crypto.password)
        self.assertIsNotNone(crypto.salt)
        self.assertGreaterEqual(crypto.iterations, 100000)
        self.assertTrue(crypto.is_initialized())

        # 验证密码强度
        self.assertTrue(CryptoManager.validate_password_strength(crypto.password))

        # 验证盐值长度
        self.assertGreaterEqual(len(crypto.salt), CryptoManager.MIN_SALT_LENGTH)

    def test_custom_initialization(self):
        """测试自定义参数初始化"""
        crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        self.assertEqual(crypto.password, self.strong_password)
        self.assertEqual(crypto.salt, self.test_salt)
        self.assertEqual(crypto.iterations, self.test_iterations)
        self.assertTrue(crypto.is_initialized())

    def test_initialization_with_weak_password(self):
        """测试弱密码初始化（应该抛出异常）"""
        weak_password = "weak123"

        with self.assertRaises(ValueError) as context:
            CryptoManager(password=weak_password)

        self.assertIn("密码强度不足", str(context.exception))

    def test_initialization_with_short_salt(self):
        """测试短盐值初始化（应该抛出异常）"""
        short_salt = b"short_salt"

        with self.assertRaises(ValueError) as context:
            CryptoManager(salt=short_salt, password=self.strong_password)

        self.assertIn("盐值长度必须至少为", str(context.exception))

    def test_initialization_with_low_iterations(self):
        """测试低迭代次数初始化（应该发出警告但成功）"""
        low_iterations = 50000

        with self.assertLogs(level="WARNING") as log:
            crypto = CryptoManager(
                password=self.strong_password,
                salt=self.test_salt,
                iterations=low_iterations,
            )

        self.assertEqual(crypto.iterations, low_iterations)
        self.assertIn("迭代次数 50000 过低", log.output[0])

    def test_skip_password_validation(self):
        """测试跳过密码验证"""
        weak_password = "weak123"

        # 应该成功，因为跳过了密码验证
        crypto = CryptoManager(
            password=weak_password, salt=self.test_salt, skip_password_validation=True
        )

        self.assertEqual(crypto.password, weak_password)
        self.assertTrue(crypto.is_initialized())

    # 测试密码强度验证
    def test_validate_password_strength(self):
        """测试密码强度验证"""
        # 强密码
        self.assertTrue(CryptoManager.validate_password_strength("StrongP@ssw0rd123!"))
        self.assertTrue(
            CryptoManager.validate_password_strength(
                "VeryL0ngP@ssw0rdWithSpecialChars!"
            )
        )

        # 弱密码
        self.assertFalse(CryptoManager.validate_password_strength("short"))
        self.assertFalse(CryptoManager.validate_password_strength("nouppercase123!"))
        self.assertFalse(CryptoManager.validate_password_strength("NOLOWERCASE123!"))
        self.assertFalse(CryptoManager.validate_password_strength("NoNumbers!@#"))
        self.assertFalse(CryptoManager.validate_password_strength("NoSpecialChars123"))

    def test_get_password_strength(self):
        """测试密码强度评估"""
        # 非常强的密码
        self.assertEqual(
            CryptoManager.get_password_strength("VeryL0ngP@ssw0rdWithSpecialChars!"),
            "very_strong",
        )

        # 强密码
        self.assertEqual(
            CryptoManager.get_password_strength("StrongP@ssw0rd123!"), "strong"
        )

        # 中等强度密码
        self.assertEqual(CryptoManager.get_password_strength("MediumPass123"), "medium")

        # 弱密码
        self.assertEqual(CryptoManager.get_password_strength("weak"), "weak")
        self.assertEqual(CryptoManager.get_password_strength("password"), "weak")

    # 测试加密解密功能
    def test_encrypt_decrypt_string(self):
        """测试字符串加密解密"""
        crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        # 加密
        encrypted = crypto.encrypt(self.test_data)

        # 验证加密结果
        self.assertIsInstance(encrypted, str)
        self.assertNotEqual(encrypted, self.test_data)

        # 解密
        decrypted = crypto.decrypt(encrypted)

        # 验证解密结果
        self.assertEqual(decrypted, self.test_data)

    def test_encrypt_decrypt_bytes(self):
        """测试字节数据加密解密"""
        crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        # 加密字节数据
        encrypted_bytes = crypto.encrypt_bytes(self.test_bytes)

        # 验证加密结果
        self.assertIsInstance(encrypted_bytes, bytes)
        self.assertNotEqual(encrypted_bytes, self.test_bytes)

        # 解密字节数据
        decrypted_bytes = crypto.decrypt_bytes(encrypted_bytes)

        # 验证解密结果
        self.assertEqual(decrypted_bytes, self.test_bytes)

    def test_encrypt_empty_string(self):
        """测试空字符串加密（应该抛出异常）"""
        crypto = CryptoManager()

        with self.assertRaises(ValueError) as context:
            crypto.encrypt("")

        self.assertIn("加密数据不能为空", str(context.exception))

    def test_decrypt_invalid_data(self):
        """测试解密无效数据（应该抛出异常）"""
        crypto = CryptoManager()

        # 测试无效的base64数据
        with self.assertRaises(ValueError):
            crypto.decrypt("invalid_base64_data")

        # 测试使用不同密钥解密
        crypto1 = CryptoManager()
        crypto2 = CryptoManager()

        encrypted = crypto1.encrypt(self.test_data)

        with self.assertRaises(CryptoError):
            crypto2.decrypt(encrypted)

    def test_encrypt_decrypt_special_characters(self):
        """测试特殊字符加密解密"""
        crypto = CryptoManager()

        test_cases = [
            "普通文本",
            "Text with spaces",
            "文本 with 混合 characters 🌟",
            "特殊字符!@#$%^&*()",
            "新行\n和制表符\t",
            "Unicode字符: 中文，日本語，한국어",
            "Very long text " * 100,  # 长文本测试
        ]

        for test_case in test_cases:
            with self.subTest(test_case=test_case):
                encrypted = crypto.encrypt(test_case)
                decrypted = crypto.decrypt(encrypted)
                self.assertEqual(decrypted, test_case)

    # 测试密钥管理功能
    def test_get_key_info(self):
        """测试获取密钥信息"""
        crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        key_info = crypto.get_key_info()

        # 验证密钥信息结构
        self.assertIn("password", key_info)
        self.assertIn("salt", key_info)
        self.assertIn("iterations", key_info)

        # 验证值
        self.assertEqual(key_info["password"], self.strong_password)
        self.assertEqual(key_info["iterations"], self.test_iterations)

        # 验证盐值编码
        salt_bytes = base64.urlsafe_b64decode(key_info["salt"].encode("utf-8"))
        self.assertEqual(salt_bytes, self.test_salt)

    def test_from_saved_key(self):
        """测试从保存的密钥恢复实例"""
        # 创建原始实例
        original_crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        # 获取密钥信息
        key_info = original_crypto.get_key_info()

        # 从保存的密钥恢复实例
        restored_crypto = CryptoManager.from_saved_key(
            key_info["password"], key_info["salt"], key_info["iterations"]
        )

        # 验证恢复的实例
        self.assertEqual(restored_crypto.password, original_crypto.password)
        self.assertEqual(restored_crypto.salt, original_crypto.salt)
        self.assertEqual(restored_crypto.iterations, original_crypto.iterations)

        # 验证加密解密功能
        encrypted = original_crypto.encrypt(self.test_data)
        decrypted = restored_crypto.decrypt(encrypted)
        self.assertEqual(decrypted, self.test_data)

    def test_from_saved_key_invalid_parameters(self):
        """测试从无效的保存密钥恢复实例"""
        # 测试空参数
        with self.assertRaises(ValueError):
            CryptoManager.from_saved_key("", "", None)

        # 测试无效的base64盐值
        with self.assertRaises(CryptoError):
            CryptoManager.from_saved_key("password", "invalid_base64", 100000)

    def test_create_secure_instance(self):
        """测试创建安全实例便捷方法"""
        # 使用自定义密码
        crypto1 = CryptoManager.create_secure_instance(self.strong_password)
        self.assertEqual(crypto1.password, self.strong_password)
        self.assertEqual(crypto1.iterations, CryptoManager.DEFAULT_ITERATIONS)

        # 使用自动生成密码
        crypto2 = CryptoManager.create_secure_instance()
        self.assertIsNotNone(crypto2.password)
        self.assertTrue(CryptoManager.validate_password_strength(crypto2.password))

    def test_create_secure_instance_weak_password(self):
        """测试使用弱密码创建安全实例（应该抛出异常）"""
        with self.assertRaises(ValueError):
            CryptoManager.create_secure_instance("weak")

    # 测试安全特性
    def test_verify_encryption(self):
        """测试加密验证功能"""
        crypto = CryptoManager()

        # 正常情况应该返回True
        self.assertTrue(crypto.verify_encryption())

        # 使用自定义测试数据
        self.assertTrue(crypto.verify_encryption("custom_test_data"))

    def test_change_password(self):
        """测试更改密码功能"""
        crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        # 保存原始密钥信息
        original_key_info = crypto.get_key_info()

        # 更改密码
        new_password = "NewStrongP@ssw0rd456!"
        crypto.change_password(new_password)

        # 验证密码已更改
        self.assertEqual(crypto.password, new_password)

        # 验证盐值和迭代次数保持不变
        self.assertEqual(crypto.salt, self.test_salt)
        self.assertEqual(crypto.iterations, self.test_iterations)

        # 验证加密解密功能正常
        self.assertTrue(crypto.verify_encryption())

    def test_change_password_weak_password(self):
        """测试更改密码为弱密码（应该抛出异常）"""
        crypto = CryptoManager()

        with self.assertRaises(ValueError):
            crypto.change_password("weak", validate_strength=True)

    def test_change_password_skip_validation(self):
        """测试更改密码跳过验证"""
        crypto = CryptoManager()

        # 应该成功，因为跳过了验证
        crypto.change_password("weak", validate_strength=False)
        self.assertEqual(crypto.password, "weak")

    def test_sensitive_data_clearing(self):
        """测试敏感数据清理"""
        crypto = CryptoManager(
            password=self.strong_password,
            salt=self.test_salt,
            iterations=self.test_iterations,
        )

        # 手动调用敏感数据清理
        crypto._clear_sensitive_data()

        # 验证敏感数据已被清理
        self.assertEqual(crypto.password, "")
        self.assertEqual(crypto.salt, b"")
        self.assertIsNone(crypto.fernet)

    # 测试工具方法
    def test_generate_secure_password(self):
        """测试安全密码生成"""
        crypto = CryptoManager()
        password = crypto._generate_secure_password()

        self.assertIsInstance(password, str)
        self.assertGreaterEqual(len(password), 16)
        self.assertTrue(CryptoManager.validate_password_strength(password))

    def test_generate_secure_salt(self):
        """测试安全盐值生成"""
        crypto = CryptoManager()
        salt = crypto._generate_secure_salt()

        self.assertIsInstance(salt, bytes)
        self.assertEqual(len(salt), CryptoManager.DEFAULT_SALT_LENGTH)

    def test_auto_adjust_iterations(self):
        """测试自动调整迭代次数"""
        crypto = CryptoManager()
        iterations = crypto._auto_adjust_iterations()

        # 验证迭代次数在合理范围内
        self.assertGreaterEqual(iterations, 100000)
        self.assertLessEqual(iterations, 1000000)
        # 应该是10000的倍数
        self.assertEqual(iterations % 10000, 0)

    # 测试异常处理
    def test_encrypt_after_destruction(self):
        """测试销毁后加密（应该抛出异常）"""
        crypto = CryptoManager()

        # 销毁实例
        crypto._clear_sensitive_data()

        # 尝试加密应该抛出异常
        with self.assertRaises(CryptoError):
            crypto.encrypt(self.test_data)

    def test_decrypt_after_destruction(self):
        """测试销毁后解密（应该抛出异常）"""
        crypto = CryptoManager()
        encrypted = crypto.encrypt(self.test_data)

        # 销毁实例
        crypto._clear_sensitive_data()

        # 尝试解密应该抛出异常
        with self.assertRaises(CryptoError):
            crypto.decrypt(encrypted)

    def test_fernet_creation_failure(self):
        """测试Fernet实例创建失败"""
        with patch("cryptography.fernet.Fernet") as mock_fernet:
            mock_fernet.side_effect = Exception("Fernet creation failed")

            with self.assertRaises(CryptoError) as context:
                CryptoManager()

            self.assertIn("加密系统初始化失败", str(context.exception))

    # 测试字符串表示
    def test_str_representation(self):
        """测试字符串表示"""
        crypto = CryptoManager()

        str_repr = str(crypto)
        self.assertIn("CryptoManager", str_repr)
        self.assertIn("盐值长度", str_repr)
        self.assertIn("迭代次数", str_repr)

    def test_repr_representation(self):
        """测试详细表示"""
        crypto = CryptoManager()

        repr_str = repr(crypto)
        self.assertIn("CryptoManager object", repr_str)
        self.assertIn("salt_length", repr_str)
        self.assertIn("password_length", repr_str)
        self.assertIn("iterations", repr_str)

    def test_get_security_info(self):
        """测试获取安全信息"""
        crypto = CryptoManager()

        security_info = crypto.get_security_info()

        # 验证安全信息结构
        expected_keys = [
            "salt_length",
            "password_length",
            "iterations",
            "is_initialized",
            "algorithm",
            "key_derivation",
        ]

        for key in expected_keys:
            self.assertIn(key, security_info)

        # 验证值
        self.assertEqual(security_info["salt_length"], len(crypto.salt))
        self.assertEqual(security_info["password_length"], len(crypto.password))
        self.assertEqual(security_info["iterations"], crypto.iterations)
        self.assertTrue(security_info["is_initialized"])


if __name__ == "__main__":
    # 运行测试
    unittest.main(verbosity=2)
