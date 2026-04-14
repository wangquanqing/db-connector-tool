import unittest

from src.db_connector_tool.core.exceptions import ConfigError
from src.db_connector_tool.core.validators import (
    ConfigValidator,
    GenericValidator,
    PasswordValidator,
)


class TestConfigValidator(unittest.TestCase):
    """测试配置验证器"""

    def test_validate_config_valid(self) -> None:
        """测试验证有效的配置"""
        valid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
            },
        }

        try:
            ConfigValidator.validate_config(valid_config)
        except Exception as e:
            self.fail(f"验证有效配置时抛出了异常: {e}")

    def test_validate_config_missing_fields(self) -> None:
        """测试验证缺少字段的配置"""
        invalid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            # 缺少 connections 和 metadata
        }

        with self.assertRaises(ConfigError):
            ConfigValidator.validate_config(invalid_config)

    def test_is_valid_version_format(self) -> None:
        """测试版本号格式验证"""
        # 有效的版本号
        self.assertTrue(ConfigValidator.is_valid_version_format("1.0.0"))
        self.assertTrue(ConfigValidator.is_valid_version_format("1.10.20"))
        self.assertTrue(ConfigValidator.is_valid_version_format("0.1.0"))

        # 无效的版本号
        self.assertFalse(ConfigValidator.is_valid_version_format("1.0"))
        self.assertFalse(ConfigValidator.is_valid_version_format("1.0.0.0"))
        self.assertFalse(ConfigValidator.is_valid_version_format("1.0.a"))
        self.assertFalse(ConfigValidator.is_valid_version_format(None))  # type: ignore
        self.assertFalse(ConfigValidator.is_valid_version_format(123))  # type: ignore
        self.assertFalse(ConfigValidator.is_valid_version_format(""))

    def test_validate_connection_name(self) -> None:
        """测试连接名称验证"""
        # 有效的连接名称
        ConfigValidator.validate_connection_name("valid_name")
        ConfigValidator.validate_connection_name("name123")
        ConfigValidator.validate_connection_name("name_123")

        # 无效的连接名称
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_name("")
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_name("name with spaces")
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_name("name@symbol")
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_name("a" * 51)  # 过长的名称


class TestConfigValidatorConnectionMethods(unittest.TestCase):
    """测试配置验证器的连接相关方法"""

    def test_validate_connection_config_valid(self) -> None:
        """测试验证有效的连接配置"""
        valid_connection = {
            "host": "localhost",
            "port": 5432,
            "username": "test",
            "password": "test123",
            "database": "test_db",
        }

        try:
            ConfigValidator.validate_connection_config(valid_connection)
        except Exception as e:
            self.fail(f"验证有效连接配置时抛出了异常: {e}")

    def test_validate_connection_config_invalid(self) -> None:
        """测试验证无效的连接配置"""
        # 空配置
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config({})

        # 非字典配置
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config("invalid")  # type: ignore

        # 非字符串键
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config({123: "value"})  # type: ignore


class TestPasswordValidator(unittest.TestCase):
    """测试密码验证器"""

    def test_get_strength(self) -> None:
        """测试密码强度评估"""
        # 弱密码
        self.assertEqual(PasswordValidator.get_strength("weak"), "weak")

        # 中等强度密码
        self.assertEqual(PasswordValidator.get_strength("Short1!"), "medium")
        self.assertEqual(PasswordValidator.get_strength("Weak123"), "medium")
        self.assertEqual(PasswordValidator.get_strength("NoSpecialChar123"), "medium")
        self.assertEqual(PasswordValidator.get_strength("nouppercase123!"), "medium")
        self.assertEqual(PasswordValidator.get_strength("NOLOWERCASE123!"), "medium")
        self.assertEqual(PasswordValidator.get_strength("NoDigitPassword!"), "medium")

        # 强密码
        self.assertEqual(PasswordValidator.get_strength("Medium1!"), "strong")  # 8字符+4种类型
        self.assertEqual(PasswordValidator.get_strength("StrongP@ssw0rd123!"), "strong")

        # 非常强的密码
        self.assertEqual(PasswordValidator.get_strength("VeryStrongPassword123!@#"), "very_strong")

    def test_validate_strength(self) -> None:
        """测试密码强度验证"""
        # 无效密码
        self.assertFalse(PasswordValidator.validate_strength("Weak123"))
        self.assertFalse(PasswordValidator.validate_strength("Short1!"))

        # 有效密码
        self.assertTrue(PasswordValidator.validate_strength("StrongP@ssw0rd123!"))
        self.assertTrue(PasswordValidator.validate_strength("VeryStrongPassword123!@#"))


class TestGenericValidator(unittest.TestCase):
    """测试通用验证器"""

    def test_validate_required_fields(self) -> None:
        """测试必需字段验证"""
        # 包含所有必需字段
        test_data = {"name": "test", "value": 123}
        try:
            GenericValidator.validate_required_fields(test_data, ["name", "value"])
        except Exception as e:
            self.fail(f"验证必需字段时抛出了异常: {e}")

        # 缺少必需字段
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields(test_data, ["name", "value", "missing"])
        self.assertIn("缺少必需字段", str(context.exception))

    def test_validate_field_type(self) -> None:
        """测试字段类型验证"""
        # 类型正确
        try:
            GenericValidator.validate_field_type(123, int, "测试字段")
            GenericValidator.validate_field_type("string", str, "测试字段")
            GenericValidator.validate_field_type(True, bool, "测试字段")
        except Exception as e:
            self.fail(f"验证字段类型时抛出了异常: {e}")

        # 类型错误
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_field_type("123", int, "测试字段")
        self.assertIn("测试字段必须是int类型", str(context.exception))


if __name__ == "__main__":
    unittest.main()
