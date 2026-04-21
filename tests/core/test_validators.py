"""
验证器模块测试文件

测试数据库连接器工具中的各种验证器功能，包括配置验证、密码验证和通用验证。
"""

import unittest
from typing import Any, Dict, List, Tuple

from src.db_connector_tool.core.exceptions import ConfigError
from src.db_connector_tool.core.validators import (
    ConfigValidator,
    GenericValidator,
    PasswordValidator,
)

# 类型别名，提高代码可读性
VersionTestCase = Tuple[str, bool, str]
PasswordTestCase = Tuple[str, str, str]
FieldTypeTestCase = Tuple[Any, type, str, bool]


class TestConfigValidator(unittest.TestCase):
    """测试配置验证器"""

    def setUp(self) -> None:
        """设置测试数据"""
        self.valid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
            },
        }

        # 边界测试数据
        self.boundary_test_cases = [
            # (version_string, expected_result, description)
            ("0.0.0", True, "最小版本号"),
            ("999.999.999", True, "最大版本号"),
            ("01.0.0", False, "前导零版本号"),
            ("1.00.0", False, "前导零版本号"),
            ("1.0.00", False, "前导零版本号"),
            ("-1.0.0", False, "负版本号"),
            ("1.0.-1", False, "负版本号"),
            ("1.0.0.1", False, "超出版本号格式"),
            ("v1.0.0", False, "带前缀版本号"),
            ("1.0.0-beta", False, "带后缀版本号"),
        ]

    def test_validate_config_valid(self) -> None:
        """测试验证有效的配置"""
        try:
            ConfigValidator.validate_config(self.valid_config)
        except Exception as e:
            self.fail(f"验证有效配置时抛出了异常: {e}")

    def test_is_valid_version_format(self) -> None:
        """测试版本号格式验证"""
        # 有效的版本号格式
        valid_versions = ["1.0.0", "2.1.5", "0.1.0", "10.20.30", "255.255.255"]
        for version in valid_versions:
            with self.subTest(version=version):
                self.assertTrue(
                    ConfigValidator.is_valid_version_format(version),
                    f"版本号 {version} 应该被识别为有效",
                )

        # 边界测试用例
        for version, expected, description in self.boundary_test_cases:
            with self.subTest(version=version, description=description):
                result = ConfigValidator.is_valid_version_format(version)
                self.assertEqual(
                    result,
                    expected,
                    f"{description}: 版本号 {version} 预期 {expected}, 实际 {result}",
                )

        # 无效的版本号格式
        invalid_versions = [
            ("1.0", "缺少第三部分"),
            ("1.0.0.0", "超出三部分"),
            ("version", "非数字字符"),
            ("", "空字符串"),
            ("1.a.0", "字母字符"),
            ("1.0.0a", "后缀字符"),
            (None, "None值"),
            (123, "数字类型"),
        ]

        # 专门测试会触发异常处理分支的情况
        exception_cases = [
            (object(), "对象类型"),  # 会触发 AttributeError (没有 split 方法)
            (
                ["1", "0", "0"],
                "列表类型",
            ),  # 会触发 AttributeError (列表没有 split 方法)
            (
                {"version": "1.0.0"},
                "字典类型",
            ),  # 会触发 AttributeError (字典没有 split 方法)
            ("1.0.0", "正常字符串"),  # 这个应该通过，用于对比
        ]

        for version, description in exception_cases:
            with self.subTest(version=version, description=description):
                if description == "正常字符串":
                    # 这个应该返回 True
                    self.assertTrue(
                        ConfigValidator.is_valid_version_format(version),
                        f"正常版本号应该通过验证",
                    )
                else:
                    # 其他情况应该返回 False（触发异常处理）
                    self.assertFalse(
                        ConfigValidator.is_valid_version_format(version),
                        f"{description}: 版本号 {version} 应该触发异常处理并返回 False",
                    )

        for version, description in invalid_versions:
            with self.subTest(version=version, description=description):
                if version is None or isinstance(version, int):
                    # 测试异常处理
                    self.assertFalse(ConfigValidator.is_valid_version_format(version))  # type: ignore
                else:
                    self.assertFalse(
                        ConfigValidator.is_valid_version_format(version),
                        f"{description}: 版本号 {version} 应该被识别为无效",
                    )

    def test_validate_config_missing_fields(self) -> None:
        """测试验证缺少字段的配置"""
        invalid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            # 缺少 connections 和 metadata
        }

        with self.assertRaises(ConfigError):
            ConfigValidator.validate_config(invalid_config)

    def test_validate_config_invalid_version(self) -> None:
        """测试验证无效版本号的配置"""
        invalid_config = {
            "version": "1.0",
            "app_name": "test_app",
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
            },
        }

        with self.assertRaises(ConfigError) as context:
            ConfigValidator.validate_config(invalid_config)
        self.assertIn("无效的版本号格式", str(context.exception))

    def test_validate_config_invalid_key_version(self) -> None:
        """测试验证无效 key_version 的配置"""
        invalid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            "connections": {},
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "not_a_number",
            },
        }

        with self.assertRaises(ConfigError) as context:
            ConfigValidator.validate_config(invalid_config)
        self.assertIn("key_version必须是有效的数字字符串", str(context.exception))

    def test_validate_config_connections_not_dict(self) -> None:
        """测试验证 connections 字段不是字典的情况"""
        invalid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            "connections": "not_a_dict",  # 不是字典
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
            },
        }

        with self.assertRaises(ConfigError) as context:
            ConfigValidator.validate_config(invalid_config)
        self.assertIn("connections字段必须是dict类型", str(context.exception))

    def test_validate_config_metadata_not_dict(self) -> None:
        """测试验证 metadata 字段不是字典的情况"""
        invalid_config = {
            "version": "1.0.0",
            "app_name": "test_app",
            "connections": {},
            "metadata": "not_a_dict",  # 不是字典
        }

        with self.assertRaises(ConfigError) as context:
            ConfigValidator.validate_config(invalid_config)
        self.assertIn("metadata字段必须是dict类型", str(context.exception))

    def test_validate_connection_config(self) -> None:
        """测试验证连接配置"""
        # 有效的连接配置
        valid_config = {
            "host": "localhost",
            "port": 3306,
            "username": "user",
            "password": "pass",
            "database": "test_db",
        }

        try:
            ConfigValidator.validate_connection_config(valid_config)
        except Exception as e:
            self.fail(f"验证有效连接配置时抛出异常: {e}")

    def test_validate_connection_config_invalid(self) -> None:
        """测试验证无效连接配置"""
        # 空配置
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config(None)  # type: ignore

        # 非字典配置
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config("invalid")  # type: ignore

        # 空字典配置
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config({})

        # 包含非字符串键的配置
        invalid_config = {123: "value"}  # 数字键
        with self.assertRaises(ValueError):
            ConfigValidator.validate_connection_config(invalid_config)  # type: ignore

    def test_validate_connection_name_valid(self) -> None:
        """测试有效的连接名称验证"""
        valid_names = [
            "valid_name",
            "name123",
            "name_123",
            "a",
            "A",
            "name_with_underscore",
            "NameWithCaps",
            "name123with456numbers",
            "_name",
            "name_",
            "a" * 50,  # 最大长度
        ]

        for name in valid_names:
            with self.subTest(name=name):
                try:
                    ConfigValidator.validate_connection_name(name)
                except ValueError as e:
                    self.fail(f"有效的连接名称 '{name}' 验证失败: {e}")

    def test_validate_connection_name_invalid(self) -> None:
        """测试无效的连接名称验证"""
        # 格式错误测试用例
        invalid_cases = [
            ("", "空字符串"),
            ("name with spaces", "包含空格"),
            ("name@symbol", "特殊字符"),
            ("name-symbol", "连字符"),
            ("name.symbol", "点号"),
            ("name/symbol", "斜杠"),
            ("a" * 51, "超过最大长度"),
            (None, "None值"),
            (123, "数字类型"),
            ("", "空字符串"),
            (" ", "空格字符串"),
            ("\t", "制表符"),
            ("\n", "换行符"),
        ]

        for name, description in invalid_cases:
            with self.subTest(name=name, description=description):
                with self.assertRaises(ValueError):
                    ConfigValidator.validate_connection_name(name)

    def test_validate_connection_name_reserved_words(self) -> None:
        """测试保留字连接名称验证"""
        reserved_words = ["default", "test", "backup"]

        for word in reserved_words:
            with self.subTest(word=word):
                with self.assertRaises(ValueError) as context:
                    ConfigValidator.validate_connection_name(word)
                self.assertIn("连接名称不能使用保留字", str(context.exception))


class TestConfigValidatorConnections(unittest.TestCase):
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
        with self.assertRaises(ValueError) as context:
            ConfigValidator.validate_connection_config({})
        self.assertIn("连接配置不能为空且必须是字典", str(context.exception))

        # 非字典配置
        with self.assertRaises(ValueError) as context:
            ConfigValidator.validate_connection_config("invalid")  # type: ignore
        self.assertIn("连接配置不能为空且必须是字典", str(context.exception))

        # 非字符串键
        with self.assertRaises(ValueError) as context:
            ConfigValidator.validate_connection_config({123: "value"})  # type: ignore
        self.assertIn("连接配置的键必须是字符串", str(context.exception))

    def test_validate_connection_config_partial(self) -> None:
        """测试部分有效的连接配置"""
        # 部分字段缺失的配置
        partial_config = {
            "host": "localhost",
            "port": 5432,
            # 缺少 username, password, database
        }

        # 应该通过基本验证（当前实现只验证字典结构和键类型，不验证具体字段）
        try:
            ConfigValidator.validate_connection_config(partial_config)
        except Exception as e:
            self.fail(f"验证部分连接配置时抛出了异常: {e}")


class TestPasswordValidator(unittest.TestCase):
    """测试密码验证器"""

    def setUp(self) -> None:
        """设置测试数据"""
        self.password_strength_cases = [
            # (password, expected_strength, description)
            ("weak", "weak", "太短且无复杂度"),
            ("password", "weak", "长度够但无复杂度"),
            ("12345678", "weak", "只有数字"),
            ("abcdefgh", "weak", "只有小写字母"),
            ("ABCDEFGH", "weak", "只有大写字母"),
            ("!@#$%^&*", "weak", "只有特殊字符"),
            ("Short1!", "medium", "长度够但复杂度不足"),
            ("Weak123", "medium", "缺少特殊字符"),
            ("NoSpecialChar123", "medium", "长度够但缺少特殊字符"),
            ("nouppercase123!", "medium", "缺少大写字母"),
            ("NOLOWERCASE123!", "medium", "缺少小写字母"),
            ("NoDigitPassword!", "medium", "缺少数字"),
            ("Ab1!", "medium", "长度不足但复杂度够"),
            ("Medium1!", "strong", "8字符+4种类型"),
            ("StrongP@ssw0rd123!", "strong", "标准强密码"),
            ("Passw0rd!", "strong", "刚好8字符"),
            ("VeryStrongPassword123!@#", "very_strong", "长度超过24字符"),
            ("ThisIsAVeryLongPassword123!@#$", "very_strong", "超长强密码"),
            ("a" * 24 + "A1!", "very_strong", "刚好24字符"),
        ]

    def test_get_strength(self) -> None:
        """测试密码强度评估"""
        for password, expected_strength, description in self.password_strength_cases:
            with self.subTest(password=password, description=description):
                result = PasswordValidator.get_strength(password)
                self.assertEqual(
                    result,
                    expected_strength,
                    f"{description}: 密码 '{password}' 预期强度 {expected_strength}, 实际 {result}",
                )

    def test_get_strength_edge_cases(self) -> None:
        """测试密码强度评估的边界情况"""
        edge_cases = [
            ("", "weak", "空密码"),
            ("A" * 100, "weak", "超长单一字符"),
            ("!" * 100, "weak", "超长特殊字符"),
            ("1" * 100, "weak", "超长数字"),
        ]

        for password, expected_strength, description in edge_cases:
            with self.subTest(password=password, description=description):
                result = PasswordValidator.get_strength(password)
                self.assertEqual(
                    result,
                    expected_strength,
                    f"{description}: 密码 '{password}' 预期强度 {expected_strength}, 实际 {result}",
                )

    def test_get_strength_invalid_input(self) -> None:
        """测试无效输入的密码强度评估"""
        # 测试None值和其他无效输入
        invalid_cases = [
            (None, "None密码应该抛出异常"),
            (123456, "数字类型密码应该抛出异常"),
            ([], "列表类型密码应该抛出异常"),
            ({}, "字典类型密码应该抛出异常"),
        ]

        for password, description in invalid_cases:
            with self.subTest(password=password, description=description):
                with self.assertRaises((TypeError, AttributeError)):
                    PasswordValidator.get_strength(password)

    def test_validate_strength(self) -> None:
        """测试密码强度验证"""
        # 测试各种密码强度
        strength_cases = [
            ("weak", False, "弱密码应该失败"),
            ("Short1!", False, "中等密码应该失败"),
            ("MediumPassword1!", True, "强密码应该通过"),  # 修改为16字符
            ("StrongP@ssw0rd123!", True, "标准强密码应该通过"),
            ("VeryStrongPassword123!@#", True, "非常强密码应该通过"),
            ("", False, "空密码应该失败"),
        ]

        for password, expected, description in strength_cases:
            with self.subTest(password=password, description=description):
                result = PasswordValidator.validate_strength(password)
                self.assertEqual(
                    result,
                    expected,
                    f"{description}: 密码 '{password}' 预期 {expected}, 实际 {result}",
                )

    def test_validate_strength_invalid_input(self) -> None:
        """测试无效输入的密码强度验证"""
        # 测试None值和其他无效输入
        invalid_cases = [
            (None, "None密码应该抛出异常"),
            (123, "数字类型密码应该抛出异常"),
            ([], "列表类型密码应该抛出异常"),
            ({}, "字典类型密码应该抛出异常"),
        ]

        for password, description in invalid_cases:
            with self.subTest(password=password, description=description):
                with self.assertRaises((TypeError, ValueError)):
                    PasswordValidator.validate_strength(password)

    def test_validate_strength_detailed_requirements(self) -> None:
        """测试密码强度验证的详细要求检查"""
        # 测试各种不满足要求的密码
        requirement_cases = [
            ("Short1!", {"length_ok": False}, "长度不足"),
            ("nouppercase123!", {"has_uppercase": False}, "缺少大写字母"),
            ("NOLOWERCASE123!", {"has_lowercase": False}, "缺少小写字母"),
            ("NoDigitPassword!", {"has_digit": False}, "缺少数字"),
            ("NoSpecialChar123", {"has_special": False}, "缺少特殊字符"),
        ]

        for password, failed_requirement, description in requirement_cases:
            with self.subTest(password=password, description=description):
                result = PasswordValidator.validate_strength(password)
                self.assertFalse(
                    result, f"{description}: 密码 '{password}' 应该验证失败"
                )

    def test_get_strength_complex_cases(self) -> None:
        """测试密码强度评估的复杂情况"""
        complex_cases = [
            # (密码, 预期强度, 描述)
            ("Aa1!", "medium", "4字符但4种类型"),  # 长度<8，但复杂度=4 → medium
            ("AaBbCcDd", "medium", "8字符但只有字母"),  # 长度=8，复杂度=2 → medium
            (
                "12345678!",
                "medium",
                "8字符但只有数字和特殊字符",
            ),  # 长度=8，复杂度=2 → medium
            ("AaBbCcDd1", "medium", "9字符但缺少特殊字符"),  # 长度>8，复杂度=3 → medium
            ("Aa1!Bb2@", "strong", "8字符4种类型"),  # 长度=8，复杂度=4 → strong
            (
                "A" * 24 + "a1!",
                "very_strong",
                "刚好24字符但只有3种类型",
            ),  # 长度>24，复杂度=4 → very_strong
            (
                "a" * 100 + "A1!",
                "very_strong",
                "超长密码但复杂度够",
            ),  # 长度>24，复杂度=4 → very_strong
        ]

        for password, expected_strength, description in complex_cases:
            with self.subTest(password=password, description=description):
                result = PasswordValidator.get_strength(password)
                self.assertEqual(
                    result,
                    expected_strength,
                    f"{description}: 密码 '{password}' 预期强度 {expected_strength}, 实际 {result}",
                )

    def test_check_password_requirements_internal(self) -> None:
        """测试内部密码要求检查方法"""
        # 测试有效的密码
        valid_password = "StrongPassword123!"
        requirements = PasswordValidator._check_password_requirements(valid_password)

        self.assertTrue(all(requirements.values()), "有效密码应该满足所有要求")
        self.assertEqual(len(requirements), 5, "应该有5个检查项")

        # 测试无效密码
        invalid_password = "weak"
        requirements = PasswordValidator._check_password_requirements(invalid_password)
        self.assertFalse(all(requirements.values()), "无效密码不应该满足所有要求")

    def test_check_password_requirements(self) -> None:
        """测试密码要求检查"""
        # 使用反射访问私有方法进行测试
        from src.db_connector_tool.core.validators import PasswordValidator

        # 测试弱密码
        weak_result = PasswordValidator._check_password_requirements("weak")
        self.assertFalse(weak_result["length_ok"])

        # 测试强密码
        strong_result = PasswordValidator._check_password_requirements(
            "StrongP@ssw0rd123!"
        )
        self.assertTrue(all(strong_result.values()))

        # 测试部分满足的密码
        partial_result = PasswordValidator._check_password_requirements(
            "StrongPassword123"
        )
        self.assertTrue(partial_result["length_ok"])
        self.assertTrue(partial_result["has_uppercase"])
        self.assertTrue(partial_result["has_lowercase"])
        self.assertTrue(partial_result["has_digit"])
        self.assertFalse(partial_result["has_special"])


class TestGenericValidator(unittest.TestCase):
    """测试通用验证器"""

    def setUp(self) -> None:
        """设置测试数据"""
        self.test_data = {
            "name": "test",
            "value": 123,
            "active": True,
            "items": [1, 2, 3],
        }

    def test_validate_required_fields(self) -> None:
        """测试必需字段验证"""
        # 包含所有必需字段
        try:
            GenericValidator.validate_required_fields(self.test_data, ["name", "value"])
        except Exception as e:
            self.fail(f"验证必需字段时抛出了异常: {e}")

        # 测试各种必需字段组合
        field_combinations = [
            ([], "空字段列表"),
            (["name"], "单个字段"),
            (["name", "value"], "多个字段"),
            (["name", "value", "active"], "所有字段"),
        ]

        for fields, description in field_combinations:
            with self.subTest(fields=fields, description=description):
                try:
                    GenericValidator.validate_required_fields(self.test_data, fields)
                except Exception as e:
                    self.fail(f"{description} 验证失败: {e}")

    def test_validate_required_fields_missing(self) -> None:
        """测试缺少必需字段的情况"""
        # 单个缺少字段
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields(
                self.test_data, ["name", "value", "missing_field"]
            )
        self.assertIn("缺少必需字段: missing_field", str(context.exception))

        # 多个缺少字段
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields(
                self.test_data, ["name", "missing1", "missing2"]
            )
        error_msg = str(context.exception)
        self.assertIn("缺少必需字段", error_msg)
        # 注意：当前实现只报告第一个缺失字段

        # 空数据字典
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields({}, ["name"])
        self.assertIn("缺少必需字段: name", str(context.exception))

    def test_validate_required_fields_with_context(self) -> None:
        """测试带上下文的必需字段验证"""
        # 带上下文的错误消息
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields(
                self.test_data, ["name", "value", "missing"], "测试数据"
            )
        self.assertIn("测试数据缺少必需字段: missing", str(context.exception))

        # 空上下文
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields(
                self.test_data, ["name", "value", "missing"], ""
            )
        self.assertIn("缺少必需字段: missing", str(context.exception))

        # 特殊字符上下文
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_required_fields(
                self.test_data, ["name", "value", "missing"], "测试-数据_123"
            )
        self.assertIn("测试-数据_123缺少必需字段: missing", str(context.exception))

    def test_validate_field_type_valid(self) -> None:
        """测试有效的字段类型验证"""
        # 基本类型验证
        valid_cases = [
            (123, int, "整数"),
            ("string", str, "字符串"),
            (True, bool, "布尔值"),
            ([1, 2, 3], list, "列表"),
            ({"key": "value"}, dict, "字典"),
            (3.14, float, "浮点数"),
            (None, type(None), "None类型"),
            (object(), object, "对象"),
            ((1, 2), tuple, "元组"),
            ({1, 2}, set, "集合"),
        ]

        for value, expected_type, description in valid_cases:
            with self.subTest(value=value, type=expected_type, description=description):
                try:
                    GenericValidator.validate_field_type(
                        value, expected_type, f"{description}字段"
                    )
                except ConfigError as e:
                    self.fail(f"有效的类型验证失败: {e}")

    def test_validate_field_type_invalid(self) -> None:
        """测试无效的字段类型验证"""
        # 类型错误测试用例
        invalid_cases = [
            ("123", int, "字符串转整数"),
            (123, str, "整数转字符串"),
            ("string", bool, "字符串转布尔"),
            ("true", bool, "字符串true转布尔"),
            ("1", bool, "字符串1转布尔"),
            ([1, 2], dict, "列表转字典"),
            ({"key": "value"}, list, "字典转列表"),
            (3.14, int, "浮点数转整数"),
            (None, int, "None转整数"),
            (None, str, "None转字符串"),
            ("", int, "空字符串转整数"),
            (0, bool, "数字0转布尔"),
            (1, bool, "数字1转布尔"),
        ]

        for value, expected_type, description in invalid_cases:
            with self.subTest(value=value, type=expected_type, description=description):
                with self.assertRaises(ConfigError) as context:
                    GenericValidator.validate_field_type(
                        value, expected_type, f"{description}字段"
                    )
                expected_msg = f"{description}字段必须是{expected_type.__name__}类型"
                self.assertIn(expected_msg, str(context.exception))

    def test_validate_field_type_edge_cases(self) -> None:
        """测试字段类型验证的边界情况"""
        # 空值和特殊值
        edge_cases = [
            ("", str, "空字符串", True),  # 空字符串是有效的字符串
            ([], list, "空列表", True),  # 空列表是有效的列表
            ({}, dict, "空字典", True),  # 空字典是有效的字典
            (0, int, "零值", True),  # 零是有效的整数
            (False, bool, "假值", True),  # False是有效的布尔值
            # 类型不匹配测试（使用不相关的类型）
            ("123", int, "字符串转整数", False),  # 字符串不是整数
            (123, str, "整数转字符串", False),  # 整数不是字符串
        ]

        for value, expected_type, description, should_pass in edge_cases:
            with self.subTest(value=value, type=expected_type, description=description):
                if should_pass:
                    try:
                        GenericValidator.validate_field_type(
                            value, expected_type, description
                        )
                    except ConfigError as e:
                        self.fail(f"边界情况验证失败: {e}")
                else:
                    with self.assertRaises(ConfigError):
                        GenericValidator.validate_field_type(
                            value, expected_type, description
                        )

    def test_validate_field_type_custom_objects(self) -> None:
        """测试自定义对象的类型验证"""

        class CustomClass:
            pass

        # 自定义类验证
        custom_obj = CustomClass()
        try:
            GenericValidator.validate_field_type(custom_obj, CustomClass, "自定义对象")
        except ConfigError as e:
            self.fail(f"自定义对象验证失败: {e}")

        # 自定义类类型不匹配
        with self.assertRaises(ConfigError):
            GenericValidator.validate_field_type(custom_obj, str, "自定义对象转字符串")

    def test_validate_field_type_none_values(self) -> None:
        """测试None值的类型验证"""
        # None值验证为各种类型都应该失败
        types_to_test = [int, str, bool, list, dict, float]

        for expected_type in types_to_test:
            with self.subTest(type=expected_type.__name__):
                with self.assertRaises(ConfigError):
                    GenericValidator.validate_field_type(
                        None, expected_type, f"None值验证为{expected_type.__name__}"
                    )

    def test_validate_required_fields_edge_cases(self) -> None:
        """测试必需字段验证的边界情况"""
        # 空数据字典
        with self.assertRaises(ConfigError):
            GenericValidator.validate_required_fields({}, ["required_field"], "空数据")

        # None数据应该抛出TypeError（不是ConfigError）
        with self.assertRaises(TypeError):
            GenericValidator.validate_required_fields(None, ["field"], "None数据")  # type: ignore

        # 空字段列表（应该总是成功）
        try:
            GenericValidator.validate_required_fields(self.test_data, [], "空字段列表")
        except Exception as e:
            self.fail(f"空字段列表验证失败: {e}")

    def test_validate_field_type_with_none_field_name(self) -> None:
        """测试字段名称为None的类型验证"""
        # 测试字段名称为None的情况
        with self.assertRaises(ConfigError) as context:
            GenericValidator.validate_field_type("string", int, None)  # type: ignore

        # 错误消息应该包含默认字段名称
        error_msg = str(context.exception)
        self.assertIn("必须是int类型", error_msg)


class TestValidatorIntegration(unittest.TestCase):
    """测试验证器集成功能"""

    def test_config_validator_integration(self) -> None:
        """测试配置验证器的集成功能"""
        # 完整的配置验证流程
        config = {
            "version": "1.0.0",
            "app_name": "test_app",
            "connections": {
                "db1": {
                    "host": "localhost",
                    "port": 5432,
                    "username": "user",
                    "password": "pass",
                    "database": "test_db",
                }
            },
            "metadata": {
                "created": "2024-01-01T00:00:00",
                "last_modified": "2024-01-01T00:00:00",
                "key_version": "1",
            },
        }

        # 验证配置结构
        try:
            ConfigValidator.validate_config(config)
        except Exception as e:
            self.fail(f"完整配置验证失败: {e}")

        # 验证连接名称
        try:
            ConfigValidator.validate_connection_name("db1")
        except Exception as e:
            self.fail(f"连接名称验证失败: {e}")

        # 验证连接配置
        try:
            ConfigValidator.validate_connection_config(config["connections"]["db1"])
        except Exception as e:
            self.fail(f"连接配置验证失败: {e}")

    def test_password_validator_integration(self) -> None:
        """测试密码验证器的集成功能"""
        # 测试密码强度评估和验证的集成
        strong_password = "VeryStrongPassword123!@#"

        # 验证强度等级
        strength = PasswordValidator.get_strength(strong_password)
        self.assertEqual(strength, "very_strong")

        # 验证强度要求
        is_strong = PasswordValidator.validate_strength(strong_password)
        self.assertTrue(is_strong)

        # 验证密码要求
        requirements = PasswordValidator._check_password_requirements(strong_password)
        self.assertTrue(all(requirements.values()))

    def test_generic_validator_integration(self) -> None:
        """测试通用验证器的集成功能"""
        # 测试必需字段和类型验证的集成
        data = {"name": "test", "value": 123, "active": True, "items": [1, 2, 3]}

        # 验证必需字段
        try:
            GenericValidator.validate_required_fields(data, ["name", "value"])
        except Exception as e:
            self.fail(f"必需字段验证失败: {e}")

        # 验证字段类型
        try:
            GenericValidator.validate_field_type(data["name"], str, "名称字段")
            GenericValidator.validate_field_type(data["value"], int, "值字段")
            GenericValidator.validate_field_type(data["active"], bool, "激活字段")
            GenericValidator.validate_field_type(data["items"], list, "项目字段")
        except Exception as e:
            self.fail(f"字段类型验证失败: {e}")


if __name__ == "__main__":
    unittest.main()
