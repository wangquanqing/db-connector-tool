"""测试验证器模块

测试重构后的验证器功能是否正常工作。
"""

from src.db_connector_tool.core.exceptions import ConfigError
from src.db_connector_tool.core.validators import (
    ConfigValidator,
    ConnectionValidator,
    GenericValidator,
    PasswordValidator,
)


def test_config_validator():
    """测试配置验证器"""
    print("\n=== 测试配置验证器 ===")

    # 测试有效的配置
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
        print("✅ 配置验证成功")
    except Exception as e:
        print(f"❌ 配置验证失败: {e}")

    # 测试版本号格式
    print("\n测试版本号格式:")
    test_versions = ["1.0.0", "1.10.20", "0.1.0", "1.0", "1.0.0.0", "1.0.a"]
    for version in test_versions:
        try:
            result = ConfigValidator.is_valid_version_format(version)
            print(f"  {version}: {'✅ 有效' if result else '❌ 无效'}")
        except Exception as e:
            print(f"  {version}: ❌ 错误: {e}")

    # 测试连接名称
    print("\n测试连接名称:")
    test_names = [
        "valid_name",
        "name123",
        "name_123",
        "",
        "name with spaces",
        "name@symbol",
    ]
    for name in test_names:
        try:
            ConfigValidator.validate_connection_name(name)
            print(f"  '{name}': ✅ 有效")
        except ValueError as e:
            print(f"  '{name}': ❌ {e}")


def test_connection_validator():
    """测试连接验证器"""
    print("\n=== 测试连接验证器 ===")

    # 测试有效的连接配置
    valid_connection = {
        "type": "postgresql",
        "host": "localhost",
        "port": 5432,
        "username": "test",
        "password": "test123",
        "database": "test_db",
    }

    try:
        ConnectionValidator.validate_basic_config(valid_connection)
        print("✅ 连接配置验证成功")
    except Exception as e:
        print(f"❌ 连接配置验证失败: {e}")

    # 测试不支持的数据库类型
    invalid_connection = {
        "type": "unsupported",
        "host": "localhost",
        "username": "test",
        "password": "test123",
    }

    try:
        ConnectionValidator.validate_basic_config(invalid_connection)
        print("❌ 不支持的数据库类型应该失败")
    except ConfigError as e:
        print(f"✅ 不支持的数据库类型验证失败: {e}")


def test_password_validator():
    """测试密码验证器"""
    print("\n=== 测试密码验证器 ===")

    # 测试密码强度
    test_passwords = [
        "Weak123",  # 弱密码
        "StrongP@ssw0rd123!",  # 强密码
        "VeryStrongPassword123!@#",  # 非常强的密码
        "Short1!",  # 太短
        "NoSpecialChar123",  # 无特殊字符
        "nouppercase123!",  # 无大写字母
        "NOLOWERCASE123!",  # 无小写字母
        "NoDigitPassword!",  # 无数字
    ]

    for password in test_passwords:
        strength = PasswordValidator.get_strength(password)
        is_valid = PasswordValidator.validate_strength(password)
        print(
            f"  '{password}' (长度: {len(password)}): 强度={strength}, 有效={is_valid}"
        )


def test_generic_validator():
    """测试通用验证器"""
    print("\n=== 测试通用验证器 ===")

    # 测试必需字段验证
    test_data = {"name": "test", "value": 123}

    try:
        GenericValidator.validate_required_fields(test_data, ["name", "value"])
        print("✅ 必需字段验证成功")
    except Exception as e:
        print(f"❌ 必需字段验证失败: {e}")

    # 测试字段类型验证
    try:
        GenericValidator.validate_field_type(123, int, "测试字段")
        print("✅ 字段类型验证成功")
    except Exception as e:
        print(f"❌ 字段类型验证失败: {e}")


def test_module_imports():
    """测试模块导入"""
    print("\n=== 测试模块导入 ===")

    try:
        from db_connector_tool.core.config import ConfigManager

        print("✅ ConfigManager 导入成功")
    except Exception as e:
        print(f"❌ ConfigManager 导入失败: {e}")

    try:
        from db_connector_tool.core.connections import DatabaseManager

        print("✅ DatabaseManager 导入成功")
    except Exception as e:
        print(f"❌ DatabaseManager 导入失败: {e}")

    try:
        from db_connector_tool.core.crypto import CryptoManager

        print("✅ CryptoManager 导入成功")
    except Exception as e:
        print(f"❌ CryptoManager 导入失败: {e}")


if __name__ == "__main__":
    print("开始测试验证器模块...")

    test_config_validator()
    test_connection_validator()
    test_password_validator()
    test_generic_validator()
    test_module_imports()

    print("\n测试完成!")
