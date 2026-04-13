"""验证器模块 (Validators)

提供统一的验证功能，包括配置验证、连接验证、密码验证等。
所有验证方法集中管理，便于维护和复用。

Example:
>>> from db_connector_tool.core.validators import (
...     ConfigValidator, PasswordValidator, GenericValidator
... )
>>> # 验证配置文件结构
>>> config = {
...     "version": "1.0.0",
...     "app_name": "test",
...     "connections": {},
...     "metadata": {
...         "created": "2023-01-01",
...         "last_modified": "2023-01-01",
...         "key_version": "1"
...     }
... }
>>> ConfigValidator.validate_config(config)
>>> # 验证连接名称
>>> ConfigValidator.validate_connection_name("test_db")
>>> # 验证密码强度
>>> is_strong = PasswordValidator.validate_strength("StrongPassword123!")
>>> # 验证必需字段
>>> GenericValidator.validate_required_fields({"name": "test"}, ["name"], "测试数据")
"""

import re
from typing import Any, Dict, List

from .exceptions import ConfigError


class ConfigValidator:
    """配置验证器 (Config Validator)

    提供配置文件结构和连接配置的验证功能，确保配置数据的完整性和有效性。
    支持版本号格式验证、连接名称验证等功能。

    Example:
        >>> config = {
        ...     "version": "1.0.0",
        ...     "app_name": "test",
        ...     "connections": {},
        ...     "metadata": {
        ...         "created": "2023-01-01",
        ...         "last_modified": "2023-01-01",
        ...         "key_version": "1"
        ...     }
        ... }
        >>> ConfigValidator.validate_config(config)
        >>> ConfigValidator.validate_connection_name("test_db")
        >>> is_valid_version = ConfigValidator.is_valid_version_format("1.0.0")
    """

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """验证配置文件结构

        验证配置文件的结构完整性和有效性，确保所有必需字段存在且格式正确。

        Args:
            config: 要验证的配置字典

        Raises:
            ConfigError: 配置结构无效

        Example:
            >>> config = {
            ...     "version": "1.0.0",
            ...     "app_name": "test",
            ...     "connections": {},
            ...     "metadata": {
            ...         "created": "2023-01-01",
            ...         "last_modified": "2023-01-01",
            ...         "key_version": "1"
            ...     }
            ... }
            >>> ConfigValidator.validate_config(config)
        """

        # 验证必需字段
        required_fields = ["version", "app_name", "connections", "metadata"]
        GenericValidator.validate_required_fields(config, required_fields, "配置文件")

        # 验证版本号格式
        if not ConfigValidator.is_valid_version_format(config["version"]):
            raise ConfigError(f"无效的版本号格式: {config['version']}")

        # 验证connections字段类型
        GenericValidator.validate_field_type(
            config["connections"], dict, "connections字段"
        )

        # 验证metadata字段结构
        metadata = config.get("metadata", {})
        GenericValidator.validate_field_type(metadata, dict, "metadata字段")

        # 验证metadata必需子字段
        required_metadata_fields = ["created", "last_modified", "key_version"]
        GenericValidator.validate_required_fields(
            metadata, required_metadata_fields, "metadata"
        )

        # 验证密钥版本格式
        key_version = metadata.get("key_version")
        if not isinstance(key_version, (str, int)) or not str(key_version).isdigit():
            raise ConfigError("key_version必须是有效的数字字符串")

        # 验证审计日志格式
        audit_log = metadata.get("audit_log", [])
        GenericValidator.validate_field_type(audit_log, list, "audit_log字段")

    @staticmethod
    def is_valid_version_format(version: str) -> bool:
        """验证版本号格式是否符合语义化版本规范

        验证版本号格式是否符合语义化版本规范，格式为 x.y.z，其中 x、y、z 为非负整数且不允许前导零。

        Args:
            version: 版本号字符串

        Returns:
            bool: 版本号格式是否有效

        Example:
            >>> ConfigValidator.is_valid_version_format("1.0.0")
            True
            >>> ConfigValidator.is_valid_version_format("1.0")
            False
            >>> ConfigValidator.is_valid_version_format("1.0.0.1")
            False
            >>> ConfigValidator.is_valid_version_format("1.0.0a")
            False
        """

        try:
            parts = version.split(".")
            if len(parts) != 3:
                return False

            for part in parts:
                if not part.isdigit():
                    return False
                if len(part) > 1 and part.startswith("0"):
                    return False  # 不允许前导零
                num = int(part)
                if num < 0:
                    return False

            return True
        except (ValueError, AttributeError):
            return False

    @staticmethod
    def validate_connection_name(name: str) -> None:
        """验证连接名称是否有效

        验证连接名称是否符合要求，包括非空、字符串类型、长度限制、字符格式和保留字检查。

        Args:
            name: 连接名称

        Raises:
            ValueError: 连接名称无效

        Example:
            >>> ConfigValidator.validate_connection_name("test_db")
            >>> ConfigValidator.validate_connection_name("invalid-name")  # 会抛出 ValueError
            >>> ConfigValidator.validate_connection_name("default")  # 会抛出 ValueError
        """

        if not name or not isinstance(name, str):
            raise ValueError("连接名称不能为空且必须是字符串")

        # 长度限制
        if len(name) > 50:
            raise ValueError("连接名称长度不能超过50个字符")

        # 字符格式（只允许字母、数字、下划线）
        if not re.match(r"^\w+$", name):
            raise ValueError("连接名称只能包含字母、数字和下划线")

        # 保留字检查
        if name in ["default", "test", "backup"]:
            raise ValueError("连接名称不能使用保留字")

    @staticmethod
    def validate_connection_config(connection_config: Dict[str, Any]) -> None:
        """验证连接配置字典是否有效

        验证连接配置字典是否符合要求，包括非空、字典类型和键名格式检查。

        Args:
            connection_config: 连接配置字典

        Raises:
            ValueError: 连接配置无效

        Example:
            >>> config = {"host": "localhost", "port": 3306, "username": "root"}
            >>> ConfigValidator.validate_connection_config(config)
            >>> ConfigValidator.validate_connection_config("invalid")  # 会抛出 ValueError
        """

        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError("连接配置不能为空且必须是字典")

        # 键名格式检查
        for key in connection_config.keys():
            if not isinstance(key, str):
                raise ValueError("连接配置的键必须是字符串")


class PasswordValidator:
    """密码验证器 (Password Validator)

    提供密码强度验证和强度等级评估功能，确保密码安全性。
    支持密码长度、大小写字母、数字和特殊字符的验证。

    Example:
        >>> is_strong = PasswordValidator.validate_strength("StrongPassword123!")
        >>> strength_level = PasswordValidator.get_strength("StrongPassword123!")
        >>> print(f"密码强度: {strength_level}")
    """

    @staticmethod
    def validate_strength(password: str) -> bool:
        """验证密码强度

        验证密码强度是否足够，要求密码长度至少16位，包含大小写字母、数字和特殊字符。

        Args:
            password: 要验证的密码字符串

        Returns:
            bool: 密码强度是否足够

        Example:
            >>> PasswordValidator.validate_strength("StrongPassword123!")
            True
            >>> PasswordValidator.validate_strength("weak")
            False
        """

        requirements = PasswordValidator._check_password_requirements(password)
        return all(requirements.values())

    @staticmethod
    def get_strength(password: str) -> str:
        """获取密码强度等级

        评估密码强度并返回强度等级，分为 very_strong、strong、medium 和 weak 四个等级。

        Args:
            password: 要评估的密码字符串

        Returns:
            str: 密码强度等级

        Example:
            >>> PasswordValidator.get_strength("StrongPassword123!")
            "strong"
            >>> PasswordValidator.get_strength("weak")
            "weak"
        """

        score = 0

        # 长度得分
        if len(password) >= 24:
            score += 3
        elif len(password) >= 16:
            score += 2
        elif len(password) >= 8:
            score += 1

        # 使用统一的密码要求检查方法
        requirements = PasswordValidator._check_password_requirements(password)

        # 复杂度得分（排除长度检查）
        for req_name, req_met in requirements.items():
            if req_name != "length_ok" and req_met:
                score += 1

        # 评估强度等级
        if score >= 7:
            return "very_strong"
        if score >= 5:
            return "strong"
        if score >= 3:
            return "medium"
        return "weak"

    @staticmethod
    def _check_password_requirements(password: str) -> Dict[str, bool]:
        """检查密码是否满足各项要求

        检查密码是否满足各项要求，包括长度、大小写字母、数字和特殊字符。

        Args:
            password: 要验证的密码字符串

        Returns:
            Dict[str, bool]: 各项要求的满足情况

        Example:
            >>> PasswordValidator._check_password_requirements("StrongPassword123!")
            {
                "length_ok": True,
                "has_uppercase": True,
                "has_lowercase": True,
                "has_digit": True,
                "has_special": True
            }
        """

        return {
            "length_ok": len(password) >= 16,
            "has_uppercase": bool(re.search(r"[A-Z]", password)),
            "has_lowercase": bool(re.search(r"[a-z]", password)),
            "has_digit": bool(re.search(r"\d", password)),
            "has_special": bool(
                re.search(r"[!@#$%^&*()_+\-=\[\]{}|;:,.<>?~`\"\'\\/]", password)
            ),
        }


class GenericValidator:
    """通用验证器 (Generic Validator)

    提供通用的验证功能，包括必需字段验证和字段类型验证。
    可用于各种数据结构的基本验证。

    Example:
        >>> data = {"name": "test", "age": 25}
        >>> GenericValidator.validate_required_fields(data, ["name", "age"], "用户数据")
        >>> GenericValidator.validate_field_type(data["age"], int, "年龄字段")
    """

    @staticmethod
    def validate_required_fields(
        data: Dict[str, Any], required_fields: List[str], context: str = ""
    ) -> None:
        """验证必需字段是否存在

        验证数据字典中是否包含所有必需字段，若缺少则抛出异常。

        Args:
            data: 要验证的数据字典
            required_fields: 必需字段列表
            context: 上下文描述，用于错误消息

        Raises:
            ConfigError: 缺少必需字段

        Example:
            >>> data = {"name": "test", "age": 25}
            >>> GenericValidator.validate_required_fields(
            ...     data, ["name", "age"], "用户数据"
            ... )
            >>> GenericValidator.validate_required_fields(
            ...     data, ["name", "email"], "用户数据"
            ... )  # 会抛出 ConfigError
        """

        for field in required_fields:
            if field not in data:
                error_msg = (
                    f"{context}缺少必需字段: {field}"
                    if context
                    else f"缺少必需字段: {field}"
                )
                raise ConfigError(error_msg)

    @staticmethod
    def validate_field_type(value: Any, expected_type: type, field_name: str) -> None:
        """验证字段类型

        验证值的类型是否与期望类型匹配，若不匹配则抛出异常。

        Args:
            value: 要验证的值
            expected_type: 期望的类型
            field_name: 字段名称，用于错误消息

        Raises:
            ConfigError: 类型不匹配

        Example:
            >>> GenericValidator.validate_field_type(25, int, "年龄字段")
            >>> GenericValidator.validate_field_type("25", int, "年龄字段")  # 会抛出 ConfigError
        """

        if not isinstance(value, expected_type):
            raise ConfigError(f"{field_name}必须是{expected_type.__name__}类型")
