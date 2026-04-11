"""验证器模块

提供统一的验证功能，包括配置验证、 连接验证、 密码验证等。
所有验证方法集中管理，便于维护和复用。
"""

import re
from typing import Any, Dict, List, Set

from .exceptions import ConfigError


class ConfigValidator:
    """配置验证器"""

    @staticmethod
    def validate_config(config: Dict[str, Any]) -> None:
        """
        验证配置文件结构

        Args:
            config: 要验证的配置字典

        Raises:
            ConfigError: 配置结构无效
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
        """
        验证版本号格式是否符合语义化版本规范

        Args:
            version: 版本号字符串

        Returns:
            bool: 版本号格式是否有效
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
        """
        验证连接名称是否有效

        Args:
            name: 连接名称

        Raises:
            ValueError: 连接名称无效
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
        """
        验证连接配置字典是否有效

        Args:
            connection_config: 连接配置字典

        Raises:
            ValueError: 连接配置无效
        """
        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError("连接配置不能为空且必须是字典")

        # 键名格式检查
        for key in connection_config.keys():
            if not isinstance(key, str):
                raise ValueError("连接配置的键必须是字符串")


class ConnectionValidator:
    """连接验证器"""

    # 支持的数据库类型
    SUPPORTED_DATABASE_TYPES: Set[str] = {
        "oracle",
        "postgresql",
        "mysql",
        "mssql",
        "sqlite",
        "gbasedbt",
    }

    @staticmethod
    def validate_basic_config(config: Dict[str, Any]) -> None:
        """
        基本配置验证

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当基本配置验证失败时
        """
        ConnectionValidator._validate_config_structure(config)
        db_type = ConnectionValidator._validate_database_type(config)
        ConnectionValidator._validate_string_parameters(config)

        # SQLite数据库特殊处理
        if db_type == "sqlite":
            ConnectionValidator._handle_sqlite_config(config)
            return

        # 应用特定数据库类型的默认配置
        ConnectionValidator._apply_default_config(config)

        # 验证必需参数和其他参数
        ConnectionValidator._validate_required_parameters(config)
        ConnectionValidator._validate_optional_parameters(config)
        ConnectionValidator._validate_database_specific_parameters(config, db_type)

    @staticmethod
    def _validate_config_structure(config: Dict[str, Any]) -> None:
        """
        验证配置结构

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当配置结构无效时
        """
        if not config or not isinstance(config, dict):
            raise ConfigError("连接配置不能为空且必须是字典")

    @staticmethod
    def _validate_database_type(config: Dict[str, Any]) -> str:
        """
        验证数据库类型

        Args:
            config: 连接配置字典

        Returns:
            str: 数据库类型

        Raises:
            ConfigError: 当数据库类型不支持时
        """
        db_type = config.get("type", "").lower()
        if db_type not in ConnectionValidator.SUPPORTED_DATABASE_TYPES:
            supported_types = ", ".join(
                sorted(ConnectionValidator.SUPPORTED_DATABASE_TYPES)
            )
            raise ConfigError(
                f"不支持的数据库类型: {db_type}，支持的类型: {supported_types}"
            )
        return db_type

    @staticmethod
    def _validate_string_parameters(config: Dict[str, Any]) -> None:
        """
        验证字符串参数

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当字符串参数无效时
        """
        string_params = [
            "username",
            "password",
            "host",
            "database",
            "service_name",
            "sid",
            "server",
        ]
        for param in string_params:
            if param in config:
                ConnectionValidator._validate_string_param(param, config[param])

    @staticmethod
    def _validate_string_param(
        param_name: str, value: Any, max_length: int = 100
    ) -> None:
        """
        验证字符串参数

        Args:
            param_name: 参数名称
            value: 参数值
            max_length: 最大长度

        Raises:
            ConfigError: 当字符串参数无效时
        """
        if not isinstance(value, str):
            raise ConfigError(f"{param_name}必须是字符串类型")
        if len(value) > max_length:
            raise ConfigError(f"{param_name}长度不能超过{max_length}个字符")
        # 检查特殊字符，防止注入攻击
        if re.search(r"[;\\\'\"]", value):
            raise ConfigError(f"{param_name}包含不允许的特殊字符")

    @staticmethod
    def _handle_sqlite_config(config: Dict[str, Any]) -> None:
        """
        处理SQLite数据库配置

        Args:
            config: 连接配置字典
        """
        if "database" not in config:
            config["database"] = ":memory:"

    @staticmethod
    def _validate_required_parameters(config: Dict[str, Any]) -> None:
        """
        验证必需参数

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当缺少必需参数时
        """
        required_fields = ["username", "password", "host"]
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise ConfigError(f"缺少必需的连接参数: {', '.join(missing_fields)}")

    @staticmethod
    def _validate_optional_parameters(config: Dict[str, Any]) -> None:
        """
        验证可选参数

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当可选参数无效时
        """
        # 验证端口号
        if "port" in config:
            port = config["port"]
            if not isinstance(port, int) or port <= 0 or port > 65535:
                raise ConfigError("端口号必须是1-65535之间的整数")

        # 验证连接超时参数
        if "timeout" in config:
            timeout = config["timeout"]
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise ConfigError("连接超时必须是大于0的数值")

        # 验证连接池参数
        if "pool_size" in config:
            pool_size = config["pool_size"]
            if not isinstance(pool_size, int) or pool_size <= 0:
                raise ConfigError("连接池大小必须是大于0的整数")

    @staticmethod
    def _validate_database_specific_parameters(
        config: Dict[str, Any], db_type: str
    ) -> None:
        """
        验证特定数据库类型的参数

        Args:
            config: 连接配置字典
            db_type: 数据库类型

        Raises:
            ConfigError: 当特定数据库类型的参数无效时
        """
        if db_type == "oracle":
            if "service_name" not in config and "sid" not in config:
                raise ConfigError("Oracle数据库必须提供service_name或sid")
        elif db_type == "postgresql" and "database" not in config:
            raise ConfigError("PostgreSQL数据库必须提供database参数")
        elif db_type == "mysql" and "database" not in config:
            raise ConfigError("MySQL数据库必须提供database参数")
        elif db_type == "mssql" and "database" not in config:
            raise ConfigError("SQL Server数据库必须提供database参数")
        elif db_type == "gbasedbt" and "database" not in config:
            raise ConfigError("GBase数据库必须提供database参数")

    @staticmethod
    def _apply_default_config(config: Dict[str, Any]) -> None:
        """
        应用特定数据库类型的默认配置

        Args:
            config: 连接配置字典
        """
        db_type = config.get("type", "").lower()

        if db_type == "oracle" and "service_name" not in config:
            config["service_name"] = "XE"
        elif db_type == "postgresql" and "gssencmode" not in config:
            config["gssencmode"] = "disable"
        elif db_type == "mssql":
            if "charset" not in config:
                config["charset"] = "cp936"
            if "tds_version" not in config:
                config["tds_version"] = "7.0"
        elif db_type == "gbasedbt" and "server" not in config:
            config["server"] = "gbase01"


class PasswordValidator:
    """密码验证器"""

    @staticmethod
    def validate_strength(password: str) -> bool:
        """
        验证密码强度

        Args:
            password: 要验证的密码字符串

        Returns:
            bool: 密码强度是否足够
        """
        requirements = PasswordValidator._check_password_requirements(password)
        return all(requirements.values())

    @staticmethod
    def get_strength(password: str) -> str:
        """
        获取密码强度等级

        Args:
            password: 要评估的密码字符串

        Returns:
            str: 密码强度等级
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
        """
        检查密码是否满足各项要求

        Args:
            password: 要验证的密码字符串

        Returns:
            Dict[str, bool]: 各项要求的满足情况
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
    """通用验证器"""

    @staticmethod
    def validate_required_fields(
        data: Dict[str, Any], required_fields: List[str], context: str = ""
    ) -> None:
        """
        验证必需字段是否存在

        Args:
            data: 要验证的数据字典
            required_fields: 必需字段列表
            context: 上下文描述，用于错误消息

        Raises:
            ConfigError: 缺少必需字段
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
        """
        验证字段类型

        Args:
            value: 要验证的值
            expected_type: 期望的类型
            field_name: 字段名称，用于错误消息

        Raises:
            ConfigError: 类型不匹配
        """
        if not isinstance(value, expected_type):
            raise ConfigError(f"{field_name}必须是{expected_type.__name__}类型")
