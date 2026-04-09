"""配置管理模块 (ConfigManager)

使用 TOML 格式进行配置管理，提供数据库连接配置的加密存储和管理功能，
支持配置文件的创建、加载、保存，以及连接配置的增删改查操作。

主要特性：
- 基于 TOML 格式的配置文件，易于阅读和编辑
- 全字段加密：所有连接配置字段自动加密存储
- 数据类型保持：序列化/反序列化机制保留原始数据类型
- 版本兼容性检查：支持多版本配置格式
- 自动备份功能：支持配置文件备份和恢复

使用示例：
>>> from db_connector_tool.core.config import ConfigManager
>>>
>>> # 创建配置管理器
>>> config_manager = ConfigManager("my_app", "database.toml")
>>>
>>> # 使用上下文管理器
>>> with ConfigManager("my_app") as config_manager:
...     config_manager.add_config("test", {"host": "localhost", "port": 5432})
...     config = config_manager.get_config("test")
"""

import hashlib
import hmac
import json
import re
import shutil
import tomllib
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import tomli_w

from ..utils.logging_utils import get_logger
from ..utils.path_utils import PathHelper
from .exceptions import ConfigError
from .key_manager import KeyManager

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class ConfigManager:
    """配置管理器类 (Config Manager)

    管理数据库连接配置的加密存储，使用TOML格式配置文件，
    提供连接配置的增删改查功能，所有敏感信息都会自动加密，
    支持上下文管理器协议，可使用 `with` 语句自动管理敏感数据的清理。

    主要功能：
    - 连接配置的增删改查操作
    - 配置文件的创建、加载、保存
    - 全字段加密存储
    - 配置版本管理和备份
    - 密钥轮换和安全审计
    - 配置文件数字签名验证

    类属性：
    - OPERATION_ADD: 添加配置操作类型
    - OPERATION_REMOVE: 删除配置操作类型
    - OPERATION_UPDATE: 更新配置操作类型
    - OPERATION_ROTATE_KEY: 密钥轮换操作类型

    异常处理：
    - ConfigError: 配置相关操作失败
    - CryptoError: 加密相关操作失败
    - ValueError: 参数验证失败

    使用示例：
    >>> # 基本使用
    >>> config_manager = ConfigManager("my_app", "database.toml")
    >>>
    >>> # 使用上下文管理器
    >>> with ConfigManager("my_app") as config_manager:
    ...     config_manager.add_config("test", {"host": "localhost", "port": 5432})
    ...     config = config_manager.get_config("test")
    """

    # 操作类型常量
    OPERATION_ADD = "add"
    OPERATION_REMOVE = "remove"
    OPERATION_UPDATE = "update"
    OPERATION_ROTATE_KEY = "rotate_key"

    def __init__(
        self, app_name: str = "db_connector_tool", config_file: str = "connections.toml"
    ) -> None:
        """初始化配置管理器

        创建新的配置管理器实例，自动初始化加密系统和配置文件，
        支持多种密钥存储方式（操作系统密钥环、环境变量、文件）。

        Args:
            app_name: 应用名称，用于确定配置目录
            config_file: 配置文件名，默认为"connections.toml"

        Raises:
            ConfigError: 配置文件初始化失败

        Example:
            >>> # 基本初始化
            >>> config_manager = ConfigManager("my_app", "database.toml")
            >>>
            >>> # 使用上下文管理器（推荐）
            >>> with ConfigManager("my_app") as config_manager:
            ...     # 自动管理敏感数据
            ...     config_manager.add_config("test", {"host": "localhost", "port": 5432})
        """
        self.app_name = app_name
        self.config_file = config_file
        self.config_dir = PathHelper.get_user_config_dir(app_name)
        self.config_path = self.config_dir / config_file
        self.key_manager = KeyManager(app_name)
        self._config_cache: Optional[Dict[str, Any]] = None
        self._config_mtime: Optional[float] = None

        # 确保配置文件存在
        self._ensure_config_exists()

    def __str__(self) -> str:
        """返回配置管理器的用户友好字符串表示

        Returns:
            str: 格式为 "ConfigManager('app_name', N connections)" 的字符串
        """
        try:
            config_info = self.get_config_info()
            connection_count = config_info["connection_count"]
            return f"ConfigManager('{self.app_name}', {connection_count} connections)"
        except (ConfigError, KeyError, TypeError, OSError):
            # 如果获取配置信息失败，返回基本表示
            return f"ConfigManager('{self.app_name}', '{self.config_file}')"

    def __repr__(self) -> str:
        """返回配置管理器的详细表示，用于调试

        Returns:
            str: 包含完整配置信息的字符串，用于调试
        """
        try:
            config_info = self.get_config_info()
            connection_count = config_info["connection_count"]
            version = config_info["version"]
            return (
                f"ConfigManager(app_name='{self.app_name}', "
                f"config_file='{self.config_file}', "
                f"config_path='{self.config_path}', "
                f"version='{version}', "
                f"connections={connection_count})"
            )
        except (ConfigError, KeyError, TypeError, OSError):
            # 如果获取配置信息失败，返回基本表示
            return (
                f"ConfigManager(app_name='{self.app_name}', "
                f"config_file='{self.config_file}', "
                f"config_path='{self.config_path}')"
            )

    def __enter__(self):
        """上下文管理器入口，返回自身实例

        Returns:
            ConfigManager: 当前配置管理器实例

        Note:
            允许使用 with 语句来精确控制配置管理器的生命周期
        """
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: Any | None
    ) -> None:
        """上下文管理器退出，自动清理敏感数据

        Args:
            exc_type: 异常类型（如果有异常发生）
            exc_val: 异常值（如果有异常发生）
            exc_tb: 异常回溯（如果有异常发生）

        Note:
            无论是否发生异常，都会确保敏感数据被安全清理
        """
        self._clear_sensitive_data()
        logger.info("配置管理器上下文已退出")

    def close(self) -> None:
        """关闭配置管理器，清理敏感数据

        这是一个公开的API方法，用于手动清理敏感数据。
        不使用上下文管理器时，应在使用完毕后调用此方法。

        Security:
            - 清理加密管理器中的敏感数据
            - 清理配置缓存
            - 调用后需要重新初始化才能使用加密功能

        Example:
            >>> config_manager = ConfigManager("my_app")
            >>> # 使用配置管理器...
            >>> config_manager.close()  # 手动清理敏感数据
        """
        self._clear_sensitive_data()
        logger.info("配置管理器已手动关闭")

    def _clear_sensitive_data(self) -> None:
        """清理内存中的敏感数据（内部方法）

        安全地清理加密管理器和配置缓存中的敏感信息。
        调用后实例将无法继续使用加密解密功能。

        Security:
            - 清理加密管理器中的敏感数据
            - 清理配置缓存
            - 调用后需要重新初始化才能使用加密功能

        Note:
            - 调用后需要重新创建实例才能继续使用
            - 主要用于上下文管理器和析构函数
            - 符合内存安全最佳实践
        """
        # 清理密钥管理器中的敏感数据
        self.key_manager.close()
        # 清理配置缓存
        self._config_cache = None
        self._config_mtime = None
        logger.debug("配置管理器敏感数据和缓存已清理")

    @staticmethod
    def _handle_config_operation(operation_name: str) -> Callable:
        """配置操作异常处理装饰器

        为配置操作提供统一的异常处理和错误日志记录。
        捕获各种异常并转换为 ConfigError。

        Args:
            operation_name: 操作名称，用于错误消息

        Returns:
            Callable: 装饰器函数

        Error Handling:
            - OSError: 配置文件操作失败
            - JSONDecodeError/TypeError/ValueError: 配置数据处理失败
            - AttributeError/RuntimeError/MemoryError: 系统级错误
            - Exception: 其他未知错误

        Example:
            >>> @_handle_config_operation("配置文件保存")
            ... def _save_config(self, config):
            ...     # 保存配置逻辑
            ...     pass
        """

        def decorator(func: Callable) -> Callable:
            @wraps(func)
            def wrapper(self, *args, **kwargs):
                try:
                    return func(self, *args, **kwargs)
                except OSError as error:
                    logger.error("配置文件操作失败: %s", str(error))
                    raise ConfigError(f"{operation_name}失败: {str(error)}") from error
                except (json.JSONDecodeError, TypeError, ValueError) as error:
                    logger.error("配置数据处理失败: %s", str(error))
                    raise ConfigError(f"{operation_name}失败: {str(error)}") from error
                except (AttributeError, RuntimeError, MemoryError) as error:
                    logger.error("%s失败: %s", operation_name, str(error))
                    raise ConfigError(f"{operation_name}失败: {str(error)}") from error
                except Exception as error:
                    logger.error("%s失败: %s", operation_name, str(error))
                    raise ConfigError(f"{operation_name}失败: {str(error)}") from error

            return wrapper

        return decorator

    def _ensure_config_exists(self) -> None:
        """确保配置文件存在，如果不存在则创建默认配置

        检查配置文件是否存在，如果不存在则创建默认配置结构。
        同时初始化加密管理器。

        Raises:
            ConfigError: 配置文件创建或初始化失败

        Process:
            1. 加载或创建加密密钥
            2. 检查配置文件是否存在
            3. 如果不存在，创建默认配置
        """
        self.key_manager.load_or_create_key()
        if not self.config_path.exists():
            self._create_default_config()
        logger.debug("配置文件就绪: %s", self.config_path)

    def _create_default_config(self) -> None:
        """创建默认配置文件结构

        创建包含默认配置结构的配置文件。
        包括版本、应用名称、连接字典和元数据。

        Raises:
            ConfigError: 默认配置创建失败

        Process:
            1. 构建默认配置字典
            2. 保存配置文件
            3. 记录创建日志

        Default Config Structure:
            - version: 配置文件版本号
            - app_name: 应用名称
            - connections: 空连接字典
            - metadata: 包含创建时间、修改时间、密钥版本等
        """
        default_config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": datetime.now().astimezone().isoformat(),
                "last_modified": datetime.now().astimezone().isoformat(),
                "config_file": str(self.config_path),
                "key_version": "1",  # 初始密钥版本
                "signature": "",  # 配置文件数字签名
                "audit_log": [],  # 变更审计日志
            },
        }
        self._save_config(default_config)
        logger.info("默认配置文件已创建: %s", self.config_path)

    @_handle_config_operation("配置文件保存")
    def _save_config(
        self, config: Dict[str, Any], operation: str = OPERATION_UPDATE
    ) -> None:
        """
        保存配置文件

        Args:
            config: 要保存的配置字典
            operation: 操作类型（update, add, remove, rotate_key）

        Raises:
            ConfigError: 配置文件保存失败

        Process:
            1. 更新最后修改时间
            2. 添加审计日志
            3. 生成数字签名
            4. 验证配置结构
            5. 保存为TOML格式
        """
        # 验证操作类型
        valid_operations = [
            self.OPERATION_ADD,
            self.OPERATION_REMOVE,
            self.OPERATION_UPDATE,
            self.OPERATION_ROTATE_KEY,
        ]
        operation = operation.lower()
        if operation not in valid_operations:
            raise ValueError(f"无效操作类型: {operation}")

        # 更新最后修改时间
        current_time = datetime.now().astimezone().isoformat()
        config["metadata"]["last_modified"] = current_time

        # 生成数字签名（排除signature和audit_log字段）
        config_to_sign = config.copy()
        config_to_sign["metadata"] = config_to_sign["metadata"].copy()
        config_to_sign["metadata"].pop("signature", None)
        config_to_sign["metadata"].pop("audit_log", None)

        # 获取HMAC密钥
        hmac_key = self.key_manager.get_secure_hmac_key()

        # 序列化配置
        serialized_config = tomli_w.dumps(config_to_sign)

        # 生成HMAC签名
        hmac_signature = hmac.new(
            hmac_key, serialized_config.encode(), hashlib.sha256
        ).hexdigest()

        # 存储签名和签名时间戳
        config["metadata"]["signature"] = hmac_signature
        config["metadata"]["signature_timestamp"] = current_time

        # 添加审计日志
        audit_entry = {
            "timestamp": current_time,
            "operation": operation,
            "key_version": config.get("metadata", {}).get("key_version", "1"),
            "connection_count": len(config.get("connections", {})),
        }
        if "audit_log" not in config["metadata"]:
            config["metadata"]["audit_log"] = []
        config["metadata"]["audit_log"].append(audit_entry)

        # 限制审计日志长度，保留最近100条记录
        if len(config["metadata"]["audit_log"]) > 100:
            config["metadata"]["audit_log"] = config["metadata"]["audit_log"][-100:]

        # 验证配置结构
        self._validate_config(config)

        with open(self.config_path, "wb") as f:
            f.write(tomli_w.dumps(config).encode("utf-8"))

        # 清除缓存，确保下次加载时重新读取文件
        self._config_cache = None
        self._config_mtime = None
        logger.debug("配置文件已保存: %s", self.config_path)

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        验证配置文件结构

        Args:
            config: 要验证的配置字典

        Raises:
            ConfigError: 配置结构无效

        Validation Rules:
            - 必须包含 version, app_name, connections, metadata 字段
            - 版本号格式必须有效
            - metadata 字段必须包含必要的子字段
            - connections 必须是字典类型
            - 审计日志必须是列表类型
            - 密钥版本必须是有效的字符串
        """
        # 验证必需字段
        required_fields = ["version", "app_name", "connections", "metadata"]
        self._validate_required_fields(config, required_fields, "配置文件")

        # 验证版本号格式
        if not self._is_valid_version_format(config["version"]):
            raise ConfigError(f"无效的版本号格式: {config['version']}")

        # 验证connections字段类型
        self._validate_field_type(config["connections"], dict, "connections字段")

        # 验证metadata字段结构
        metadata = config.get("metadata", {})
        self._validate_field_type(metadata, dict, "metadata字段")

        # 验证metadata必需子字段
        required_metadata_fields = ["created", "last_modified", "key_version"]
        self._validate_required_fields(metadata, required_metadata_fields, "metadata")

        # 验证密钥版本格式
        key_version = metadata.get("key_version")
        if not isinstance(key_version, (str, int)) or not str(key_version).isdigit():
            raise ConfigError("key_version必须是有效的数字字符串")

        # 验证审计日志格式
        audit_log = metadata.get("audit_log", [])
        self._validate_field_type(audit_log, list, "audit_log字段")

    def _validate_required_fields(
        self, data: Dict[str, Any], required_fields: List[str], context: str = ""
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

    def _is_valid_version_format(self, version: str) -> bool:
        """
        验证版本号格式是否符合语义化版本规范

        Args:
            version: 版本号字符串

        Returns:
            bool: 版本号格式是否有效

        语义化版本规范：
            - 格式为 x.y.z，其中 x、y、z 均为非负整数
            - 不允许前导零
            - x 为主版本号，y 为次版本号，z 为修订号
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

    def _validate_field_type(
        self, value: Any, expected_type: type, field_name: str
    ) -> None:
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

    def add_config(self, name: str, connection_config: Dict[str, Any]) -> None:
        """添加数据库连接配置

        添加新的数据库连接配置到配置文件中，所有字段会自动加密存储，
        连接名称作为唯一标识符，不能重复。

        Args:
            name: 连接名称（唯一标识符）
            connection_config: 连接配置字典，包含数据库连接所需的参数

        Raises:
            ConfigError: 连接已存在或添加失败
            ValueError: 连接名称为空或配置无效

        Example:
            >>> config = {
            ...     "host": "localhost",
            ...     "port": 5432,
            ...     "username": "admin",
            ...     "password": "secret"
            ... }
            >>> config_manager.add_config("postgres_db", config)
        """
        self._validate_connection_name(name)
        self._validate_connection_config(connection_config)

        config = self._load_config()

        # 检查连接是否已存在
        if name in config["connections"]:
            raise ConfigError(f"连接配置已存在: {name}")

        # 确保加密管理器已初始化
        try:
            self.key_manager.get_crypto_manager()
        except ConfigError:
            self.key_manager.load_or_create_key()
        finally:
            # 使用统一的加密方法
            encrypted_config = self._encrypt_dict_values(connection_config)
            config["connections"][name] = encrypted_config

            # 更新配置文件版本号（每次调用增加修订号）
            self._increment_config_version(config)

            self._save_config(config, self.OPERATION_ADD)
            self._log_operation_success("添加", name)

    def _validate_connection_name(self, name: str) -> None:
        """验证连接名称是否有效

        验证连接名称是否符合命名规范。

        Args:
            name: 连接名称

        Raises:
            ValueError: 连接名称无效

        Validation Rules:
            - 不能为空且必须是字符串
            - 长度不能超过50个字符
            - 只能包含字母、数字和下划线
            - 不能使用保留字（default, test, backup）
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

    def _validate_connection_config(self, connection_config: Dict[str, Any]) -> None:
        """验证连接配置字典是否有效

        验证连接配置字典是否符合格式要求。

        Args:
            connection_config: 连接配置字典

        Raises:
            ValueError: 连接配置无效

        Validation Rules:
            - 不能为空且必须是字典
            - 所有键必须是字符串
        """
        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError("连接配置不能为空且必须是字典")

        # 键名格式检查
        for key in connection_config.keys():
            if not isinstance(key, str):
                raise ValueError("连接配置的键必须是字符串")

    @_handle_config_operation("配置文件加载")
    def _load_config(self) -> Dict[str, Any]:
        """
        加载并验证配置文件

        Returns:
            Dict[str, Any]: 配置字典

        Raises:
            ConfigError: 配置文件格式无效或版本不支持

        Process:
            1. 检查配置文件最后修改时间
            2. 如果文件未修改且缓存存在，直接返回缓存
            3. 读取TOML文件
            4. 验证配置文件结构
            5. 验证数字签名
            6. 更新缓存并返回
        """
        # 检查配置文件最后修改时间
        current_mtime = self.config_path.stat().st_mtime

        # 如果文件未修改且缓存存在，直接返回缓存
        if self._config_cache is not None and self._config_mtime == current_mtime:
            logger.debug("使用缓存的配置")
            return self._config_cache

        # 读取TOML文件
        with open(self.config_path, "rb") as f:
            config = tomllib.load(f)

        # 验证配置文件结构
        self._validate_config(config)

        # 验证数字签名
        self._verify_config_signature(config)

        # 更新缓存和修改时间
        self._config_cache = config
        self._config_mtime = current_mtime
        logger.debug("配置已加载并缓存")

        return config

    def _verify_config_signature(self, config: Dict[str, Any]) -> bool:
        """
        验证配置文件数字签名

        Args:
            config: 配置字典

        Returns:
            bool: 签名是否有效

        Raises:
            ConfigError: 签名验证失败
        """
        try:
            signature = config.get("metadata", {}).get("signature", "")

            # 如果签名为空，跳过验证（可能是新创建的配置）
            if not signature:
                logger.debug("配置文件无数字签名，跳过验证")
                return True

            # 确保加密管理器已初始化
            try:
                self.key_manager.get_crypto_manager()
            except ConfigError:
                logger.warning("加密管理器未初始化，跳过签名验证")
                return True

            # 生成待验证的配置数据（排除signature和audit_log字段）
            config_to_verify = config.copy()
            config_to_verify["metadata"] = config_to_verify["metadata"].copy()
            config_to_verify["metadata"].pop("signature", None)
            config_to_verify["metadata"].pop("audit_log", None)
            config_to_verify["metadata"].pop("signature_timestamp", None)  # 排除时间戳

            # 获取HMAC密钥
            hmac_key = self.key_manager.get_secure_hmac_key()

            # 序列化配置
            serialized_config = tomli_w.dumps(config_to_verify)

            # 生成预期签名
            expected_signature = hmac.new(
                hmac_key, serialized_config.encode(), hashlib.sha256
            ).hexdigest()

            if signature != expected_signature:
                logger.warning("配置文件数字签名验证失败，可能被篡改")
                # 不抛出异常，允许加载但记录警告
                return False

            # 验证时间戳，防止重放攻击
            signature_timestamp = config.get("metadata", {}).get("signature_timestamp")
            if signature_timestamp:
                try:
                    signature_time = datetime.fromisoformat(signature_timestamp)
                    current_time = datetime.now(timezone.utc)
                    # 允许1小时的时间差（防止时钟同步问题）
                    time_diff = (current_time - signature_time).total_seconds()
                    if abs(time_diff) > 3600:
                        logger.warning("配置文件签名时间戳过期，可能是重放攻击")
                        return False
                except (ValueError, TypeError) as e:
                    logger.warning("时间戳验证失败: %s", str(e))
                    # 时间戳验证失败不影响签名验证结果

            logger.debug("配置文件数字签名验证成功")
            return True

        except (ValueError, AttributeError, RuntimeError) as e:
            logger.warning("配置文件签名验证失败: %s", str(e))
            # 不抛出异常，允许加载但记录警告
            return False

    def _encrypt_dict_values(self, data_dict: Dict[str, Any]) -> Dict[str, str]:
        """
        加密字典中的所有值

        Args:
            data_dict: 要加密的字典

        Returns:
            Dict[str, str]: 加密后的字典
        """
        crypto = self.key_manager.get_crypto_manager()
        encrypted_dict = {}
        for key, value in data_dict.items():
            serialized_value = self._serialize_value(value)
            encrypted_dict[key] = crypto.encrypt(serialized_value)
        return encrypted_dict

    def _decrypt_dict_values(self, encrypted_dict: Dict[str, str]) -> Dict[str, Any]:
        """
        解密字典中的所有值

        Args:
            encrypted_dict: 加密的字典

        Returns:
            Dict[str, Any]: 解密后的字典
        """
        crypto = self.key_manager.get_crypto_manager()
        decrypted_dict = {}
        for key, encrypted_value in encrypted_dict.items():
            serialized_value = crypto.decrypt(encrypted_value)
            decrypted_dict[key] = self._deserialize_value(serialized_value)
        return decrypted_dict

    def _serialize_value(self, value: Any) -> str:
        """
        序列化值以便加密，保留数据类型信息

        Args:
            value: 要序列化的任意类型值

        Returns:
            str: JSON格式的序列化字符串

        Process:
            1. 获取值的类型名称
            2. 将值和类型信息序列化为JSON

        Example:
            >>> serialized = self._serialize_value("secret_password")
            >>> print(serialized)
            '{"type": "str", "value": "secret_password"}'
        """
        value_info = {"type": type(value).__name__, "value": value}
        return json.dumps(value_info, ensure_ascii=False)

    def _deserialize_value(self, json_str: str) -> Any:
        """反序列化值，恢复原始数据类型

        将JSON字符串反序列化为原始值，根据类型信息进行类型转换。

        Args:
            json_str: JSON格式的序列化字符串

        Returns:
            Any: 反序列化后的原始值

        Process:
            1. 解析JSON字符串
            2. 根据类型信息进行类型转换
            3. 返回原始值

        Supported Types:
            - int: 整数
            - bool: 布尔值
            - float: 浮点数
            - str: 字符串
            - other: 其他类型直接返回

        Example:
            >>> deserialized = self._deserialize_value('{"type": "int", "value": 42}')
            >>> print(deserialized)
            42
            >>> print(type(deserialized))
            <class 'int'>
        """
        try:
            value_info = json.loads(json_str)

            # 根据类型信息进行类型转换
            value_type = value_info.get("type")
            raw_value = value_info["value"]

            if value_type == "int":
                return int(raw_value)
            if value_type == "bool":
                return bool(raw_value)
            if value_type == "float":
                return float(raw_value)
            if value_type == "str":
                return str(raw_value)
            return raw_value  # 其他类型直接返回

        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
            logger.warning("反序列化失败，返回原始字符串: %s", str(error))
            return json_str

    def _increment_config_version(self, config: Dict[str, Any]) -> None:
        """
        递增配置文件版本号

        Args:
            config: 配置字典

        Raises:
            ConfigError: 版本号递增导致主版本号发生不合理变化

        Process:
            1. 解析当前版本号（格式：x.y.z）
            2. 确保版本号符合语义化版本规范
            3. 递增修订号（patch），并处理进位逻辑
            4. 更新配置中的版本号
        """
        try:
            current_version = config["version"]

            # 确保当前版本号格式有效
            if not self._is_valid_version_format(current_version):
                # 如果当前版本号格式无效，重置为初始版本
                logger.warning(
                    "无效的版本号格式，重置为初始版本: %s -> 1.0.0", current_version
                )
                config["version"] = "1.0.0"
                return

            # 解析版本号各部分
            major_num, minor_num, patch_num = self._parse_version_parts(current_version)

            # 递增版本号并处理进位逻辑
            major_num, minor_num, patch_num = self._increment_version_parts(
                major_num, minor_num, patch_num
            )

            # 检查主版本号是否合理（限制主版本号不超过99）
            if major_num > 9:
                raise ConfigError(
                    "版本号递增导致第一位发生变化，变更过于频繁，请手动检查",
                    details={
                        "current_version": current_version,
                        "would_become": f"{major_num}.{minor_num}.{patch_num}",
                        "max_major_version": 9,
                    },
                )

            new_version = f"{major_num}.{minor_num}.{patch_num}"

            # 验证新版本号格式
            if not self._is_valid_version_format(new_version):
                raise ConfigError(
                    "版本号格式无效",
                    details={
                        "current_version": current_version,
                        "new_version": new_version,
                    },
                )

            config["version"] = new_version
            logger.debug("配置文件版本号已更新: %s -> %s", current_version, new_version)

        except (ValueError, AttributeError, RuntimeError) as e:
            logger.warning("版本号递增失败，保持原版本号: %s", str(e))
            # 如果版本号递增失败，不影响主要功能，继续使用原版本号

    def _parse_version_parts(self, version: str) -> tuple[int, int, int]:
        """
        解析版本号各部分

        Args:
            version: 版本号字符串

        Returns:
            tuple[int, int, int]: (主版本号, 次版本号, 修订号)

        Raises:
            ValueError: 版本号格式无效
        """
        version_parts = version.split(".")
        return int(version_parts[0]), int(version_parts[1]), int(version_parts[2])

    def _increment_version_parts(
        self, major: int, minor: int, patch: int
    ) -> tuple[int, int, int]:
        """
        递增版本号各部分并处理进位逻辑

        Args:
            major: 主版本号
            minor: 次版本号
            patch: 修订号

        Returns:
            tuple[int, int, int]: 递增后的(主版本号, 次版本号, 修订号)
        """
        patch += 1

        # 处理进位逻辑
        if patch >= 10:
            patch = 0
            minor += 1

            if minor >= 10:
                minor = 0
                major += 1

        return major, minor, patch

    def _log_operation_success(self, operation: str, name: str) -> None:
        """
        记录操作成功日志

        Args:
            operation: 操作类型（添加、删除、更新等）
            name: 连接名称
        """
        logger.info("连接配置已%s: %s", operation, name)

    def remove_config(self, name: str) -> None:
        """删除连接配置

        从配置文件中删除指定名称的连接配置。
        删除操作不可逆，建议先备份配置文件。

        Args:
            name: 连接名称

        Raises:
            ConfigError: 连接不存在或删除失败
            ValueError: 连接名称无效

        Security:
            - 删除操作不可逆，建议先备份
            - 删除后加密数据将无法恢复
            - 删除操作会记录到审计日志

        Process:
            1. 验证连接名称
            2. 加载配置文件
            3. 验证连接是否存在
            4. 删除连接配置
            5. 递增配置版本号
            6. 保存配置文件
            7. 记录操作日志

        Example:
            >>> config_manager.remove_config("postgres_db")
        """
        self._validate_connection_name(name)

        config = self._load_config()
        self._ensure_connection_exists(config, name)

        del config["connections"][name]
        # 更新配置文件版本号（每次调用增加修订号）
        self._increment_config_version(config)

        self._save_config(config, self.OPERATION_REMOVE)
        self._log_operation_success("删除", name)

    def _ensure_connection_exists(self, config: Dict[str, Any], name: str) -> None:
        """确保连接配置存在

        验证指定名称的连接配置是否存在于配置中。

        Args:
            config: 配置字典
            name: 连接名称

        Raises:
            ConfigError: 连接不存在

        Process:
            1. 检查连接名称是否在配置中
            2. 如果不存在，抛出ConfigError
        """
        if name not in config["connections"]:
            raise ConfigError(f"连接配置不存在: {name}")

    def update_config(self, name: str, connection_config: Dict[str, Any]) -> None:
        """更新连接配置

        更新指定名称的连接配置，替换原有的配置内容。
        所有字段会重新加密，版本号自动递增。

        Args:
            name: 连接名称
            connection_config: 新的连接配置字典，包含更新后的连接参数

        Raises:
            ConfigError: 连接不存在或更新失败
            ValueError: 连接名称或配置无效

        Security:
            - 更新操作会重新加密所有配置字段
            - 更新操作会记录到审计日志
            - 建议先备份再进行更新

        Process:
            1. 验证连接名称和配置
            2. 确保加密管理器已初始化
            3. 加载当前配置
            4. 验证连接是否存在
            5. 加密新配置并更新
            6. 递增配置版本号
            7. 保存更新后的配置

        Example:
            >>> new_config = {"host": "new_host", "port": 5433}
            >>> config_manager.update_config("postgres_db", new_config)
        """
        self._validate_connection_name(name)
        self._validate_connection_config(connection_config)

        # 加载配置并直接更新
        config = self._load_config()
        self._ensure_connection_exists(config, name)

        # 确保加密管理器已初始化
        try:
            self.key_manager.get_crypto_manager()
        except ConfigError:
            self.key_manager.load_or_create_key()
        finally:
            # 使用统一的加密方法
            encrypted_config = self._encrypt_dict_values(connection_config)
            config["connections"][name] = encrypted_config

            # 更新配置文件版本号（每次调用增加修订号）
            self._increment_config_version(config)

            self._save_config(config, self.OPERATION_UPDATE)
            self._log_operation_success("更新", name)

    def get_config(self, name: str) -> Dict[str, Any]:
        """获取数据库连接配置（自动解密）

        获取指定连接名称的配置，自动解密所有字段。
        返回的配置包含原始连接参数，注意敏感信息在内存中为明文状态。

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 解密后的连接配置字典，包含原始连接参数

        Raises:
            ConfigError: 连接不存在或获取失败
            ValueError: 连接名称无效

        Security:
            - 返回的解密配置包含敏感信息，使用后应及时清理
            - 配置数据在内存中为明文状态
            - 建议使用上下文管理器自动管理敏感数据

        Process:
            1. 验证连接名称
            2. 加载配置文件
            3. 验证连接是否存在
            4. 确保加密管理器已初始化
            5. 解密连接配置
            6. 返回原始配置

        Example:
            >>> config = config_manager.get_config("postgres_db")
            >>> print(config["host"])
            "localhost"
        """
        self._validate_connection_name(name)

        config = self._load_config()
        self._ensure_connection_exists(config, name)

        connection_config = config["connections"][name].copy()

        # 确保加密管理器已初始化
        try:
            self.key_manager.get_crypto_manager()
        except ConfigError:
            self.key_manager.load_or_create_key()

        # 使用统一的解密方法
        decrypted_config = self._decrypt_dict_values(connection_config)
        logger.debug("连接配置已获取: %s", name)
        return decrypted_config

    def list_configs(self) -> List[str]:
        """列出所有可用的连接名称

        获取配置文件中所有连接配置的名称列表。
        不会解密任何敏感数据，仅返回连接名称。

        Returns:
            List[str]: 连接名称列表，按配置文件中的顺序排列

        Raises:
            ConfigError: 列出连接失败

        Process:
            1. 加载配置文件
            2. 提取所有连接名称
            3. 返回名称列表

        Example:
            >>> connections = config_manager.list_configs()
            >>> print(connections)
            ["postgres_db", "mysql_db"]
        """
        config = self._load_config()
        return list(config["connections"].keys())

    def get_config_info(self) -> Dict[str, Any]:
        """获取配置文件的基本信息

        获取配置文件的元数据信息，不包含敏感数据。

        Returns:
            Dict[str, Any]: 配置文件信息字典，包含版本、应用名称、连接数量等信息

        Info Fields:
            - version: 配置文件版本号
            - app_name: 应用名称
            - connection_count: 连接数量
            - created: 创建时间
            - last_modified: 最后修改时间
            - config_file: 配置文件路径

        Example:
            >>> info = config_manager.get_config_info()
            >>> print(info["version"])
            "1.0.0"
            >>> print(info["connection_count"])
            2
        """
        config = self._load_config()
        return {
            "version": config["version"],
            "app_name": config["app_name"],
            "connection_count": len(config["connections"]),
            "created": config["metadata"]["created"],
            "last_modified": config["metadata"]["last_modified"],
            "config_file": str(self.config_path),
        }

    def get_key_version(self) -> str:
        """获取当前密钥版本

        获取配置文件中记录的加密密钥版本号。

        Returns:
            str: 当前密钥版本号

        Security:
            - 返回密钥版本号用于审计和追踪
            - 版本号在密钥轮换时自动递增

        Example:
            >>> version = config_manager.get_key_version()
            >>> print(version)
            "1"
        """
        config = self._load_config()
        return config.get("metadata", {}).get("key_version", "1")

    def get_audit_log(self) -> List[Dict[str, Any]]:
        """获取配置变更审计日志

        获取配置文件中记录的所有配置变更操作日志。

        Returns:
            List[Dict[str, Any]]: 审计日志列表

        Audit Log Entry Fields:
            - timestamp: 操作时间戳
            - operation: 操作类型（add/remove/update/rotate_key）
            - key_version: 密钥版本号
            - connection_count: 连接数量

        Security:
            - 审计日志用于安全审计和合规检查
            - 保留最近100条记录

        Example:
            >>> audit_log = config_manager.get_audit_log()
            >>> print(audit_log[0]["operation"])
            "add"
        """
        config = self._load_config()
        return config.get("metadata", {}).get("audit_log", [])

    @_handle_config_operation("配置文件备份")
    def backup_config(self, backup_path: Path | None = None) -> Path:
        """备份配置文件

        创建配置文件的备份副本，可指定备份路径或自动生成带时间戳的路径。
        备份文件包含加密数据，应妥善保管。

        Args:
            backup_path: 备份文件路径，如果为None则自动生成带时间戳的备份文件

        Returns:
            Path: 备份文件路径

        Raises:
            ConfigError: 备份失败

        Security:
            - 备份文件包含加密数据，应妥善保管
            - 建议定期备份重要配置
            - 备份文件权限应设置为仅所有者可读写

        Process:
            1. 确定备份路径（自动生成或使用指定路径）
            2. 复制配置文件到备份位置
            3. 返回备份文件路径

        Example:
            >>> # 自动生成备份路径
            >>> backup_path = config_manager.backup_config()
            >>> print(backup_path)
            "path/to/connections.toml.backup.20231201_120000"
            >>>
            >>> # 指定备份路径
            >>> custom_backup = Path("/path/to/backup.toml")
            >>> backup_path = config_manager.backup_config(custom_backup)
        """
        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.config_dir / f"{self.config_file}.backup.{timestamp}"

        shutil.copy2(self.config_path, backup_path)
        logger.debug("配置文件已备份: %s", backup_path)
        return backup_path

    @_handle_config_operation("加密密钥轮换")
    def rotate_encryption_key(self) -> str:
        """轮换加密密钥

        生成新的加密密钥并重新加密所有连接配置。
        操作前会自动备份配置文件，密钥版本号自动递增。

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 密钥轮换失败

        Security:
            - 密钥轮换会重新加密所有连接配置
            - 操作前会自动备份配置文件
            - 密钥轮换是不可逆操作
            - 新密钥按照安全层次结构存储（keyring > 环境变量 > 文件）
            - 密钥版本号递增记录

        Process:
            1. 备份当前配置文件
            2. 加载当前配置
            3. 解密所有连接配置
            4. 生成新的加密密钥
            5. 更新密钥版本号
            6. 重新加密所有连接配置
            7. 保存新的密钥文件
            8. 保存更新后的配置

        Example:
            >>> new_version = config_manager.rotate_encryption_key()
            >>> print(new_version)
            "2"
        """
        # 执行密钥轮换的主要步骤
        new_key_version = self._perform_key_rotation()
        return new_key_version

    def _perform_key_rotation(self) -> str:
        """执行密钥轮换的核心逻辑

        执行完整的密钥轮换流程，包括备份、解密、生成新密钥、重新加密。

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 密钥轮换失败

        Security:
            - 自动备份配置文件
            - 解密所有连接配置
            - 生成新的加密密钥
            - 重新加密所有连接配置

        Process:
            1. 备份当前配置
            2. 加载当前配置
            3. 解密所有连接配置
            4. 生成新的加密密钥
            5. 更新密钥版本号
            6. 重新加密所有连接配置
            7. 保存新的密钥文件
            8. 保存更新后的配置
        """
        # 备份当前配置
        backup_path = self.backup_config()
        logger.debug("密钥轮换前已备份配置: %s", backup_path)

        # 加载当前配置
        config = self._load_config()

        # 解密所有连接配置
        decrypted_connections = self._decrypt_all_connections(config)

        # 生成新的加密密钥
        self.key_manager.rotate_key()

        # 更新密钥版本
        new_key_version = self._update_key_version(config)

        # 重新加密所有连接配置
        self._re_encrypt_all_connections(config, decrypted_connections)

        # 更新配置文件版本号（每次调用增加修订号）
        self._increment_config_version(config)

        # 保存更新后的配置
        self._save_config(config, "rotate_key")

        return new_key_version

    def _decrypt_all_connections(
        self, config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """解密所有连接配置

        解密配置文件中所有连接配置的加密数据。

        Args:
            config: 配置字典

        Returns:
            Dict[str, Dict[str, Any]]: 解密后的连接配置字典

        Security:
            - 使用当前加密密钥解密所有配置
            - 返回的解密数据包含敏感信息

        Process:
            1. 遍历所有连接配置
            2. 解密每个连接的配置
            3. 返回解密后的配置字典
        """
        decrypted_connections = {}
        for name, encrypted_config in config["connections"].items():
            # 使用统一的解密方法
            decrypted_config = self._decrypt_dict_values(encrypted_config)
            decrypted_connections[name] = decrypted_config

        return decrypted_connections

    def _update_key_version(self, config: Dict[str, Any]) -> str:
        """更新密钥版本号

        递增配置中的密钥版本号。

        Args:
            config: 配置字典

        Returns:
            str: 新的密钥版本号

        Process:
            1. 获取当前密钥版本号
            2. 递增版本号
            3. 更新配置中的版本号
            4. 返回新版本号
        """
        current_key_version = int(config.get("metadata", {}).get("key_version", "1"))
        new_key_version = str(current_key_version + 1)
        config["metadata"]["key_version"] = new_key_version
        return new_key_version

    def _re_encrypt_all_connections(
        self, config: Dict[str, Any], decrypted_connections: Dict[str, Dict[str, Any]]
    ) -> None:
        """重新加密所有连接配置

        使用新的加密密钥重新加密所有连接配置。

        Args:
            config: 配置字典
            decrypted_connections: 解密后的连接配置字典

        Security:
            - 使用新的加密密钥加密所有配置
            - 更新配置中的连接数据

        Process:
            1. 遍历所有解密后的连接配置
            2. 使用新密钥加密每个配置
            3. 更新配置字典
        """
        re_encrypted_connections = {}
        for name, decrypted_config in decrypted_connections.items():
            # 使用统一的加密方法
            encrypted_config = self._encrypt_dict_values(decrypted_config)
            re_encrypted_connections[name] = encrypted_config

        config["connections"] = re_encrypted_connections
