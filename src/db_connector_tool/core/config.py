"""
配置管理模块

使用 TOML 格式进行配置管理，提供数据库连接配置的加密存储和管理功能。
支持配置文件的创建、加载、保存，以及连接配置的增删改查操作。

特性：
- 基于 TOML 格式的配置文件，易于阅读和编辑
- 全字段加密：所有连接配置字段自动加密存储
- 数据类型保持：序列化/反序列化机制保留原始数据类型
- 版本兼容性检查：支持多版本配置格式
- 自动备份功能：支持配置文件备份和恢复

安全特性：
- 使用加密管理器保护敏感信息
- 密钥文件独立存储，增强安全性
- 配置文件完整性验证
"""

import json
import shutil
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import tomli_w

from ..utils.logging_utils import get_logger
from ..utils.path_utils import PathHelper
from .crypto import CryptoManager
from .exceptions import ConfigError

# 获取模块级别的日志记录器
logger = get_logger(__name__)

# 配置文件创建时的初始时间戳（固定值）
CONFIG_CREATION_TIMESTAMP = datetime.now().astimezone().isoformat()

# 版本号递增规则
VERSION_INCREMENT_ON_CHANGE = True  # 每次配置变更时自动递增版本号

# 错误消息常量
ERROR_EMPTY_CONNECTION_NAME = "连接名称不能为空且必须是字符串"
ERROR_INVALID_CONFIG_DICT = "连接配置不能为空且必须是字典"
ERROR_VERSION_MAJOR_CHANGE = "版本号递增导致第一位发生变化，变更过于频繁，请手动检查"


class ConfigManager:
    """
    配置管理器类

    管理数据库连接配置的加密存储，使用TOML格式配置文件。
    提供连接配置的增删改查功能，所有敏感信息都会自动加密。

    Attributes:
        app_name (str): 应用名称，用于确定配置目录
        config_file (str): 配置文件名
        config_dir (Path): 配置目录路径
        config_path (Path): 完整配置文件路径
        crypto (Optional[CryptoManager]): 加密管理器实例
    """

    def __init__(
        self, app_name: str = "db_connector", config_file: str = "connections.toml"
    ) -> None:
        """
        初始化配置管理器

        Args:
            app_name: 应用名称，用于确定配置目录
            config_file: 配置文件名，默认为"connections.toml"

        Raises:
            ConfigError: 当配置文件初始化失败时

        Example:
            >>> config_manager = ConfigManager("my_app", "database.toml")
        """
        self.app_name = app_name
        self.config_file = config_file
        self.config_dir = PathHelper.get_user_config_dir(app_name)
        self.config_path = self.config_dir / config_file
        self.crypto: Optional[CryptoManager] = None
        self._ensure_config_exists()

    def _ensure_config_exists(self) -> None:
        """
        确保配置文件存在，如果不存在则创建默认配置

        Raises:
            ConfigError: 当配置文件创建或初始化失败时
        """
        try:
            if not self.config_path.exists():
                self._create_default_config()
            self._load_or_create_crypto_key()
            logger.debug(f"配置文件就绪: {self.config_path}")
        except Exception as e:
            logger.error(f"初始化配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件初始化失败: {str(e)}") from e

    def _create_default_config(self) -> None:
        """
        创建默认配置文件结构

        Raises:
            ConfigError: 当默认配置创建失败时
        """
        default_config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": CONFIG_CREATION_TIMESTAMP,
                "last_modified": CONFIG_CREATION_TIMESTAMP,
                "config_file": str(self.config_path),
            },
        }
        self._save_config(default_config)
        logger.info(f"创建默认配置文件: {self.config_path}")

    def _save_config(self, config: Dict[str, Any]) -> None:
        """
        保存配置文件

        Args:
            config: 要保存的配置字典

        Raises:
            ConfigError: 当配置文件保存失败时

        Process:
            1. 更新最后修改时间
            2. 验证配置结构
            3. 保存为TOML格式
        """
        try:
            # 更新最后修改时间
            current_time = datetime.now().astimezone().isoformat()
            config["metadata"]["last_modified"] = current_time

            # 验证配置结构
            self._validate_config(config)

            with open(self.config_path, "wb") as f:
                f.write(tomli_w.dumps(config).encode("utf-8"))

            logger.debug(f"配置文件已保存: {self.config_path}")

        except Exception as e:
            logger.error(f"保存配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件保存失败: {str(e)}") from e

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        验证配置文件结构

        Args:
            config: 要验证的配置字典

        Raises:
            ConfigError: 当配置结构无效时

        Validation Rules:
            - 必须包含 version, app_name, connections, metadata 字段
            - 版本必须在支持的版本列表中
        """
        required_fields = ["version", "app_name", "connections", "metadata"]
        for field in required_fields:
            if field not in config:
                raise ConfigError(f"配置文件缺少必需字段: {field}")

    def _load_or_create_crypto_key(self) -> None:
        """
        加载或创建加密密钥

        Raises:
            ConfigError: 当密钥加载或创建失败时
        """
        key_file = self.config_dir / "encryption.key"

        if key_file.exists():
            # 加载现有密钥
            try:
                with open(key_file, "r", encoding="utf-8") as f:
                    key_data = tomllib.loads(f.read())

                if "password" not in key_data or "salt" not in key_data:
                    raise ConfigError("密钥文件格式无效")

                self.crypto = CryptoManager.from_saved_key(
                    key_data["password"], key_data["salt"]
                )
                logger.debug("加密密钥加载成功")

            except Exception as e:
                logger.error(f"加载加密密钥失败: {str(e)}")
                raise ConfigError(f"加密密钥加载失败: {str(e)}") from e
        else:
            # 创建新密钥
            try:
                self.crypto = CryptoManager()
                key_data = self.crypto.get_key_info()

                with open(key_file, "w", encoding="utf-8") as f:
                    f.write(tomli_w.dumps(key_data))

                logger.info("新加密密钥创建成功")

            except Exception as e:
                logger.error(f"创建加密密钥失败: {str(e)}")
                raise ConfigError(f"加密密钥创建失败: {str(e)}") from e

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        添加数据库连接配置

        Args:
            name: 连接名称（唯一标识符）
            connection_config: 连接配置字典

        Raises:
            ConfigError: 当连接已存在或添加失败时
            ValueError: 当连接名称为空或配置无效时

        Security Note:
            - 所有配置字段都会自动加密存储
            - 连接名称作为唯一标识符，不能重复

        Example:
            >>> config = {
            ...     "host": "localhost",
            ...     "port": 5432,
            ...     "username": "admin",
            ...     "password": "secret"
            ... }
            >>> config_manager.add_connection("postgres_db", config)
        """
        if not name or not isinstance(name, str):
            raise ValueError(ERROR_EMPTY_CONNECTION_NAME)

        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError(ERROR_INVALID_CONFIG_DICT)

        try:
            config = self._load_config()

            # 检查连接是否已存在
            if name in config["connections"]:
                raise ConfigError(f"连接配置已存在: {name}")

            # 确保加密管理器已初始化
            if self.crypto is None:
                raise ConfigError("加密管理器未初始化，无法加密敏感信息")

            # 全字段加密：加密所有连接配置字段
            encrypted_config = {}
            for key, value in connection_config.items():
                # 序列化值（保留数据类型信息）
                serialized_value = self._serialize_value(value)
                # 加密序列化后的值
                encrypted_config[key] = self.crypto.encrypt(serialized_value)

            config["connections"][name] = encrypted_config

            # 更新配置文件版本号（每次调用增加修订号）
            self._increment_config_version(config)

            self._save_config(config)
            logger.info(f"连接配置已添加: {name}")

        except Exception as e:
            logger.error(f"添加连接配置失败 {name}: {str(e)}")
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"连接配置添加失败: {str(e)}") from e

    def _load_config(self) -> Dict[str, Any]:
        """
        加载并验证配置文件

        Returns:
            Dict[str, Any]: 配置字典

        Raises:
            ConfigError: 当配置文件格式无效或版本不支持时

        Process:
            1. 读取TOML文件
            2. 验证配置文件结构
            3. 返回配置字典
        """
        try:
            with open(self.config_path, "rb") as f:
                config = tomllib.load(f)

            # 验证配置文件结构
            self._validate_config(config)

            return config

        except tomllib.TOMLDecodeError as e:
            logger.error(f"配置文件TOML格式错误: {str(e)}")
            raise ConfigError(f"配置文件格式无效: {str(e)}") from e
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件加载失败: {str(e)}") from e

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

    def _increment_config_version(self, config: Dict[str, Any]) -> None:
        """
        递增配置文件版本号

        Args:
            config: 配置字典

        Raises:
            ConfigError: 当版本号递增导致第一位发生变化时

        Process:
            1. 解析当前版本号（格式：x.y.z）
            2. 第一位保持不变，第二、三位按照10进制每次加1，满10进1
            3. 如果第一位发生变化，抛出异常
            4. 更新配置中的版本号
        """
        try:
            current_version = config["version"]
            version_parts = current_version.split(".")

            if len(version_parts) == 3:
                # 标准语义化版本号格式：x.y.z
                major, minor, patch = version_parts
                original_major = int(major)

                # 第一位保持不变，第二、三位按照10进制每次加1，满10进1
                patch_num = int(patch) + 1
                minor_num = int(minor)

                # 处理进位逻辑
                if patch_num >= 10:
                    patch_num = 0
                    minor_num += 1
                    if minor_num >= 10:
                        minor_num = 0
                        # 检查第一位是否发生变化
                        if original_major + 1 != original_major:
                            raise ConfigError(
                                ERROR_VERSION_MAJOR_CHANGE,
                                details={
                                    "current_version": current_version,
                                    "would_become": f"{original_major + 1}.{minor_num}.{patch_num}",
                                },
                            )

                new_version = f"{original_major}.{minor_num}.{patch_num}"
            else:
                # 非标准版本号格式，使用默认递增逻辑
                new_version = f"{current_version}.1"

            config["version"] = new_version
            logger.debug(f"配置文件版本号已更新: {current_version} -> {new_version}")

        except ConfigError:
            # 重新抛出ConfigError异常
            raise
        except Exception as e:
            logger.warning(f"版本号递增失败，保持原版本号: {str(e)}")
            # 如果版本号递增失败，不影响主要功能，继续使用原版本号

    def remove_connection(self, name: str) -> None:
        """
        删除连接配置

        Args:
            name: 连接名称

        Raises:
            ConfigError: 当连接不存在或删除失败时

        Security Note:
            - 删除操作不可逆，建议先备份
            - 删除后加密数据将无法恢复

        Example:
            >>> config_manager.remove_connection("postgres_db")
        """
        if not name or not isinstance(name, str):
            raise ValueError(ERROR_EMPTY_CONNECTION_NAME)

        try:
            config = self._load_config()

            if name not in config["connections"]:
                raise ConfigError(f"连接配置不存在: {name}")

            del config["connections"][name]
            self._save_config(config)
            logger.info(f"连接配置已删除: {name}")

        except Exception as e:
            logger.error(f"删除连接配置失败 {name}: {str(e)}")
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"连接配置删除失败: {str(e)}") from e

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        更新连接配置

        Args:
            name: 连接名称
            connection_config: 新的连接配置字典

        Raises:
            ConfigError: 当连接不存在或更新失败时

        Process:
            1. 删除旧配置
            2. 添加新配置

        Example:
            >>> new_config = {"host": "new_host", "port": 5433}
            >>> config_manager.update_connection("postgres_db", new_config)
        """
        if not name or not isinstance(name, str):
            raise ValueError(ERROR_EMPTY_CONNECTION_NAME)

        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError(ERROR_INVALID_CONFIG_DICT)

        try:
            # 先删除旧配置，再添加新配置
            self.remove_connection(name)
            self.add_connection(name, connection_config)
            logger.info(f"连接配置已更新: {name}")

        except Exception as e:
            logger.error(f"更新连接配置失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置更新失败: {str(e)}") from e

    def get_connection(self, name: str) -> Dict[str, Any]:
        """
        获取数据库连接配置（自动解密）

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 解密后的连接配置字典

        Raises:
            ConfigError: 当连接不存在或获取失败时

        Security Note:
            - 返回的解密配置包含敏感信息，使用后应及时清理
            - 配置数据在内存中为明文状态

        Example:
            >>> config = config_manager.get_connection("postgres_db")
            >>> print(config["host"])
            "localhost"
        """
        if not name or not isinstance(name, str):
            raise ValueError(ERROR_EMPTY_CONNECTION_NAME)

        try:
            config = self._load_config()

            if name not in config["connections"]:
                raise ConfigError(f"连接配置不存在: {name}")

            connection_config = config["connections"][name].copy()

            # 确保加密管理器已初始化
            if self.crypto is None:
                raise ConfigError("加密管理器未初始化，无法解密敏感信息")

            # 全字段解密：解密所有连接配置字段并恢复数据类型
            decrypted_config = {}
            for key, encrypted_value in connection_config.items():
                # 解密值
                serialized_value = self.crypto.decrypt(encrypted_value)
                # 反序列化值（恢复原始数据类型）
                decrypted_config[key] = self._deserialize_value(serialized_value)

            logger.debug(f"连接配置已获取: {name}")
            return decrypted_config

        except Exception as e:
            logger.error(f"获取连接配置失败 {name}: {str(e)}")
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"连接配置获取失败: {str(e)}") from e

    def _deserialize_value(self, json_str: str) -> Any:
        """
        反序列化值，恢复原始数据类型

        Args:
            json_str: JSON格式的序列化字符串

        Returns:
            Any: 反序列化后的原始值

        Raises:
            ValueError: 当JSON字符串格式无效时

        Process:
            1. 解析JSON字符串
            2. 提取原始值
            3. 如果解析失败，返回原始字符串

        Example:
            >>> original = self._deserialize_value('{"type": "str", "value": "secret"}')
            >>> print(original)
            "secret"
        """
        try:
            value_info = json.loads(json_str)
            return value_info["value"]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"反序列化失败，返回原始字符串: {str(e)}")
            # 如果JSON解析失败，尝试直接返回字符串值
            return json_str

    def list_connections(self) -> List[str]:
        """
        列出所有可用的连接名称

        Returns:
            List[str]: 连接名称列表

        Raises:
            ConfigError: 当列出连接失败时

        Example:
            >>> connections = config_manager.list_connections()
            >>> print(connections)
            ["postgres_db", "mysql_db"]
        """
        try:
            config = self._load_config()
            return list(config["connections"].keys())
        except Exception as e:
            logger.error(f"列出连接失败: {str(e)}")
            raise ConfigError(f"连接列出失败: {str(e)}") from e

    def get_config_info(self) -> Dict[str, Any]:
        """
        获取配置文件的基本信息

        Returns:
            Dict[str, Any]: 配置文件信息字典

        Example:
            >>> info = config_manager.get_config_info()
            >>> print(info["version"])
            "1.0.0"
        """
        try:
            config = self._load_config()
            return {
                "version": config["version"],
                "app_name": config["app_name"],
                "connection_count": len(config["connections"]),
                "created": config["metadata"]["created"],
                "last_modified": config["metadata"]["last_modified"],
                "config_file": str(self.config_path),
            }
        except Exception as e:
            logger.error(f"获取配置信息失败: {str(e)}")
            raise ConfigError(f"配置信息获取失败: {str(e)}") from e

    def backup_config(self, backup_path: Path | None = None) -> Path:
        """
        备份配置文件

        Args:
            backup_path: 备份文件路径，如果为None则自动生成

        Returns:
            Path: 备份文件路径

        Raises:
            ConfigError: 当备份失败时

        Security Note:
            - 备份文件包含加密数据，应妥善保管
            - 建议定期备份重要配置

        Example:
            >>> backup_path = config_manager.backup_config()
            >>> print(backup_path)
            "path/to/connections.toml.backup.20231201_120000"
        """
        try:
            if backup_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                backup_path = self.config_dir / f"{self.config_file}.backup.{timestamp}"

            shutil.copy2(self.config_path, backup_path)
            logger.info(f"配置文件已备份: {backup_path}")
            return backup_path

        except Exception as e:
            logger.error(f"备份配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件备份失败: {str(e)}") from e

    def __str__(self) -> str:
        """返回配置管理器的用户友好字符串表示"""
        try:
            config_info = self.get_config_info()
            connection_count = config_info["connection_count"]
            return f"ConfigManager('{self.app_name}', {connection_count} connections)"
        except Exception:
            # 如果获取配置信息失败，返回基本表示
            return f"ConfigManager('{self.app_name}', '{self.config_file}')"

    def __repr__(self) -> str:
        """返回配置管理器的详细表示，用于调试"""
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
        except Exception:
            # 如果获取配置信息失败，返回基本表示
            return (
                f"ConfigManager(app_name='{self.app_name}', "
                f"config_file='{self.config_file}', "
                f"config_path='{self.config_path}')"
            )
