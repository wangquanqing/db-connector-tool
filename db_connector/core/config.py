"""
配置管理模块

使用 TOML 格式进行配置管理，提供数据库连接配置的加密存储和管理功能。
支持配置文件的创建、加载、保存，以及连接配置的增删改查操作。
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

logger = get_logger(__name__)

# 配置文件创建时的初始时间戳（固定值）
CONFIG_CREATION_TIMESTAMP = datetime.now().astimezone().isoformat()

# 支持的配置版本
SUPPORTED_VERSIONS = ["1.0.0", "1.1.0"]

# 错误消息常量
ERROR_EMPTY_CONNECTION_NAME = "连接名称不能为空且必须是字符串"
ERROR_INVALID_CONFIG_DICT = "连接配置不能为空且必须是字典"


class ConfigManager:
    """
    配置管理器类

    管理数据库连接配置的加密存储，使用TOML格式配置文件。
    提供连接配置的增删改查功能，所有敏感信息都会自动加密。
    """

    def __init__(
        self, app_name: str = "db_connector", config_file: str = "connections.toml"
    ):
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

    def _serialize_value(self, value: Any) -> str:
        """
        序列化值以便加密，保留数据类型信息

        Args:
            value: 要序列化的任意类型值

        Returns:
            str: JSON格式的序列化字符串

        Example:
            >>> serialized = self._serialize_value("secret_password")
            >>> print(serialized)
            '{"type": "str", "value": "secret_password"}'
        """
        value_info = {"type": type(value).__name__, "value": value}
        return json.dumps(value_info, ensure_ascii=False)

    def _deserialize_value(self, json_str: str) -> Any:
        """
        反序列化值，恢复原始数据类型

        Args:
            json_str: JSON格式的序列化字符串

        Returns:
            Any: 反序列化后的原始值

        Raises:
            ValueError: 当JSON字符串格式无效时

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

    def _ensure_config_exists(self) -> None:
        """确保配置文件存在，如果不存在则创建默认配置"""
        try:
            if not self.config_path.exists():
                self._create_default_config()
            self._load_or_create_crypto_key()
            logger.debug(f"配置文件就绪: {self.config_path}")
        except Exception as e:
            logger.error(f"初始化配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件初始化失败: {str(e)}")

    def _create_default_config(self) -> None:
        """创建默认配置文件结构"""
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

    def _load_or_create_crypto_key(self) -> None:
        """加载或创建加密密钥"""
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
                logger.info("加密密钥加载成功")

            except Exception as e:
                logger.error(f"加载加密密钥失败: {str(e)}")
                raise ConfigError(f"加密密钥加载失败: {str(e)}")
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
                raise ConfigError(f"加密密钥创建失败: {str(e)}")

    def _load_config(self) -> Dict[str, Any]:
        """
        加载并验证配置文件

        Returns:
            Dict[str, Any]: 配置字典

        Raises:
            ConfigError: 当配置文件格式无效或版本不支持时
        """
        try:
            with open(self.config_path, "rb") as f:
                config = tomllib.load(f)

            # 验证配置文件结构
            self._validate_config(config)

            return config

        except tomllib.TOMLDecodeError as e:
            logger.error(f"配置文件TOML格式错误: {str(e)}")
            raise ConfigError(f"配置文件格式无效: {str(e)}")
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件加载失败: {str(e)}")

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """
        验证配置文件结构

        Args:
            config: 要验证的配置字典

        Raises:
            ConfigError: 当配置结构无效时
        """
        required_fields = ["version", "app_name", "connections", "metadata"]
        for field in required_fields:
            if field not in config:
                raise ConfigError(f"配置文件缺少必需字段: {field}")

        # 验证版本兼容性
        if config["version"] not in SUPPORTED_VERSIONS:
            raise ConfigError(f"不支持的配置版本: {config['version']}")

    def _save_config(self, config: Dict[str, Any]) -> None:
        """
        保存配置文件

        Args:
            config: 要保存的配置字典

        Raises:
            ConfigError: 当配置文件保存失败时
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
            raise ConfigError(f"配置文件保存失败: {str(e)}")

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        添加数据库连接配置

        Args:
            name: 连接名称（唯一标识符）
            connection_config: 连接配置字典

        Raises:
            ConfigError: 当连接已存在或添加失败时
            ValueError: 当连接名称为空或配置无效时

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
            self._save_config(config)
            logger.info(f"连接配置已添加: {name}")

        except Exception as e:
            logger.error(f"添加连接配置失败 {name}: {str(e)}")
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"添加连接配置失败: {str(e)}")

    def get_connection(self, name: str) -> Dict[str, Any]:
        """
        获取数据库连接配置（自动解密）

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 解密后的连接配置字典

        Raises:
            ConfigError: 当连接不存在或获取失败时

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
            raise ConfigError(f"获取连接配置失败: {str(e)}")

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
            raise ConfigError(f"列出连接失败: {str(e)}")

    def connection_exists(self, name: str) -> bool:
        """
        检查连接配置是否存在

        Args:
            name: 连接名称

        Returns:
            bool: 连接是否存在

        Example:
            >>> exists = config_manager.connection_exists("postgres_db")
            >>> print(exists)
            True
        """
        try:
            config = self._load_config()
            return name in config["connections"]
        except Exception:
            return False

    def remove_connection(self, name: str) -> None:
        """
        删除连接配置

        Args:
            name: 连接名称

        Raises:
            ConfigError: 当连接不存在或删除失败时

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
            raise ConfigError(f"删除连接配置失败: {str(e)}")

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        更新连接配置

        Args:
            name: 连接名称
            connection_config: 新的连接配置字典

        Raises:
            ConfigError: 当连接不存在或更新失败时

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
            raise ConfigError(f"更新连接配置失败: {str(e)}")

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
            raise ConfigError(f"获取配置信息失败: {str(e)}")

    def backup_config(self, backup_path: Optional[Path] = None) -> Path:
        """
        备份配置文件

        Args:
            backup_path: 备份文件路径，如果为None则自动生成

        Returns:
            Path: 备份文件路径

        Raises:
            ConfigError: 当备份失败时
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
            raise ConfigError(f"配置文件备份失败: {str(e)}")

    def __str__(self) -> str:
        """返回配置管理器的字符串表示"""
        return f"ConfigManager(app_name='{self.app_name}', config_file='{self.config_file}')"

    def __repr__(self) -> str:
        """返回配置管理器的详细表示"""
        return f"ConfigManager(app_name='{self.app_name}', config_file='{self.config_file}', config_path='{self.config_path}')"
