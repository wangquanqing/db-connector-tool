"""
配置管理模块
使用 TOML 格式进行配置管理
"""

import tomllib
from datetime import datetime

import tomli_w

from ..utils.logger import get_logger
from ..utils.path_helper import PathHelper
from .crypto_manager import CryptoManager
from .exceptions import ConfigError

logger = get_logger(__name__)

# 配置文件创建时的初始时间戳（固定值）
CONFIG_CREATION_TIMESTAMP = datetime.now().astimezone().isoformat()


class ConfigManager:
    """配置管理器"""

    def __init__(
        self, app_name: str = "db_connector", config_file: str = "connections.toml"
    ):
        """
        初始化配置管理器

        Args:
            app_name: 应用名称
            config_file: 配置文件名称
        """
        self.app_name = app_name
        self.config_file = config_file
        self.config_dir = PathHelper.get_user_config_dir(app_name)
        self.config_path = self.config_dir / config_file
        self.crypto = None
        self._ensure_config_exists()

    def _ensure_config_exists(self):
        """确保配置文件存在"""
        try:
            if not self.config_path.exists():
                self._create_default_config()
            self._load_or_create_crypto_key()
        except Exception as e:
            logger.error(f"初始化配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件初始化失败: {str(e)}")

    def _create_default_config(self):
        """创建默认配置文件"""
        default_config = {
            "version": "1.0.0",
            "app_name": self.app_name,
            "connections": {},
            "metadata": {
                "created": CONFIG_CREATION_TIMESTAMP,
                "last_modified": CONFIG_CREATION_TIMESTAMP,
            },
        }
        self._save_config(default_config)
        logger.info(f"创建默认配置文件: {self.config_path}")

    def _load_or_create_crypto_key(self):
        """加载或创建加密密钥"""
        key_file = self.config_dir / "encryption.key"

        if key_file.exists():
            # 加载现有密钥
            try:
                with open(key_file, "r", encoding="utf-8") as f:
                    key_data = tomllib.loads(f.read())
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

    def _load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_path, "rb") as f:
                return tomllib.load(f)
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件加载失败: {str(e)}")

    def _save_config(self, config: dict):
        """保存配置文件"""
        try:
            # 更新最后修改时间
            current_time = datetime.now().astimezone().isoformat()
            config["metadata"]["last_modified"] = current_time

            with open(self.config_path, "wb") as f:
                f.write(tomli_w.dumps(config).encode("utf-8"))
        except Exception as e:
            logger.error(f"保存配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件保存失败: {str(e)}")

    def add_connection(self, name: str, connection_config: dict):
        """
        添加数据库连接配置

        Args:
            name: 连接名称
            connection_config: 连接配置字典
        """
        try:
            config = self._load_config()

            # 确保 crypto 已初始化
            if self.crypto is None:
                raise ConfigError("加密管理器未初始化，无法加密敏感信息")

            # 加密敏感信息
            encrypted_config = connection_config.copy()
            sensitive_fields = ["password", "passwd", "pwd"]

            for field in sensitive_fields:
                if field in encrypted_config:
                    encrypted_config[field] = self.crypto.encrypt(
                        encrypted_config[field]
                    )

            config["connections"][name] = encrypted_config
            self._save_config(config)
            logger.info(f"连接配置已添加: {name}")

        except Exception as e:
            logger.error(f"添加连接配置失败 {name}: {str(e)}")
            raise ConfigError(f"添加连接配置失败: {str(e)}")

    def get_connection(self, name: str) -> dict:
        """
        获取数据库连接配置（自动解密）

        Args:
            name: 连接名称

        Returns:
            连接配置字典
        """
        try:
            config = self._load_config()

            if name not in config["connections"]:
                raise ConfigError(f"连接配置不存在: {name}")

            connection_config = config["connections"][name].copy()

            # 确保 crypto 已初始化
            if self.crypto is None:
                raise ConfigError("加密管理器未初始化，无法加密敏感信息")

            # 解密敏感信息
            sensitive_fields = ["password", "passwd", "pwd"]

            for field in sensitive_fields:
                if field in connection_config:
                    connection_config[field] = self.crypto.decrypt(
                        connection_config[field]
                    )

            logger.debug(f"连接配置已获取: {name}")
            return connection_config

        except Exception as e:
            logger.error(f"获取连接配置失败 {name}: {str(e)}")
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"获取连接配置失败: {str(e)}")

    def list_connections(self) -> list:
        """
        列出所有连接名称

        Returns:
            连接名称列表
        """
        try:
            config = self._load_config()
            return list(config["connections"].keys())
        except Exception as e:
            logger.error(f"列出连接失败: {str(e)}")
            raise ConfigError(f"列出连接失败: {str(e)}")

    def remove_connection(self, name: str):
        """
        删除连接配置

        Args:
            name: 连接名称
        """
        try:
            config = self._load_config()

            if name in config["connections"]:
                del config["connections"][name]
                self._save_config(config)
                logger.info(f"连接配置已删除: {name}")
            else:
                raise ConfigError(f"连接配置不存在: {name}")

        except Exception as e:
            logger.error(f"删除连接配置失败 {name}: {str(e)}")
            if isinstance(e, ConfigError):
                raise
            raise ConfigError(f"删除连接配置失败: {str(e)}")

    def update_connection(self, name: str, connection_config: dict):
        """
        更新连接配置

        Args:
            name: 连接名称
            connection_config: 新的连接配置
        """
        try:
            self.remove_connection(name)
            self.add_connection(name, connection_config)
            logger.info(f"连接配置已更新: {name}")
        except Exception as e:
            logger.error(f"更新连接配置失败 {name}: {str(e)}")
            raise ConfigError(f"更新连接配置失败: {str(e)}")
