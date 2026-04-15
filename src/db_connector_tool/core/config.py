"""配置管理模块 (ConfigManager)

使用 TOML 格式进行配置管理，提供数据库连接配置的加密存储和管理功能。

Example:
>>> from db_connector_tool.core.config import ConfigManager
>>> config_manager = ConfigManager("my_app", "database.toml")
>>> with ConfigManager("my_app") as cm:
...     cm.add_config("test", {"host": "localhost", "port": 5432})
...     config = cm.get_config("test")
>>> connections = config_manager.list_configs()
>>> backup_path = config_manager.backup_config()
>>> new_version = config_manager.rotate_encryption_key()
"""

import getpass
import shutil
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import tomli_w

from ..utils.logging_utils import get_logger
from ..utils.path_utils import PathHelper
from .config_security import ConfigSecurityManager
from .exceptions import ConfigError
from .key_manager import KeyManager
from .validators import ConfigValidator

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class ConfigManager:
    """配置管理器类 (Config Manager)

    管理数据库连接配置的加密存储，使用TOML格式配置文件，
    提供连接配置的增删改查功能，所有敏感信息都会自动加密，
    支持上下文管理器协议，可使用 `with` 语句自动管理敏感数据的清理。

    Example:
    >>> config_manager = ConfigManager("my_app", "database.toml")
    >>> with ConfigManager("my_app") as cm:
    ...     cm.add_config("test", {"host": "localhost", "port": 5432})
    ...     config = cm.get_config("test")
    >>> config_manager = ConfigManager("my_app")
    >>> try:
    ...     config_manager.add_config("test", {"host": "localhost", "port": 5432})
    ... finally:
    ...     config_manager.close()
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

        创建新的配置管理器实例，自动初始化加密系统和配置文件。

        Args:
            app_name: 应用名称，用于确定配置目录和密钥存储
            config_file: 配置文件名，默认为"connections.toml"

        Raises:
            ConfigError: 配置文件初始化失败
            OSError: 文件系统操作失败
            ValueError: 参数验证失败

        Example:
            >>> config_manager = ConfigManager("my_app", "database.toml")
            >>> config_manager = ConfigManager()  # 使用默认参数
            >>> with ConfigManager("my_app") as cm:
            ...     cm.add_config("test", {"host": "localhost", "port": 5432})
        """

        try:
            self.app_name = app_name
            self.config_file = config_file
            self.key_manager = KeyManager(app_name)
            self.security_manager = ConfigSecurityManager(self.key_manager)
            self._config_cache: Optional[Dict[str, Any]] = None
            self._config_mtime: Optional[float] = None

            # 确保配置文件存在
            self._ensure_config_exists()

            logger.info(
                "配置管理器初始化成功: 应用=%s, 配置文件=%s",
                app_name,
                self.config_path,
            )

        except Exception as error:
            logger.error("初始化配置管理器失败: %s", str(error))
            raise ConfigError(f"配置管理器初始化失败: {str(error)}") from error

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
        """

        self._clear_sensitive_data()
        logger.info("配置管理器上下文已退出")

    @property
    def config_dir(self) -> Path:
        """获取配置目录路径

        Returns:
            Path: 配置目录路径
        """
        return PathHelper.get_user_config_dir(self.app_name)

    @property
    def config_path(self) -> Path:
        """获取配置文件路径

        Returns:
            Path: 配置文件路径
        """
        return self.config_dir / self.config_file

    def close(self) -> None:
        """关闭配置管理器，清理敏感数据

        Example:
            >>> config_manager = ConfigManager("my_app")
            >>> # 使用配置管理器...
            >>> config_manager.close()  # 手动清理敏感数据
        """
        self._clear_sensitive_data()
        logger.info("配置管理器已手动关闭")

    def _clear_sensitive_data(self) -> None:
        """清理内存中的敏感数据（内部方法）"""

        # 清理密钥管理器中的敏感数据
        self.key_manager.close()
        # 清理配置缓存
        self._config_cache = None
        self._config_mtime = None
        logger.debug("配置管理器敏感数据和缓存已清理")

    def _ensure_config_exists(self) -> None:
        """确保配置文件存在，如果不存在则创建默认配置

        Raises:
            ConfigError: 配置文件创建或初始化失败
        """

        self.key_manager.load_or_create_key()
        if not self.config_path.exists():
            self._create_default_config()

    def _create_default_config(self) -> None:
        """创建默认配置文件结构

        Raises:
            ConfigError: 默认配置创建失败
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
        # 设置安全的文件权限
        self._set_secure_file_permissions()
        logger.info("默认配置文件已创建: %s", self.config_path)

    @KeyManager.handle_config_operation("配置文件保存")
    def _save_config(
        self, config: Dict[str, Any], operation: str = OPERATION_UPDATE
    ) -> None:
        """保存配置文件

        Args:
            config: 要保存的配置字典
            operation: 操作类型（update, add, remove, rotate_key）

        Raises:
            ConfigError: 配置文件保存失败
            ValueError: 操作类型无效
            OSError: 文件系统操作失败
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

        # 生成数字签名
        hmac_signature = self.security_manager.generate_config_signature(config)

        # 存储签名和签名时间戳
        config["metadata"]["signature"] = hmac_signature
        config["metadata"]["signature_timestamp"] = current_time

        # 添加审计日志
        self.security_manager.add_audit_log_entry(config, operation, current_time)

        # 验证配置结构
        ConfigValidator.validate_config(config)

        with open(self.config_path, "wb") as f:
            f.write(tomli_w.dumps(config).encode("utf-8"))

        # 清除缓存，确保下次加载时重新读取文件
        self._config_cache = None
        self._config_mtime = None
        logger.debug("配置文件已保存: %s", self.config_path)

    def add_config(self, name: str, connection_config: Dict[str, Any]) -> None:
        """添加数据库连接配置

        Args:
            name: 连接名称（唯一标识符），不能为空且必须是字符串
            connection_config: 连接配置字典，包含数据库连接所需的参数

        Raises:
            ConfigError: 连接已存在或添加失败
            ValueError: 连接名称为空或配置无效
            OSError: 文件系统操作失败

        Example:
            >>> config = {
            ...     "host": "localhost",
            ...     "port": 5432,
            ...     "username": "admin",
            ...     "password": "secret"
            ... }
            >>> config_manager.add_config("postgres_db", config)
            >>> with ConfigManager("my_app") as cm:
            ...     cm.add_config("mysql_db", {"host": "localhost", "port": 3306})
        """

        ConfigValidator.validate_connection_name(name)
        ConfigValidator.validate_connection_config(connection_config)

        config = self._load_config()

        # 检查连接是否已存在
        if name in config["connections"]:
            raise ConfigError(f"连接配置已存在: {name}")

        # 确保加密管理器已初始化
        if self.key_manager.crypto is None:
            self.key_manager.load_or_create_key()
        # 使用统一的加密方法
        encrypted_config = self.security_manager.encrypt_dict_values(
            connection_config
        )
        config["connections"][name] = encrypted_config

        # 更新配置文件版本号（每次调用增加修订号）
        self._increment_config_version(config)

        self._save_config(config, self.OPERATION_ADD)
        self._log_operation_success("添加", name)

    @KeyManager.handle_config_operation("配置文件加载")
    def _load_config(self) -> Dict[str, Any]:
        """加载并验证配置文件

        Returns:
            Dict[str, Any]: 配置字典

        Raises:
            ConfigError: 配置文件格式无效或版本不支持
            OSError: 文件系统操作失败
            ValueError: 配置数据无效
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
        ConfigValidator.validate_config(config)

        # 验证数字签名
        self.security_manager.verify_config_signature(config)

        # 更新缓存和修改时间
        self._config_cache = config
        self._config_mtime = current_mtime
        logger.debug("配置已加载并缓存")

        return config

    def _increment_config_version(self, config: Dict[str, Any]) -> None:
        """递增配置文件版本号

        Args:
            config: 配置字典，包含版本号字段

        Raises:
            ConfigError: 版本号递增导致主版本号发生不合理变化
            ValueError: 版本号格式无效
        """

        try:
            current_version = config["version"]

            # 确保当前版本号格式有效
            if not ConfigValidator.is_valid_version_format(current_version):
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

            # 检查主版本号是否合理（限制主版本号不超过9）
            if major_num > 9:
                raise ConfigError(
                    "版本号递增导致主版本号发生不合理变化",
                    details={
                        "current_version": current_version,
                        "would_become": f"{major_num}.{minor_num}.{patch_num}",
                        "max_major_version": 9,
                    },
                )

            new_version = f"{major_num}.{minor_num}.{patch_num}"

            # 验证新版本号格式
            if not ConfigValidator.is_valid_version_format(new_version):
                raise ConfigError(
                    "版本号格式无效",
                    details={
                        "current_version": current_version,
                        "new_version": new_version,
                    },
                )

            config["version"] = new_version
            logger.debug("配置文件版本号已更新: %s -> %s", current_version, new_version)

        except ConfigError:
            # 重新抛出ConfigError，因为这是需要用户处理的错误
            raise
        except (ValueError, AttributeError, RuntimeError) as e:
            logger.warning("版本号递增失败，保持原版本号: %s", str(e))
            # 如果版本号递增失败，不影响主要功能，继续使用原版本号

    def _parse_version_parts(self, version: str) -> tuple[int, int, int]:
        """解析版本号各部分

        Args:
            version: 版本号字符串，格式为 "x.y.z"

        Returns:
            tuple[int, int, int]: (主版本号, 次版本号, 修订号)

        Raises:
            ValueError: 版本号格式无效

        Example:
            >>> self._parse_version_parts("1.2.3")
            (1, 2, 3)
        """

        version_parts = version.split(".")
        return int(version_parts[0]), int(version_parts[1]), int(version_parts[2])

    def _increment_version_parts(
        self, major: int, minor: int, patch: int
    ) -> tuple[int, int, int]:
        """递增版本号各部分并处理进位逻辑

        Args:
            major: 主版本号
            minor: 次版本号
            patch: 修订号

        Returns:
            tuple[int, int, int]: 递增后的(主版本号, 次版本号, 修订号)

        Example:
            >>> self._increment_version_parts(1, 2, 3)
            (1, 2, 4)
            >>> self._increment_version_parts(1, 2, 9)
            (1, 3, 0)
            >>> self._increment_version_parts(1, 9, 9)
            (2, 0, 0)
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
        """记录操作成功日志

        Args:
            operation: 操作类型（添加、删除、更新等）
            name: 连接名称
        """

        logger.info("连接配置已%s: %s", operation, name)

    def remove_config(self, name: str) -> None:
        """删除连接配置

        Args:
            name: 连接名称

        Raises:
            ConfigError: 连接不存在或删除失败
            ValueError: 连接名称无效

        Example:
            >>> config_manager.remove_config("postgres_db")
        """

        ConfigValidator.validate_connection_name(name)

        config = self._load_config()
        self._ensure_connection_exists(config, name)

        del config["connections"][name]
        # 更新配置文件版本号（每次调用增加修订号）
        self._increment_config_version(config)

        self._save_config(config, self.OPERATION_REMOVE)
        self._log_operation_success("删除", name)

    def _ensure_connection_exists(self, config: Dict[str, Any], name: str) -> None:
        """确保连接配置存在

        Args:
            config: 配置字典
            name: 连接名称

        Raises:
            ConfigError: 连接不存在
        """

        if name not in config["connections"]:
            raise ConfigError(f"连接配置不存在: {name}")

    def update_config(self, name: str, connection_config: Dict[str, Any]) -> None:
        """更新连接配置

        Args:
            name: 连接名称
            connection_config: 新的连接配置字典，包含更新后的连接参数

        Raises:
            ConfigError: 连接不存在或更新失败
            ValueError: 连接名称或配置无效

        Example:
            >>> new_config = {"host": "new_host", "port": 5433}
            >>> config_manager.update_config("postgres_db", new_config)
        """

        ConfigValidator.validate_connection_name(name)
        ConfigValidator.validate_connection_config(connection_config)

        # 加载配置并直接更新
        config = self._load_config()
        self._ensure_connection_exists(config, name)

        # 确保加密管理器已初始化
        if self.key_manager.crypto is None:
            self.key_manager.load_or_create_key()
        # 使用统一的加密方法
        encrypted_config = self.security_manager.encrypt_dict_values(
            connection_config
        )
        config["connections"][name] = encrypted_config

        # 更新配置文件版本号（每次调用增加修订号）
        self._increment_config_version(config)

        self._save_config(config, self.OPERATION_UPDATE)
        self._log_operation_success("更新", name)

    def get_config(self, name: str) -> Dict[str, Any]:
        """获取数据库连接配置（自动解密）

        Args:
            name: 连接名称，不能为空且必须是字符串

        Returns:
            Dict[str, Any]: 解密后的连接配置字典，包含原始连接参数

        Raises:
            ConfigError: 连接不存在或获取失败
            ValueError: 连接名称无效
            OSError: 文件系统操作失败

        Example:
            >>> config = config_manager.get_config("postgres_db")
            >>> print(config["host"])
            "localhost"
            >>> with ConfigManager("my_app") as cm:
            ...     config = cm.get_config("mysql_db")
        """

        ConfigValidator.validate_connection_name(name)

        config = self._load_config()
        self._ensure_connection_exists(config, name)

        connection_config = config["connections"][name].copy()

        # 确保加密管理器已初始化
        if self.key_manager.crypto is None:
            self.key_manager.load_or_create_key()

        # 使用统一的解密方法
        decrypted_config = self.security_manager.decrypt_dict_values(connection_config)
        logger.debug("连接配置已获取: %s", name)
        return decrypted_config

    def list_configs(self) -> List[str]:
        """列出所有可用的连接名称

        Returns:
            List[str]: 连接名称列表，按配置文件中的顺序排列

        Raises:
            ConfigError: 列出连接失败

        Example:
            >>> connections = config_manager.list_configs()
            >>> print(connections)
            ["postgres_db", "mysql_db"]
        """

        config = self._load_config()
        return list(config["connections"].keys())

    def get_config_info(self) -> Dict[str, Any]:
        """获取配置文件的基本信息

        Returns:
            Dict[str, Any]: 配置文件信息字典，包含版本、应用名称、连接数量等信息

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

        Returns:
            str: 当前密钥版本号

        Example:
            >>> version = config_manager.get_key_version()
            >>> print(version)
            "1"
        """

        config = self._load_config()
        return config.get("metadata", {}).get("key_version", "1")

    def get_audit_log(self) -> List[Dict[str, Any]]:
        """获取配置变更审计日志

        Returns:
            List[Dict[str, Any]]: 审计日志列表

        Example:
            >>> audit_log = config_manager.get_audit_log()
            >>> print(audit_log[0]["operation"])
            "add"
        """

        config = self._load_config()
        return config.get("metadata", {}).get("audit_log", [])

    @KeyManager.handle_config_operation("配置文件备份")
    def _set_secure_file_permissions(self) -> None:
        """设置配置文件的安全权限

        设置配置文件的安全权限，确保只有所有者可以访问。
        """
        import stat
        import platform
        
        try:
            system = platform.system().lower()
            
            if system == "windows":
                # Windows系统权限设置
                import subprocess
                username = getpass.getuser()
                result = subprocess.run(
                    [
                        "icacls",
                        str(self.config_path),
                        "/inheritance:r",
                        "/grant:r",
                        f"{username}:(R,W)",
                        "/remove",
                        "*S-1-1-0",
                    ],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                if result.returncode != 0:
                    logger.warning("Windows权限设置警告: %s", result.stderr)
            else:
                # Unix/Linux系统权限设置
                self.config_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            logger.debug("配置文件权限已设置: %s", self.config_path)
        except Exception as e:
            logger.warning("设置配置文件权限失败: %s", str(e))

    def backup_config(self, backup_path: Path | None = None) -> Path:
        """备份配置文件

        Args:
            backup_path: 备份文件路径，如果为None则自动生成带时间戳的备份文件

        Returns:
            Path: 备份文件路径

        Raises:
            ConfigError: 备份失败

        Example:
            >>> backup_path = config_manager.backup_config()
            >>> print(backup_path)
            "path/to/connections.toml.backup.20231201_120000"
            >>> custom_backup = Path("/path/to/backup.toml")
            >>> backup_path = config_manager.backup_config(custom_backup)
        """

        if backup_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = self.config_dir / f"{self.config_file}.backup.{timestamp}"

        shutil.copy2(self.config_path, backup_path)
        # 设置备份文件的安全权限
        try:
            import stat
            backup_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        except Exception as e:
            logger.warning("设置备份文件权限失败: %s", str(e))
        logger.debug("配置文件已备份: %s", backup_path)
        return backup_path

    @KeyManager.handle_config_operation("加密密钥轮换")
    def rotate_encryption_key(self) -> str:
        """轮换加密密钥

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 密钥轮换失败
            OSError: 文件系统操作失败
            ValueError: 配置无效

        Example:
            >>> new_version = config_manager.rotate_encryption_key()
            >>> print(new_version)
            "2"
            >>> with ConfigManager("my_app") as cm:
            ...     new_version = cm.rotate_encryption_key()
            ...     print(f"新密钥版本: {new_version}")
        """

        # 执行密钥轮换的主要步骤
        new_key_version = self._perform_key_rotation()
        return new_key_version

    def _perform_key_rotation(self) -> str:
        """执行密钥轮换的核心逻辑

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 密钥轮换失败
        """

        # 备份当前配置
        backup_path = self.backup_config()
        logger.debug("密钥轮换前已备份配置: %s", backup_path)

        # 加载当前配置
        config = self._load_config()

        # 执行密钥轮换
        new_key_version = self.security_manager.perform_key_rotation(config)

        # 更新配置文件版本号（每次调用增加修订号）
        self._increment_config_version(config)

        # 保存更新后的配置
        self._save_config(config, "rotate_key")

        return new_key_version
