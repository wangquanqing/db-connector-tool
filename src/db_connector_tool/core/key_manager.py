"""密钥管理模块 (KeyManager)

提供加密密钥的安全管理功能，包括密钥的创建、加载、存储和轮换，
支持多种密钥存储方式（操作系统密钥环、环境变量、文件），
确保加密密钥的安全性和可用性。

Example:
>>> from db_connector_tool.core.key_manager import KeyManager
>>>
>>> # 创建密钥管理器
>>> key_manager = KeyManager("my_app")
>>>
>>> # 加载或创建加密密钥
>>> key_manager.load_or_create_key()
>>>
>>> # 获取加密管理器实例
>>> crypto_manager = key_manager.get_crypto_manager()
>>>
>>> # 轮换密钥
>>> key_manager.rotate_key()
"""

import getpass
import hashlib
import json
import os
import platform
import secrets
import stat
import subprocess
import threading
import tomllib
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Optional

import tomli_w

from ..utils.logging_utils import get_logger
from ..utils.path_utils import PathHelper
from .crypto import CryptoManager
from .exceptions import ConfigError, CryptoError

# 条件导入keyring库
keyring_available = False  # pylint: disable=invalid-name
keyring_module = None  # 内部使用的模块引用  # pylint: disable=invalid-name
try:
    import keyring

    keyring_available = True  # pylint: disable=invalid-name
    keyring_module = keyring
except ImportError:
    pass

# 获取模块级别的日志记录器
logger = get_logger(__name__)

# 模块级别的锁，用于保护类级别锁的初始化
_module_lock = threading.Lock()


class KeyManager:
    """密钥管理器类 (Key Manager)

    管理加密密钥的安全存储和操作，支持多种密钥存储方式，
    提供统一的密钥管理接口，与 CryptoManager 无缝集成。

    Example:
    >>> # 创建密钥管理器
    >>> key_manager = KeyManager("my_app")
    >>>
    >>> # 加载或创建密钥
    >>> key_manager.load_or_create_key()
    >>>
    >>> # 获取加密管理器
    >>> crypto = key_manager.get_crypto_manager()
    >>>
    >>> # 轮换密钥
    >>> key_manager.rotate_key()
    >>>
    >>> # 获取 HMAC 密钥
    >>> hmac_key = key_manager.get_secure_hmac_key()
    >>>
    >>> # 关闭密钥管理器（清理敏感数据）
    >>> key_manager.close()
    """

    _env_key = None
    # 类级别的依赖检查结果（全局依赖，与应用名无关）
    _env_key_available = None
    _dependencies_checked = False
    # 线程安全锁，确保依赖检查只执行一次
    _dependency_check_lock = None

    def __init__(self, app_name: str = "db_connector_tool") -> None:
        """初始化密钥管理器

        创建新的密钥管理器实例，用于管理加密密钥的安全存储和操作。

        Args:
            app_name: 应用名称，用于确定配置目录和密钥存储标识

        Raises:
            ConfigError: 初始化失败

        Example:
            >>> # 使用默认应用名称
            >>> key_manager = KeyManager()

            >>> # 使用自定义应用名称
            >>> key_manager = KeyManager("my_application")
        """

        self.app_name = app_name
        self.config_dir = PathHelper.get_user_config_dir(app_name)
        self.crypto: Optional[CryptoManager] = None

        # 检查依赖可用性（类级别，只执行一次，线程安全）
        if not KeyManager._dependencies_checked:
            # 使用双重检查锁定模式确保线程安全
            if KeyManager._dependency_check_lock is None:
                # 使用模块级别的锁来保护类级别锁的初始化
                with _module_lock:
                    if KeyManager._dependency_check_lock is None:
                        KeyManager._dependency_check_lock = threading.RLock()

            # 确保锁已经正确初始化
            if KeyManager._dependency_check_lock is not None:
                with KeyManager._dependency_check_lock:
                    # 再次检查，防止竞态条件
                    if not KeyManager._dependencies_checked:
                        KeyManager._check_dependencies()

    def close(self) -> None:
        """关闭密钥管理器，清理敏感数据

        清理内存中的敏感数据，确保密钥信息不会被泄露。

        Example:
            >>> key_manager = KeyManager()
            >>> # 使用密钥管理器...
            >>> key_manager.close()  # 手动清理敏感数据
        """

        if self.crypto is not None:
            self.crypto.close()
            self.crypto = None
        logger.debug("密钥管理器已关闭")

    @staticmethod
    def handle_config_operation(operation_name: str) -> Callable:
        """配置操作异常处理装饰器

        捕获并处理配置操作中的异常，提供统一的错误处理和日志记录。

        Args:
            operation_name: 操作名称，用于错误消息和日志记录

        Returns:
            Callable: 装饰器函数

        Example:
            >>> @handle_config_operation("配置文件保存")
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

    def get_crypto_manager(self) -> CryptoManager:
        """获取加密管理器实例

        获取初始化好的加密管理器实例，用于执行加密和解密操作。

        Returns:
            CryptoManager: 初始化好的加密管理器实例

        Raises:
            ConfigError: 加密管理器未初始化，无法处理敏感信息

        Example:
            >>> key_manager = KeyManager()
            >>> key_manager.load_or_create_key()
            >>> crypto = key_manager.get_crypto_manager()
            >>> encrypted = crypto.encrypt("敏感数据")
        """

        if self.crypto is None:
            raise ConfigError("加密管理器未初始化，无法处理敏感信息")
        return self.crypto

    @handle_config_operation("安全密钥管理")
    def load_or_create_key(self) -> None:
        """加载或创建加密密钥

        加载现有的加密密钥，如果不存在则创建新的密钥，
        按照安全层次结构选择密钥存储方式。

        Raises:
            ConfigError: 密钥加载或创建失败

        Example:
            >>> key_manager = KeyManager()
            >>> key_manager.load_or_create_key()
            >>> # 密钥已加载或创建成功
        """

        # 尝试使用keyring库（如果可用）
        if keyring_available and keyring_module is not None:
            self._load_or_create_key_from_keyring()
        elif KeyManager._env_key_available and KeyManager._env_key:
                # 环境变量密钥可用，使用环境变量
                try:
                    key_data = json.loads(KeyManager._env_key)
                    self._load_crypto_from_key_data(key_data)
                    logger.debug("使用环境变量中的加密密钥")
                except (json.JSONDecodeError, TypeError) as e:
                    logger.error("环境变量密钥格式错误: %s，请检查 DB_CONNECTOR_TOOL_ENCRYPTION_KEY 环境变量的格式", str(e))
                    logger.warning("环境变量密钥加载失败，使用文件存储方案")
                    self._load_or_create_key_from_file()
                except ConfigError as e:
                    logger.error("环境变量密钥数据无效: %s，请检查 DB_CONNECTOR_TOOL_ENCRYPTION_KEY 环境变量的内容", str(e))
                    logger.warning("环境变量密钥加载失败，使用文件存储方案")
                    self._load_or_create_key_from_file()
        else:
            # 回退到文件权限方案
            logger.warning("keyring库和环境变量都不可用，使用文件权限保护方案")
            self._load_or_create_key_from_file()

    def _load_or_create_key_from_keyring(self) -> None:
        """从操作系统密钥环加载或创建密钥

        从操作系统的密钥环中加载现有的加密密钥，
        如果不存在则创建新的密钥并存储到密钥环。

        Raises:
            ConfigError: 密钥操作失败

        Example:
            >>> key_manager = KeyManager()
            >>> key_manager._load_or_create_key_from_keyring()
        """

        assert keyring_module is not None, "keyring库可用"

        service_name = self.app_name
        username = "master_key"

        # 尝试从密钥环获取密钥
        stored_key = keyring_module.get_password(service_name, username)

        if stored_key:
            # 使用统一的密钥加载方法
            self._load_crypto_from_key_data(json.loads(stored_key))
            logger.debug("从操作系统密钥存储加载密钥成功")
        else:
            # 创建新密钥并存储
            key_data = self._create_new_crypto_key()
            keyring_module.set_password(service_name, username, json.dumps(key_data))
            logger.info("新加密密钥已安全存储到操作系统密钥环")

    def _load_crypto_from_key_data(self, key_data: Dict[str, Any]) -> None:
        """从密钥数据加载加密管理器

        从包含密码和盐值的密钥数据中加载加密管理器。

        Args:
            key_data: 包含password和salt的密钥数据字典

        Raises:
            ConfigError: 密钥数据无效

        Example:
            >>> key_data = {"password": "secure_password", "salt": "random_salt"}
            >>> key_manager._load_crypto_from_key_data(key_data)
        """

        # 验证密钥数据格式
        if "password" not in key_data or "salt" not in key_data:
            raise ConfigError("密钥数据格式无效")

        # 加载加密管理器
        self.crypto = CryptoManager.from_saved_key(
            key_data["password"], key_data["salt"]
        )
        logger.debug("加密密钥加载成功")

    def _create_new_crypto_key(self) -> Dict[str, str]:
        """创建新的加密密钥

        创建新的加密密钥并初始化加密管理器。

        Returns:
            Dict[str, str]: 包含password和salt的密钥数据

        Example:
            >>> key_data = key_manager._create_new_crypto_key()
            >>> print("password" in key_data)
            True
            >>> print("salt" in key_data)
            True
        """

        self.crypto = CryptoManager()
        key_data = self.crypto.get_key_info()
        logger.debug("新加密密钥创建成功")
        return key_data

    def _load_or_create_key_from_file(self) -> None:
        """从文件加载或创建密钥（回退方案）

        从文件中加载现有的加密密钥，
        如果不存在则创建新的密钥文件。

        Raises:
            ConfigError: 密钥加载或创建失败

        Example:
            >>> key_manager = KeyManager()
            >>> key_manager._load_or_create_key_from_file()
        """

        if KeyManager._env_key:
            # 使用环境变量中的密钥
            self._load_crypto_from_key_data(json.loads(KeyManager._env_key))
            logger.debug("使用环境变量中的加密密钥")
        else:
            key_file_path = self.config_dir / "encryption.key"
            if key_file_path.exists():
                self._load_existing_key(key_file_path)
                logger.warning(
                    "警告: 使用文件存储加密密钥（安全性较低）。\n"
                    "安全风险: 文件存储的密钥可能被本地攻击者获取，导致加密数据被解密。\n"
                    "建议: 1. 安装keyring库 (pip install keyring)，使用操作系统密钥环存储\n"
                    "      2. 或设置环境变量 DB_CONNECTOR_TOOL_ENCRYPTION_KEY，使用环境变量存储\n"
                    "      3. 确保密钥文件权限设置正确，仅允许所有者访问"
                )
            else:
                self._create_new_key(key_file_path)
                logger.warning(
                    "警告: 使用文件存储加密密钥（安全性较低）。\n"
                    "安全风险: 文件存储的密钥可能被本地攻击者获取，导致加密数据被解密。\n"
                    "建议: 1. 安装keyring库 (pip install keyring)，使用操作系统密钥环存储\n"
                    "      2. 或设置环境变量 DB_CONNECTOR_TOOL_ENCRYPTION_KEY，使用环境变量存储\n"
                    "      3. 确保密钥文件权限设置正确，仅允许所有者访问"
                )

    def _load_existing_key(self, key_file_path: Path) -> None:
        """加载现有的加密密钥文件

        加载现有的加密密钥文件，并设置安全的文件权限。

        Args:
            key_file_path: 密钥文件路径

        Raises:
            ConfigError: 密钥加载失败

        Example:
            >>> key_file = Path("/path/to/encryption.key")
            >>> key_manager._load_existing_key(key_file)
        """

        try:
            # 设置文件权限为仅所有者可读写
            self._set_secure_file_permissions(key_file_path)

            with open(key_file_path, "rb") as f:
                key_data = tomllib.load(f)

            # 使用统一的密钥加载方法
            self._load_crypto_from_key_data(key_data)
        except CryptoError as error:
            # 解密失败，可能是因为密钥生成逻辑改变，删除旧密钥文件并创建新的
            self._handle_crypto_error(key_file_path, error)

    def _create_new_key(self, key_file_path: Path) -> None:
        """创建新的加密密钥文件

        创建新的加密密钥文件，并设置安全的文件权限。

        Args:
            key_file_path: 密钥文件路径

        Example:
            >>> key_file = Path("/path/to/encryption.key")
            >>> key_manager._create_new_key(key_file)
        """

        key_data = self._create_new_crypto_key()

        # 先写入文件，然后设置安全权限
        with open(key_file_path, "wb") as f:
            f.write(tomli_w.dumps(key_data).encode("utf-8"))

        # 设置文件权限为仅所有者可读写
        self._set_secure_file_permissions(key_file_path)

        logger.info("新加密密钥文件创建成功")

    def _set_secure_file_permissions(self, file_path: Path) -> None:
        """设置文件安全权限（最小权限原则）

        设置文件的安全权限，确保只有所有者可以访问。

        Args:
            file_path: 文件路径

        Raises:
            ConfigError: 权限设置失败

        Example:
            >>> key_file = Path("/path/to/encryption.key")
            >>> key_manager._set_secure_file_permissions(key_file)
        """

        try:
            system = platform.system().lower()

            if system == "windows":
                self._set_windows_permissions(file_path)
            else:
                self._set_unix_permissions(file_path)

            logger.debug("设置文件权限成功: %s", file_path)

        except (OSError, AttributeError) as e:
            logger.warning("设置文件权限失败 %s: %s", file_path, str(e))
            # 权限设置失败不应阻止程序运行，但记录警告

    def _set_windows_permissions(self, file_path: Path) -> None:
        """设置Windows文件权限（最小权限原则）

        设置Windows系统下的文件权限，确保只有当前用户可以访问。

        Args:
            file_path: 密钥文件路径

        Example:
            >>> key_file = Path("C:\\path\\to\\encryption.key")
            >>> key_manager._set_windows_permissions(key_file)
        """

        username = getpass.getuser()

        # 使用icacls设置权限：
        # /inheritance:r - 移除继承权限
        # /grant:r - 授予当前用户读写权限（最小必要权限）
        # /remove - 移除其他用户权限
        result = subprocess.run(
            [
                "icacls",
                str(file_path),
                "/inheritance:r",
                "/grant:r",
                f"{username}:(R,W)",  # 仅读写权限，非完全控制
                "/remove",
                "*S-1-1-0",  # 移除Everyone组
            ],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            logger.warning("icacls设置权限警告: %s", result.stderr)
        else:
            logger.debug("Windows: 已设置密钥文件权限为仅当前用户读写")

    def _set_unix_permissions(self, file_path: Path) -> None:
        """设置Unix/Linux文件权限（最小权限原则）

        设置Unix/Linux系统下的文件权限，确保只有所有者可以访问。

        Args:
            file_path: 密钥文件路径

        Example:
            >>> key_file = Path("/path/to/encryption.key")
            >>> key_manager._set_unix_permissions(key_file)
        """

        # 设置权限为600：仅所有者可读写
        file_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
        logger.debug("Unix/Linux: 已设置密钥文件权限为600（仅所有者可读写）")

    def _handle_crypto_error(
        self, key_file_path: Path, crypto_error: CryptoError
    ) -> None:
        """处理加密错误：删除旧密钥并创建新的

        处理加密错误，删除旧的密钥文件并创建新的密钥。

        Args:
            key_file_path: 密钥文件路径
            crypto_error: 加密错误异常

        Raises:
            ConfigError: 删除旧密钥文件失败

        Example:
            >>> key_file = Path("/path/to/encryption.key")
            >>> crypto_error = CryptoError("解密失败")
            >>> key_manager._handle_crypto_error(key_file, crypto_error)
        """

        logger.warning("解密密钥文件失败: %s，将创建新的密钥文件", str(crypto_error))
        try:
            key_file_path.unlink()
            logger.info("已删除旧的密钥文件")
            # 直接创建新的密钥，避免递归调用
            self._create_new_key(key_file_path)
        except Exception as delete_error:
            logger.error("删除旧密钥文件失败: %s", str(delete_error))
            raise ConfigError(
                f"加密密钥加载失败: {str(crypto_error)}"
            ) from crypto_error

    def get_secure_hmac_key(self) -> bytes:
        """获取安全的HMAC密钥（从主加密密钥派生）

        获取用于生成HMAC签名的安全密钥，优先从主加密密钥派生。

        Returns:
            bytes: 安全的HMAC密钥

        Example:
            >>> key_manager = KeyManager()
            >>> key_manager.load_or_create_key()
            >>> hmac_key = key_manager.get_secure_hmac_key()
            >>> print(len(hmac_key))
            32
        """

        # 优先从主加密密钥派生HMAC密钥
        if self.crypto is not None:
            key_info = self.crypto.get_key_info()
            # 使用主密钥的password和salt组合派生HMAC密钥
            hmac_key_input = f"{key_info['password']}:{key_info['salt']}:hmac".encode(
                "utf-8"
            )
            return hashlib.sha256(hmac_key_input).digest()

        # 其次使用环境变量
        hmac_env_key = os.environ.get("DB_CONNECTOR_TOOL_HMAC_KEY")
        if hmac_env_key:
            logger.debug("使用环境变量中的HMAC密钥")
            return bytes.fromhex(hmac_env_key)

        # 最后生成临时HMAC密钥（仅用于当前会话）
        logger.warning("主加密密钥未初始化，使用临时生成的HMAC密钥")
        return secrets.token_bytes(32)

    def rotate_key(self) -> Dict[str, str]:
        """轮换加密密钥

        生成新的加密密钥并按照安全层次结构保存。

        Returns:
            Dict[str, str]: 新的密钥数据

        Raises:
            ConfigError: 密钥轮换失败

        Example:
            >>> key_manager = KeyManager()
            >>> key_manager.load_or_create_key()
            >>> new_key = key_manager.rotate_key()
            >>> print("password" in new_key)
            True
        """

        # 生成新的加密密钥
        key_data = self._create_new_crypto_key()

        # 按照安全层次结构保存新密钥
        self._save_new_key_secure(key_data)

        return key_data

    def _save_new_key_secure(self, key_data: Dict[str, str]) -> None:
        """按照安全层次结构保存新密钥：keyring > 环境变量 > 文件

        按照安全优先级保存新的加密密钥。

        Args:
            key_data: 包含新密钥信息的字典

        Example:
            >>> key_data = {"password": "new_password", "salt": "new_salt"}
            >>> key_manager._save_new_key_secure(key_data)
        """

        # 1. 优先尝试保存到操作系统密钥环
        if keyring_available and keyring_module is not None:
            service_name = self.app_name
            username = "master_key"

            keyring_module.set_password(service_name, username, json.dumps(key_data))
            logger.info("轮换加密密钥已安全存储到操作系统密钥环")
            return

        # 2. 如果keyring不可用，检查是否应该使用环境变量
        if KeyManager._env_key_available:
            # 环境变量方案需要用户手动设置，这里只记录建议
            logger.warning(
                "建议将新密钥设置为环境变量: %s",
                f"DB_CONNECTOR_TOOL_ENCRYPTION_KEY={json.dumps(key_data)}",
            )
            # 继续使用文件存储作为后备

        # 3. 最后回退到文件存储
        key_file = self.config_dir / "encryption.key"

        # 先写入文件，然后设置安全权限
        with open(key_file, "wb") as f:
            f.write(tomli_w.dumps(key_data).encode("utf-8"))

        # 设置文件权限为仅所有者可读写
        self._set_secure_file_permissions(key_file)

        logger.warning(
            "轮换密钥已保存到文件（安全性较低）。建议使用keyring或环境变量存储"
        )
        logger.debug("轮换密钥文件保存成功: %s", key_file)

    @classmethod
    def _check_dependencies(cls) -> None:
        """检查依赖项可用性（类方法，只执行一次）

        检查密钥存储相关的依赖项可用性，包括keyring库和环境变量。

        Example:
            >>> KeyManager._check_dependencies()
        """

        # 检查环境变量密钥
        cls._env_key = os.environ.get("DB_CONNECTOR_TOOL_ENCRYPTION_KEY")
        cls._env_key_available = False
        
        if cls._env_key:
            try:
                # 验证环境变量格式
                key_data = json.loads(cls._env_key)
                if "password" in key_data and "salt" in key_data:
                    cls._env_key_available = True
                    logger.debug("环境变量中的加密密钥可用")
                else:
                    logger.warning("环境变量中的加密密钥格式无效，缺少必要字段")
            except (json.JSONDecodeError, TypeError) as e:
                logger.warning("环境变量中的加密密钥格式无效: %s", str(e))
        else:
            logger.debug("环境变量中无加密密钥")

        # 检查是否有可用的密钥存储
        if not keyring_available and not cls._env_key_available:
            logger.warning(
                "警告: 未找到安全的密钥存储方案。\n"
                "建议: 1. 安装keyring库 (pip install keyring)\n"
                "      2. 或设置环境变量 DB_CONNECTOR_TOOL_ENCRYPTION_KEY"
            )

        # 标记依赖检查已完成
        cls._dependencies_checked = True
