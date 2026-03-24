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
import platform
import shutil
import stat
import subprocess
import tomllib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import tomli_w

from ..utils.logging_utils import get_logger
from ..utils.path_utils import PathHelper
from .crypto import CryptoManager
from .exceptions import ConfigError, CryptoError

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

    # 类级别的依赖检查结果（全局依赖，与应用名无关）
    _keyring_available = None
    _env_key_available = None
    _dependencies_checked = False
    # 线程安全锁，确保依赖检查只执行一次
    _dependency_check_lock = None
    
    def __init__(
        self, app_name: str = "db_connector_tool", config_file: str = "connections.toml"
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
        import os
        self.app_name = app_name
        self.config_file = config_file
        self.config_dir = PathHelper.get_user_config_dir(app_name)
        self.config_path = self.config_dir / config_file
        self.crypto: Optional[CryptoManager] = None
        # 配置缓存
        self._config_cache: Optional[Dict[str, Any]] = None
        # 配置文件最后修改时间
        self._config_mtime: Optional[float] = None
        
        # 检查依赖可用性（类级别，只执行一次，线程安全）
        if not ConfigManager._dependencies_checked:
            # 懒加载锁对象
            if ConfigManager._dependency_check_lock is None:
                import threading
                ConfigManager._dependency_check_lock = threading.RLock()
            
            with ConfigManager._dependency_check_lock:
                # 再次检查，防止竞态条件
                if not ConfigManager._dependencies_checked:
                    ConfigManager._check_dependencies()
        
        # 确保配置文件存在
        self._ensure_config_exists()
    
    @classmethod
    def _check_dependencies(cls) -> None:
        """
        检查依赖项可用性（类方法，只执行一次）
        
        注意: 依赖检查结果是全局的，与应用名无关，因为：
        1. keyring库是否可用是系统级别的
        2. 环境变量DB_CONNECTOR_TOOL_ENCRYPTION_KEY是全局的
        3. 虽然keyring的服务名和用户名与应用名相关，但库的可用性是全局的
        """
        import os
        # 检查keyring库可用性
        try:
            import keyring
            cls._keyring_available = True
            logger.debug("keyring库可用")
        except ImportError:
            cls._keyring_available = False
            logger.debug("keyring库不可用")
        
        # 检查环境变量密钥
        env_key = os.environ.get(f"DB_CONNECTOR_TOOL_ENCRYPTION_KEY")
        cls._env_key_available = bool(env_key)
        if cls._env_key_available:
            logger.debug("环境变量中的加密密钥可用")
        else:
            logger.debug("环境变量中无加密密钥")
        
        # 检查是否有可用的密钥存储
        if not cls._keyring_available and not cls._env_key_available:
            logger.warning(
                "警告: 未找到安全的密钥存储方案。\n"
                "建议: 1. 安装keyring库 (pip install keyring)\n"
                "      2. 或设置环境变量 DB_CONNECTOR_TOOL_ENCRYPTION_KEY\n"
                "      3. 或使用 --secure 安装额外依赖 (pip install 'db-connector-tool[secure]')"
            )
        
        # 标记依赖检查已完成
        cls._dependencies_checked = True

    def _ensure_config_exists(self) -> None:
        """
        确保配置文件存在，如果不存在则创建默认配置

        Raises:
            ConfigError: 当配置文件创建或初始化失败时
        """
        try:
            if not self.config_path.exists():
                self._create_default_config()
            self._load_or_create_crypto_key_secure()
            logger.debug(f"配置文件就绪: {self.config_path}")
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败: {str(e)}")
            raise ConfigError(f"配置文件初始化失败: {str(e)}") from e
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
                "key_version": "1",  # 初始密钥版本
                "signature": "",  # 配置文件数字签名
                "audit_log": [],  # 变更审计日志
            },
        }
        self._save_config(default_config)
        logger.info(f"默认配置文件已创建: {self.config_path}")

    def _save_config(self, config: Dict[str, Any], operation: str = "update") -> None:
        """
        保存配置文件

        Args:
            config: 要保存的配置字典
            operation: 操作类型（update, add, remove, rotate_key）

        Raises:
            ConfigError: 当配置文件保存失败时

        Process:
            1. 更新最后修改时间
            2. 添加审计日志
            3. 生成数字签名
            4. 验证配置结构
            5. 保存为TOML格式
        """
        try:
            # 验证操作类型
            valid_operations = ['update', 'add', 'remove', 'rotate_key']
            operation = operation.lower()
            if operation not in valid_operations:
                raise ValueError(f'无效操作类型: {operation}')
            
            # 更新最后修改时间
            current_time = datetime.now().astimezone().isoformat()
            config["metadata"]["last_modified"] = current_time

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

            # 生成数字签名（排除signature和audit_log字段）
            config_to_sign = config.copy()
            config_to_sign["metadata"] = config_to_sign["metadata"].copy()
            config_to_sign["metadata"].pop("signature", None)
            config_to_sign["metadata"].pop("audit_log", None)
            
            # 改进的签名机制：使用HMAC-SHA256并包含时间戳
            import hashlib
            import hmac
            
            # 获取HMAC密钥
            hmac_key = self._get_secure_hmac_key()
            
            # 序列化配置
            serialized_config = tomli_w.dumps(config_to_sign)
            
            # 生成HMAC签名
            hmac_signature = hmac.new(hmac_key, serialized_config.encode(), hashlib.sha256).hexdigest()
            
            # 存储签名和签名时间戳
            config["metadata"]["signature"] = hmac_signature
            config["metadata"]["signature_timestamp"] = current_time

            # 验证配置结构
            self._validate_config(config)

            with open(self.config_path, "wb") as f:
                f.write(tomli_w.dumps(config).encode("utf-8"))

            # 清除缓存，确保下次加载时重新读取文件
            self._config_cache = None
            self._config_mtime = None
            logger.debug(f"配置文件已保存: {self.config_path}")

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件写入失败: {str(e)}")
            raise ConfigError(f"配置文件保存失败: {str(e)}") from e
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"配置数据序列化失败: {str(e)}")
            raise ConfigError(f"配置文件保存失败: {str(e)}") from e
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
            - 版本号格式必须有效
            - metadata 字段必须包含必要的子字段
            - connections 必须是字典类型
            - 审计日志必须是列表类型
            - 密钥版本必须是有效的字符串
        """
        # 验证必需字段
        required_fields = ["version", "app_name", "connections", "metadata"]
        for field in required_fields:
            if field not in config:
                raise ConfigError(f"配置文件缺少必需字段: {field}")

        # 验证版本号格式
        if not self._is_valid_version_format(config["version"]):
            raise ConfigError(f"无效的版本号格式: {config['version']}")

        # 验证connections字段类型
        if not isinstance(config["connections"], dict):
            raise ConfigError("connections字段必须是字典类型")

        # 验证metadata字段结构
        metadata = config.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ConfigError("metadata字段必须是字典类型")

        # 验证metadata必需子字段
        required_metadata_fields = ["created", "last_modified", "key_version"]
        for field in required_metadata_fields:
            if field not in metadata:
                raise ConfigError(f"metadata缺少必需字段: {field}")

        # 验证密钥版本格式
        key_version = metadata.get("key_version")
        if not isinstance(key_version, (str, int)) or not str(key_version).isdigit():
            raise ConfigError("key_version必须是有效的数字字符串")

        # 验证审计日志格式
        audit_log = metadata.get("audit_log", [])
        if not isinstance(audit_log, list):
            raise ConfigError("audit_log必须是列表类型")

    def _load_or_create_crypto_key_secure(self) -> None:
        """
        使用操作系统密钥存储服务加载或创建加密密钥

        Raises:
            ConfigError: 当密钥加载或创建失败时
        """
        try:
            # 尝试使用keyring库（如果可用）
            try:
                import keyring

                service_name = f"{self.app_name}_crypto"
                username = "master_key"

                # 尝试从密钥环获取密钥
                stored_key = keyring.get_password(service_name, username)

                if stored_key:
                    # 解析存储的密钥数据
                    key_data = json.loads(stored_key)
                    self.crypto = CryptoManager.from_saved_key(
                        key_data["password"], key_data["salt"]
                    )
                    logger.debug("从操作系统密钥存储加载密钥成功")
                else:
                    # 创建新密钥并存储
                    self.crypto = CryptoManager()
                    key_data = self.crypto.get_key_info()
                    keyring.set_password(service_name, username, json.dumps(key_data))
                    logger.info("新加密密钥已安全存储到操作系统密钥环")

            except ImportError:
                # keyring不可用，回退到文件权限方案
                logger.warning("keyring库不可用，使用文件权限保护方案")
                self._load_or_create_crypto_key()

        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"密钥数据解析失败: {str(e)}")
            raise ConfigError(f"安全密钥管理失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"安全密钥管理失败: {str(e)}")
            raise ConfigError(f"安全密钥管理失败: {str(e)}") from e

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
                # 设置文件权限为仅所有者可读写
                self._secure_file_permissions(key_file)

                with open(key_file, "rb") as f:
                    encrypted_key_data = f.read()

                # 解密密钥数据
                key_data = self._decrypt_key_file(encrypted_key_data)

                if "password" not in key_data or "salt" not in key_data:
                    raise ConfigError("密钥文件格式无效")

                self.crypto = CryptoManager.from_saved_key(
                    key_data["password"], key_data["salt"]
                )
                logger.debug("加密密钥加载成功")

            except (FileNotFoundError, PermissionError, OSError) as e:
                logger.error(f"密钥文件操作失败: {str(e)}")
                raise ConfigError(f"加密密钥加载失败: {str(e)}") from e
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logger.error(f"密钥数据解析失败: {str(e)}")
                raise ConfigError(f"加密密钥加载失败: {str(e)}") from e
            except CryptoError as e:
                # 解密失败，可能是因为密钥生成逻辑改变，删除旧密钥文件并创建新的
                logger.warning(f"解密密钥文件失败: {str(e)}，将创建新的密钥文件")
                try:
                    key_file.unlink()
                    logger.info("已删除旧的密钥文件")
                    # 直接创建新的密钥，避免递归调用
                    self.crypto = CryptoManager()
                    key_data = self.crypto.get_key_info()

                    # 加密密钥数据
                    encrypted_key_data = self._encrypt_key_file(key_data)

                    # 先写入文件，然后设置安全权限
                    with open(key_file, "wb") as f:
                        f.write(encrypted_key_data)

                    # 设置文件权限为仅所有者可读写
                    self._secure_file_permissions(key_file)

                    logger.info("新加密密钥创建成功")
                    return
                except Exception as delete_error:
                    logger.error(f"删除旧密钥文件失败: {str(delete_error)}")
                    raise ConfigError(f"加密密钥加载失败: {str(e)}") from e
            except Exception as e:
                logger.error(f"加载加密密钥失败: {str(e)}")
                raise ConfigError(f"加密密钥加载失败: {str(e)}") from e
        else:
            # 创建新密钥
            try:
                self.crypto = CryptoManager()
                key_data = self.crypto.get_key_info()

                # 加密密钥数据
                encrypted_key_data = self._encrypt_key_file(key_data)

                # 先写入文件，然后设置安全权限
                with open(key_file, "wb") as f:
                    f.write(encrypted_key_data)

                # 设置文件权限为仅所有者可读写
                self._secure_file_permissions(key_file)

                logger.info("新加密密钥创建成功")

            except (FileNotFoundError, PermissionError, OSError) as e:
                logger.error(f"密钥文件操作失败: {str(e)}")
                raise ConfigError(f"加密密钥创建失败: {str(e)}") from e
            except (json.JSONDecodeError, TypeError, ValueError) as e:
                logger.error(f"密钥数据处理失败: {str(e)}")
                raise ConfigError(f"加密密钥创建失败: {str(e)}") from e
            except Exception as e:
                logger.error(f"创建加密密钥失败: {str(e)}")
                raise ConfigError(f"加密密钥创建失败: {str(e)}") from e

    def _encrypt_key_file(self, key_data: Dict[str, Any]) -> bytes:
        """
        加密密钥文件数据

        Args:
            key_data: 密钥数据字典

        Returns:
            bytes: 加密后的字节数据（包含盐值和HMAC）

        Security Note:
            使用分层密钥策略：主密钥加密存储，HMAC确保数据完整性
        """
        import os
        import secrets
        import hmac
        import hashlib
        
        # 生成随机盐值
        salt = secrets.token_bytes(16)
        
        # 获取加密密钥（使用分层密钥策略）
        encryption_key = self._get_secure_encryption_key()
        
        # 创建加密管理器
        temp_crypto = CryptoManager(encryption_key, salt, skip_password_validation=True)
        
        # 序列化并加密
        serialized_data = json.dumps(key_data, ensure_ascii=False)
        encrypted_data = temp_crypto.encrypt_bytes(serialized_data.encode("utf-8"))
        
        # 计算HMAC校验值（使用独立的HMAC密钥）
        hmac_key = self._get_secure_hmac_key()
        hmac_value = hmac.new(hmac_key, salt + encrypted_data, hashlib.sha256).digest()
        
        # 存储格式：盐值(16字节) + HMAC(32字节) + 加密数据
        return salt + hmac_value + encrypted_data
    
    def _get_secure_encryption_key(self) -> str:
        """
        获取安全的加密密钥（分层密钥策略）
        
        Returns:
            str: 安全的加密密钥
        """
        import os
        import secrets
        
        # 优先使用系统级密钥存储
        if ConfigManager._keyring_available:
            try:
                import keyring
                service_name = f"{self.app_name}_key_file_encryption"
                username = f"{self.app_name}_user"
                
                # 尝试从密钥环获取密钥
                keyring_key = keyring.get_password(service_name, username)
                
                if keyring_key:
                    logger.debug("使用keyring存储的加密密钥")
                    return keyring_key
                else:
                    # 生成新的安全密钥并存储到密钥环
                    keyring_key = secrets.token_hex(32)
                    keyring.set_password(service_name, username, keyring_key)
                    logger.debug("生成并存储新的keyring加密密钥")
                    return keyring_key
            except Exception as e:
                logger.warning(f"keyring操作失败: {str(e)}")
        
        # 其次使用环境变量
        if ConfigManager._env_key_available:
            env_key = os.environ.get("DB_CONNECTOR_TOOL_ENCRYPTION_KEY")
            if env_key:
                logger.debug("使用环境变量中的加密密钥")
                return env_key
        
        # 最后使用基于文件的安全密钥存储作为回退方案
        key_file = self.config_dir / f"{self.app_name}_master.key"
        if key_file.exists():
            try:
                with open(key_file, "rb") as f:
                    file_key = f.read().decode('utf-8').strip()
                logger.debug("使用本地密钥文件进行加密")
                return file_key
            except Exception as e:
                logger.warning(f"读取本地密钥文件失败: {str(e)}")
        
        # 生成新的密钥文件
        file_key = secrets.token_hex(32)
        # 设置文件权限（仅所有者可读写）
        with open(key_file, "w") as f:
            f.write(file_key)
        
        # 跨平台文件权限设置
        if platform.system() == "Windows":
            # Windows: 改进的权限设置
            try:
                # 尝试使用win32security模块设置ACL（如果可用）
                try:
                    import win32security
                    import ntsecuritycon as con
                    
                    # 获取文件句柄
                    import win32file
                    import pywintypes
                    
                    # 打开文件
                    hfile = win32file.CreateFile(
                        str(key_file),
                        win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                        0,  # 不共享
                        None,
                        win32file.OPEN_EXISTING,
                        win32file.FILE_ATTRIBUTE_NORMAL,
                        None
                    )
                    
                    # 获取当前用户SID
                    token = win32security.OpenProcessToken(
                        win32security.GetCurrentProcess(),
                        win32security.TOKEN_QUERY
                    )
                    sid, _, _ = win32security.GetTokenInformation(
                        token, win32security.TokenUser
                    )
                    
                    # 创建DACL
                    dacl = win32security.ACL()
                    
                    # 添加当前用户的完全控制权限
                    dacl.AddAccessAllowedAce(
                        win32security.ACL_REVISION,
                        con.FILE_ALL_ACCESS,
                        sid
                    )
                    
                    # 设置安全描述符
                    sd = win32security.SECURITY_DESCRIPTOR()
                    sd.SetSecurityDescriptorDacl(1, dacl, 0)
                    
                    # 应用安全描述符
                    win32security.SetFileSecurity(
                        str(key_file),
                        win32security.DACL_SECURITY_INFORMATION,
                        sd
                    )
                    
                    win32file.CloseHandle(hfile)
                    logger.debug("Windows: 已设置密钥文件ACL权限")
                except ImportError:
                    # win32security不可用，尝试使用icacls命令
                    import subprocess
                    import getpass
                    
                    username = getpass.getuser()
                    # 移除所有权限，然后仅添加当前用户权限
                    subprocess.run(
                        ["icacls", str(key_file), "/inheritance:r", "/grant:r", f"{username}:(F)", "/remove", "*S-1-1-0"],
                        capture_output=True,
                        check=False
                    )
                    logger.debug("Windows: 已使用icacls设置密钥文件权限")
            except Exception as e:
                logger.warning(f"Windows文件权限设置失败: {str(e)}，请手动确保文件安全")
        else:
            import stat
            key_file.chmod(stat.S_IRUSR | stat.S_IWUSR)
            logger.debug("Unix/Linux: 已设置密钥文件权限为600")
        
        logger.debug("生成新的本地密钥文件")
        
        # 记录警告
        logger.warning(
            f"使用本地密钥文件 {key_file} 作为密钥存储方案。\n"
            "建议: 1. 安装keyring库 (pip install keyring)\n"
            "      2. 或设置环境变量 DB_CONNECTOR_TOOL_ENCRYPTION_KEY\n"
            "      3. 或使用 --secure 安装额外依赖 (pip install 'db-connector-tool[secure]')"
        )
        
        return file_key
    
    def _get_secure_hmac_key(self) -> bytes:
        """
        获取安全的HMAC密钥
        
        Returns:
            bytes: 安全的HMAC密钥
        """
        import os
        import secrets
        
        # 优先使用系统级密钥存储
        if ConfigManager._keyring_available:
            try:
                import keyring
                service_name = f"{self.app_name}_hmac_key"
                username = f"{self.app_name}_user"
                
                # 尝试从密钥环获取HMAC密钥
                hmac_key_str = keyring.get_password(service_name, username)
                
                if hmac_key_str:
                    logger.debug("使用keyring存储的HMAC密钥")
                    return bytes.fromhex(hmac_key_str)
                else:
                    # 生成新的HMAC密钥并存储到密钥环
                    hmac_key = secrets.token_bytes(32)
                    keyring.set_password(service_name, username, hmac_key.hex())
                    logger.debug("生成并存储新的keyring HMAC密钥")
                    return hmac_key
            except Exception as e:
                logger.warning(f"keyring HMAC操作失败: {str(e)}")
        
        # 其次使用环境变量
        hmac_env_key = os.environ.get("DB_CONNECTOR_TOOL_HMAC_KEY")
        if hmac_env_key:
            logger.debug("使用环境变量中的HMAC密钥")
            return bytes.fromhex(hmac_env_key)
        
        # 最后生成临时HMAC密钥（不存储）
        hmac_key = secrets.token_bytes(32)
        logger.debug("使用临时生成的HMAC密钥")
        return hmac_key

    def _decrypt_key_file(self, encrypted_data: bytes) -> Dict[str, Any]:
        """
        解密密钥文件数据

        Args:
            encrypted_data: 加密的字节数据（包含盐值、HMAC和加密数据）

        Returns:
            Dict[str, Any]: 解密后的密钥数据字典

        Raises:
            ConfigError: 当数据格式无效、HMAC验证失败或解密失败时
        """
        import os
        import secrets
        import hmac
        import hashlib
        
        # 验证数据长度（盐值16字节 + HMAC 32字节 + 至少1字节加密数据）
        if len(encrypted_data) < 49:
            raise ConfigError("密钥文件数据格式无效：数据长度不足")
        
        # 提取各部分内容
        salt = encrypted_data[:16]
        stored_hmac = encrypted_data[16:48]
        actual_encrypted_data = encrypted_data[48:]
        
        # 获取加密密钥（使用分层密钥策略）
        encryption_key = self._get_secure_encryption_key()
        
        # 创建加密管理器
        temp_crypto = CryptoManager(encryption_key, salt, skip_password_validation=True)
        
        # 验证HMAC，确保数据完整性
        hmac_key = self._get_secure_hmac_key()
        expected_hmac = hmac.new(hmac_key, salt + actual_encrypted_data, hashlib.sha256).digest()
        if not hmac.compare_digest(stored_hmac, expected_hmac):
            raise ConfigError("密钥文件数据完整性验证失败：数据可能被篡改")
        
        # 解密并反序列化
        try:
            decrypted_bytes = temp_crypto.decrypt_bytes(actual_encrypted_data)
            serialized_data = decrypted_bytes.decode("utf-8")
            return json.loads(serialized_data)
        except Exception as e:
            raise ConfigError(f"密钥文件解密失败：{str(e)}") from e

    def _secure_file_permissions(self, file_path: Path) -> None:
        """
        设置文件安全权限，仅允许所有者读写

        Args:
            file_path: 文件路径

        Raises:
            ConfigError: 当权限设置失败时
        """
        try:
            system = platform.system().lower()

            if system == "windows":
                # Windows系统：使用icacls设置权限
                try:
                    # 移除所有用户的权限
                    subprocess.run(
                        ["icacls", str(file_path), "/reset"],
                        check=True,
                        capture_output=True,
                    )
                    # 仅当前用户完全控制
                    subprocess.run(
                        ["icacls", str(file_path), "/grant", "%username%:F"],
                        check=True,
                        capture_output=True,
                    )
                    # 拒绝其他所有用户访问
                    subprocess.run(
                        ["icacls", str(file_path), "/inheritance:r"],
                        check=True,
                        capture_output=True,
                    )
                except (subprocess.CalledProcessError, FileNotFoundError):
                    # 如果icacls不可用，使用基本的文件属性
                    file_path.chmod(stat.S_IREAD | stat.S_IWRITE)
            else:
                # Unix-like系统：设置600权限（仅所有者可读写）
                file_path.chmod(stat.S_IREAD | stat.S_IWRITE)

            logger.debug("设置文件权限成功")

        except Exception as e:
            logger.warning(f"设置文件权限失败 {file_path}: {str(e)}")
            # 权限设置失败不应阻止程序运行，但记录警告

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
        self._validate_connection_name(name)
        self._validate_connection_config(connection_config)

        try:
            config = self._load_config()

            # 检查连接是否已存在
            if name in config["connections"]:
                raise ConfigError(f"连接配置已存在: {name}")

            # 确保加密管理器已初始化
            self._ensure_crypto_initialized()

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

            self._save_config(config, "add")
            logger.info(f"连接配置已添加: {name}")

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置添加失败: {str(e)}") from e
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"配置数据处理失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置添加失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"添加连接配置失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置添加失败: {str(e)}") from e

    def _load_config(self) -> Dict[str, Any]:
        """
        加载并验证配置文件

        Returns:
            Dict[str, Any]: 配置字典

        Raises:
            ConfigError: 当配置文件格式无效或版本不支持时

        Process:
            1. 检查配置文件最后修改时间
            2. 如果文件未修改且缓存存在，直接返回缓存
            3. 读取TOML文件
            4. 验证配置文件结构
            5. 验证数字签名
            6. 更新缓存并返回
        """
        try:
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

        except tomllib.TOMLDecodeError as e:
            logger.error(f"配置文件TOML格式错误: {str(e)}")
            raise ConfigError(f"配置文件格式无效: {str(e)}") from e
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败: {str(e)}")
            raise ConfigError(f"配置文件加载失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"加载配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件加载失败: {str(e)}") from e

    def _verify_config_signature(self, config: Dict[str, Any]) -> bool:
        """
        验证配置文件数字签名

        Args:
            config: 配置字典

        Returns:
            bool: 签名是否有效

        Raises:
            ConfigError: 当签名验证失败时
        """
        try:
            signature = config.get("metadata", {}).get("signature", "")
            
            # 如果签名为空，跳过验证（可能是新创建的配置）
            if not signature:
                logger.debug("配置文件无数字签名，跳过验证")
                return True

            # 确保加密管理器已初始化
            if self.crypto is None:
                logger.warning("加密管理器未初始化，跳过签名验证")
                return True

            # 生成待验证的配置数据（排除signature和audit_log字段）
            config_to_verify = config.copy()
            config_to_verify["metadata"] = config_to_verify["metadata"].copy()
            config_to_verify["metadata"].pop("signature", None)
            config_to_verify["metadata"].pop("audit_log", None)
            config_to_verify["metadata"].pop("signature_timestamp", None)  # 排除时间戳

            # 验证签名 - 支持HMAC-SHA256
            import hashlib
            import hmac
            
            # 获取HMAC密钥
            hmac_key = self._get_secure_hmac_key()
            
            # 序列化配置
            serialized_config = tomli_w.dumps(config_to_verify)
            
            # 生成预期签名
            expected_signature = hmac.new(hmac_key, serialized_config.encode(), hashlib.sha256).hexdigest()

            if signature != expected_signature:
                logger.warning("配置文件数字签名验证失败，可能被篡改")
                # 不抛出异常，允许加载但记录警告
                return False

            # 验证时间戳，防止重放攻击
            signature_timestamp = config.get("metadata", {}).get("signature_timestamp")
            if signature_timestamp:
                from datetime import datetime, timezone
                try:
                    signature_time = datetime.fromisoformat(signature_timestamp)
                    current_time = datetime.now(timezone.utc)
                    # 允许1小时的时间差（防止时钟同步问题）
                    time_diff = (current_time - signature_time).total_seconds()
                    if abs(time_diff) > 3600:
                        logger.warning("配置文件签名时间戳过期，可能是重放攻击")
                        return False
                except Exception as e:
                    logger.warning(f"时间戳验证失败: {str(e)}")
                    # 时间戳验证失败不影响签名验证结果

            logger.debug("配置文件数字签名验证成功")
            return True

        except Exception as e:
            logger.warning(f"配置文件签名验证失败: {str(e)}")
            # 不抛出异常，允许加载但记录警告
            return False

    def get_audit_log(self) -> List[Dict[str, Any]]:
        """
        获取配置变更审计日志

        Returns:
            List[Dict[str, Any]]: 审计日志列表

        Example:
            >>> audit_log = config_manager.get_audit_log()
            >>> print(audit_log[0]["operation"])
            "add"
        """
        try:
            config = self._load_config()
            return config.get("metadata", {}).get("audit_log", [])
        except Exception as e:
            logger.error(f"获取审计日志失败: {str(e)}")
            return []

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
            ConfigError: 当版本号递增导致主版本号发生不合理变化时

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
                logger.warning(f"无效的版本号格式，重置为初始版本: {current_version} -> 1.0.0")
                config["version"] = "1.0.0"
                return

            # 解析版本号各部分
            version_parts = current_version.split(".")
            major_num = int(version_parts[0])
            minor_num = int(version_parts[1])
            patch_num = int(version_parts[2])

            # 递增修订号（patch）
            patch_num += 1

            # 处理进位逻辑
            if patch_num >= 100:
                patch_num = 0
                minor_num += 1

                # 处理次要版本进位
                if minor_num >= 100:
                    minor_num = 0
                    major_num += 1

                    # 检查主版本号是否合理（限制主版本号不超过99）
                    if major_num > 99:
                        raise ConfigError(
                            ERROR_VERSION_MAJOR_CHANGE,
                            details={
                                "current_version": current_version,
                                "would_become": f"{major_num}.{minor_num}.{patch_num}",
                                "max_major_version": 99,
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
            logger.debug(f"配置文件版本号已更新: {current_version} -> {new_version}")

        except Exception as e:
            logger.warning(f"版本号递增失败，保持原版本号: {str(e)}")
            # 如果版本号递增失败，不影响主要功能，继续使用原版本号

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
                if len(part) > 1 and part.startswith('0'):
                    return False  # 不允许前导零
                num = int(part)
                if num < 0:
                    return False

            return True
        except (ValueError, AttributeError):
            return False

    def _validate_connection_name(self, name: str) -> None:
        """
        验证连接名称是否有效

        Args:
            name: 连接名称

        Raises:
            ValueError: 当连接名称为空或不是字符串时
        """
        if not name or not isinstance(name, str):
            raise ValueError(ERROR_EMPTY_CONNECTION_NAME)

    def _validate_connection_config(self, connection_config: Dict[str, Any]) -> None:
        """
        验证连接配置字典是否有效

        Args:
            connection_config: 连接配置字典

        Raises:
            ValueError: 当连接配置为空或不是字典时
        """
        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError(ERROR_INVALID_CONFIG_DICT)

    def _ensure_crypto_initialized(self) -> None:
        """
        确保加密管理器已初始化

        Raises:
            ConfigError: 当加密管理器未初始化时
        """
        if self.crypto is None:
            raise ConfigError("加密管理器未初始化，无法处理敏感信息")

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
        self._validate_connection_name(name)

        try:
            config = self._load_config()

            if name not in config["connections"]:
                raise ConfigError(f"连接配置不存在: {name}")

            del config["connections"][name]
            self._save_config(config, "remove")
            logger.info(f"连接配置已删除: {name}")

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置删除失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"删除连接配置失败 {name}: {str(e)}")
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
        self._validate_connection_name(name)
        self._validate_connection_config(connection_config)

        try:
            # 确保加密管理器已初始化
            self._ensure_crypto_initialized()

            # 加载配置并直接更新
            config = self._load_config()
            
            if name not in config["connections"]:
                raise ConfigError(f"连接配置不存在: {name}")

            # 加密新的连接配置
            encrypted_config = {}
            for key, value in connection_config.items():
                serialized_value = self._serialize_value(value)
                encrypted_config[key] = self.crypto.encrypt(serialized_value)

            # 更新配置
            config["connections"][name] = encrypted_config
            self._increment_config_version(config)
            self._save_config(config, "update")
            logger.info(f"连接配置已更新: {name}")

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置更新失败: {str(e)}") from e
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"配置数据处理失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置更新失败: {str(e)}") from e
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
        self._validate_connection_name(name)

        try:
            config = self._load_config()

            if name not in config["connections"]:
                raise ConfigError(f"连接配置不存在: {name}")

            connection_config = config["connections"][name].copy()

            # 确保加密管理器已初始化
            self._ensure_crypto_initialized()

            # 全字段解密：解密所有连接配置字段并恢复数据类型
            decrypted_config = {}
            for key, encrypted_value in connection_config.items():
                # 解密值
                serialized_value = self.crypto.decrypt(encrypted_value)
                # 反序列化值（恢复原始数据类型）
                decrypted_config[key] = self._deserialize_value(serialized_value)

            logger.debug(f"连接配置已获取: {name}")
            return decrypted_config

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置获取失败: {str(e)}") from e
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"配置数据解密或解析失败 {name}: {str(e)}")
            raise ConfigError(f"连接配置获取失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"获取连接配置失败 {name}: {str(e)}")
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
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败: {str(e)}")
            raise ConfigError(f"连接列出失败: {str(e)}") from e
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
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败: {str(e)}")
            raise ConfigError(f"配置信息获取失败: {str(e)}") from e
        except (KeyError, TypeError) as e:
            logger.error(f"配置数据结构错误: {str(e)}")
            raise ConfigError(f"配置信息获取失败: {str(e)}") from e
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

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件备份操作失败: {str(e)}")
            raise ConfigError(f"配置文件备份失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"备份配置文件失败: {str(e)}")
            raise ConfigError(f"配置文件备份失败: {str(e)}") from e

    def rotate_encryption_key(self) -> str:
        """
        轮换加密密钥

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 当密钥轮换失败时

        Security Note:
            - 密钥轮换会重新加密所有连接配置
            - 操作前会自动备份配置文件
            - 密钥轮换是不可逆操作

        Example:
            >>> new_version = config_manager.rotate_encryption_key()
            >>> print(new_version)
            "2"
        """
        try:
            # 备份当前配置
            backup_path = self.backup_config()
            logger.info(f"密钥轮换前已备份配置: {backup_path}")

            # 加载当前配置
            config = self._load_config()

            # 解密所有连接配置
            decrypted_connections = {}
            for name, encrypted_config in config["connections"].items():
                # 确保加密管理器已初始化
                if self.crypto is None:
                    raise ConfigError("加密管理器未初始化，无法解密敏感信息")

                # 解密连接配置
                decrypted_config = {}
                for key, encrypted_value in encrypted_config.items():
                    serialized_value = self.crypto.decrypt(encrypted_value)
                    decrypted_config[key] = self._deserialize_value(serialized_value)
                decrypted_connections[name] = decrypted_config

            # 生成新的加密密钥
            new_crypto = CryptoManager()
            self.crypto = new_crypto

            # 更新密钥版本
            current_key_version = int(config.get("metadata", {}).get("key_version", "1"))
            new_key_version = str(current_key_version + 1)
            config["metadata"]["key_version"] = new_key_version

            # 重新加密所有连接配置
            re_encrypted_connections = {}
            for name, decrypted_config in decrypted_connections.items():
                encrypted_config = {}
                for key, value in decrypted_config.items():
                    serialized_value = self._serialize_value(value)
                    encrypted_config[key] = self.crypto.encrypt(serialized_value)
                re_encrypted_connections[name] = encrypted_config

            config["connections"] = re_encrypted_connections

            # 保存新的密钥文件
            key_file = self.config_dir / "encryption.key"
            key_data = self.crypto.get_key_info()
            encrypted_key_data = self._encrypt_key_file(key_data)

            with open(key_file, "wb") as f:
                f.write(encrypted_key_data)

            self._secure_file_permissions(key_file)

            # 保存更新后的配置
            self._save_config(config, "rotate_key")

            logger.info(f"密钥轮换成功，新密钥版本: {new_key_version}")
            return new_key_version

        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件或密钥文件操作失败: {str(e)}")
            raise ConfigError(f"密钥轮换失败: {str(e)}") from e
        except (json.JSONDecodeError, TypeError, ValueError) as e:
            logger.error(f"配置数据处理失败: {str(e)}")
            raise ConfigError(f"密钥轮换失败: {str(e)}") from e
        except Exception as e:
            logger.error(f"密钥轮换失败: {str(e)}")
            raise ConfigError(f"密钥轮换失败: {str(e)}") from e

    def get_key_version(self) -> str:
        """
        获取当前密钥版本

        Returns:
            str: 当前密钥版本号

        Example:
            >>> version = config_manager.get_key_version()
            >>> print(version)
            "1"
        """
        try:
            config = self._load_config()
            return config.get("metadata", {}).get("key_version", "1")
        except (FileNotFoundError, PermissionError, OSError) as e:
            logger.error(f"配置文件操作失败: {str(e)}")
            return "1"  # 默认返回版本1
        except (KeyError, TypeError) as e:
            logger.error(f"配置数据结构错误: {str(e)}")
            return "1"  # 默认返回版本1
        except Exception as e:
            logger.error(f"获取密钥版本失败: {str(e)}")
            return "1"  # 默认返回版本1

    def __str__(self) -> str:
        """返回配置管理器的用户友好字符串表示"""
        try:
            config_info = self.get_config_info()
            connection_count = config_info["connection_count"]
            return f"ConfigManager('{self.app_name}', {connection_count} connections)"
        except Exception:
            # 如果获取配置信息失败，返回基本表示
            return f"ConfigManager('{self.app_name}', '{self.config_file}')"

    def __del__(self):
        """
        析构函数，确保敏感数据在对象销毁时被清理
        """
        self.clear_sensitive_data()

    def clear_sensitive_data(self):
        """
        清理内存中的敏感数据

        Security Note:
            - 清理加密管理器中的敏感数据
            - 清理配置缓存
            - 调用后需要重新初始化才能使用加密功能
        """
        if self.crypto is not None:
            self.crypto.clear_sensitive_data()
            self.crypto = None
        # 清理配置缓存
        self._config_cache = None
        self._config_mtime = None
        logger.debug("配置管理器敏感数据和缓存已清理")

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
