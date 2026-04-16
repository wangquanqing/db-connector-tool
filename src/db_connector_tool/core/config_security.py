"""配置安全和审计模块 (ConfigSecurityManager)

负责配置文件的安全验证、数字签名、审计日志管理和密钥轮换功能。

Example:
>>> from db_connector_tool.core.config_security import ConfigSecurityManager
>>> from db_connector_tool.core.key_manager import KeyManager
>>> key_manager = KeyManager("my_app")
>>> security_manager = ConfigSecurityManager(key_manager)
>>> config = {"connections": {}, "metadata": {}}
>>> signature = security_manager.generate_config_signature(config)
>>> is_valid = security_manager.verify_config_signature(config)
>>> new_version = security_manager.perform_key_rotation(config)
"""

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any, Dict

import tomli_w

from ..utils.logging_utils import get_logger
from .exceptions import ConfigError

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class ConfigSecurityManager:
    """配置安全管理器 (Config Security Manager)

    负责配置文件的安全验证、数字签名、审计日志管理和密钥轮换功能。
    提供配置文件的完整性验证和敏感数据的加密存储。

    Example:
    >>> from db_connector_tool.core.config_security import ConfigSecurityManager
    >>> from db_connector_tool.core.key_manager import KeyManager
    >>> key_manager = KeyManager("my_app")
    >>> security_manager = ConfigSecurityManager(key_manager)
    >>> config = {"connections": {}, "metadata": {}}
    >>> signature = security_manager.generate_config_signature(config)
    >>> is_valid = security_manager.verify_config_signature(config)
    >>> new_version = security_manager.perform_key_rotation(config)
    """

    def __init__(self, key_manager):
        """初始化安全管理器

        创建新的配置安全管理器实例，用于管理配置文件的安全验证和加密操作。

        Args:
            key_manager: 密钥管理器实例，用于获取加密密钥

        Example:
            >>> from db_connector_tool.core.config_security import ConfigSecurityManager
            >>> from db_connector_tool.core.key_manager import KeyManager
            >>> key_manager = KeyManager("my_app")
            >>> security_manager = ConfigSecurityManager(key_manager)
        """

        self.key_manager = key_manager

    def generate_config_signature(self, config: Dict[str, Any]) -> str:
        """生成配置文件数字签名

        为配置文件生成HMAC数字签名，用于验证配置文件的完整性和防止篡改。

        Args:
            config: 配置字典，包含要签名的配置数据

        Returns:
            str: 生成的HMAC签名，用于验证配置文件的完整性

        Example:
            >>> config = {"connections": {}, "metadata": {}}
            >>> signature = security_manager.generate_config_signature(config)
            >>> print(len(signature))
            64
        """

        # 生成数字签名（排除signature、audit_log和signature_timestamp字段）
        config_to_sign = config.copy()
        config_to_sign["metadata"] = config_to_sign["metadata"].copy()
        config_to_sign["metadata"].pop("signature", None)
        config_to_sign["metadata"].pop("audit_log", None)
        config_to_sign["metadata"].pop("signature_timestamp", None)

        # 获取HMAC密钥
        hmac_key = self.key_manager.get_secure_hmac_key()

        # 序列化配置
        serialized_config = tomli_w.dumps(config_to_sign)

        # 生成HMAC签名
        hmac_signature = hmac.new(
            hmac_key, serialized_config.encode(), hashlib.sha256
        ).hexdigest()

        return hmac_signature

    def add_audit_log_entry(
        self, config: Dict[str, Any], operation: str, current_time: str
    ) -> None:
        """添加审计日志条目

        向配置文件的元数据中添加审计日志条目，记录配置变更操作。

        Args:
            config: 配置字典，包含metadata字段
            operation: 操作类型，如add、remove、update、rotate_key
            current_time: 当前时间戳，格式为ISO 8601

        Example:
            >>> import datetime
            >>> current_time = datetime.datetime.now().astimezone().isoformat()
            >>> security_manager.add_audit_log_entry(config, "add", current_time)
            >>> print(len(config["metadata"]["audit_log"]))
            1
        """

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

    def verify_config_signature(self, config: Dict[str, Any]) -> bool:
        """验证配置文件数字签名

        验证配置文件的数字签名，确保配置文件未被篡改，并检查签名时间戳。

        Args:
            config: 配置字典，包含metadata字段和signature字段

        Returns:
            bool: 签名是否有效

        Raises:
            ConfigError: 签名验证失败

        Example:
            >>> is_valid = security_manager.verify_config_signature(config)
            >>> print(is_valid)
            True
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
            except ConfigError as exc:
                logger.error("加密管理器未初始化，无法验证签名")
                raise ConfigError("加密管理器未初始化，无法验证配置文件签名") from exc

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
                logger.error("配置文件数字签名验证失败，可能被篡改")
                raise ConfigError("配置文件数字签名验证失败，可能被篡改")

            # 验证时间戳，防止重放攻击
            signature_timestamp = config.get("metadata", {}).get("signature_timestamp")
            if not signature_timestamp:
                logger.error("配置文件缺少签名时间戳，可能是重放攻击")
                raise ConfigError("配置文件缺少签名时间戳，可能是重放攻击")

            try:
                signature_time = datetime.fromisoformat(signature_timestamp)
                # 确保签名时间是带时区的
                if signature_time.tzinfo is None:
                    # 如果没有时区信息，假设是本地时间
                    signature_time = signature_time.astimezone()
                current_time = datetime.now(timezone.utc)
                # 确保当前时间也是带时区的
                current_time = current_time.astimezone()
                # 允许1小时的时间差（防止时钟同步问题）
                time_diff = (current_time - signature_time).total_seconds()
                if abs(time_diff) > 3600:
                    logger.error("配置文件签名时间戳过期，可能是重放攻击")
                    raise ConfigError("配置文件签名时间戳过期，可能是重放攻击")
            except (ValueError, TypeError) as e:
                logger.error("时间戳验证失败: %s", str(e))
                raise ConfigError(f"时间戳验证失败: {str(e)}") from e

            logger.debug("配置文件数字签名验证成功")
            return True

        except ConfigError:
            # 重新抛出ConfigError
            raise
        except (ValueError, AttributeError, RuntimeError) as e:
            logger.error("配置文件签名验证失败: %s", str(e))
            raise ConfigError(f"配置文件签名验证失败: {str(e)}") from e

    def encrypt_dict_values(self, data_dict: Dict[str, Any]) -> Dict[str, str]:
        """加密字典中的所有值

        加密字典中的所有值，使用密钥管理器提供的加密方法，保留数据类型信息。

        Args:
            data_dict: 要加密的字典，值可以是任意类型

        Returns:
            Dict[str, str]: 加密后的字典，所有值均为加密后的字符串

        Example:
            >>> config = {"host": "localhost", "port": 5432}
            >>> encrypted = security_manager.encrypt_dict_values(config)
            >>> print("host" in encrypted)
            True
            >>> print(isinstance(encrypted["host"], str))
            True
        """

        crypto = self.key_manager.get_crypto_manager()
        encrypted_dict = {}
        for key, value in data_dict.items():
            serialized_value = self._serialize_value(value)
            encrypted_dict[key] = crypto.encrypt(serialized_value)
        return encrypted_dict

    def decrypt_dict_values(self, encrypted_dict: Dict[str, str]) -> Dict[str, Any]:
        """解密字典中的所有值

        解密字典中的所有值，使用密钥管理器提供的解密方法，恢复原始数据类型。

        Args:
            encrypted_dict: 加密的字典，所有值均为加密后的字符串

        Returns:
            Dict[str, Any]: 解密后的字典，值为原始类型

        Example:
            >>> config = {"host": "localhost", "port": 5432}
            >>> encrypted = security_manager.encrypt_dict_values(config)
            >>> decrypted = security_manager.decrypt_dict_values(encrypted)
            >>> print(decrypted["host"])
            "localhost"
            >>> print(decrypted["port"])
            5432
        """

        crypto = self.key_manager.get_crypto_manager()
        decrypted_dict = {}
        for key, encrypted_value in encrypted_dict.items():
            serialized_value = crypto.decrypt(encrypted_value)
            decrypted_dict[key] = self._deserialize_value(serialized_value)
        return decrypted_dict

    def _serialize_value(self, value: Any) -> str:
        """序列化值以便加密，保留数据类型信息

        将任意类型的值序列化为JSON字符串，包含类型信息，以便解密时恢复原始类型。

        Args:
            value: 要序列化的任意类型值

        Returns:
            str: JSON格式的序列化字符串，包含类型信息

        Example:
            >>> serialized = security_manager._serialize_value(42)
            >>> print(serialized)
            '{"type": "int", "value": 42}'
        """

        value_info = {"type": type(value).__name__, "value": value}
        return json.dumps(value_info, ensure_ascii=False)

    def _deserialize_value(self, json_str: str) -> Any:
        """反序列化值，恢复原始数据类型

        从JSON字符串中反序列化值，并根据类型信息恢复原始数据类型。

        Args:
            json_str: JSON格式的序列化字符串，包含类型信息

        Returns:
            Any: 反序列化后的原始值，保持原始数据类型

        Example:
            >>> serialized = '{"type": "int", "value": 42}'
            >>> value = security_manager._deserialize_value(serialized)
            >>> print(value)
            42
            >>> print(type(value))
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
            logger.error("反序列化失败: %s", str(error))
            raise ConfigError(f"反序列化失败: {str(error)}") from error

    def perform_key_rotation(self, config: Dict[str, Any]) -> str:
        """执行密钥轮换的核心逻辑

        执行密钥轮换，包括解密所有连接配置、生成新密钥、重新加密所有连接配置。

        Args:
            config: 配置字典，包含connections和metadata字段

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 密钥轮换失败

        Example:
            >>> new_version = security_manager.perform_key_rotation(config)
            >>> print(new_version)
            "2"
        """

        # 保存原始配置的完整副本，用于回滚
        original_config = {}
        for key, value in config.items():
            if isinstance(value, dict):
                original_config[key] = value.copy()
            else:
                original_config[key] = value
        original_connections = config["connections"].copy()
        original_key_version = config["metadata"].get("key_version", "1")

        # 保存原始密钥信息，用于回滚
        original_key_info = (
            self.key_manager.crypto.get_key_info() if self.key_manager.crypto else None
        )

        try:
            # 解密所有连接配置
            decrypted_connections = self._decrypt_all_connections(config)

            # 生成新的加密密钥
            self.key_manager.rotate_key()

            # 更新密钥版本
            new_key_version = self._update_key_version(config)

            # 重新加密所有连接配置
            self._re_encrypt_all_connections(config, decrypted_connections)

            return new_key_version
        except Exception as error:
            # 发生异常时回滚
            logger.error("密钥轮换失败，执行回滚: %s", str(error))
            # 恢复原始配置的所有字段
            for key, value in original_config.items():
                config[key] = value
            # 恢复原始连接配置
            config["connections"] = original_connections
            config["metadata"]["key_version"] = original_key_version
            # 如果可能，恢复原始密钥
            if original_key_info:
                try:
                    # 尝试恢复原始密钥
                    from .crypto import CryptoManager

                    self.key_manager.crypto = CryptoManager.from_saved_key(
                        original_key_info["password"],
                        original_key_info["salt"],
                        original_key_info["iterations"],
                    )
                    logger.debug("已恢复原始加密密钥")
                except Exception as key_restore_error:
                    logger.warning("恢复原始密钥失败: %s", str(key_restore_error))
            raise ConfigError(f"密钥轮换失败: {str(error)}") from error

    def _decrypt_all_connections(
        self, config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """解密所有连接配置

        解密配置文件中的所有连接配置，返回解密后的连接配置字典。

        Args:
            config: 配置字典，包含connections字段

        Returns:
            Dict[str, Dict[str, Any]]: 解密后的连接配置字典

        Example:
            >>> decrypted = security_manager._decrypt_all_connections(config)
            >>> print(isinstance(decrypted, dict))
            True
        """

        decrypted_connections = {}
        for name, encrypted_config in config["connections"].items():
            # 使用统一的解密方法
            decrypted_config = self.decrypt_dict_values(encrypted_config)
            decrypted_connections[name] = decrypted_config

        return decrypted_connections

    def _update_key_version(self, config: Dict[str, Any]) -> str:
        """更新密钥版本号

        更新配置文件中的密钥版本号，递增版本号。

        Args:
            config: 配置字典，包含metadata字段

        Returns:
            str: 新的密钥版本号

        Example:
            >>> new_version = security_manager._update_key_version(config)
            >>> print(new_version)
            "2"
        """

        current_key_version = int(config.get("metadata", {}).get("key_version", "1"))
        new_key_version = str(current_key_version + 1)
        config["metadata"]["key_version"] = new_key_version
        return new_key_version

    def _re_encrypt_all_connections(
        self, config: Dict[str, Any], decrypted_connections: Dict[str, Dict[str, Any]]
    ) -> None:
        """重新加密所有连接配置

        使用新的加密密钥重新加密所有连接配置，并更新配置字典。

        Args:
            config: 配置字典，包含connections字段
            decrypted_connections: 解密后的连接配置字典

        Example:
            >>> security_manager._re_encrypt_all_connections(config, decrypted_connections)
            >>> print("connections" in config)
            True
        """

        re_encrypted_connections = {}
        for name, decrypted_config in decrypted_connections.items():
            # 使用统一的加密方法
            encrypted_config = self.encrypt_dict_values(decrypted_config)
            re_encrypted_connections[name] = encrypted_config

        config["connections"] = re_encrypted_connections
