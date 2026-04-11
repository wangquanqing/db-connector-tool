"""配置安全和审计模块 (ConfigSecurityManager)

负责配置文件的安全验证、 数字签名、 审计日志管理和密钥轮换功能。
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
    """配置安全管理器

    负责配置文件的安全验证、数字签名、审计日志管理和密钥轮换功能。
    """

    def __init__(self, key_manager):
        """初始化安全管理器

        Args:
            key_manager: 密钥管理器实例
        """

        self.key_manager = key_manager

    def generate_config_signature(self, config: Dict[str, Any]) -> str:
        """生成配置文件数字签名

        Args:
            config: 配置字典

        Returns:
            str: 生成的HMAC签名
        """

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

        return hmac_signature

    def add_audit_log_entry(
        self, config: Dict[str, Any], operation: str, current_time: str
    ) -> None:
        """添加审计日志条目

        Args:
            config: 配置字典
            operation: 操作类型
            current_time: 当前时间戳
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

    def encrypt_dict_values(self, data_dict: Dict[str, Any]) -> Dict[str, str]:
        """加密字典中的所有值

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

    def decrypt_dict_values(self, encrypted_dict: Dict[str, str]) -> Dict[str, Any]:
        """解密字典中的所有值

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
        """序列化值以便加密，保留数据类型信息

        Args:
            value: 要序列化的任意类型值

        Returns:
            str: JSON格式的序列化字符串
        """

        value_info = {"type": type(value).__name__, "value": value}
        return json.dumps(value_info, ensure_ascii=False)

    def _deserialize_value(self, json_str: str) -> Any:
        """反序列化值，恢复原始数据类型

        Args:
            json_str: JSON格式的序列化字符串

        Returns:
            Any: 反序列化后的原始值
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

    def perform_key_rotation(self, config: Dict[str, Any]) -> str:
        """执行密钥轮换的核心逻辑

        Args:
            config: 配置字典

        Returns:
            str: 新的密钥版本号

        Raises:
            ConfigError: 密钥轮换失败
        """

        # 解密所有连接配置
        decrypted_connections = self._decrypt_all_connections(config)

        # 生成新的加密密钥
        self.key_manager.rotate_key()

        # 更新密钥版本
        new_key_version = self._update_key_version(config)

        # 重新加密所有连接配置
        self._re_encrypt_all_connections(config, decrypted_connections)

        return new_key_version

    def _decrypt_all_connections(
        self, config: Dict[str, Any]
    ) -> Dict[str, Dict[str, Any]]:
        """解密所有连接配置

        Args:
            config: 配置字典

        Returns:
            Dict[str, Dict[str, Any]]: 解密后的连接配置字典
        """

        decrypted_connections = {}
        for name, encrypted_config in config["connections"].items():
            # 使用统一的解密方法
            decrypted_config = self.decrypt_dict_values(encrypted_config)
            decrypted_connections[name] = decrypted_config

        return decrypted_connections

    def _update_key_version(self, config: Dict[str, Any]) -> str:
        """更新密钥版本号

        Args:
            config: 配置字典

        Returns:
            str: 新的密钥版本号
        """

        current_key_version = int(config.get("metadata", {}).get("key_version", "1"))
        new_key_version = str(current_key_version + 1)
        config["metadata"]["key_version"] = new_key_version
        return new_key_version

    def _re_encrypt_all_connections(
        self, config: Dict[str, Any], decrypted_connections: Dict[str, Dict[str, Any]]
    ) -> None:
        """重新加密所有连接配置

        Args:
            config: 配置字典
            decrypted_connections: 解密后的连接配置字典
        """

        re_encrypted_connections = {}
        for name, decrypted_config in decrypted_connections.items():
            # 使用统一的加密方法
            encrypted_config = self.encrypt_dict_values(decrypted_config)
            re_encrypted_connections[name] = encrypted_config

        config["connections"] = re_encrypted_connections
