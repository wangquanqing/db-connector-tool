"""
加密管理模块

使用 cryptography.fernet 进行对称加密，提供安全的密码管理和数据加密功能。
支持密码派生、盐值生成、数据加密解密等操作。
"""

import base64
import logging
import secrets
from typing import Any, Dict, Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from .exceptions import CryptoError

logger = logging.getLogger(__name__)


class CryptoManager:
    """
    加密管理器类

    提供基于Fernet的对称加密功能，使用PBKDF2进行密钥派生。
    支持密码管理、数据加密解密、密钥持久化等功能。
    """

    # 默认加密参数
    DEFAULT_SALT_LENGTH = 16
    DEFAULT_PASSWORD_LENGTH = 32
    DEFAULT_ITERATIONS = 480000  # OWASP推荐的迭代次数

    def __init__(self, password: Optional[str] = None, salt: Optional[bytes] = None):
        """
        初始化加密管理器

        Args:
            password: 加密密码，如果为None则自动生成安全的随机密码
            salt: 盐值，如果为None则自动生成安全的随机盐值

        Raises:
            CryptoError: 当加密系统初始化失败时

        Example:
            >>> # 使用自动生成的密码和盐值
            >>> crypto = CryptoManager()
            >>>
            >>> # 使用自定义密码和盐值
            >>> crypto = CryptoManager("my_secret_password", b"custom_salt")
        """
        try:
            self.password = password or self._generate_secure_password()
            self.salt = salt or self._generate_secure_salt()
            self.fernet = self._create_fernet_instance()

            logger.info("加密管理器初始化成功")

        except Exception as e:
            logger.error(f"加密管理器初始化失败: {str(e)}")
            raise CryptoError(f"加密系统初始化失败: {str(e)}")

    def _generate_secure_password(self) -> str:
        """
        生成安全的随机密码

        Returns:
            str: base64编码的安全随机密码

        Note:
            使用secrets模块生成密码，比random模块更安全
        """
        random_bytes = secrets.token_bytes(self.DEFAULT_PASSWORD_LENGTH)
        return base64.urlsafe_b64encode(random_bytes).decode("utf-8")

    def _generate_secure_salt(self) -> bytes:
        """
        生成安全的随机盐值

        Returns:
            bytes: 安全的随机盐值

        Note:
            使用secrets模块生成盐值，确保密码学安全性
        """
        return secrets.token_bytes(self.DEFAULT_SALT_LENGTH)

    def _create_fernet_instance(self) -> Fernet:
        """
        创建Fernet加密实例

        Returns:
            Fernet: 配置好的Fernet实例

        Raises:
            CryptoError: 当密钥派生或Fernet实例创建失败时
        """
        try:
            # 使用PBKDF2进行密钥派生
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=self.salt,
                iterations=self.DEFAULT_ITERATIONS,
                backend=default_backend(),
            )

            # 派生密钥
            key_material = kdf.derive(self.password.encode("utf-8"))
            key = base64.urlsafe_b64encode(key_material)

            return Fernet(key)

        except Exception as e:
            logger.error(f"Fernet实例创建失败: {str(e)}")
            raise CryptoError(f"加密密钥派生失败: {str(e)}")

    def encrypt(self, data: str) -> str:
        """
        加密字符串数据

        Args:
            data: 要加密的明文字符串数据

        Returns:
            str: base64编码的加密数据

        Raises:
            CryptoError: 当加密过程失败时
            ValueError: 当输入数据为空或无效时

        Example:
            >>> crypto = CryptoManager()
            >>> encrypted = crypto.encrypt("sensitive_data")
            >>> print(encrypted)
            "gAAAAABf..."
        """
        if not data or not isinstance(data, str):
            raise ValueError("加密数据不能为空且必须是字符串")

        try:
            # 转换为字节并加密
            encrypted_bytes = self.fernet.encrypt(data.encode("utf-8"))
            # 返回base64编码的字符串
            return base64.urlsafe_b64encode(encrypted_bytes).decode("utf-8")

        except Exception as e:
            logger.error(f"数据加密失败: {str(e)}")
            raise CryptoError(f"加密失败: {str(e)}")

    def decrypt(self, encrypted_data: str) -> str:
        """
        解密加密数据

        Args:
            encrypted_data: base64编码的加密字符串数据

        Returns:
            str: 解密后的原始明文字符串

        Raises:
            CryptoError: 当解密过程失败时
            InvalidToken: 当加密数据被篡改或密钥不匹配时
            ValueError: 当输入数据为空或格式无效时

        Example:
            >>> crypto = CryptoManager()
            >>> decrypted = crypto.decrypt("gAAAAABf...")
            >>> print(decrypted)
            "sensitive_data"
        """
        if not encrypted_data or not isinstance(encrypted_data, str):
            raise ValueError("加密数据不能为空且必须是字符串")

        try:
            # 解码base64并解密
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode("utf-8"))
            decrypted_bytes = self.fernet.decrypt(encrypted_bytes)
            return decrypted_bytes.decode("utf-8")

        except InvalidToken as e:
            logger.error(f"解密令牌无效: {str(e)}")
            raise CryptoError("解密失败: 加密数据可能被篡改或密钥不匹配")
        except Exception as e:
            logger.error(f"数据解密失败: {str(e)}")
            raise CryptoError(f"解密失败: {str(e)}")

    def encrypt_bytes(self, data: bytes) -> bytes:
        """
        加密字节数据

        Args:
            data: 要加密的字节数据

        Returns:
            bytes: 加密后的字节数据
        """
        if not data or not isinstance(data, bytes):
            raise ValueError("加密数据不能为空且必须是字节")

        try:
            return self.fernet.encrypt(data)
        except Exception as e:
            logger.error(f"字节数据加密失败: {str(e)}")
            raise CryptoError(f"字节数据加密失败: {str(e)}")

    def decrypt_bytes(self, encrypted_data: bytes) -> bytes:
        """
        解密字节数据

        Args:
            encrypted_data: 加密的字节数据

        Returns:
            bytes: 解密后的原始字节数据
        """
        if not encrypted_data or not isinstance(encrypted_data, bytes):
            raise ValueError("加密数据不能为空且必须是字节")

        try:
            return self.fernet.decrypt(encrypted_data)
        except InvalidToken as e:
            logger.error(f"字节解密令牌无效: {str(e)}")
            raise CryptoError("字节解密失败: 加密数据可能被篡改或密钥不匹配")
        except Exception as e:
            logger.error(f"字节数据解密失败: {str(e)}")
            raise CryptoError(f"字节数据解密失败: {str(e)}")

    def get_key_info(self) -> Dict[str, Any]:
        """
        获取密钥信息（用于持久化存储）

        Returns:
            Dict[str, Any]: 包含密码和盐值的字典

        Warning:
            密钥信息应安全存储，避免泄露

        Example:
            >>> key_info = crypto.get_key_info()
            >>> print(key_info)
            {
                'salt': 'abc123...',
                'password': 'def456...',
                'iterations': 480000
            }
        """
        return {
            "salt": base64.urlsafe_b64encode(self.salt).decode("utf-8"),
            "password": self.password,
            "iterations": self.DEFAULT_ITERATIONS,
        }

    @classmethod
    def from_saved_key(cls, password: str, salt: str) -> "CryptoManager":
        """
        从保存的密钥信息创建加密管理器实例

        Args:
            password: 之前保存的密码
            salt: base64编码的盐值字符串

        Returns:
            CryptoManager: 新的加密管理器实例

        Raises:
            ValueError: 当密码或盐值格式无效时

        Example:
            >>> crypto = CryptoManager.from_saved_key("saved_password", "saved_salt_base64")
        """
        if not password or not salt:
            raise ValueError("密码和盐值不能为空")

        try:
            salt_bytes = base64.urlsafe_b64decode(salt.encode("utf-8"))
            return cls(password, salt_bytes)
        except Exception as e:
            logger.error(f"从保存的密钥创建实例失败: {str(e)}")
            raise CryptoError(f"密钥恢复失败: {str(e)}")

    def verify_encryption(self, test_data: str = "test") -> bool:
        """
        验证加密解密功能是否正常工作

        Args:
            test_data: 测试用的数据

        Returns:
            bool: 加密解密功能是否正常

        Example:
            >>> is_working = crypto.verify_encryption()
            >>> print(is_working)
            True
        """
        try:
            encrypted = self.encrypt(test_data)
            decrypted = self.decrypt(encrypted)
            return decrypted == test_data
        except Exception as e:
            logger.warning(f"加密验证失败: {str(e)}")
            return False

    def __str__(self) -> str:
        """返回加密管理器的字符串表示"""
        return f"CryptoManager(salt_length={len(self.salt)}, password_length={len(self.password)})"

    def __repr__(self) -> str:
        """返回加密管理器的详细表示"""
        return "CryptoManager(password='***', salt=b'...')"
