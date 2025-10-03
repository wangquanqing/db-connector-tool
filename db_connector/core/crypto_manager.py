"""
加密管理模块
使用 cryptography.fernet 进行对称加密
"""

import os
import base64
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from ..exceptions import CryptoError
import logging

logger = logging.getLogger(__name__)

class CryptoManager:
    """加密管理器"""
    
    def __init__(self, password: str = None, salt: bytes = None):
        """
        初始化加密管理器
        
        Args:
            password: 加密密码，如果为None则自动生成
            salt: 盐值，如果为None则自动生成
        """
        self.password = password or self._generate_password()
        self.salt = salt or os.urandom(16)
        self.fernet = self._create_fernet()
        
    def _generate_password(self) -> str:
        """生成随机密码"""
        return base64.urlsafe_b64encode(os.urandom(32)).decode('utf-8')
    
    def _create_fernet(self) -> Fernet:
        """创建Fernet加密实例"""
        try:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=self.salt,
                iterations=480000,
            )
            key = base64.urlsafe_b64encode(kdf.derive(self.password.encode()))
            return Fernet(key)
        except Exception as e:
            logger.error(f"创建Fernet实例失败: {str(e)}")
            raise CryptoError(f"加密系统初始化失败: {str(e)}")
    
    def encrypt(self, data: str) -> str:
        """
        加密数据
        
        Args:
            data: 要加密的字符串数据
            
        Returns:
            加密后的字符串
        """
        try:
            if not data:
                return ""
            encrypted_data = self.fernet.encrypt(data.encode('utf-8'))
            return base64.urlsafe_b64encode(encrypted_data).decode('utf-8')
        except Exception as e:
            logger.error(f"数据加密失败: {str(e)}")
            raise CryptoError(f"加密失败: {str(e)}")
    
    def decrypt(self, encrypted_data: str) -> str:
        """
        解密数据
        
        Args:
            encrypted_data: 加密的字符串数据
            
        Returns:
            解密后的原始字符串
        """
        try:
            if not encrypted_data:
                return ""
            encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
            decrypted_data = self.fernet.decrypt(encrypted_bytes)
            return decrypted_data.decode('utf-8')
        except Exception as e:
            logger.error(f"数据解密失败: {str(e)}")
            raise CryptoError(f"解密失败: {str(e)}")
    
    def get_key_info(self) -> dict:
        """获取密钥信息（用于持久化存储）"""
        return {
            'salt': base64.urlsafe_b64encode(self.salt).decode('utf-8'),
            'password': self.password
        }
    
    @classmethod
    def from_saved_key(cls, password: str, salt: str):
        """
        从保存的密钥信息创建实例
        
        Args:
            password: 密码
            salt: base64编码的盐值
        """
        salt_bytes = base64.urlsafe_b64decode(salt.encode('utf-8'))
        return cls(password, salt_bytes)
