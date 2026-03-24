"""
加密管理模块

使用 cryptography.fernet 进行对称加密，提供安全的密码管理和数据加密功能。
支持密码派生、盐值生成、数据加密解密等操作。

特性：
- 基于 PBKDF2 的密钥派生，防止暴力破解
- 使用 secrets 模块生成密码学安全的随机数
- 支持字符串和字节数据的加密解密
- 提供密钥持久化和恢复功能
- 完整的错误处理和日志记录
"""

import base64
import secrets
from typing import Any, Dict

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ..utils.logging_utils import get_logger
from .exceptions import CryptoError

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class CryptoManager:
    """
    加密管理器类

    提供基于 Fernet 的对称加密功能，使用 PBKDF2 进行密钥派生。
    支持密码管理、数据加密解密、密钥持久化等功能。

    Attributes:
        DEFAULT_SALT_LENGTH (int): 默认盐值长度（16字节）
        DEFAULT_PASSWORD_LENGTH (int): 默认密码长度（32字节）
        DEFAULT_ITERATIONS (int): PBKDF2 迭代次数（480000次，符合OWASP推荐）
    """

    # 默认加密参数
    DEFAULT_SALT_LENGTH = 16
    MIN_SALT_LENGTH = 16  # 最小盐值长度（16字节，符合安全要求）
    DEFAULT_PASSWORD_LENGTH = 32
    DEFAULT_ITERATIONS = 480000  # OWASP 推荐的迭代次数

    def __init__(self, password: str | None = None, salt: bytes | None = None, iterations: int | None = None, skip_password_validation: bool = False):
        """
        初始化加密管理器

        Args:
            password: 加密密码，如果为 None 则自动生成安全的随机密码
            salt: 盐值，如果为 None 则自动生成安全的随机盐值
            iterations: PBKDF2 迭代次数，如果为 None 则根据系统性能自动调整
            skip_password_validation: 是否跳过密码强度验证，用于从保存的密钥创建实例时

        Raises:
            CryptoError: 当加密系统初始化失败时
            ValueError: 当盐值长度不符合安全要求或密码强度不足时

        Example:
            >>> # 使用自动生成的密码和盐值，自动调整迭代次数
            >>> crypto = CryptoManager()
            >>>
            >>> # 使用自定义密码、盐值和迭代次数
            >>> crypto = CryptoManager("My$trongP@ssw0rd123", b"custom_salt", 600000)
        """
        # 验证用户提供的密码强度
        if password is not None and not skip_password_validation and not self.validate_password_strength(password):
            strength = self.get_password_strength(password)
            raise ValueError(f"密码强度不足 ({strength})，请使用更强的密码")
            
        # 验证盐值长度
        if salt is not None and len(salt) < self.MIN_SALT_LENGTH:
            raise ValueError(f"盐值长度必须至少为 {self.MIN_SALT_LENGTH} 字节")
            
        try:
            self.password = password or self._generate_secure_password()
            self.salt = salt or self._generate_secure_salt()
            
            # 确定迭代次数
            if iterations is not None:
                if iterations < 100000:
                    logger.warning(f"迭代次数 {iterations} 过低，建议至少 100000 次")
                self.iterations = iterations
            else:
                # 根据系统性能自动调整迭代次数
                self.iterations = self._auto_adjust_iterations()
                
            self.fernet = self._create_fernet_instance()

            logger.debug(
                f"加密管理器初始化成功，盐值长度: {len(self.salt)}, 密码长度: {len(self.password)}, 迭代次数: {self.iterations}"
            )

        except Exception as e:
            logger.error(f"初始化加密管理器失败: {str(e)}")
            raise CryptoError(f"加密系统初始化失败: {str(e)}") from e

    def __del__(self):
        """
        析构函数，确保敏感数据在对象销毁时被清理
        """
        self.clear_sensitive_data()

    def clear_sensitive_data(self):
        """
        清理内存中的敏感数据

        Security Note:
            - 覆盖密码和盐值，防止内存泄漏
            - 调用后需要重新初始化才能使用
        """
        import secrets
        if hasattr(self, 'password') and self.password:
            # 使用固定字符覆盖密码字符串，确保长度一致
            original_len = len(self.password)
            self.password = '0' * original_len
            # 再次覆盖，使用随机字符
            self.password = secrets.token_hex(original_len)[:original_len]
            # 最终清零
            self.password = ''
        if hasattr(self, 'salt') and self.salt:
            # 使用零字节覆盖盐值，确保长度一致
            salt_len = len(self.salt)
            self.salt = bytes(salt_len)
            # 再次覆盖，使用随机字节
            self.salt = secrets.token_bytes(salt_len)
            # 最终清零
            self.salt = b''
        if hasattr(self, 'fernet'):
            # 清除 Fernet 实例
            self.fernet = None
        logger.debug("敏感数据已清理")

    def _generate_secure_password(self, max_attempts: int = 10) -> str:
        """
        生成安全的随机密码

        Args:
            max_attempts: 最大尝试次数，防止无限递归

        Returns:
            str: base64 编码的安全随机密码

        Note:
            使用 secrets 模块生成密码，比 random 模块更安全，适用于密码学场景
        """
        random_bytes = secrets.token_bytes(self.DEFAULT_PASSWORD_LENGTH)
        password = base64.urlsafe_b64encode(random_bytes).decode("utf-8")
        # 验证生成的密码强度
        if not self.validate_password_strength(password) and max_attempts > 0:
            # 如果密码强度不足，重新生成
            logger.warning("生成的密码强度不足，重新生成")
            return self._generate_secure_password(max_attempts - 1)
        elif not self.validate_password_strength(password):
            # 如果达到最大尝试次数，使用当前密码
            logger.warning("达到最大尝试次数，使用当前密码")
        return password

    def validate_password_strength(self, password: str) -> bool:
        """
        验证密码强度

        Args:
            password: 要验证的密码

        Returns:
            bool: 密码强度是否足够

        Password Strength Requirements:
            - 长度至少为 16 字符
            - 包含至少 1 个大写字母
            - 包含至少 1 个小写字母
            - 包含至少 1 个数字
            - 包含至少 1 个特殊字符
        """
        import re
        
        # 检查密码长度
        if len(password) < 16:
            return False
        
        # 检查是否包含大写字母
        if not re.search(r'[A-Z]', password):
            return False
        
        # 检查是否包含小写字母
        if not re.search(r'[a-z]', password):
            return False
        
        # 检查是否包含数字
        if not re.search(r'[0-9]', password):
            return False
        
        # 检查是否包含特殊字符
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False
        
        return True

    def get_password_strength(self, password: str) -> str:
        """
        获取密码强度等级

        Args:
            password: 要评估的密码

        Returns:
            str: 密码强度等级 (weak, medium, strong, very_strong)
        """
        score = 0
        
        # 长度得分
        if len(password) >= 24:
            score += 3
        elif len(password) >= 16:
            score += 2
        elif len(password) >= 8:
            score += 1
        
        # 复杂度得分
        import re
        if re.search(r'[A-Z]', password):
            score += 1
        if re.search(r'[a-z]', password):
            score += 1
        if re.search(r'[0-9]', password):
            score += 1
        if re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            score += 1
        
        # 评估强度等级
        if score >= 7:
            return "very_strong"
        elif score >= 5:
            return "strong"
        elif score >= 3:
            return "medium"
        else:
            return "weak"

    def _generate_secure_salt(self) -> bytes:
        """
        生成安全的随机盐值

        Returns:
            bytes: 安全的随机盐值

        Note:
            使用 secrets 模块生成盐值，确保密码学安全性
        """
        return secrets.token_bytes(self.DEFAULT_SALT_LENGTH)

    def _auto_adjust_iterations(self) -> int:
        """
        根据系统性能自动调整 PBKDF2 迭代次数

        Returns:
            int: 适合当前系统性能的迭代次数

        Process:
            1. 测试不同迭代次数的执行时间
            2. 目标是找到一个在 100-200ms 内完成的迭代次数
            3. 确保最低迭代次数不低于 100000
            4. 最高迭代次数不超过 1000000
        """
        import time
        
        # 测试基础迭代次数的执行时间
        test_iterations = 100000
        test_password = "test_password"
        test_salt = b"test_salt_123456"
        
        start_time = time.time()
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=test_salt,
            iterations=test_iterations,
            backend=default_backend(),
        )
        kdf.derive(test_password.encode("utf-8"))
        elapsed_time = time.time() - start_time
        
        # 计算目标迭代次数（目标执行时间 150ms）
        target_time = 0.15  # 150ms
        estimated_iterations = int((target_time / elapsed_time) * test_iterations)
        
        # 确保迭代次数在合理范围内
        min_iterations = 100000
        max_iterations = 1000000
        adjusted_iterations = max(min_iterations, min(estimated_iterations, max_iterations))
        
        # 调整为 10000 的倍数，使数值更整洁
        adjusted_iterations = (adjusted_iterations // 10000) * 10000
        
        logger.debug(
            f"自动调整迭代次数: 测试时间={elapsed_time:.3f}s, 估计次数={estimated_iterations}, 调整后次数={adjusted_iterations}"
        )
        
        return adjusted_iterations

    def _create_fernet_instance(self) -> Fernet:
        """
        创建 Fernet 加密实例

        Returns:
            Fernet: 配置好的 Fernet 实例

        Raises:
            CryptoError: 当密钥派生或 Fernet 实例创建失败时

        Process:
            1. 使用 PBKDF2 从密码和盐值派生密钥
            2. 将派生的密钥编码为 base64 格式
            3. 创建 Fernet 实例
        """
        try:
            # 使用 PBKDF2 进行密钥派生
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=self.salt,
                iterations=self.iterations,
                backend=default_backend(),
            )

            # 派生密钥
            key_material = kdf.derive(self.password.encode("utf-8"))
            key = base64.urlsafe_b64encode(key_material)

            return Fernet(key)

        except Exception as e:
            logger.error(f"Fernet 实例创建失败: {str(e)}")
            raise CryptoError(f"加密密钥派生失败: {str(e)}") from e

    def _encrypt(self, data: bytes) -> bytes:
        """
        通用加密方法，处理字节数据

        Args:
            data: 要加密的字节数据

        Returns:
            bytes: 加密后的字节数据

        Raises:
            CryptoError: 当加密过程失败时
        """
        try:
            return self.fernet.encrypt(data)
        except Exception as e:
            logger.error(f"数据加密失败: {str(e)}")
            raise CryptoError(f"加密失败: {str(e)}") from e

    def _decrypt(self, encrypted_data: bytes) -> bytes:
        """
        通用解密方法，处理字节数据

        Args:
            encrypted_data: 加密的字节数据

        Returns:
            bytes: 解密后的原始字节数据

        Raises:
            CryptoError: 当解密过程失败时
            InvalidToken: 当加密数据被篡改或密钥不匹配时
        """
        try:
            return self.fernet.decrypt(encrypted_data)
        except InvalidToken as e:
            logger.error(f"解密令牌无效: {str(e)}")
            raise CryptoError("解密失败: 加密数据可能被篡改或密钥不匹配") from e
        except Exception as e:
            logger.error(f"数据解密失败: {str(e)}")
            raise CryptoError(f"解密失败: {str(e)}") from e

    def encrypt(self, data: str) -> str:
        """
        加密字符串数据

        Args:
            data: 要加密的明文字符串数据

        Returns:
            str: base64 编码的加密数据

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

        # 转换为字节并加密
        encrypted_bytes = self._encrypt(data.encode("utf-8"))
        # 返回 base64 编码的字符串
        return base64.urlsafe_b64encode(encrypted_bytes).decode("utf-8")

    def decrypt(self, encrypted_data: str) -> str:
        """
        解密加密数据

        Args:
            encrypted_data: base64 编码的加密字符串数据

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

        # 解码 base64 并解密
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode("utf-8"))
        decrypted_bytes = self._decrypt(encrypted_bytes)
        return decrypted_bytes.decode("utf-8")

    def encrypt_bytes(self, data: bytes) -> bytes:
        """
        加密字节数据

        Args:
            data: 要加密的字节数据

        Returns:
            bytes: 加密后的字节数据

        Raises:
            CryptoError: 当加密过程失败时
            ValueError: 当输入数据为空或无效时

        Example:
            >>> crypto = CryptoManager()
            >>> data = b"binary_data"
            >>> encrypted = crypto.encrypt_bytes(data)
        """
        if not data or not isinstance(data, bytes):
            raise ValueError("加密数据不能为空且必须是字节")

        return self._encrypt(data)

    def decrypt_bytes(self, encrypted_data: bytes) -> bytes:
        """
        解密字节数据

        Args:
            encrypted_data: 加密的字节数据

        Returns:
            bytes: 解密后的原始字节数据

        Raises:
            CryptoError: 当解密过程失败时
            InvalidToken: 当加密数据被篡改或密钥不匹配时
            ValueError: 当输入数据为空或无效时

        Example:
            >>> crypto = CryptoManager()
            >>> decrypted = crypto.decrypt_bytes(encrypted_bytes)
        """
        if not encrypted_data or not isinstance(encrypted_data, bytes):
            raise ValueError("加密数据不能为空且必须是字节")

        return self._decrypt(encrypted_data)

    def get_key_info(self) -> Dict[str, Any]:
        """
        获取密钥信息（用于持久化存储）

        Returns:
            Dict[str, Any]: 包含密码、盐值和迭代次数的字典

        Warning:
            密钥信息应安全存储，避免泄露。建议使用安全的存储机制。

        Security Note:
            - 盐值和密码都以 base64 编码形式存储
            - 实际应用中应考虑额外的安全措施（如硬件安全模块）

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
            "iterations": self.iterations,
        }

    @classmethod
    def from_saved_key(cls, password: str, salt: str, iterations: int | None = None) -> "CryptoManager":
        """
        从保存的密钥信息创建加密管理器实例

        Args:
            password: 之前保存的密码
            salt: base64 编码的盐值字符串
            iterations: 之前使用的迭代次数，如果为 None 则使用默认值

        Returns:
            CryptoManager: 新的加密管理器实例

        Raises:
            CryptoError: 当密钥恢复失败时
            ValueError: 当密码或盐值格式无效时

        Example:
            >>> crypto = CryptoManager.from_saved_key("saved_password", "saved_salt_base64", 480000)
        """
        if not password or not salt:
            raise ValueError("密码和盐值不能为空")

        try:
            salt_bytes = base64.urlsafe_b64decode(salt.encode("utf-8"))
            return cls(password, salt_bytes, iterations, skip_password_validation=True)
        except Exception as e:
            logger.error(f"从保存的密钥创建实例失败: {str(e)}")
            raise CryptoError(f"密钥恢复失败: {str(e)}") from e

    def __str__(self) -> str:
        """返回加密管理器的用户友好字符串表示"""
        return f"CryptoManager(盐值长度={len(self.salt)}, 迭代次数={self.iterations})"

    def __repr__(self) -> str:
        """返回加密管理器的详细表示（安全版本）"""
        return f"<CryptoManager object at {hex(id(self))}, salt_length={len(self.salt)}, password_length={len(self.password)}, iterations={self.iterations}>"
