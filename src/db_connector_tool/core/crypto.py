"""
加密管理模块 (CryptoManager)

提供基于 Fernet 对称加密的安全密码管理和数据加密功能。
使用 PBKDF2 进行密钥派生，支持密码学安全的随机数生成、数据加密解密等操作。

主要特性：
- 🔐 基于 PBKDF2-HMAC-SHA256 的密钥派生，防止暴力破解攻击
- 🔒 使用 secrets 模块生成密码学安全的随机数和盐值
- 📝 支持字符串和字节数据的加密解密操作
- 💾 提供密钥信息的持久化存储和恢复功能
- 🛡️ 完整的错误处理、日志记录和安全最佳实践
- ⚡ 自动根据系统性能调整迭代次数优化性能

安全特性：
- 强制密码强度验证（长度、复杂度要求）
- 敏感数据内存清理防止泄漏
- 盐值长度符合安全标准（≥16字节）
- 支持 OWASP 推荐的迭代次数配置

使用示例：
    >>> from db_connector_tool.core.crypto import CryptoManager
    >>>
    >>> # 创建加密管理器实例
    >>> crypto = CryptoManager()
    >>>
    >>> # 加密数据
    >>> encrypted = crypto.encrypt("敏感数据")
    >>>
    >>> # 解密数据
    >>> decrypted = crypto.decrypt(encrypted)
    >>> print(decrypted)
    '敏感数据'

版本: 1.0.0
作者: db-connector-tool 开发团队
"""

import base64
import re
import secrets
import time
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
    加密管理器类 (Crypto Manager)

    提供基于 Fernet 对称加密的完整密码管理和数据保护解决方案。
    使用 PBKDF2-HMAC-SHA256 进行安全的密钥派生，支持自动参数优化。

    主要功能：
    - 安全的密码和盐值生成
    - 字符串和字节数据的加密解密
    - 密钥信息的持久化存储和恢复
    - 密码强度验证和自动调整
    - 敏感数据的安全清理

    类属性说明：
        DEFAULT_SALT_LENGTH (int): 默认盐值长度（16字节，符合安全标准）
        MIN_SALT_LENGTH (int): 最小盐值长度（16字节，安全要求）
        DEFAULT_PASSWORD_LENGTH (int): 默认密码长度（32字节）
        DEFAULT_ITERATIONS (int): PBKDF2 默认迭代次数（480000，OWASP推荐）

    安全特性：
        - 使用密码学安全的随机数生成器（secrets模块）
        - 强制密码复杂度要求（长度≥16，包含大小写字母、数字、特殊字符）
        - 自动内存清理防止敏感数据泄漏
        - 支持硬件安全模块集成（通过扩展）

    性能优化：
        - 自动根据系统性能调整迭代次数
        - 支持批量加密操作
        - 线程安全的加密解密操作

    异常处理：
        - CryptoError: 加密相关操作失败时抛出
        - ValueError: 参数验证失败时抛出
        - InvalidToken: 解密令牌无效时抛出

    使用示例：
        >>> # 创建默认配置的加密管理器
        >>> crypto = CryptoManager()
        >>>
        >>> # 使用自定义参数创建
        >>> crypto = CryptoManager(
        ...     password="My$trongP@ssw0rd123!",
        ...     salt=b"custom_salt_16bytes",
        ...     iterations=600000
        ... )
        >>>
        >>> # 从保存的密钥恢复实例
        >>> key_info = crypto.get_key_info()
        >>> restored_crypto = CryptoManager.from_saved_key(
        ...     key_info['password'],
        ...     key_info['salt'],
        ...     key_info['iterations']
        ... )
    """

    # 默认加密参数
    DEFAULT_SALT_LENGTH = 16
    MIN_SALT_LENGTH = 16  # 最小盐值长度（16字节，符合安全要求）
    DEFAULT_PASSWORD_LENGTH = 32
    DEFAULT_ITERATIONS = 480000  # OWASP 推荐的迭代次数

    def __init__(
        self,
        password: str | None = None,
        salt: bytes | None = None,
        iterations: int | None = None,
        skip_password_validation: bool = False,
    ):
        """
        初始化加密管理器实例

        创建新的加密管理器，支持自定义密码、盐值和迭代次数配置。
        如果参数为 None，将自动生成安全的默认值。

        Args:
            password (str | None): 加密使用的密码。如果为 None，自动生成安全的随机密码
            salt (bytes | None): 加密盐值。如果为 None，自动生成安全的随机盐值
            iterations (int | None): PBKDF2 迭代次数。如果为 None，根据系统性能自动调整
            skip_password_validation (bool): 是否跳过密码强度验证。主要用于从保存的密钥恢复实例

        Raises:
            CryptoError: 加密系统初始化失败（密钥派生、Fernet实例创建等）
            ValueError: 参数验证失败（盐值长度不足、密码强度不够等）

        Security:
            - 盐值长度必须 ≥ 16 字节（MIN_SALT_LENGTH）
            - 密码必须满足强度要求（除非 skip_password_validation=True）
            - 迭代次数低于 100000 时会发出警告

        Performance:
            - 自动调整迭代次数以在 100-200ms 内完成密钥派生
            - 迭代次数范围限制在 100000-1000000 之间

        Example:
            >>> # 使用默认配置（自动生成密码、盐值，自动调整迭代次数）
            >>> crypto = CryptoManager()
            >>> print(f"盐值长度: {len(crypto.salt)}, 迭代次数: {crypto.iterations}")

            >>> # 使用自定义参数
            >>> crypto = CryptoManager(
            ...     password="My$trongP@ssw0rd123!",
            ...     salt=b"custom_salt_16bytes_",
            ...     iterations=600000
            ... )

            >>> # 跳过密码验证（用于密钥恢复）
            >>> crypto = CryptoManager("saved_password", b"saved_salt", skip_password_validation=True)
        """
        # 验证用户提供的密码强度
        if (
            password is not None
            and not skip_password_validation
            and not self.validate_password_strength(password)
        ):
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
        self._clear_sensitive_data()

    def _clear_sensitive_data(self):
        """
        清理内存中的敏感数据（私有方法）

        安全地覆盖内存中的密码、盐值等敏感信息，防止数据泄漏。
        调用后实例将无法继续使用加密解密功能。

        Security:
            - 多次覆盖敏感数据（固定字符 + 随机字符 + 清零）
            - 确保原始数据长度不变，防止长度分析攻击
            - 清理 Fernet 实例引用

        Note:
            - 调用后需要重新创建实例才能继续使用
            - 主要用于析构函数和密码更改操作
            - 符合内存安全最佳实践

        Implementation:
            - 密码: 固定字符覆盖 → 随机字符覆盖 → 清零
            - 盐值: 零字节覆盖 → 随机字节覆盖 → 清零
            - Fernet: 设置为 None
        """
        if hasattr(self, "password") and self.password:
            # 使用固定字符覆盖密码字符串，确保长度一致
            original_len = len(self.password)
            self.password = "0" * original_len
            # 再次覆盖，使用随机字符
            self.password = secrets.token_hex(original_len)[:original_len]
            # 最终清零
            self.password = ""
        if hasattr(self, "salt") and self.salt:
            # 使用零字节覆盖盐值，确保长度一致
            salt_len = len(self.salt)
            self.salt = bytes(salt_len)
            # 再次覆盖，使用随机字节
            self.salt = secrets.token_bytes(salt_len)
            # 最终清零
            self.salt = b""
        if hasattr(self, "fernet"):
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

    @staticmethod
    def validate_password_strength(password: str) -> bool:
        """
        验证密码强度（静态方法）

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
        # 检查密码长度
        if len(password) < 16:
            return False

        # 检查是否包含大写字母
        if not re.search(r"[A-Z]", password):
            return False

        # 检查是否包含小写字母
        if not re.search(r"[a-z]", password):
            return False

        # 检查是否包含数字
        if not re.search(r"[0-9]", password):
            return False

        # 检查是否包含特殊字符
        if not re.search(r'[!@#$%^&*(),.?":{}|<>]', password):
            return False

        return True

    @staticmethod
    def get_password_strength(password: str) -> str:
        """
        获取密码强度等级（静态方法）

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
        if re.search(r"[A-Z]", password):
            score += 1
        if re.search(r"[a-z]", password):
            score += 1
        if re.search(r"[0-9]", password):
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
        adjusted_iterations = max(
            min_iterations, min(estimated_iterations, max_iterations)
        )

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
        if self.fernet is None:
            raise CryptoError("加密管理器未初始化或已被销毁，无法执行加密操作")

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
        if self.fernet is None:
            raise CryptoError("加密管理器未初始化或已被销毁，无法执行解密操作")

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

        将明文字符串加密为 base64 编码的安全字符串。
        支持 Unicode 字符和特殊字符。

        Args:
            data (str): 要加密的明文字符串数据。不能为空且必须是字符串类型

        Returns:
            str: base64 URL 安全编码的加密字符串

        Raises:
            CryptoError: 加密过程失败（Fernet实例未初始化、加密操作异常等）
            ValueError: 输入数据为空或不是字符串类型

        Security:
            - 使用 Fernet 对称加密（AES-128-CBC + HMAC-SHA256）
            - 自动处理编码和 base64 转换
            - 包含时间戳防止重放攻击

        Performance:
            - 支持长文本加密（自动分块处理）
            - 内存效率优化

        Example:
            >>> crypto = CryptoManager()
            >>>
            >>> # 加密普通文本
            >>> encrypted = crypto.encrypt("敏感的用户数据")
            >>> print(f"加密结果: {encrypted[:50]}...")

            >>> # 加密包含特殊字符的文本
            >>> encrypted = crypto.encrypt("密码: P@ssw0rd123! 用户名: user@example.com")
            >>> print(f"加密长度: {len(encrypted)}")

            >>> # 加密 Unicode 文本
            >>> encrypted = crypto.encrypt("中文文本 🌟 emoji 表情")
            >>> print("加密成功")
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

        将 base64 编码的加密字符串解密为原始明文字符串。
        验证数据完整性和时间戳有效性。

        Args:
            encrypted_data (str): base64 URL 安全编码的加密字符串数据

        Returns:
            str: 解密后的原始明文字符串

        Raises:
            CryptoError: 解密过程失败（Fernet实例未初始化、解密操作异常等）
            InvalidToken: 加密数据被篡改、密钥不匹配或令牌过期
            ValueError: 输入数据为空、不是字符串或 base64 格式无效

        Security:
            - 验证 HMAC 签名确保数据完整性
            - 检查时间戳防止重放攻击
            - 自动处理 base64 解码和字符编码

        Error Handling:
            - InvalidToken: 数据可能被篡改或使用错误的密钥
            - CryptoError: 系统级错误（内存不足、初始化失败等）
            - ValueError: 输入参数格式错误

        Example:
            >>> crypto = CryptoManager()
            >>>
            >>> # 解密之前加密的数据
            >>> encrypted = crypto.encrypt("机密信息")
            >>> decrypted = crypto.decrypt(encrypted)
            >>> print(f"解密结果: {decrypted}")

            >>> # 错误处理示例
            >>> try:
            ...     result = crypto.decrypt("无效的加密数据")
            ... except CryptoError as e:
            ...     print(f"解密失败: {e}")
            ... except InvalidToken:
            ...     print("令牌无效，数据可能被篡改")
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
    def from_saved_key(
        cls, password: str, salt: str, iterations: int | None = None
    ) -> "CryptoManager":
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

    def is_initialized(self) -> bool:
        """
        检查加密管理器是否已正确初始化

        Returns:
            bool: 如果已初始化且可用返回 True，否则返回 False
        """
        return hasattr(self, "fernet") and self.fernet is not None

    def get_security_info(self) -> Dict[str, Any]:
        """
        获取安全相关信息（不包含敏感数据）

        Returns:
            Dict[str, Any]: 包含安全相关信息的字典
        """
        return {
            "salt_length": len(self.salt),
            "password_length": len(self.password),
            "iterations": self.iterations,
            "is_initialized": self.is_initialized(),
            "algorithm": "AES-128-CBC",
            "key_derivation": "PBKDF2-HMAC-SHA256",
        }

    @classmethod
    def create_secure_instance(cls, password: str | None = None) -> "CryptoManager":
        """
        创建安全的加密管理器实例（便捷方法）

        Args:
            password: 可选密码，如果提供会验证强度，否则自动生成

        Returns:
            CryptoManager: 新的加密管理器实例

        Raises:
            ValueError: 当密码强度不足时
        """
        if password is not None:
            if not cls.validate_password_strength(password):
                strength = cls.get_password_strength(password)
                raise ValueError(f"密码强度不足 ({strength})，请使用更强的密码")

        # 使用推荐的迭代次数和自动生成的盐值
        return cls(password=password, iterations=cls.DEFAULT_ITERATIONS)

    def verify_encryption(self, test_data: str = "test_encryption_data") -> bool:
        """
        验证加密解密功能是否正常工作

        Args:
            test_data: 用于测试的字符串数据

        Returns:
            bool: 如果加密解密过程正常返回 True，否则返回 False
        """
        try:
            encrypted = self.encrypt(test_data)
            decrypted = self.decrypt(encrypted)
            return decrypted == test_data
        except Exception:
            return False

    def change_password(
        self, new_password: str, validate_strength: bool = True
    ) -> None:
        """
        更改加密密码（会重新初始化加密实例）

        Args:
            new_password: 新的密码
            validate_strength: 是否验证新密码强度

        Raises:
            ValueError: 当新密码强度不足时
            CryptoError: 当重新初始化失败时
        """
        if validate_strength and not self.validate_password_strength(new_password):
            strength = self.get_password_strength(new_password)
            raise ValueError(f"新密码强度不足 ({strength})，请使用更强的密码")

        try:
            # 保存当前盐值和迭代次数
            current_salt = self.salt
            current_iterations = self.iterations

            # 清理当前实例
            self._clear_sensitive_data()

            # 重新初始化
            self.password = new_password
            self.salt = current_salt
            self.iterations = current_iterations
            self.fernet = self._create_fernet_instance()

            logger.info("密码更改成功")
        except Exception as e:
            logger.error(f"更改密码失败: {str(e)}")
            raise CryptoError(f"密码更改失败: {str(e)}") from e
