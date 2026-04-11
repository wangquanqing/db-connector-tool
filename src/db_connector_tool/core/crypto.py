"""加密管理模块 (CryptoManager)

提供基于 Fernet 对称加密的安全密码管理和数据加密功能，
使用 PBKDF2-HMAC-SHA256 进行密钥派生，支持密码学安全的随机数生成、
数据加密解密等操作。

Example:
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
"""

import base64
import secrets
import string
import time
from typing import Any, Dict

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from ..utils.logging_utils import get_logger
from .exceptions import CryptoError
from .validators import PasswordValidator

logger = get_logger(__name__)


class CryptoManager:
    """加密管理器类 (Crypto Manager)

    提供基于 Fernet 对称加密的完整密码管理和数据保护解决方案，
    使用 PBKDF2-HMAC-SHA256 进行安全的密钥派生，支持自动参数优化，
    支持上下文管理器模式，确保敏感数据的精确生命周期管理。

    Example:
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
    ...     key_info["password"],
    ...     key_info["salt"],
    ...     key_info["iterations"]
    ... )
    >>>
    >>> # 使用上下文管理器（推荐方式）
    >>> with CryptoManager() as crypto:
    ...     encrypted = crypto.encrypt("敏感数据")
    ...     # 退出 with 块时自动清理敏感数据
    ...
    >>> # 手动关闭（备选方式）
    >>> crypto = CryptoManager()
    >>> try:
    ...     encrypted = crypto.encrypt("敏感数据")
    ... finally:
    ...     crypto.close()  # 确保清理
    """

    # 默认加密参数
    DEFAULT_SALT_LENGTH = 16
    MIN_SALT_LENGTH = 16
    DEFAULT_PASSWORD_LENGTH = 32
    DEFAULT_ITERATIONS = 480000  # OWASP 推荐的迭代次数
    MIN_ITERATIONS = 100000

    # 特殊字符常量定义
    SPECIAL_CHARACTERS = "!@#$%^&*()_+-=[]{}|;:,.<>?~`\"'\\/"

    def __init__(
        self,
        password: str | None = None,
        salt: bytes | None = None,
        iterations: int | None = None,
        skip_password_validation: bool = False,
    ):
        """初始化加密管理器实例

        Args:
            password: 加密使用的密码。None 时自动生成安全的随机密码
            salt: 加密盐值。None 时自动生成安全的随机盐值
            iterations: PBKDF2 迭代次数。None 时根据系统性能自动调整
            skip_password_validation: 是否跳过密码强度验证，主要用于从保存的密钥恢复实例

        Raises:
            CryptoError: 加密系统初始化失败
            ValueError: 参数验证失败（盐值长度不足、密码强度不够等）

        Example:
            >>> # 默认配置
            >>> crypto = CryptoManager()

            >>> # 自定义参数
            >>> crypto = CryptoManager(
            ...     password="My$trongP@ssw0rd123!",
            ...     salt=b"custom_salt_16bytes_",
            ...     iterations=600000
            ... )

            >>> # 跳过密码验证（用于密钥恢复）
            >>> crypto = CryptoManager(
            ...     "saved_password", b"saved_salt", skip_password_validation=True
            ... )
        """

        # 验证用户提供的密码强度
        if (
            password is not None
            and not skip_password_validation
            and not PasswordValidator.validate_strength(password)
        ):
            strength = PasswordValidator.get_strength(password)
            raise ValueError(f"密码强度不足 ({strength})，请使用更强的密码")

        # 验证盐值长度
        if salt is not None and len(salt) < self.MIN_SALT_LENGTH:
            raise ValueError(f"盐值长度必须至少为 {self.MIN_SALT_LENGTH} 字节")

        try:
            self.password = password or self._generate_secure_password()
            self.salt = salt or self._generate_secure_salt()

            # 初始化清理状态属性
            self._cleaned = False

            # 确定迭代次数
            if iterations is not None:
                if iterations < self.MIN_ITERATIONS:
                    logger.warning(
                        "迭代次数 %s 过低，建议至少 %s 次",
                        iterations,
                        self.MIN_ITERATIONS,
                    )
                self.iterations = iterations
            else:
                # 根据系统性能自动调整迭代次数
                self.iterations = self._auto_adjust_iterations()

            self.fernet = self._create_fernet_instance()

            logger.info(
                "加密管理器初始化成功，盐值长度: %s, 密码长度: %s, 迭代次数: %s",
                len(self.salt),
                len(self.password),
                self.iterations,
            )

        except Exception as error:
            logger.error("初始化加密管理器失败: %s", str(error))
            raise CryptoError(f"加密系统初始化失败: {str(error)}") from error

    def __str__(self) -> str:
        """返回加密管理器的用户友好字符串表示"""

        status = "✅ 已初始化" if self.is_initialized() else "❌ 未初始化"
        return (
            f"CryptoManager({status}, 盐值:{len(self.salt)}B, 迭代:{self.iterations:,})"
        )

    def __repr__(self) -> str:
        """返回加密管理器的详细表示（安全版本）"""

        status = (
            "initialized"
            if hasattr(self, "fernet") and self.fernet
            else "uninitialized"
        )
        cleaned = "cleaned" if getattr(self, "_cleaned", False) else "active"
        return (
            f"CryptoManager(status='{status}', cleaned='{cleaned}', "
            f"salt_length={len(self.salt)}, password_length={len(self.password)}, "
            f"iterations={self.iterations})"
        )

    def __enter__(self):
        """上下文管理器入口，返回自身实例

        Returns:
            CryptoManager: 当前加密管理器实例
        """

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器退出，确保敏感数据被安全清理

        Args:
            exc_type: 异常类型（如果有异常发生）
            exc_val: 异常值（如果有异常发生）
            exc_tb: 异常回溯（如果有异常发生）
        """

        self._clear_sensitive_data()
        logger.info("加密管理器上下文已退出")

    def close(self):
        """手动关闭加密管理器，清理敏感数据"""

        self._clear_sensitive_data()
        logger.info("加密管理器已手动关闭")

    def _clear_sensitive_data(self):
        """清理内存中的敏感数据（私有方法）"""

        # 标记清理状态
        self._cleaned = True

        if hasattr(self, "password") and self.password:
            # 使用固定字符覆盖密码字符串，确保长度一致
            original_length = len(self.password)
            self.password = "0" * original_length
            # 再次覆盖，使用随机字符
            self.password = secrets.token_hex(original_length)[:original_length]
            # 最终清零
            self.password = ""
        if hasattr(self, "salt") and self.salt:
            # 使用零字节覆盖盐值，确保长度一致
            salt_length = len(self.salt)
            self.salt = bytes(salt_length)
            # 再次覆盖，使用随机字节
            self.salt = secrets.token_bytes(salt_length)
            # 最终清零
            self.salt = b""
        if hasattr(self, "fernet"):
            # 清除 Fernet 实例
            self.fernet = None
        logger.debug("敏感数据已安全清理")

    def _generate_secure_password(self, max_attempts: int = 10) -> str:
        """生成安全的随机密码

        使用密码学安全的随机数生成器创建符合强度要求的密码。
        采用更丰富的字符集提高密码熵值，使用迭代代替递归优化性能。

        Args:
            max_attempts: 最大尝试次数，防止无限循环，默认10次

        Returns:
            str: 符合安全标准的随机密码

        Security:
            - 使用 secrets 模块确保密码学安全性
            - 包含大小写字母、数字、特殊字符的丰富字符集
            - 强制满足密码强度要求

        Performance:
            - 使用迭代代替递归，避免栈溢出风险
            - 优化的字符选择算法

        Character Set (94个字符):
            - 大写字母: A-Z (26个)
            - 小写字母: a-z (26个)
            - 数字: 0-9 (10个)
            - 特殊字符: !@#$%^&*()_+-=[]{}|;:,.<>?~`\"'\\/ (32个)
        """
        # 定义丰富的字符集（94个字符）
        character_set = (
            string.ascii_uppercase  # 大写字母 A-Z (26)
            + string.ascii_lowercase  # 小写字母 a-z (26)
            + string.digits  # 数字 0-9 (10)
            + self.SPECIAL_CHARACTERS  # 特殊字符 (32)
        )

        # 使用迭代代替递归
        for attempt in range(max_attempts):
            # 方法1: 直接选择字符（更优的熵值）
            generated_password = "".join(
                secrets.choice(character_set) for _ in range(24)
            )

            # 验证密码强度
            if PasswordValidator.validate_strength(generated_password):
                logger.debug("密码生成成功，尝试次数: %s", attempt + 1)
                return generated_password

            # 方法2: 如果方法1失败，使用Base64后备方案
            if attempt == max_attempts // 2:  # 中途切换策略
                random_bytes = secrets.token_bytes(24)
                generated_password = base64.urlsafe_b64encode(random_bytes).decode(
                    "utf-8"
                )
                # 移除可能存在的填充字符
                generated_password = generated_password.rstrip("=")

                if PasswordValidator.validate_strength(generated_password):
                    logger.debug("Base64方法生成成功，尝试次数: %s", attempt + 1)
                    return generated_password

            logger.debug("密码强度不足，第%s次重新生成", attempt + 1)

        # 达到最大尝试次数，使用强制生成方法
        logger.warning("达到最大尝试次数(%s)，使用强制生成的密码", max_attempts)
        return self._generate_forced_strong_password()

    def _generate_forced_strong_password(self) -> str:
        """强制生成符合强度要求的密码

        Returns:
            str: 强制生成的符合强度要求的密码
        """

        # 确保每个类别至少有一个字符
        uppercase_char = secrets.choice(string.ascii_uppercase)
        lowercase_char = secrets.choice(string.ascii_lowercase)
        digit_char = secrets.choice(string.digits)
        special_char = secrets.choice(self.SPECIAL_CHARACTERS)

        # 生成剩余字符
        character_set = string.ascii_letters + string.digits + self.SPECIAL_CHARACTERS
        remaining_length = 20  # 总长度24 - 4个强制字符
        remaining_characters = "".join(
            secrets.choice(character_set) for _ in range(remaining_length)
        )

        # 组合所有字符并随机打乱
        all_characters = list(
            uppercase_char
            + lowercase_char
            + digit_char
            + special_char
            + remaining_characters
        )
        secrets.SystemRandom().shuffle(all_characters)

        generated_password = "".join(all_characters)
        logger.debug("使用强制方法生成密码成功")
        return generated_password

    def _generate_secure_salt(self, length: int | None = None) -> bytes:
        """生成安全的随机盐值

        Args:
            length: 盐值长度，默认为 DEFAULT_SALT_LENGTH

        Returns:
            bytes: 安全的随机盐值

        Raises:
            ValueError: 盐值长度不足
        """

        salt_length = length or self.DEFAULT_SALT_LENGTH

        if salt_length < self.MIN_SALT_LENGTH:
            raise ValueError(f"盐值长度必须至少为 {self.MIN_SALT_LENGTH} 字节")

        return secrets.token_bytes(salt_length)

    def _auto_adjust_iterations(self) -> int:
        """根据系统性能自动调整 PBKDF2 迭代次数

        Returns:
            int: 适合当前系统性能的迭代次数
        """

        # 测试基础迭代次数的执行时间
        test_iterations = self.MIN_ITERATIONS
        test_password = "test_password"
        test_salt = secrets.token_bytes(self.MIN_SALT_LENGTH)

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
            "自动调整迭代次数: 测试时间=%.3fs, 估计次数=%s, 调整后次数=%s",
            elapsed_time,
            estimated_iterations,
            adjusted_iterations,
        )

        return adjusted_iterations

    def _create_fernet_instance(self) -> Fernet:
        """创建 Fernet 加密实例

        Returns:
            Fernet: 配置好的 Fernet 实例

        Raises:
            CryptoError: 密钥派生或 Fernet 实例创建失败
        """

        try:
            # 使用 PBKDF2 进行密钥派生
            key_derivation_function = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=self.salt,
                iterations=self.iterations,
                backend=default_backend(),
            )

            # 派生密钥
            key_material = key_derivation_function.derive(self.password.encode("utf-8"))
            encoded_key = base64.urlsafe_b64encode(key_material)

            return Fernet(encoded_key)

        except Exception as error:
            logger.error("Fernet 实例创建失败: %s", str(error))
            raise CryptoError(f"加密密钥派生失败: {str(error)}") from error

    def encrypt(self, data: str) -> str:
        """加密字符串数据

        Args:
            data: 要加密的明文字符串数据，不能为空且必须是字符串类型

        Returns:
            str: base64 URL 安全编码的加密字符串

        Raises:
            CryptoError: 加密过程失败
            ValueError: 输入数据为空或不是字符串类型

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
        """解密加密数据

        Args:
            encrypted_data: base64 URL 安全编码的加密字符串数据

        Returns:
            str: 解密后的原始明文字符串

        Raises:
            CryptoError: 解密过程失败
            InvalidToken: 加密数据被篡改、密钥不匹配或令牌过期
            ValueError: 输入数据为空、不是字符串或 base64 格式无效

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
        """加密字节数据

        Args:
            data: 要加密的字节数据

        Returns:
            bytes: 加密后的字节数据

        Raises:
            CryptoError: 加密过程失败
            ValueError: 输入数据为空或无效

        Example:
            >>> crypto = CryptoManager()
            >>> data = b"binary_data"
            >>> encrypted = crypto.encrypt_bytes(data)
        """

        if not data or not isinstance(data, bytes):
            raise ValueError("加密数据不能为空且必须是字节")

        return self._encrypt(data)

    def decrypt_bytes(self, encrypted_data: bytes) -> bytes:
        """解密字节数据

        Args:
            encrypted_data: 加密的字节数据

        Returns:
            bytes: 解密后的原始字节数据

        Raises:
            CryptoError: 解密过程失败
            InvalidToken: 加密数据被篡改或密钥不匹配
            ValueError: 输入数据为空或无效

        Example:
            >>> crypto = CryptoManager()
            >>> decrypted = crypto.decrypt_bytes(encrypted_bytes)
        """

        if not encrypted_data or not isinstance(encrypted_data, bytes):
            raise ValueError("加密数据不能为空且必须是字节")

        return self._decrypt(encrypted_data)

    def _encrypt(self, data: bytes) -> bytes:
        """通用加密方法，处理字节数据

        Args:
            data: 要加密的字节数据

        Returns:
            bytes: 加密后的字节数据

        Raises:
            CryptoError: 加密过程失败
        """

        if self.fernet is None:
            raise CryptoError("加密管理器未初始化或已被销毁，无法执行加密操作")

        try:
            return self.fernet.encrypt(data)
        except Exception as error:
            logger.error("数据加密失败: %s", str(error))
            raise CryptoError(f"加密失败: {str(error)}") from error

    def _decrypt(self, encrypted_data: bytes) -> bytes:
        """通用解密方法，处理字节数据

        Args:
            encrypted_data: 加密的字节数据

        Returns:
            bytes: 解密后的原始字节数据

        Raises:
            CryptoError: 解密过程失败
            InvalidToken: 加密数据被篡改或密钥不匹配
        """

        if self.fernet is None:
            raise CryptoError("加密管理器未初始化或已被销毁，无法执行解密操作")

        try:
            return self.fernet.decrypt(encrypted_data)
        except InvalidToken as error:
            logger.error("解密令牌无效: %s", str(error))
            raise CryptoError("解密失败: 加密数据可能被篡改或密钥不匹配") from error
        except Exception as error:
            logger.error("数据解密失败: %s", str(error))
            raise CryptoError(f"解密失败: {str(error)}") from error

    def get_key_info(self) -> Dict[str, Any]:
        """获取密钥信息（用于持久化存储）

        Returns:
            Dict[str, Any]: 包含密码、盐值和迭代次数的字典

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

    def get_security_info(self) -> Dict[str, Any]:
        """获取安全相关信息（不包含敏感数据）

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

    def is_initialized(self) -> bool:
        """检查加密管理器是否已正确初始化

        Returns:
            bool: 如果已初始化且可用返回 True，否则返回 False
        """

        return hasattr(self, "fernet") and self.fernet is not None

    def verify_encryption(self, test_data: str = "test_encryption_data") -> bool:
        """验证加密解密功能是否正常工作

        Args:
            test_data: 用于测试的字符串数据

        Returns:
            bool: 如果加密解密过程正常返回 True，否则返回 False
        """

        try:
            encrypted = self.encrypt(test_data)
            decrypted = self.decrypt(encrypted)
            return decrypted == test_data
        except (CryptoError, ValueError, InvalidToken):
            # 捕获加密解密过程中可能发生的具体异常
            return False

    def change_password(
        self, new_password: str, validate_strength: bool = True
    ) -> None:
        """更改加密密码（会重新初始化加密实例）

        Args:
            new_password: 新的密码
            validate_strength: 是否验证新密码强度

        Raises:
            ValueError: 新密码强度不足
            CryptoError: 重新初始化失败
        """

        if validate_strength and not PasswordValidator.validate_strength(new_password):
            password_strength = PasswordValidator.get_strength(new_password)
            raise ValueError(f"新密码强度不足 ({password_strength})，请使用更强的密码")

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
        except Exception as error:
            logger.error("更改密码失败: %s", str(error))
            raise CryptoError(f"密码更改失败: {str(error)}") from error

    @classmethod
    def create_secure_instance(cls, password: str | None = None) -> "CryptoManager":
        """创建安全的加密管理器实例（便捷方法）

        Args:
            password: 可选密码，如果提供会验证强度，否则自动生成

        Returns:
            CryptoManager: 新的加密管理器实例

        Raises:
            ValueError: 密码强度不足
        """

        if password is not None and not PasswordValidator.validate_strength(password):
            password_strength = PasswordValidator.get_strength(password)
            raise ValueError(f"密码强度不足 ({password_strength})，请使用更强的密码")

        # 使用推荐的迭代次数和自动生成的盐值
        return cls(password=password, iterations=cls.DEFAULT_ITERATIONS)

    @classmethod
    def from_saved_key(
        cls, password: str, salt: str, iterations: int | None = None
    ) -> "CryptoManager":
        """从保存的密钥信息创建加密管理器实例

        Args:
            password: 之前保存的密码
            salt: base64 编码的盐值字符串
            iterations: 之前使用的迭代次数，如果为 None 则使用默认值

        Returns:
            CryptoManager: 新的加密管理器实例

        Raises:
            CryptoError: 密钥恢复失败
            ValueError: 密码或盐值格式无效

        Example:
            >>> crypto = CryptoManager.from_saved_key(
            ...     "saved_password", "saved_salt_base64", 480000
            ... )
        """

        if not password or not salt:
            raise ValueError("密码和盐值不能为空")

        try:
            salt_bytes = base64.urlsafe_b64decode(salt.encode("utf-8"))
            return cls(password, salt_bytes, iterations, skip_password_validation=True)
        except Exception as error:
            logger.error("从保存的密钥创建实例失败: %s", str(error))
            raise CryptoError(f"密钥恢复失败: {str(error)}") from error
