"""
加密模块测试
"""

import pytest

from db_connector.core.crypto import CryptoManager
from db_connector.core.exceptions import CryptoError


class TestCryptoManager:
    """CryptoManager测试类"""

    def test_encrypt_decrypt(self):
        """测试加密解密功能"""
        crypto = CryptoManager()
        test_data = "这是一个测试字符串"

        encrypted = crypto.encrypt(test_data)
        decrypted = crypto.decrypt(encrypted)

        assert decrypted == test_data
        assert encrypted != test_data

    def test_empty_string(self):
        """测试空字符串处理"""
        crypto = CryptoManager()

        encrypted = crypto.encrypt("")
        decrypted = crypto.decrypt(encrypted)

        assert decrypted == ""

    def test_special_characters(self):
        """测试特殊字符"""
        crypto = CryptoManager()
        test_data = '特殊字符!@#$%^&*()_+{}[]|:;"<>,.?/'

        encrypted = crypto.encrypt(test_data)
        decrypted = crypto.decrypt(encrypted)

        assert decrypted == test_data

    def test_decrypt_invalid_data(self):
        """测试解密无效数据"""
        crypto = CryptoManager()

        with pytest.raises(CryptoError):
            crypto.decrypt("invalid_encrypted_data")

    def test_key_persistence(self):
        """测试密钥持久化"""
        crypto1 = CryptoManager()
        key_info = crypto1.get_key_info()

        crypto2 = CryptoManager.from_saved_key(key_info["password"], key_info["salt"])

        test_data = "测试数据"
        encrypted = crypto1.encrypt(test_data)
        decrypted = crypto2.decrypt(encrypted)

        assert decrypted == test_data


if __name__ == "__main__":
    pytest.main()
