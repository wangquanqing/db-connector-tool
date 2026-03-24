#!/usr/bin/env python3
"""
测试盐值长度验证功能
"""

import sys
from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.exceptions import CryptoError


def test_valid_salt():
    """测试使用有效盐值初始化"""
    print("测试1: 使用有效盐值初始化...")
    # 16字节盐值（符合要求）
    valid_salt = b"x" * 16
    try:
        crypto = CryptoManager(salt=valid_salt)
        print("✓ 有效盐值初始化成功")
        return True
    except Exception as e:
        print(f"✗ 有效盐值初始化失败: {e}")
        return False


def test_invalid_salt():
    """测试使用无效盐值初始化"""
    print("\n测试2: 使用无效盐值初始化...")
    # 15字节盐值（不符合要求）
    invalid_salt = b"x" * 15
    try:
        crypto = CryptoManager(salt=invalid_salt)
        print("✗ 无效盐值初始化不应该成功")
        return False
    except ValueError as e:
        print(f"✓ 无效盐值正确触发异常: {e}")
        return True
    except Exception as e:
        print(f"✗ 无效盐值触发了错误的异常: {e}")
        return False


def test_auto_generated_salt():
    """测试自动生成的盐值"""
    print("\n测试3: 自动生成的盐值...")
    try:
        crypto = CryptoManager()
        salt_length = len(crypto.salt)
        print(f"✓ 自动生成的盐值长度: {salt_length}")
        return salt_length >= 16
    except Exception as e:
        print(f"✗ 自动生成盐值失败: {e}")
        return False


def test_from_saved_key():
    """测试从保存的密钥创建实例"""
    print("\n测试4: 从保存的密钥创建实例...")
    try:
        # 创建一个实例并获取密钥信息
        crypto1 = CryptoManager()
        key_info = crypto1.get_key_info()
        
        # 使用保存的密钥创建新实例
        crypto2 = CryptoManager.from_saved_key(key_info['password'], key_info['salt'])
        print("✓ 从保存的密钥创建实例成功")
        
        # 测试加密解密功能
        test_data = "test_data"
        encrypted = crypto2.encrypt(test_data)
        decrypted = crypto2.decrypt(encrypted)
        assert decrypted == test_data
        print("✓ 加密解密功能正常")
        return True
    except Exception as e:
        print(f"✗ 从保存的密钥创建实例失败: {e}")
        return False


if __name__ == "__main__":
    print("开始测试盐值长度验证功能...")
    print("=" * 50)
    
    tests = [
        test_valid_salt,
        test_invalid_salt,
        test_auto_generated_salt,
        test_from_saved_key
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"测试结果: {passed}/{total} 通过")
    
    if passed == total:
        print("✓ 所有测试通过！")
        sys.exit(0)
    else:
        print("✗ 部分测试失败！")
        sys.exit(1)
