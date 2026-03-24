#!/usr/bin/env python3
"""
测试加密模块的功能
"""

from src.db_connector_tool.core.crypto import CryptoManager

print("=== 测试加密模块 ===")

# 测试1: 使用默认参数（自动调整迭代次数）
print("\n测试1: 使用默认参数")
crypto1 = CryptoManager()
print(f"实例信息: {crypto1}")
key_info1 = crypto1.get_key_info()
print(f"迭代次数: {key_info1['iterations']}")
print(f"盐值长度: {len(crypto1.salt)}")
print(f"密码长度: {len(crypto1.password)}")

# 测试2: 使用自定义迭代次数
print("\n测试2: 使用自定义迭代次数")
crypto2 = CryptoManager(iterations=600000)
print(f"实例信息: {crypto2}")
key_info2 = crypto2.get_key_info()
print(f"迭代次数: {key_info2['iterations']}")

# 测试3: 测试加密和解密功能
print("\n测试3: 测试加密和解密功能")
test_data = "这是一个测试数据"
encrypted = crypto1.encrypt(test_data)
print(f"加密后: {encrypted[:50]}...")
decrypted = crypto1.decrypt(encrypted)
print(f"解密后: {decrypted}")
print(f"解密结果是否正确: {decrypted == test_data}")

# 测试4: 从保存的密钥创建实例
print("\n测试4: 从保存的密钥创建实例")
crypto3 = CryptoManager.from_saved_key(
    key_info1['password'],
    key_info1['salt'],
    key_info1['iterations']
)
print(f"实例信息: {crypto3}")
print(f"迭代次数: {crypto3.get_key_info()['iterations']}")

# 测试5: 测试字节数据加密解密
print("\n测试5: 测试字节数据加密解密")
test_bytes = b"test binary data"
encrypted_bytes = crypto1.encrypt_bytes(test_bytes)
print(f"加密后字节长度: {len(encrypted_bytes)}")
decrypted_bytes = crypto1.decrypt_bytes(encrypted_bytes)
print(f"解密后: {decrypted_bytes}")
print(f"解密结果是否正确: {decrypted_bytes == test_bytes}")

print("\n=== 测试完成 ===")
