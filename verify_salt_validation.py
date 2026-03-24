"""
验证盐值长度验证功能
"""

from src.db_connector_tool.core.crypto import CryptoManager

print("验证盐值长度验证功能...")
print("=" * 50)

# 测试1: 有效盐值
print("测试1: 有效盐值 (16字节)")
try:
    crypto1 = CryptoManager(salt=b"x" * 16)
    print("✓ 有效盐值初始化成功")
except Exception as e:
    print(f"✗ 有效盐值初始化失败: {e}")

# 测试2: 无效盐值
print("\n测试2: 无效盐值 (15字节)")
try:
    crypto2 = CryptoManager(salt=b"x" * 15)
    print("✗ 无效盐值初始化不应该成功")
except ValueError as e:
    print(f"✓ 无效盐值正确触发异常: {e}")
except Exception as e:
    print(f"✗ 无效盐值触发了错误的异常: {e}")

# 测试3: 自动生成的盐值
print("\n测试3: 自动生成的盐值")
try:
    crypto3 = CryptoManager()
    salt_length = len(crypto3.salt)
    print(f"✓ 自动生成的盐值长度: {salt_length}")
    print(f"✓ 盐值长度是否符合要求: {salt_length >= 16}")
except Exception as e:
    print(f"✗ 自动生成盐值失败: {e}")

print("\n" + "=" * 50)
print("验证完成！")
