#!/usr/bin/env python3
"""
测试基本功能是否正常工作
"""

from db_connector_tool.core.connections import DatabaseManager
from db_connector_tool.core.config import ConfigManager
from db_connector_tool.core.crypto import CryptoManager

print("=== 测试基本功能 ===")

# 测试1: 测试加密管理器
print("\n测试1: 加密管理器")
try:
    crypto = CryptoManager()
    test_data = "测试数据"
    encrypted = crypto.encrypt(test_data)
    decrypted = crypto.decrypt(encrypted)
    print(f"✓ 加密/解密功能正常: {test_data} → {decrypted}")
except Exception as e:
    print(f"✗ 加密管理器测试失败: {e}")

# 测试2: 测试配置管理器
print("\n测试2: 配置管理器")
try:
    config_manager = ConfigManager("test_app", "test_connections.toml")
    connections = config_manager.list_connections()
    print(f"✓ 配置管理器初始化成功，当前连接数: {len(connections)}")
except Exception as e:
    print(f"✗ 配置管理器测试失败: {e}")

# 测试3: 测试数据库管理器
print("\n测试3: 数据库管理器")
try:
    db_manager = DatabaseManager("test_app", "test_connections.toml")
    connections = db_manager.list_connections()
    print(f"✓ 数据库管理器初始化成功，当前连接数: {len(connections)}")
except Exception as e:
    print(f"✗ 数据库管理器测试失败: {e}")

print("\n=== 测试完成 ===")
