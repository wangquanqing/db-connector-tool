#!/usr/bin/env python3
"""
测试配置验证逻辑
"""

from db_connector_tool.core.config import ConfigManager


def test_config_validation():
    """
    测试配置验证逻辑
    """
    print("开始测试配置验证逻辑...")
    
    try:
        # 初始化配置管理器
        config_manager = ConfigManager("test_app", "test_config.toml")
        print("✓ 配置管理器初始化成功")
        
        # 测试连接名称验证
        try:
            config_manager.remove_connection("")
            print("✗ 空连接名称验证失败")
        except ValueError as e:
            print(f"✓ 空连接名称验证成功: {e}")
        
        try:
            config_manager.remove_connection(123)
            print("✗ 非字符串连接名称验证失败")
        except ValueError as e:
            print(f"✓ 非字符串连接名称验证成功: {e}")
        
        # 测试连接配置验证
        try:
            config_manager.add_connection("test_conn", "not a dict")
            print("✗ 非字典配置验证失败")
        except ValueError as e:
            print(f"✓ 非字典配置验证成功: {e}")
        
        # 测试有效配置添加
        test_config = {
            "host": "localhost",
            "port": 5432,
            "username": "test",
            "password": "test"
        }
        
        try:
            config_manager.add_connection("test_conn", test_config)
            print("✓ 有效配置添加成功")
            
            # 测试获取连接
            retrieved_config = config_manager.get_connection("test_conn")
            print(f"✓ 连接获取成功: {retrieved_config}")
            
            # 测试更新连接
            updated_config = {
                "host": "newhost",
                "port": 5433,
                "username": "test",
                "password": "test"
            }
            config_manager.update_connection("test_conn", updated_config)
            print("✓ 连接更新成功")
            
            # 测试删除连接
            config_manager.remove_connection("test_conn")
            print("✓ 连接删除成功")
            
        except Exception as e:
            print(f"✗ 配置操作失败: {e}")
        
        # 测试配置信息获取
        try:
            config_info = config_manager.get_config_info()
            print(f"✓ 配置信息获取成功: {config_info}")
        except Exception as e:
            print(f"✗ 配置信息获取失败: {e}")
        
        # 测试连接列表获取
        try:
            connections = config_manager.list_connections()
            print(f"✓ 连接列表获取成功: {connections}")
        except Exception as e:
            print(f"✗ 连接列表获取失败: {e}")
        
        # 测试配置备份
        try:
            backup_path = config_manager.backup_config()
            print(f"✓ 配置备份成功: {backup_path}")
        except Exception as e:
            print(f"✗ 配置备份失败: {e}")
        
        print("\n所有测试完成！")
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")


if __name__ == "__main__":
    test_config_validation()
