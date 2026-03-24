#!/usr/bin/env python3
"""
测试输入验证和SQL注入防护
"""

from db_connector_tool.core.connections import DatabaseManager
from db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver


def test_connection_params_validation():
    """
    测试连接参数验证
    """
    print("开始测试连接参数验证...")
    
    # 测试有效的连接配置
    valid_config = {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "username": "test",
        "password": "test",
        "database": "test_db"
    }
    
    try:
        driver = SQLAlchemyDriver(valid_config)
        print("✓ 有效连接配置验证成功")
    except Exception as e:
        print(f"✗ 有效连接配置验证失败: {e}")
    
    # 测试包含特殊字符的连接配置
    invalid_config = {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "username": "test'; DROP TABLE users; --",
        "password": "test",
        "database": "test_db"
    }
    
    try:
        driver = SQLAlchemyDriver(invalid_config)
        print("✗ 特殊字符验证失败")
    except ValueError as e:
        print(f"✓ 特殊字符验证成功: {e}")
    
    # 测试过长的连接参数
    long_config = {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "username": "a" * 101,  # 超过100个字符
        "password": "test",
        "database": "test_db"
    }
    
    try:
        driver = SQLAlchemyDriver(long_config)
        print("✗ 长度限制验证失败")
    except ValueError as e:
        print(f"✓ 长度限制验证成功: {e}")


def test_sql_injection_protection():
    """
    测试SQL注入防护
    """
    print("\n开始测试SQL注入防护...")
    
    # 测试有效的SQL查询
    valid_query = "SELECT * FROM users WHERE id = :id"
    
    # 测试包含SQL注入的查询
    injection_queries = [
        "SELECT * FROM users WHERE id = 1 OR 1=1",
        "SELECT * FROM users WHERE id = 1; DROP TABLE users;",
        "SELECT * FROM users WHERE id = 1 -- comment",
        "SELECT * FROM users WHERE id = '1' OR '1'='1'",
        "SELECT * FROM users UNION SELECT username, password FROM admin",
        "SELECT * FROM users INTO OUTFILE '/tmp/passwords.txt'"
    ]
    
    # 创建一个驱动实例（不需要实际连接）
    config = {
        "type": "mysql",
        "host": "localhost",
        "port": 3306,
        "username": "test",
        "password": "test",
        "database": "test_db"
    }
    
    try:
        driver = SQLAlchemyDriver(config)
        
        # 测试有效查询
        try:
            driver._validate_sql_query(valid_query)
            print("✓ 有效SQL查询验证成功")
        except ValueError as e:
            print(f"✗ 有效SQL查询验证失败: {e}")
        
        # 测试注入查询
        for query in injection_queries:
            try:
                driver._validate_sql_query(query)
                print(f"✗ SQL注入防护失败: {query[:50]}...")
            except ValueError as e:
                print(f"✓ SQL注入防护成功: {query[:50]}...")
                
    except Exception as e:
        print(f"✗ 测试设置失败: {e}")


def test_database_manager_validation():
    """
    测试数据库管理器的验证
    """
    print("\n开始测试数据库管理器验证...")
    
    try:
        # 初始化数据库管理器
        db_manager = DatabaseManager("test_app", "test_config.toml")
        print("✓ 数据库管理器初始化成功")
        
        # 测试有效的连接配置
        valid_config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "test",
            "password": "test",
            "database": "test_db"
        }
        
        try:
            db_manager.add_connection("test_conn", valid_config)
            print("✓ 有效连接添加成功")
            db_manager.remove_connection("test_conn")
        except Exception as e:
            print(f"✗ 有效连接添加失败: {e}")
        
        # 测试包含特殊字符的连接配置
        invalid_config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "test'; DROP TABLE users; --",
            "password": "test",
            "database": "test_db"
        }
        
        try:
            db_manager.add_connection("test_conn", invalid_config)
            print("✗ 特殊字符验证失败")
            db_manager.remove_connection("test_conn")
        except Exception as e:
            print(f"✓ 特殊字符验证成功: {e}")
            
    except Exception as e:
        print(f"✗ 测试失败: {e}")


if __name__ == "__main__":
    test_connection_params_validation()
    test_sql_injection_protection()
    test_database_manager_validation()
    print("\n所有测试完成！")
