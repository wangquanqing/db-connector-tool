"""
基础使用示例
"""

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_connector import DatabaseManager


def basic_usage_example():
    """基础使用示例"""

    # 创建数据库管理器
    db_manager = DatabaseManager()

    # 添加MySQL连接配置
    mysql_config = {
        "type": "mysql",
        "host": "localhost",
        "port": "3306",
        "username": "your_username",
        "password": "your_password",
        "database": "your_database",
    }

    try:
        db_manager.create_connection("my_mysql", mysql_config)
        print("✅ MySQL连接配置已创建")
    except Exception as e:
        print(f"❌ 创建MySQL连接配置失败: {e}")

    # 添加PostgreSQL连接配置
    pg_config = {
        "type": "postgresql",
        "host": "localhost",
        "port": "5432",
        "username": "your_username",
        "password": "your_password",
        "database": "your_database",
    }

    try:
        db_manager.create_connection("my_postgres", pg_config)
        print("✅ PostgreSQL连接配置已创建")
    except Exception as e:
        print(f"❌ 创建PostgreSQL连接配置失败: {e}")

    # 添加SQLite连接配置
    sqlite_config = {"type": "sqlite", "database": "/path/to/your/database.db"}

    try:
        db_manager.create_connection("my_sqlite", sqlite_config)
        print("✅ SQLite连接配置已创建")
    except Exception as e:
        print(f"❌ 创建SQLite连接配置失败: {e}")

    # 列出所有连接
    connections = db_manager.list_connections()
    print(f"\n📋 所有连接: {connections}")

    # 测试连接
    for conn_name in connections:
        try:
            if db_manager.test_connection(conn_name):
                print(f"✅ 连接测试成功: {conn_name}")
            else:
                print(f"❌ 连接测试失败: {conn_name}")
        except Exception as e:
            print(f"⚠️ 连接测试错误 {conn_name}: {e}")

    # 执行查询示例（需要真实的数据库连接）
    try:
        # 这只是示例，需要真实的数据库才能执行
        # results = db_manager.execute_query('my_mysql', 'SELECT * FROM users LIMIT 5')
        # print(f"查询结果: {results}")
        pass
    except Exception as e:
        print(f"查询执行失败: {e}")

    # 清理
    db_manager.close_all_connections()


if __name__ == "__main__":
    basic_usage_example()
