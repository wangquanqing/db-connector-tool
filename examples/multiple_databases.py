"""
多数据库操作示例
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db_connector import DatabaseManager

def multiple_databases_example():
    """多数据库操作示例"""
    
    db_manager = DatabaseManager()
    
    # 定义多个数据库配置
    databases = {
        'company_mysql': {
            'type': 'mysql',
            'host': 'mysql.company.com',
            'port': '3306',
            'username': 'app_user',
            'password': 'mysql_pass123',
            'database': 'company_data'
        },
        'analytics_postgres': {
            'type': 'postgresql',
            'host': 'pgsql.analytics.com',
            'port': '5432',
            'username': 'analytics_user',
            'password': 'pg_pass456',
            'database': 'analytics_db'
        },
        'local_sqlite': {
            'type': 'sqlite',
            'database': '/data/local_cache.db'
        }
    }
    
    # 批量创建连接配置
    for name, config in databases.items():
        try:
            db_manager.create_connection(name, config)
            print(f"✅ 连接配置已创建: {name}")
        except Exception as e:
            print(f"❌ 创建连接配置失败 {name}: {e}")
    
    # 显示所有连接
    print(f"
🔗 所有数据库连接: {db_manager.list_connections()}")
    
    # 跨数据库查询示例
    print("
🔄 跨数据库操作示例:")
    
    for conn_name in db_manager.list_connections():
        try:
            # 测试连接
            if db_manager.test_connection(conn_name):
                print(f"   ✅ {conn_name}: 连接正常")
                
                # 这里可以执行特定于每个数据库的查询
                # 例如：results = db_manager.execute_query(conn_name, "SELECT version()")
                
            else:
                print(f"   ❌ {conn_name}: 连接失败")
                
        except Exception as e:
            print(f"   ⚠️ {conn_name}: 错误 - {e}")
    
    # 清理
    db_manager.close_all_connections()
    print("
🧹 所有连接已关闭")

if __name__ == "__main__":
    multiple_databases_example()
