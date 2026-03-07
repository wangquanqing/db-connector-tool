# 批量管理器使用示例
from db_connector_tool import BatchDatabaseManager, generate_ip_range

# 1. 创建批量管理器
batch_manager = BatchDatabaseManager("user_databases")

# 2. 设置基础配置模板
base_config = {
    "type": "mysql",
    "port": 3306,
    "username": "admin",
    "password": "secure_password",
    "database": "user_database",
    "charset": "utf8mb4"
}
batch_manager.set_base_config(base_config)

# 3. 生成IP地址列表
ip_list = generate_ip_range("192.168.1.100", 200)

# 4. 批量添加连接
add_results = batch_manager.add_batch_connections(ip_list, "user_db")

# 5. 批量测试连接
test_results = batch_manager.test_batch_connections(max_workers=10)

# 6. 批量表结构升级
upgrade_sqls = [
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TIMESTAMP",
    "ALTER TABLE users MODIFY COLUMN email VARCHAR(255) NOT NULL"
]

upgrade_results = batch_manager.upgrade_table_structure(
    upgrade_sqls=upgrade_sqls,
    max_workers=5
)

# 7. 统计结果
success_count = sum(1 for result in upgrade_results.values() if result["success"])
print(f"升级成功: {success_count}/{len(ip_list)}")

# 8. 清理资源
batch_manager.close_all_connections()
