from db_connector_tool import DatabaseManager

# 创建数据库管理器
db_manager = DatabaseManager()

gb = db_manager.get_connection('db_kes', {"port": "9088", "server": "gbase01"})
print(gb)
# 执行查询
results = db_manager.execute_query('gb', "select dbinfo('version_gbase','full') from dual")
print(results)

# 关闭所有连接
db_manager.close_all_connections()