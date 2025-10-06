from db_connector import DatabaseManager

# 创建数据库管理器
db_manager = DatabaseManager()

# 添加数据库连接
mysql_config = {
    "type": "mysql",
    "host": "localhost",
    "port": "3306",
    "username": "cvicse",
    "password": "Cvicsejszx@2022",
    "database": "db_station",
}

db_manager.create_connection("my_mysql", mysql_config)

# 执行查询
results = db_manager.execute_query("my_mysql", "SELECT host, user FROM mysql.user LIMIT 10")
print(results)

# 关闭连接
db_manager.close_all_connections()
