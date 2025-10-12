from db_connector.core.config import ConfigManager
from db_connector.drivers.sqlalchemy_driver import SQLAlchemyDriver
from db_connector.utils.logging_utils import setup_logging

logger = setup_logging(level="DEBUG")

# 添加数据库连接
mysql_config = {
    "type": "mysql",
    "host": "localhost",
    "port": "3306",
    "username": "cvicse",
    "password": "Cvicsejszx@2022",
    "database": "db_station",
}
pg_config = {
    "type": "postgresql",
    "host": "localhost",
    "port": "5432",
    "username": "cvicse",
    "password": "Cvicsejszx@2022",
    "database": "db_station",
    "gssencmode": "disable",
}
sqlite_config = { "type": "sqlite", "database": r"D:\wangq\Documents\work\707-巴万清江\707_SqliteAllPathTable\db_station.db" }
config_manager = ConfigManager()

with SQLAlchemyDriver(mysql_config) as driver:
    # 执行查询
    result = driver.execute_query(
        "SELECT 1"
    )
    print(result)
    result = driver.get_connection_info()
    print(result)
    result = driver.test_connection()
    print(result)
    # result = driver.execute_command(
    #     "UPDATE public.sample_users set name=:name where id=:id",
    #     {"name": "张一", "id": 1},
    # )
    # print(result)
