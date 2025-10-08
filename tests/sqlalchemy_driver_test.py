from db_connector.core.config import ConfigManager
from db_connector.drivers.sqlalchemy_driver import SQLAlchemyDriver
from db_connector.utils.logging_utils import setup_logging

logger = setup_logging()

# 添加数据库连接
mysql_config = {
    "type": "mysql",
    "host": "localhost",
    "port": "3306",
    "username": "cvicse",
    "password": "Cvicsejszx@2022",
    "database": "db_station",
}

config_manager = ConfigManager()

config = config_manager.get_connection("pg")
print(config)

with SQLAlchemyDriver(config) as driver:
    # 执行查询
    result = driver.execute_query(
        "SELECT * FROM public.sample_users where name=:name", {"name": "张三"}
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
