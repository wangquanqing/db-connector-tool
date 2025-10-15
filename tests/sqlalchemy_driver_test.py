from db_connector.core.config import ConfigManager
from db_connector.drivers.sqlalchemy_driver import SQLAlchemyDriver
from db_connector.utils.logging_utils import setup_logging

logger = setup_logging(level="DEBUG")

config_manager = ConfigManager()
mysql_config = config_manager("mysql_local")

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
