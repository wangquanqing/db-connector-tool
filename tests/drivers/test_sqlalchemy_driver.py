import unittest
from unittest.mock import MagicMock, patch

from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver
from src.db_connector_tool.core.exceptions import DriverError


class TestSQLAlchemyDriver(unittest.TestCase):
    """测试 SQLAlchemyDriver 类的功能"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.base_config = {
            "type": "sqlite",
            "database": ":memory:",
        }

    def test_init_valid_config(self) -> None:
        """测试使用有效配置初始化"""
        driver = SQLAlchemyDriver(self.base_config)
        self.assertIsInstance(driver, SQLAlchemyDriver)
        self.assertEqual(driver.config, self.base_config)

    def test_init_invalid_db_type(self) -> None:
        """测试使用无效的数据库类型初始化"""
        invalid_config = {
            "type": "invalid_db",
            "database": ":memory:",
        }
        with self.assertRaises(DriverError) as context:
            SQLAlchemyDriver(invalid_config)
        self.assertIn("不支持的数据库类型", str(context.exception))

    def test_init_missing_required_params(self) -> None:
        """测试缺少必需参数"""
        incomplete_config = {
            "type": "mysql",
        }
        with self.assertRaises(DriverError) as context:
            SQLAlchemyDriver(incomplete_config)
        self.assertIn("缺少必需参数", str(context.exception))

    def test_validate_config_sqlite(self) -> None:
        """测试 SQLite 配置验证"""
        config = {
            "type": "sqlite",
            "database": ":memory:",
        }
        driver = SQLAlchemyDriver(config)
        self.assertIsNotNone(driver)

    def test_validate_config_mysql(self) -> None:
        """测试 MySQL 配置验证"""
        config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        self.assertIsNotNone(driver)

    def test_validate_config_postgresql(self) -> None:
        """测试 PostgreSQL 配置验证"""
        config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        self.assertIsNotNone(driver)

    def test_validate_config_oracle(self) -> None:
        """测试 Oracle 配置验证"""
        config = {
            "type": "oracle",
            "host": "localhost",
            "port": 1521,
            "service_name": "ORCL",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        self.assertIsNotNone(driver)

    def test_validate_config_sqlserver(self) -> None:
        """测试 SQL Server 配置验证"""
        config = {
            "type": "sqlserver",
            "host": "localhost",
            "port": 1433,
            "database": "test_db",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        self.assertIsNotNone(driver)

    def test_build_connection_url_sqlite(self) -> None:
        """测试构建 SQLite 连接 URL"""
        config = {
            "type": "sqlite",
            "database": ":memory:",
        }
        driver = SQLAlchemyDriver(config)
        url = driver._build_connection_url()
        self.assertEqual(url, "sqlite:///:memory:")

    def test_build_connection_url_mysql(self) -> None:
        """测试构建 MySQL 连接 URL"""
        config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        url = driver._build_connection_url()
        self.assertIn("mysql+pymysql://", url)
        self.assertIn("localhost", url)
        self.assertIn("test_db", url)

    def test_build_connection_url_with_special_chars(self) -> None:
        """测试包含特殊字符的连接 URL"""
        config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "user@name",
            "password": "pass#word!",
        }
        driver = SQLAlchemyDriver(config)
        url = driver._build_connection_url()
        self.assertNotIn("@name", url)
        self.assertNotIn("#word!", url)

    def test_validate_sql_query_safe_select(self) -> None:
        """测试验证安全的 SELECT 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("SELECT * FROM users")

    def test_validate_sql_query_safe_insert(self) -> None:
        """测试验证安全的 INSERT 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("INSERT INTO users (name) VALUES ('test')")

    def test_validate_sql_query_safe_update(self) -> None:
        """测试验证安全的 UPDATE 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("UPDATE users SET status = 'active' WHERE id = 1")

    def test_validate_sql_query_safe_delete(self) -> None:
        """测试验证安全的 DELETE 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("DELETE FROM users WHERE id = 1")

    def test_validate_sql_query_safe_create_table(self) -> None:
        """测试验证安全的 CREATE TABLE 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("CREATE TABLE users (id INT, name VARCHAR(100))")

    def test_validate_sql_query_too_long(self) -> None:
        """测试验证过长的查询"""
        driver = SQLAlchemyDriver(self.base_config)
        long_query = "SELECT * FROM users " * 1000
        with self.assertRaises(ValueError):
            driver._validate_sql_query(long_query)

    def test_validate_sql_query_drop_table(self) -> None:
        """测试验证危险的 DROP TABLE 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("DROP TABLE users")

    def test_validate_sql_query_truncate_table(self) -> None:
        """测试验证危险的 TRUNCATE TABLE 查询"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("TRUNCATE TABLE users")

    def test_validate_sql_query_sql_injection_1(self) -> None:
        """测试验证 SQL 注入模式 1"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users WHERE id = '1' OR '1'='1'")

    def test_validate_sql_query_sql_injection_2(self) -> None:
        """测试验证 SQL 注入模式 2"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users; DROP TABLE users; --")

    def test_validate_sql_query_union_injection(self) -> None:
        """测试验证 UNION 注入"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users UNION ALL SELECT * FROM secrets")

    def test_validate_sql_query_grant(self) -> None:
        """测试验证 GRANT 命令"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("GRANT ALL ON *.* TO 'user'@'%'")

    def test_validate_sql_query_xp_cmdshell(self) -> None:
        """测试验证 xp_cmdshell"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("EXEC xp_cmdshell 'dir'")

    def test_context_manager(self) -> None:
        """测试上下文管理器功能"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch.object(driver, 'connect') as mock_connect, \
             patch.object(driver, 'disconnect') as mock_disconnect:
            with driver:
                pass
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    def test_disconnect_idempotent(self) -> None:
        """测试多次调用 disconnect 不会出错"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.disconnect()
        driver.disconnect()


class TestSQLAlchemyDriverAdvanced(unittest.TestCase):
    """测试 SQLAlchemyDriver 的高级功能"""

    def setUp(self) -> None:
        """设置测试环境"""
        self.base_config = {
            "type": "sqlite",
            "database": ":memory:",
        }

    def test_test_connection_no_engine(self) -> None:
        """测试没有引擎时的连接测试"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch.object(driver, 'connect') as mock_connect:
            mock_connect.side_effect = Exception("连接失败")
            result = driver.test_connection()
            self.assertFalse(result)

    def test_perform_connection_test_no_engine(self) -> None:
        """测试没有引擎时的连接测试执行"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        result = driver._perform_connection_test()
        self.assertFalse(result)

    def test_db_configs_contains_all_types(self) -> None:
        """测试 DB_CONFIGS 包含所有支持的数据库类型"""
        expected_types = ["oracle", "postgresql", "mysql", "sqlserver", "sqlite", "gbase"]
        for db_type in expected_types:
            self.assertIn(db_type, SQLAlchemyDriver.DB_CONFIGS)

    def test_db_configs_required_params(self) -> None:
        """测试每个数据库配置都有 required_params"""
        for db_type, config in SQLAlchemyDriver.DB_CONFIGS.items():
            self.assertIn("required_params", config)
            self.assertIsInstance(config["required_params"], list)
            self.assertGreater(len(config["required_params"]), 0)

    def test_db_configs_url_template(self) -> None:
        """测试每个数据库配置都有 url_template"""
        for db_type, config in SQLAlchemyDriver.DB_CONFIGS.items():
            self.assertIn("url_template", config)
            self.assertIsInstance(config["url_template"], str)


if __name__ == "__main__":
    unittest.main()
