import unittest
from unittest.mock import MagicMock, patch

from src.db_connector_tool.core.exceptions import DriverError, QueryError
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver


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
            driver._validate_sql_query(
                "SELECT * FROM users UNION ALL SELECT * FROM secrets"
            )

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
        with patch.object(driver, "connect") as mock_connect, patch.object(
            driver, "disconnect"
        ) as mock_disconnect:
            with driver:
                pass
            mock_connect.assert_called_once()
            mock_disconnect.assert_called_once()

    def test_disconnect_idempotent(self) -> None:
        """测试多次调用 disconnect 不会出错"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.disconnect()
        driver.disconnect()

    def test_str_representation(self) -> None:
        """测试 __str__ 方法"""
        driver = SQLAlchemyDriver(self.base_config)
        str_repr = str(driver)
        self.assertIn("SQLAlchemyDriver", str_repr)
        self.assertIn("sqlite", str_repr)
        self.assertIn("connected: False", str_repr)

    def test_repr_representation(self) -> None:
        """测试 __repr__ 方法"""
        driver = SQLAlchemyDriver(self.base_config)
        repr_repr = repr(driver)
        self.assertIn("SQLAlchemyDriver", repr_repr)
        self.assertIn("type='sqlite'", repr_repr)
        self.assertIn("database=':memory:'", repr_repr)
        self.assertIn("connected=False", repr_repr)

    def test_str_with_connection(self) -> None:
        """测试有连接时的 __str__ 方法"""
        config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        driver.engine = MagicMock()
        str_repr = str(driver)
        self.assertIn("mysql", str_repr)
        self.assertIn("connected: True", str_repr)

    def test_repr_with_mysql_config(self) -> None:
        """测试 MySQL 配置的 __repr__ 方法"""
        config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "database": "test_db",
            "username": "user",
            "password": "password",
        }
        driver = SQLAlchemyDriver(config)
        repr_repr = repr(driver)
        self.assertIn("type='mysql'", repr_repr)
        self.assertIn("host='localhost'", repr_repr)
        self.assertIn("port='3306'", repr_repr)
        self.assertIn("database='test_db'", repr_repr)

    def test_connect(self) -> None:
        """测试建立数据库连接"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine
            driver.connect()
            mock_create_engine.assert_called_once()
            self.assertIsNotNone(driver.engine)

    def test_execute_query(self) -> None:
        """测试执行查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "test")]
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        
        results = driver.execute_query("SELECT * FROM users")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 1)
        self.assertEqual(results[0]["name"], "test")

    def test_execute_query_with_parameters(self) -> None:
        """测试带参数的查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "test")]
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        
        results = driver.execute_query("SELECT * FROM users WHERE id = :id", {"id": 1})
        self.assertEqual(len(results), 1)

    def test_execute_command(self) -> None:
        """测试执行命令"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        
        affected = driver.execute_command("UPDATE users SET name = 'test' WHERE id = 1")
        self.assertEqual(affected, 1)

    def test_execute_command_with_parameters(self) -> None:
        """测试带参数的命令"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 1
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        
        affected = driver.execute_command("UPDATE users SET name = :name WHERE id = :id", {"name": "test", "id": 1})
        self.assertEqual(affected, 1)

    def test_get_tables(self) -> None:
        """测试获取表列表"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users", "orders"]
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", return_value=mock_inspector):
            tables = driver.get_tables()
            self.assertEqual(len(tables), 2)
            self.assertIn("users", tables)
            self.assertIn("orders", tables)

    def test_get_table_schema(self) -> None:
        """测试获取表结构"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "default": None},
            {"name": "name", "type": "VARCHAR", "nullable": True, "default": None}
        ]
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", return_value=mock_inspector):
            schema = driver.get_table_schema("users")
            self.assertEqual(len(schema), 2)
            self.assertEqual(schema[0]["name"], "id")
            self.assertEqual(schema[1]["name"], "name")

    def test_perform_connection_test(self) -> None:
        """测试执行连接测试"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (1,)
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        
        result = driver._perform_connection_test()
        self.assertTrue(result)

    def test_test_connection_success(self) -> None:
        """测试连接测试成功"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "_perform_connection_test", return_value=True):
            result = driver.test_connection()
            self.assertTrue(result)

    def test_test_connection_failure(self) -> None:
        """测试连接测试失败"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "_perform_connection_test", side_effect=Exception("连接失败")):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_os_error(self) -> None:
        """测试连接测试时的OS错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "_perform_connection_test", side_effect=OSError("网络错误")):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_value_error(self) -> None:
        """测试连接测试时的值错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "_perform_connection_test", side_effect=ValueError("配置错误")):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_attribute_error(self) -> None:
        """测试连接测试时的属性错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "_perform_connection_test", side_effect=AttributeError("属性错误")):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_type_error(self) -> None:
        """测试连接测试时的类型错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "_perform_connection_test", side_effect=TypeError("类型错误")):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_connect_with_existing_engine(self) -> None:
        """测试连接时引擎已存在的情况"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "disconnect") as mock_disconnect:
            with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
                mock_create_engine.return_value = MagicMock()
                driver.connect()
                mock_disconnect.assert_called_once()

    def test_connect_sqlite(self) -> None:
        """测试SQLite连接池配置"""
        sqlite_config = {
            "type": "sqlite",
            "database": ":memory:",
        }
        driver = SQLAlchemyDriver(sqlite_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            # 验证调用参数中包含SQLite特定配置
            args, kwargs = mock_create_engine.call_args
            self.assertIn("pool_size", kwargs)
            self.assertNotIn("max_overflow", kwargs)

    def test_connect_mysql(self) -> None:
        """测试MySQL连接池配置"""
        mysql_config = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "database": "test_db",
        }
        driver = SQLAlchemyDriver(mysql_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_recycle"], 280)

    def test_connect_postgresql(self) -> None:
        """测试PostgreSQL连接池配置"""
        pg_config = {
            "type": "postgresql",
            "host": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "password",
            "database": "test_db",
        }
        driver = SQLAlchemyDriver(pg_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_recycle"], 3600)



    def test_execute_sql_no_engine(self) -> None:
        """测试execute_sql方法在engine未初始化时的处理"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_connection = MagicMock()
            mock_result = MagicMock()
            mock_result.all.return_value = []
            mock_connection.execute.return_value = mock_result
            mock_engine.connect.return_value.__enter__.return_value = mock_connection
            mock_create_engine.return_value = mock_engine
            result = driver.execute_sql("SELECT * FROM users")
            self.assertEqual(result, [])

    def test_get_tables_no_engine(self) -> None:
        """测试get_tables方法在engine未初始化时的处理"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["users", "posts"]
            with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", return_value=mock_inspector):
                mock_create_engine.return_value = mock_engine
                tables = driver.get_tables()
                self.assertEqual(tables, ["users", "posts"])

    def test_get_table_schema_no_engine(self) -> None:
        """测试get_table_schema方法在engine未初始化时的处理"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_columns.return_value = [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR", "nullable": True},
            ]
            with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", return_value=mock_inspector):
                mock_create_engine.return_value = mock_engine
                schema = driver.get_table_schema("users")
                self.assertEqual(len(schema), 2)
                self.assertEqual(schema[0]["name"], "id")

    def test_execute_sql_sqlalchemy_error(self) -> None:
        """测试execute_sql方法的SQLAlchemyError处理"""
        from sqlalchemy.exc import SQLAlchemyError
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = SQLAlchemyError("Database error")
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        with self.assertRaises(QueryError) as context:
            driver.execute_sql("SELECT * FROM users")
        self.assertIn("执行SQL失败", str(context.exception))

    def test_get_tables_sqlalchemy_error(self) -> None:
        """测试get_tables方法的SQLAlchemyError处理"""
        from sqlalchemy.exc import SQLAlchemyError
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect") as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.side_effect = SQLAlchemyError("Database error")
            mock_inspect.return_value = mock_inspector
            with self.assertRaises(QueryError) as context:
                driver.get_tables()
            self.assertIn("获取表列表失败", str(context.exception))

    def test_get_table_schema_sqlalchemy_error(self) -> None:
        """测试get_table_schema方法的SQLAlchemyError处理"""
        from sqlalchemy.exc import SQLAlchemyError
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect") as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.get_columns.side_effect = SQLAlchemyError("Database error")
            mock_inspect.return_value = mock_inspector
            with self.assertRaises(QueryError) as context:
                driver.get_table_schema("users")
            self.assertIn("获取表结构失败", str(context.exception))

    def test_connect_oracle(self) -> None:
        """测试Oracle连接池配置"""
        oracle_config = {
            "type": "oracle",
            "host": "localhost",
            "port": 1521,
            "username": "system",
            "password": "password",
            "service_name": "ORCL",
        }
        driver = SQLAlchemyDriver(oracle_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_recycle"], 1800)

    def test_connect_sqlserver(self) -> None:
        """测试SQL Server连接池配置"""
        sqlserver_config = {
            "type": "sqlserver",
            "host": "localhost",
            "port": 1433,
            "username": "sa",
            "password": "password",
            "database": "test_db",
        }
        driver = SQLAlchemyDriver(sqlserver_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_recycle"], 3600)

    def test_connect_with_user_pool_config(self) -> None:
        """测试使用用户自定义连接池配置"""
        config_with_pool = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "database": "test_db",
            "pool_config": {"pool_size": 10, "max_overflow": 20},
        }
        driver = SQLAlchemyDriver(config_with_pool)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_size"], 10)
            self.assertEqual(kwargs["max_overflow"], 20)

    def test_connect_sqlalchemy_error(self) -> None:
        """测试连接时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        from sqlalchemy.exc import SQLAlchemyError
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine", side_effect=SQLAlchemyError("连接错误")):
            from src.db_connector_tool.core.exceptions import DBConnectionError
            with self.assertRaises(DBConnectionError):
                driver.connect()

    def test_connect_generic_error(self) -> None:
        """测试连接时的通用错误"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine", side_effect=Exception("未知错误")):
            from src.db_connector_tool.core.exceptions import DBConnectionError
            with self.assertRaises(DBConnectionError):
                driver.connect()

    def test_disconnect_with_sqlalchemy_error(self) -> None:
        """测试断开连接时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        driver.session = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError
        driver.engine.dispose.side_effect = SQLAlchemyError("断开错误")
        # 应该不会抛出异常
        driver.disconnect()

    def test_disconnect_with_generic_error(self) -> None:
        """测试断开连接时的通用错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        driver.session = MagicMock()
        driver.engine.dispose.side_effect = Exception("未知错误")
        # 应该会抛出异常
        with self.assertRaises(Exception):
            driver.disconnect()

    def test_execute_sql_value_error(self) -> None:
        """测试执行SQL时的值错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = ValueError("验证错误")
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        from src.db_connector_tool.core.exceptions import QueryError
        with self.assertRaises(QueryError):
            driver._execute_sql("SELECT * FROM users")

    def test_execute_sql_generic_error(self) -> None:
        """测试执行SQL时的通用错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_connection.execute.side_effect = Exception("未知错误")
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        from src.db_connector_tool.core.exceptions import QueryError
        with self.assertRaises(QueryError):
            driver._execute_sql("SELECT * FROM users")

    def test_validate_sql_query_too_long(self) -> None:
        """测试验证过长的SQL查询"""
        driver = SQLAlchemyDriver(self.base_config)
        long_query = "SELECT * FROM users WHERE " + "a = 1 AND " * 1000
        with self.assertRaises(ValueError):
            driver._validate_sql_query(long_query)

    def test_validate_sql_query_safe_ddl(self) -> None:
        """测试验证安全的DDL查询"""
        driver = SQLAlchemyDriver(self.base_config)
        # 这些应该不会抛出异常
        driver._validate_sql_query("CREATE TABLE users (id INT)")
        driver._validate_sql_query("ALTER TABLE users ADD COLUMN name VARCHAR(100)")
        driver._validate_sql_query("CREATE INDEX idx_name ON users(name)")
        driver._validate_sql_query("CREATE VIEW user_view AS SELECT * FROM users")
        driver._validate_sql_query("CREATE PROCEDURE get_user() BEGIN SELECT * FROM users; END")
        driver._validate_sql_query("CREATE FUNCTION get_id() RETURNS INT BEGIN RETURN 1; END")
        driver._validate_sql_query("CREATE TRIGGER user_trigger AFTER INSERT ON users BEGIN END")

    def test_validate_sql_query_suspicious_comment(self) -> None:
        """测试验证包含可疑注释的查询"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users WHERE id = 1' --")
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users /*!50000 SELECT */")
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users; /* comment */ SELECT * FROM orders")

    def test_context_manager(self) -> None:
        """测试上下文管理器"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch.object(driver, "connect") as mock_connect:
            with patch.object(driver, "disconnect") as mock_disconnect:
                with driver:
                    pass
                mock_connect.assert_called_once()
                mock_disconnect.assert_called_once()

    def test_context_manager_with_exception(self) -> None:
        """测试上下文管理器在异常情况下的行为"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch.object(driver, "connect") as mock_connect:
            with patch.object(driver, "disconnect") as mock_disconnect:
                try:
                    with driver:
                        raise Exception("测试异常")
                except Exception:
                    pass
                mock_connect.assert_called_once()
                mock_disconnect.assert_called_once()

    def test_parse_kingbase_version(self) -> None:
        """测试解析 Kingbase 版本信息"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import parse_kingbase_version
        # 模拟连接对象
        mock_connection = MagicMock()
        # 测试 PostgreSQL 格式的版本字符串
        mock_connection.exec_driver_sql.return_value.scalar.return_value = "PostgreSQL 8.6.0 on x86_64"
        version = parse_kingbase_version(None, mock_connection)
        self.assertEqual(version, (8, 6, 0))
        
        # 测试 Kingbase 格式的版本字符串
        mock_connection.exec_driver_sql.return_value.scalar.return_value = "Kingbase V8R6C4B10"
        version = parse_kingbase_version(None, mock_connection)
        self.assertEqual(version, (8, 6, 4))
        
        # 测试无法解析的版本字符串
        mock_connection.exec_driver_sql.return_value.scalar.return_value = "Unknown version"
        with self.assertRaises(AssertionError):
            parse_kingbase_version(None, mock_connection)

    def test_connect_with_user_pool_config(self) -> None:
        """测试使用用户自定义连接池配置"""
        config_with_pool = {
            "type": "mysql",
            "host": "localhost",
            "port": 3306,
            "username": "root",
            "password": "password",
            "database": "test_db",
            "pool_config": {"pool_size": 10, "max_overflow": 20},
        }
        driver = SQLAlchemyDriver(config_with_pool)
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.create_engine") as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_size"], 10)
            self.assertEqual(kwargs["max_overflow"], 20)

    def test_disconnect_with_engine(self) -> None:
        """测试断开连接时引擎存在的情况"""
        driver = SQLAlchemyDriver(self.base_config)
        mock_engine = MagicMock()
        driver.engine = mock_engine
        driver.session = MagicMock()
        driver.disconnect()
        mock_engine.dispose.assert_called_once()
        self.assertIsNone(driver.engine)

    def test_test_connection_database_error(self) -> None:
        """测试连接测试时的数据库错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError
        with patch.object(driver, "_perform_connection_test", side_effect=SQLAlchemyError("数据库错误")):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_execute_sql_no_engine(self) -> None:
        """测试执行SQL时引擎未初始化的情况"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch.object(driver, "connect") as mock_connect:
            mock_connect.return_value = None
            driver.engine = MagicMock()
            mock_connection = MagicMock()
            mock_result = MagicMock()
            mock_result.keys.return_value = ["id"]
            mock_result.fetchall.return_value = [(1,)]
            mock_connection.execute.return_value = mock_result
            driver.engine.connect.return_value.__enter__.return_value = mock_connection
            results = driver.execute_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)

    def test_execute_sql_sqlalchemy_error(self) -> None:
        """测试执行SQL时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError
        mock_connection.execute.side_effect = SQLAlchemyError("数据库错误")
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        with self.assertRaises(QueryError):
            driver.execute_query("SELECT * FROM users")

    def test_get_tables_no_engine(self) -> None:
        """测试获取表列表时引擎未初始化的情况"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch.object(driver, "connect") as mock_connect:
            mock_connect.return_value = None
            driver.engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["users"]
            with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", return_value=mock_inspector):
                tables = driver.get_tables()
                self.assertEqual(len(tables), 1)

    def test_get_tables_sqlalchemy_error(self) -> None:
        """测试获取表列表时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", side_effect=SQLAlchemyError("数据库错误")):
            with self.assertRaises(QueryError):
                driver.get_tables()

    def test_get_table_schema_no_engine(self) -> None:
        """测试获取表结构时引擎未初始化的情况"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch.object(driver, "connect") as mock_connect:
            mock_connect.return_value = None
            driver.engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_columns.return_value = [{"name": "id", "type": "INTEGER"}]
            with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", return_value=mock_inspector):
                schema = driver.get_table_schema("users")
                self.assertEqual(len(schema), 1)

    def test_get_table_schema_sqlalchemy_error(self) -> None:
        """测试获取表结构时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError
        with patch("src.db_connector_tool.drivers.sqlalchemy_driver.inspect", side_effect=SQLAlchemyError("数据库错误")):
            with self.assertRaises(QueryError):
                driver.get_table_schema("users")


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
        with patch.object(driver, "connect") as mock_connect:
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
        expected_types = [
            "oracle",
            "postgresql",
            "mysql",
            "sqlserver",
            "sqlite",
            "gbase",
        ]
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
