import unittest
from unittest.mock import MagicMock, patch

from sqlalchemy.dialects.sqlite import pysqlite as sqlite_dialect

from src.db_connector_tool.core.exceptions import DriverError, QueryError
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver


def _dialect_stub(default_from: str = ""):
    """构造一个**真实**方言对象，可指定编译时补出的伪表

    ``select(1).compile(dialect=...)`` 必须走 SQLAlchemy 真实的编译路径，
    因此这里刻意用真实方言/编译器子类而非 ``MagicMock``：用 mock 时任何字符串都
    “编译成功”，真实方言之间的差异（如伪表补全、DBAPI 探活字符串）会被
    整个吞掉 —— 这正是本文件此前漏掉 PostgreSQL 回归的原因。

    注意 ``default_from()`` 定义在**编译器**上（见 ``sql/compiler.py`` 里
    ``text += self.default_from()``），不是方言上；GBase 8s 方言也正是这么做的
    （``GBase8sCompiler.default_from``），故这里照同一机制复刻。

    Args:
        default_from: 非空时，``select(1)`` 会编译成
            ``SELECT 1 FROM <default_from>``（GBase/MySQL 补伪表的方式）。
    """
    sqlite = sqlite_dialect.SQLiteDialect_pysqlite

    class _StubCompiler(sqlite.statement_compiler):  # type: ignore[misc]
        def default_from(self) -> str:
            return f" FROM {default_from}" if default_from else ""

    class _StubDialect(sqlite):  # type: ignore[misc, valid-type]
        statement_compiler = _StubCompiler

        # 显式给出 DBAPI 探活字符串：真实方言都有该属性，这里保持它的存在，
        # 以便用例能验证“实现里没有去读它”。
        _dialect_specific_select_one = ";"

    return _StubDialect()


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
        """显式 DROP TABLE 属管理语句，应放行（CLI 已声明支持）"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("DROP TABLE users")

    def test_validate_sql_query_truncate_table(self) -> None:
        """显式 TRUNCATE TABLE 属管理语句，应放行"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("TRUNCATE TABLE users")

    def test_validate_sql_query_sql_injection_1(self) -> None:
        """测试验证 SQL 注入模式 1"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users WHERE id = '1' OR '1'='1'")

    def test_validate_sql_query_sql_injection_2(self) -> None:
        """测试验证 SQL 注入模式 2（分号后追加第二条语句）"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users; DROP TABLE users; --")

    def test_validate_sql_query_or_1_equals_1(self) -> None:
        """结构性 OR 1=1 注入仍被拦截"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users WHERE id = 1 OR 1=1")

    def test_validate_sql_query_blocks_multi_statement(self) -> None:
        """一次执行多条语句（语句走私）必须拒绝"""
        driver = SQLAlchemyDriver(self.base_config)
        for sql in (
            "SELECT * FROM users; DROP TABLE users",
            "SELECT 1; DELETE FROM users",
            "SELECT * FROM users; /* comment */ SELECT * FROM orders",
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(ValueError):
                    driver._validate_sql_query(sql)

    def test_validate_sql_query_allows_metadata_and_union(self) -> None:
        """information_schema / sys 元数据查询与 UNION 查询都是合法 SQL"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("SELECT table_name FROM information_schema.tables")
        driver._validate_sql_query("SELECT name FROM sys.tables")
        driver._validate_sql_query("SELECT id FROM users UNION SELECT id FROM admins")
        driver._validate_sql_query(
            "SELECT id FROM users UNION ALL SELECT id FROM admins"
        )

    def test_validate_sql_query_ignores_string_literals(self) -> None:
        """字面量里出现的注入文本属于数据内容，不应误报"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("SELECT * FROM logs WHERE msg = 'OR 1=1'")
        driver._validate_sql_query("SELECT * FROM logs WHERE msg = 'xp_cmdshell'")
        driver._validate_sql_query("SELECT * FROM logs WHERE msg = 'a; DROP TABLE t'")
        driver._validate_sql_query("SELECT * FROM logs WHERE msg = 'UNION SELECT'")

    def test_validate_sql_query_grant(self) -> None:
        """显式权限语句属管理操作，应放行（与 DROP/CREATE 同等对待）"""
        driver = SQLAlchemyDriver(self.base_config)
        driver._validate_sql_query("GRANT ALL ON *.* TO 'user'@'%'")
        driver._validate_sql_query("REVOKE ALL ON *.* FROM 'user'@'%'")

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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        # 当前实现通过 result.mappings().all() 返回行字典列表
        mock_result.mappings.return_value.all.return_value = [
            {"id": 1, "name": "test"}
        ]
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        results = driver.execute_query("SELECT * FROM users")
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 1)
        self.assertEqual(results[0]["name"], "test")

    def test_execute_query_with_metadata(self) -> None:
        """测试同时返回列元数据的查询

        元数据必须在取值**之前**读取（取完最后一行后结果集关闭、
        ``cursor.description`` 失效），故断言方言钩子被优先调用。
        """
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [{"id": 1}]
        mock_result.cursor = MagicMock()
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection
        driver.engine.dialect.get_column_metadata.return_value = [
            {"name": "id", "type_name": "INTEGER", "python_type": "int"}
        ]

        rows, columns = driver.execute_query_with_metadata("SELECT id FROM users")
        self.assertEqual(rows, [{"id": 1}])
        self.assertEqual(columns[0]["python_type"], "int")
        driver.engine.dialect.get_column_metadata.assert_called_once_with(
            mock_result.cursor
        )

    def test_execute_query_with_metadata_falls_back(self) -> None:
        """方言未提供元数据钩子时退回 DBAPI description"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        del driver.engine.dialect.get_column_metadata
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = []
        mock_result.cursor.description = [("id", None, None, None, 10, 0, True)]
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        _, columns = driver.execute_query_with_metadata("SELECT id FROM users")
        self.assertEqual(columns[0]["name"], "id")
        self.assertEqual(columns[0]["precision"], 10)

    def test_execute_query_with_parameters(self) -> None:
        """测试带参数的查询"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.mappings.return_value.all.return_value = [
            {"id": 1, "name": "test"}
        ]
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        results = driver.execute_query("SELECT * FROM users WHERE id = :id", {"id": 1})
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["id"], 1)

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

        affected = driver.execute_command(
            "UPDATE users SET name = :name WHERE id = :id", {"name": "test", "id": 1}
        )
        self.assertEqual(affected, 1)

    def test_get_tables(self) -> None:
        """测试获取表列表"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users", "orders"]
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
            return_value=mock_inspector,
        ):
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
            {"name": "name", "type": "VARCHAR", "nullable": True, "default": None},
        ]
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
            return_value=mock_inspector,
        ):
            schema = driver.get_table_schema("users")
            self.assertEqual(len(schema), 2)
            self.assertEqual(schema[0]["name"], "id")
            self.assertEqual(schema[1]["name"], "name")

    def test_perform_connection_test(self) -> None:
        """测试执行连接测试"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        # 方言需是可编译的真实方言对象（见下方 _dialect_stub）
        driver.engine.dialect = _dialect_stub("SELECT 1")
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchone.return_value = (1,)
        mock_connection.execute.return_value = mock_result
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        result = driver._perform_connection_test()
        self.assertTrue(result)

    def test_connection_test_uses_dialect_rendered_select(self) -> None:
        """连接测试必须采用方言渲染的语句，而非硬编码的 ``SELECT 1``

        回归 GBase 8s：gbase/mysql 模式下不接受无 FROM 的 SELECT，
        硬编码会让连接测试误报失败。方言在编译 ``select(1)`` 时会补上伪表。
        """
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        driver.engine.dialect = _dialect_stub("sysmaster:sysdual")
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchone.return_value = (1,)
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        self.assertTrue(driver._perform_connection_test())
        executed = mock_connection.execute.call_args[0][0]
        self.assertIn("sysmaster:sysdual", str(executed))

    def test_connection_test_ignores_dbapi_ping_string(self) -> None:
        """必须**忽略** ``_dialect_specific_select_one``，改用正常编译

        回归 PostgreSQL：psycopg 方言的 ``_dialect_specific_select_one`` 是
        ``";"``（SQLAlchemy 内部给 DBAPI 层 ``do_ping()`` 用的探活字符串，
        见 ``engine/default.py``），并非可执行的完整 SELECT。此前直接把它交给
        ``conn.execute(text(...))``，psycopg 报
        “This result object does not return rows”，导致连接测试**误报失败**。
        """
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        # 模拟 psycopg 方言：ping 字符串是 ";"，但编译结果必须是完整的 SELECT
        dialect = _dialect_stub("")
        dialect._dialect_specific_select_one = ";"
        driver.engine.dialect = dialect
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchone.return_value = (1,)
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        self.assertTrue(driver._perform_connection_test())
        executed = str(mock_connection.execute.call_args[0][0])
        self.assertNotEqual(executed.strip(), ";", "不能把 DBAPI 探活字符串当查询执行")
        self.assertIn("SELECT 1", executed)

    def test_connection_test_falls_back_when_compile_fails(self) -> None:
        """方言编译失败时回退到通用写法，不抛异常"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        dialect = _dialect_stub("SELECT 1")
        dialect.statement_compiler.side_effect = TypeError("不支持的方言")
        driver.engine.dialect = dialect
        mock_connection = MagicMock()
        mock_connection.execute.return_value.fetchone.return_value = (1,)
        driver.engine.connect.return_value.__enter__.return_value = mock_connection

        self.assertTrue(driver._perform_connection_test())
        executed = mock_connection.execute.call_args[0][0]
        self.assertIn("SELECT 1", str(executed))

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
        with patch.object(
            driver, "_perform_connection_test", side_effect=Exception("连接失败")
        ):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_os_error(self) -> None:
        """测试连接测试时的OS错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(
            driver, "_perform_connection_test", side_effect=OSError("网络错误")
        ):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_value_error(self) -> None:
        """测试连接测试时的值错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(
            driver, "_perform_connection_test", side_effect=ValueError("配置错误")
        ):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_attribute_error(self) -> None:
        """测试连接测试时的属性错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(
            driver, "_perform_connection_test", side_effect=AttributeError("属性错误")
        ):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_test_connection_type_error(self) -> None:
        """测试连接测试时的类型错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(
            driver, "_perform_connection_test", side_effect=TypeError("类型错误")
        ):
            result = driver.test_connection()
            self.assertFalse(result)

    def test_connect_with_existing_engine(self) -> None:
        """测试连接时引擎已存在的情况"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch.object(driver, "disconnect") as mock_disconnect:
            with patch(
                "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
            ) as mock_create_engine:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_recycle"], 3600)

    def test_execute_sql_no_engine(self) -> None:
        """测试execute_sql方法在engine未初始化时的处理"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
            mock_engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["users", "posts"]
            with patch(
                "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
                return_value=mock_inspector,
            ):
                mock_create_engine.return_value = mock_engine
                tables = driver.get_tables()
                self.assertEqual(tables, ["users", "posts"])

    def test_get_table_schema_no_engine(self) -> None:
        """测试get_table_schema方法在engine未初始化时的处理"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = None
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
            mock_engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_columns.return_value = [
                {"name": "id", "type": "INTEGER", "nullable": False},
                {"name": "name", "type": "VARCHAR", "nullable": True},
            ]
            with patch(
                "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
                return_value=mock_inspector,
            ):
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.inspect"
        ) as mock_inspect:
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.side_effect = SQLAlchemyError(
                "Database error"
            )
            mock_inspect.return_value = mock_inspector
            with self.assertRaises(QueryError) as context:
                driver.get_tables()
            self.assertIn("获取表列表失败", str(context.exception))

    def test_get_table_schema_sqlalchemy_error(self) -> None:
        """测试get_table_schema方法的SQLAlchemyError处理"""
        from sqlalchemy.exc import SQLAlchemyError

        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.inspect"
        ) as mock_inspect:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
            mock_create_engine.return_value = MagicMock()
            driver.connect()
            args, kwargs = mock_create_engine.call_args
            self.assertEqual(kwargs["pool_size"], 10)
            self.assertEqual(kwargs["max_overflow"], 20)

    def test_connect_sqlalchemy_error(self) -> None:
        """测试连接时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        from sqlalchemy.exc import SQLAlchemyError

        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine",
            side_effect=SQLAlchemyError("连接错误"),
        ):
            from src.db_connector_tool.core.exceptions import DBConnectionError

            with self.assertRaises(DBConnectionError):
                driver.connect()

    def test_connect_generic_error(self) -> None:
        """测试连接时的通用错误"""
        driver = SQLAlchemyDriver(self.base_config)
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine",
            side_effect=Exception("未知错误"),
        ):
            from src.db_connector_tool.core.exceptions import DBConnectionError

            with self.assertRaises(DBConnectionError):
                driver.connect()

    def test_disconnect_with_sqlalchemy_error(self) -> None:
        """测试断开连接时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError

        driver.engine.dispose.side_effect = SQLAlchemyError("断开错误")
        # 应该不会抛出异常
        driver.disconnect()

    def test_disconnect_with_generic_error(self) -> None:
        """测试断开连接时的通用错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
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
        driver._validate_sql_query(
            "CREATE PROCEDURE get_user() BEGIN SELECT * FROM users; END"
        )
        driver._validate_sql_query(
            "CREATE FUNCTION get_id() RETURNS INT BEGIN RETURN 1; END"
        )
        driver._validate_sql_query(
            "CREATE TRIGGER user_trigger AFTER INSERT ON users BEGIN END"
        )

    def test_validate_sql_query_suspicious_comment(self) -> None:
        """测试验证包含可疑注释的查询"""
        driver = SQLAlchemyDriver(self.base_config)
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users WHERE id = 1' --")
        with self.assertRaises(ValueError):
            driver._validate_sql_query("SELECT * FROM users /*!50000 SELECT */")
        with self.assertRaises(ValueError):
            driver._validate_sql_query(
                "SELECT * FROM users; /* comment */ SELECT * FROM orders"
            )

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
        from src.db_connector_tool.drivers.sqlalchemy_driver import (
            parse_kingbase_version,
        )

        # 模拟连接对象
        mock_connection = MagicMock()
        # 测试 PostgreSQL 格式的版本字符串
        mock_connection.exec_driver_sql.return_value.scalar.return_value = (
            "PostgreSQL 8.6.0 on x86_64"
        )
        version = parse_kingbase_version(None, mock_connection)
        self.assertEqual(version, (8, 6, 0))

        # 测试 Kingbase 格式的版本字符串
        mock_connection.exec_driver_sql.return_value.scalar.return_value = (
            "Kingbase V8R6C4B10"
        )
        version = parse_kingbase_version(None, mock_connection)
        self.assertEqual(version, (8, 6, 4))

        # 测试无法解析的版本字符串
        mock_connection.exec_driver_sql.return_value.scalar.return_value = (
            "Unknown version"
        )
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
        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.create_engine"
        ) as mock_create_engine:
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
        driver.disconnect()
        mock_engine.dispose.assert_called_once()
        self.assertIsNone(driver.engine)

    def test_test_connection_database_error(self) -> None:
        """测试连接测试时的数据库错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError

        with patch.object(
            driver,
            "_perform_connection_test",
            side_effect=SQLAlchemyError("数据库错误"),
        ):
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
            mock_result.mappings.return_value.all.return_value = [{"id": 1}]
            mock_connection.execute.return_value = mock_result
            driver.engine.connect.return_value.__enter__.return_value = mock_connection
            results = driver.execute_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0]["id"], 1)

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
            with patch(
                "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
                return_value=mock_inspector,
            ):
                tables = driver.get_tables()
                self.assertEqual(len(tables), 1)

    def test_get_tables_sqlalchemy_error(self) -> None:
        """测试获取表列表时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError

        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
            side_effect=SQLAlchemyError("数据库错误"),
        ):
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
            mock_inspector.get_columns.return_value = [
                {"name": "id", "type": "INTEGER"}
            ]
            with patch(
                "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
                return_value=mock_inspector,
            ):
                schema = driver.get_table_schema("users")
                self.assertEqual(len(schema), 1)

    def test_get_table_schema_sqlalchemy_error(self) -> None:
        """测试获取表结构时的SQLAlchemy错误"""
        driver = SQLAlchemyDriver(self.base_config)
        driver.engine = MagicMock()
        from sqlalchemy.exc import SQLAlchemyError

        with patch(
            "src.db_connector_tool.drivers.sqlalchemy_driver.inspect",
            side_effect=SQLAlchemyError("数据库错误"),
        ):
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
            "gbase8s",
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


class TestSQLServerCharsetDefault(unittest.TestCase):
    """SQL Server 的 charset 默认值必须是 utf8

    回归守卫：早期默认是 ``cp936``(GBK)，导致 emoji / 德文变音等
    非 GBK 字符**直接抛 UnicodeEncodeError**，而列本是 NVARCHAR(Unicode)，
    完全可以安全存储。实测 ``测试😀`` / ``Grüße`` 在 cp936 下写入失败。
    """

    def test_default_charset_is_utf8(self) -> None:
        """默认 charset 是 utf8，不是 cp936"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        defaults = SQLAlchemyDriver.DB_CONFIGS["sqlserver"]["defaults"]
        self.assertEqual(
            defaults.get("charset"), "utf8",
            msg="SQL Server 的 charset 必须为 utf8，否则非 GBK 字符无法写入",
        )

    def test_tds_version_preserved(self) -> None:
        """tds_version 保持原值（改动 charset 不应影响它）"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        defaults = SQLAlchemyDriver.DB_CONFIGS["sqlserver"]["defaults"]
        self.assertEqual(defaults.get("tds_version"), "7.0")

    def test_other_dialects_unchanged(self) -> None:
        """其它方言的 charset 不受影响（防止误改）"""
        from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

        configs = SQLAlchemyDriver.DB_CONFIGS
        self.assertEqual(configs["mysql"]["defaults"].get("charset"), "utf8mb4")
        self.assertEqual(
            configs["postgresql"]["defaults"].get("client_encoding"), "utf8"
        )
