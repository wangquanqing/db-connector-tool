import unittest
from typing import Dict, Any

from src.db_connector_tool.core.exceptions import (
    DBConnectorError,
    ConfigError,
    CryptoError,
    DatabaseError,
    DBConnectionError,
    DriverError,
    QueryError,
    ValidationError,
    FileSystemError,
    DBTimeoutError,
)


class TestDBConnectorError(unittest.TestCase):
    """测试 DBConnectorError 基础异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = DBConnectorError("测试异常")
        self.assertEqual(error.message, "测试异常")
        self.assertIsNone(error.error_code)
        self.assertEqual(error.details, {})

    def test_init_with_error_code(self) -> None:
        """测试带错误代码的初始化"""
        error = DBConnectorError("测试异常", "TEST_001")
        self.assertEqual(error.message, "测试异常")
        self.assertEqual(error.error_code, "TEST_001")
        self.assertEqual(error.details, {})

    def test_init_with_details(self) -> None:
        """测试带详细信息的初始化"""
        details = {"key": "value", "number": 42}
        error = DBConnectorError("测试异常", "TEST_001", details)
        self.assertEqual(error.message, "测试异常")
        self.assertEqual(error.error_code, "TEST_001")
        self.assertEqual(error.details, details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error1 = DBConnectorError("测试异常")
        self.assertIn("DBConnectorError: 测试异常", str(error1))

        error2 = DBConnectorError("测试异常", "TEST_001")
        self.assertIn("DBConnectorError: 测试异常 (错误代码: TEST_001)", str(error2))

    def test_to_dict(self) -> None:
        """测试转换为字典"""
        details = {"key": "value"}
        error = DBConnectorError("测试异常", "TEST_001", details)
        error_dict = error.to_dict()
        
        self.assertEqual(error_dict["error_type"], "DBConnectorError")
        self.assertEqual(error_dict["message"], "测试异常")
        self.assertEqual(error_dict["error_code"], "TEST_001")
        self.assertEqual(error_dict["details"], details)


class TestConfigError(unittest.TestCase):
    """测试 ConfigError 配置异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = ConfigError("配置错误")
        self.assertEqual(error.message, "配置错误")
        self.assertIsNone(error.config_file)
        self.assertIsNone(error.config_section)
        self.assertIsNone(error.config_key)

    def test_init_with_config_info(self) -> None:
        """测试带配置信息的初始化"""
        error = ConfigError(
            "配置文件格式错误",
            "CONFIG_001",
            config_file="connections.toml",
            config_section="database",
            config_key="host"
        )
        self.assertEqual(error.message, "配置文件格式错误")
        self.assertEqual(error.error_code, "CONFIG_001")
        self.assertEqual(error.config_file, "connections.toml")
        self.assertEqual(error.config_section, "database")
        self.assertEqual(error.config_key, "host")
        self.assertIn("config_file", error.details)
        self.assertIn("config_section", error.details)
        self.assertIn("config_key", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = ConfigError("配置错误", "CONFIG_001")
        self.assertIn("ConfigError: 配置错误 (错误代码: CONFIG_001)", str(error))


class TestCryptoError(unittest.TestCase):
    """测试 CryptoError 加密异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = CryptoError("加密失败")
        self.assertEqual(error.message, "加密失败")
        self.assertIsNone(error.operation)
        self.assertIsNone(error.algorithm)

    def test_init_with_crypto_info(self) -> None:
        """测试带加密信息的初始化"""
        error = CryptoError(
            "加密失败",
            "CRYPTO_001",
            operation="encrypt",
            algorithm="AES-256"
        )
        self.assertEqual(error.message, "加密失败")
        self.assertEqual(error.error_code, "CRYPTO_001")
        self.assertEqual(error.operation, "encrypt")
        self.assertEqual(error.algorithm, "AES-256")
        self.assertIn("operation", error.details)
        self.assertIn("algorithm", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = CryptoError("加密失败", "CRYPTO_001")
        self.assertIn("CryptoError: 加密失败 (错误代码: CRYPTO_001)", str(error))


class TestDatabaseError(unittest.TestCase):
    """测试 DatabaseError 数据库基础异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = DatabaseError("数据库操作失败")
        self.assertEqual(error.message, "数据库操作失败")
        self.assertIsNone(error.database_type)
        self.assertIsNone(error.operation)

    def test_init_with_db_info(self) -> None:
        """测试带数据库信息的初始化"""
        error = DatabaseError(
            "数据库操作失败",
            "DB_001",
            database_type="mysql",
            operation="query"
        )
        self.assertEqual(error.message, "数据库操作失败")
        self.assertEqual(error.error_code, "DB_001")
        self.assertEqual(error.database_type, "mysql")
        self.assertEqual(error.operation, "query")
        self.assertIn("database_type", error.details)
        self.assertIn("operation", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = DatabaseError("数据库操作失败", "DB_001")
        self.assertIn("DatabaseError: 数据库操作失败 (错误代码: DB_001)", str(error))


class TestDBConnectionError(unittest.TestCase):
    """测试 DBConnectionError 数据库连接异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = DBConnectionError("连接失败")
        self.assertEqual(error.message, "连接失败")
        self.assertIsNone(error.connection_name)
        self.assertIsNone(error.host)
        self.assertIsNone(error.port)
        self.assertIsNone(error.database)

    def test_init_with_connection_info(self) -> None:
        """测试带连接信息的初始化"""
        error = DBConnectionError(
            "连接失败",
            "CONN_001",
            connection_name="main_db",
            host="localhost",
            port=3306,
            database="test_db"
        )
        self.assertEqual(error.message, "连接失败")
        self.assertEqual(error.error_code, "CONN_001")
        self.assertEqual(error.connection_name, "main_db")
        self.assertEqual(error.host, "localhost")
        self.assertEqual(error.port, 3306)
        self.assertEqual(error.database, "test_db")
        self.assertIn("connection_name", error.details)
        self.assertIn("host", error.details)
        self.assertIn("port", error.details)
        self.assertIn("database", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = DBConnectionError("连接失败", "CONN_001")
        self.assertIn("DBConnectionError: 连接失败 (错误代码: CONN_001)", str(error))


class TestDriverError(unittest.TestCase):
    """测试 DriverError 数据库驱动异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = DriverError("驱动加载失败")
        self.assertEqual(error.message, "驱动加载失败")
        self.assertIsNone(error.driver_name)
        self.assertIsNone(error.driver_version)

    def test_init_with_driver_info(self) -> None:
        """测试带驱动信息的初始化"""
        error = DriverError(
            "驱动加载失败",
            "DRIVER_001",
            driver_name="mysql-connector",
            driver_version="8.0.0"
        )
        self.assertEqual(error.message, "驱动加载失败")
        self.assertEqual(error.error_code, "DRIVER_001")
        self.assertEqual(error.driver_name, "mysql-connector")
        self.assertEqual(error.driver_version, "8.0.0")
        self.assertIn("driver_name", error.details)
        self.assertIn("driver_version", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = DriverError("驱动加载失败", "DRIVER_001")
        self.assertIn("DriverError: 驱动加载失败 (错误代码: DRIVER_001)", str(error))


class TestQueryError(unittest.TestCase):
    """测试 QueryError 查询执行异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = QueryError("查询错误")
        self.assertEqual(error.message, "查询错误")
        self.assertIsNone(error.query)
        self.assertIsNone(error.query_type)
        self.assertIsNone(error.parameters)

    def test_init_with_query_info(self) -> None:
        """测试带查询信息的初始化"""
        error = QueryError(
            "查询语法错误",
            "QUERY_001",
            query="SELECT * FROM users WHERE id = ?",
            query_type="SELECT",
            parameters={"id": 1}
        )
        self.assertEqual(error.message, "查询语法错误")
        self.assertEqual(error.error_code, "QUERY_001")
        self.assertEqual(error.query, "SELECT * FROM users WHERE id = ?")
        self.assertEqual(error.query_type, "SELECT")
        self.assertEqual(error.parameters, {"id": 1})
        self.assertIn("query_preview", error.details)
        self.assertIn("query_type", error.details)
        self.assertIn("parameter_keys", error.details)

    def test_get_query_preview(self) -> None:
        """测试获取查询预览"""
        error = QueryError("查询错误")
        
        # 测试短查询
        short_query = "SELECT * FROM users"
        preview = error._get_query_preview(short_query)
        self.assertEqual(preview, short_query)
        
        # 测试长查询
        long_query = "SELECT * FROM users WHERE id = 1 AND name = 'test' AND age > 18"
        preview = error._get_query_preview(long_query, max_length=20)
        self.assertEqual(preview, "SELECT * FROM users ...")

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = QueryError("查询错误", "QUERY_001")
        self.assertIn("QueryError: 查询错误 (错误代码: QUERY_001)", str(error))


class TestValidationError(unittest.TestCase):
    """测试 ValidationError 数据验证异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = ValidationError("验证失败")
        self.assertEqual(error.message, "验证失败")
        self.assertIsNone(error.field_name)
        self.assertIsNone(error.expected_type)
        self.assertIsNone(error.actual_value)
        self.assertIsNone(error.validation_rules)

    def test_init_with_validation_info(self) -> None:
        """测试带验证信息的初始化"""
        error = ValidationError(
            "参数验证失败",
            "VALID_001",
            field_name="username",
            expected_type="str",
            actual_value="test",
            validation_rules={"min_length": 3, "max_length": 20}
        )
        self.assertEqual(error.message, "参数验证失败")
        self.assertEqual(error.error_code, "VALID_001")
        self.assertEqual(error.field_name, "username")
        self.assertEqual(error.expected_type, "str")
        self.assertEqual(error.actual_value, "test")
        self.assertEqual(error.validation_rules, {"min_length": 3, "max_length": 20})
        self.assertIn("field_name", error.details)
        self.assertIn("expected_type", error.details)
        self.assertIn("validation_rules", error.details)
        # actual_value 不应该在 details 中
        self.assertNotIn("actual_value", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = ValidationError("验证失败", "VALID_001")
        self.assertIn("ValidationError: 验证失败 (错误代码: VALID_001)", str(error))


class TestFileSystemError(unittest.TestCase):
    """测试 FileSystemError 文件系统异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = FileSystemError("文件操作失败")
        self.assertEqual(error.message, "文件操作失败")
        self.assertIsNone(error.file_path)
        self.assertIsNone(error.operation)

    def test_init_with_file_info(self) -> None:
        """测试带文件信息的初始化"""
        error = FileSystemError(
            "文件读取失败",
            "FS_001",
            file_path="/path/to/file.txt",
            operation="read"
        )
        self.assertEqual(error.message, "文件读取失败")
        self.assertEqual(error.error_code, "FS_001")
        self.assertEqual(error.file_path, "/path/to/file.txt")
        self.assertEqual(error.operation, "read")
        self.assertIn("file_path", error.details)
        self.assertIn("operation", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = FileSystemError("文件操作失败", "FS_001")
        self.assertIn("FileSystemError: 文件操作失败 (错误代码: FS_001)", str(error))


class TestDBTimeoutError(unittest.TestCase):
    """测试 DBTimeoutError 超时异常类"""

    def test_init_basic(self) -> None:
        """测试基本初始化"""
        error = DBTimeoutError("操作超时")
        self.assertEqual(error.message, "操作超时")
        self.assertIsNone(error.timeout_seconds)
        self.assertIsNone(error.operation)

    def test_init_with_timeout_info(self) -> None:
        """测试带超时信息的初始化"""
        error = DBTimeoutError(
            "数据库查询超时",
            "TIMEOUT_001",
            timeout_seconds=30.0,
            operation="query"
        )
        self.assertEqual(error.message, "数据库查询超时")
        self.assertEqual(error.error_code, "TIMEOUT_001")
        self.assertEqual(error.timeout_seconds, 30.0)
        self.assertEqual(error.operation, "query")
        self.assertIn("timeout_seconds", error.details)
        self.assertIn("operation", error.details)

    def test_str_representation(self) -> None:
        """测试字符串表示"""
        error = DBTimeoutError("操作超时", "TIMEOUT_001")
        self.assertIn("DBTimeoutError: 操作超时 (错误代码: TIMEOUT_001)", str(error))


class TestExceptionHierarchy(unittest.TestCase):
    """测试异常类层次结构"""

    def test_inheritance(self) -> None:
        """测试异常类的继承关系"""
        # 测试所有异常都继承自 DBConnectorError
        self.assertTrue(issubclass(ConfigError, DBConnectorError))
        self.assertTrue(issubclass(CryptoError, DBConnectorError))
        self.assertTrue(issubclass(DatabaseError, DBConnectorError))
        self.assertTrue(issubclass(DBConnectionError, DatabaseError))
        self.assertTrue(issubclass(DriverError, DatabaseError))
        self.assertTrue(issubclass(QueryError, DatabaseError))
        self.assertTrue(issubclass(ValidationError, DBConnectorError))
        self.assertTrue(issubclass(FileSystemError, DBConnectorError))
        self.assertTrue(issubclass(DBTimeoutError, DBConnectorError))

    def test_exception_catching(self) -> None:
        """测试异常捕获"""
        # 测试可以用基类捕获子类异常
        try:
            raise ConfigError("配置错误")
        except DBConnectorError as e:
            self.assertIsInstance(e, ConfigError)
            self.assertIsInstance(e, DBConnectorError)

        try:
            raise DBConnectionError("连接错误")
        except DatabaseError as e:
            self.assertIsInstance(e, DBConnectionError)
            self.assertIsInstance(e, DatabaseError)
            self.assertIsInstance(e, DBConnectorError)


if __name__ == "__main__":
    unittest.main()
