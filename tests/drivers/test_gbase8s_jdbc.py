"""GBase 8s JDBC 驱动测试模块

测试 GBase 8s JDBC 驱动的功能。
"""

from unittest import TestCase, mock

from src.db_connector_tool.drivers.gbase8s_jdbc import (
    GBase8sCursor,
    GBase8sJDBCDialect,
    ObTimestamp,
)


class TestGBase8sCursor(TestCase):
    """测试 GBase 8s 游标类"""

    def test_init(self):
        """测试游标初始化"""
        mock_connection = mock.Mock()
        mock_converters = {}
        with mock.patch("jaydebeapi.Cursor.__init__", return_value=None):
            cursor = GBase8sCursor(mock_connection, mock_converters)
            self.assertIsNotNone(cursor)


class TestObTimestamp(TestCase):
    """测试 GBase 8s 时间戳类型装饰器"""

    def test_process_bind_param_none(self):
        """测试 None 值的绑定参数处理"""
        timestamp = ObTimestamp()
        result = timestamp.process_bind_param(None, None)
        self.assertIsNone(result)

    def test_process_bind_param_non_datetime(self):
        """测试非 datetime 类型的绑定参数处理"""
        timestamp = ObTimestamp()
        result = timestamp.process_bind_param("not a datetime", None)
        self.assertEqual(result, "not a datetime")

    def test_process_bind_param_datetime(self):
        """测试 datetime 对象的绑定参数处理"""
        from datetime import datetime

        timestamp = ObTimestamp()
        test_datetime = datetime(2024, 1, 1, 12, 0, 0)
        with mock.patch("jpype.JClass") as mock_jclass:
            mock_timestamp_class = mock.Mock()
            mock_jclass.return_value = mock_timestamp_class
            mock_timestamp_class.valueOf.return_value = "java_timestamp"
            result = timestamp.process_bind_param(test_datetime, None)
            self.assertEqual(result, "java_timestamp")

    def test_process_result_value(self):
        """测试结果值处理"""
        from datetime import datetime

        timestamp = ObTimestamp()

        result = timestamp.process_result_value(None, None)
        self.assertIsNone(result)

        test_str = "2024-01-01 12:00:00"
        result = timestamp.process_result_value(test_str, None)
        self.assertIsInstance(result, datetime)


class TestGBase8sJDBCDialect(TestCase):
    """测试 GBase 8s JDBC 方言"""

    def test_import_dbapi(self):
        """测试 import_dbapi 方法"""
        dialect = GBase8sJDBCDialect()
        result = dialect.import_dbapi()
        self.assertIsNotNone(result)

    def test_do_rollback(self):
        """测试 do_rollback 方法"""
        dialect = GBase8sJDBCDialect()
        dialect.do_rollback(mock.Mock())

    def test_is_oracle_8(self):
        """测试 _is_oracle_8 属性"""
        dialect = GBase8sJDBCDialect()
        self.assertFalse(dialect._is_oracle_8)

    def test_get_default_schema_name(self):
        """测试 get_default_schema_name 方法"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()
        mock_connection.engine.url.username = "test_user"
        result = dialect.get_default_schema_name(mock_connection)
        self.assertEqual(result, "TEST_USER")

        mock_connection.engine.url.username = None
        result = dialect.get_default_schema_name(mock_connection)
        self.assertIsNone(result)

    def test_get_default_schema_name_internal(self):
        """测试 _get_default_schema_name 方法"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()
        mock_connection.engine.url.username = "test_user"
        with mock.patch.object(
            dialect, "get_default_schema_name", return_value="TEST_USER"
        ) as mock_get:
            result = dialect._get_default_schema_name(mock_connection)
            self.assertEqual(result, "TEST_USER")
            mock_get.assert_called_once_with(mock_connection)

    def test_get_server_version_info_success(self):
        """测试成功获取服务器版本信息"""
        from sqlalchemy import exc

        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        mock_result = mock.Mock()
        mock_result.scalar.return_value = "GBase8sV8.6.2_2.0"
        mock_connection.execute.return_value = mock_result
        result = dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6, 2))

    def test_get_server_version_info_simple(self):
        """测试简单版本格式"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        mock_result = mock.Mock()
        mock_result.scalar.return_value = "GBase8s 8.6"
        mock_connection.execute.return_value = mock_result
        result = dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6))

    def test_get_server_version_info_no_match(self):
        """测试无法匹配版本格式"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        mock_result = mock.Mock()
        mock_result.scalar.return_value = "Unknown version string"
        mock_connection.execute.return_value = mock_result
        result = dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)

    def test_is_disconnect(self):
        """测试 is_disconnect 方法"""
        dialect = GBase8sJDBCDialect()

        indicators = [
            "Connection closed",
            "Socket closed",
            "Broken pipe",
            "Connection reset",
            "JDBC connection",
            "Network error",
        ]
        for indicator in indicators:
            error = Exception(indicator)
            result = dialect.is_disconnect(error, None, None)
            self.assertTrue(result)

        other_error = Exception("Other error")
        result = dialect.is_disconnect(other_error, None, None)
        self.assertFalse(result)

    def test_build_jdbc_url_full(self):
        """测试完整的 JDBC URL 构建"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.host = "localhost"
        mock_url_obj.port = 9088
        mock_url_obj.database = "testdb"

        result = dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost:9088/testdb")

    def test_build_jdbc_url_no_port(self):
        """测试没有端口的 JDBC URL 构建"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.host = "localhost"
        mock_url_obj.port = None
        mock_url_obj.database = "testdb"

        result = dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost/testdb")

    def test_build_jdbc_url_no_database(self):
        """测试没有数据库的 JDBC URL 构建"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.host = "localhost"
        mock_url_obj.port = 9088
        mock_url_obj.database = None

        result = dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost:9088")

    def test_build_connect_args_full(self):
        """测试完整的连接参数构建"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.username = "user"
        mock_url_obj.password = "pass"
        mock_url_obj.query = {"param1": "value1"}

        result = dialect._build_connect_args(mock_url_obj)
        self.assertEqual(result["user"], "user")
        self.assertEqual(result["password"], "pass")
        self.assertEqual(result["param1"], "value1")
        self.assertEqual(result["rewriteBatchedStatements"], "true")

    def test_build_connect_args_minimal(self):
        """测试最小连接参数构建"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.username = None
        mock_url_obj.password = None
        mock_url_obj.query = None

        result = dialect._build_connect_args(mock_url_obj)
        self.assertNotIn("user", result)
        self.assertNotIn("password", result)
        self.assertEqual(result["rewriteBatchedStatements"], "true")

    def test_build_connect_args_non_string_query(self):
        """测试非字符串查询参数"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.username = None
        mock_url_obj.password = None
        mock_url_obj.query = {"param1": 123}

        result = dialect._build_connect_args(mock_url_obj)
        self.assertEqual(result["param1"], "123")

    def test_build_connect_args_with_rewrite(self):
        """测试已存在 rewriteBatchedStatements 参数"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.username = None
        mock_url_obj.password = None
        mock_url_obj.query = {"rewriteBatchedStatements": "false"}

        result = dialect._build_connect_args(mock_url_obj)
        self.assertEqual(result["rewriteBatchedStatements"], "false")

    def test_create_connect_args(self):
        """测试 create_connect_args 方法"""
        dialect = GBase8sJDBCDialect()
        mock_url = mock.Mock()

        with mock.patch("sqlalchemy.engine.url.make_url", return_value=mock_url):
            with mock.patch.object(dialect, "_build_jdbc_url", return_value="jdbc:url"):
                with mock.patch.object(
                    dialect, "_build_connect_args", return_value={"user": "test"}
                ):
                    with mock.patch.object(dialect, "_handle_jar_path") as mock_handle:
                        args, kwargs = dialect.create_connect_args(mock_url)
                        self.assertEqual(args, ())
                        self.assertEqual(
                            kwargs["jclassname"], "com.gbasedbt.jdbc.Driver"
                        )
                        self.assertEqual(kwargs["url"], "jdbc:url")
                        self.assertEqual(kwargs["driver_args"], {"user": "test"})
                        mock_handle.assert_called_once()


if __name__ == "__main__":
    import unittest

    unittest.main()
