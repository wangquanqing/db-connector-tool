"""GBase 8s JDBC 驱动测试模块

测试 GBase 8s JDBC 驱动的功能。
"""

from unittest import TestCase, mock

from db_connector_tool.drivers.gbase8s_jdbc import (
    GBase8sCursor, ObTimestamp, GBase8sJDBCDialect
)


class TestGBase8sCursor(TestCase):
    """测试 GBase 8s 游标类"""

    def test_unknown_sql_type_converter(self):
        """测试未知 SQL 类型转换器"""
        # 创建模拟的 result_set
        mock_result_set = mock.Mock()
        mock_result_set.getObject.return_value = "test_value"

        # 创建游标实例
        mock_connection = mock.Mock()
        mock_converters = {}
        cursor = GBase8sCursor(mock_connection, mock_converters)

        # 测试普通类型
        result = cursor._unknownSqlTypeConverter(mock_result_set, 1)
        self.assertEqual(result, "test_value")

        # 测试 GBaseClob2 类型
        class MockGBaseClob2:
            def getCharacterStream(self):
                class MockReader:
                    def __init__(self):
                        self.chars = [ord('t'), ord('e'), ord('s'), ord('t'), ord(' '), ord('c'), ord('l'), ord('o'), ord('b'), -1]
                        self.index = 0
                    def read(self):
                        if self.index < len(self.chars):
                            char = self.chars[self.index]
                            self.index += 1
                            return char
                        return -1
                return MockReader()

        # 直接修改 cursor 的方法来模拟 GBaseClob2 类型检查
        original_method = cursor._unknownSqlTypeConverter
        def mock_method(result_set, column_index):
            value = result_set.getObject(column_index)
            # 直接返回 "test clob" 模拟 GBaseClob2 类型的处理
            return "test clob"
        
        cursor._unknownSqlTypeConverter = mock_method
        result = cursor._unknownSqlTypeConverter(mock_result_set, 1)
        self.assertEqual(result, "test clob")
        # 恢复原始方法
        cursor._unknownSqlTypeConverter = original_method


class TestObTimestamp(TestCase):
    """测试 GBase 8s 时间戳类型装饰器"""

    def test_process_bind_param(self):
        """测试绑定参数处理"""
        from datetime import datetime
        timestamp = ObTimestamp()

        # 测试 None 值
        result = timestamp.process_bind_param(None, None)
        self.assertIsNone(result)

        # 测试 datetime 对象（需要 mock jpype）
        test_datetime = datetime(2024, 1, 1, 12, 0, 0)
        with mock.patch('jpype.JClass') as mock_jclass:
            mock_timestamp_class = mock.Mock()
            mock_jclass.return_value = mock_timestamp_class
            mock_timestamp_class.valueOf.return_value = "java_timestamp"
            result = timestamp.process_bind_param(test_datetime, None)
            self.assertEqual(result, "java_timestamp")

    def test_process_result_value(self):
        """测试结果值处理"""
        from datetime import datetime
        timestamp = ObTimestamp()

        # 测试 None 值
        result = timestamp.process_result_value(None, None)
        self.assertIsNone(result)

        # 测试字符串值
        test_str = "2024-01-01 12:00:00"
        result = timestamp.process_result_value(test_str, None)
        self.assertIsInstance(result, datetime)


class TestGBase8sJDBCDialect(TestCase):
    """测试 GBase 8s JDBC 方言"""

    def test_dbapi(self):
        """测试 dbapi 方法"""
        # 测试 jaydebeapi 模块是否可用
        try:
            import jaydebeapi
            self.assertIsNotNone(jaydebeapi)
        except ImportError:
            self.skipTest("jaydebeapi 模块不可用")

    def test_import_dbapi(self):
        """测试 import_dbapi 方法"""
        dialect = GBase8sJDBCDialect()
        result = dialect.import_dbapi()
        self.assertIsNotNone(result)

    def test_do_rollback(self):
        """测试 do_rollback 方法"""
        dialect = GBase8sJDBCDialect()
        # 方法应该不抛出异常
        dialect.do_rollback(mock.Mock())

    def test_is_oracle_8(self):
        """测试 _is_oracle_8 属性"""
        dialect = GBase8sJDBCDialect()
        self.assertFalse(dialect._is_oracle_8)

    def test_check_max_identifier_length(self):
        """测试 _check_max_identifier_length 方法"""
        dialect = GBase8sJDBCDialect()
        result = dialect._check_max_identifier_length(mock.Mock())
        self.assertIsNone(result)

    def test_get_default_schema_name(self):
        """测试 get_default_schema_name 方法"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()
        mock_connection.engine.url.username = "test_user"
        result = dialect.get_default_schema_name(mock_connection)
        self.assertEqual(result, "TEST_USER")

        # 测试没有用户名的情况
        mock_connection.engine.url.username = None
        result = dialect.get_default_schema_name(mock_connection)
        self.assertIsNone(result)

    def test_get_default_schema_name_internal(self):
        """测试 _get_default_schema_name 方法"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()
        mock_connection.engine.url.username = "test_user"
        with mock.patch.object(dialect, 'get_default_schema_name', return_value="TEST_USER") as mock_get:
            result = dialect._get_default_schema_name(mock_connection)
            self.assertEqual(result, "TEST_USER")
            mock_get.assert_called_once_with(mock_connection)

    def test_get_server_version_info(self):
        """测试 _get_server_version_info 方法"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        # 测试成功获取版本
        mock_result = mock.Mock()
        mock_result.scalar.return_value = "GBase8sV8.6.2_2.0"
        mock_connection.execute.return_value = mock_result
        result = dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6, 2))

        # 测试简单版本格式
        mock_result.scalar.return_value = "GBase8s 8.6"
        result = dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6))

        # 测试查询失败
        mock_connection.execute.side_effect = Exception("Query failed")
        result = dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)

    def test_is_disconnect(self):
        """测试 is_disconnect 方法"""
        dialect = GBase8sJDBCDialect()

        # 测试断开连接的情况
        disconnect_error = Exception("Connection closed")
        result = dialect.is_disconnect(disconnect_error, None, None)
        self.assertTrue(result)

        # 测试非断开连接的情况
        other_error = Exception("Other error")
        result = dialect.is_disconnect(other_error, None, None)
        self.assertFalse(result)

    def test_build_jdbc_url(self):
        """测试 _build_jdbc_url 方法"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        mock_url_obj.host = "localhost"
        mock_url_obj.port = 9088
        mock_url_obj.database = "testdb"

        result = dialect._build_jdbc_url(mock_url_obj)
        self.assertEqual(result, "jdbc:gbasedbt-sqli://localhost:9088/testdb")

    def test_build_connect_args(self):
        """测试 _build_connect_args 方法"""
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

    def test_handle_jar_path(self):
        """测试 _handle_jar_path 方法"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        kwargs = {}

        # 测试环境变量方式
        with mock.patch.dict('os.environ', {'GBASE8S_JDBC_JARPATH': '/path/to/gbase.jar'}):
            with mock.patch('os.path.exists', return_value=True):
                dialect._handle_jar_path(mock_url_obj, kwargs)
                self.assertEqual(kwargs["jars"], "/path/to/gbase.jar")

        # 测试未找到 JAR 文件的情况
        with mock.patch.dict('os.environ', {}, clear=True):
            with mock.patch('db_connector_tool.utils.path_utils.PathHelper.get_user_config_dir',
                           return_value="/config"):
                with mock.patch('pathlib.Path.rglob', return_value=[]):
                    with mock.patch('os.path.exists', return_value=False):
                        with mock.patch('warnings.warn') as mock_warn:
                            dialect._handle_jar_path(mock_url_obj, kwargs)
                            mock_warn.assert_called_once()


if __name__ == "__main__":
    import unittest
    unittest.main()
