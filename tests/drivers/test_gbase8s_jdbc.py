"""GBase 8s JDBC 驱动测试模块

测试 GBase 8s JDBC 驱动的功能。
"""

import os
import warnings
from unittest import TestCase, mock

from db_connector_tool.drivers.gbase8s_jdbc import (
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

    def test_unknown_sql_type_converter(self):
        """测试未知 SQL 类型转换器"""
        mock_result_set = mock.Mock()
        mock_result_set.getObject.return_value = "test_value"

        mock_connection = mock.Mock()
        mock_converters = {}
        with mock.patch("jaydebeapi.Cursor.__init__", return_value=None):
            cursor = GBase8sCursor(mock_connection, mock_converters)

        result = cursor._unknownSqlTypeConverter(mock_result_set, 1)
        self.assertEqual(result, "test_value")

    def test_unknown_sql_type_converter_gbaseclob2(self):
        """测试 GBaseClob2 类型转换器"""
        mock_connection = mock.Mock()
        mock_converters = {}
        with mock.patch("jaydebeapi.Cursor.__init__", return_value=None):
            cursor = GBase8sCursor(mock_connection, mock_converters)

        class MockReader:
            def __init__(self):
                self.chars = [
                    ord("t"),
                    ord("e"),
                    ord("s"),
                    ord("t"),
                    ord(" "),
                    ord("c"),
                    ord("l"),
                    ord("o"),
                    ord("b"),
                    -1,
                ]
                self.index = 0

            def read(self):
                if self.index < len(self.chars):
                    char = self.chars[self.index]
                    self.index += 1
                    return char
                return -1

        class MockGBaseClob2:
            def getCharacterStream(self):
                return MockReader()

            def __repr__(self):
                return "<java class 'com.gbasedbt.jdbc.GBaseClob2'>"

        mock_value = MockGBaseClob2()

        result_set = mock.Mock()
        result_set.getObject.return_value = mock_value

        with mock.patch(
            "db_connector_tool.drivers.gbase8s_jdbc.str",
            return_value="<java class 'com.gbasedbt.jdbc.GBaseClob2'>",
        ):
            result = cursor._unknownSqlTypeConverter(result_set, 1)
            self.assertEqual(result, "test clob")


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

    def test_initialize(self):
        """测试 initialize 方法"""
        dialect = GBase8sJDBCDialect()
        dialect.initialize(mock.Mock())

    def test_dbapi(self):
        """测试 dbapi 方法"""
        dialect = GBase8sJDBCDialect()
        import jaydebeapi

        original_cursor = jaydebeapi.Cursor
        try:
            result = GBase8sJDBCDialect.dbapi()
            self.assertIsNotNone(result)
            self.assertEqual(jaydebeapi.Cursor, GBase8sCursor)
        finally:
            jaydebeapi.Cursor = original_cursor

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

    def test_get_server_version_info_dbapi_error(self):
        """测试 DBAPIError 异常"""
        from sqlalchemy import exc

        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()
        mock_connection.execute.side_effect = exc.DBAPIError(
            statement=None, params=None, orig=None
        )
        result = dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)

    def test_get_server_version_info_general_exception(self):
        """测试一般异常"""
        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()
        mock_connection.execute.side_effect = Exception("Query failed")
        result = dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)

    def test_get_server_version_info_try_second_query(self):
        """测试尝试第二个查询"""
        from sqlalchemy import exc

        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        mock_result = mock.Mock()
        mock_result.scalar.return_value = "GBase8sV8.6.2"

        def side_effect(*args, **kwargs):
            if side_effect.counter == 0:
                side_effect.counter += 1
                raise exc.DBAPIError(statement=None, params=None, orig=None)
            side_effect.counter += 1
            return mock_result

        side_effect.counter = 0
        mock_connection.execute.side_effect = side_effect
        result = dialect._get_server_version_info(mock_connection)
        self.assertEqual(result, (8, 6, 2))

    def test_get_server_version_info_both_queries_fail(self):
        """测试两个查询都失败的情况"""
        from sqlalchemy import exc

        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        def side_effect(*args, **kwargs):
            raise exc.DBAPIError(statement=None, params=None, orig=None)

        mock_connection.execute.side_effect = side_effect
        result = dialect._get_server_version_info(mock_connection)
        self.assertIsNone(result)

    def test_get_server_version_info_outer_exception(self):
        """测试外层 try-except 块捕获异常的情况"""
        from sqlalchemy import exc

        dialect = GBase8sJDBCDialect()
        mock_connection = mock.Mock()

        mock_connection.execute.side_effect = exc.DBAPIError(
            statement=None, params=None, orig=None
        )
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

    def test_handle_jar_path_env_var_exists(self):
        """测试从环境变量获取 JAR 路径且文件存在"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        kwargs = {}

        with mock.patch.dict(
            "os.environ", {"GBASE8S_JDBC_JARPATH": "/path/to/gbase.jar"}
        ):
            with mock.patch("os.path.exists", return_value=True):
                dialect._handle_jar_path(mock_url_obj, kwargs)
                self.assertEqual(kwargs["jars"], "/path/to/gbase.jar")

    def test_handle_jar_path_env_var_not_exists(self):
        """测试从环境变量获取 JAR 路径但文件不存在"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        kwargs = {}

        with mock.patch.dict(
            "os.environ", {"GBASE8S_JDBC_JARPATH": "/path/to/gbase.jar"}
        ):
            with mock.patch("os.path.exists", return_value=False):
                with mock.patch(
                    "db_connector_tool.utils.path_utils.PathHelper.get_user_config_dir",
                    return_value="/config",
                ):
                    with mock.patch("warnings.warn") as mock_warn:
                        dialect._handle_jar_path(mock_url_obj, kwargs)
                        mock_warn.assert_called_once()
                        self.assertEqual(kwargs["jars"], "/path/to/gbase.jar")

    def test_handle_jar_path_default_dir_found(self):
        """测试从默认目录找到 JAR 文件"""
        from pathlib import Path

        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        kwargs = {}

        mock_file = mock.Mock()
        mock_file.is_file.return_value = True
        mock_file.__str__ = lambda self: "/config/jars/gbase.jar"

        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch(
                "db_connector_tool.utils.path_utils.PathHelper.get_user_config_dir",
                return_value="/config",
            ):
                with mock.patch.object(Path, "rglob", return_value=[mock_file]):
                    with mock.patch("os.path.exists", return_value=True):
                        dialect._handle_jar_path(mock_url_obj, kwargs)
                        self.assertEqual(kwargs["jars"], "/config/jars/gbase.jar")

    def test_handle_jar_path_default_dir_not_found(self):
        """测试从默认目录未找到 JAR 文件"""
        dialect = GBase8sJDBCDialect()
        mock_url_obj = mock.Mock()
        kwargs = {}

        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch(
                "db_connector_tool.utils.path_utils.PathHelper.get_user_config_dir",
                return_value="/config",
            ):
                with mock.patch("pathlib.Path.rglob", return_value=[]):
                    with mock.patch("os.path.exists", return_value=False):
                        with mock.patch("warnings.warn") as mock_warn:
                            dialect._handle_jar_path(mock_url_obj, kwargs)
                            mock_warn.assert_called_once()


if __name__ == "__main__":
    import unittest

    unittest.main()
