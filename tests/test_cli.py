import argparse
import unittest
from unittest import mock

from src.db_connector_tool.cli import DBConnectorCLI, create_argument_parser, main


class TestDBConnectorCLI(unittest.TestCase):
    """测试命令行接口"""

    def setUp(self):
        """设置测试环境"""
        self.cli = DBConnectorCLI()

    def test_ensure_db_manager_initialized(self):
        """测试确保数据库管理器已初始化"""
        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            db_manager = self.cli._ensure_db_manager_initialized()
            mock_db_manager.assert_called_once()
            self.assertIsNotNone(self.cli.db_manager)

    def test_build_connection_config(self):
        """测试构建连接配置"""

        # 创建模拟参数
        class Args(argparse.Namespace):
            type = "mysql"
            host = "localhost"
            port = 3306
            username = "root"
            password = "password"
            database = "test_db"
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = ["timeout=30", "ssl=true"]

        args = Args()
        config = self.cli._build_connection_config(args)

        self.assertEqual(config["type"], "mysql")
        self.assertEqual(config["host"], "localhost")
        self.assertEqual(config["port"], 3306)
        self.assertEqual(config["username"], "root")
        self.assertEqual(config["password"], "password")
        self.assertEqual(config["database"], "test_db")
        self.assertEqual(config["timeout"], 30)
        self.assertEqual(config["ssl"], True)

    def test_parse_custom_params(self):
        """测试解析自定义参数"""
        params = ["timeout=30", "ssl=true", "name=test"]
        result = self.cli._parse_custom_params(params)

        self.assertEqual(result["timeout"], 30)
        self.assertEqual(result["ssl"], True)
        self.assertEqual(result["name"], "test")

    def test_convert_value_type(self):
        """测试值类型转换"""
        self.assertEqual(self.cli._convert_value_type("true"), True)
        self.assertEqual(self.cli._convert_value_type("false"), False)
        self.assertEqual(self.cli._convert_value_type("123"), 123)
        self.assertEqual(self.cli._convert_value_type("3.14"), 3.14)
        self.assertEqual(self.cli._convert_value_type("test"), "test")

    def test_sanitize_sensitive_info(self):
        """测试隐藏敏感信息"""
        config = {
            "type": "mysql",
            "password": "secret",
            "passwd": "password",
            "pwd": "pwd",
        }
        safe_config = self.cli._sanitize_sensitive_info(config)

        self.assertEqual(safe_config["type"], "mysql")
        self.assertEqual(safe_config["password"], "***")
        self.assertEqual(safe_config["passwd"], "***")
        self.assertEqual(safe_config["pwd"], "***")

    def test_build_update_config(self):
        """测试构建更新配置"""
        existing_config = {"type": "mysql", "host": "old_host", "port": 3306}

        class Args(argparse.Namespace):
            type = "mysql"
            host = "new_host"
            port = None
            username = None
            password = None
            database = None
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = None

        args = Args()
        update_config = self.cli._build_update_config(existing_config, args)

        self.assertEqual(update_config["type"], "mysql")
        self.assertEqual(update_config["host"], "new_host")
        self.assertEqual(update_config["port"], 3306)

    def test_read_and_split_sql_file(self):
        """测试读取和分割SQL文件"""
        # 创建临时SQL文件
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
            f.write("SELECT * FROM users;\nINSERT INTO users VALUES (1, 'test');")
            temp_file = f.name

        try:
            statements = self.cli._read_and_split_sql_file(temp_file)
            self.assertEqual(len(statements), 2)
            self.assertEqual(statements[0], "SELECT * FROM users;")
            self.assertEqual(statements[1], "INSERT INTO users VALUES (1, 'test');")
        finally:
            os.unlink(temp_file)

    def test_split_sql_statements(self):
        """测试分割SQL语句"""
        sql_content = "SELECT * FROM users;\nINSERT INTO users VALUES (1, 'test');"
        statements = self.cli._split_sql_statements(sql_content)

        self.assertEqual(len(statements), 2)
        self.assertEqual(statements[0], "SELECT * FROM users;")
        self.assertEqual(statements[1], "INSERT INTO users VALUES (1, 'test');")

    def test_truncate_sql(self):
        """测试截断SQL语句"""
        long_sql = "SELECT * FROM users WHERE id = 1 AND name = 'test' AND age > 18"
        truncated = self.cli._truncate_sql(long_sql, 20)
        self.assertEqual(truncated, "SELECT * FROM users...")

    def test_create_argument_parser(self):
        """测试创建参数解析器"""
        parser = create_argument_parser(self.cli)
        self.assertIsInstance(parser, argparse.ArgumentParser)

    def test_main_with_help(self):
        """测试主函数（显示帮助）"""
        with mock.patch("sys.argv", ["db-connector"]):
            with mock.patch("argparse.ArgumentParser.print_help"):
                with mock.patch("sys.exit") as mock_exit:
                    main()
                    mock_exit.assert_called_once_with(0)

    def test_main_with_version(self):
        """测试主函数（显示版本）"""
        with mock.patch("sys.argv", ["db-connector", "--version"]):
            with mock.patch("db_connector_tool.cli.DBConnectorCLI.show_version"):
                with mock.patch("sys.exit") as mock_exit:
                    main()
                    mock_exit.assert_called_once_with(0)

    def test_add_connection(self):
        """测试添加连接命令"""

        class Args(argparse.Namespace):
            name = "test_conn"
            type = "mysql"
            host = "localhost"
            port = 3306
            username = "root"
            password = "password"
            database = "test_db"
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = None

        args = Args()

        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            with mock.patch("sys.exit"):
                mock_instance = mock_db_manager.return_value
                self.cli.add_connection(args)
                mock_instance.add_connection.assert_called_once()

    def test_remove_connection(self):
        """测试删除连接命令"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            with mock.patch("sys.exit"):
                mock_instance = mock_db_manager.return_value
                self.cli.remove_connection(args)
                mock_instance.remove_connection.assert_called_once_with("test_conn")

    def test_list_connections(self):
        """测试列出连接命令"""

        class Args(argparse.Namespace):
            pass

        args = Args()

        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            mock_instance = mock_db_manager.return_value
            mock_instance.list_connections.return_value = ["conn1", "conn2"]
            self.cli.list_connections(args)
            mock_instance.list_connections.assert_called_once()

    def test_test_connection(self):
        """测试测试连接命令"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            with mock.patch("sys.exit"):
                mock_instance = mock_db_manager.return_value
                mock_instance.test_connection.return_value = True
                self.cli.test_connection(args)
                mock_instance.test_connection.assert_called_once_with("test_conn")

    def test_execute_query(self):
        """测试执行查询命令"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            query = "SELECT * FROM users"
            output = None
            format = "table"

        args = Args()

        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            with mock.patch("db_connector_tool.cli.DBConnectorCLI._display_results"):
                mock_instance = mock_db_manager.return_value
                mock_instance.execute_query.return_value = [{"id": 1, "name": "test"}]
                self.cli.execute_query(args)
                mock_instance.execute_query.assert_called_once_with(
                    "test_conn", "SELECT * FROM users"
                )

    def test_execute_file(self):
        """测试执行SQL文件命令"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["SELECT * FROM users"],
            ):
                with mock.patch(
                    "db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([{"id": 1, "name": "test"}], 1, 0),
                ):
                    with mock.patch(
                        "db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch(
                            "db_connector_tool.cli.DBConnectorCLI._display_results"
                        ):
                            self.cli.execute_file(args)

    def test_interactive_shell(self):
        """测试交互式Shell"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        with mock.patch("db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            with mock.patch("builtins.input", side_effect=["exit"]):
                with mock.patch(
                    "db_connector_tool.cli.DBConnectorCLI._print_shell_help"
                ):
                    mock_instance = mock_db_manager.return_value
                    self.cli.interactive_shell(args)


if __name__ == "__main__":
    unittest.main()
