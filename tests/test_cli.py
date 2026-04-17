import argparse
import os
import sys
import tempfile
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
        # 测试初始状态
        self.assertIsNone(self.cli.db_manager)

        # 直接设置一个 mock 实例到 db_manager 属性
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        # 再次调用方法，应该返回已存在的实例
        result = self.cli._ensure_db_manager_initialized()
        self.assertEqual(result, mock_instance)
        self.assertEqual(self.cli.db_manager, mock_instance)

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

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit"):
            self.cli.add_connection(args)
            mock_instance.add_connection.assert_called_once()

    def test_remove_connection(self):
        """测试删除连接命令"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit"):
            self.cli.remove_connection(args)
            mock_instance.remove_connection.assert_called_once_with("test_conn")

    def test_list_connections(self):
        """测试列出连接命令"""

        class Args(argparse.Namespace):
            pass

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.list_connections.return_value = ["conn1", "conn2"]
        self.cli.db_manager = mock_instance

        self.cli.list_connections(args)
        mock_instance.list_connections.assert_called_once()

    def test_test_connection(self):
        """测试测试连接命令"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.test_connection.return_value = True
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit"):
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

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.execute_query.return_value = [{"id": 1, "name": "test"}]
        self.cli.db_manager = mock_instance

        with mock.patch("db_connector_tool.cli.DBConnectorCLI._display_results"):
            with mock.patch("sys.exit"):
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

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

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
                            with mock.patch("sys.exit"):
                                self.cli.execute_file(args)

    def test_interactive_shell(self):
        """测试交互式Shell"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("builtins.input", side_effect=["exit"]):
            with mock.patch("db_connector_tool.cli.DBConnectorCLI._print_shell_help"):
                self.cli.interactive_shell(args)

    def test_update_connection(self):
        """测试更新连接命令"""

        class Args(argparse.Namespace):
            name = "test_conn"
            type = "mysql"
            host = "new_host"
            port = 3306
            username = "root"
            password = "new_password"
            database = "test_db"
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = None

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.show_connection.return_value = {
            "type": "mysql",
            "host": "old_host",
        }
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit"):
            self.cli.update_connection(args)
            mock_instance.update_connection.assert_called_once()

    def test_show_connection(self):
        """测试显示连接详情命令"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.show_connection.return_value = {
            "type": "mysql",
            "host": "localhost",
        }
        self.cli.db_manager = mock_instance

        with mock.patch(
            "db_connector_tool.cli.DBConnectorCLI._display_connection_details"
        ):
            with mock.patch("sys.exit"):
                self.cli.show_connection(args)
                mock_instance.show_connection.assert_called_once_with("test_conn")

    def test_save_output_json(self):
        """测试保存输出为JSON格式"""
        results = [{"id": 1, "name": "test"}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            self.cli._save_output(results, output_path, "json")
            with open(output_path, "r") as f:
                content = f.read()
            self.assertIn("id", content)
            self.assertIn("test", content)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_save_output_csv(self):
        """测试保存输出为CSV格式"""
        results = [{"id": 1, "name": "test"}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            self.cli._save_output(results, output_path, "csv")
            with open(output_path, "r") as f:
                content = f.read()
            self.assertIn("id", content)
            self.assertIn("name", content)
            self.assertIn("test", content)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_display_results_table(self):
        """测试以表格形式显示结果"""
        results = [{"id": 1, "name": "test"}]
        with mock.patch("db_connector_tool.cli.DBConnectorCLI._display_table"):
            self.cli._display_results(results, "table")

    def test_display_results_json(self):
        """测试以JSON形式显示结果"""
        results = [{"id": 1, "name": "test"}]
        with mock.patch("db_connector_tool.cli.DBConnectorCLI._display_json"):
            self.cli._display_results(results, "json")

    def test_display_results_csv(self):
        """测试以CSV形式显示结果"""
        results = [{"id": 1, "name": "test"}]
        with mock.patch("db_connector_tool.cli.DBConnectorCLI._display_csv"):
            self.cli._display_results(results, "csv")

    def test_display_results_invalid_format(self):
        """测试无效的输出格式"""
        results = [{"id": 1, "name": "test"}]
        with mock.patch("sys.exit"):
            self.cli._display_results(results, "invalid")

    def test_execute_sql_statements(self):
        """测试执行SQL语句列表"""
        mock_db_manager = mock.Mock()
        mock_db_manager.execute_query.return_value = [{"id": 1, "name": "test"}]
        mock_db_manager.execute_command.return_value = 1

        statements = ["SELECT * FROM users", "INSERT INTO users VALUES (1, 'test')"]
        results, success_count, error_count = self.cli._execute_sql_statements(
            mock_db_manager, statements, "test_conn", False
        )

        self.assertEqual(len(results), 1)
        self.assertEqual(success_count, 2)
        self.assertEqual(error_count, 0)

    def test_print_execution_summary(self):
        """测试打印执行统计信息"""
        with mock.patch("builtins.print"):
            self.cli._print_execution_summary(2, 1)

    def test_print_shell_help(self):
        """测试打印Shell帮助信息"""
        with mock.patch("builtins.print"):
            self.cli._print_shell_help()

    def test_print_custom_params(self):
        """测试打印自定义参数"""
        config = {"type": "mysql", "host": "localhost", "custom_param": "value"}
        with mock.patch("builtins.print"):
            self.cli._print_custom_params(config)

    def test_display_connection_details(self):
        """测试显示连接配置详情"""
        config = {"type": "mysql", "host": "localhost"}
        with mock.patch("builtins.print"):
            self.cli._display_connection_details(config)

    def test_truncate_value(self):
        """测试截断值"""
        long_value = "This is a very long value that needs to be truncated"
        truncated = self.cli._truncate_value(long_value, 20)
        self.assertEqual(truncated, "This is a very lo...")

    def test_show_version(self):
        """测试显示版本信息"""

        class Args(argparse.Namespace):
            pass

        args = Args()
        with mock.patch("builtins.print"):
            self.cli.show_version(args)

    def test_ensure_db_manager_initialized_new(self):
        """测试初始化数据库管理器"""
        # 创建新的 CLI 实例以确保状态干净
        cli = DBConnectorCLI()

        # 模拟DatabaseManager初始化成功
        with mock.patch("src.db_connector_tool.cli.DatabaseManager") as mock_db_manager:
            result = cli._ensure_db_manager_initialized()
            mock_db_manager.assert_called_once()
            self.assertEqual(result, mock_db_manager.return_value)

    def test_ensure_db_manager_initialized_error(self):
        """测试初始化数据库管理器失败"""
        # 创建新的 CLI 实例以确保状态干净
        cli = DBConnectorCLI()

        # 模拟DatabaseManager初始化失败
        with mock.patch(
            "src.db_connector_tool.cli.DatabaseManager",
            side_effect=Exception("初始化失败"),
        ):
            with mock.patch("sys.exit") as mock_exit:
                cli._ensure_db_manager_initialized()
                mock_exit.assert_called_once_with(1)

    def test_add_connection_error(self):
        """测试添加连接失败"""

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

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.add_connection.side_effect = Exception("添加失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.add_connection(args)
            mock_exit.assert_called_once_with(1)

    def test_build_connection_config_with_empty_password(self):
        """测试构建连接配置（空密码）"""

        class Args(argparse.Namespace):
            type = "mysql"
            host = "localhost"
            port = 3306
            username = "root"
            password = ""
            database = "test_db"
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = None

        args = Args()

        with mock.patch("getpass.getpass", return_value="input_password"):
            config = self.cli._build_connection_config(args)
            self.assertEqual(config["password"], "input_password")

    def test_parse_custom_params_invalid(self):
        """测试解析无效的自定义参数"""
        params = ["invalid_param", "key=", "=value"]
        result = self.cli._parse_custom_params(params)
        # 只有"key="会被解析为{'key': ''}，其他无效参数会被忽略
        self.assertEqual(result, {"key": ""})

    def test_build_update_config_with_empty_password(self):
        """测试构建更新配置（空密码）"""
        existing_config = {"type": "mysql", "host": "old_host", "port": 3306}

        class Args(argparse.Namespace):
            type = "mysql"
            host = "new_host"
            port = None
            username = None
            password = ""
            database = None
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = None

        args = Args()

        with mock.patch("getpass.getpass", return_value="input_password"):
            update_config = self.cli._build_update_config(existing_config, args)
            self.assertEqual(update_config["password"], "input_password")

    def test_execute_sql_statements_with_error(self):
        """测试执行SQL语句失败"""
        mock_db_manager = mock.Mock()
        mock_db_manager.execute_query.side_effect = Exception("执行失败")

        statements = ["SELECT * FROM users"]

        with mock.patch("sys.exit") as mock_exit:
            self.cli._execute_sql_statements(
                mock_db_manager, statements, "test_conn", False
            )
            mock_exit.assert_called_once_with(1)

    def test_display_json_error(self):
        """测试显示JSON结果失败"""
        results = [{"id": 1, "name": "test"}]

        with mock.patch("json.dumps", side_effect=Exception("JSON序列化失败")):
            with mock.patch("sys.exit") as mock_exit:
                self.cli._display_json(results)
                mock_exit.assert_called_once_with(1)

    def test_read_and_split_sql_file_decode_error(self):
        """测试读取SQL文件解码失败"""
        # 创建临时SQL文件
        import os
        import tempfile

        with tempfile.NamedTemporaryFile(mode="wb", suffix=".sql", delete=False) as f:
            f.write(b"\xff\xfeSELECT * FROM users;")  # 写入无效的UTF-8数据
            temp_file = f.name

        try:
            with mock.patch("sys.exit") as mock_exit:
                self.cli._read_and_split_sql_file(temp_file)
                mock_exit.assert_called_once_with(1)
        finally:
            if os.path.exists(temp_file):
                os.unlink(temp_file)

    def test_execute_file_not_exists(self):
        """测试执行不存在的SQL文件"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "non_existent.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=False):
            with mock.patch("sys.exit") as mock_exit:
                self.cli.execute_file(args)
                # 期望sys.exit被调用，不关心具体次数
                mock_exit.assert_called()

    def test_execute_file_no_statements(self):
        """测试执行空SQL文件"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "empty.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=[],
            ):
                with mock.patch("builtins.print") as mock_print:
                    self.cli.execute_file(args)
                    mock_print.assert_any_call("ℹ️  SQL文件中没有有效的SQL语句")

    def test_interactive_shell_error(self):
        """测试交互式Shell执行错误"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        mock_instance.execute_query.side_effect = Exception("执行错误")
        self.cli.db_manager = mock_instance

        with mock.patch("builtins.input", side_effect=["SELECT * FROM users", "exit"]):
            with mock.patch("builtins.print") as mock_print:
                self.cli.interactive_shell(args)
                mock_print.assert_any_call("❌ 执行错误: 执行错误")

    def test_interactive_shell_keyboard_interrupt(self):
        """测试交互式Shell键盘中断"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("builtins.input", side_effect=KeyboardInterrupt):
            with mock.patch("builtins.print") as mock_print:
                self.cli.interactive_shell(args)
                mock_print.assert_any_call("\n👋 再见!")

    def test_interactive_shell_help(self):
        """测试交互式Shell帮助命令"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        # 直接创建 mock 实例并设置到 cli.db_manager
        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("builtins.input", side_effect=["help", "exit"]):
            with mock.patch("builtins.print") as mock_print:
                self.cli.interactive_shell(args)
                # 验证帮助信息被打印（检查是否有包含帮助信息的调用）
                help_called = any(
                    "SQL Shell 命令:" in str(call) for call in mock_print.call_args_list
                )
                self.assertTrue(help_called)

    def test_remove_connection_error(self):
        """测试删除连接失败"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.remove_connection.side_effect = Exception("删除失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.remove_connection(args)
            mock_exit.assert_called_once_with(1)

    def test_update_connection_error(self):
        """测试更新连接失败"""

        class Args(argparse.Namespace):
            name = "test_conn"
            type = "mysql"
            host = "new_host"
            port = 3306
            username = "root"
            password = "new_password"
            database = "test_db"
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = None

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.show_connection.side_effect = Exception("更新失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.update_connection(args)
            mock_exit.assert_called_once_with(1)

    def test_show_connection_error(self):
        """测试显示连接详情失败"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.show_connection.side_effect = Exception("获取失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.show_connection(args)
            mock_exit.assert_called_once_with(1)

    def test_list_connections_error(self):
        """测试列出连接失败"""

        class Args(argparse.Namespace):
            pass

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.list_connections.side_effect = Exception("列出失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.list_connections(args)
            mock_exit.assert_called_once_with(1)

    def test_test_connection_false(self):
        """测试连接测试返回False"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.test_connection.return_value = False
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.test_connection(args)
            mock_exit.assert_called_once_with(1)

    def test_test_connection_error(self):
        """测试连接测试异常"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.test_connection.side_effect = Exception("测试失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.test_connection(args)
            mock_exit.assert_called_once_with(1)

    def test_execute_query_with_output(self):
        """测试执行查询并保存输出"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            query = "SELECT * FROM users"
            output = "output.json"
            format = "json"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.execute_query.return_value = [{"id": 1, "name": "test"}]
        self.cli.db_manager = mock_instance

        with mock.patch(
            "src.db_connector_tool.cli.DBConnectorCLI._save_output"
        ) as mock_save:
            with mock.patch("sys.exit"):
                self.cli.execute_query(args)
                mock_save.assert_called_once()

    def test_execute_query_error(self):
        """测试执行查询失败"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            query = "SELECT * FROM users"
            output = None
            format = "table"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.execute_query.side_effect = Exception("查询失败")
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit") as mock_exit:
            self.cli.execute_query(args)
            mock_exit.assert_called_once_with(1)

    def test_save_output_table(self):
        """测试保存输出为表格格式"""
        results = [{"id": 1, "name": "test"}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = f.name

        try:
            with mock.patch("db_connector_tool.cli.DBConnectorCLI._display_table"):
                self.cli._save_output(results, output_path, "table")
                # 验证文件存在
                self.assertTrue(os.path.exists(output_path))
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_save_output_error(self):
        """测试保存输出失败"""
        results = [{"id": 1, "name": "test"}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            output_path = f.name

        try:
            with mock.patch("json.dump", side_effect=Exception("保存失败")):
                with mock.patch("sys.exit") as mock_exit:
                    self.cli._save_output(results, output_path, "json")
                    mock_exit.assert_called_once_with(1)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_display_table_empty(self):
        """测试显示空结果表格"""
        results = []
        with mock.patch("builtins.print") as mock_print:
            self.cli._display_table(results)
            mock_print.assert_not_called()

    def test_display_results_empty(self):
        """测试显示空结果"""
        results = []
        with mock.patch("builtins.print") as mock_print:
            self.cli._display_results(results, "table")
            mock_print.assert_called_once_with("没有结果")

    def test_display_csv_empty(self):
        """测试显示空CSV结果"""
        results = []
        with mock.patch("csv.DictWriter") as mock_writer:
            self.cli._display_csv(results)
            mock_writer.assert_not_called()

    def test_execute_file_with_output(self):
        """测试执行SQL文件并保存输出"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = "output.json"
            format = "json"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["SELECT * FROM users"],
            ):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([{"id": 1, "name": "test"}], 1, 0),
                ):
                    with mock.patch(
                        "src.db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch(
                            "src.db_connector_tool.cli.DBConnectorCLI._save_output"
                        ) as mock_save:
                            with mock.patch("sys.exit"):
                                self.cli.execute_file(args)
                                mock_save.assert_called_once()

    def test_execute_file_error(self):
        """测试执行SQL文件异常"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                side_effect=Exception("读取失败"),
            ):
                with mock.patch("sys.exit") as mock_exit:
                    self.cli.execute_file(args)
                    mock_exit.assert_called_once_with(1)

    def test_split_sql_statements_various_cases(self):
        """测试分割SQL语句的各种情况"""
        # 测试单行注释
        sql_content = "-- 这是注释\nSELECT * FROM users;"
        statements = self.cli._split_sql_statements(sql_content)
        self.assertEqual(len(statements), 1)
        self.assertIn("SELECT * FROM users", statements[0])

        # 测试多行注释
        sql_content = "/* 这是多行\n注释 */ SELECT * FROM users;"
        statements = self.cli._split_sql_statements(sql_content)
        self.assertEqual(len(statements), 1)

        # 测试无分号结尾
        sql_content = "SELECT * FROM users"
        statements = self.cli._split_sql_statements(sql_content)
        self.assertEqual(len(statements), 1)

    def test_execute_sql_statements_continue_on_error(self):
        """测试执行SQL语句时继续错误"""
        mock_db_manager = mock.Mock()
        mock_db_manager.execute_query.side_effect = Exception("执行失败")
        mock_db_manager.execute_command.return_value = 1

        statements = ["SELECT * FROM users", "INSERT INTO users VALUES (1, 'test')"]

        results, success_count, error_count = self.cli._execute_sql_statements(
            mock_db_manager, statements, "test_conn", True
        )

        self.assertEqual(success_count, 1)
        self.assertEqual(error_count, 1)

    def test_execute_sql_statements_empty(self):
        """测试执行空SQL语句"""
        mock_db_manager = mock.Mock()
        statements = ["", "   "]
        results, success_count, error_count = self.cli._execute_sql_statements(
            mock_db_manager, statements, "test_conn", False
        )
        self.assertEqual(len(results), 0)

    def test_interactive_shell_various_commands(self):
        """测试交互式Shell各种命令"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.execute_query.return_value = [{"id": 1, "name": "test"}]
        mock_instance.execute_command.return_value = 1
        self.cli.db_manager = mock_instance

        # 测试空输入、非SELECT语句
        with mock.patch(
            "builtins.input",
            side_effect=["", "INSERT INTO users VALUES (1, 'test')", "exit"],
        ):
            with mock.patch("builtins.print"):
                with mock.patch(
                    "db_connector_tool.cli.DBConnectorCLI._display_results"
                ):
                    self.cli.interactive_shell(args)
                    mock_instance.execute_command.assert_called_once()

    def test_interactive_shell_init_error(self):
        """测试交互式Shell初始化错误"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        self.cli.db_manager = None

        with mock.patch(
            "src.db_connector_tool.cli.DBConnectorCLI._ensure_db_manager_initialized",
            side_effect=Exception("初始化失败"),
        ):
            with mock.patch("sys.exit") as mock_exit:
                with mock.patch("builtins.print"):
                    try:
                        self.cli.interactive_shell(args)
                    except SystemExit:
                        pass
                    except Exception:
                        pass

    def test_main_no_func(self):
        """测试主函数无func分支"""
        with mock.patch("sys.argv", ["db-connector", "invalid_command"]):
            with mock.patch("argparse.ArgumentParser.parse_args") as mock_parse:
                mock_args = mock.Mock()
                mock_args.version = False
                delattr(mock_args, "func")
                mock_parse.return_value = mock_args
                with mock.patch(
                    "argparse.ArgumentParser.print_help"
                ) as mock_print_help:
                    main()
                    mock_print_help.assert_called_once()

    def test_list_connections_empty(self):
        """测试列出空连接"""

        class Args(argparse.Namespace):
            pass

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.list_connections.return_value = []
        self.cli.db_manager = mock_instance

        with mock.patch("builtins.print") as mock_print:
            self.cli.list_connections(args)
            mock_print.assert_any_call("ℹ️  没有配置任何连接")

    def test_execute_file_with_results_and_output(self):
        """测试执行SQL文件有结果并保存输出"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = "output.json"
            format = "json"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["SELECT * FROM users"],
            ):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([{"id": 1, "name": "test"}], 1, 0),
                ):
                    with mock.patch(
                        "src.db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch(
                            "src.db_connector_tool.cli.DBConnectorCLI._save_output"
                        ) as mock_save:
                            with mock.patch("sys.exit"):
                                self.cli.execute_file(args)
                                mock_save.assert_called_once()

    def test_save_output_table_format(self):
        """测试保存输出为真正的表格格式"""
        results = [{"id": 1, "name": "test"}]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            output_path = f.name

        try:
            self.cli._save_output(results, output_path, "table")
            with open(output_path, "r") as f:
                content = f.read()
            self.assertIn("id", content)
            self.assertIn("name", content)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_update_connection_with_custom_params(self):
        """测试更新连接时包含自定义参数"""

        class Args(argparse.Namespace):
            name = "test_conn"
            type = "mysql"
            host = "new_host"
            port = 3306
            username = "root"
            password = "new_password"
            database = "test_db"
            service_name = None
            gssencmode = None
            charset = None
            tds_version = None
            custom_params = ["timeout=60", "ssl=true"]

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.show_connection.return_value = {
            "type": "mysql",
            "host": "old_host",
        }
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit"):
            self.cli.update_connection(args)
            mock_instance.update_connection.assert_called_once()

    def test_show_connection_with_custom_params(self):
        """测试显示连接详情时包含自定义参数"""

        class Args(argparse.Namespace):
            name = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.show_connection.return_value = {
            "type": "mysql",
            "host": "localhost",
            "custom_param": "value",
        }
        self.cli.db_manager = mock_instance

        with mock.patch("sys.exit"):
            with mock.patch("builtins.print"):
                self.cli.show_connection(args)

    def test_execute_file_real_save_output(self):
        """测试执行SQL文件时真正保存输出"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = "output.json"
            format = "json"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["SELECT * FROM users"],
            ):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([{"id": 1, "name": "test"}], 1, 0),
                ):
                    with mock.patch(
                        "src.db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch("sys.exit"):
                            with tempfile.NamedTemporaryFile(
                                mode="w", suffix=".json", delete=False
                            ) as f:
                                args.output = f.name
                            try:
                                self.cli.execute_file(args)
                            finally:
                                if os.path.exists(args.output):
                                    os.unlink(args.output)

    def test_interactive_shell_with_exception_handling(self):
        """测试交互式Shell的完整异常处理流程"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        self.cli.db_manager = None

        with mock.patch(
            "src.db_connector_tool.cli.DBConnectorCLI._ensure_db_manager_initialized",
            side_effect=Exception("初始化失败"),
        ):
            with mock.patch("sys.exit") as mock_exit:
                with mock.patch("builtins.print"):
                    try:
                        self.cli.interactive_shell(args)
                    except SystemExit:
                        pass
                    except Exception:
                        pass

    def test_execute_file_display_results(self):
        """测试执行SQL文件时显示结果而不保存"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["SELECT * FROM users"],
            ):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([{"id": 1, "name": "test"}], 1, 0),
                ):
                    with mock.patch(
                        "src.db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch(
                            "src.db_connector_tool.cli.DBConnectorCLI._display_results"
                        ) as mock_display:
                            with mock.patch("sys.exit"):
                                self.cli.execute_file(args)
                                mock_display.assert_called_once()

    def test_main_no_func_branch(self):
        """测试主函数无func分支的完整流程"""
        with mock.patch("sys.argv", ["db-connector", "invalid_command"]):
            with mock.patch("argparse.ArgumentParser.parse_args") as mock_parse:
                mock_args = mock.Mock()
                mock_args.version = False
                delattr(mock_args, "func")
                mock_parse.return_value = mock_args
                with mock.patch(
                    "argparse.ArgumentParser.print_help"
                ) as mock_print_help:
                    main()
                    mock_print_help.assert_called_once()

    def test_save_output_csv_empty_results(self):
        """测试保存空结果的CSV文件"""
        results = []

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            self.cli._save_output(results, output_path, "csv")
            self.assertTrue(os.path.exists(output_path))
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_interactive_shell_outer_exception(self):
        """测试交互式Shell的外层异常处理"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        self.cli.db_manager = None

        with mock.patch(
            "src.db_connector_tool.cli.DBConnectorCLI._ensure_db_manager_initialized"
        ) as mock_init:
            mock_init.side_effect = Exception("初始化失败")
            with mock.patch("sys.exit") as mock_exit:
                with mock.patch("builtins.print"):
                    try:
                        self.cli.interactive_shell(args)
                    except SystemExit:
                        pass
                    except Exception:
                        pass

    def test_build_connection_config_no_custom_params(self):
        """测试构建连接配置时没有自定义参数"""

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

        args = Args()
        config = self.cli._build_connection_config(args)
        self.assertEqual(config["type"], "mysql")

    def test_interactive_shell_complete_coverage(self):
        """测试交互式Shell的完整覆盖，包括异常分支和非SELECT语句"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        mock_instance = mock.Mock()
        mock_instance.execute_command.return_value = 5
        mock_instance.execute_query.return_value = [{"id": 1, "name": "test"}]
        self.cli.db_manager = mock_instance

        # 测试SELECT语句结果显示 - 不需要mock display
        with mock.patch("builtins.input", side_effect=["SELECT * FROM users", "exit"]):
            with mock.patch("builtins.print"):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._display_table"
                ):
                    self.cli.interactive_shell(args)

    def test_main_with_func(self):
        """测试主函数有func分支的完整覆盖"""
        with mock.patch("sys.argv", ["db-connector", "list"]):
            with mock.patch("argparse.ArgumentParser.parse_args") as mock_parse:
                mock_args = mock.Mock()
                mock_args.version = False
                mock_func = mock.Mock()
                mock_args.func = mock_func
                mock_parse.return_value = mock_args
                main()
                mock_func.assert_called_once()

    def test_interactive_shell_outer_exception(self):
        """测试交互式Shell的外层异常处理"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        self.cli.db_manager = None

        with mock.patch(
            "src.db_connector_tool.cli.DBConnectorCLI._ensure_db_manager_initialized"
        ) as mock_init:
            mock_init.side_effect = Exception("启动失败")
            with mock.patch("sys.exit"):
                with mock.patch("builtins.print"):
                    try:
                        self.cli.interactive_shell(args)
                    except Exception:
                        pass

    def test_execute_file_final_coverage(self):
        """测试execute_file的最后几个分支，确保完全覆盖"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["SELECT * FROM users"],
            ):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([{"id": 1, "name": "test"}], 1, 0),
                ):
                    with mock.patch(
                        "src.db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch(
                            "src.db_connector_tool.cli.DBConnectorCLI._display_results"
                        ):
                            with mock.patch("sys.exit"):
                                self.cli.execute_file(args)

    def test_execute_file_no_results(self):
        """测试execute_file没有结果的情况"""

        class Args(argparse.Namespace):
            connection = "test_conn"
            file = "test.sql"
            output = None
            format = "table"
            continue_on_error = False

        args = Args()

        mock_instance = mock.Mock()
        self.cli.db_manager = mock_instance

        with mock.patch("os.path.exists", return_value=True):
            with mock.patch(
                "src.db_connector_tool.cli.DBConnectorCLI._read_and_split_sql_file",
                return_value=["INSERT INTO users VALUES (1, 'test')"],
            ):
                with mock.patch(
                    "src.db_connector_tool.cli.DBConnectorCLI._execute_sql_statements",
                    return_value=([], 1, 0),
                ):
                    with mock.patch(
                        "src.db_connector_tool.cli.DBConnectorCLI._print_execution_summary"
                    ):
                        with mock.patch("sys.exit"):
                            self.cli.execute_file(args)

    def test_interactive_shell_outer_exception_complete(self):
        """测试交互式Shell的外层异常处理的完整覆盖"""

        class Args(argparse.Namespace):
            connection = "test_conn"

        args = Args()

        # 先设置 db_manager
        mock_db_manager = mock.Mock()
        self.cli.db_manager = mock_db_manager

        # 让第 825 行的 print 抛出异常，但外层异常处理中的 print 可以正常运行
        call_count = [0]

        def print_side_effect(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise Exception("启动失败")
            return mock.DEFAULT

        with mock.patch("builtins.print", side_effect=print_side_effect):
            try:
                self.cli.interactive_shell(args)
            except SystemExit:
                pass
            except Exception:
                pass


if __name__ == "__main__":
    unittest.main()
