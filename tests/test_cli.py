"""命令行层测试 (CLI)

覆盖参数解析、连接配置构建、结果渲染与导出、SQL 文件执行、
各子命令处理函数以及入口函数的分发逻辑。
"""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from decimal import Decimal
from unittest import mock

from src.db_connector_tool.cli import create_argument_parser, main
from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
)
from src.db_connector_tool.core.result import ColumnInfo, QueryResult

from ._isolation import IsolatedTestCase


def make_result(rows, elapsed: float = 0.01, names=None):
    """构造 ``QueryResult`` 供 CLI 测试使用

    CLI 现在消费 ``execute_query_result()`` 返回的 ``QueryResult``
    （按位置保真），因此测试也要给出同形态的对象，而不是裸字典列表。

    Args:
        rows: 行数据；元素为字典时按字典值取列，否则视作按位置的序列
        elapsed: 耗时
        names: 列名列表；``None`` 时从数据推断（字典取键，序列用 ``c0/c1...``）

    Returns:
        QueryResult: 构造好的结果对象
    """
    materialized = [dict(row) if isinstance(row, dict) else tuple(row) for row in rows]
    if materialized and isinstance(rows[0], dict):
        column_names = list(rows[0].keys())
        position_rows = [tuple(row.values()) for row in materialized]
    else:
        position_rows = materialized
        if names is not None:
            column_names = list(names)
        elif position_rows:
            column_names = [f"c{i}" for i in range(len(position_rows[0]))]
        else:
            column_names = []
    columns = [
        ColumnInfo(ordinal=i, name=name, display_name=name)
        for i, name in enumerate(column_names)
    ]
    return QueryResult(columns=columns, rows=position_rows, elapsed=elapsed)


class CLITestCase(IsolatedTestCase):
    """CLI 测试基类：隔离配置目录 + 提供已注入模拟管理器的 CLI 实例"""

    def setUp(self) -> None:
        super().setUp()
        self.cli = DBConnectorCLI()
        self.cli.db_manager = mock.Mock()
        self.parser = create_argument_parser(self.cli)

    def parse(self, argv):
        """解析命令行参数"""
        return self.parser.parse_args(argv)

    @staticmethod
    def capture(callable_, *args, **kwargs):
        """执行调用并返回 (返回值, 标准输出)"""
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            result = callable_(*args, **kwargs)
        return result, buffer.getvalue()


class TestArgumentParser(CLITestCase):
    """命令行参数解析器"""

    def test_subcommands_registered(self) -> None:
        """所有子命令均已注册"""
        group_actions = (
            self.parser._subparsers._group_actions  # pylint: disable=protected-access
        )
        choices = set(group_actions[0].choices)
        self.assertEqual(
            choices,
            {
                "add",
                "remove",
                "update",
                "show",
                "list",
                "test",
                "query",
                "command",
                "file",
                "shell",
            },
        )

    def test_add_requires_type(self) -> None:
        """add 子命令的 --type 为必填"""
        with self.assertRaises(SystemExit):
            with redirect_stdout(io.StringIO()):
                self.parse(["add", "mysql-dev", "-H", "localhost"])

    def test_add_parses_connection_arguments(self) -> None:
        """add 子命令解析连接参数与自定义参数"""
        args = self.parse(
            [
                "add",
                "gb8s",
                "-T",
                "gbase8s",
                "-H",
                "127.0.0.1",
                "-P",
                "9088",
                "-u",
                "cvicse",
                "-d",
                "db_station",
                "-S",
                "gbase01",
                "-c",
                "sqlmode=oracle",
                "CLIENT_LOCALE=zh_CN.GB18030",
            ]
        )
        self.assertEqual(args.name, "gb8s")
        self.assertEqual(args.type, "gbase8s")
        self.assertEqual(args.port, 9088)
        self.assertEqual(args.server, "gbase01")
        self.assertEqual(args.custom_params, ["sqlmode=oracle", "CLIENT_LOCALE=zh_CN.GB18030"])

    def test_password_flag_without_value_means_prompt(self) -> None:
        """-p 不带值时以空串表示“提示输入密码”"""
        args = self.parse(["add", "db", "-T", "mysql", "-p"])
        self.assertEqual(args.password, "")

        args = self.parse(["add", "db", "-T", "mysql", "-p", "secret"])
        self.assertEqual(args.password, "secret")

    def test_file_subcommand_flags(self) -> None:
        """file 子命令支持导出与遇错继续"""
        args = self.parse(["file", "db", "init.sql", "-o", "out.csv", "-c"])
        self.assertEqual(args.output, "out.csv")
        self.assertTrue(args.continue_on_error)

    def test_query_and_command_arguments(self) -> None:
        """query/command 子命令解析 SQL 与输出参数"""
        args = self.parse(["query", "db", "SELECT 1", "-o", "r.json"])
        self.assertEqual(args.sql_content, "SELECT 1")
        self.assertEqual(args.output, "r.json")

        args = self.parse(["command", "db", "DELETE FROM t"])
        self.assertEqual(args.command, "command")

    def test_version_flag(self) -> None:
        """-v/--version 可被解析"""
        self.assertTrue(self.parse(["-v"]).version)


class TestConnectionConfigBuilding(CLITestCase):
    """连接配置构建"""

    def test_build_conn_config_from_args(self) -> None:
        """从命令行参数构建连接配置"""
        args = self.parse(
            ["add", "db", "-T", "mysql", "-H", "localhost", "-P", "3306", "-d", "app"]
        )
        config = self.cli._build_conn_config(args)  # pylint: disable=protected-access
        self.assertEqual(config["type"], "mysql")
        self.assertEqual(config["host"], "localhost")
        self.assertEqual(config["port"], 3306)
        self.assertEqual(config["database"], "app")
        self.assertNotIn("username", config)

    def test_build_conn_config_prompts_password(self) -> None:
        """-p 不带值时交互式读取密码"""
        args = self.parse(["add", "db", "-T", "mysql", "-p"])
        with mock.patch(
            "src.db_connector_tool.cli_core.getpass.getpass", return_value="prompted"
        ) as mock_getpass:
            config = self.cli._build_conn_config(args)  # pylint: disable=protected-access
        mock_getpass.assert_called_once()
        self.assertEqual(config["password"], "prompted")

    def test_build_conn_config_with_custom_params(self) -> None:
        """自定义参数被解析并合并进配置"""
        args = self.parse(
            ["add", "db", "-T", "postgresql", "-c", "connect_timeout=10", "sslmode=require"]
        )
        config = self.cli._build_conn_config(args)  # pylint: disable=protected-access
        self.assertEqual(config["connect_timeout"], 10)
        self.assertEqual(config["sslmode"], "require")

    def test_parse_custom_params_type_coercion(self) -> None:
        """自定义参数按布尔/整数/浮点/字符串依次尝试转换"""
        parsed = self.cli._parse_custom_params(  # pylint: disable=protected-access
            ["flag=true", "off=false", "n=42", "f=1.5", "s=abc", "bad", "=1"]
        )
        self.assertEqual(
            parsed,
            {"flag": True, "off": False, "n": 42, "f": 1.5, "s": "abc"},
        )

    def test_build_updated_conn_config_merges(self) -> None:
        """更新时仅覆盖命令行显式给出的字段"""
        args = self.parse(["update", "db", "-T", "mysql", "-H", "new-host"])
        updated = self.cli._build_updated_conn_config(  # pylint: disable=protected-access
            {"type": "mysql", "host": "old-host", "port": 3306, "database": "app"}, args
        )
        self.assertEqual(updated["host"], "new-host")
        self.assertEqual(updated["port"], 3306)
        self.assertEqual(updated["database"], "app")


class TestOutputHelpers(CLITestCase):
    """结果渲染与导出"""

    def test_mask_sensitive(self) -> None:
        """密码类字段被掩码"""
        masked = self.cli._mask_sensitive(  # pylint: disable=protected-access
            {"host": "h", "password": "p", "passwd": "p2", "pwd": "p3"}
        )
        self.assertEqual(masked["host"], "h")
        self.assertEqual(masked["password"], "***")
        self.assertEqual(masked["passwd"], "***")
        self.assertEqual(masked["pwd"], "***")

    def test_to_serializable(self) -> None:
        """驱动对象被转换为可序列化类型"""
        to_serializable = DBConnectorCLI._to_serializable  # pylint: disable=protected-access
        self.assertEqual(to_serializable({"a": 1}), {"a": 1})
        self.assertEqual(to_serializable([1, 2]), [1, 2])
        # JSON 原生类型原样保留，避免数字被写成字符串
        self.assertEqual(to_serializable(3), 3)
        self.assertEqual(to_serializable(1.5), 1.5)
        self.assertIs(to_serializable(True), True)
        self.assertEqual(to_serializable("x"), "x")
        self.assertIsNone(to_serializable(None))

    def test_to_serializable_decimal_keeps_precision(self) -> None:
        """Decimal 转字符串而非 float，避免丢精度"""
        to_serializable = DBConnectorCLI._to_serializable  # pylint: disable=protected-access
        self.assertEqual(to_serializable(Decimal("1.50")), "1.50")
        self.assertEqual(
            to_serializable(Decimal("12345678901234567.89")),
            "12345678901234567.89",
        )

    def test_to_serializable_java_object_not_lost(self) -> None:
        """回归：带空 __dict__ 的 Java 对象不再被转成 {}

        JPype 包装的 Java 数值（java.lang.Integer / JDouble）带一个空的
        ``__dict__``；若先走 ``__dict__`` 分支，导出 CSV 时整列变成 ``{}``。
        """

        class FakeJavaInteger:
            """模拟 JPype 包装对象：空 __dict__，不可迭代"""

            def __init__(self) -> None:
                self.__dict__ = {}

            def __str__(self) -> str:
                return "1"

        to_serializable = DBConnectorCLI._to_serializable  # pylint: disable=protected-access
        self.assertEqual(to_serializable(FakeJavaInteger()), "1")
        self.assertNotEqual(to_serializable(FakeJavaInteger()), {})

    def test_save_csv_keeps_java_object_values(self) -> None:
        """回归：CSV 导出不再写出 {} """

        class FakeJavaInteger:
            def __init__(self) -> None:
                self.__dict__ = {}

            def __str__(self) -> str:
                return "1"

        with tempfile.TemporaryDirectory() as tmp:
            output = os.path.join(tmp, "result.csv")
            self.cli._save_csv(  # pylint: disable=protected-access
                [{"i": FakeJavaInteger(), "s": "x"}], output
            )
            with open(output, encoding="utf-8") as handle:
                content = handle.read()
        self.assertIn("1,x", content)
        self.assertNotIn("{}", content)

    def test_display_width_counts_cjk_as_two(self) -> None:
        """CJK 字符按 2 列计算"""
        self.assertEqual(self.cli._get_char_width("中"), 2)  # pylint: disable=protected-access
        self.assertEqual(self.cli._get_char_width("a"), 1)  # pylint: disable=protected-access
        self.assertEqual(self.cli._get_display_width("中a"), 3)  # pylint: disable=protected-access

    def test_calc_col_widths_caps_at_50(self) -> None:
        """列宽按内容取最大值并封顶 50"""
        widths = self.cli._calc_col_widths(  # pylint: disable=protected-access
            ["id", "note"], [{"id": 1, "note": "x" * 120}]
        )
        self.assertEqual(widths["id"], 2)
        self.assertEqual(widths["note"], 50)

    def test_pick_display_cols(self) -> None:
        """终端过窄时按顺序挑选可容纳的列（每列占宽 + 3 分隔）"""
        selected = self.cli._pick_display_cols(  # pylint: disable=protected-access
            ["a", "b", "c"], 13, {"a": 3, "b": 3, "c": 3}
        )
        self.assertEqual(selected, ["a", "b"])

    def test_truncate_cell(self) -> None:
        """超宽单元格被截断并追加省略号"""
        truncated = self.cli._truncate_cell("abcdefghij", 6)  # pylint: disable=protected-access
        self.assertEqual(truncated, "abc...")
        self.assertEqual(self.cli._truncate_cell("abc", 6), "abc")  # pylint: disable=protected-access

    def test_show_results_handles_empty_and_narrow_terminal(self) -> None:
        """空结果、列超宽与小终端宽度都有友好提示"""
        _, output = self.capture(self.cli._show_results, [])  # pylint: disable=protected-access
        self.assertIn("没有结果", output)

        rows = [{"col1": "a" * 40, "col2": "b" * 40}]

        # 宽度 60：只能放下第一列
        with mock.patch(
            "src.db_connector_tool.result_render.shutil.get_terminal_size"
        ) as mock_size:
            mock_size.return_value = os.terminal_size((60, 24))
            _, output = self.capture(self.cli._show_results, rows)  # pylint: disable=protected-access
        self.assertIn("终端宽度限制", output)
        self.assertIn("隐藏了 1 列", output)

        # 宽度 20：一列都放不下
        with mock.patch(
            "src.db_connector_tool.result_render.shutil.get_terminal_size"
        ) as mock_size:
            mock_size.return_value = os.terminal_size((20, 24))
            _, output = self.capture(self.cli._show_results, rows)  # pylint: disable=protected-access
        self.assertIn("终端宽度过小", output)

    def test_show_results_prints_table(self) -> None:
        """正常宽度下输出带边框表格"""
        _, output = self.capture(
            self.cli._show_results, [{"id": 1, "name": "张三"}]  # pylint: disable=protected-access
        )
        self.assertIn("| id", output)
        self.assertIn("张三", output)

    def test_save_json_and_csv(self) -> None:
        """按扩展名导出 JSON / CSV"""
        rows = [{"id": 1, "name": "张三"}]
        json_path = self.temp_path / "out.json"
        csv_path = self.temp_path / "out.csv"

        _, output = self.capture(self.cli._save_output, rows, str(json_path))  # pylint: disable=protected-access
        self.assertIn("结果已保存", output)
        self.assertEqual(
            json.loads(json_path.read_text(encoding="utf-8")), [{"id": 1, "name": "张三"}]
        )

        self.capture(self.cli._save_output, rows, str(csv_path))  # pylint: disable=protected-access
        self.assertIn("张三", csv_path.read_text(encoding="utf-8"))

    def test_save_output_rejects_unknown_extension(self) -> None:
        """不支持的导出格式直接退出"""
        with self.assertRaises(SystemExit) as context:
            self.capture(
                self.cli._save_output, [{"id": 1}], str(self.temp_path / "out.xml")  # pylint: disable=protected-access
            )
        self.assertEqual(context.exception.code, 1)


class TestSqlFileExecution(CLITestCase):
    """SQL 文件解析与执行"""

    def test_parse_sql_file_utf8(self) -> None:
        """UTF-8 文件按语句切分"""
        path = self.temp_path / "init.sql"
        path.write_text("CREATE TABLE t (id INT);\nINSERT INTO t VALUES (1);\n", encoding="utf-8")
        statements = self.cli._parse_sql_file(str(path))  # pylint: disable=protected-access
        self.assertEqual(len(statements), 2)
        self.assertTrue(statements[0].upper().startswith("CREATE TABLE"))

    def test_parse_sql_file_gbk_fallback(self) -> None:
        """非 UTF-8 文件回退到 GBK，且不会被 UTF-16 误解码成乱码"""
        path = self.temp_path / "gbk.sql"
        path.write_bytes("SELECT * FROM 用户表".encode("gbk"))
        statements = self.cli._parse_sql_file(str(path))  # pylint: disable=protected-access
        self.assertEqual(len(statements), 1)
        self.assertIn("用户表", statements[0])

    def test_parse_sql_file_utf8_bom(self) -> None:
        """带 UTF-8 BOM 的文件按 BOM 解码"""
        path = self.temp_path / "bom.sql"
        path.write_bytes("SELECT * FROM 订单".encode("utf-8-sig"))
        statements = self.cli._parse_sql_file(str(path))  # pylint: disable=protected-access
        self.assertEqual(len(statements), 1)
        self.assertIn("订单", statements[0])

    def test_detect_encodings(self) -> None:
        """编码探测：BOM 优先，单字节文件不按 UTF-16 解码"""
        detect = DBConnectorCLI._detect_encodings  # pylint: disable=protected-access
        self.assertEqual(detect(b"SELECT 1"), ("utf-8", "gbk", "cp1252", "latin-1"))
        self.assertEqual(detect(b"\xef\xbb\xbfSELECT 1"), ("utf-8-sig",))
        self.assertEqual(detect(b"\xff\xfeS\x00"), ("utf-16",))

    def test_parse_sql_file_missing(self) -> None:
        """文件不存在时抛出 FileNotFoundError"""
        with self.assertRaises(FileNotFoundError):
            self.cli._parse_sql_file(str(self.temp_path / "nope.sql"))  # pylint: disable=protected-access

    def test_run_single_sql_routes_by_statement_type(self) -> None:
        """SELECT 走查询，其余走命令"""
        self.cli.db_manager.execute_query_result.return_value = make_result([{"c": 1}])
        self.cli.db_manager.execute_command.return_value = (2, 0.01)

        ok, output = self.capture(
            self.cli._run_single_sql, "db", "SELECT 1"  # pylint: disable=protected-access
        )
        self.assertTrue(ok)
        self.assertIn("总计: 1 行", output)

        ok, output = self.capture(
            self.cli._run_single_sql, "db", "UPDATE t SET a = 1"  # pylint: disable=protected-access
        )
        self.assertTrue(ok)
        self.assertIn("影响: 2 行", output)

    def test_run_single_sql_reports_failure(self) -> None:
        """执行失败返回 False 并打印错误"""
        self.cli.db_manager.execute_command.side_effect = ConfigError("boom")
        ok, output = self.capture(
            self.cli._run_single_sql, "db", "UPDATE t SET a = 1"  # pylint: disable=protected-access
        )
        self.assertFalse(ok)
        self.assertIn("执行语句失败", output)

    def test_execute_file_counts_success_rate(self) -> None:
        """文件执行统计成功/失败与成功率"""
        path = self.temp_path / "script.sql"
        path.write_text("SELECT 1;\nSELECT 2;\n", encoding="utf-8")
        self.cli.db_manager.execute_query_result.return_value = make_result([])
        args = self.parse(["file", "db", str(path), "-c"])

        _, output = self.capture(self.cli.execute_file, args)
        self.assertIn("成功 2 条，失败 0 条", output)
        self.assertIn("成功率: 100.0%", output)

    def test_execute_file_stops_without_continue_flag(self) -> None:
        """未指定 --continue-on-error 时遇错即退出"""
        path = self.temp_path / "script.sql"
        path.write_text("SELECT 1;\nSELECT 2;\n", encoding="utf-8")
        self.cli.db_manager.execute_query_result.side_effect = ConfigError("boom")
        args = self.parse(["file", "db", str(path)])

        with self.assertRaises(SystemExit) as context:
            self.capture(self.cli.execute_file, args)
        self.assertEqual(context.exception.code, 1)

    def test_execute_file_empty(self) -> None:
        """空 SQL 文件给出提示"""
        path = self.temp_path / "empty.sql"
        path.write_text("   \n", encoding="utf-8")
        args = self.parse(["file", "db", str(path)])
        _, output = self.capture(self.cli.execute_file, args)
        self.assertIn("没有有效的SQL语句", output)


class TestCommandHandlers(CLITestCase):
    """各子命令处理函数"""

    def test_ensure_db_manager_caches_instance(self) -> None:
        """数据库管理器延迟初始化并被缓存"""
        cli = DBConnectorCLI()
        with mock.patch(
            "src.db_connector_tool.cli_core.DatabaseManager"
        ) as mock_manager:
            first = cli._ensure_db_manager()  # pylint: disable=protected-access
            second = cli._ensure_db_manager()  # pylint: disable=protected-access
        mock_manager.assert_called_once()
        self.assertIs(first, second)

    def test_ensure_db_manager_exits_on_failure(self) -> None:
        """初始化失败时退出码为 1"""
        cli = DBConnectorCLI()
        with mock.patch(
            "src.db_connector_tool.cli_core.DatabaseManager",
            side_effect=DBConnectorError("init failed"),
        ):
            with self.assertRaises(SystemExit) as context:
                self.capture(cli._ensure_db_manager)  # pylint: disable=protected-access
        self.assertEqual(context.exception.code, 1)

    def test_show_version(self) -> None:
        """版本信息包含版本号与支持类型"""
        _, output = self.capture(self.cli.show_version, self.parse(["-v"]))
        self.assertIn("DB Connector Tool 版本", output)
        self.assertIn("GBase 8s", output)

    def test_list_connections_output(self) -> None:
        """list 子命令输出连接清单"""
        self.cli.db_manager.list_connections.return_value = ["a", "b"]
        _, output = self.capture(self.cli.list_connections, self.parse(["list"]))
        self.assertIn("已配置的连接", output)
        self.assertIn("1. a", output)

        self.cli.db_manager.list_connections.return_value = []
        _, output = self.capture(self.cli.list_connections, self.parse(["list"]))
        self.assertIn("没有配置任何连接", output)

    def test_add_connection_success_and_failure(self) -> None:
        """add 子命令成功提示 / 失败退出"""
        args = self.parse(["add", "db", "-T", "sqlite", "-d", ":memory:"])
        _, output = self.capture(self.cli.add_connection, args)
        self.assertIn("添加成功", output)

        self.cli.db_manager.add_connection.side_effect = ConfigError("exists")
        with self.assertRaises(SystemExit) as context:
            self.capture(self.cli.add_connection, args)
        self.assertEqual(context.exception.code, 1)

    def test_update_connection_merges_existing(self) -> None:
        """update 子命令合并现有配置"""
        self.cli.db_manager.show_connection.return_value = {
            "type": "mysql",
            "host": "old",
            "port": 3306,
        }
        args = self.parse(["update", "db", "-T", "mysql", "-H", "new"])
        _, output = self.capture(self.cli.update_connection, args)
        self.assertIn("更新成功", output)
        self.cli.db_manager.update_connection.assert_called_once_with(
            "db", {"type": "mysql", "host": "new", "port": 3306}
        )

    def test_show_connection_masks_password(self) -> None:
        """show 子命令隐藏密码"""
        self.cli.db_manager.show_connection.return_value = {
            "type": "mysql",
            "host": "h",
            "password": "secret",
        }
        _, output = self.capture(self.cli.show_connection, self.parse(["show", "db"]))
        self.assertIn("***", output)
        self.assertNotIn("secret", output)

    def test_remove_connection(self) -> None:
        """remove 子命令调用管理器并提示"""
        _, output = self.capture(self.cli.remove_connection, self.parse(["remove", "db"]))
        self.cli.db_manager.remove_connection.assert_called_once_with("db")
        self.assertIn("已删除", output)

    def test_test_connection_exit_codes(self) -> None:
        """test 子命令按连通性返回退出码"""
        self.cli.db_manager.test_connection.return_value = True
        _, output = self.capture(self.cli.test_connection, self.parse(["test", "db"]))
        self.assertIn("测试成功", output)

        self.cli.db_manager.test_connection.return_value = False
        with self.assertRaises(SystemExit) as context:
            self.capture(self.cli.test_connection, self.parse(["test", "db"]))
        self.assertEqual(context.exception.code, 1)

    def test_execute_query_with_output_file(self) -> None:
        """query 子命令把结果写入文件"""
        self.cli.db_manager.execute_query_result.return_value = make_result([{"id": 1}])
        out_path = self.temp_path / "rows.csv"
        args = self.parse(["query", "db", "SELECT 1", "-o", str(out_path)])
        _, output = self.capture(self.cli.execute_query, args)
        self.assertIn("总计: 1 行", output)
        self.assertTrue(out_path.exists())

    def test_execute_command(self) -> None:
        """command 子命令输出影响行数"""
        self.cli.db_manager.execute_command.return_value = (3, 0.01)
        _, output = self.capture(
            self.cli.execute_command, self.parse(["command", "db", "DELETE FROM t"])
        )
        self.assertIn("影响: 3 行", output)

    def test_interactive_shell_loop(self) -> None:
        """交互式 Shell 支持 help 与退出指令"""
        outputs = []
        with mock.patch("builtins.input", side_effect=["help", "SELECT 1", "exit"]):
            self.cli.db_manager.execute_query_result.return_value = make_result([])
            _, output = self.capture(
                self.cli.interactive_shell, self.parse(["shell", "db"])
            )
        outputs.append(output)
        self.assertIn("启动SQL Shell", output)
        self.assertIn("SQL Shell 命令", output)

    def test_interactive_shell_keyboard_interrupt(self) -> None:
        """Ctrl+C 退出 Shell 且不报错"""
        with mock.patch("builtins.input", side_effect=KeyboardInterrupt):
            _, output = self.capture(
                self.cli.interactive_shell, self.parse(["shell", "db"])
            )
        self.assertIn("再见", output)


class TestCommandHandlerFailures(CLITestCase):
    """各子命令的统一失败行为：友好提示 + 退出码 1（不裸抛异常）"""

    def _assert_exit_1(self, handler, argv):
        """执行处理函数，断言退出码为 1，并返回这段时间的标准输出"""
        buffer = io.StringIO()
        with self.assertRaises(SystemExit) as context:
            with redirect_stdout(buffer):
                handler(self.parse(argv))
        self.assertEqual(context.exception.code, 1)
        return buffer.getvalue()

    def test_remove_connection_failure(self) -> None:
        """remove 失败时友好退出"""
        self.cli.db_manager.remove_connection.side_effect = ConfigError("not found")
        self._assert_exit_1(self.cli.remove_connection, ["remove", "db"])

    def test_update_connection_failure(self) -> None:
        """update 读取现有配置失败时友好退出"""
        self.cli.db_manager.show_connection.side_effect = ConfigError("missing")
        self._assert_exit_1(self.cli.update_connection, ["update", "db", "-T", "mysql"])

    def test_show_connection_failure(self) -> None:
        """show 失败时友好退出"""
        self.cli.db_manager.show_connection.side_effect = DatabaseError("boom")
        self._assert_exit_1(self.cli.show_connection, ["show", "db"])

    def test_list_connections_failure(self) -> None:
        """list 失败时友好退出"""
        self.cli.db_manager.list_connections.side_effect = DatabaseError("boom")
        self._assert_exit_1(self.cli.list_connections, ["list"])

    def test_test_connection_exception(self) -> None:
        """test 抛错时友好退出"""
        self.cli.db_manager.test_connection.side_effect = DatabaseError("boom")
        self._assert_exit_1(self.cli.test_connection, ["test", "db"])

    def test_execute_query_failure(self) -> None:
        """query 执行失败时友好退出"""
        self.cli.db_manager.execute_query_result.side_effect = DatabaseError("boom")
        self._assert_exit_1(self.cli.execute_query, ["query", "db", "SELECT 1"])

    def test_execute_command_failure(self) -> None:
        """command 执行失败时友好退出"""
        self.cli.db_manager.execute_command.side_effect = DatabaseError("boom")
        self._assert_exit_1(self.cli.execute_command, ["command", "db", "DELETE FROM t"])

    def test_save_output_oserror_is_friendly(self) -> None:
        """导出目录不存在时应给出中文提示而非裸 traceback"""
        self.cli.db_manager.execute_query_result.return_value = make_result([{"id": 1}])
        bad_path = str(self.temp_path / "no_such_dir" / "out.csv")

        output = self._assert_exit_1(
            self.cli.execute_query, ["query", "db", "SELECT 1", "-o", bad_path]
        )
        self.assertIn("保存结果失败", output)

    def test_execute_file_missing_file_is_friendly(self) -> None:
        """SQL 文件不存在时应给出中文提示而非裸 traceback"""
        missing = str(self.temp_path / "missing.sql")

        output = self._assert_exit_1(
            self.cli.execute_file, ["file", "db", missing, "-c"]
        )
        self.assertIn("执行SQL文件失败", output)


class TestMainEntry(CLITestCase):
    """入口函数分发"""

    def test_main_without_arguments_prints_help(self) -> None:
        """无参数时打印帮助并退出 0"""
        with mock.patch("sys.argv", ["db-connector"]):
            with self.assertRaises(SystemExit) as context:
                with redirect_stdout(io.StringIO()) as buffer:
                    main()
        self.assertEqual(context.exception.code, 0)
        self.assertIn("使用情况", buffer.getvalue())

    def test_main_version_flag(self) -> None:
        """-v 打印版本"""
        with mock.patch("sys.argv", ["db-connector", "-v"]):
            with self.assertRaises(SystemExit) as context:
                with redirect_stdout(io.StringIO()) as buffer:
                    main()
        self.assertEqual(context.exception.code, 0)
        self.assertIn("DB Connector Tool 版本", buffer.getvalue())

    def test_main_dispatches_subcommand(self) -> None:
        """子命令被分发到对应处理函数"""
        with mock.patch("sys.argv", ["db-connector", "list"]):
            with mock.patch.object(
                DBConnectorCLI, "list_connections"
            ) as mock_handler:
                main()
        mock_handler.assert_called_once()


if __name__ == "__main__":
    unittest.main()
