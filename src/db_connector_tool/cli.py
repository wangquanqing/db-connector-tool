"""命令行接口入口模块 (CLI Entry Point)

提供中文友好的命令行参数解析器和程序入口，支持连接管理、
SQL 执行和交互式 Shell 等子命令。

Example:
>>> from db_connector_tool.cli import main
>>> main()
"""

import argparse
import sys

from .cli_core import DBConnectorCLI
from .drivers.sqlalchemy_driver import SQLAlchemyDriver

SUPPORTED_DATABASE_TYPES = list(SQLAlchemyDriver.DB_CONFIGS.keys())


class ChineseHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """中文帮助格式化器 (Chinese Help Formatter)

    将 argparse 默认的英文帮助信息本地化为中文，优化命令行使用体验。

    Example:
    >>> formatter = ChineseHelpFormatter(prog="db-connector")
    """

    def _format_usage(self, usage, actions, groups, prefix):
        """格式化使用说明 (Format Usage)

        Args:
            usage: 用法字符串
            actions: 动作列表
            groups: 参数组
            prefix: 前缀文本
        """
        if prefix is None:
            prefix = "\n使用情况: "
        return super()._format_usage(usage, actions, groups, prefix)

    def start_section(self, heading):
        """开始章节 (Start Section)

        Args:
            heading: 章节标题
        """
        if heading == "options":
            heading = "下列选项可用"
        super().start_section(heading)


def create_argument_parser(cli_instance: "DBConnectorCLI") -> argparse.ArgumentParser:
    """创建命令行参数解析器

    Args:
        cli_instance: DBConnectorCLI 实例

    Returns:
        argparse.ArgumentParser: 配置完成的参数解析器

    Example:
    >>> cli = DBConnectorCLI()
    >>> parser = create_argument_parser(cli)
    >>> args = parser.parse_args(["list"])
    """
    parser = argparse.ArgumentParser(
        usage="db-connector [<命令>] [<选项>]",
        description="DB Connector - 数据库连接管理工具",
        formatter_class=ChineseHelpFormatter,
        epilog="可在此找到更多帮助: https://github.com/wangquanqing/db-connector-tool/blob/main/TUTORIAL.md",
        add_help=False,
    )

    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="显示选定命令的帮助信息",
    )

    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="显示当前模块版本信息",
    )

    subparsers = parser.add_subparsers(title="下列命令有效", dest="command")

    add_parser = subparsers.add_parser("add", help="添加新的数据库连接")
    _setup_connection_arguments(add_parser)
    add_parser.set_defaults(func=cli_instance.add_connection)

    remove_parser = subparsers.add_parser("remove", help="删除连接")
    remove_parser.add_argument("name", help="连接名称")
    remove_parser.set_defaults(func=cli_instance.remove_connection)

    update_parser = subparsers.add_parser("update", help="更新连接配置")
    _setup_connection_arguments(update_parser)
    update_parser.set_defaults(func=cli_instance.update_connection)

    show_parser = subparsers.add_parser("show", help="显示连接详情")
    show_parser.add_argument("name", help="连接名称")
    show_parser.set_defaults(func=cli_instance.show_connection)

    list_parser = subparsers.add_parser("list", help="列出所有连接")
    list_parser.set_defaults(func=cli_instance.list_connections)

    test_parser = subparsers.add_parser("test", help="测试连接")
    test_parser.add_argument("name", help="连接名称")
    test_parser.set_defaults(func=cli_instance.test_connection)

    query_parser = subparsers.add_parser("query", help="执行SQL查询语句")
    query_parser.add_argument("connection", help="连接名称")
    query_parser.add_argument("sql_content", help="SQL查询语句")
    query_parser.add_argument("-o", "--output", help="输出文件路径")
    query_parser.set_defaults(func=cli_instance.execute_query)

    command_parser = subparsers.add_parser("command", help="执行增删改操作")
    command_parser.add_argument("connection", help="连接名称")
    command_parser.add_argument("sql_content", help="SQL命令语句")
    command_parser.set_defaults(func=cli_instance.execute_command)

    file_parser = subparsers.add_parser("file", help="执行SQL文件")
    file_parser.add_argument("connection", help="连接名称")
    file_parser.add_argument("file_path", help="SQL文件路径")
    file_parser.add_argument("-o", "--output", help="输出文件路径")
    file_parser.add_argument(
        "-c", "--continue-on-error", action="store_true", help="遇到错误时继续执行"
    )
    file_parser.set_defaults(func=cli_instance.execute_file)

    shell_parser = subparsers.add_parser("shell", help="启动交互式SQL Shell")
    shell_parser.add_argument("connection", help="连接名称")
    shell_parser.set_defaults(func=cli_instance.interactive_shell)

    return parser


def _setup_connection_arguments(parser: argparse.ArgumentParser) -> None:
    """为子命令注册连接参数

    Args:
        parser: 子命令解析器实例

    Example:
    >>> _setup_connection_arguments(add_parser)
    """
    parser.add_argument("name", help="连接名称")
    parser.add_argument(
        "-T",
        "--type",
        required=True,
        choices=SUPPORTED_DATABASE_TYPES,
        help="数据库类型",
    )
    parser.add_argument("-H", "--host", help="数据库主机")
    parser.add_argument("-P", "--port", type=int, help="数据库端口")
    parser.add_argument("-u", "--username", help="用户名")
    parser.add_argument(
        "-p",
        "--password",
        nargs="?",
        default=None,
        const="",
        help="密码（使用-p不提供参数将提示输入密码）",
    )
    parser.add_argument("-d", "--database", help="数据库名")
    parser.add_argument("-s", "--service-name", help="Oracle 服务名称")
    parser.add_argument("-S", "--server", help="GBase 8s 实例")
    parser.add_argument(
        "-c",
        "--custom-params",
        nargs="+",
        help="自定义参数 (格式: key=value), 例如: -c options='-c search_path=myschema' connect_timeout=10",
    )


def main() -> None:
    """命令行入口函数

    Example:
    >>> if __name__ == "__main__":
    ...     main()
    """
    cli = DBConnectorCLI()
    parser = create_argument_parser(cli)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if hasattr(args, "version") and args.version:
        cli.show_version(args)
        sys.exit(0)

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
