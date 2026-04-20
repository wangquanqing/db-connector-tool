"""命令行参数解析工具模块 (Argparse Utils)"""

import argparse

from ..drivers.sqlalchemy_driver import SQLAlchemyDriver

# 支持的数据库类型
SUPPORTED_DATABASE_TYPES = list(SQLAlchemyDriver.DB_CONFIGS.keys())


class ChineseHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """中文帮助格式化器，优化帮助信息显示

    Example:
        >>> formatter = ChineseHelpFormatter(prog="db-connector")
    """

    def _format_usage(self, usage, actions, groups, prefix):
        """格式化使用说明"""
        if prefix is None:
            prefix = "\n使用情况: "
        return super()._format_usage(usage, actions, groups, prefix)

    def start_section(self, heading):
        """开始新的帮助章节"""
        if heading == "options":
            heading = "下列选项可用"
        super().start_section(heading)


def create_argument_parser(cli_instance) -> argparse.ArgumentParser:
    """创建命令行参数解析器

    Args:
        cli_instance: 已初始化的CLI实例

    Returns:
        argparse.ArgumentParser: 配置好的参数解析器

    Example:
        >>> parser = create_argument_parser(cli)
    """
    parser = argparse.ArgumentParser(
        usage="db-connector [<命令>] [<选项>]",
        description="DB Connector - 数据库连接管理工具",
        formatter_class=ChineseHelpFormatter,
        epilog="可在此找到更多帮助: https://github.com/wangquanqing/db-connector-tool/blob/main/TUTORIAL.md",
        add_help=False,
    )

    # 添加自定义的帮助选项
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        default=argparse.SUPPRESS,
        help="显示选定命令的帮助信息",
    )

    # 添加版本选项
    parser.add_argument(
        "-v",
        "--version",
        action="store_true",
        help="显示当前模块版本信息",
    )

    subparsers = parser.add_subparsers(title="下列命令有效", dest="command")

    # add 命令
    add_parser = subparsers.add_parser("add", help="添加新的数据库连接")
    _setup_connection_arguments(add_parser)
    add_parser.set_defaults(func=cli_instance.add_connection)

    # remove 命令
    remove_parser = subparsers.add_parser("remove", help="删除连接")
    remove_parser.add_argument("name", help="连接名称")
    remove_parser.set_defaults(func=cli_instance.remove_connection)

    # update 命令
    update_parser = subparsers.add_parser("update", help="更新连接配置")
    _setup_connection_arguments(update_parser)
    update_parser.set_defaults(func=cli_instance.update_connection)

    # show 命令
    show_parser = subparsers.add_parser("show", help="显示连接详情")
    show_parser.add_argument("name", help="连接名称")
    show_parser.set_defaults(func=cli_instance.show_connection)

    # list 命令
    list_parser = subparsers.add_parser("list", help="列出所有连接")
    list_parser.set_defaults(func=cli_instance.list_connections)

    # test 命令
    test_parser = subparsers.add_parser("test", help="测试连接")
    test_parser.add_argument("name", help="连接名称")
    test_parser.set_defaults(func=cli_instance.test_connection)

    # query 命令
    query_parser = subparsers.add_parser("query", help="执行SQL查询")
    query_parser.add_argument("connection", help="连接名称")
    query_parser.add_argument("query", help="SQL查询语句")
    query_parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="输出格式 (默认: table)",
    )
    query_parser.add_argument("--output", help="输出文件路径")
    query_parser.set_defaults(func=cli_instance.execute_query)

    # execute 命令
    execute_parser = subparsers.add_parser("execute", help="执行SQL文件")
    execute_parser.add_argument("connection", help="连接名称")
    execute_parser.add_argument("file", help="SQL文件路径")
    execute_parser.add_argument(
        "--format",
        choices=["table", "json", "csv"],
        default="table",
        help="输出格式 (默认: table)",
    )
    execute_parser.add_argument("--output", help="输出文件路径")
    execute_parser.add_argument(
        "--continue-on-error", action="store_true", help="遇到错误时继续执行"
    )
    execute_parser.set_defaults(func=cli_instance.execute_file)

    # shell 命令
    shell_parser = subparsers.add_parser("shell", help="启动交互式SQL Shell")
    shell_parser.add_argument("connection", help="连接名称")
    shell_parser.set_defaults(func=cli_instance.interactive_shell)

    return parser


def _setup_connection_arguments(parser: argparse.ArgumentParser) -> None:
    """设置连接相关的命令行参数

    Args:
        parser: 参数解析器实例

    Example:
        >>> _setup_connection_arguments(parser)
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
    parser.add_argument("-s", "--service-name", help="Oracle服务名称")
    parser.add_argument(
        "-g",
        "--gssencmode",
        choices=["disable", "allow", "prefer", "require"],
        help="PostgreSQL GSSENCMODE 参数",
    )
    parser.add_argument(
        "-k",
        "--charset",
        choices=["cp936", "utf8", "utf8mb4"],
        help="MySQL/SQL Server字符集",
    )
    parser.add_argument(
        "-t",
        "--tds-version",
        choices=["7.0", "7.1", "7.2", "7.3", "7.4", "8.0"],
        help="SQL Server TDS 版本",
    )
    parser.add_argument(
        "-c",
        "--custom-params",
        nargs="+",
        help="自定义参数 (格式: key=value), 例如: -c options=-csearch_path=myschema connect_timeout=10",
    )
