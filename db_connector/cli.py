"""
DB Connector CLI 工具
提供命令行界面来管理数据库连接和执行查询
"""

import argparse
import csv
import json
import sys
from typing import Dict, List

from .core.database import DatabaseManager
from .utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


class DBConnectorCLI:
    """DB Connector 命令行接口"""

    DB_MANAGER_NOT_INIT_MSG = "❌ 数据库管理器未初始化"

    def __init__(self):
        self.db_manager = None
        self.setup_logging()

    def setup_logging(self):
        """设置日志"""
        setup_logging(level="INFO")

    def init_db_manager(self):
        """初始化数据库管理器"""
        if self.db_manager is None:
            try:
                self.db_manager = DatabaseManager()
            except Exception as e:
                print(f"❌ 初始化数据库管理器失败: {e}")
                sys.exit(1)

    def add_connection(self, args):
        """添加数据库连接"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        # 构建连接配置
        config = {
            "type": args.type,
            "host": args.host,
            "port": args.port,
            "username": args.username,
            "password": args.password,
            "database": args.database,
        }

        # 移除空值
        config = {k: v for k, v in config.items() if v is not None}

        try:
            self.db_manager.create_connection(args.name, config)
            print(f"✅ 连接 '{args.name}' 添加成功")
        except Exception as e:
            print(f"❌ 添加连接失败: {e}")
            sys.exit(1)

    def list_connections(self):
        """列出所有连接"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            connections = self.db_manager.list_connections()
            if connections:
                print("📋 已配置的连接:")
                for i, conn in enumerate(connections, 1):
                    print(f"  {i}. {conn}")
            else:
                print("ℹ️  没有配置任何连接")
        except Exception as e:
            print(f"❌ 列出连接失败: {e}")
            sys.exit(1)

    def test_connection(self, args):
        """测试连接"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            if self.db_manager.test_connection(args.name):
                print(f"✅ 连接 '{args.name}' 测试成功")
            else:
                print(f"❌ 连接 '{args.name}' 测试失败")
                sys.exit(1)
        except Exception as e:
            print(f"❌ 连接测试失败: {e}")
            sys.exit(1)

    def remove_connection(self, args):
        """删除连接"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            self.db_manager.remove_connection(args.name)
            print(f"✅ 连接 '{args.name}' 已删除")
        except Exception as e:
            print(f"❌ 删除连接失败: {e}")
            sys.exit(1)

    def show_connection(self, args):
        """显示连接详情"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            config = self.db_manager.config_manager.get_connection(args.name)
            # 隐藏密码
            safe_config = config.copy()
            if "password" in safe_config:
                safe_config["password"] = "***"
            if "passwd" in safe_config:
                safe_config["passwd"] = "***"
            if "pwd" in safe_config:
                safe_config["pwd"] = "***"

            print(f"🔍 连接 '{args.name}' 的配置:")
            for key, value in safe_config.items():
                print(f"  {key}: {value}")

        except Exception as e:
            print(f"❌ 获取连接详情失败: {e}")
            sys.exit(1)

    def execute_query(self, args):
        """执行查询"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            results = self.db_manager.execute_query(args.connection, args.query)

            if args.output:
                self._save_output(results, args.output, args.format)
            else:
                self._display_results(results, args.format)

        except Exception as e:
            print(f"❌ 执行查询失败: {e}")
            sys.exit(1)

    def execute_file(self, args):
        """执行SQL文件"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            with open(args.file, "r", encoding="utf-8") as f:
                sql_content = f.read()

            # 分割SQL语句（简单实现）
            statements = [
                stmt.strip() for stmt in sql_content.split(";") if stmt.strip()
            ]

            total_results = []
            for i, statement in enumerate(statements, 1):
                print(f"执行语句 {i}/{len(statements)}: {statement[:50]}...")

                try:
                    if statement.lower().strip().startswith("select"):
                        results = self.db_manager.execute_query(
                            args.connection, statement
                        )
                        total_results.extend(results)
                    else:
                        affected = self.db_manager.execute_command(
                            args.connection, statement
                        )
                        print(f"  影响行数: {affected}")
                except Exception as e:
                    print(f"❌ 执行语句失败: {e}")
                    if not args.continue_on_error:
                        sys.exit(1)

            if total_results and args.output:
                self._save_output(total_results, args.output, args.format)
            elif total_results:
                self._display_results(total_results, args.format)

        except Exception as e:
            print(f"❌ 执行SQL文件失败: {e}")
            sys.exit(1)

    def _display_results(self, results: List[Dict], format: str = "table"):
        """显示结果"""
        if not results:
            print("没有结果")
            return

        if format == "table":
            self._display_table(results)
        elif format == "json":
            print(json.dumps(results, indent=2, ensure_ascii=False))
        elif format == "csv":
            self._display_csv(results)
        else:
            self._display_table(results)

    def _display_table(self, results: List[Dict]):
        """以表格形式显示结果"""
        if not results:
            return

        headers = list(results[0].keys())

        # 计算列宽
        col_widths = {}
        for header in headers:
            col_widths[header] = len(str(header))

        for row in results:
            for header in headers:
                value = str(row.get(header, ""))
                col_widths[header] = max(col_widths[header], len(value))

        # 打印表头
        header_line = " | ".join(
            [f"{header:<{col_widths[header]}}" for header in headers]
        )
        separator = "-+-".join(["-" * col_widths[header] for header in headers])

        print(separator)
        print(header_line)
        print(separator)

        # 打印数据行
        for row in results:
            row_line = " | ".join(
                [
                    f"{str(row.get(header, '')):<{col_widths[header]}}"
                    for header in headers
                ]
            )
            print(row_line)

        print(separator)
        print(f"总计: {len(results)} 行")

    def _display_csv(self, results: List[Dict]):
        """以CSV格式显示结果"""
        if not results:
            return

        headers = list(results[0].keys())
        writer = csv.DictWriter(sys.stdout, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)

    def _save_output(self, results: List[Dict], output_file: str, format: str):
        """保存结果到文件"""
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                if format == "json":
                    json.dump(results, f, indent=2, ensure_ascii=False)
                elif format == "csv":
                    if results:
                        headers = list(results[0].keys())
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        writer.writerows(results)
                else:  # table format as CSV
                    self._write_csv_results(f, results)

            print(f"✅ 结果已保存到: {output_file}")
        except Exception as e:
            print(f"❌ 保存结果失败: {e}")
            sys.exit(1)

    def _write_csv_results(self, file_handle, results: List[Dict]):
        """将结果以CSV格式写入文件"""
        if results:
            headers = list(results[0].keys())
            writer = csv.DictWriter(file_handle, fieldnames=headers)
            writer.writeheader()
            writer.writerows(results)

    def interactive_shell(self, args):
        """交互式SQL Shell"""
        self.init_db_manager()

        # 确保 db_manager 已初始化
        if self.db_manager is None:
            print(self.DB_MANAGER_NOT_INIT_MSG)
            sys.exit(1)

        try:
            connection = self.db_manager.get_connection(args.connection)
            print(f"🔗 已连接到: {args.connection}")
            print("输入 SQL 语句执行查询，输入 'quit' 或 'exit' 退出")
            print("-" * 50)

            while True:
                try:
                    sql = input("SQL> ").strip()

                    if sql.lower() in ["quit", "exit", "q"]:
                        break
                    elif not sql:
                        continue
                    elif sql.lower().startswith("select"):
                        results = connection.execute_query(sql)
                        self._display_results(results, "table")
                    else:
                        affected = connection.execute_command(sql)
                        print(f"✅ 执行成功，影响行数: {affected}")

                except Exception as e:
                    print(f"❌ 错误: {e}")

        except Exception as e:
            print(f"❌ 启动交互式Shell失败: {e}")
            sys.exit(1)
        finally:
            self.db_manager.close_connection(args.connection)


def main():
    """CLI 主函数"""
    cli = DBConnectorCLI()
    parser = create_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # 执行对应的命令
    if hasattr(args, "func"):
        args.func(cli, args)
    else:
        parser.print_help()


def create_parser():
    """创建命令行解析器"""
    parser = argparse.ArgumentParser(
        description="DB Connector - 跨平台数据库连接管理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 添加MySQL连接
  db-connector add mysql-dev --type mysql --host localhost --user root --password secret --database testdb
  
  # 列出所有连接
  db-connector list
  
  # 测试连接
  db-connector test mysql-dev
  
  # 执行查询
  db-connector query mysql-dev "SELECT * FROM users"
  
  # 交互式SQL Shell
  db-connector shell mysql-dev
  
  # 执行SQL文件
  db-connector file mysql-dev queries.sql
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="可用命令")

    # add 命令
    add_parser = subparsers.add_parser("add", help="添加数据库连接")
    add_parser.add_argument("name", help="连接名称")
    add_parser.add_argument(
        "--type",
        "-t",
        required=True,
        choices=["mysql", "postgresql", "oracle", "mssql", "sqlite"],
        help="数据库类型",
    )
    add_parser.add_argument("--host", "-H", help="数据库主机")
    add_parser.add_argument("--port", "-P", help="数据库端口")
    add_parser.add_argument("--user", "-u", "--username", help="用户名")
    add_parser.add_argument("--password", "-p", help="密码")
    add_parser.add_argument("--database", "-d", "--db", help="数据库名")
    add_parser.set_defaults(func=lambda cli, args: cli.add_connection(args))

    # list 命令
    list_parser = subparsers.add_parser("list", help="列出所有连接")
    list_parser.set_defaults(func=lambda cli, args: cli.list_connections(args))

    # test 命令
    test_parser = subparsers.add_parser("test", help="测试连接")
    test_parser.add_argument("name", help="连接名称")
    test_parser.set_defaults(func=lambda cli, args: cli.test_connection(args))

    # remove 命令
    remove_parser = subparsers.add_parser("remove", help="删除连接")
    remove_parser.add_argument("name", help="连接名称")
    remove_parser.set_defaults(func=lambda cli, args: cli.remove_connection(args))

    # show 命令
    show_parser = subparsers.add_parser("show", help="显示连接详情")
    show_parser.add_argument("name", help="连接名称")
    show_parser.set_defaults(func=lambda cli, args: cli.show_connection(args))

    # query 命令
    query_parser = subparsers.add_parser("query", help="执行SQL查询")
    query_parser.add_argument("connection", help="连接名称")
    query_parser.add_argument("query", help="SQL查询语句")
    query_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json", "csv"],
        default="table",
        help="输出格式",
    )
    query_parser.add_argument("--output", "-o", help="输出文件路径")
    query_parser.set_defaults(func=lambda cli, args: cli.execute_query(args))

    # file 命令
    file_parser = subparsers.add_parser("file", help="执行SQL文件")
    file_parser.add_argument("connection", help="连接名称")
    file_parser.add_argument("file", help="SQL文件路径")
    file_parser.add_argument(
        "--format",
        "-f",
        choices=["table", "json", "csv"],
        default="table",
        help="输出格式",
    )
    file_parser.add_argument("--output", "-o", help="输出文件路径")
    file_parser.add_argument(
        "--continue-on-error", "-c", action="store_true", help="遇到错误时继续执行"
    )
    file_parser.set_defaults(func=lambda cli, args: cli.execute_file(args))

    # shell 命令
    shell_parser = subparsers.add_parser("shell", help="启动交互式SQL Shell")
    shell_parser.add_argument("connection", help="连接名称")
    shell_parser.set_defaults(func=lambda cli, args: cli.interactive_shell(args))

    return parser


if __name__ == "__main__":
    main()
