"""
DB Connector CLI 工具
====================

提供命令行界面来管理数据库连接和执行查询。

功能特性:
- 支持多种数据库类型 (MySQL, PostgreSQL, Oracle, SQL Server, SQLite)
- 连接配置管理 (添加、删除、测试、查看)
- SQL查询执行和结果导出
- SQL文件批量执行
- 交互式SQL Shell
- 多种输出格式支持 (表格、JSON、CSV)

使用示例:
    db-connector add mysql-dev --type mysql --host localhost --username root
    db-connector list
    db-connector query mysql-dev "SELECT * FROM users"
    db-connector shell mysql-dev

版本: 1.0.0
作者: DB Connector Team
"""

import argparse
import csv
import json
import re
import sys
from typing import Dict, List, Optional

from .core.database import DatabaseManager
from .utils.logger import get_logger, setup_logging

logger = get_logger(__name__)


class DBConnectorCLI:
    """
    DB Connector 命令行接口主类

    负责处理所有命令行操作，包括连接管理、查询执行等。

    Attributes:
        db_manager (Optional[DatabaseManager]): 数据库管理器实例
        DB_MANAGER_NOT_INIT_MSG (str): 数据库管理器未初始化的错误消息
    """

    DB_MANAGER_NOT_INIT_MSG = "❌ 数据库管理器未初始化"

    def __init__(self):
        """
        初始化DB Connector CLI

        设置日志系统并准备数据库管理器。
        """
        self.db_manager: Optional[DatabaseManager] = None
        self.setup_logging()

    def setup_logging(self) -> None:
        """设置日志系统，配置日志级别为INFO"""
        setup_logging(level="INFO")

    def _ensure_db_manager_initialized(self) -> DatabaseManager:
        """
        确保数据库管理器已初始化

        如果数据库管理器未初始化，则创建新的实例。

        Returns:
            DatabaseManager: 已初始化的数据库管理器实例

        Raises:
            SystemExit: 如果初始化失败则退出程序
        """
        if self.db_manager is None:
            try:
                self.db_manager = DatabaseManager()
            except Exception as e:
                logger.error("初始化数据库管理器失败: %s", e)
                print(f"❌ 初始化数据库管理器失败: {e}")
                sys.exit(1)
        return self.db_manager

    def add_connection(self, args: argparse.Namespace) -> None:
        """
        添加新的数据库连接配置

        Args:
            args: 命令行参数，包含连接配置信息

        Raises:
            SystemExit: 如果添加连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        # 构建连接配置字典
        config = {
            "type": args.type,
            "host": args.host,
            "port": args.port,
            "username": args.username,
            "password": args.password,
            "database": args.database,
        }

        # 移除空值配置项
        config = {k: v for k, v in config.items() if v is not None}

        try:
            db_manager.create_connection(args.name, config)
            logger.info("连接 '%s' 添加成功", args.name)
            print(f"✅ 连接 '{args.name}' 添加成功")
        except Exception as e:
            logger.error("添加连接失败: %s", e)
            print(f"❌ 添加连接失败: {e}")
            sys.exit(1)

    def list_connections(self, args: argparse.Namespace) -> None:
        """
        列出所有已配置的数据库连接

        Args:
            args: 命令行参数

        Raises:
            SystemExit: 如果列出连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            connections = db_manager.list_connections(args)
            if connections:
                print("📋 已配置的连接:")
                for i, conn in enumerate(connections, 1):
                    print(f"  {i}. {conn}")
            else:
                print("ℹ️  没有配置任何连接")
        except Exception as e:
            logger.error("列出连接失败: %s", e)
            print(f"❌ 列出连接失败: {e}")
            sys.exit(1)

    def test_connection(self, args: argparse.Namespace) -> None:
        """
        测试指定连接的连通性

        Args:
            args: 命令行参数，包含要测试的连接名称

        Raises:
            SystemExit: 如果连接测试失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            if db_manager.test_connection(args.name):
                print(f"✅ 连接 '{args.name}' 测试成功")
            else:
                print(f"❌ 连接 '{args.name}' 测试失败")
                sys.exit(1)
        except Exception as e:
            logger.error("连接测试失败: %s", e)
            print(f"❌ 连接测试失败: {e}")
            sys.exit(1)

    def remove_connection(self, args: argparse.Namespace) -> None:
        """
        删除指定的数据库连接配置

        Args:
            args: 命令行参数，包含要删除的连接名称

        Raises:
            SystemExit: 如果删除连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            db_manager.remove_connection(args.name)
            logger.info("连接 '%s' 已删除", args.name)
            print(f"✅ 连接 '{args.name}' 已删除")
        except Exception as e:
            logger.error("删除连接失败: %s", e)
            print(f"❌ 删除连接失败: {e}")
            sys.exit(1)

    def show_connection(self, args: argparse.Namespace) -> None:
        """
        显示指定连接的详细配置信息

        注意：敏感信息（如密码）会被隐藏显示。

        Args:
            args: 命令行参数，包含要显示详情的连接名称

        Raises:
            SystemExit: 如果获取连接详情失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            config = db_manager.config_manager.get_connection(args.name)
            # 隐藏敏感信息（密码等）
            safe_config = config.copy()
            password_fields = ["password", "passwd", "pwd"]
            for field in password_fields:
                if field in safe_config:
                    safe_config[field] = "***"

            print(f"🔍 连接 '{args.name}' 的配置:")
            for key, value in safe_config.items():
                print(f"  {key}: {value}")

        except Exception as e:
            logger.error("获取连接详情失败: %s", e)
            print(f"❌ 获取连接详情失败: {e}")
            sys.exit(1)

    def execute_query(self, args: argparse.Namespace) -> None:
        """
        在指定连接上执行SQL查询

        Args:
            args: 命令行参数，包含连接名称、查询语句和输出选项

        Raises:
            SystemExit: 如果执行查询失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            results = db_manager.execute_query(args.connection, args.query)

            if args.output:
                self._save_output(results, args.output, args.format)
            else:
                self._display_results(results, args.format)

        except Exception as e:
            logger.error("执行查询失败: %s", e)
            print(f"❌ 执行查询失败: {e}")
            sys.exit(1)

    def execute_file(self, args: argparse.Namespace) -> None:
        """
        执行SQL文件中的多个语句

        支持批量执行SQL文件，可选择在遇到错误时继续执行。

        Args:
            args: 命令行参数，包含连接名称、文件路径和执行选项

        Raises:
            SystemExit: 如果执行文件失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            statements = self._read_and_split_sql_file(args.file)
            results, success_count, error_count = self._execute_sql_statements(
                db_manager, statements, args.connection, args.continue_on_error
            )

            self._print_execution_summary(success_count, error_count)

            if results and args.output:
                self._save_output(results, args.output, args.format)
            elif results:
                self._display_results(results, args.format)

        except FileNotFoundError:
            logger.error("SQL文件不存在: %s", args.file)
            print(f"❌ SQL文件不存在: {args.file}")
            sys.exit(1)
        except Exception as e:
            logger.error("执行SQL文件失败: %s", e)
            print(f"❌ 执行SQL文件失败: {e}")
            sys.exit(1)

    def _read_and_split_sql_file(self, file_path: str) -> List[str]:
        """
        读取SQL文件内容并分割为独立的SQL语句

        Args:
            file_path: SQL文件路径

        Returns:
            List[str]: 分割后的SQL语句列表
        """
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
        return self._split_sql_statements(sql_content)

    def _execute_sql_statements(
        self,
        db_manager: DatabaseManager,
        statements: List[str],
        connection_name: str,
        continue_on_error: bool,
    ) -> tuple[List[Dict], int, int]:
        """
        执行SQL语句列表

        Args:
            db_manager: 数据库管理器实例
            statements: SQL语句列表
            connection_name: 连接名称
            continue_on_error: 遇到错误时是否继续执行

        Returns:
            tuple: (查询结果列表, 成功执行数, 失败执行数)
        """
        total_results = []
        success_count = 0
        error_count = 0

        for i, statement in enumerate(statements, 1):
            if not statement.strip():
                continue

            print(f"执行语句 {i}/{len(statements)}: {self._truncate_sql(statement)}")

            try:
                if statement.lower().strip().startswith("select"):
                    results = db_manager.execute_query(connection_name, statement)
                    total_results.extend(results)
                    success_count += 1
                else:
                    affected = db_manager.execute_command(connection_name, statement)
                    print(f"  影响行数: {affected}")
                    success_count += 1
            except Exception as e:
                error_count += 1
                logger.error("执行语句失败: %s", e)
                print(f"❌ 执行语句失败: {e}")
                if not continue_on_error:
                    sys.exit(1)

        return total_results, success_count, error_count

    def _split_sql_statements(self, sql_content: str) -> List[str]:
        """
        将SQL内容分割为独立的语句

        支持处理多行注释和复杂语句。

        Args:
            sql_content: 原始SQL内容

        Returns:
            List[str]: 分割后的SQL语句列表
        """
        # 移除多行注释
        sql_content = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)

        # 按分号分割语句
        statements = []
        current_statement = ""

        for line in sql_content.split("\n"):
            line = line.strip()
            if line.startswith("--"):  # 跳过单行注释
                continue
            current_statement += " " + line
            if line.endswith(";"):
                statements.append(current_statement.strip())
                current_statement = ""

        # 处理最后可能没有分号的语句
        if current_statement.strip():
            statements.append(current_statement.strip())

        return [stmt for stmt in statements if stmt and not stmt.isspace()]

    def _truncate_sql(self, sql: str, max_length: int = 50) -> str:
        """
        截断SQL语句用于显示，避免过长的输出

        Args:
            sql: 原始SQL语句
            max_length: 最大显示长度

        Returns:
            str: 截断后的SQL语句
        """
        if len(sql) <= max_length:
            return sql
        return sql[:max_length] + "..."

    def _print_execution_summary(self, success_count: int, error_count: int) -> None:
        """
        打印SQL执行统计信息

        Args:
            success_count: 成功执行的语句数
            error_count: 执行失败的语句数
        """
        print(f"\n执行完成: 成功 {success_count} 条，失败 {error_count} 条")

    def _display_results(self, results: List[Dict], format: str = "table") -> None:
        """
        以指定格式显示查询结果

        Args:
            results: 查询结果列表
            format: 显示格式 (table/json/csv)
        """
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

    def _display_table(self, results: List[Dict]) -> None:
        """
        以表格形式显示查询结果

        Args:
            results: 查询结果列表
        """
        if not results:
            return

        headers = list(results[0].keys())

        # 计算每列的最大宽度
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

    def _display_csv(self, results: List[Dict]) -> None:
        """
        以CSV格式显示查询结果

        Args:
            results: 查询结果列表
        """
        if not results:
            return

        headers = list(results[0].keys())
        writer = csv.DictWriter(sys.stdout, fieldnames=headers)
        writer.writeheader()
        writer.writerows(results)

    def _save_output(self, results: List[Dict], output_file: str, format: str) -> None:
        """
        将查询结果保存到文件

        Args:
            results: 查询结果列表
            output_file: 输出文件路径
            format: 输出格式 (table/json/csv)

        Raises:
            SystemExit: 如果保存失败则退出程序
        """
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

            logger.info("结果已保存到: %s", output_file)
            print(f"✅ 结果已保存到: {output_file}")
        except Exception as e:
            logger.error("保存结果失败: %s", e)
            print(f"❌ 保存结果失败: {e}")
            sys.exit(1)

    def _write_csv_results(self, file_handle, results: List[Dict]) -> None:
        """
        将结果以CSV格式写入文件句柄

        Args:
            file_handle: 文件句柄
            results: 查询结果列表
        """
        if results:
            headers = list(results[0].keys())
            writer = csv.DictWriter(file_handle, fieldnames=headers)
            writer.writeheader()
            writer.writerows(results)

    def interactive_shell(self, args: argparse.Namespace) -> None:
        """
        启动交互式SQL Shell

        提供交互式环境执行SQL语句。

        Args:
            args: 命令行参数，包含连接名称

        Raises:
            SystemExit: 如果启动Shell失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            connection = db_manager.get_connection(args.connection)
            print(f"🔗 已连接到: {args.connection}")
            print("输入 SQL 语句执行查询，输入 'quit' 或 'exit' 退出")
            print("输入 'help' 查看可用命令")
            print("-" * 50)

            while True:
                try:
                    user_input = input("SQL> ").strip()

                    if user_input.lower() in ["quit", "exit", "q"]:
                        break
                    elif not user_input:
                        continue
                    elif user_input.lower() == "help":
                        self._show_shell_help()
                        continue

                    # 执行SQL语句
                    if user_input.lower().startswith("select"):
                        results = connection.execute_query(user_input)
                        self._display_results(results, "table")
                    else:
                        affected = connection.execute_command(user_input)
                        print(f"✅ 执行成功，影响行数: {affected}")

                except KeyboardInterrupt:
                    print("\n输入 'quit' 或 'exit' 退出")
                except Exception as e:
                    print(f"❌ 错误: {e}")

        except Exception as e:
            logger.error("启动交互式Shell失败: %s", e)
            print(f"❌ 启动交互式Shell失败: {e}")
            sys.exit(1)
        finally:
            db_manager.close_connection(args.connection)

    def _show_shell_help(self) -> None:
        """显示交互式Shell的帮助信息"""
        help_text = """
可用命令:
  help          - 显示此帮助信息
  quit / exit   - 退出交互式Shell
  SQL语句       - 执行SQL查询或命令

示例:
  SELECT * FROM users;
  INSERT INTO users (name) VALUES ('test');
  UPDATE users SET name = 'new' WHERE id = 1;
        """
        print(help_text)


def main() -> None:
    """
    CLI 主函数

    解析命令行参数并执行相应的命令。
    """
    cli = DBConnectorCLI()
    parser = create_parser()

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    # 执行对应的命令
    if hasattr(args, "func"):
        try:
            args.func(cli, args)
        except KeyboardInterrupt:
            print("\n程序被用户中断")
            sys.exit(130)
        except Exception as e:
            logger.error("命令执行失败: %s", e)
            print(f"❌ 命令执行失败: {e}")
            sys.exit(1)
    else:
        parser.print_help()


def create_parser() -> argparse.ArgumentParser:
    """
    创建命令行参数解析器

    Returns:
        argparse.ArgumentParser: 配置好的参数解析器
    """
    parser = argparse.ArgumentParser(
        description="DB Connector - 跨平台数据库连接管理工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 添加MySQL连接
  db-connector add mysql-dev --type mysql --host localhost --username root --password secret --database testdb
  
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
    add_parser.add_argument("--username", "-u", "--user", help="用户名")
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
