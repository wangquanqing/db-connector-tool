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
"""

import argparse
import csv
import json
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

from .core.connections import SUPPORTED_DATABASE_TYPES, DatabaseManager
from .utils.logging_utils import setup_logging

logger = setup_logging()


class DBConnectorCLI:
    """
    DB Connector 命令行接口主类

    负责处理所有命令行操作，包括连接管理、查询执行等。

    Attributes:
        db_manager (Optional[DatabaseManager]): 数据库管理器实例
        DB_MANAGER_NOT_INIT_MSG (str): 数据库管理器未初始化的错误消息
        BASIC_PARAMS (List[str]): 基本连接参数列表
    """

    DB_MANAGER_NOT_INIT_MSG = "❌ 数据库管理器未初始化"
    BASIC_PARAMS = [
        "type",
        "host",
        "port",
        "username",
        "password",
        "database",
        "service_name",
        "gssencmode",
        "charset",
        "tds_version",
    ]

    def __init__(self):
        """
        初始化DB Connector CLI

        设置日志系统并准备数据库管理器。
        """
        self.db_manager: Optional[DatabaseManager] = None

    def add_connection(self, args: argparse.Namespace) -> None:
        """
        添加新的数据库连接配置

        Args:
            args (argparse.Namespace): 命令行参数，包含连接配置信息

        Raises:
            SystemExit: 如果添加连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()
        config = self._build_connection_config(args)

        try:
            db_manager.add_connection(args.name, config)
            logger.info(f"连接 '{args.name}' 添加成功")
            print(f"✅ 连接 '{args.name}' 添加成功")
            self._print_custom_params(config)
        except Exception as e:
            logger.error(f"添加连接失败: {e}")
            print(f"❌ 添加连接失败: {e}")
            sys.exit(1)

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
                logger.error(f"初始化数据库管理器失败: {e}")
                print(f"❌ 初始化数据库管理器失败: {e}")
                sys.exit(1)
        return self.db_manager

    def _build_connection_config(self, args: argparse.Namespace) -> Dict[str, Any]:
        """
        构建连接配置字典

        Args:
            args (argparse.Namespace): 命令行参数

        Returns:
            Dict[str, Any]: 完整的连接配置字典
        """
        config = {}

        # 添加基本参数
        for param in self.BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                config[param] = value

        # 添加自定义参数
        if hasattr(args, "custom_params") and args.custom_params:
            custom_config = self._parse_custom_params(args.custom_params)
            config.update(custom_config)

        return config

    def _parse_custom_params(self, params: List[str]) -> Dict[str, Any]:
        """
        解析自定义参数列表，支持类型自动转换

        Args:
            params (List[str]): 参数字符串列表，格式为 key=value

        Returns:
            Dict[str, Any]: 转换后的键值对字典

        Raises:
            ValueError: 如果参数格式无效

        Example:
            >>> cli = DBConnectorCLI()
            >>> cli._parse_custom_params(["timeout=30", "ssl=true"])
            {'timeout': 30, 'ssl': True}
        """
        result = {}
        for param in params:
            if "=" not in param:
                logger.warning(f"忽略无效的自定义参数格式: {param}")
                continue

            key, value = param.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key:
                logger.warning(f"忽略空键名的参数: {param}")
                continue

            result[key] = self._convert_value_type(value)

        return result

    def _convert_value_type(self, value: str) -> Union[str, int, float, bool]:
        """
        智能转换参数值的数据类型

        支持转换: 布尔值(true/false)、整数、浮点数、字符串

        Args:
            value (str): 原始字符串值

        Returns:
            Union[str, int, float, bool]: 转换后的值

        Example:
            >>> cli = DBConnectorCLI()
            >>> cli._convert_value_type("true")
            True
            >>> cli._convert_value_type("123")
            123
            >>> cli._convert_value_type("3.14")
            3.14
            >>> cli._convert_value_type("hello")
            'hello'
        """
        value_lower = value.lower().strip()

        # 布尔值转换
        if value_lower in ("true", "false"):
            return value_lower == "true"

        # 整数转换
        if value.isdigit():
            return int(value)

        # 浮点数转换
        try:
            return float(value)
        except ValueError:
            pass

        # 保持原字符串
        return value

    def _print_custom_params(self, config: Dict[str, Any]) -> None:
        """
        打印自定义参数信息

        Args:
            config (Dict[str, Any]): 连接配置字典
        """
        custom_params = [k for k in config.keys() if k not in self.BASIC_PARAMS]
        if custom_params:
            print(f"📋 自定义参数: {', '.join(custom_params)}")

    def remove_connection(self, args: argparse.Namespace) -> None:
        """
        删除指定的数据库连接配置

        Args:
            args (argparse.Namespace): 命令行参数，包含要删除的连接名称

        Raises:
            SystemExit: 如果删除连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            db_manager.remove_connection(args.name)
            logger.info(f"连接 '{args.name}' 已删除")
            print(f"✅ 连接 '{args.name}' 已删除")
        except Exception as e:
            logger.error(f"删除连接失败: {e}")
            print(f"❌ 删除连接失败: {e}")
            sys.exit(1)

    def update_connection(self, args: argparse.Namespace) -> None:
        """
        更新数据库连接配置

        Args:
            args (argparse.Namespace): 命令行参数，包含连接名称和更新配置

        Raises:
            SystemExit: 如果更新连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            existing_config = db_manager.show_connection(args.name)
            update_config = self._build_update_config(existing_config, args)

            db_manager.update_connection(args.name, update_config)
            logger.info(f"连接 '{args.name}' 更新成功")
            print(f"✅ 连接 '{args.name}' 更新成功")
            self._print_custom_params(update_config)
        except Exception as e:
            logger.error(f"更新连接失败: {e}")
            print(f"❌ 更新连接失败: {e}")
            sys.exit(1)

    def _build_update_config(
        self, existing_config: Dict[str, Any], args: argparse.Namespace
    ) -> Dict[str, Any]:
        """
        根据命令行参数构建更新后的配置

        Args:
            existing_config (Dict[str, Any]): 现有配置
            args (argparse.Namespace): 命令行参数

        Returns:
            Dict[str, Any]: 更新后的配置
        """
        update_config = existing_config.copy()

        # 更新基本参数
        for param in self.BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                update_config[param] = value

        # 更新自定义参数
        if hasattr(args, "custom_params") and args.custom_params:
            custom_config = self._parse_custom_params(args.custom_params)
            update_config.update(custom_config)

        return update_config

    def show_connection(self, args: argparse.Namespace) -> None:
        """
        显示指定连接的详细配置信息

        注意：敏感信息（如密码）会被隐藏显示。

        Args:
            args (argparse.Namespace): 命令行参数，包含要显示详情的连接名称

        Raises:
            SystemExit: 如果获取连接详情失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            config = db_manager.show_connection(args.name)
            safe_config = self._sanitize_sensitive_info(config)

            print(f"🔍 连接 '{args.name}' 的配置:")
            self._display_connection_details(safe_config)

        except Exception as e:
            logger.error(f"获取连接详情失败: {e}")
            print(f"❌ 获取连接详情失败: {e}")
            sys.exit(1)

    def _sanitize_sensitive_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        隐藏敏感信息（如密码）

        Args:
            config (Dict[str, Any]): 原始配置

        Returns:
            Dict[str, Any]: 安全配置（敏感信息已隐藏）
        """
        safe_config = config.copy()
        password_fields = ["password", "passwd", "pwd"]

        for field in password_fields:
            if field in safe_config:
                safe_config[field] = "***"

        return safe_config

    def _display_connection_details(self, config: Dict[str, Any]) -> None:
        """
        显示连接配置详情

        Args:
            config (Dict[str, Any]): 连接配置
        """
        # 显示基本参数
        for key in self.BASIC_PARAMS:
            if key in config:
                print(f"  {key}: {config[key]}")

        # 显示自定义参数
        custom_params = [k for k in config.keys() if k not in self.BASIC_PARAMS]
        if custom_params:
            print("\n  📋 自定义参数:")
            for key in custom_params:
                print(f"    {key}: {config[key]}")

    def list_connections(self, _args: argparse.Namespace) -> None:
        """
        列出所有已配置的数据库连接

        Args:
            _args (argparse.Namespace): 命令行参数（未使用）

        Raises:
            SystemExit: 如果列出连接失败则退出程序
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            connections = db_manager.list_connections()
            if connections:
                print("📋 已配置的连接:")
                for i, conn in enumerate(connections, 1):
                    print(f"  {i}. {conn}")
            else:
                print("ℹ️  没有配置任何连接")
        except Exception as e:
            logger.error(f"列出连接失败: {e}")
            print(f"❌ 列出连接失败: {e}")
            sys.exit(1)

    def test_connection(self, args: argparse.Namespace) -> None:
        """
        测试指定连接的连通性

        Args:
            args (argparse.Namespace): 命令行参数，包含要测试的连接名称

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
            logger.error(f"连接测试失败: {e}")
            print(f"❌ 连接测试失败: {e}")
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
            logger.error(f"执行查询失败: {e}")
            print(f"❌ 执行查询失败: {e}")
            sys.exit(1)

    def _save_output(self, results: List[Dict], output_path: str, format: str) -> None:
        """
        将查询结果保存到文件

        Args:
            results: 查询结果列表
            output_path: 输出文件路径
            format: 输出格式

        Raises:
            SystemExit: 如果保存失败
        """
        try:
            if format == "json":
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
            elif format == "csv":
                with open(output_path, "w", newline="", encoding="utf-8") as f:
                    if results:
                        headers = list(results[0].keys())
                        writer = csv.DictWriter(f, fieldnames=headers)
                        writer.writeheader()
                        writer.writerows(results)
            else:  # table格式保存为文本
                with open(output_path, "w", encoding="utf-8") as f:
                    # 重定向标准输出到文件
                    original_stdout = sys.stdout
                    sys.stdout = f
                    self._display_table(results)
                    sys.stdout = original_stdout

            print(f"✅ 结果已保存到: {output_path}")
        except Exception as e:
            logger.error(f"保存结果失败: {e}")
            print(f"❌ 保存结果失败: {e}")
            sys.exit(1)

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

        # 限制最大列宽
        max_col_width = 50
        for header in headers:
            col_widths[header] = min(col_widths[header], max_col_width)

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
                    f"{self._truncate_value(str(row.get(header, '')), col_widths[header]):<{col_widths[header]}}"
                    for header in headers
                ]
            )
            print(row_line)

        print(separator)
        print(f"总计: {len(results)} 行")

    def _truncate_value(self, value: str, max_length: int) -> str:
        """
        截断过长的值用于表格显示

        Args:
            value: 原始值
            max_length: 最大长度

        Returns:
            str: 截断后的值
        """
        if len(value) <= max_length:
            return value
        return value[: max_length - 3] + "..."

    def _display_results(self, results: List[Dict], format: str = "table") -> None:
        """
        以指定格式显示查询结果

        Args:
            results: 查询结果列表
            format: 显示格式 (table/json/csv)

        Raises:
            ValueError: 如果格式不支持
        """
        if not results:
            print("没有结果")
            return

        if format == "table":
            self._display_table(results)
        elif format == "json":
            self._display_json(results)
        elif format == "csv":
            self._display_csv(results)
        else:
            print(f"❌ 不支持的输出格式: {format}")
            print("✅ 支持的格式: table, json, csv")
            sys.exit(1)

    def _display_json(self, results: List[Dict]) -> None:
        """
        以JSON格式显示查询结果

        Args:
            results: 查询结果列表
        """
        try:
            print(json.dumps(results, indent=2, ensure_ascii=False))
        except Exception as e:
            logger.error(f"JSON序列化失败: {e}")
            print(f"❌ JSON序列化失败: {e}")
            sys.exit(1)

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
        for row in results:
            writer.writerow(row)

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

        # 验证文件存在性
        if not os.path.exists(args.file):
            logger.error(f"SQL文件不存在: {args.file}")
            print(f"❌ SQL文件不存在: {args.file}")
            sys.exit(1)

        try:
            statements = self._read_and_split_sql_file(args.file)
            if not statements:
                print("ℹ️  SQL文件中没有有效的SQL语句")
                return

            results, success_count, error_count = self._execute_sql_statements(
                db_manager, statements, args.connection, args.continue_on_error
            )

            self._print_execution_summary(success_count, error_count)

            if results and args.output:
                self._save_output(results, args.output, args.format)
            elif results:
                self._display_results(results, args.format)

        except Exception as e:
            logger.error(f"执行SQL文件失败: {e}")
            print(f"❌ 执行SQL文件失败: {e}")
            sys.exit(1)

    def _read_and_split_sql_file(self, file_path: str) -> List[str]:
        """
        读取SQL文件内容并分割为独立的SQL语句

        Args:
            file_path: SQL文件路径

        Returns:
            List[str]: 分割后的SQL语句列表

        Raises:
            UnicodeDecodeError: 如果文件编码不支持
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                sql_content = f.read()
            return self._split_sql_statements(sql_content)
        except UnicodeDecodeError:
            # 尝试其他编码
            try:
                with open(file_path, "r", encoding="gbk") as f:
                    sql_content = f.read()
                return self._split_sql_statements(sql_content)
            except UnicodeDecodeError as e:
                logger.error(f"无法解码SQL文件: {e}")
                print("❌ 无法解码SQL文件，请检查文件编码")
                sys.exit(1)

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

    def _execute_sql_statements(
        self,
        db_manager: DatabaseManager,
        statements: List[str],
        connection_name: str,
        continue_on_error: bool,
    ) -> Tuple[List[Dict], int, int]:
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
                    print(f"✅ 查询成功，返回 {len(results)} 行结果")
                else:
                    affected = db_manager.execute_command(connection_name, statement)
                    print(f"✅ 执行成功，影响行数: {affected}")
                    success_count += 1
            except Exception as e:
                error_count += 1
                logger.error(f"执行语句失败: {e}")
                print(f"❌ 执行语句失败: {e}")
                if not continue_on_error:
                    sys.exit(1)

        return total_results, success_count, error_count

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
        total = success_count + error_count
        success_rate = (success_count / total * 100) if total > 0 else 0
        print(f"\n执行完成: 成功 {success_count} 条，失败 {error_count} 条")
        print(f"成功率: {success_rate:.1f}%")

    def interactive_shell(self, args: argparse.Namespace) -> None:
        """
        启动交互式SQL Shell

        Args:
            args: 命令行参数，包含连接名称

        Raises:
            SystemExit: 如果启动Shell失败
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            print(f"🚀 启动SQL Shell (连接: {args.connection})")
            print("输入 'exit' 或 'quit' 退出")
            print("输入 'help' 查看帮助")

            while True:
                try:
                    sql = input(f"{args.connection}> ").strip()
                    if sql.lower() in ["exit", "quit"]:
                        break
                    elif sql.lower() == "help":
                        self._print_shell_help()
                        continue
                    elif not sql:
                        continue

                    # 执行SQL
                    if sql.lower().startswith("select"):
                        results = db_manager.execute_query(args.connection, sql)
                        self._display_results(results, "table")
                    else:
                        affected = db_manager.execute_command(args.connection, sql)
                        print(f"影响行数: {affected}")

                except KeyboardInterrupt:
                    print("\n👋 再见!")
                    break
                except Exception as e:
                    print(f"❌ 执行错误: {e}")

        except Exception as e:
            logger.error(f"启动SQL Shell失败: {e}")
            print(f"❌ 启动SQL Shell失败: {e}")
            sys.exit(1)

    def _print_shell_help(self) -> None:
        """打印SQL Shell帮助信息"""
        help_text = """
SQL Shell 命令:
  exit, quit    - 退出Shell
  help         - 显示此帮助信息

支持的SQL语句:
  SELECT       - 执行查询并显示结果
  INSERT       - 插入数据
  UPDATE       - 更新数据
  DELETE       - 删除数据
  CREATE       - 创建表或数据库
  DROP         - 删除表或数据库
        """
        print(help_text)


class ChineseHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """中文帮助格式化器，优化帮助信息显示"""

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


def create_argument_parser(cli_instance: DBConnectorCLI) -> argparse.ArgumentParser:
    """
    创建命令行参数解析器

    Args:
        cli_instance (DBConnectorCLI): 已初始化的CLI实例

    Returns:
        argparse.ArgumentParser: 配置好的参数解析器
    """
    parser = argparse.ArgumentParser(
        usage="db-connector [<命令>] [<选项>]",
        description="DB Connector - 数据库连接管理工具",
        formatter_class=ChineseHelpFormatter,
        epilog="""
使用示例:
  db-connector add mysql-dev --type mysql --host localhost --username root
  db-connector list
  db-connector query mysql-dev "SELECT * FROM users"
  db-connector shell mysql-dev

更多帮助请参考项目文档。
        """,
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
    """
    设置连接相关的命令行参数

    Args:
        parser (argparse.ArgumentParser): 参数解析器实例
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
    parser.add_argument("-p", "--password", help="密码")
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
        help="MySQL/MSSQL字符集",
    )
    parser.add_argument(
        "-t",
        "--tds-version",
        choices=["7.0", "7.1", "7.2", "7.3", "7.4", "8.0"],
        help="MSSQL TDS 版本",
    )
    parser.add_argument(
        "-c", "--custom-params", nargs="+", help="自定义参数 (格式: key=value)"
    )


def main():
    """DB Connector CLI 主入口函数"""
    cli = DBConnectorCLI()
    parser = create_argument_parser(cli)

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(0)

    args = parser.parse_args()

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
