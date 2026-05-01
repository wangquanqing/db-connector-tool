"""命令行界面模块 (CLI Module)

提供数据库连接管理和 SQL 执行的命令行接口，支持连接增删改查、
SQL 查询/命令/文件执行及交互式 Shell。

Example:
>>> from db_connector_tool.cli_core import DBConnectorCLI
>>> cli = DBConnectorCLI()
>>> # 命令行用法:
>>> # db-connector add mysql-dev --type mysql --host localhost --username root
>>> # db-connector list
>>> # db-connector query mysql-dev "SELECT * FROM users"
>>> # db-connector file mysql-dev script.sql
>>> # db-connector shell mysql-dev
"""

import argparse
import csv
import getpass
import json
import shutil
import sys
from typing import Any, Dict, List, Optional, Union

from .__about__ import __version__
from .core.connections import DatabaseManager
from .core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
    FileSystemError,
)
from .drivers.sqlalchemy_driver import BASIC_PARAMS
from .utils.logging_utils import setup_logging
from .utils.path_utils import PathHelper
from .utils.sqlparse_utils import read_and_split_sql_file

logger = setup_logging(app_name="db_connector_tool", level="debug")


class DBConnectorCLI:
    """命令行接口主类 (DB Connector CLI)

    管理数据库连接配置并通过命令行执行 SQL 操作，
    提供连接增删改查、SQL 查询/命令/文件执行和交互式 Shell 功能。

    Example:
    >>> cli = DBConnectorCLI()
    >>> cli.add_connection(args)
    >>> cli.execute_query(args)
    """

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

    def __init__(self) -> None:
        """初始化命令行接口实例

        Example:
        >>> cli = DBConnectorCLI()
        """
        self.database_manager: Optional[DatabaseManager] = None

    def show_version(self, _args: argparse.Namespace) -> None:
        """显示当前模块版本信息

        Args:
            _args: 命令行参数（未使用）

        Example:
        >>> # db-connector --version
        """
        print(f"DB Connector Tool 版本: {__version__}")
        print("支持的数据类型: Oracle, PostgreSQL, MySQL, SQL Server, SQLite, GBase 8s")
        print("许可证: MIT")
        print("作者: wangquanqing")

    def add_connection(self, args: argparse.Namespace) -> None:
        """添加新的数据库连接配置

        Args:
            args: 命令行参数，包含连接配置信息

        Raises:
            SystemExit: 添加连接失败时退出

        Example:
        >>> # db-connector add mysql-dev --type mysql --host localhost --username root
        """
        database_manager = self._ensure_database_manager_initialized()
        connection_config = self._build_connection_config(args)

        try:
            database_manager.add_connection(args.name, connection_config)
            logger.info("连接 '%s' 添加成功", args.name)
            print(f"✅ 连接 '{args.name}' 添加成功")
            self._print_custom_parameters(connection_config)
        except (DatabaseError, ConfigError) as error:
            logger.error("添加连接失败: %s", error)
            print(f"❌ 添加连接失败: {error}")
            sys.exit(1)

    def _ensure_database_manager_initialized(self) -> DatabaseManager:
        """确保数据库管理器已初始化

        Returns:
            DatabaseManager: 已初始化的数据库管理器实例

        Raises:
            SystemExit: 初始化失败时退出
        """
        if self.database_manager is None:
            try:
                self.database_manager = DatabaseManager()
            except DBConnectorError as error:
                logger.error("初始化数据库管理器失败: %s", error)
                print(f"❌ 初始化数据库管理器失败: {error}")
                sys.exit(1)
        return self.database_manager

    def _build_connection_config(self, args: argparse.Namespace) -> Dict[str, Any]:
        """根据命令行参数构建连接配置字典

        Args:
            args: 命令行参数

        Returns:
            Dict[str, Any]: 完整的连接配置字典
        """
        connection_config: Dict[str, Any] = {}

        for param in BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                connection_config[param] = value

        if hasattr(args, "password") and args.password is not None:
            if args.password == "":
                connection_config["password"] = getpass.getpass("请输入数据库密码: ")
            else:
                connection_config["password"] = args.password

        if hasattr(args, "custom_params") and args.custom_params:
            custom_config = self._parse_custom_parameters(args.custom_params)
            connection_config.update(custom_config)

        return connection_config

    def _parse_custom_parameters(self, parameters: List[str]) -> Dict[str, Any]:
        """解析自定义参数列表，支持类型自动转换

        Args:
            parameters: 参数字符串列表，格式为 key=value

        Returns:
            Dict[str, Any]: 转换后的键值对字典

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._parse_custom_parameters(["timeout=30", "ssl=true"])
        {'timeout': 30, 'ssl': True}
        """
        result: Dict[str, Any] = {}
        for param in parameters:
            if "=" not in param:
                logger.warning("忽略无效的自定义参数格式: %s", param)
                continue

            key, value = param.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key:
                logger.warning("忽略空键名的参数: %s", param)
                continue

            result[key] = self._coerce_value_type(value)

        return result

    def _coerce_value_type(self, value: str) -> Union[str, int, float, bool]:
        """智能转换参数值的数据类型

        Args:
            value: 原始字符串值

        Returns:
            Union[str, int, float, bool]: 转换后的值

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._coerce_value_type("true")
        True
        >>> cli._coerce_value_type("123")
        123
        """
        value_lower = value.lower().strip()

        if value_lower in ("true", "false"):
            return value_lower == "true"

        if value.isdigit():
            return int(value)

        try:
            return float(value)
        except ValueError:
            pass

        return value

    def _print_custom_parameters(self, connection_config: Dict[str, Any]) -> None:
        """打印自定义参数信息

        Args:
            connection_config: 连接配置字典
        """
        custom_keys = [k for k in connection_config.keys() if k not in BASIC_PARAMS]
        if custom_keys:
            print(f"📋 自定义参数: {', '.join(custom_keys)}")

    def remove_connection(self, args: argparse.Namespace) -> None:
        """删除指定的数据库连接配置

        Args:
            args: 命令行参数，包含要删除的连接名称

        Raises:
            SystemExit: 删除连接失败时退出

        Example:
        >>> # db-connector remove mysql-dev
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            database_manager.remove_connection(args.name)
            logger.info("连接 '%s' 已删除", args.name)
            print(f"✅ 连接 '{args.name}' 已删除")
        except (DatabaseError, ConfigError) as error:
            logger.error("删除连接失败: %s", error)
            print(f"❌ 删除连接失败: {error}")
            sys.exit(1)

    def update_connection(self, args: argparse.Namespace) -> None:
        """更新数据库连接配置

        Args:
            args: 命令行参数，包含连接名称和更新配置

        Raises:
            SystemExit: 更新连接失败时退出

        Example:
        >>> # db-connector update mysql-dev --host new_host
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            existing_config = database_manager.show_connection(args.name)
            updated_config = self._build_updated_config(existing_config, args)

            database_manager.update_connection(args.name, updated_config)
            logger.info("连接 '%s' 更新成功", args.name)
            print(f"✅ 连接 '{args.name}' 更新成功")
            self._print_custom_parameters(updated_config)
        except (DatabaseError, ConfigError) as error:
            logger.error("更新连接失败: %s", error)
            print(f"❌ 更新连接失败: {error}")
            sys.exit(1)

    def _build_updated_config(
        self, existing_config: Dict[str, Any], args: argparse.Namespace
    ) -> Dict[str, Any]:
        """根据命令行参数构建更新后的配置

        Args:
            existing_config: 现有配置
            args: 命令行参数

        Returns:
            Dict[str, Any]: 更新后的配置
        """
        updated_config = existing_config.copy()

        for param in BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                updated_config[param] = value

        if hasattr(args, "password") and args.password is not None:
            if args.password == "":
                updated_config["password"] = getpass.getpass("请输入数据库密码: ")
            else:
                updated_config["password"] = args.password

        if hasattr(args, "custom_params") and args.custom_params:
            custom_config = self._parse_custom_parameters(args.custom_params)
            updated_config.update(custom_config)

        return updated_config

    def show_connection(self, args: argparse.Namespace) -> None:
        """显示指定连接的详细配置信息

        Args:
            args: 命令行参数，包含要显示详情的连接名称

        Raises:
            SystemExit: 获取连接详情失败时退出

        Example:
        >>> # db-connector show mysql-dev
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            connection_config = database_manager.show_connection(args.name)
            safe_config = self._mask_sensitive_fields(connection_config)

            print(f"🔍 连接 '{args.name}' 的配置:")
            self._display_connection_details(safe_config)

        except (DatabaseError, ConfigError) as error:
            logger.error("获取连接详情失败: %s", error)
            print(f"❌ 获取连接详情失败: {error}")
            sys.exit(1)

    def _mask_sensitive_fields(
        self, connection_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """隐藏配置中的敏感字段（如密码）

        Args:
            connection_config: 原始配置

        Returns:
            Dict[str, Any]: 掩码后的安全配置
        """
        safe_config = connection_config.copy()
        for field in ("password", "passwd", "pwd"):
            if field in safe_config:
                safe_config[field] = "***"
        return safe_config

    def _display_connection_details(self, connection_config: Dict[str, Any]) -> None:
        """显示连接配置详情

        Args:
            connection_config: 连接配置
        """
        for key in BASIC_PARAMS:
            if key in connection_config:
                print(f"  {key}: {connection_config[key]}")

        custom_keys = [k for k in connection_config.keys() if k not in BASIC_PARAMS]
        if custom_keys:
            print("\n  📋 自定义参数:")
            for key in custom_keys:
                print(f"    {key}: {connection_config[key]}")

    def list_connections(self, _args: argparse.Namespace) -> None:
        """列出所有已配置的数据库连接

        Args:
            _args: 命令行参数（未使用）

        Raises:
            SystemExit: 列出连接失败时退出

        Example:
        >>> # db-connector list
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            connections = database_manager.list_connections()
            if connections:
                print("📋 已配置的连接:")
                for index, name in enumerate(connections, 1):
                    print(f"  {index}. {name}")
            else:
                print("ℹ️  没有配置任何连接")
        except (DatabaseError, ConfigError) as error:
            logger.error("列出连接失败: %s", error)
            print(f"❌ 列出连接失败: {error}")
            sys.exit(1)

    def test_connection(self, args: argparse.Namespace) -> None:
        """测试指定连接的连通性

        Args:
            args: 命令行参数，包含要测试的连接名称

        Raises:
            SystemExit: 连接测试失败时退出

        Example:
        >>> # db-connector test mysql-dev
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            if database_manager.test_connection(args.name):
                print(f"✅ 连接 '{args.name}' 测试成功")
            else:
                print(f"❌ 连接 '{args.name}' 测试失败")
                sys.exit(1)
        except (DatabaseError, ConfigError) as error:
            logger.error("连接测试失败: %s", error)
            print(f"❌ 连接测试失败: {error}")
            sys.exit(1)

    def execute_query(self, args: argparse.Namespace) -> None:
        """执行 SQL 查询语句

        Args:
            args: 命令行参数，包含连接名称、SQL 查询和输出选项

        Raises:
            SystemExit: 执行失败时退出

        Example:
        >>> # db-connector query mysql-dev "SELECT * FROM users"
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            results, response_time = database_manager.execute_query(
                args.connection, args.sql_content
            )

            if args.output:
                self._save_output(results, args.output)
            else:
                self._display_results(results)
            print(f"✅ 总计: {len(results)} 行 ({response_time:.3f} 秒)")

        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行SQL失败: %s", error)
            print(f"❌ 执行SQL失败: {error}")
            sys.exit(1)

    def _save_output(self, results: List[Dict], output_path: str) -> None:
        """将查询结果保存到文件

        Args:
            results: 查询结果列表
            output_path: 输出文件路径

        Raises:
            SystemExit: 保存失败时退出
        """
        try:
            extension = PathHelper.get_file_extension(output_path, include_dot=False)
            if extension == "json":
                self._save_as_json(results, output_path)
            elif extension == "csv":
                self._save_as_csv(results, output_path)
            else:
                raise FileSystemError(f"不支持的输出文件格式: {output_path}")
            print(f"✅ 结果已保存到: {output_path}")
        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("保存结果失败: %s", error)
            print(f"❌ 保存结果失败: {error}")
            sys.exit(1)

    @staticmethod
    def _to_serializable(obj: Any) -> Any:
        """将对象转换为可序列化格式

        Args:
            obj: 需要转换的对象

        Returns:
            Any: 可序列化的值
        """
        try:
            return dict(obj)
        except (TypeError, ValueError):
            pass
        if hasattr(obj, "_asdict") and callable(getattr(obj, "_asdict")):
            try:
                return obj._asdict()
            except (TypeError, ValueError):
                pass
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        if hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes)):
            try:
                return list(obj)
            except (TypeError, ValueError):
                pass
        return str(obj)

    def _save_as_json(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为 JSON 格式

        Args:
            results: 查询结果列表
            output_path: 输出文件路径
        """
        if not results:
            return
        try:
            with open(output_path, "w", encoding="utf-8") as file:
                json.dump(
                    results,
                    file,
                    indent=2,
                    ensure_ascii=False,
                    default=self._to_serializable,
                )
        except (ValueError, TypeError) as error:
            logger.error("JSON序列化失败: %s", error)
            print(f"❌ JSON保存失败: {error}")
            sys.exit(1)

    def _save_as_csv(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为 CSV 格式

        Args:
            results: 查询结果列表
            output_path: 输出文件路径
        """
        if not results:
            return
        with open(output_path, "w", newline="", encoding="utf-8") as file:
            headers = list(results[0].keys())
            writer = csv.DictWriter(file, fieldnames=headers)
            writer.writeheader()
            for row in results:
                writer.writerow(
                    {key: self._to_serializable(value) for key, value in row.items()}
                )

    def _display_results(self, results: List[Dict]) -> None:
        """以表格格式在终端显示查询结果

        当终端宽度不足以显示所有列时，自动选择能容纳的列显示，
        并提示用户使用 --output 导出完整数据。

        Args:
            results: 查询结果列表
        """
        if not results:
            print("没有结果")
            return

        headers = list(results[0].keys())
        terminal_width = self._get_terminal_width()
        column_widths = self._calculate_column_widths(headers, results)
        total_table_width = sum(column_widths[header] + 3 for header in headers) - 1

        if total_table_width > terminal_width:
            display_headers = self._select_columns_for_display(
                headers, terminal_width, column_widths
            )
            hidden_count = len(headers) - len(display_headers)

            if display_headers:
                print(
                    f"📋 显示 {len(display_headers)}/{len(headers)} 列（终端宽度限制）"
                )
                print(
                    f"🔍 隐藏了 {hidden_count} 列，使用 --output csv/json 查看完整数据"
                )
                print()
                filtered_results = [
                    {key: value for key, value in row.items() if key in display_headers}
                    for row in results
                ]
                filtered_column_widths = {h: column_widths[h] for h in display_headers}
                self._print_table(
                    display_headers, filtered_results, filtered_column_widths
                )
            else:
                print("❌ 终端宽度过小，无法显示任何列")
                print("💡 建议使用 --output csv/json 参数导出数据查看")
        else:
            self._print_table(headers, results, column_widths)

    def _get_terminal_width(self) -> int:
        """获取终端宽度

        Returns:
            int: 终端宽度（字符列数）
        """
        try:
            return shutil.get_terminal_size().columns
        except OSError:
            return 120

    def _calculate_column_widths(
        self, headers: List[str], results: List[Dict]
    ) -> Dict[str, int]:
        """计算每列的最大显示宽度

        Args:
            headers: 表头列表
            results: 查询结果列表

        Returns:
            Dict[str, int]: 列名到显示宽度的映射
        """
        max_column_width = 50
        column_widths: Dict[str, int] = {}

        for header in headers:
            column_widths[header] = self._get_display_width(str(header))

        for row in results:
            for header in headers:
                value_width = self._get_display_width(str(row.get(header, "")))
                column_widths[header] = max(column_widths[header], value_width)

        for header in headers:
            column_widths[header] = min(column_widths[header], max_column_width)

        return column_widths

    def _get_display_width(self, text: str) -> int:
        """计算字符串在终端中的显示宽度

        中文字符和中文标点占 2 个宽度，英文字符占 1 个。

        Args:
            text: 输入字符串

        Returns:
            int: 显示宽度
        """
        return sum(self._get_character_width(char) for char in text)

    def _get_character_width(self, char: str) -> int:
        """计算单个字符的显示宽度

        Args:
            char: 单个字符

        Returns:
            int: 字符宽度（CJK 字符占 2，ASCII 字符占 1）
        """
        if (
            "\u4e00" <= char <= "\u9fff"
            or "\u3000" <= char <= "\u303f"
            or "\uff00" <= char <= "\uffef"
        ):
            return 2
        return 1

    def _select_columns_for_display(
        self,
        headers: List[str],
        terminal_width: int,
        column_widths: Dict[str, int],
    ) -> List[str]:
        """根据终端宽度选择可容纳的显示列

        Args:
            headers: 所有表头列表
            terminal_width: 终端宽度
            column_widths: 列宽度字典

        Returns:
            List[str]: 按原始顺序选择的列列表
        """
        if not headers:
            return []

        selected: List[str] = []
        current_width = 0

        for header in headers:
            column_total = column_widths[header] + 3
            if current_width + column_total <= terminal_width:
                selected.append(header)
                current_width += column_total
            else:
                break

        return selected

    def _print_table(
        self,
        headers: List[str],
        results: List[Dict],
        column_widths: Dict[str, int],
    ) -> None:
        """打印表格

        Args:
            headers: 表头列表
            results: 查询结果
            column_widths: 列宽度映射
        """
        separator_parts = ["-" * (column_widths[header] + 2) for header in headers]
        separator = "+" + "+".join(separator_parts) + "+"

        print(separator)
        self._print_table_row(headers, headers, column_widths, is_header=True)
        print(separator)
        for row in results:
            self._print_table_row(row, headers, column_widths, is_header=False)
        print(separator)

    def _print_table_row(
        self,
        row_data: List[str] | Dict[str, Any],
        headers: List[str],
        column_widths: Dict[str, int],
        is_header: bool = False,
    ) -> None:
        """打印表格的单个数据行

        Args:
            row_data: 行数据（表头为 List[str]，数据行为 Dict[str, Any]）
            headers: 表头列表
            column_widths: 列宽度映射
            is_header: 是否为表头行
        """
        cells: List[str] = []
        for header in headers:
            if is_header:
                if isinstance(row_data, list):
                    cell_text = str(row_data[headers.index(header)])
                else:
                    cell_text = str(header)
            else:
                if isinstance(row_data, dict):
                    cell_text = self._truncate_value(
                        str(row_data.get(header, "")), column_widths[header]
                    )
                else:
                    cell_text = ""

            actual_width = self._get_display_width(cell_text)
            padding = column_widths[header] - actual_width
            left_padding = padding // 2
            right_padding = padding - left_padding
            cells.append(f"{' ' * left_padding}{cell_text}{' ' * right_padding}")

        print(f"| {' | '.join(cells)} |")

    def _truncate_value(self, value: str, max_width: int) -> str:
        """截断过长的值用于表格显示

        考虑 CJK 字符宽度进行智能截断。

        Args:
            value: 原始值
            max_width: 最大显示宽度

        Returns:
            str: 截断后的值

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._truncate_value("长文本内容", 5)
        '长...'
        """
        if self._get_display_width(value) <= max_width:
            return value

        current_width = 0
        truncated = ""
        for char in value:
            char_width = self._get_character_width(char)
            if current_width + char_width > max_width - 3:
                break
            truncated += char
            current_width += char_width

        return truncated + "..."

    def execute_command(self, args: argparse.Namespace) -> None:
        """执行 SQL 增删改操作（INSERT/UPDATE/DELETE 等）

        Args:
            args: 命令行参数，包含连接名称和 SQL 命令

        Raises:
            SystemExit: 执行失败时退出

        Example:
        >>> # db-connector command mysql-dev "INSERT INTO users VALUES (1, 'John')"
        """
        database_manager = self._ensure_database_manager_initialized()

        try:
            affected, response_time = database_manager.execute_command(
                args.connection, args.sql_content
            )
            print(f"✅ 影响: {affected} 行 ({response_time:.3f} 秒)")

        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行命令失败: %s", error)
            print(f"❌ 执行命令失败: {error}")
            sys.exit(1)

    def execute_file(self, args: argparse.Namespace) -> None:
        """执行 SQL 文件

        读取并解析 SQL 文件，自动识别每条语句类型（查询/命令）并分别执行。

        Args:
            args: 命令行参数，包含连接名称、SQL 文件路径和执行选项

        Raises:
            SystemExit: 执行失败时退出

        Example:
        >>> # db-connector file mysql-dev script.sql
        """
        try:
            statements = read_and_split_sql_file(args.file_path)
            if not statements:
                print("ℹ️  SQL文件中没有有效的SQL语句")
                return

            success_count = 0
            error_count = 0

            for index, statement in enumerate(statements, 1):
                if not statement.strip():
                    continue

                print(f"执行语句 {index}/{len(statements)}:", end=" ")

                success = self._execute_sql_statement(
                    args.connection, statement, show_truncated=True
                )

                if success:
                    success_count += 1
                else:
                    error_count += 1
                    if not args.continue_on_error:
                        sys.exit(1)

            total = success_count + error_count
            success_rate = (success_count / total * 100) if total > 0 else 0
            print(f"\n执行完成: 成功 {success_count} 条，失败 {error_count} 条")
            print(f"成功率: {success_rate:.1f}%")

        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行SQL文件失败: %s", error)
            print(f"❌ 执行SQL文件失败: {error}")
            sys.exit(1)

    def _execute_sql_statement(
        self,
        connection_name: str,
        sql: str,
        show_truncated: bool = False,
    ) -> bool:
        """执行单个 SQL 语句

        Args:
            connection_name: 连接名称
            sql: SQL 语句
            show_truncated: 是否显示截断后的语句

        Returns:
            bool: 执行是否成功
        """
        database_manager = self._ensure_database_manager_initialized()

        if show_truncated:
            print(f"执行: {self._truncate_sql(sql)}")

        try:
            if sql.lower().strip().startswith("select"):
                results, response_time = database_manager.execute_query(
                    connection_name, sql
                )
                self._display_results(results)
                print(f"✅ 总计: {len(results)} 行 ({response_time:.3f} 秒)")
            else:
                affected, response_time = database_manager.execute_command(
                    connection_name, sql
                )
                print(f"✅ 影响: {affected} 行 ({response_time:.3f} 秒)")
            return True
        except (DatabaseError, ConfigError) as error:
            logger.error("执行语句失败: %s", error)
            print(f"❌ 执行语句失败: {error}")
            return False

    def _truncate_sql(self, sql: str, max_length: int = 50) -> str:
        """截断 SQL 语句用于显示

        Args:
            sql: 原始 SQL 语句
            max_length: 最大显示长度

        Returns:
            str: 截断后的 SQL 语句
        """
        if len(sql) <= max_length:
            return sql
        return sql[:max_length].rstrip() + "..."

    def interactive_shell(self, args: argparse.Namespace) -> None:
        """启动交互式 SQL Shell

        Args:
            args: 命令行参数，包含连接名称

        Raises:
            SystemExit: 启动 Shell 失败时退出

        Example:
        >>> # db-connector shell mysql-dev
        """
        try:
            print(f"🚀 启动SQL Shell (连接: {args.connection})")
            print("输入 'exit' 或 'quit' 退出")
            print("输入 'help' 查看帮助")

            while True:
                try:
                    sql = input(f"{args.connection}> ").strip()
                    if sql.lower() in ("exit", "quit"):
                        break
                    if sql.lower() == "help":
                        print(self.help_text)
                        continue
                    if not sql:
                        continue

                    self._execute_sql_statement(args.connection, sql)

                except KeyboardInterrupt:
                    print("\n👋 再见!")
                    break
                except (DatabaseError, ConfigError) as error:
                    print(f"❌ 执行错误: {error}")

        except (DatabaseError, ConfigError) as error:
            logger.error("启动SQL Shell失败: %s", error)
            print(f"❌ 启动SQL Shell失败: {error}")
            sys.exit(1)
