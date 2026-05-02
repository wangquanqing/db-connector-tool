"""命令行界面模块 (CLI Core)

提供数据库连接管理和 SQL 执行的命令行接口，支持连接的增删改查、
SQL 查询/命令/文件执行及交互式 SQL Shell，结果自动适配终端宽度显示。

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

import sqlparse

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

logger = setup_logging(app_name="db_connector_tool", level="debug")


class DBConnectorCLI:
    """命令行接口主类 (DB Connector CLI)

    管理数据库连接配置并通过命令行执行 SQL 操作，提供连接增删改查、
    SQL 查询/命令/文件执行和交互式 Shell 功能，支持 JSON/CSV 格式结果导出。

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

        数据库管理器将在首次使用时延迟初始化。

        Example:
        >>> cli = DBConnectorCLI()
        """
        self.db_manager: Optional[DatabaseManager] = None

    def show_version(self, _args: argparse.Namespace) -> None:
        """显示当前模块版本信息

        Args:
            _args: 命令行参数（未使用）
        """
        print(f"DB Connector Tool 版本: {__version__}")
        print("支持的数据类型: Oracle, PostgreSQL, MySQL, SQL Server, SQLite, GBase 8s")
        print("许可证: MIT")
        print("作者: wangquanqing")

    def add_connection(self, args: argparse.Namespace) -> None:
        """添加新的数据库连接配置

        从命令行参数构建配置并持久化，空密码触发交互式输入。

        Args:
            args: 命令行参数，包含 name/type/host/username 等

        Raises:
            SystemExit: 初始化失败或添加失败时退出
        """
        db_manager = self._ensure_db_manager()
        conn_config = self._build_conn_config(args)
        try:
            db_manager.add_connection(args.name, conn_config)
            logger.info("连接 '%s' 添加成功", args.name)
            print(f"✅ 连接 '{args.name}' 添加成功")
            self._print_custom_params(conn_config)
        except (DatabaseError, ConfigError) as error:
            logger.error("添加连接失败: %s", error)
            print(f"❌ 添加连接失败: {error}")
            sys.exit(1)

    def _ensure_db_manager(self) -> DatabaseManager:
        """确保数据库管理器已初始化

        延迟初始化并缓存，后续调用直接返回缓存实例。

        Returns:
            DatabaseManager: 已初始化的实例

        Raises:
            SystemExit: 初始化失败时退出
        """
        if self.db_manager is None:
            try:
                self.db_manager = DatabaseManager()
            except DBConnectorError as error:
                logger.error("初始化数据库管理器失败: %s", error)
                print(f"❌ 初始化数据库管理器失败: {error}")
                sys.exit(1)
        return self.db_manager

    def _build_conn_config(self, args: argparse.Namespace) -> Dict[str, Any]:
        """根据命令行参数构建连接配置字典

        Args:
            args: 命令行参数

        Returns:
            Dict[str, Any]: 完整的连接配置字典
        """
        conn_config: Dict[str, Any] = {}
        for param in BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                conn_config[param] = value
        if hasattr(args, "password") and args.password is not None:
            if args.password == "":
                conn_config["password"] = getpass.getpass("请输入数据库密码: ")
            else:
                conn_config["password"] = args.password
        if hasattr(args, "custom_params") and args.custom_params:
            conn_config.update(self._parse_custom_params(args.custom_params))
        return conn_config

    def _parse_custom_params(self, params: List[str]) -> Dict[str, Any]:
        """解析 key=value 参数列表，自动转换类型

        Args:
            params: 参数字符串列表

        Returns:
            Dict[str, Any]: 类型转换后的键值对字典

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._parse_custom_params(["timeout=30", "ssl=true"])
        {'timeout': 30, 'ssl': True}
        """
        result: Dict[str, Any] = {}
        for param in params:
            if "=" not in param:
                logger.warning("忽略无效的自定义参数格式: %s", param)
                continue
            key, value = param.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                logger.warning("忽略空键名的参数: %s", param)
                continue
            result[key] = self._coerce_type(value)
        return result

    def _coerce_type(self, value: str) -> Union[str, int, float, bool]:
        """智能转换参数值类型

        按优先级：布尔值 > 整数 > 浮点数 > 原始字符串。

        Args:
            value: 原始字符串值

        Returns:
            Union[str, int, float, bool]: 转换后的值

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._coerce_type("true")
        True
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

    def _print_custom_params(self, conn_config: Dict[str, Any]) -> None:
        """打印自定义参数信息

        Args:
            conn_config: 连接配置字典
        """
        custom_keys = [k for k in conn_config.keys() if k not in BASIC_PARAMS]
        if custom_keys:
            print(f"📋 自定义参数: {', '.join(custom_keys)}")

    def remove_connection(self, args: argparse.Namespace) -> None:
        """删除指定的数据库连接配置

        Args:
            args: 命令行参数，包含连接名称 name

        Raises:
            SystemExit: 初始化失败或删除失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            db_manager.remove_connection(args.name)
            logger.info("连接 '%s' 已删除", args.name)
            print(f"✅ 连接 '{args.name}' 已删除")
        except (DatabaseError, ConfigError) as error:
            logger.error("删除连接失败: %s", error)
            print(f"❌ 删除连接失败: {error}")
            sys.exit(1)

    def update_connection(self, args: argparse.Namespace) -> None:
        """更新数据库连接配置

        获取现有配置，与命令行新值合并后保存。

        Args:
            args: 命令行参数，包含 name 及需更新的字段

        Raises:
            SystemExit: 初始化失败或更新失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            existing = db_manager.show_connection(args.name)
            updated = self._build_updated_conn_config(existing, args)
            db_manager.update_connection(args.name, updated)
            logger.info("连接 '%s' 更新成功", args.name)
            print(f"✅ 连接 '{args.name}' 更新成功")
            self._print_custom_params(updated)
        except (DatabaseError, ConfigError) as error:
            logger.error("更新连接失败: %s", error)
            print(f"❌ 更新连接失败: {error}")
            sys.exit(1)

    def _build_updated_conn_config(
        self, existing: Dict[str, Any], args: argparse.Namespace
    ) -> Dict[str, Any]:
        """在现有配置基础上覆盖命令行指定参数

        Args:
            existing: 当前存储的连接配置
            args: 命令行参数（仅非 None 值覆盖）

        Returns:
            Dict[str, Any]: 合并后的完整配置
        """
        updated = existing.copy()
        for param in BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                updated[param] = value
        if hasattr(args, "password") and args.password is not None:
            if args.password == "":
                updated["password"] = getpass.getpass("请输入数据库密码: ")
            else:
                updated["password"] = args.password
        if hasattr(args, "custom_params") and args.custom_params:
            updated.update(self._parse_custom_params(args.custom_params))
        return updated

    def show_connection(self, args: argparse.Namespace) -> None:
        """显示指定连接的详细配置（密码掩码）

        Args:
            args: 命令行参数，包含连接名称 name

        Raises:
            SystemExit: 初始化失败或获取详情失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            conn_config = db_manager.show_connection(args.name)
            safe_config = self._mask_sensitive(conn_config)
            print(f"🔍 连接 '{args.name}' 的配置:")
            self._show_conn_details(safe_config)
        except (DatabaseError, ConfigError) as error:
            logger.error("获取连接详情失败: %s", error)
            print(f"❌ 获取连接详情失败: {error}")
            sys.exit(1)

    def _mask_sensitive(self, conn_config: Dict[str, Any]) -> Dict[str, Any]:
        """隐藏密码等敏感字段

        Args:
            conn_config: 原始配置字典

        Returns:
            Dict[str, Any]: 敏感字段已替换为 *** 的安全副本
        """
        safe = conn_config.copy()
        for field in ("password", "passwd", "pwd"):
            if field in safe:
                safe[field] = "***"
        return safe

    def _show_conn_details(self, conn_config: Dict[str, Any]) -> None:
        """显示连接配置详情（基本参数 + 自定义参数）

        Args:
            conn_config: 已掩码的连接配置字典
        """
        for key in BASIC_PARAMS:
            if key in conn_config:
                print(f"  {key}: {conn_config[key]}")
        custom_keys = [k for k in conn_config.keys() if k not in BASIC_PARAMS]
        if custom_keys:
            print("\n  📋 自定义参数:")
            for key in custom_keys:
                print(f"    {key}: {conn_config[key]}")

    def list_connections(self, _args: argparse.Namespace) -> None:
        """列出所有已配置的数据库连接

        Args:
            _args: 命令行参数（未使用）

        Raises:
            SystemExit: 初始化失败或列出失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            connections = db_manager.list_connections()
            if connections:
                print("📋 已配置的连接:")
                for idx, name in enumerate(connections, 1):
                    print(f"  {idx}. {name}")
            else:
                print("ℹ️  没有配置任何连接")
        except (DatabaseError, ConfigError) as error:
            logger.error("列出连接失败: %s", error)
            print(f"❌ 列出连接失败: {error}")
            sys.exit(1)

    def test_connection(self, args: argparse.Namespace) -> None:
        """测试指定数据库连接的连通性

        Args:
            args: 命令行参数，包含连接名称 name

        Raises:
            SystemExit: 初始化失败或测试失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            if db_manager.test_connection(args.name):
                print(f"✅ 连接 '{args.name}' 测试成功")
            else:
                print(f"❌ 连接 '{args.name}' 测试失败")
                sys.exit(1)
        except (DatabaseError, ConfigError) as error:
            logger.error("连接测试失败: %s", error)
            print(f"❌ 连接测试失败: {error}")
            sys.exit(1)

    def execute_query(self, args: argparse.Namespace) -> None:
        """执行 SQL 查询并显示/导出结果

        支持终端表格显示或 JSON/CSV 文件导出。

        Args:
            args: 命令行参数，包含 connection/sql_content/output

        Raises:
            SystemExit: 初始化失败或SQL执行失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            results, elapsed = db_manager.execute_query(
                args.connection, args.sql_content
            )
            if args.output:
                self._save_output(results, args.output)
            else:
                self._show_results(results)
            print(f"✅ 总计: {len(results)} 行 ({elapsed:.3f} 秒)")
        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行SQL失败: %s", error)
            print(f"❌ 执行SQL失败: {error}")
            sys.exit(1)

    def _save_output(self, results: List[Dict], output_path: str) -> None:
        """将查询结果保存到文件

        根据扩展名自动选择 JSON 或 CSV 格式。

        Args:
            results: 查询结果列表
            output_path: 输出文件路径（.json/.csv）

        Raises:
            SystemExit: 格式不支持或保存失败时退出
        """
        try:
            ext = PathHelper.get_file_extension(output_path, include_dot=False)
            if ext == "json":
                self._save_json(results, output_path)
            elif ext == "csv":
                self._save_csv(results, output_path)
            else:
                raise FileSystemError(f"不支持的输出文件格式: {output_path}")
            print(f"✅ 结果已保存到: {output_path}")
        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("保存结果失败: %s", error)
            print(f"❌ 保存结果失败: {error}")
            sys.exit(1)

    @staticmethod
    def _to_serializable(obj: Any) -> Any:
        """将数据库驱动对象转换为可序列化原生类型

        Args:
            obj: 原始对象（Row/ResultProxy 等）

        Returns:
            Any: 可序列化的 Python 原生类型
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

    def _save_json(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为 JSON（UTF-8，2空格缩进）

        Args:
            results: 查询结果列表
            output_path: 输出文件路径

        Raises:
            SystemExit: 序列化失败时退出
        """
        if not results:
            return
        try:
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(
                    results,
                    fh,
                    indent=2,
                    ensure_ascii=False,
                    default=self._to_serializable,
                )
        except (ValueError, TypeError) as error:
            logger.error("JSON序列化失败: %s", error)
            print(f"❌ JSON保存失败: {error}")
            sys.exit(1)

    def _save_csv(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为 CSV（UTF-8）

        Args:
            results: 查询结果列表
            output_path: 输出文件路径
        """
        if not results:
            return
        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            headers = list(results[0].keys())
            writer = csv.DictWriter(fh, fieldnames=headers)
            writer.writeheader()
            for row in results:
                writer.writerow(
                    {key: self._to_serializable(value) for key, value in row.items()}
                )

    def _show_results(self, results: List[Dict]) -> None:
        """以表格格式在终端显示查询结果

        终端宽度不足时自动选择可容纳的列，提示使用 --output 导出完整数据。

        Args:
            results: 查询结果列表
        """
        if not results:
            print("没有结果")
            return
        headers = list(results[0].keys())
        term_width = self._get_term_width()
        col_widths = self._calc_col_widths(headers, results)
        table_width = sum(col_widths[header] + 3 for header in headers) - 1
        if table_width > term_width:
            display_headers = self._pick_display_cols(headers, term_width, col_widths)
            hidden = len(headers) - len(display_headers)
            if display_headers:
                print(
                    f"📋 显示 {len(display_headers)}/{len(headers)} 列（终端宽度限制）"
                )
                print(f"🔍 隐藏了 {hidden} 列，使用 --output csv/json 查看完整数据")
                print()
                trimmed = [
                    {k: v for k, v in row.items() if k in display_headers}
                    for row in results
                ]
                trimmed_widths = {h: col_widths[h] for h in display_headers}
                self._print_table(display_headers, trimmed, trimmed_widths)
            else:
                print("❌ 终端宽度过小，无法显示任何列")
                print("💡 建议使用 --output csv/json 参数导出数据查看")
        else:
            self._print_table(headers, results, col_widths)

    def _get_term_width(self) -> int:
        """获取终端宽度

        Returns:
            int: 终端字符列数，失败时返回 120
        """
        try:
            return shutil.get_terminal_size().columns
        except OSError:
            return 120

    def _calc_col_widths(
        self, headers: List[str], results: List[Dict]
    ) -> Dict[str, int]:
        """计算每列最大显示宽度（上限50）

        Args:
            headers: 表头列表
            results: 查询结果列表

        Returns:
            Dict[str, int]: 列名到宽度的映射
        """
        max_width = 50
        col_widths: Dict[str, int] = {}
        for header in headers:
            col_widths[header] = self._get_display_width(str(header))
        for row in results:
            for header in headers:
                w = self._get_display_width(str(row.get(header, "")))
                col_widths[header] = max(col_widths[header], w)
        for header in headers:
            col_widths[header] = min(col_widths[header], max_width)
        return col_widths

    def _get_display_width(self, text: str) -> int:
        """计算字符串终端显示宽度（中文2，英文1）

        Args:
            text: 输入字符串

        Returns:
            int: 终端显示宽度
        """
        return sum(self._get_char_width(char) for char in text)

    def _get_char_width(self, char: str) -> int:
        """计算单字符终端显示宽度

        Args:
            char: 单个字符

        Returns:
            int: CJK统一汉字/CJK标点/全角字符返回2，其余返回1
        """
        if (
            "\u4e00" <= char <= "\u9fff"
            or "\u3000" <= char <= "\u303f"
            or "\uff00" <= char <= "\uffef"
        ):
            return 2
        return 1

    def _pick_display_cols(
        self, headers: List[str], term_width: int, col_widths: Dict[str, int]
    ) -> List[str]:
        """按终端宽度从左到右选择可容纳的列

        Args:
            headers: 所有表头
            term_width: 终端可用宽度
            col_widths: 列宽映射

        Returns:
            List[str]: 可容纳的列名列表
        """
        if not headers:
            return []
        selected: List[str] = []
        current = 0
        for header in headers:
            col_total = col_widths[header] + 3
            if current + col_total <= term_width:
                selected.append(header)
                current += col_total
            else:
                break
        return selected

    def _print_table(
        self, headers: List[str], results: List[Dict], col_widths: Dict[str, int]
    ) -> None:
        """打印带边框的格式化数据表格

        Args:
            headers: 表头列表
            results: 查询结果行
            col_widths: 列宽映射
        """
        sep_parts = ["-" * (col_widths[header] + 2) for header in headers]
        separator = "+" + "+".join(sep_parts) + "+"
        print(separator)
        self._print_row(headers, headers, col_widths, is_header=True)
        print(separator)
        for row in results:
            self._print_row(row, headers, col_widths, is_header=False)
        print(separator)

    def _print_row(
        self,
        row_data: List[str] | Dict[str, Any],
        headers: List[str],
        col_widths: Dict[str, int],
        is_header: bool = False,
    ) -> None:
        """打印表格单行（居中，表头不截断）

        Args:
            row_data: 行数据（表头为List，数据行为Dict）
            headers: 当前表头列表
            col_widths: 列宽映射
            is_header: 是否为表头行
        """
        cells: List[str] = []
        for header in headers:
            if is_header:
                cell_text = str(
                    row_data[headers.index(header)]
                    if isinstance(row_data, list)
                    else header
                )
            else:
                cell_text = self._truncate_cell(
                    str(row_data.get(header, "")) if isinstance(row_data, dict) else "",
                    col_widths[header],
                )
            actual = self._get_display_width(cell_text)
            padding = col_widths[header] - actual
            left_pad = padding // 2
            right_pad = padding - left_pad
            cells.append(f"{' ' * left_pad}{cell_text}{' ' * right_pad}")
        print(f"| {' | '.join(cells)} |")

    def _truncate_cell(self, value: str, max_width: int) -> str:
        """智能截断过长单元格（考虑CJK宽度）

        Args:
            value: 原始文本
            max_width: 最大显示宽度

        Returns:
            str: 截断后文本，末尾带 ...

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._truncate_cell("长文本内容", 5)
        '长...'
        """
        if self._get_display_width(value) <= max_width:
            return value
        current_width = 0
        truncated = ""
        for char in value:
            char_width = self._get_char_width(char)
            if current_width + char_width > max_width - 3:
                break
            truncated += char
            current_width += char_width
        return truncated + "..."

    def execute_command(self, args: argparse.Namespace) -> None:
        """执行 SQL 增删改操作

        Args:
            args: 命令行参数，包含 connection/sql_content

        Raises:
            SystemExit: 初始化失败或执行失败时退出
        """
        db_manager = self._ensure_db_manager()
        try:
            affected, elapsed = db_manager.execute_command(
                args.connection, args.sql_content
            )
            print(f"✅ 影响: {affected} 行 ({elapsed:.3f} 秒)")
        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行命令失败: %s", error)
            print(f"❌ 执行命令失败: {error}")
            sys.exit(1)

    def execute_file(self, args: argparse.Namespace) -> None:
        """执行SQL文件

        逐条解析执行，自动识别SELECT/命令类型，统计成功率。

        Args:
            args: 命令行参数，包含 connection/file_path/continue_on_error

        Raises:
            SystemExit: 初始化失败或遇错且不继续时退出
        """
        try:
            statements = self._parse_sql_file(args.file_path)
            if not statements:
                print("ℹ️  SQL文件中没有有效的SQL语句")
                return
            success = 0
            failed = 0
            for idx, statement in enumerate(statements, 1):
                if not statement.strip():
                    continue
                print(f"执行语句 {idx}/{len(statements)}:", end=" ")
                ok = self._run_single_sql(
                    args.connection, statement, show_truncated=True
                )
                if ok:
                    success += 1
                else:
                    failed += 1
                    if not args.continue_on_error:
                        sys.exit(1)
            total_count = success + failed
            rate = (success / total_count * 100) if total_count > 0 else 0
            print(f"\n执行完成: 成功 {success} 条，失败 {failed} 条")
            print(f"成功率: {rate:.1f}%")
        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行SQL文件失败: %s", error)
            print(f"❌ 执行SQL文件失败: {error}")
            sys.exit(1)

    def _parse_sql_file(self, file_path: str) -> List[str]:
        """解析SQL文件（自动尝试多种编码）

        Args:
            file_path: SQL文件路径

        Returns:
            List[str]: 解析后的语句列表，失败返回空列表

        Raises:
            FileNotFoundError: 文件不存在
        """
        encodings = [
            "utf-8",
            "utf-16",
            "utf-16-le",
            "utf-16-be",
            "gbk",
            "latin-1",
            "cp1252",
        ]
        for encoding in encodings:
            try:
                if encoding.startswith("utf-16"):
                    with open(file_path, "rb") as fh:
                        raw_bytes = fh.read()
                    sql_content = raw_bytes.decode(encoding, errors="strict")
                    if sql_content.startswith("\ufeff"):
                        sql_content = sql_content[1:]
                else:
                    with open(file_path, "r", encoding=encoding) as fh:
                        sql_content = fh.read()
                parsed = sqlparse.parse(sql_content)
                statements = [str(stmt).strip() for stmt in parsed if str(stmt).strip()]
                if statements:
                    logger.debug("成功使用 %s 编码解析SQL文件", encoding)
                    return statements
            except UnicodeDecodeError as error:
                logger.debug("编码 %s 解析失败: %s", encoding, error)
                continue
        logger.warning("所有编码尝试均失败，文件: %s", file_path)
        return []

    def _run_single_sql(
        self, conn_name: str, sql: str, show_truncated: bool = False
    ) -> bool:
        """执行单条SQL（SELECT走查询，其余走命令）

        Args:
            conn_name: 数据库连接名称
            sql: SQL语句
            show_truncated: 是否预览截断语句

        Returns:
            bool: 成功返回True
        """
        db_manager = self._ensure_db_manager()
        if show_truncated:
            print(f"执行: {self._shorten_sql(sql)}")
        try:
            if sql.lower().strip().startswith("select"):
                results, elapsed = db_manager.execute_query(conn_name, sql)
                self._show_results(results)
                print(f"✅ 总计: {len(results)} 行 ({elapsed:.3f} 秒)")
            else:
                affected, elapsed = db_manager.execute_command(conn_name, sql)
                print(f"✅ 影响: {affected} 行 ({elapsed:.3f} 秒)")
            return True
        except (DatabaseError, ConfigError) as error:
            logger.error("执行语句失败: %s", error)
            print(f"❌ 执行语句失败: {error}")
            return False

    def _shorten_sql(self, sql: str, max_length: int = 50) -> str:
        """截断SQL用于预览

        Args:
            sql: 原始SQL
            max_length: 最大长度

        Returns:
            str: 截断后文本
        """
        if len(sql) <= max_length:
            return sql
        return sql[:max_length].rstrip() + "..."

    def interactive_shell(self, args: argparse.Namespace) -> None:
        """启动交互式SQL Shell（REPL）

        支持逐条执行、help帮助、exit/quit退出、Ctrl+C中断。

        Args:
            args: 命令行参数，包含 connection

        Raises:
            SystemExit: 初始化失败时退出
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
                    self._run_single_sql(args.connection, sql)
                except KeyboardInterrupt:
                    print("\n👋 再见!")
                    break
                except (DatabaseError, ConfigError) as error:
                    print(f"❌ 执行错误: {error}")
        except (DatabaseError, ConfigError) as error:
            logger.error("启动SQL Shell失败: %s", error)
            print(f"❌ 启动SQL Shell失败: {error}")
            sys.exit(1)
