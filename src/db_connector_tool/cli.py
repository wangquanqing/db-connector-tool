"""数据库连接管理工具命令行界面 (DB Connector CLI)

Example:
>>> db-connector add mysql-dev --type mysql --host localhost --username root
>>> db-connector list
>>> db-connector query mysql-dev "SELECT * FROM users"
>>> db-connector shell mysql-dev
"""

import argparse
import csv
import getpass
import json
import os
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

from .__about__ import __version__
from .core.connections import DatabaseManager
from .core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
    FileSystemError,
)
from .drivers.sqlalchemy_driver import BASIC_PARAMS
from .utils.argparse_utils import create_argument_parser
from .utils.logging_utils import setup_logging
from .utils.sqlparse_utils import read_and_split_sql_file

logger = setup_logging(app_name="db_connector_tool", level="debug")


class DBConnectorCLI:
    """数据库连接管理工具命令行接口主类 (DB Connector CLI)

    Example:
        >>> cli = DBConnectorCLI()
    """

    def __init__(self):
        """初始化DB Connector CLI

        Example:
            >>> cli = DBConnectorCLI()
        """
        self.db_manager: Optional[DatabaseManager] = None

    def show_version(self, _: argparse.Namespace) -> None:
        """显示当前模块版本信息

        Args:
            _: 命令行参数

        Example:
            >>> # 命令行使用
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
            SystemExit: 如果添加连接失败则退出程序

        Example:
            >>> # 命令行使用
            >>> # db-connector add mysql-dev --type mysql --host localhost --username root
        """
        db_manager = self._ensure_db_manager_initialized()
        config = self._build_connection_config(args)

        try:
            db_manager.add_connection(args.name, config)
            logger.info("连接 '%s' 添加成功", args.name)
            print(f"✅ 连接 '{args.name}' 添加成功")
            self._print_custom_params(config)
        except (DatabaseError, ConfigError) as e:
            logger.error("添加连接失败: %s", e)
            print(f"❌ 添加连接失败: {e}")
            sys.exit(1)

    def _ensure_db_manager_initialized(self) -> DatabaseManager:
        """确保数据库管理器已初始化

        Returns:
            DatabaseManager: 已初始化的数据库管理器实例

        Raises:
            SystemExit: 如果初始化失败则退出程序

        Example:
            >>> db_manager = cli._ensure_db_manager_initialized()
        """
        if self.db_manager is None:
            try:
                self.db_manager = DatabaseManager()
            except DBConnectorError as e:
                logger.error("初始化数据库管理器失败: %s", e)
                print(f"❌ 初始化数据库管理器失败: {e}")
                sys.exit(1)
        return self.db_manager

    def _build_connection_config(self, args: argparse.Namespace) -> Dict[str, Any]:
        """构建连接配置字典

        Args:
            args: 命令行参数

        Returns:
            Dict[str, Any]: 完整的连接配置字典

        Example:
            >>> config = cli._build_connection_config(args)
        """
        config = {}

        # 添加基本参数
        for param in BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                config[param] = value

        # 检查密码参数，如果使用了密码选项但密码为空，提示用户输入密码
        if hasattr(args, "password") and args.password is not None:
            if args.password == "":
                # 密码为空字符串，提示用户输入密码
                config["password"] = getpass.getpass("请输入数据库密码: ")
            else:
                config["password"] = args.password

        # 添加自定义参数
        if hasattr(args, "custom_params") and args.custom_params:
            custom_config = self._parse_custom_params(args.custom_params)
            config.update(custom_config)

        return config

    def _parse_custom_params(self, params: List[str]) -> Dict[str, Any]:
        """解析自定义参数列表，支持类型自动转换

        Args:
            params: 参数字符串列表，格式为 key=value

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
                logger.warning("忽略无效的自定义参数格式: %s", param)
                continue

            key, value = param.split("=", 1)
            key = key.strip()
            value = value.strip()

            if not key:
                logger.warning("忽略空键名的参数: %s", param)
                continue

            result[key] = self._convert_value_type(value)

        return result

    def _convert_value_type(self, value: str) -> Union[str, int, float, bool]:
        """智能转换参数值的数据类型

        Args:
            value: 原始字符串值

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
        """打印自定义参数信息

        Args:
            config: 连接配置字典

        Example:
            >>> cli._print_custom_params(config)
        """
        custom_params = [k for k in config.keys() if k not in BASIC_PARAMS]
        if custom_params:
            print(f"📋 自定义参数: {', '.join(custom_params)}")

    def remove_connection(self, args: argparse.Namespace) -> None:
        """删除指定的数据库连接配置

        Args:
            args: 命令行参数，包含要删除的连接名称

        Raises:
            SystemExit: 如果删除连接失败则退出程序

        Example:
            >>> # 命令行使用
            >>> # db-connector remove mysql-dev
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            db_manager.remove_connection(args.name)
            logger.info("连接 '%s' 已删除", args.name)
            print(f"✅ 连接 '{args.name}' 已删除")
        except (DatabaseError, ConfigError) as e:
            logger.error("删除连接失败: %s", e)
            print(f"❌ 删除连接失败: {e}")
            sys.exit(1)

    def update_connection(self, args: argparse.Namespace) -> None:
        """更新数据库连接配置

        Args:
            args: 命令行参数，包含连接名称和更新配置

        Raises:
            SystemExit: 如果更新连接失败则退出程序

        Example:
            >>> # 命令行使用
            >>> # db-connector update mysql-dev --host new_host
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            existing_config = db_manager.show_connection(args.name)
            update_config = self._build_update_config(existing_config, args)

            db_manager.update_connection(args.name, update_config)
            logger.info("连接 '%s' 更新成功", args.name)
            print(f"✅ 连接 '{args.name}' 更新成功")
            self._print_custom_params(update_config)
        except (DatabaseError, ConfigError) as e:
            logger.error("更新连接失败: %s", e)
            print(f"❌ 更新连接失败: {e}")
            sys.exit(1)

    def _build_update_config(
        self, existing_config: Dict[str, Any], args: argparse.Namespace
    ) -> Dict[str, Any]:
        """根据命令行参数构建更新后的配置

        Args:
            existing_config: 现有配置
            args: 命令行参数

        Returns:
            Dict[str, Any]: 更新后的配置

        Example:
            >>> update_config = cli._build_update_config(existing_config, args)
        """
        update_config = existing_config.copy()

        # 更新基本参数
        for param in BASIC_PARAMS:
            value = getattr(args, param, None)
            if value is not None:
                update_config[param] = value

        # 检查密码参数，如果使用了密码选项但密码为空，提示用户输入密码
        if hasattr(args, "password") and args.password is not None:
            if args.password == "":
                # 密码为空字符串，提示用户输入密码
                update_config["password"] = getpass.getpass("请输入数据库密码: ")
            else:
                update_config["password"] = args.password

        # 更新自定义参数
        if hasattr(args, "custom_params") and args.custom_params:
            custom_config = self._parse_custom_params(args.custom_params)
            update_config.update(custom_config)

        return update_config

    def show_connection(self, args: argparse.Namespace) -> None:
        """显示指定连接的详细配置信息

        Args:
            args: 命令行参数，包含要显示详情的连接名称

        Raises:
            SystemExit: 如果获取连接详情失败则退出程序

        Example:
            >>> # 命令行使用
            >>> # db-connector show mysql-dev
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            config = db_manager.show_connection(args.name)
            safe_config = self._sanitize_sensitive_info(config)

            print(f"🔍 连接 '{args.name}' 的配置:")
            self._display_connection_details(safe_config)

        except (DatabaseError, ConfigError) as e:
            logger.error("获取连接详情失败: %s", e)
            print(f"❌ 获取连接详情失败: {e}")
            sys.exit(1)

    def _sanitize_sensitive_info(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """隐藏敏感信息（如密码）

        Args:
            config: 原始配置

        Returns:
            Dict[str, Any]: 安全配置（敏感信息已隐藏）

        Example:
            >>> safe_config = cli._sanitize_sensitive_info(config)
        """
        safe_config = config.copy()
        password_fields = ["password", "passwd", "pwd"]

        for field in password_fields:
            if field in safe_config:
                safe_config[field] = "***"

        return safe_config

    def _display_connection_details(self, config: Dict[str, Any]) -> None:
        """显示连接配置详情

        Args:
            config: 连接配置

        Example:
            >>> cli._display_connection_details(config)
        """
        # 显示基本参数
        for key in BASIC_PARAMS:
            if key in config:
                print(f"  {key}: {config[key]}")

        # 显示自定义参数
        custom_params = [k for k in config.keys() if k not in BASIC_PARAMS]
        if custom_params:
            print("\n  📋 自定义参数:")
            for key in custom_params:
                print(f"    {key}: {config[key]}")

    def list_connections(self, _args: argparse.Namespace) -> None:
        """列出所有已配置的数据库连接

        Args:
            _args: 命令行参数（未使用）

        Raises:
            SystemExit: 如果列出连接失败则退出程序

        Example:
            >>> # 命令行使用
            >>> # db-connector list
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
        except (DatabaseError, ConfigError) as e:
            logger.error("列出连接失败: %s", e)
            print(f"❌ 列出连接失败: {e}")
            sys.exit(1)

    def test_connection(self, args: argparse.Namespace) -> None:
        """测试指定连接的连通性

        Args:
            args: 命令行参数，包含要测试的连接名称

        Raises:
            SystemExit: 如果连接测试失败则退出程序

        Example:
            >>> # 命令行使用
            >>> # db-connector test mysql-dev
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            if db_manager.test_connection(args.name):
                print(f"✅ 连接 '{args.name}' 测试成功")
            else:
                print(f"❌ 连接 '{args.name}' 测试失败")
                sys.exit(1)
        except (DatabaseError, ConfigError) as e:
            logger.error("连接测试失败: %s", e)
            print(f"❌ 连接测试失败: {e}")
            sys.exit(1)

    def execute_query(self, args: argparse.Namespace) -> None:
        """执行SQL查询或SQL文件

        根据传入的是SQL字符串还是文件路径自动判断执行方式

        Args:
            args: 命令行参数，包含连接名称、SQL内容/文件路径和输出选项

        Raises:
            SystemExit: 如果执行失败则退出程序

        Example:
            >>> # 执行SQL查询
            >>> # db-connector sql mysql-dev "SELECT * FROM users"
            >>> # 执行SQL文件
            >>> # db-connector sql mysql-dev script.sql
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            # 判断是SQL字符串还是文件路径
            if os.path.exists(args.sql_content):
                # 执行SQL文件（文件存在）
                statements = read_and_split_sql_file(args.sql_content)
                if not statements:
                    print("ℹ️  SQL文件中没有有效的SQL语句")
                    return

                results, success_count, error_count = self._execute_sql_statements(
                    db_manager, statements, args.connection, args.continue_on_error
                )

                self._print_execution_summary(success_count, error_count)
                response_time = 0  # 文件执行不统计响应时间
            else:
                # 执行SQL查询
                results, response_time = db_manager.execute_query(
                    args.connection, args.sql_content
                )

            # 处理输出
            if args.output:
                self._save_output(results, args.output, args.format)
            else:
                self._display_results(results, response_time, args.format)

        except (DatabaseError, ConfigError, FileSystemError) as e:
            logger.error("执行SQL失败: %s", e)
            print(f"❌ 执行SQL失败: {e}")
            sys.exit(1)

    def _execute_sql_statements(
        self,
        db_manager: DatabaseManager,
        statements: List[str],
        connection_name: str,
        continue_on_error: bool,
    ) -> Tuple[List[Dict], int, int]:
        """执行SQL语句列表

        Args:
            db_manager: 数据库管理器实例
            statements: SQL语句列表
            connection_name: 连接名称
            continue_on_error: 遇到错误时是否继续执行

        Returns:
            Tuple[List[Dict], int, int]: (查询结果列表, 成功执行数, 失败执行数)

        Example:
            >>> results, success, error = cli._execute_sql_statements(
            ...     db_manager, statements, "mysql-dev", True
            ... )
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
            except (DatabaseError, ConfigError) as e:
                error_count += 1
                logger.error("执行语句失败: %s", e)
                print(f"❌ 执行语句失败: {e}")
                if not continue_on_error:
                    sys.exit(1)

        return total_results, success_count, error_count

    def _truncate_sql(self, sql: str, max_length: int = 50) -> str:
        """截断SQL语句用于显示，避免过长的输出

        Args:
            sql: 原始SQL语句
            max_length: 最大显示长度

        Returns:
            str: 截断后的SQL语句

        Example:
            >>> truncated = cli._truncate_sql("SELECT * FROM users WHERE id = 1", 30)
        """
        if len(sql) <= max_length:
            return sql
        truncated = sql[:max_length].rstrip()
        return truncated + "..."

    def _print_execution_summary(self, success_count: int, error_count: int) -> None:
        """打印SQL执行统计信息

        Args:
            success_count: 成功执行的语句数
            error_count: 执行失败的语句数

        Example:
            >>> cli._print_execution_summary(5, 1)
        """
        total = success_count + error_count
        success_rate = (success_count / total * 100) if total > 0 else 0
        print(f"\n执行完成: 成功 {success_count} 条，失败 {error_count} 条")
        print(f"成功率: {success_rate:.1f}%")

    def _save_output(
        self, results: List[Dict], output_path: str, output_format: str
    ) -> None:
        """将查询结果保存到文件

        Args:
            results: 查询结果列表
            output_path: 输出文件路径
            output_format: 输出格式

        Raises:
            SystemExit: 如果保存失败

        Example:
            >>> cli._save_output(results, "output.json", "json")
        """
        try:
            converted_results = self._convert_results_for_saving(results)
            self._save_results_to_file(converted_results, output_path, output_format)
            print(f"✅ 结果已保存到: {output_path}")
        except (DatabaseError, ConfigError, FileSystemError) as e:
            logger.error("保存结果失败: %s", e)
            print(f"❌ 保存结果失败: {e}")
            sys.exit(1)

    def _convert_results_for_saving(self, results: List[Dict]) -> List[Dict]:
        """转换查询结果为适合保存的格式

        Args:
            results: 原始查询结果列表

        Returns:
            List[Dict]: 转换后的结果列表
        """
        converted_results = []
        for row in results:
            converted_row = {}
            for key, value in row.items():
                converted_row[key] = self._convert_single_value(value)
            converted_results.append(converted_row)

        return converted_results

    def _convert_single_value(self, obj: Any) -> Any:
        """转换单个值为适合保存的格式

        Args:
            obj: 需要转换的对象

        Returns:
            Any: 转换后的值
        """
        # 尝试标准字典转换
        dict_result = self._try_dict_conversion(obj)
        if dict_result is not None:
            return dict_result

        # 尝试_asdict方法转换
        asdict_result = self._try_asdict_conversion(obj)
        if asdict_result is not None:
            return asdict_result

        # 尝试__dict__属性转换
        dict_attr_result = self._try_dict_attr_conversion(obj)
        if dict_attr_result is not None:
            return dict_attr_result

        # 尝试可迭代对象转换
        iterable_result = self._try_iterable_conversion(obj)
        if iterable_result is not None:
            return iterable_result

        # 对于无法转换的对象，返回原样
        return obj

    def _try_dict_conversion(self, obj: Any) -> Any | None:
        """尝试将对象转换为字典

        Args:
            obj: 需要转换的对象

        Returns:
            Any | None: 转换后的字典，如果转换失败返回None
        """
        try:
            return dict(obj)
        except (TypeError, ValueError):
            return None

    def _try_asdict_conversion(self, obj: Any) -> Any | None:
        """尝试使用_asdict方法转换对象

        Args:
            obj: 需要转换的对象

        Returns:
            Any | None: 转换后的字典，如果转换失败返回None
        """
        if hasattr(obj, "_asdict") and callable(getattr(obj, "_asdict")):
            try:
                return obj._asdict()
            except (TypeError, ValueError):
                return None
        return None

    def _try_dict_attr_conversion(self, obj: Any) -> Any | None:
        """尝试使用__dict__属性转换对象

        Args:
            obj: 需要转换的对象

        Returns:
            Any | None: 转换后的字典，如果转换失败返回None
        """
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        return None

    def _try_iterable_conversion(self, obj: Any) -> Any | None:
        """尝试将可迭代对象转换为列表

        Args:
            obj: 需要转换的对象

        Returns:
            Any | None: 转换后的列表，如果转换失败返回None
        """
        if hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes)):
            try:
                return list(obj)
            except (TypeError, ValueError):
                return None
        return None

    def _save_results_to_file(
        self, results: List[Dict], output_path: str, output_format: str
    ) -> None:
        """将转换后的结果保存到文件

        Args:
            results: 转换后的结果列表
            output_path: 输出文件路径
            output_format: 输出格式
        """
        if output_format == "json":
            self._save_as_json(results, output_path)
        elif output_format == "csv":
            self._save_as_csv(results, output_path)
        else:  # table格式保存为文本
            self._save_as_table(results, output_path)

    def _save_as_json(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为JSON格式

        Args:
            results: 转换后的结果列表
            output_path: 输出文件路径
        """
        try:
            # 自定义JSON序列化器，处理RowMapping等特殊对象
            def default_serializer(obj):
                # 首先尝试标准字典转换
                try:
                    return dict(obj)
                except (TypeError, ValueError):
                    pass

                # 处理有_asdict方法的对象（如SQLAlchemy Row）
                if hasattr(obj, "_asdict") and callable(getattr(obj, "_asdict")):
                    try:
                        return obj._asdict()
                    except (TypeError, ValueError):
                        pass

                # 处理有__dict__属性的普通对象
                if hasattr(obj, "__dict__"):
                    return obj.__dict__

                # 处理可迭代对象
                try:
                    return list(obj)
                except (TypeError, ValueError):
                    pass

                # 对于无法序列化的对象，返回字符串表示
                return str(obj)

            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(
                    results, f, indent=2, ensure_ascii=False, default=default_serializer
                )
        except (ValueError, TypeError) as e:
            logger.error("JSON序列化失败: %s", e)
            print(f"❌ JSON保存失败: {e}")
            sys.exit(1)

    def _save_as_csv(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为CSV格式

        Args:
            results: 转换后的结果列表
            output_path: 输出文件路径
        """
        if not results:
            return

        with open(output_path, "w", newline="", encoding="utf-8") as f:
            headers = list(results[0].keys())
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            writer.writerows(results)

    def _save_as_table(self, results: List[Dict], output_path: str) -> None:
        """将结果保存为表格格式（文本）

        Args:
            results: 转换后的结果列表
            output_path: 输出文件路径
        """
        with open(output_path, "w", encoding="utf-8") as f:
            # 重定向标准输出到文件
            original_stdout = sys.stdout
            sys.stdout = f
            self._display_table(results)
            sys.stdout = original_stdout

    def _display_results(
        self,
        results: List[Dict],
        response_time: float = 0,
        output_format: str = "table",
    ) -> None:
        """以指定格式显示查询结果

        Args:
            results: 查询结果列表
            response_time: 查询响应时间
            output_format: 显示格式 (table/json/csv)

        Raises:
            ValueError: 如果格式不支持

        Example:
            >>> cli._display_results(results, "json")
        """
        if not results:
            print("没有结果")
            return

        if output_format == "json":
            self._display_json(results)
        elif output_format == "csv":
            self._display_csv(results)
        else:
            self._display_table(results)
        print(f"总计: {len(results)} 行 ({response_time:.3f} 秒)")

    def _display_json(self, results: List[Dict]) -> None:
        """以JSON格式显示查询结果

        Args:
            results: 查询结果列表

        Example:
            >>> cli._display_json(results)
        """
        try:
            # 自定义JSON序列化器，处理RowMapping等特殊对象
            def default_serializer(obj):
                # 首先尝试标准字典转换
                try:
                    return dict(obj)
                except (TypeError, ValueError):
                    pass

                # 处理有_asdict方法的对象（如SQLAlchemy Row）
                if hasattr(obj, "_asdict") and callable(getattr(obj, "_asdict")):
                    try:
                        return obj._asdict()
                    except (TypeError, ValueError):
                        pass

                # 处理有__dict__属性的普通对象
                if hasattr(obj, "__dict__"):
                    return obj.__dict__

                # 处理可迭代对象
                try:
                    return list(obj)
                except (TypeError, ValueError):
                    pass

                # 对于无法序列化的对象，返回字符串表示
                return str(obj)

            print(
                json.dumps(
                    results, indent=2, ensure_ascii=False, default=default_serializer
                )
            )
        except (ValueError, TypeError) as e:
            logger.error("JSON序列化失败: %s", e)
            print(f"❌ JSON序列化失败: {e}")
            sys.exit(1)

    def _display_csv(self, results: List[Dict]) -> None:
        """以CSV格式显示查询结果

        Args:
            results: 查询结果列表

        Example:
            >>> cli._display_csv(results)
        """
        if not results:
            return

        headers = list(results[0].keys())
        writer = csv.DictWriter(sys.stdout, fieldnames=headers)
        writer.writeheader()
        for row in results:
            writer.writerow(row)

    def _display_table(self, results: List[Dict]) -> None:
        """以表格形式显示查询结果（带边框，居中显示，MySQL风格）

        Args:
            results: 查询结果列表

        Example:
            >>> cli._display_table(results)
        """
        if not results:
            return

        headers = list(results[0].keys())
        col_widths = self._calculate_column_widths(headers, results)
        separator = self._create_separator(headers, col_widths)

        # 打印表格
        print(separator)
        self._print_header_row(headers, col_widths)
        print(separator)
        self._print_data_rows(results, headers, col_widths)
        print(separator)

    def _calculate_column_widths(
        self, headers: List[str], results: List[Dict]
    ) -> Dict[str, int]:
        """计算每列的最大显示宽度

        Args:
            headers: 表头列表
            results: 查询结果列表

        Returns:
            Dict[str, int]: 列名到宽度的映射
        """
        col_widths = {}
        max_col_width = 50

        # 计算表头宽度
        for header in headers:
            col_widths[header] = self._get_display_width(str(header))

        # 计算数据行宽度
        for row in results:
            for header in headers:
                value = str(row.get(header, ""))
                value_width = self._get_display_width(value)
                col_widths[header] = max(col_widths[header], value_width)

        # 限制最大列宽
        for header in headers:
            col_widths[header] = min(col_widths[header], max_col_width)

        return col_widths

    def _create_separator(self, headers: List[str], col_widths: Dict[str, int]) -> str:
        """创建表格分隔线

        Args:
            headers: 表头列表
            col_widths: 列宽度映射

        Returns:
            str: 分隔线字符串
        """
        separator_parts = ["-" * (col_widths[header] + 2) for header in headers]
        return "+" + "+".join(separator_parts) + "+"

    def _print_header_row(self, headers: List[str], col_widths: Dict[str, int]) -> None:
        """打印表头行

        Args:
            headers: 表头列表
            col_widths: 列宽度映射
        """
        header_cells = []
        for header in headers:
            header_text = str(header)
            actual_width = self._get_display_width(header_text)
            padding = col_widths[header] - actual_width
            left_padding = padding // 2
            right_padding = padding - left_padding
            header_cells.append(
                f"{' ' * left_padding}{header_text}{' ' * right_padding}"
            )

        header_line = f"| {" | ".join(header_cells)} |"
        print(header_line)

    def _print_data_rows(
        self, results: List[Dict], headers: List[str], col_widths: Dict[str, int]
    ) -> None:
        """打印数据行

        Args:
            results: 查询结果列表
            headers: 表头列表
            col_widths: 列宽度映射
        """
        for row in results:
            row_cells = []
            for header in headers:
                value_text = self._truncate_value(
                    str(row.get(header, "")), col_widths[header]
                )
                actual_width = self._get_display_width(value_text)
                padding = col_widths[header] - actual_width
                left_padding = padding // 2
                right_padding = padding - left_padding
                row_cells.append(
                    f"{' ' * left_padding}{value_text}{' ' * right_padding}"
                )

            row_line = f"| {" | ".join(row_cells)} |"
            print(row_line)

    # 添加新的辅助方法
    def _get_display_width(self, text: str) -> int:
        """计算字符串在终端中的显示宽度

        Args:
            text: 输入字符串

        Returns:
            int: 显示宽度（中文字符和中文标点占2，英文字符占1）
        """
        width = 0
        for char in text:
            # 中文字符和中文标点通常占2个宽度，英文字符占1个
            if (
                "\u4e00" <= char <= "\u9fff"  # 中文字符范围
                or "\u3000" <= char <= "\u303f"  # CJK标点符号
                or "\uff00" <= char <= "\uffef"
            ):  # 全角字符（包括全角标点）
                width += 2
            else:
                width += 1
        return width

    # 修改现有的 _truncate_value 方法
    def _truncate_value(self, value: str, max_length: int) -> str:
        """截断过长的值用于表格显示，考虑中文字符宽度

        Args:
            value: 原始值
            max_length: 最大显示宽度

        Returns:
            str: 截断后的值

        Example:
            >>> truncated = cli._truncate_value("长文本", 5)
        """
        if self._get_display_width(value) <= max_length:
            return value

        # 智能截断，考虑中文字符和中文标点
        current_width = 0
        truncated = ""
        for char in value:
            # 中文字符和中文标点通常占2个宽度，英文字符占1个
            char_width = (
                2
                if (
                    "\u4e00" <= char <= "\u9fff"  # 中文字符范围
                    or "\u3000" <= char <= "\u303f"  # CJK标点符号
                    or "\uff00" <= char <= "\uffef"
                )
                else 1
            )  # 全角字符
            if current_width + char_width > max_length - 3:  # 留3个字符给"..."
                break
            truncated += char
            current_width += char_width

        return truncated + "..."

    def execute_command(self, args: argparse.Namespace) -> None:
        """执行增删改操作（INSERT/UPDATE/DELETE等）

        与execute_sql方法类似，但专门用于非查询操作

        Args:
            args: 命令行参数，包含连接名称、SQL内容/文件路径和执行选项

        Raises:
            SystemExit: 如果执行失败则退出程序

        Example:
            >>> # 执行SQL命令
            >>> # db-connector command mysql-dev "INSERT INTO users VALUES (1, 'John')"
            >>> # 执行SQL文件
            >>> # db-connector command mysql-dev script.sql
        """
        db_manager = self._ensure_db_manager_initialized()

        try:
            # 判断是SQL字符串还是文件路径
            if os.path.exists(args.sql_content):
                # 执行SQL文件
                if not os.path.exists(args.sql_content):
                    logger.error("SQL文件不存在: %s", args.sql_content)
                    print(f"❌ SQL文件不存在: {args.sql_content}")
                    sys.exit(1)

                statements = read_and_split_sql_file(args.sql_content)
                if not statements:
                    print("ℹ️  SQL文件中没有有效的SQL语句")
                    return

                results, success_count, error_count = self._execute_sql_statements(
                    db_manager, statements, args.connection, args.continue_on_error
                )

                self._print_execution_summary(success_count, error_count)

                # 对于增删改操作，显示受影响的行数
                if results:
                    total_affected = sum(
                        r.get("affected_rows", 0) for r in results if r
                    )
                    print(f"✅ 总共影响行数: {total_affected}")
            else:
                # 执行SQL命令
                affected_rows = db_manager.execute_command(
                    args.connection, args.sql_content
                )
                print(f"✅ 影响行数: {affected_rows}")

        except (DatabaseError, ConfigError, FileSystemError) as e:
            logger.error("执行命令失败: %s", e)
            print(f"❌ 执行命令失败: {e}")
            sys.exit(1)

    def interactive_shell(self, args: argparse.Namespace) -> None:
        """启动交互式SQL Shell

        Args:
            args: 命令行参数，包含连接名称

        Raises:
            SystemExit: 如果启动Shell失败

        Example:
            >>> # 命令行使用
            >>> # db-connector shell mysql-dev
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
                    if sql.lower() == "help":
                        self._print_shell_help()
                        continue
                    if not sql:
                        continue

                    # 执行SQL
                    if sql.lower().startswith("select"):
                        results, response_time = db_manager.execute_query(
                            args.connection, sql
                        )
                        self._display_results(results, response_time, "table")
                    else:
                        affected = db_manager.execute_command(args.connection, sql)
                        print(f"影响行数: {affected}")

                except KeyboardInterrupt:
                    print("\n👋 再见!")
                    break
                except (DatabaseError, ConfigError) as e:
                    print(f"❌ 执行错误: {e}")

        except (DatabaseError, ConfigError) as e:
            logger.error("启动SQL Shell失败: %s", e)
            print(f"❌ 启动SQL Shell失败: {e}")
            sys.exit(1)

    def _print_shell_help(self) -> None:
        """打印SQL Shell帮助信息

        Example:
            >>> cli._print_shell_help()
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
        print(help_text)


def main():
    """DB Connector CLI 主入口函数

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

    # 处理版本选项
    if hasattr(args, "version") and args.version:
        cli.show_version(args)
        sys.exit(0)

    if hasattr(args, "func"):
        args.func(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
