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
import codecs
import getpass
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
from .core.sql_splitter import SplitStatement, split_sql
from .drivers.sqlalchemy_driver import BASIC_PARAMS
from .core.result_render import ResultRenderMixin
from .utils.logging_utils import get_logger, setup_logging

# 注意：日志在 DBConnectorCLI 实例化时才初始化（见 __init__），
# 避免仅仅 import 本模块就在用户配置目录里创建日志文件。
logger = get_logger(__name__)


class DBConnectorCLI(ResultRenderMixin):
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

        初始化日志系统，数据库管理器将在首次使用时延迟初始化。

        Example:
        >>> cli = DBConnectorCLI()
        """
        setup_logging(app_name="db_connector_tool", level="debug")
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
            # 用保真接口：行按位置存放，重名列（SELECT * JOIN）不会互相覆盖
            result = db_manager.execute_query_result(
                args.connection, args.sql_content
            )
            if args.output:
                self._save_output(result, args.output)
            else:
                self._show_results(result)
            print(f"✅ 总计: {len(result)} 行 ({result.elapsed:.3f} 秒)")
        except (DatabaseError, ConfigError, FileSystemError) as error:
            logger.error("执行SQL失败: %s", error)
            print(f"❌ 执行SQL失败: {error}")
            sys.exit(1)

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
            statements = self._parse_sql_file(
                args.file_path, dialect=self._dialect_of(args.connection)
            )
            if not statements:
                print("ℹ️  SQL文件中没有有效的SQL语句")
                return
            incomplete = [s for s in statements if not s.is_complete]
            if incomplete:
                print(
                    f"⚠️  有 {len(incomplete)} 条语句似乎不完整"
                    f"（字符串/注释或过程体未闭合），首条在第 {incomplete[0].line} 行"
                )
            success = 0
            failed = 0
            for idx, statement in enumerate(statements, 1):
                if not statement.text.strip():
                    continue
                print(f"执行语句 {idx}/{len(statements)}:", end=" ")
                ok = self._run_single_sql(
                    args.connection, statement.text, show_truncated=True
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
        except (DatabaseError, ConfigError, FileSystemError, OSError) as error:
            # OSError 覆盖文件不存在、无读取权限等常见输入错误
            logger.error("执行SQL文件失败: %s", error)
            print(f"❌ 执行SQL文件失败: {error}")
            sys.exit(1)

    def _dialect_of(self, connection_name: str) -> Optional[str]:
        """取连接对应的方言名，供 SQL 切分器选择特征

        取不到配置时返回 ``None``（切分器会退到保守通用档），
        不影响脚本执行。

        Args:
            connection_name: 连接名称

        Returns:
            Optional[str]: 方言名（如 ``gbase8s`` / ``postgresql``）；未知返回 ``None``

        Example:
            >>> cli._dialect_of("db_gb")          # doctest: +SKIP
            'gbase8s'
        """
        try:
            db_manager = self._ensure_db_manager()
            config = db_manager.config_manager.get_config(connection_name)
        except Exception as error:  # pylint: disable=broad-except
            # 配置读不到不应阻断执行：切分器有保守默认档
            logger.debug("获取连接方言失败，使用通用切分档: %s", error)
            return None
        if not isinstance(config, dict):
            return None
        return config.get("type")

    def _parse_sql_file(
        self, file_path: str, dialect: Optional[str] = None
    ) -> List[SplitStatement]:
        """解析SQL文件（自动识别编码 + 方言感知切分）

        编码判定顺序见 :meth:`_detect_encodings`：带 BOM 时以 BOM 为准，
        否则按 UTF-8 → GBK → CP1252 → Latin-1 依次尝试。

        切分交给 :func:`db_connector_tool.core.sql_splitter.split_sql`：
        它理解字符串/注释/引用体与**过程体边界**，因此 GBase SPL、
        MySQL ``BEGIN...END``、SQL Server ``GO`` 等都能正确切分
        （原先用的 sqlparse 会把过程体切碎）。

        Args:
            file_path: SQL文件路径
            dialect: 方言名；``None`` 时用保守通用档

        Returns:
            List[SplitStatement]: 语句列表，失败返回空列表

        Raises:
            FileNotFoundError: 文件不存在
        """
        with open(file_path, "rb") as file_handle:
            raw_bytes = file_handle.read()

        for encoding in self._detect_encodings(raw_bytes):
            try:
                sql_content = raw_bytes.decode(encoding)
            except UnicodeDecodeError as error:
                logger.debug("编码 %s 解析失败: %s", encoding, error)
                continue
            statements = split_sql(sql_content, dialect)
            if statements:
                logger.debug(
                    "成功使用 %s 编码解析SQL文件（方言=%s，%d 条语句）",
                    encoding,
                    dialect or "generic",
                    len(statements),
                )
                return statements
        logger.warning("所有编码尝试均失败，文件: %s", file_path)
        return []

    @staticmethod
    def _detect_encodings(raw_bytes: bytes) -> Tuple[str, ...]:
        """给出 SQL 文件的候选解码顺序

        关键点：**不能盲目尝试 UTF-16**。单字节编码（GBK/Latin-1）的文件
        按 UTF-16 解码往往“成功但整篇乱码”，会把中文 SQL 悄悄解成乱码。
        因此只有存在 BOM 时才按 UTF-16 解码。

        Args:
            raw_bytes: 文件原始字节

        Returns:
            Tuple[str, ...]: 候选编码序列

        Example:
            >>> DBConnectorCLI._detect_encodings(b"SELECT 1")
            ('utf-8', 'gbk', 'cp1252', 'latin-1')
        """
        if raw_bytes.startswith(codecs.BOM_UTF8):
            return ("utf-8-sig",)
        if raw_bytes.startswith((codecs.BOM_UTF16_LE, codecs.BOM_UTF16_BE)):
            return ("utf-16",)
        return ("utf-8", "gbk", "cp1252", "latin-1")

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
                result = db_manager.execute_query_result(conn_name, sql)
                self._show_results(result)
                print(f"✅ 总计: {len(result)} 行 ({result.elapsed:.3f} 秒)")
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
        except (DatabaseError, ConfigError) as error:  # pragma: no cover
            # 外层兜底：仅当「初始化 DB manager」本身失败时走到，
            # 需要真实的环境/配置故障才能复现。
            logger.error("启动SQL Shell失败: %s", error)
            print(f"❌ 启动SQL Shell失败: {error}")
            sys.exit(1)
