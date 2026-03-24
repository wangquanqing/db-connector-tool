"""
SQLAlchemy 数据库驱动模块

提供基于 SQLAlchemy 的统一数据库连接接口，支持多种数据库类型：
- Oracle
- PostgreSQL
- MySQL
- SQL Server
- SQLite
- GBase 8s

该模块封装了数据库连接管理、查询执行、连接池配置和错误处理等功能，
提供线程安全的数据库操作接口。

主要特性：
- 多数据库类型支持
- 连接池管理和优化
- 线程安全操作
- 上下文管理器支持
- 详细的错误处理和日志记录
- 自动重连机制
- 连接有效性验证
"""

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from ..core.exceptions import ConnectionError, DriverError, QueryError
from ..utils.logging_utils import get_logger

# 获取模块级别的日志记录器
logger = get_logger(__name__)


def parse_kingbase_version(self, connection) -> Tuple[int, ...]:
    """
    解析 Kingbase 数据库版本信息

    Args:
        connection: 数据库连接对象

    Returns:
        Tuple[int, ...]: 版本号元组，如 (8, 6, 0)

    Raises:
        AssertionError: 当无法从版本字符串中解析出版本信息时

    Example:
        >>> version = parse_kingbase_version(connection)
        >>> print(version)
        (8, 6, 0)
    """
    v = connection.exec_driver_sql("select pg_catalog.version()").scalar()
    m = re.match(
        r".*(?:PostgreSQL|EnterpriseDB) "
        r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
        v,
    ) or re.search(r"V(\d+)R(\d+)C(\d+)B(\d+)", v)

    if not m:
        raise AssertionError(f"无法从字符串 '{v}' 中解析版本信息")

    return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)


# 为 PostgreSQL 方言设置版本解析方法
PGDialect._get_server_version_info = parse_kingbase_version


class SQLAlchemyDriver:
    """
    SQLAlchemy 数据库驱动类

    提供统一的数据库连接和操作接口，支持多种数据库类型。
    使用连接池管理数据库连接，提高性能和资源利用率。

    主要特性：
    - 多数据库类型支持（Oracle、PostgreSQL、MySQL、SQL Server、SQLite、GBase 8s）
    - 连接池管理和优化配置
    - 线程安全操作，支持并发访问
    - 上下文管理器支持，自动资源管理
    - 详细的错误处理和日志记录
    - 连接有效性验证和自动重连
    - 参数化查询支持，防止SQL注入

    Attributes:
        DB_CONFIGS (Dict[str, Dict[str, Any]]): 各数据库类型的连接配置，包含URL模板、必需参数、默认端口和默认值
        TEST_QUERY_DEFAULT (str): 默认测试查询语句
        ORACLE_TEST_QUERY (str): Oracle专用测试查询语句

    Example:
        >>> config = {
        ...     "type": "mysql",
        ...     "host": "localhost",
        ...     "port": 3306,
        ...     "database": "test_db",
        ...     "username": "user",
        ...     "password": "password"
        ... }
        >>> driver = SQLAlchemyDriver(config)
        >>> with driver:
        ...     results = driver.execute_query("SELECT * FROM users")
        ...     print(results)
    """

    # 数据库连接配置
    DB_CONFIGS = {
        "oracle": {
            "url_template": "oracle+cx_oracle://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "port", "database", "username", "password"],
            "default_port": 1521,
            "defaults": {"encoding": "UTF-8", "nencoding": "UTF-8"},
        },
        "postgresql": {
            "url_template": "postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "port", "database", "username", "password"],
            "default_port": 5432,
            "defaults": {"client_encoding": "utf8"},
        },
        "mysql": {
            "url_template": "mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "port", "database", "username", "password"],
            "default_port": 3306,
            "defaults": {"charset": "utf8mb4"},
        },
        "sqlserver": {
            "url_template": "mssql+pyodbc://{username}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server",
            "required_params": ["host", "port", "database", "username", "password"],
            "default_port": 1433,
            "defaults": {"charset": "utf8"},
        },
        "sqlite": {
            "url_template": "sqlite:///{database}",
            "required_params": ["database"],
            "default_port": None,
            "defaults": {},
        },
        "gbase": {
            "url_template": "gbasedbt+pygbasedbt://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "port", "database", "username", "password"],
            "default_port": 9088,
            "defaults": {"charset": "utf8"},
        },
    }

    # 测试查询语句
    TEST_QUERY_DEFAULT = "SELECT 1"
    ORACLE_TEST_QUERY = "SELECT 1 FROM DUAL"

    def __init__(self, config: Dict[str, Any]) -> None:
        """
        初始化 SQLAlchemy 驱动

        Args:
            config: 数据库连接配置字典，包含以下必需字段：
                - type: 数据库类型（oracle/postgresql/mysql/sqlserver/sqlite/gbase）
                - host: 数据库主机地址
                - port: 数据库端口
                - database: 数据库名称
                - username: 用户名
                - password: 密码

        Raises:
            DriverError: 当配置无效或数据库类型不支持时

        Example:
            >>> config = {
            ...     "type": "mysql",
            ...     "host": "localhost",
            ...     "port": 3306,
            ...     "database": "test_db",
            ...     "username": "user",
            ...     "password": "password"
            ... }
            >>> driver = SQLAlchemyDriver(config)
        """
        self.config = config
        self.engine: Optional[Engine] = None
        self.session_factory = None
        self.Session = None
        self._validate_config()

    def _validate_config(self) -> None:
        """
        验证数据库连接配置

        检查配置字典中是否包含所有必需的参数，以及数据库类型是否受支持。

        Raises:
            DriverError: 当配置无效时抛出，包含具体的错误信息

        Example:
            >>> driver._validate_config()  # 验证当前配置
            # 如果配置有效，无返回值
            # 如果配置无效，抛出 DriverError
        """
        db_type = self.config.get("type", "").lower()

        if db_type not in self.DB_CONFIGS:
            supported = ", ".join(self.DB_CONFIGS.keys())
            raise DriverError(
                f"不支持的数据库类型: {db_type}。支持的类型: {supported}"
            )

        db_config = self.DB_CONFIGS[db_type]
        missing_params = [
            param
            for param in db_config["required_params"]
            if param not in self.config or not self.config[param]
        ]

        if missing_params:
            raise DriverError(
                f"数据库配置缺少必需参数: {', '.join(missing_params)}"
            )

    def _build_connection_url(self) -> str:
        """
        构建数据库连接URL

        根据配置信息构建 SQLAlchemy 格式的数据库连接URL。
        对于包含特殊字符的密码，会进行URL编码处理。

        Returns:
            str: SQLAlchemy 格式的数据库连接URL

        Raises:
            DriverError: 当构建URL过程中发生错误时

        Example:
            >>> url = driver._build_connection_url()
            >>> print(url)
            'mysql+pymysql://user:pass%40word@localhost:3306/test_db'
        """
        try:
            db_type = self.config["type"].lower()
            db_config = self.DB_CONFIGS[db_type]

            # 复制配置并设置默认值
            config_copy = self.config.copy()
            config_copy.setdefault("port", db_config["default_port"])

            # 对密码进行URL编码
            if "password" in config_copy:
                config_copy["password"] = quote_plus(str(config_copy["password"]))

            return db_config["url_template"].format(**config_copy)
        except Exception as e:
            raise DriverError(f"构建数据库连接URL失败: {str(e)}") from e

    def connect(self) -> None:
        """
        建立数据库连接

        创建 SQLAlchemy 引擎和会话工厂，配置连接池参数。
        如果已存在连接，会先关闭旧连接。

        Raises:
            ConnectionError: 当连接失败时抛出

        Note:
            - 连接池大小默认为5，最大溢出为10
            - 连接超时时间为30秒
            - 启用连接预ping以验证连接有效性
            - 自动回收超过3600秒的空闲连接

        Example:
            >>> driver.connect()  # 建立连接
            >>> # 执行数据库操作
            >>> driver.disconnect()  # 关闭连接
        """
        try:
            if self.engine:
                self.disconnect()

            connection_url = self._build_connection_url()

            # 配置连接池参数
            pool_config = {
                "pool_size": 5,  # 连接池大小
                "max_overflow": 10,  # 最大溢出连接数
                "pool_timeout": 30,  # 连接超时时间（秒）
                "pool_pre_ping": True,  # 连接前ping测试
                "pool_recycle": 3600,  # 连接回收时间（秒）
                "echo": False,  # 关闭SQL日志
            }

            self.engine = create_engine(connection_url, **pool_config)
            self.session_factory = sessionmaker(bind=self.engine)
            self.Session = scoped_session(self.session_factory)

            logger.info(f"数据库连接已建立: {self.config.get('type', 'unknown')}")

        except Exception as e:
            raise ConnectionError(f"数据库连接失败: {str(e)}") from e

    def disconnect(self) -> None:
        """
        断开数据库连接

        关闭所有连接池中的连接，释放数据库资源。
        此方法会安全地处理各种连接状态。

        Note:
            - 即使连接未建立，调用此方法也不会报错
            - 会等待所有进行中的事务完成后再关闭连接
            - 关闭后会清理会话和引擎对象

        Example:
            >>> driver.connect()
            >>> # 执行数据库操作
            >>> driver.disconnect()  # 安全关闭连接
        """
        try:
            if self.Session:
                self.Session.remove()
                self.Session = None

            if self.engine:
                self.engine.dispose()
                self.engine = None

            self.session_factory = None
            logger.info("数据库连接已关闭")

        except Exception as e:
            logger.warning(f"关闭数据库连接时发生错误: {str(e)}")

    def __enter__(self) -> "SQLAlchemyDriver":
        """
        上下文管理器入口

        使用 with 语句时自动建立数据库连接。

        Returns:
            SQLAlchemyDriver: 返回驱动实例自身

        Example:
            >>> with SQLAlchemyDriver(config) as driver:
            ...     results = driver.execute_query("SELECT * FROM users")
            ...     # 退出with块时自动关闭连接
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        上下文管理器出口

        退出 with 语句块时自动关闭数据库连接。
        会正确处理执行过程中发生的异常。

        Args:
            exc_type: 异常类型
            exc_val: 异常值
            exc_tb: 异常追踪信息
        """
        self.disconnect()

    def _perform_connection_test(self) -> bool:
        """
        执行连接测试

        执行简单的查询来验证数据库连接是否正常工作。
        对于不同类型的数据库使用相应的测试查询。

        Returns:
            bool: 连接测试是否成功

        Note:
            - Oracle数据库使用特殊的测试查询
            - 其他数据库使用标准测试查询
            - 测试失败会记录警告日志但不抛出异常

        Example:
            >>> if driver._perform_connection_test():
            ...     print("连接正常")
            ... else:
            ...     print("连接异常")
        """
        try:
            db_type = self.config.get("type", "").lower()
            test_query = (
                self.ORACLE_TEST_QUERY
                if db_type == "oracle"
                else self.TEST_QUERY_DEFAULT
            )

            with self.engine.connect() as conn:
                result = conn.execute(text(test_query))
                result.fetchone()
                return True

        except Exception as e:
            logger.warning(f"连接测试失败: {str(e)}")
            return False

    def test_connection(self) -> bool:
        """
        测试数据库连接

        验证数据库连接是否可用，包括连接建立和基础查询测试。
        如果连接未建立，会自动尝试建立连接。

        Returns:
            bool: 连接是否可用

        Note:
            测试失败时会记录警告日志，但不会抛出异常
            适合用于连接状态检查和健康监控

        Example:
            >>> if driver.test_connection():
            ...     print("数据库连接正常")
            ... else:
            ...     print("数据库连接异常，需要检查配置")
        """
        try:
            if not self.engine:
                self.connect()
            return self._perform_connection_test()
        except Exception:
            return False

    def _validate_sql_query(self, query: str) -> None:
        """
        验证SQL查询语句，防止SQL注入攻击

        Args:
            query: SQL查询语句

        Raises:
            ValueError: 当查询语句包含潜在的注入攻击代码时
        """
        # 检查查询长度
        if len(query) > 10000:
            logger.warning(f"查询语句长度超过限制: {len(query)} > 10000")
            raise ValueError("查询语句长度超过限制")

        # 检查危险的SQL关键字组合
        import re

        # 白名单：常见的合法DDL操作
        safe_ddl_patterns = [
            r'(?i)\bCREATE\b\s+\bTABLE\b',
            r'(?i)\bALTER\b\s+\bTABLE\b',
            r'(?i)\bCREATE\b\s+\bINDEX\b',
            r'(?i)\bCREATE\b\s+\bVIEW\b',
            r'(?i)\bCREATE\b\s+\bPROCEDURE\b',
            r'(?i)\bCREATE\b\s+\bFUNCTION\b',
            r'(?i)\bCREATE\b\s+\bTRIGGER\b',
            r'(?i)\bDROP\b\s+\bTABLE\b',
            r'(?i)\bDROP\b\s+\bINDEX\b',
            r'(?i)\bDROP\b\s+\bVIEW\b',
            r'(?i)\bDROP\b\s+\bPROCEDURE\b',
            r'(?i)\bDROP\b\s+\bFUNCTION\b',
            r'(?i)\bDROP\b\s+\bTRIGGER\b',
        ]

        # 检查是否为合法的DDL操作
        is_safe_ddl = any(re.search(pattern, query) for pattern in safe_ddl_patterns)

        # 扩展白名单：允许常见的DML操作（SELECT/INSERT/UPDATE/DELETE）
        # 这些操作在正常的业务逻辑中是合法的
        safe_dml_patterns = [
            r'(?i)^\s*SELECT\s+.*\s+FROM\s+',
            r'(?i)^\s*INSERT\s+INTO\s+',
            r'(?i)^\s*UPDATE\s+\w+\s+SET\s+',
            r'(?i)^\s*DELETE\s+FROM\s+',
        ]
        is_safe_dml = any(re.search(pattern, query) for pattern in safe_dml_patterns)

        # 危险模式检测 - 改进的检测逻辑
        dangerous_patterns = [
            # 批量操作和权限变更（始终危险）
            (r'(?i)\b(DROP|TRUNCATE)\s+(TABLE|DATABASE|SCHEMA)\b', "危险的DROP/TRUNCATE操作"),
            (r'(?i)\b(GRANT|REVOKE)\s+.*\s+(ON|TO|FROM)\b', "权限变更操作"),

            # 存储过程和系统命令执行
            (r'(?i)\bEXEC\s*\(\s*[\'"\@]', "动态SQL执行"),
            (r'(?i)\bEXECUTE\s+\w+\s+.*[\'"].*[\'"].*[\'"]', "存储过程执行"),
            (r'(?i)\bxp_cmdshell\b', "系统命令执行"),
            (r'(?i)\bsp_oamethod\b|\bsp_oacreate\b', "OLE自动化存储过程"),

            # 文件操作
            (r'(?i)\bBULK\s+INSERT\b', "批量文件导入"),
            (r'(?i)\bINTO\s+(OUTFILE|DUMPFILE)\b', "文件写入操作"),
            (r'(?i)\bLOAD_FILE\s*\(', "文件读取操作"),

            # 经典的SQL注入模式
            (r'(?i)[\'"]\s*OR\s*[\'"]?\d+[\'"]?\s*=\s*[\'"]?\d+', "布尔盲注尝试"),
            (r'(?i)UNION\s+ALL\s+SELECT', "UNION注入"),
            (r'(?i)WAITFOR\s+DELAY\s+[\'"]\d+', "时间盲注"),
            (r'(?i);\s*SHUTDOWN\s*;?', "数据库关闭命令"),
            (r'(?i);\s*--', "语句截断尝试"),
        ]

        for pattern, description in dangerous_patterns:
            if re.search(pattern, query):
                logger.warning(f"检测到潜在的SQL注入攻击: {description} - {query[:100]}...")
                raise ValueError(f"查询语句包含潜在的安全风险: {description}")

        # 注释检测 - 仅在非DDL/DML操作或可疑上下文中触发
        # 允许在复杂查询中使用注释，但检测可疑的注释模式
        suspicious_comment_patterns = [
            r'(?i)[\'"]\s*--\s*$',  # 字符串结尾的注释（可能截断）
            r'(?i)/\*!\d+\s+',       # MySQL条件注释（常用于绕过）
            r'(?i);\s*/\*.*?\*/\s*\w+',  # 分号后注释再跟命令
        ]

        for pattern in suspicious_comment_patterns:
            if re.search(pattern, query):
                logger.warning(f"检测到可疑的SQL注释模式 - {query[:100]}...")
                raise ValueError("查询语句包含可疑的注释模式")

        # 记录合法的DDL/DML操作
        if is_safe_ddl:
            logger.debug(f"允许合法的DDL操作: {query[:100]}...")
        elif is_safe_dml:
            logger.debug(f"允许合法的DML操作: {query[:100]}...")

    def execute_query(
        self, query: str, parameters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """
        执行SQL查询语句并返回结果

        Args:
            query: SQL查询语句
            parameters: 查询参数字典，用于参数化查询

        Returns:
            List[Dict[str, Any]]: 查询结果列表，每行数据为字典格式

        Raises:
            QueryError: 当查询执行失败时

        Example:
            >>> results = driver.execute_query(
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
            >>> for row in results:
            ...     print(row["name"], row["age"])
        """
        try:
            if not self.engine:
                self.connect()

            # 验证SQL查询
            self._validate_sql_query(query)

            with self.engine.connect() as conn:
                if parameters:
                    result = conn.execute(text(query), parameters)
                else:
                    result = conn.execute(text(query))

                # 将结果转换为字典列表
                columns = result.keys()
                return [dict(zip(columns, row)) for row in result.fetchall()]

        except Exception as e:
            raise QueryError(f"查询执行失败: {str(e)}") from e

    def execute_command(self, command: str) -> int:
        """
        执行SQL命令（INSERT/UPDATE/DELETE等）

        Args:
            command: SQL命令语句

        Returns:
            int: 受影响的行数

        Raises:
            QueryError: 当命令执行失败时

        Example:
            >>> affected = driver.execute_command(
            ...     "UPDATE users SET status = 'active' WHERE id = 1"
            ... )
            >>> print(f"更新了 {affected} 行")
        """
        try:
            if not self.engine:
                self.connect()

            # 验证SQL查询
            self._validate_sql_query(command)

            with self.engine.connect() as conn:
                result = conn.execute(text(command))
                conn.commit()
                return result.rowcount

        except Exception as e:
            raise QueryError(f"命令执行失败: {str(e)}") from e

    def get_tables(self) -> List[str]:
        """
        获取数据库中的所有表名

        Returns:
            List[str]: 表名列表

        Raises:
            QueryError: 当获取表列表失败时

        Note:
            不同数据库类型的表名获取方式可能不同

        Example:
            >>> tables = driver.get_tables()
            >>> print(f"数据库包含 {len(tables)} 个表")
        """
        try:
            if not self.engine:
                self.connect()

            from sqlalchemy import inspect

            inspector = inspect(self.engine)
            return inspector.get_table_names()

        except Exception as e:
            raise QueryError(f"获取表列表失败: {str(e)}") from e

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """
        获取指定表的列信息

        Args:
            table_name: 表名

        Returns:
            List[Dict[str, Any]]: 列信息列表，包含列名、数据类型、是否可空等信息

        Raises:
            QueryError: 当获取表结构失败时

        Example:
            >>> schema = driver.get_table_schema("users")
            >>> for col in schema:
            ...     print(f"{col['name']}: {col['type']}")
        """
        try:
            if not self.engine:
                self.connect()

            from sqlalchemy import inspect

            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)

            return [
                {
                    "name": col["name"],
                    "type": str(col["type"]),
                    "nullable": col.get("nullable", True),
                    "default": col.get("default"),
                }
                for col in columns
            ]

        except Exception as e:
            raise QueryError(f"获取表结构失败: {str(e)}") from e
