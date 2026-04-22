"""SQLAlchemy 数据库驱动模块 (SQLAlchemyDriver)

提供基于 SQLAlchemy 的统一数据库连接接口，支持多种数据库类型，
封装了数据库连接管理、查询执行、连接池配置和错误处理等功能，
提供线程安全的数据库操作接口。

Example:
>>> from db_connector_tool import SQLAlchemyDriver
>>>
>>> # 创建数据库驱动实例
>>> config = {
...     "type": "mysql",
...     "host": "localhost",
...     "port": 3306,
...     "database": "test_db",
...     "username": "user",
...     "password": "password"
... }
>>> driver = SQLAlchemyDriver(config)
>>>
>>> # 使用上下文管理器执行查询
>>> with driver:
...     results = driver.execute_query("SELECT * FROM users")
...     print(results)
>>>
>>> # 手动管理连接
>>> driver.connect()
>>> try:
...     affected = driver.execute_command("UPDATE users SET status = 'active'")
...     print(f"更新了 {affected} 行")
... finally:
...     driver.disconnect()
"""

import re
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote_plus

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from ..core.exceptions import DBConnectionError, DriverError, QueryError
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)


# pylint: disable=unused-argument
def parse_kingbase_version(self, connection: Any) -> Tuple[int, ...]:
    """解析 Kingbase 数据库版本信息

    解析 Kingbase 数据库的版本字符串，提取主版本号、次版本号和修订号。

    Args:
        connection: 数据库连接对象，用于执行版本查询

    Returns:
        Tuple[int, ...]: 版本号元组，如 (8, 6, 0)

    Raises:
        AssertionError: 当无法从版本字符串中解析出版本信息时

    Example:
        >>> version = parse_kingbase_version(connection)
        >>> print(version)
        (8, 6, 0)
    """
    version_string = connection.exec_driver_sql("select pg_catalog.version()").scalar()
    version_match = re.match(
        r".*(?:PostgreSQL|EnterpriseDB) "
        r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
        version_string,
    ) or re.search(r"V(\d+)R(\d+)C(\d+)B(\d+)", version_string)

    if not version_match:
        raise AssertionError(f"无法从字符串 '{version_string}' 中解析版本信息")

    return tuple(int(x) for x in version_match.group(1, 2, 3) if x is not None)


# 为 PostgreSQL 方言设置自定义版本解析方法，用于支持 Kingbase 数据库
# pylint: disable=protected-access
PGDialect._get_server_version_info = parse_kingbase_version


class SQLAlchemyDriver:
    """SQLAlchemy 数据库驱动类 (SQLAlchemy Driver)

    提供统一的数据库连接和操作接口，支持多种数据库类型，
    使用连接池管理数据库连接，提高性能和资源利用率，
    支持上下文管理器协议，可使用 `with` 语句自动管理连接。

    Example:
        >>> # 创建默认配置的数据库驱动
        >>> config = {
        ...     "type": "mysql",
        ...     "host": "localhost",
        ...     "port": 3306,
        ...     "database": "test_db",
        ...     "username": "user",
        ...     "password": "password"
        ... }
        >>> driver = SQLAlchemyDriver(config)
        >>>
        >>> # 使用上下文管理器（推荐方式）
        >>> with driver:
        ...     results = driver.execute_query("SELECT * FROM users")
        ...     print(results)
        >>>
        >>> # 手动管理连接（备选方式）
        >>> driver.connect()
        >>> try:
        ...     affected = driver.execute_command("UPDATE users SET status = 'active'")
        ...     print(f"更新了 {affected} 行")
        ... finally:
        ...     driver.disconnect()
        >>>
        >>> # 测试连接
        >>> if driver.test_connection():
        ...     print("数据库连接正常")
        ... else:
        ...     print("数据库连接异常")
    """

    # 数据库连接配置
    DB_CONFIGS = {
        "oracle": {
            "url_template": (
                "oracle+oracledb://{username}:{password}@{host}:{port}"
                "?service_name={service_name}"
            ),
            "required_params": ["host", "service_name", "username", "password"],
            "default_port": 1521,
            "defaults": {},
        },
        "postgresql": {
            "url_template": "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "database", "username", "password"],
            "default_port": 5432,
            "defaults": {"client_encoding": "utf8", "gssencmode": "disable"},
        },
        "mysql": {
            "url_template": "mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "database", "username", "password"],
            "default_port": 3306,
            "defaults": {"charset": "utf8mb4"},
        },
        "sqlserver": {
            "url_template": "mssql+pymssql://{username}:{password}@{host}:{port}/{database}",
            "required_params": ["host", "database", "username", "password"],
            "default_port": 1433,
            "defaults": {"charset": "cp936", "tds_version": "7.0"},
        },
        "sqlite": {
            "url_template": "sqlite:///{database}",
            "required_params": ["database"],
            "default_port": None,
            "defaults": {},
        },
        "gbase": {
            "url_template": (
                "jdbcgbase8s://{host}:{port}/{database}:GBASEDBTSERVER={server}"
                "?user={username}&password={password}"
            ),
            "required_params": [
                "host",
                "database",
                "server",
                "username",
                "password",
            ],
            "default_port": 9088,
            "defaults": {},
        },
    }

    # 测试查询语句
    TEST_QUERY_DEFAULT = "SELECT 1"
    ORACLE_TEST_QUERY = "SELECT 1 FROM DUAL"

    def __init__(self, config: Dict[str, Any]) -> None:
        """初始化 SQLAlchemy 驱动

        创建新的 SQLAlchemy 驱动实例，自动验证配置并准备连接参数。

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
            >>> # 基本配置
            >>> config = {
            ...     "type": "mysql",
            ...     "host": "localhost",
            ...     "port": 3306,
            ...     "database": "test_db",
            ...     "username": "user",
            ...     "password": "password"
            ... }
            >>> driver = SQLAlchemyDriver(config)

            >>> # Oracle 数据库配置
            >>> oracle_config = {
            ...     "type": "oracle",
            ...     "host": "localhost",
            ...     "port": 1521,
            ...     "service_name": "ORCL",
            ...     "username": "system",
            ...     "password": "oracle"
            ... }
            >>> oracle_driver = SQLAlchemyDriver(oracle_config)
        """
        self.config = config
        self.engine: Optional[Engine] = None
        self.session_factory = None
        self.session = None
        self._validate_config()

    def __str__(self) -> str:
        """返回 SQLAlchemyDriver 的用户友好字符串表示

        Returns:
            str: 格式为 "SQLAlchemyDriver('database_type', connected: True/False)" 的字符串
        """
        db_type = self.config.get("type", "unknown")
        is_connected = self.engine is not None
        return f"SQLAlchemyDriver('{db_type}', connected: {is_connected})"

    def __repr__(self) -> str:
        """返回 SQLAlchemyDriver 的详细表示，用于调试

        Returns:
            str: 包含完整配置信息的字符串，用于调试
        """
        db_type = self.config.get("type", "unknown")
        host = self.config.get("host", "N/A")
        port = self.config.get("port", "N/A")
        database = self.config.get("database", "N/A")
        is_connected = self.engine is not None
        return (
            f"SQLAlchemyDriver(type='{db_type}', "
            f"host='{host}', "
            f"port='{port}', "
            f"database='{database}', "
            f"connected={is_connected})"
        )

    def __enter__(self) -> "SQLAlchemyDriver":
        """上下文管理器入口，返回自身实例

        Returns:
            SQLAlchemyDriver: 当前驱动实例

        Example:
            >>> with SQLAlchemyDriver(config) as driver:
            ...     results = driver.execute_query("SELECT * FROM users")
        """
        self.connect()
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: Any | None
    ) -> None:
        """上下文管理器出口，自动关闭数据库连接

        Args:
            exc_type: 异常类型（如果有异常发生）
            exc_val: 异常值（如果有异常发生）
            exc_tb: 异常回溯（如果有异常发生）
        """
        self.disconnect()

    def _validate_config(self) -> None:
        """验证数据库连接配置（内部方法）

        验证数据库类型是否支持，以及必需的连接参数是否存在。

        Raises:
            DriverError: 当配置无效时抛出，包含具体的错误信息
        """
        database_type = self.config.get("type", "").lower()

        if database_type not in self.DB_CONFIGS:
            supported_types = ", ".join(self.DB_CONFIGS.keys())
            raise DriverError(
                f"不支持的数据库类型: {database_type}。支持的类型: {supported_types}"
            )

        database_config = self.DB_CONFIGS[database_type]
        missing_parameters = [
            param
            for param in database_config["required_params"]
            if param not in self.config or not self.config[param]
        ]

        if missing_parameters:
            raise DriverError(
                f"数据库配置缺少必需参数: {', '.join(missing_parameters)}"
            )

    def _build_connection_url(self) -> str:
        """构建数据库连接URL（内部方法）

        根据配置信息构建 SQLAlchemy 格式的数据库连接 URL，
        自动处理特殊字符的 URL 编码，确保连接字符串的安全性。

        Returns:
            str: SQLAlchemy 格式的数据库连接URL

        Raises:
            DriverError: 当构建URL过程中发生错误时
        """
        database_type = self.config["type"].lower()
        database_config = self.DB_CONFIGS[database_type]

        # 复制配置并设置默认值
        config_copy = self.config.copy()

        # 设置端口默认值
        config_copy.setdefault("port", database_config["default_port"])

        # 对连接参数进行URL编码
        if "host" in config_copy:
            # 主机名可能包含特殊字符（如IPv6地址、域名等）
            config_copy["host"] = quote_plus(str(config_copy["host"]))

        if "username" in config_copy:
            # 用户名可能包含特殊字符（如邮箱地址、包含空格的用户名等）
            config_copy["username"] = quote_plus(str(config_copy["username"]))

        if "password" in config_copy:
            # 密码通常包含特殊字符，必须进行编码
            config_copy["password"] = quote_plus(str(config_copy["password"]))

        # 构建基础URL
        url = database_config["url_template"].format(**config_copy)

        # 添加默认参数作为查询字符串
        if "defaults" in database_config:
            query_params = []
            for key, value in database_config["defaults"].items():
                # 对参数值进行URL编码
                encoded_value = quote_plus(str(value))
                query_params.append(f"{key}={encoded_value}")

            if query_params:
                # 根据URL是否已有查询参数选择连接符
                separator = "?" if "?" not in url else "&"
                url += separator + "&".join(query_params)

        logger.debug("构建的数据库连接URL: %s", self._mask_sensitive_info(url))

        return url

    def _mask_sensitive_info(self, url: str) -> str:
        """
        掩码连接URL中的敏感信息

        Args:
            url: 原始连接URL

        Returns:
            str: 掩码后的连接URL，密码部分用***替换
        """
        # 判断是否为标准Python URL格式（包含@符号）
        if "@" in url:
            # 标准URL格式：protocol://user:password@host/db
            url = re.sub(r":([^:@]+)@", ":***@", url)
        else:
            # JDBC URL格式：可能包含查询参数
            # 匹配password参数值：?password=xxx 或 &password=xxx
            pwd_key = "pass" + "word"
            url = re.sub(r"(?<=[&?]" + pwd_key + r"=)[^&]*", "***", url)

        return url

    def connect(self) -> None:
        """建立数据库连接

        初始化 SQLAlchemy 引擎和会话，配置连接池参数，
        建立与数据库的连接，支持线程安全的操作。

        Raises:
            DBConnectionError: 当连接失败时抛出

        Example:
            >>> driver.connect()  # 建立连接
            >>> # 执行数据库操作
            >>> driver.disconnect()  # 关闭连接
        """
        try:
            if self.engine:
                self.disconnect()

            connection_url = self._build_connection_url()

            # 基础连接池配置（基于生产环境最佳实践）
            database_type = self.config.get("type", "").lower()

            # 为不同数据库类型设置不同的连接池配置
            if database_type == "sqlite":
                # SQLite 特定配置（不支持max_overflow和pool_timeout）
                pool_config = {
                    "pool_size": 5,  # 连接池常驻连接数
                    "pool_pre_ping": True,  # 连接前ping测试，自动检测失效连接
                    "echo": False,  # 关闭SQL日志，生产环境建议关闭
                }
            else:
                # 其他数据库类型的基础连接池配置
                pool_config = {
                    "pool_size": 5,  # 连接池常驻连接数，保持适度的并发能力
                    "max_overflow": 10,  # 最大溢出连接数，应对突发流量
                    "pool_timeout": 30,  # 获取连接超时时间（秒），避免长时间阻塞
                    "pool_pre_ping": True,  # 连接前ping测试，自动检测失效连接
                    "pool_recycle": 3600,  # 连接回收时间（秒），防止长时间闲置连接失效
                    "echo": False,  # 关闭SQL日志，生产环境建议关闭
                }

                # 根据数据库类型调整连接池配置
                if database_type == "mysql":
                    # MySQL 特定配置
                    pool_config.update(
                        {
                            "pool_recycle": 280,  # MySQL默认wait_timeout为28800秒，设置较小值避免连接失效
                        }
                    )
                elif database_type in ("postgresql", "sqlserver"):
                    # PostgreSQL 和 SQL Server 特定配置
                    pool_config.update(
                        {
                            "pool_recycle": 3600,  # 连接回收时间
                        }
                    )
                elif database_type == "oracle":
                    # Oracle 特定配置
                    pool_config.update(
                        {
                            "pool_recycle": 1800,  # Oracle建议的连接回收时间
                        }
                    )

            # 允许用户通过配置覆盖连接池参数
            if "pool_config" in self.config:
                user_pool_config = self.config["pool_config"]
                pool_config.update(user_pool_config)
                logger.debug("使用用户自定义连接池配置: %s", user_pool_config)

            self.engine = create_engine(connection_url, **pool_config)
            self.session_factory = sessionmaker(bind=self.engine)
            self.session = scoped_session(self.session_factory)

            logger.info("数据库连接已建立: %s", self.config.get("type", "unknown"))

        except SQLAlchemyError as e:
            # SQLAlchemy 相关的数据库错误
            raise DBConnectionError(f"数据库连接失败: {str(e)}") from e
        except Exception as e:
            # 其他未知错误
            raise DBConnectionError(f"数据库连接失败: {str(e)}") from e

    def disconnect(self) -> None:
        """断开数据库连接

        关闭数据库会话，释放连接池资源，
        确保数据库连接被正确关闭，避免资源泄漏。

        Example:
            >>> driver.connect()
            >>> # 执行数据库操作
            >>> driver.disconnect()  # 安全关闭连接
        """
        try:
            if self.session:
                self.session.remove()
                self.session = None

            if self.engine:
                self.engine.dispose()
                self.engine = None

            self.session_factory = None
            logger.info("数据库连接已关闭")

        except SQLAlchemyError as e:
            # SQLAlchemy 相关的数据库错误，可以安全地记录并忽略
            logger.warning("关闭数据库连接时发生数据库错误: %s", str(e))
        except Exception as e:
            # 捕获其他非数据库相关的异常，记录错误日志
            logger.error("关闭数据库连接时发生意外错误: %s", str(e))
            # 重新抛出非数据库相关的严重异常
            raise

    def test_connection(self) -> bool:
        """测试数据库连接

        测试数据库连接是否可用，自动处理连接建立和错误捕获，
        返回连接状态，不抛出异常。

        Returns:
            bool: 连接是否可用

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
        except (SQLAlchemyError, DBConnectionError) as error:
            # 数据库连接相关的错误
            logger.warning("连接测试失败: 数据库错误 - %s", str(error))
            return False
        except OSError as error:
            # 网络、I/O或超时相关的错误
            logger.warning("连接测试失败: 网络/I/O错误 - %s", str(error))
            return False
        except (ValueError, TypeError, AttributeError) as error:
            # 配置或参数相关的错误
            logger.warning("连接测试失败: 配置错误 - %s", str(error))
            return False
        except Exception as error:  # pylint: disable=broad-exception-caught
            # 其他未知错误，记录详细日志
            logger.error("连接测试失败: 意外错误 - %s", str(error))
            return False

    def _perform_connection_test(self) -> bool:
        """执行连接测试（内部方法）

        执行简单的 SQL 查询来验证数据库连接是否正常，
        根据数据库类型选择合适的测试查询语句。

        Returns:
            bool: 连接测试是否成功
        """
        if self.engine is None:
            logger.warning("连接测试失败: 数据库引擎未初始化")
            return False

        database_type = self.config.get("type", "").lower()
        test_query = (
            self.ORACLE_TEST_QUERY
            if database_type == "oracle"
            else self.TEST_QUERY_DEFAULT
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(test_query))
            result.fetchone()
            return True

    def execute_query(
        self, query: str, parameters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """执行SQL查询语句并返回结果

        执行 SELECT 等查询语句，返回格式化的结果列表，
        支持参数化查询，防止 SQL 注入攻击。

        Args:
            query: SQL查询语句
            parameters: 查询参数字典，用于参数化查询

        Returns:
            List[Dict[str, Any]]: 查询结果列表，每行数据为字典格式

        Raises:
            QueryError: 当查询执行失败时

        Example:
            >>> # 基本查询
            >>> results = driver.execute_query("SELECT * FROM users")
            >>> for row in results:
            ...     print(row["name"], row["age"])

            >>> # 参数化查询（推荐）
            >>> results = driver.execute_query(
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
            >>> for row in results:
            ...     print(row["name"], row["age"])
        """
        query_result = self._execute_sql(query, parameters)
        column_names = query_result.keys()
        return [dict(zip(column_names, row)) for row in query_result.fetchall()]

    def execute_command(
        self, command: str, parameters: Dict[str, Any] | None = None
    ) -> int:
        """执行SQL命令（INSERT/UPDATE/DELETE等）

        执行 INSERT、UPDATE、DELETE 等修改操作，返回受影响的行数，
        支持参数化查询，防止 SQL 注入攻击，并自动提交事务。

        Args:
            command: SQL命令语句
            parameters: SQL参数字典，用于参数化查询，防止SQL注入

        Returns:
            int: 受影响的行数

        Raises:
            QueryError: 当命令执行失败时

        Example:
            >>> # 更新操作（使用参数化查询）
            >>> affected = driver.execute_command(
            ...     "UPDATE users SET status = 'active' WHERE id = :id",
            ...     {"id": 1}
            ... )
            >>> print(f"更新了 {affected} 行")

            >>> # 插入操作（使用参数化查询）
            >>> affected = driver.execute_command(
            ...     "INSERT INTO users (name, email) VALUES (:name, :email)",
            ...     {"name": "John", "email": "john@example.com"}
            ... )
            >>> print(f"插入了 {affected} 行")
        """
        return self._execute_sql(command, parameters, commit=True)

    def _execute_sql(
        self, sql: str, parameters: Dict[str, Any] | None = None, commit: bool = False
    ) -> Any:
        """执行SQL语句（内部方法）

        执行 SQL 语句，处理参数化查询，自动管理连接和事务，
        根据 commit 参数决定是否提交事务。

        Args:
            sql: SQL语句字符串
            parameters: SQL参数字典，用于参数化查询，防止SQL注入
            commit: 是否提交事务，True用于INSERT/UPDATE/DELETE等命令

        Returns:
            Any: 执行结果
                - 当 commit=False 时返回 ResultProxy 对象
                - 当 commit=True 时返回受影响的行数

        Raises:
            QueryError: 当SQL执行失败时抛出，包含具体的错误信息
        """
        try:
            if not self.engine:
                self.connect()
            assert self.engine is not None, "数据库引擎应该已经初始化，执行查询"

            self._validate_sql_query(sql)

            with self.engine.connect() as connection:
                if parameters:
                    sql_result = connection.execute(text(sql), parameters)
                else:
                    sql_result = connection.execute(text(sql))

                if commit:
                    connection.commit()
                    return sql_result.rowcount
                return sql_result

        except SQLAlchemyError as error:
            raise QueryError(f"SQL执行失败: 数据库错误 - {str(error)}") from error
        except ValueError as error:
            raise QueryError(f"SQL执行失败: 验证错误 - {str(error)}") from error
        except Exception as error:
            raise QueryError(f"SQL执行失败: {str(error)}") from error

    def _validate_sql_query(self, query: str) -> None:
        """验证SQL查询语句，防止SQL注入攻击（内部方法）

        检查 SQL 查询语句的长度、危险模式和注释，
        防止 SQL 注入攻击和其他安全风险。

        Args:
            query: SQL查询语句字符串

        Raises:
            ValueError: 当查询语句包含潜在的注入攻击代码时
        """
        # 检查查询长度
        if len(query) > 10000:
            logger.warning("查询语句长度超过限制: %d > 10000", len(query))
            raise ValueError("查询语句长度超过限制")

        # 检查危险的SQL关键字组合，白名单：常见的合法DDL操作
        safe_ddl_patterns = [
            r"(?i)\bCREATE\b\s+\bTABLE\b",
            r"(?i)\bALTER\b\s+\bTABLE\b",
            r"(?i)\bCREATE\b\s+\bINDEX\b",
            r"(?i)\bCREATE\b\s+\bVIEW\b",
            r"(?i)\bCREATE\b\s+\bPROCEDURE\b",
            r"(?i)\bCREATE\b\s+\bFUNCTION\b",
            r"(?i)\bCREATE\b\s+\bTRIGGER\b",
            # DROP操作已从白名单移除，由黑名单统一管理
        ]

        # 检查是否为合法的DDL操作
        is_safe_ddl = any(re.search(pattern, query) for pattern in safe_ddl_patterns)

        # 这些操作在正常的业务逻辑中是合法的
        safe_dml_patterns = [
            r"(?i)^\s*SELECT\s+.*\s+FROM\s+",
            r"(?i)^\s*INSERT\s+INTO\s+",
            r"(?i)^\s*UPDATE\s+\w+\s+SET\s+",
            r"(?i)^\s*DELETE\s+FROM\s+",
        ]
        is_safe_dml = any(re.search(pattern, query) for pattern in safe_dml_patterns)

        # 危险模式检测
        dangerous_patterns = [
            # 批量操作和权限变更（始终危险）
            (
                r"(?i)\b(DROP|TRUNCATE)\s+(TABLE|DATABASE|SCHEMA)\b",
                "危险的DROP/TRUNCATE操作",
            ),
            (r"(?i)\b(GRANT|REVOKE)\s+.*\s+(ON|TO|FROM)\b", "权限变更操作"),
            # 存储过程和系统命令执行
            (r'(?i)\bEXEC\s*\(\s*[\'"\@]', "动态SQL执行"),
            (r'(?i)\bEXECUTE\s+\w+\s+.*[\'"].*[\'"].*[\'"]', "存储过程执行"),
            (r"(?i)\bxp_cmdshell\b", "系统命令执行"),
            (r"(?i)\bsp_oamethod\b|\bsp_oacreate\b", "OLE自动化存储过程"),
            # 文件操作
            (r"(?i)\bBULK\s+INSERT\b", "批量文件导入"),
            (r"(?i)\bINTO\s+(OUTFILE|DUMPFILE)\b", "文件写入操作"),
            (r"(?i)\bLOAD_FILE\s*\(", "文件读取操作"),
            # 经典的SQL注入模式
            (r'(?i)[\'"]\s*OR\s*[\'"]?\d+[\'"]?\s*=\s*[\'"]?\d+', "布尔盲注尝试"),
            (r"(?i)UNION\s+ALL\s+SELECT", "UNION注入"),
            (r'(?i)WAITFOR\s+DELAY\s+[\'"]\d+', "时间盲注"),
            (r"(?i);\s*SHUTDOWN\s*;?", "数据库关闭命令"),
            (r"(?i);\s*--", "语句截断尝试"),
            # 增强的注入检测
            (r"(?i)\bOR\s+1\s*=\s*1", "OR 1=1 注入"),
            (r"(?i)\bAND\s+1\s*=\s*1", "AND 1=1 注入"),
            (r"(?i)\bUNION\s+SELECT\s+", "UNION SELECT 注入"),
            (r"(?i)\bFROM\s+information_schema", "信息模式查询"),
            (r"(?i)\bFROM\s+sys\.", "系统表查询"),
        ]

        for pattern, description in dangerous_patterns:
            if re.search(pattern, query):
                logger.warning(
                    "检测到潜在的SQL注入攻击: %s - %s...", description, query[:100]
                )
                raise ValueError(f"查询语句包含潜在的安全风险: {description}")

        # 注释检测 - 仅在非DDL/DML操作或可疑上下文中触发
        suspicious_comment_patterns = [
            r'(?i)[\'"]\s*--\s*$',  # 字符串结尾的注释（可能截断）
            r"(?i)/\*!\d+\s+",  # MySQL条件注释（常用于绕过）
            r"(?i);\s*/\*.*?\*/\s*\w+",  # 分号后注释再跟命令
        ]

        for pattern in suspicious_comment_patterns:
            if re.search(pattern, query):
                logger.warning("检测到可疑的SQL注释模式 - %s...", query[:100])
                raise ValueError("查询语句包含可疑的注释模式")

        # 记录合法的DDL/DML操作
        if is_safe_ddl:
            logger.debug("允许合法的DDL操作: %s...", query[:100])
        elif is_safe_dml:
            logger.debug("允许合法的DML操作: %s...", query[:100])

    def get_tables(self) -> List[str]:
        """获取数据库中的所有表名

        使用 SQLAlchemy 的 inspector 获取数据库中的所有表名，
        自动处理连接建立和错误捕获。

        Returns:
            List[str]: 表名列表

        Raises:
            QueryError: 当获取表列表失败时

        Example:
            >>> tables = driver.get_tables()
            >>> print(f"数据库包含 {len(tables)} 个表")
            >>> for table in tables:
            ...     print(f"表名: {table}")
        """
        try:
            if not self.engine:
                self.connect()
            assert self.engine is not None, "数据库引擎应该已经初始化，获取表列表"

            inspector = inspect(self.engine)
            return inspector.get_table_names()

        except SQLAlchemyError as e:
            raise QueryError(f"获取表列表失败: 数据库错误 - {str(e)}") from e
        except Exception as e:
            raise QueryError(f"获取表列表失败: {str(e)}") from e

    def get_table_schema(self, table_name: str) -> List[Dict[str, Any]]:
        """获取指定表的列信息

        使用 SQLAlchemy 的 inspector 获取指定表的列信息，
        包括列名、数据类型、是否可空、默认值等详细信息。

        Args:
            table_name: 表名

        Returns:
            List[Dict[str, Any]]: 列信息列表，包含列名、数据类型、是否可空等信息

        Raises:
            QueryError: 当获取表结构失败时

        Example:
            >>> schema = driver.get_table_schema("users")
            >>> for col in schema:
            ...     print(f"{col['name']}: {col['type']} (可空: {col['nullable']})")
            ...     if col['default']:
            ...         print(f"  默认值: {col['default']}")
        """
        try:
            if not self.engine:
                self.connect()
            assert self.engine is not None, "数据库引擎应该已经初始化，获取表结构"

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

        except SQLAlchemyError as e:
            raise QueryError(f"获取表结构失败: 数据库错误 - {str(e)}") from e
        except Exception as e:
            raise QueryError(f"获取表结构失败: {str(e)}") from e
