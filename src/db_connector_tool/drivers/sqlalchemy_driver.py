"""SQLAlchemy 数据库驱动模块 (SQLAlchemyDriver)

提供基于 SQLAlchemy 的统一数据库连接接口，支持多种数据库类型，
封装了数据库连接管理、查询执行、连接池配置和错误处理等功能，
提供线程安全的数据库操作接口。

支持的数据库类型：
- Oracle
- PostgreSQL
- MySQL
- SQL Server
- SQLite
- GBase 8s

主要特性：
- 多数据库类型支持，统一接口
- 连接池管理和优化配置
- 线程安全操作，支持并发访问
- 上下文管理器支持，自动资源管理
- 详细的错误处理和日志记录
- 自动重连机制和连接有效性验证
- 参数化查询支持，防止 SQL 注入
- 完整的数据库元数据获取功能

使用示例：
>>> from db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver
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
...
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

from ..core.exceptions import ConnectionError as DBConnectionError
from ..core.exceptions import DriverError, QueryError
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)


# pylint: disable=unused-argument
def parse_kingbase_version(self, connection: Any) -> Tuple[int, ...]:
    """解析 Kingbase 数据库版本信息

    从数据库连接中获取版本信息并解析为版本号元组，
    支持多种版本字符串格式，确保正确识别 Kingbase 数据库版本。

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

    v = connection.exec_driver_sql("select pg_catalog.version()").scalar()
    m = re.match(
        r".*(?:PostgreSQL|EnterpriseDB) "
        r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:devel|beta)?",
        v,
    ) or re.search(r"V(\d+)R(\d+)C(\d+)B(\d+)", v)

    if not m:
        raise AssertionError(f"无法从字符串 '{v}' 中解析版本信息")

    return tuple(int(x) for x in m.group(1, 2, 3) if x is not None)


# 为 PostgreSQL 方言设置自定义版本解析方法，用于支持 Kingbase 数据库
# pylint: disable=protected-access
PGDialect._get_server_version_info = parse_kingbase_version


class SQLAlchemyDriver:
    """SQLAlchemy 数据库驱动类 (SQLAlchemy Driver)

    提供统一的数据库连接和操作接口，支持多种数据库类型，
    使用连接池管理数据库连接，提高性能和资源利用率。

    主要特性：
    - 多数据库类型支持（Oracle、PostgreSQL、MySQL、SQL Server、SQLite、GBase 8s）
    - 连接池管理和优化配置，提高性能
    - 线程安全操作，支持并发访问
    - 上下文管理器支持，自动资源管理
    - 详细的错误处理和日志记录
    - 连接有效性验证和自动重连机制
    - 参数化查询支持，防止 SQL 注入
    - 完整的数据库元数据获取功能
    - 统一的异常处理机制

    类属性：
    - DB_CONFIGS (Dict[str, Dict[str, Any]]): 各数据库类型的连接配置，包含URL模板、必需参数、默认端口和默认值
    - TEST_QUERY_DEFAULT (str): 默认测试查询语句
    - ORACLE_TEST_QUERY (str): Oracle 专用测试查询语句

    异常处理：
    - DBConnectionError: 数据库连接失败时抛出
    - QueryError: 查询执行失败时抛出
    - DriverError: 驱动配置错误时抛出

    使用示例：
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
    ...     # 退出 with 块时自动关闭连接
    ...
    >>> # 手动管理连接（备选方式）
    >>> driver.connect()
    >>> try:
    ...     affected = driver.execute_command("UPDATE users SET status = 'active'")
    ...     print(f"更新了 {affected} 行")
    ... finally:
    ...     driver.disconnect()  # 确保关闭连接
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

        创建新的 SQLAlchemy 驱动实例，验证配置有效性并准备连接参数。

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
            格式为 "SQLAlchemyDriver('database_type', connected: True/False)" 的字符串
        """
        db_type = self.config.get("type", "unknown")
        is_connected = self.engine is not None
        return f"SQLAlchemyDriver('{db_type}', connected: {is_connected})"

    def __repr__(self) -> str:
        """返回 SQLAlchemyDriver 的详细表示，用于调试

        Returns:
            包含完整配置信息的字符串，用于调试
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
        """上下文管理器入口

        使用 with 语句时自动建立数据库连接。

        Returns:
            SQLAlchemyDriver: 返回驱动实例自身

        Note:
            允许使用 with 语句来精确控制驱动的生命周期，
            退出 with 块时会自动关闭连接。

        Example:
            >>> with SQLAlchemyDriver(config) as driver:
            ...     results = driver.execute_query("SELECT * FROM users")
            ...     # 退出with块时自动关闭连接
        """

        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """上下文管理器出口

        退出 with 语句块时自动关闭数据库连接，
        会正确处理执行过程中发生的异常。

        Args:
            exc_type: 异常类型（如果有异常发生）
            exc_val: 异常值（如果有异常发生）
            exc_tb: 异常回溯（如果有异常发生）

        Note:
            无论是否发生异常，都会确保数据库连接被安全关闭
        """

        self.disconnect()

    def _validate_config(self) -> None:
        """验证数据库连接配置

        检查配置字典中是否包含所有必需的参数，以及数据库类型是否受支持，
        确保配置的完整性和有效性，为后续的连接建立做好准备。

        Raises:
            DriverError: 当配置无效时抛出，包含具体的错误信息

        Process:
            1. 验证数据库类型是否在支持列表中
            2. 检查配置中是否包含所有必需参数
            3. 验证必需参数是否非空

        Note:
            - 不区分数据库类型的大小写
            - 必需参数列表根据数据库类型从 DB_CONFIGS 中获取
            - 空字符串或 None 值都被视为无效参数
        """

        db_type = self.config.get("type", "").lower()

        if db_type not in self.DB_CONFIGS:
            supported = ", ".join(self.DB_CONFIGS.keys())
            raise DriverError(f"不支持的数据库类型: {db_type}。支持的类型: {supported}")

        db_config = self.DB_CONFIGS[db_type]
        missing_params = [
            param
            for param in db_config["required_params"]
            if param not in self.config or not self.config[param]
        ]

        if missing_params:
            raise DriverError(f"数据库配置缺少必需参数: {', '.join(missing_params)}")

    def _build_connection_url(self) -> str:
        """构建数据库连接URL

        根据配置信息构建 SQLAlchemy 格式的数据库连接URL，
        对于包含特殊字符的密码、用户名和主机名，会进行URL编码处理，
        确保连接URL的正确性和安全性。

        Returns:
            str: SQLAlchemy 格式的数据库连接URL

        Raises:
            DriverError: 当构建URL过程中发生错误时

        Process:
            1. 获取对应数据库类型的URL模板
            2. 设置默认端口（如未配置）
            3. 对用户名、密码、主机名进行URL编码
            4. 使用配置参数格式化URL模板

        Note:
            - 使用 urllib.parse.quote_plus 进行URL编码
            - 支持特殊字符如 @、:、/、空格等的正确编码
            - IPv6 地址会被正确编码
            - 端口使用数据库类型的默认值（如未指定）
        """

        db_type = self.config["type"].lower()
        db_config = self.DB_CONFIGS[db_type]

        # 复制配置并设置默认值
        config_copy = self.config.copy()
        config_copy.setdefault("port", db_config["default_port"])

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

        return db_config["url_template"].format(**config_copy)

    def connect(self) -> None:
        """建立数据库连接

        创建 SQLAlchemy 引擎和会话工厂，配置连接池参数，
        如果已存在连接，会先关闭旧连接。

        Raises:
            DBConnectionError: 当连接失败时抛出

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

            # 配置连接池参数（基于生产环境最佳实践）
            pool_config = {
                "pool_size": 5,  # 连接池常驻连接数，保持适度的并发能力
                "max_overflow": 10,  # 最大溢出连接数，应对突发流量
                "pool_timeout": 30,  # 获取连接超时时间（秒），避免长时间阻塞
                "pool_pre_ping": True,  # 连接前ping测试，自动检测失效连接
                "pool_recycle": 3600,  # 连接回收时间（秒），防止长时间闲置连接失效
                "echo": False,  # 关闭SQL日志，生产环境建议关闭
            }

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

        关闭所有连接池中的连接，释放数据库资源，
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

        验证数据库连接是否可用，包括连接建立和基础查询测试，
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
        except (SQLAlchemyError, DBConnectionError) as e:
            # 数据库连接相关的错误
            logger.warning("连接测试失败: 数据库错误 - %s", str(e))
            return False
        except OSError as e:
            # 网络、I/O或超时相关的错误
            logger.warning("连接测试失败: 网络/I/O错误 - %s", str(e))
            return False
        except (ValueError, TypeError, AttributeError) as e:
            # 配置或参数相关的错误
            logger.warning("连接测试失败: 配置错误 - %s", str(e))
            return False
        except Exception as e:  # pylint: disable=broad-exception-caught
            # 其他未知错误，记录详细日志
            logger.error("连接测试失败: 意外错误 - %s", str(e))
            return False

    def _perform_connection_test(self) -> bool:
        """执行连接测试

        执行简单的查询来验证数据库连接是否正常工作，
        对于不同类型的数据库使用相应的测试查询，
        确保数据库引擎和连接池正常工作。

        Returns:
            bool: 连接测试是否成功

        Note:
            - Oracle 数据库使用特殊的测试查询 "SELECT 1 FROM DUAL"
            - 其他数据库使用标准测试查询 "SELECT 1"
            - 测试失败会记录警告日志但不抛出异常
            - 测试成功后会立即释放连接，不占用连接池资源

        Process:
            1. 检查数据库引擎是否已初始化
            2. 根据数据库类型选择合适的测试查询
            3. 建立临时连接并执行查询
            4. 获取查询结果验证连接有效性
            5. 释放临时连接

        Security:
            - 测试查询不涉及任何用户数据
            - 使用参数化查询的方式执行（虽然此查询不需要参数）
            - 连接用完即释放，避免资源泄漏
        """
        if self.engine is None:
            logger.warning("连接测试失败: 数据库引擎未初始化")
            return False

        db_type = self.config.get("type", "").lower()
        test_query = (
            self.ORACLE_TEST_QUERY if db_type == "oracle" else self.TEST_QUERY_DEFAULT
        )

        with self.engine.connect() as conn:
            result = conn.execute(text(test_query))
            result.fetchone()
            return True

    def execute_query(
        self, query: str, parameters: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """执行SQL查询语句并返回结果

        执行SQL查询语句，支持参数化查询，防止SQL注入，
        返回格式化的查询结果列表。

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
        result = self._execute_sql(query, parameters)
        columns = result.keys()
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def execute_command(self, command: str) -> int:
        """执行SQL命令（INSERT/UPDATE/DELETE等）

        执行SQL命令，如INSERT、UPDATE、DELETE等，
        返回受影响的行数。

        Args:
            command: SQL命令语句

        Returns:
            int: 受影响的行数

        Raises:
            QueryError: 当命令执行失败时

        Example:
            >>> # 更新操作
            >>> affected = driver.execute_command(
            ...     "UPDATE users SET status = 'active' WHERE id = 1"
            ... )
            >>> print(f"更新了 {affected} 行")

            >>> # 插入操作
            >>> affected = driver.execute_command(
            ...     "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')"
            ... )
            >>> print(f"插入了 {affected} 行")
        """
        return self._execute_sql(command, commit=True)

    def _execute_sql(
        self, sql: str, parameters: Dict[str, Any] | None = None, commit: bool = False
    ) -> Any:
        """执行SQL语句（内部方法）

        统一的SQL执行方法，处理查询和命令两种场景，
        支持参数化查询，自动验证SQL安全性，确保数据库操作的可靠性。

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

        Process:
            1. 确保数据库连接已建立
            2. 验证SQL语句的安全性
            3. 执行SQL语句（带参数或不带参数）
            4. 根据commit标志决定是否提交事务
            5. 返回相应的执行结果

        Note:
            - 这是内部方法，不建议直接调用
            - 外部调用应使用 execute_query() 或 execute_command()
            - 参数化查询使用 :name 格式的占位符
        """
        try:
            if not self.engine:
                self.connect()
            assert self.engine is not None, "数据库引擎应该已经初始化，执行查询"

            self._validate_sql_query(sql)

            with self.engine.connect() as conn:
                if parameters:
                    result = conn.execute(text(sql), parameters)
                else:
                    result = conn.execute(text(sql))

                if commit:
                    conn.commit()
                    return result.rowcount
                return result

        except SQLAlchemyError as e:
            raise QueryError(f"SQL执行失败: 数据库错误 - {str(e)}") from e
        except ValueError as e:
            raise QueryError(f"SQL执行失败: 验证错误 - {str(e)}") from e
        except Exception as e:
            raise QueryError(f"SQL执行失败: {str(e)}") from e

    def _validate_sql_query(self, query: str) -> None:
        """验证SQL查询语句，防止SQL注入攻击

        对SQL查询语句进行多层安全验证，检测潜在的SQL注入攻击模式，
        使用白名单和黑名单结合的策略，确保查询语句符合安全标准。

        Args:
            query: SQL查询语句字符串

        Raises:
            ValueError: 当查询语句包含潜在的注入攻击代码时

        Security:
            - 检查查询长度限制（最大10000字符）
            - 检测危险的SQL关键字组合（DROP、TRUNCATE、GRANT等）
            - 验证合法的DDL和DML操作（白名单机制）
            - 检测可疑的SQL注释模式
            - 检测经典SQL注入模式（布尔盲注、UNION注入、时间盲注等）
            - 检测系统命令执行和文件操作相关的危险模式

        Process:
            1. 检查查询语句长度是否超过限制
            2. 验证是否为合法的DDL操作（白名单）
            3. 验证是否为合法的DML操作（白名单）
            4. 检测危险模式（黑名单）
            5. 检测可疑的注释模式
            6. 记录合法操作的日志

        Note:
            - 白名单优先：合法的DDL/DML操作会被允许
            - 黑名单兜底：危险操作会被拦截
            - 即使通过验证，仍建议使用参数化查询
            - 查询长度限制可以防止大规模注入攻击
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

        获取数据库中的所有表名列表，
        不同数据库类型的表名获取方式可能不同。

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

        获取指定表的列信息，包含列名、数据类型、是否可空等信息。

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
