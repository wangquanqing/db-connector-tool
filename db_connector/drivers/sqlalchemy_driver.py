"""
SQLAlchemy 数据库驱动模块

提供基于 SQLAlchemy 的统一数据库连接接口，支持多种数据库类型：
- Oracle
- PostgreSQL
- MySQL
- SQL Server
- SQLite

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

from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from ..core.exceptions import ConnectionError, DriverError, QueryError
from ..utils.logging_utils import get_logger

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class SQLAlchemyDriver:
    """
    SQLAlchemy 数据库驱动类

    提供统一的数据库连接和操作接口，支持多种数据库类型。
    使用连接池管理数据库连接，提高性能和资源利用率。

    主要特性：
    - 多数据库类型支持（Oracle、PostgreSQL、MySQL、SQL Server、SQLite）
    - 连接池管理和优化配置
    - 线程安全操作，支持并发访问
    - 上下文管理器支持，自动资源管理
    - 详细的错误处理和日志记录
    - 连接有效性验证和自动重连
    - 参数化查询支持，防止SQL注入

    Attributes:
        DRIVER_MAP (Dict[str, str]): 数据库类型到驱动名称的映射
        URL_TEMPLATES (Dict[str, str]): 各数据库类型的连接URL模板
        TEST_QUERY_DEFAULT (str): 默认测试查询语句
        ORACLE_TEST_QUERY (str): Oracle专用测试查询语句

    Example:
        >>> config = {
        ...     "type": "mysql",
        ...     "username": "user",
        ...     "password": "pass",
        ...     "host": "localhost",
        ...     "database": "test_db",
        ...     "pool_config": {"pool_size": 5}
        ... }
        >>> driver = SQLAlchemyDriver(config)
        >>> with driver:
        ...     results = driver.execute_query("SELECT * FROM users")
        ...     print(results)
    """

    # 数据库驱动映射配置
    DRIVER_MAP: Dict[str, str] = {
        "oracle": "oracledb",
        "postgresql": "psycopg",
        "mysql": "pymysql",
        "mssql": "pymssql",
        "sqlite": "sqlite3",
    }

    # 连接URL模板配置
    URL_TEMPLATES: Dict[str, str] = {
        "oracle": "oracle+oracledb://{username}:{password}@{host}:{port}/?service_name={service_name}",
        "postgresql": "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}?gssencmode={gssencmode}",
        "mysql": "mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
        "mssql": "mssql+pymssql://{username}:{password}@{host}:{port}/{database}?charset={charset}&tds_version={tds_version}",
        "sqlite": "sqlite:///{database}",
    }

    # 测试查询常量定义
    TEST_QUERY_DEFAULT: str = "SELECT 1"
    ORACLE_TEST_QUERY: str = "SELECT 1 FROM DUAL"

    def __init__(self, connection_config: Dict[str, Any]) -> None:
        """
        初始化数据库驱动实例

        Args:
            connection_config: 数据库连接配置字典，必需包含以下字段：
                - type (str): 数据库类型 (oracle/postgresql/mysql/mssql/sqlite)
                - username (str): 用户名
                - password (str): 密码
                - host (str): 主机地址
                - port (int, optional): 端口号，如未提供将使用默认端口
                - database (str): 数据库名
                - pool_config (Dict[str, Any], optional): 连接池配置
                - echo_sql (bool, optional): 是否输出SQL日志，默认False

        Raises:
            ValueError: 当连接配置为空、非字典类型或缺少必需参数时
            DriverError: 当数据库类型不支持时

        Notes:
            - 配置验证在初始化时进行，确保配置有效性
            - 支持配置参数的灵活性和扩展性
            - 自动验证数据库类型支持

        Example:
            >>> config = {
            ...     "type": "mysql",
            ...     "username": "user",
            ...     "password": "pass",
            ...     "host": "localhost",
            ...     "database": "test_db",
            ...     "pool_config": {"pool_size": 5}
            ... }
            >>> driver = SQLAlchemyDriver(config)
        """
        if not connection_config:
            raise ValueError("连接配置不能为空")

        if not isinstance(connection_config, dict):
            raise ValueError("连接配置必须为字典类型")

        self.connection_config = connection_config
        self.engine: Optional[Engine] = None
        self.session_factory: Optional[scoped_session] = None
        self._connected: bool = False

        # 验证数据库类型支持
        db_type = self.connection_config.get("type", "").lower()
        if db_type not in self.URL_TEMPLATES:
            supported_types = ", ".join(self.URL_TEMPLATES.keys())
            raise DriverError(
                f"不支持的数据库类型: {db_type}，支持的类型: {supported_types}"
            )

        logger.debug(f"SQLAlchemy驱动实例初始化成功，数据库类型: {db_type}")

    def connect(self) -> None:
        """
        建立数据库连接并验证连接有效性

        该方法执行完整的数据库连接流程，包括：
        1. 检查当前连接状态，避免重复连接
        2. 构建数据库连接URL
        3. 配置连接池参数和引擎选项
        4. 创建SQLAlchemy引擎和会话工厂
        5. 执行实际连接测试验证连接有效性
        6. 更新连接状态并记录日志

        Raises:
            ConnectionError: 当连接建立或验证失败时抛出

        Notes:
            - 支持连接池配置，包括连接预检查、回收时间等
            - 自动适配不同数据库类型的特定配置
            - 严格区分引擎创建成功和实际连接成功
            - 连接失败时会自动清理已创建的引擎资源

        Example:
            >>> driver = SQLAlchemyDriver(connection_config)
            >>> driver.connect()  # 建立数据库连接
            >>> print(driver.is_connected)  # 检查连接状态
            True
        """
        try:
            # 检查连接状态，避免重复连接
            if self._connected:
                logger.warning("数据库连接已存在，无需重复连接")
                return

            # 构建数据库连接URL
            connection_url = self._build_connection_url()

            # 配置连接池参数
            pool_config = self._get_pool_config()

            # 准备引擎配置参数
            engine_kwargs: Dict[str, Any] = {
                "echo": self.connection_config.get("echo_sql", False),
                **pool_config,
            }

            # 添加数据库特定的引擎配置
            db_type = self.connection_config.get("type", "").lower()
            db_specific_config = self._get_engine_config(db_type)
            engine_kwargs.update(db_specific_config)

            # 创建SQLAlchemy引擎和会话工厂
            self.engine = create_engine(connection_url, **engine_kwargs)
            self.session_factory = scoped_session(sessionmaker(bind=self.engine))

            logger.info(
                f"数据库引擎创建成功: {self.connection_config.get('type')} "
                f"({self.connection_config.get('host') or self.connection_config.get('database')})"
            )

            # 执行实际连接测试验证连接有效性
            self._validate_connection()

        except SQLAlchemyError as e:
            # 连接过程中发生异常，清理资源并抛出错误
            self._cleanup_on_connection_failure()
            error_msg = f"数据库连接建立失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ConnectionError(error_msg)

    def _build_connection_url(self) -> str:
        """
        构建数据库连接URL

        根据配置参数构建完整的数据库连接URL，包含必要的编码和参数处理。

        Returns:
            str: 完整的数据库连接URL字符串

        Raises:
            DriverError: 当缺少必需参数或数据库类型不支持时

        Notes:
            - 对用户名、密码和主机名中的特殊字符进行URL编码
            - 自动设置默认端口
            - 支持数据库特定的查询参数
            - SQLite数据库特殊处理，支持内存数据库
            - Oracle数据库强制使用Easy Connect格式，避免TNS别名查找

        Example:
            >>> url = driver._build_connection_url()
            >>> print(url)
            mysql+pymysql://user:pass@localhost:3306/test_db
        """
        db_type = self.connection_config.get("type", "").lower()
        config = self.connection_config.copy()

        # SQLite数据库特殊处理
        if db_type == "sqlite":
            if "database" not in config:
                config["database"] = ":memory:"  # 内存数据库
            return self.URL_TEMPLATES[db_type].format(**config)

        # 验证必需参数
        required_fields = ["username", "password", "host"]
        if db_type == "oracle":
            required_fields.append("service_name")
        else:
            required_fields.append("database")
        if db_type == "postgresql":
            required_fields.append("gssencmode")
        elif db_type == "mssql":
            required_fields.append("charset")
            required_fields.append("tds_version")
        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            raise DriverError(f"缺少必需的连接参数: {', '.join(missing_fields)}")

        # 设置默认端口
        if "port" not in config:
            config["port"] = self._get_default_port(db_type)

        # URL编码敏感信息
        config["username"] = quote_plus(config["username"])
        config["password"] = quote_plus(config["password"])

        # 处理主机名中的特殊字符
        if "@" in config["host"] or ":" in config["host"]:
            config["host"] = quote_plus(config["host"])

        # 构建基础URL
        base_url = self.URL_TEMPLATES[db_type].format(**config)

        logger.debug(f"构建的连接URL: {self._mask_sensitive_info(base_url)}")
        return base_url

    def _get_default_port(self, db_type: str) -> str:
        """
        获取数据库默认端口

        Args:
            db_type: 数据库类型

        Returns:
            str: 默认端口号

        Notes:
            - 如果未配置端口，使用数据库的默认端口
            - 返回空字符串表示该数据库类型无默认端口
        """
        default_ports: Dict[str, str] = {
            "oracle": "1521",
            "postgresql": "5432",
            "mysql": "3306",
            "mssql": "1433",
        }
        return default_ports.get(db_type, "")

    def _mask_sensitive_info(self, url: str) -> str:
        """
        掩码连接URL中的敏感信息

        Args:
            url: 原始连接URL

        Returns:
            str: 掩码后的连接URL，密码部分用***替换
        """
        import re

        # 匹配密码部分进行掩码
        return re.sub(r":([^:@]+)@", ":***@", url)

    def _get_pool_config(self) -> Dict[str, Any]:
        """
        获取连接池配置

        Returns:
            Dict[str, Any]: 合并后的连接池配置字典
        """
        # 默认连接池配置
        default_pool_config = {
            "pool_pre_ping": True,  # 连接预检查，确保连接有效
            "pool_recycle": 3600,  # 连接回收时间（秒），避免数据库连接超时
            "pool_size": 10,  # 连接池大小，控制并发连接数
            "max_overflow": 20,  # 最大溢出连接数，应对突发流量
        }

        # 合并用户自定义配置
        user_pool_config = self.connection_config.get("pool_config", {})
        return {**default_pool_config, **user_pool_config}

    def _get_engine_config(self, db_type: str) -> Dict[str, Any]:
        """
        获取数据库特定的引擎配置

        Args:
            db_type: 数据库类型

        Returns:
            Dict[str, Any]: 引擎配置字典

        Notes:
            - 不同数据库类型有不同的配置需求
            - 返回空字典表示无需特殊配置
        """
        config_getters = {
            "oracle": self._get_oracle_config,
            "postgresql": self._get_postgresql_config,
            "mysql": self._get_mysql_config,
            "mssql": self._get_mssql_config,
            "sqlite": self._get_sqlite_config,
        }

        getter = config_getters.get(db_type)
        return getter() if getter else {}

    def _get_oracle_config(self) -> Dict[str, Any]:
        """
        获取Oracle数据库的引擎配置

        Returns:
            Dict[str, Any]: Oracle引擎配置字典，包含connect_args等参数
        """
        connect_args: Dict[str, Any] = {}
        if "mode" in self.connection_config:
            connect_args["mode"] = self.connection_config["mode"]
        if "threaded" in self.connection_config:
            connect_args["threaded"] = self.connection_config["threaded"]

        return {"connect_args": connect_args} if connect_args else {}

    def _get_postgresql_config(self) -> Dict[str, Any]:
        """
        获取PostgreSQL数据库的引擎配置

        Returns:
            Dict[str, Any]: PostgreSQL引擎配置字典，包含connect_args等参数
        """
        connect_args: Dict[str, Any] = {}
        if "sslmode" in self.connection_config:
            connect_args["sslmode"] = self.connection_config["sslmode"]
        if "sslrootcert" in self.connection_config:
            connect_args["sslrootcert"] = self.connection_config["sslrootcert"]
        if "sslcert" in self.connection_config:
            connect_args["sslcert"] = self.connection_config["sslcert"]
        if "sslkey" in self.connection_config:
            connect_args["sslkey"] = self.connection_config["sslkey"]
        if "connect_timeout" in self.connection_config:
            connect_args["connect_timeout"] = self.connection_config["connect_timeout"]

        return {"connect_args": connect_args} if connect_args else {}

    def _get_mysql_config(self) -> Dict[str, Any]:
        """
        获取MySQL数据库的引擎配置

        Returns:
            Dict[str, Any]: MySQL引擎配置字典，包含connect_args等参数
        """
        connect_args: Dict[str, Any] = {}
        if "charset" in self.connection_config:
            connect_args["charset"] = self.connection_config["charset"]
        if "collation" in self.connection_config:
            connect_args["collation"] = self.connection_config["collation"]
        if "ssl_ca" in self.connection_config:
            connect_args["ssl_ca"] = self.connection_config["ssl_ca"]
        if "ssl_cert" in self.connection_config:
            connect_args["ssl_cert"] = self.connection_config["ssl_cert"]
        if "ssl_key" in self.connection_config:
            connect_args["ssl_key"] = self.connection_config["ssl_key"]

        return {"connect_args": connect_args} if connect_args else {}

    def _get_mssql_config(self) -> Dict[str, Any]:
        """
        获取SQL Server数据库的引擎配置

        Returns:
            Dict[str, Any]: SQL Server引擎配置字典，包含connect_args等参数
        """
        connect_args: Dict[str, Any] = {}
        if "driver" in self.connection_config:
            connect_args["driver"] = self.connection_config["driver"]
        if "trusted_connection" in self.connection_config:
            connect_args["trusted_connection"] = self.connection_config[
                "trusted_connection"
            ]
        if "connect_timeout" in self.connection_config:
            connect_args["connect_timeout"] = self.connection_config["connect_timeout"]

        return {"connect_args": connect_args} if connect_args else {}

    def _get_sqlite_config(self) -> Dict[str, Any]:
        """
        获取SQLite数据库的引擎配置

        Returns:
            Dict[str, Any]: SQLite引擎配置字典，包含connect_args等参数
        """
        connect_args: Dict[str, Any] = {}
        if "timeout" in self.connection_config:
            connect_args["timeout"] = self.connection_config["timeout"]
        if "isolation_level" in self.connection_config:
            connect_args["isolation_level"] = self.connection_config["isolation_level"]
        if "check_same_thread" in self.connection_config:
            connect_args["check_same_thread"] = self.connection_config[
                "check_same_thread"
            ]

        return {"connect_args": connect_args} if connect_args else {}

    def _validate_connection(self) -> None:
        """
        验证数据库连接的实际有效性

        该方法执行连接测试，如果测试失败则抛出ConnectionError异常。

        Raises:
            ConnectionError: 当连接验证失败时抛出
        """
        if self.engine is None:
            raise ConnectionError("数据库引擎未初始化，无法验证连接")

        if not self._perform_connection_test():
            # 连接验证失败，清理资源
            self._cleanup_on_connection_failure()
            error_msg = "数据库连接验证失败"
            logger.error(error_msg)
            raise ConnectionError(error_msg)

        # 连接验证成功，更新状态
        self._connected = True
        logger.info("数据库连接验证成功...")

    def _perform_connection_test(self) -> bool:
        """
        执行实际的连接测试

        Returns:
            bool: True表示连接测试成功，False表示失败
        """
        if self.engine is None:
            return False

        try:
            test_query = self._get_test_query()
            with self.engine.connect() as conn:
                conn.execute(text(test_query))
            return True
        except Exception:
            return False

    def _get_test_query(self) -> str:
        """
        获取适合当前数据库的测试查询语句

        Returns:
            str: 测试查询语句

        Notes:
            - 不同数据库有不同的测试查询语法
            - Oracle使用DUAL表，其他数据库使用SELECT 1
        """
        db_type = self.connection_config.get("type", "").lower()

        test_queries: Dict[str, str] = {
            "oracle": self.ORACLE_TEST_QUERY,
            "postgresql": self.TEST_QUERY_DEFAULT,
            "mysql": self.TEST_QUERY_DEFAULT,
            "mssql": self.TEST_QUERY_DEFAULT,
            "sqlite": self.TEST_QUERY_DEFAULT,
        }

        return test_queries.get(db_type, self.TEST_QUERY_DEFAULT)

    def _cleanup_on_connection_failure(self) -> None:
        """
        连接失败时的资源清理

        清理已创建的引擎和会话工厂资源，确保资源不会泄漏。
        """
        self._cleanup_resources()
        logger.debug("连接失败，已清理相关资源")

    def _cleanup_resources(self) -> None:
        """
        清理数据库资源

        清理会话工厂和引擎资源，重置连接状态。
        """
        # 清理会话工厂
        if self.session_factory:
            self.session_factory.remove()
            self.session_factory = None

        # 清理引擎资源
        if self.engine:
            self.engine.dispose()
            self.engine = None

        # 重置连接状态
        self._connected = False

    def disconnect(self) -> None:
        """
        断开数据库连接并清理资源

        该方法执行完整的资源清理流程，包括：
        1. 检查当前连接状态，避免重复断开
        2. 清理会话工厂资源
        3. 清理引擎资源
        4. 重置连接状态
        5. 记录断开日志

        Notes:
            - 支持安全断开，即使连接不存在也不会抛出异常
            - 自动清理所有相关资源，避免内存泄漏
            - 线程安全操作，支持并发断开

        Example:
            >>> driver = SQLAlchemyDriver(connection_config)
            >>> driver.connect()
            >>> driver.disconnect()  # 断开数据库连接
            >>> print(driver.is_connected)  # 检查连接状态
            False
        """
        if not self._connected:
            logger.debug("数据库连接已断开，无需重复操作")
            return

        try:
            self._cleanup_resources()
            logger.info("数据库连接已成功断开")
        except Exception as e:
            error_msg = f"数据库连接断开失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ConnectionError(error_msg)

    def test_connection(self) -> bool:
        """
        测试数据库连接是否有效

        该方法执行连接测试，返回连接状态而不抛出异常。

        Returns:
            bool: True表示连接有效，False表示连接无效

        Notes:
            - 如果当前未连接，会自动尝试建立连接
            - 测试失败不会抛出异常，而是返回False
            - 适合用于连接健康检查或监控

        Example:
            >>> driver = SQLAlchemyDriver(connection_config)
            >>> if driver.test_connection():
            ...     print("连接正常")
            ... else:
            ...     print("连接异常")
        """
        try:
            # 如果未连接，尝试建立连接
            if not self._connected:
                self.connect()
            return self._perform_connection_test()
        except Exception:
            return False

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
            ConnectionError: 当数据库未连接时抛出
            ValueError: 当查询语句为空或仅包含空白字符时抛出
            QueryError: 当查询执行失败时抛出

        Notes:
            - 支持参数化查询，防止SQL注入攻击
            - 自动处理连接状态验证
            - 结果转换为字典格式，便于数据处理
            - 支持事务管理，确保数据一致性

        Example:
            >>> results = driver.execute_query(
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
            >>> for row in results:
            ...     print(row["name"], row["age"])
        """
        if not self._connected or self.engine is None:
            raise ConnectionError("数据库未连接，请先调用connect()方法")

        # 验证查询语句不为空
        if not query or not query.strip():
            raise ValueError("查询语句不能为空")

        try:
            with self.engine.connect() as conn:
                # 执行参数化查询
                result: Result = conn.execute(
                    text(query), parameters if parameters else {}
                )

                # 将结果转换为字典列表
                rows = result.fetchall()
                return [dict(row._mapping) for row in rows]

        except SQLAlchemyError as e:
            error_msg = f"查询执行失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise QueryError(error_msg)

    def execute_command(
        self, command: str, parameters: Dict[str, Any] | None = None
    ) -> int:
        """
        执行SQL命令语句（INSERT/UPDATE/DELETE等）

        Args:
            command: SQL命令语句
            parameters: 命令参数字典，用于参数化执行

        Returns:
            int: 受影响的行数

        Raises:
            ConnectionError: 当数据库未连接时抛出
            ValueError: 当命令语句为空或仅包含空白字符时抛出
            QueryError: 当命令执行失败时抛出

        Notes:
            - 支持参数化执行，防止SQL注入攻击
            - 自动提交事务，确保数据持久化
            - 返回受影响的行数，便于业务逻辑处理

        Example:
            >>> affected_rows = driver.execute_command(
            ...     "UPDATE users SET status = :status WHERE id = :id",
            ...     {"status": "active", "id": 1}
            ... )
            >>> print(f"更新了 {affected_rows} 行数据")
        """
        if not self._connected or self.engine is None:
            raise ConnectionError("数据库未连接，请先调用connect()方法")

        # 验证命令语句不为空
        if not command or not command.strip():
            raise ValueError("命令语句不能为空")

        try:
            with self.engine.begin() as conn:
                # 执行参数化命令
                result: Result = conn.execute(
                    text(command), parameters if parameters else {}
                )
                return result.rowcount

        except SQLAlchemyError as e:
            error_msg = f"命令执行失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise QueryError(error_msg)

    @property
    def is_connected(self) -> bool:
        """
        检查数据库连接状态

        返回当前数据库连接的有效性状态。

        Returns:
            bool: True表示连接有效，False表示连接无效

        Notes:
            - 该属性会执行实际的连接测试，而不仅仅是状态检查
            - 如果连接测试失败，会自动更新内部连接状态
            - 适合用于实时连接状态监控

        Example:
            >>> driver = SQLAlchemyDriver(connection_config)
            >>> driver.connect()
            >>> if driver.is_connected:
            ...     print("连接正常")
            ... else:
            ...     print("连接异常")
        """
        if not self._connected:
            return False

        # 执行实际的连接测试
        is_valid = self._perform_connection_test()
        if not is_valid:
            # 连接测试失败，更新状态
            self._connected = False
            logger.warning("连接状态检查失败，连接已标记为无效")

        return is_valid

    def get_connection_info(self) -> Dict[str, Any]:
        """
        获取数据库连接信息

        返回当前数据库连接的详细信息，包括配置参数和状态。

        Returns:
            Dict[str, Any]: 连接信息字典，包含以下字段：
                - database_type: 数据库类型
                - host: 主机地址
                - port: 端口号
                - database: 数据库名
                - username: 用户名（掩码处理）
                - is_connected: 连接状态
                - pool_size: 连接池大小

        Notes:
            - 敏感信息（如密码）会进行掩码处理
            - 返回完整的连接配置信息，便于调试和监控

        Example:
            >>> info = driver.get_connection_info()
            >>> print(f"连接到 {info['database_type']} 数据库")
            >>> print(f"主机: {info['host']}:{info['port']}")
            >>> print(f"状态: {'已连接' if info['is_connected'] else '未连接'}")
        """
        config = self.connection_config
        pool_config = self._get_pool_config()

        return {
            "database_type": config.get("type"),
            "host": config.get("host"),
            "port": config.get("port", self._get_default_port(config.get("type", ""))),
            "database": config.get("database"),
            "username": config.get("username", "***"),  # 掩码处理
            "is_connected": self._connected,
            "pool_size": pool_config.get("pool_size"),
        }

    def __enter__(self) -> "SQLAlchemyDriver":
        """
        上下文管理器入口方法

        支持with语句，自动管理数据库连接。

        Returns:
            SQLAlchemyDriver: 当前驱动实例

        Notes:
            - 自动建立数据库连接
            - 确保资源正确初始化
            - 支持嵌套上下文管理

        Example:
            >>> with SQLAlchemyDriver(config) as driver:
            ...     results = driver.execute_query("SELECT * FROM users")
        """
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        上下文管理器退出方法

        自动清理数据库连接资源。

        Args:
            exc_type: 异常类型
            exc_val: 异常值
            exc_tb: 异常追踪信息

        Notes:
            - 无论是否发生异常都会执行资源清理
            - 自动断开数据库连接
            - 确保资源不会泄漏

        Example:
            >>> with SQLAlchemyDriver(config) as driver:
            ...     # 执行数据库操作
            ...     pass
            >>> # 退出with块后连接自动断开
        """
        self.disconnect()

    def __repr__(self) -> str:
        """
        返回驱动实例的字符串表示

        Returns:
            str: 包含数据库类型、主机、端口、数据库名和连接状态的字符串表示

        Example:
            >>> driver = SQLAlchemyDriver(config)
            >>> print(repr(driver))
            SQLAlchemyDriver(type='mysql', host='localhost', port=3306,
                            database='mydb', username='***', connected=True)
        """
        try:
            config = self.connection_config or {}
            db_type = config.get("type", "unknown")
            host = config.get("host", "unknown")
            port = config.get("port", self._get_default_port(db_type))
            database = config.get("database", "unknown")
            username = config.get("username", "***")  # 掩码处理
            status = self._connected

            return (
                f"SQLAlchemyDriver(type={db_type!r}, host={host!r}, "
                f"port={port!r}, database={database!r}, "
                f"username={username!r}, connected={status!r})"
            )
        except Exception:
            # 确保__repr__不会抛出异常
            return "SQLAlchemyDriver(<error retrieving info>)"
