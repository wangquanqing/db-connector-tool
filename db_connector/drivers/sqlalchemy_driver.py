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

from typing import Any, Dict, List
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
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
        "oracle": "oracle+oracledb://{username}:{password}@{host}:{port}/{database}",
        "postgresql": "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}",
        "mysql": "mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
        "mssql": "mssql+pymssql://{username}:{password}@{host}:{port}/{database}",
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
        self.engine: Any | None = None
        self.session_factory: Any | None = None
        self._connected: bool = False

        # 验证数据库类型支持
        db_type = self.connection_config.get("type", "").lower()
        if db_type not in self.URL_TEMPLATES:
            supported_types = ", ".join(self.URL_TEMPLATES.keys())
            raise DriverError(
                f"不支持的数据库类型: {db_type}，支持的类型: {supported_types}"
            )

        logger.debug(f"SQLAlchemy驱动实例初始化成功，数据库类型: {db_type}")

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
        required_fields = ["username", "password", "host", "database"]
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

        # 添加查询参数
        query_params = self._build_query_params(db_type, config)
        if query_params:
            base_url += "?" + query_params

        logger.debug(f"构建的连接URL: {self._mask_sensitive_info(base_url)}")
        return base_url

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

    def _build_query_params(self, db_type: str, config: Dict[str, Any]) -> str:
        """
        构建查询参数字符串

        根据数据库类型和配置参数构建URL查询参数字符串。

        Args:
            db_type: 数据库类型
            config: 连接配置字典

        Returns:
            str: 查询参数字符串，格式为 "param1=value1&param2=value2"

        Notes:
            - 只包含当前数据库类型支持的参数
            - 自动处理布尔值参数
            - 忽略None值参数

        Example:
            >>> params = driver._build_query_params("mysql", {"charset": "utf8"})
            >>> print(params)
            charset=utf8
        """
        # 各数据库支持的查询参数映射
        query_param_map: Dict[str, List[str]] = {
            "mssql": ["charset", "tds_version", "driver", "trusted_connection"],
            "mysql": ["charset", "collation", "ssl_ca", "ssl_cert", "ssl_key"],
            "postgresql": [
                "sslmode",
                "sslrootcert",
                "sslcert",
                "sslkey",
                "connect_timeout",
            ],
            "oracle": ["service_name", "sid", "mode", "threaded"],
            "sqlite": ["timeout", "isolation_level", "check_same_thread"],
        }

        supported_params = query_param_map.get(db_type, [])
        query_params: List[str] = []

        for param in supported_params:
            if param in config and config[param] is not None:
                # 布尔值特殊处理
                if isinstance(config[param], bool):
                    value = "true" if config[param] else "false"
                else:
                    value = str(config[param])
                query_params.append(f"{param}={value}")

        return "&".join(query_params)

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
            "sqlite": self._get_sqlite_config,
            "mysql": self._get_mysql_config,
            "postgresql": self._get_postgresql_config,
            "oracle": self._get_oracle_config,
        }

        getter = config_getters.get(db_type)
        return getter() if getter else {}

    def _get_sqlite_config(self) -> Dict[str, Any]:
        """获取SQLite数据库的引擎配置"""
        config: Dict[str, Any] = {}
        if "timeout" in self.connection_config:
            config["connect_args"] = {"timeout": self.connection_config["timeout"]}
        return config

    def _get_mysql_config(self) -> Dict[str, Any]:
        """获取MySQL数据库的引擎配置"""
        connect_args: Dict[str, Any] = {}
        if "charset" in self.connection_config:
            connect_args["charset"] = self.connection_config["charset"]
        return {"connect_args": connect_args} if connect_args else {}

    def _get_postgresql_config(self) -> Dict[str, Any]:
        """获取PostgreSQL数据库的引擎配置"""
        connect_args: Dict[str, Any] = {}
        if "sslmode" in self.connection_config:
            connect_args["sslmode"] = self.connection_config["sslmode"]
        if "connect_timeout" in self.connection_config:
            connect_args["connect_timeout"] = self.connection_config["connect_timeout"]
        return {"connect_args": connect_args} if connect_args else {}

    def _get_oracle_config(self) -> Dict[str, Any]:
        """获取Oracle数据库的引擎配置"""
        connect_args: Dict[str, Any] = {}
        if "service_name" in self.connection_config:
            connect_args["service_name"] = self.connection_config["service_name"]
        if "sid" in self.connection_config:
            connect_args["sid"] = self.connection_config["sid"]
        return {"connect_args": connect_args} if connect_args else {}

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
                f"({self.connection_config.get('host')})"
            )

            # 执行实际连接测试验证连接有效性
            self._validate_connection()

        except SQLAlchemyError as e:
            # 连接过程中发生异常，清理资源并抛出错误
            self._cleanup_on_connection_failure()
            error_msg = f"数据库连接建立失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ConnectionError(error_msg)

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

    def _validate_connection(self) -> None:
        """
        验证数据库连接的实际有效性

        执行简单的测试查询来确认数据库连接正常工作。
        如果验证失败，会清理已创建的引擎资源。

        Raises:
            ConnectionError: 当连接验证失败时
        """
        if self.engine is None:
            raise ConnectionError("数据库引擎未初始化，无法验证连接")

        try:
            # 执行简单的测试查询来验证连接
            with self.engine.connect() as test_conn:
                test_query = self._get_test_query()
                test_conn.execute(text(test_query))

            # 连接验证成功，更新状态
            self._connected = True
            logger.info(
                f"数据库连接验证成功: {self.connection_config.get('type')} "
                f"({self.connection_config.get('host')})"
            )

        except SQLAlchemyError as e:
            # 连接验证失败，清理资源
            self._cleanup_on_connection_failure()
            error_msg = f"数据库连接验证失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ConnectionError(error_msg)

    def _cleanup_on_connection_failure(self) -> None:
        """
        连接失败时的资源清理

        清理已创建的引擎和会话工厂资源，确保资源不会泄漏。
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
        logger.debug("连接失败，已清理相关资源")

    def disconnect(self) -> None:
        """
        断开数据库连接

        清理连接池资源并关闭所有数据库连接。

        Raises:
            ConnectionError: 当断开连接失败时

        Notes:
            - 安全地清理会话工厂和引擎资源
            - 更新连接状态标志
            - 异常情况下仍会尝试清理资源

        Example:
            >>> driver.disconnect()  # 断开数据库连接
            >>> print(driver.is_connected)  # 检查连接状态
            False
        """
        try:
            # 清理会话工厂
            if self.session_factory:
                self.session_factory.remove()
                self.session_factory = None

            # 清理引擎资源
            if self.engine:
                self.engine.dispose()
                self.engine = None

            self._connected = False
            logger.info("数据库连接已安全断开")

        except Exception as e:
            error_msg = f"断开数据库连接失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise ConnectionError(error_msg)

    def execute_query(
        self, query: str, params: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """
        执行SQL查询语句

        Args:
            query: SQL查询语句，支持参数化查询
            params: 查询参数字典，可选

        Returns:
            List[Dict[str, Any]]: 查询结果列表，每行数据为字典格式

        Raises:
            ConnectionError: 当数据库未连接时
            QueryError: 当查询执行失败时

        Notes:
            - 使用参数化查询防止SQL注入
            - 自动处理连接状态验证
            - 结果转换为字典格式便于使用

        Example:
            >>> results = driver.execute_query(
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
            >>> print(results)
            [{'id': 1, 'name': 'Alice', 'age': 25}, ...]
        """
        # 验证连接状态
        if not self._connected or self.engine is None:
            raise ConnectionError("数据库未连接或引擎未初始化")

        # 验证查询语句
        if not query or not query.strip():
            raise QueryError("查询语句不能为空")

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                # 将结果转换为字典列表
                return [dict(row._mapping) for row in result]

        except SQLAlchemyError as e:
            error_msg = f"查询执行失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise QueryError(error_msg)

    def execute_command(
        self, command: str, params: Dict[str, Any] | None = None
    ) -> int:
        """
        执行非查询SQL语句（INSERT/UPDATE/DELETE等）

        Args:
            command: SQL命令语句
            params: 命令参数字典，可选

        Returns:
            int: 影响的行数

        Raises:
            ConnectionError: 当数据库未连接时
            QueryError: 当命令执行失败时

        Notes:
            - 使用事务执行确保数据一致性
            - 自动验证命令语句有效性
            - 返回受影响的行数便于业务逻辑处理

        Example:
            >>> affected_rows = driver.execute_command(
            ...     "UPDATE users SET name = :name WHERE id = :id",
            ...     {"name": "Bob", "id": 1}
            ... )
            >>> print(f"更新了 {affected_rows} 行")
        """
        # 验证连接状态
        if not self._connected or self.engine is None:
            raise ConnectionError("数据库未连接或引擎未初始化")

        # 验证命令语句
        if not command or not command.strip():
            raise QueryError("命令语句不能为空")

        try:
            # 使用事务执行命令
            with self.engine.begin() as connection:
                result = connection.execute(text(command), params or {})
                return result.rowcount

        except SQLAlchemyError as e:
            error_msg = f"命令执行失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise QueryError(error_msg)

    def test_connection(self) -> bool:
        """
        测试数据库连接是否有效

        Returns:
            bool: True表示连接成功，False表示连接失败

        Notes:
            - 如果当前未连接，会自动尝试连接
            - 执行简单的测试查询验证连接有效性
            - 失败时会记录详细的错误信息

        Example:
            >>> if driver.test_connection():
            ...     print("连接测试成功")
            ... else:
            ...     print("连接测试失败")
        """
        try:
            self.connect()
            test_query = self._get_test_query()
            self.execute_query(test_query)
            logger.info("数据库连接测试成功")
            return True
        except Exception as e:
            error_msg = f"连接测试失败: {e.__class__.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
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

    def __enter__(self) -> "SQLAlchemyDriver":
        """
        上下文管理器入口

        Returns:
            SQLAlchemyDriver: 当前驱动实例

        Notes:
            - 支持with语句，自动管理连接生命周期
            - 进入上下文时自动建立连接
        """
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: type | None,
        exc_val: Exception | None,
        exc_tb: Any | None,
    ) -> None:
        """
        上下文管理器出口

        Args:
            exc_type: 异常类型
            exc_val: 异常值
            exc_tb: 异常追踪信息

        Notes:
            - 退出上下文时自动断开连接
            - 异常情况下也会确保资源正确清理
        """
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        """
        获取当前连接状态

        不仅检查内部标志，还实际验证连接有效性。

        Returns:
            bool: 当前连接状态

        Notes:
            - 如果连接无效，会自动更新内部状态
            - 实际执行测试查询验证连接
        """
        if not self._connected or self.engine is None:
            return False

        # 实际验证连接有效性
        try:
            test_query = self._get_test_query()
            with self.engine.connect() as conn:
                conn.execute(text(test_query))
            return True
        except Exception:
            # 连接无效，更新状态
            self._connected = False
            return False

    def get_connection_info(self) -> Dict[str, Any]:
        """
        获取连接信息摘要

        Returns:
            Dict[str, Any]: 连接信息字典，包含数据库类型、主机、端口等

        Notes:
            - 不包含敏感信息如密码
            - 用于调试和监控目的
        """
        return {
            "database_type": self.connection_config.get("type"),
            "host": self.connection_config.get("host"),
            "port": self.connection_config.get("port"),
            "database": self.connection_config.get("database"),
            "connected": self._connected,
            "engine_initialized": self.engine is not None,
        }
