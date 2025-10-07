"""
SQLAlchemy 数据库驱动模块

提供基于SQLAlchemy的统一数据库连接接口，支持多种数据库类型：
- Oracle
- PostgreSQL
- MySQL
- SQL Server
- SQLite

该模块封装了数据库连接管理、查询执行和连接池配置等功能。
"""

from typing import Any, Dict, List, Optional
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from ..core.exceptions import ConnectionError, DriverError, QueryError
from ..utils.logging_utils import get_logger

logger = get_logger(__name__)


class SQLAlchemyDriver:
    """
    SQLAlchemy 数据库驱动类

    提供统一的数据库连接和操作接口，支持多种数据库类型。
    使用连接池管理数据库连接，提高性能和资源利用率。

    Attributes:
        DRIVER_MAP (Dict[str, str]): 数据库类型到驱动名称的映射
        URL_TEMPLATES (Dict[str, str]): 各数据库类型的连接URL模板
        TEST_QUERY_DEFAULT (str): 默认测试查询语句
        ORACLE_TEST_QUERY (str): Oracle专用测试查询语句
    """

    # 数据库驱动映射
    DRIVER_MAP: Dict[str, str] = {
        "oracle": "oracledb",
        "postgresql": "psycopg",
        "mysql": "pymysql",
        "mssql": "pymssql",
        "sqlite": "sqlite3",
    }

    # 连接URL模板
    URL_TEMPLATES: Dict[str, str] = {
        "oracle": "oracle+oracledb://{username}:{password}@{host}:{port}/{database}",
        "postgresql": "postgresql+psycopg://{username}:{password}@{host}:{port}/{database}",
        "mysql": "mysql+pymysql://{username}:{password}@{host}:{port}/{database}",
        "mssql": "mssql+pymssql://{username}:{password}@{host}:{port}/{database}",
        "sqlite": "sqlite:///{database}",
    }

    # 定义测试查询常量
    TEST_QUERY_DEFAULT: str = "SELECT 1"
    ORACLE_TEST_QUERY: str = "SELECT 1 FROM DUAL"

    def __init__(self, connection_config: Dict[str, Any]) -> None:
        """
        初始化数据库驱动

        Args:
            connection_config: 数据库连接配置字典，包含以下字段：
                - type: 数据库类型 (oracle/postgresql/mysql/mssql/sqlite)
                - username: 用户名
                - password: 密码
                - host: 主机地址
                - port: 端口号 (可选)
                - database: 数据库名
                - 其他数据库特定参数

        Raises:
            ValueError: 当连接配置为空或无效时
        """
        if not connection_config or not isinstance(connection_config, dict):
            raise ValueError("连接配置不能为空且必须为字典类型")

        self.connection_config = connection_config
        self.engine: Optional[Any] = None
        self.session_factory: Optional[Any] = None
        self._connected: bool = False

    def _build_connection_url(self) -> str:
        """
        构建数据库连接URL

        Returns:
            完整的数据库连接URL字符串

        Raises:
            DriverError: 当数据库类型不支持或缺少必需参数时
        """
        db_type = self.connection_config.get("type", "").lower()

        if db_type not in self.URL_TEMPLATES:
            raise DriverError(f"不支持的数据库类型: {db_type}")

        template = self.URL_TEMPLATES[db_type]
        config = self.connection_config.copy()

        # SQLite特殊处理
        if db_type == "sqlite":
            if "database" not in config:
                config["database"] = ":memory:"
            return template.format(**config)

        # 其他数据库类型验证必需参数
        required_fields = ["username", "password", "host", "database"]
        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            raise DriverError(f"缺少必需的连接参数: {', '.join(missing_fields)}")

        # 设置默认端口
        if "port" not in config:
            config["port"] = self._get_default_port(db_type)

        # 对用户名和密码中的特殊字符进行URL编码
        config["username"] = quote_plus(config["username"])
        config["password"] = quote_plus(config["password"])

        # 对主机名中的特殊字符进行编码（如果包含特殊字符）
        if "@" in config["host"] or ":" in config["host"]:
            config["host"] = quote_plus(config["host"])

        # 构建基础URL
        base_url = template.format(**config)

        # 添加查询参数（自定义参数）
        query_params = self._build_query_params(db_type, config)
        if query_params:
            base_url += "?" + query_params

        return base_url

    def _build_query_params(self, db_type: str, config: Dict[str, Any]) -> str:
        """
        构建查询参数字符串

        Args:
            db_type: 数据库类型
            config: 连接配置字典

        Returns:
            查询参数字符串，如 "param1=value1&param2=value2"
        """
        # 定义各数据库支持的查询参数
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

        # 过滤出当前数据库支持的参数
        supported_params = query_param_map.get(db_type, [])
        query_params: List[str] = []

        for param in supported_params:
            if param in config and config[param] is not None:
                # 特殊处理布尔值
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
            引擎配置字典
        """
        config_getters = {
            "sqlite": self._get_sqlite_config,
            "mysql": self._get_mysql_config,
            "postgresql": self._get_postgresql_config,
            "oracle": self._get_oracle_config,
        }
        getter = config_getters.get(db_type)
        if getter:
            return getter()
        return {}

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
            默认端口号字符串
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
        连接到数据库

        建立数据库连接并初始化连接池配置。

        Raises:
            ConnectionError: 当连接失败时
        """
        try:
            if self._connected:
                logger.warning("数据库连接已存在，无需重复连接")
                return

            connection_url = self._build_connection_url()

            # 准备引擎配置
            engine_kwargs: Dict[str, Any] = {
                "echo": False,  # 设置为True可显示SQL日志
                "pool_pre_ping": True,  # 连接池预检查
                "pool_recycle": 3600,  # 连接回收时间（秒）
                "pool_size": 10,  # 连接池大小
                "max_overflow": 20,  # 最大溢出连接数
            }

            # 添加数据库特定的引擎配置
            db_type = self.connection_config.get("type", "").lower()
            engine_kwargs.update(self._get_engine_config(db_type))

            # 创建引擎
            self.engine = create_engine(connection_url, **engine_kwargs)

            # 创建会话工厂
            self.session_factory = scoped_session(sessionmaker(bind=self.engine))

            self._connected = True
            logger.info(f"数据库连接成功: {self.connection_config.get('type')}")

        except SQLAlchemyError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise ConnectionError(f"数据库连接失败: {str(e)}")
        except Exception as e:
            logger.error(f"连接过程中发生未知错误: {str(e)}")
            raise ConnectionError(f"连接错误: {str(e)}")

    def disconnect(self) -> None:
        """
        断开数据库连接

        清理连接池资源并关闭所有数据库连接。

        Raises:
            ConnectionError: 当断开连接失败时
        """
        try:
            if self.session_factory:
                self.session_factory.remove()
                self.session_factory = None

            if self.engine:
                self.engine.dispose()
                self.engine = None

            self._connected = False
            logger.info("数据库连接已断开")

        except Exception as e:
            logger.error(f"断开数据库连接失败: {str(e)}")
            raise ConnectionError(f"断开连接失败: {str(e)}")

    def execute_query(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        执行查询语句

        Args:
            query: SQL查询语句
            params: 查询参数字典，可选

        Returns:
            查询结果列表，每行数据为字典格式

        Raises:
            ConnectionError: 当数据库未连接时
            QueryError: 当查询执行失败时

        Example:
            >>> driver.execute_query("SELECT * FROM users WHERE age > :age", {"age": 18})
            [{'id': 1, 'name': 'Alice', 'age': 25}, ...]
        """
        if not self._connected or self.engine is None:
            raise ConnectionError("数据库未连接或引擎未初始化")

        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return [dict(row._mapping) for row in result]

        except SQLAlchemyError as e:
            logger.error(f"查询执行失败: {str(e)}")
            raise QueryError(f"查询执行失败: {str(e)}")

    def execute_command(
        self, command: str, params: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        执行非查询语句（INSERT/UPDATE/DELETE等）

        Args:
            command: SQL命令语句
            params: 命令参数字典，可选

        Returns:
            影响的行数

        Raises:
            ConnectionError: 当数据库未连接时
            QueryError: 当命令执行失败时

        Example:
            >>> driver.execute_command("UPDATE users SET name = :name WHERE id = :id",
            ...                       {"name": "Bob", "id": 1})
            1
        """
        if not self._connected or self.engine is None:
            raise ConnectionError("数据库未连接或引擎未初始化")

        try:
            with self.engine.begin() as connection:
                result = connection.execute(text(command), params or {})
                return result.rowcount

        except SQLAlchemyError as e:
            logger.error(f"命令执行失败: {str(e)}")
            raise QueryError(f"命令执行失败: {str(e)}")

    def test_connection(self) -> bool:
        """
        测试连接是否有效

        Returns:
            True表示连接成功，False表示连接失败

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
            logger.info("连接测试成功")
            return True
        except Exception as e:
            logger.error(f"连接测试失败: {str(e)}")
            return False

    def _get_test_query(self) -> str:
        """
        获取测试连接用的查询语句

        Returns:
            适合当前数据库类型的测试查询语句
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
            SQLAlchemyDriver实例
        """
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type],
        exc_val: Optional[Exception],
        exc_tb: Optional[Any],
    ) -> None:
        """
        上下文管理器出口

        Args:
            exc_type: 异常类型
            exc_val: 异常值
            exc_tb: 异常追踪信息
        """
        self.disconnect()

    @property
    def is_connected(self) -> bool:
        """
        获取连接状态

        Returns:
            当前连接状态（True/False）
        """
        return self._connected

    def get_connection_info(self) -> Dict[str, Any]:
        """
        获取连接信息（不包含敏感信息）

        Returns:
            连接信息字典，包含数据库类型、主机、端口等
        """
        info = {
            "type": self.connection_config.get("type"),
            "host": self.connection_config.get("host"),
            "port": self.connection_config.get("port"),
            "database": self.connection_config.get("database"),
            "connected": self._connected,
        }
        return {k: v for k, v in info.items() if v is not None}
