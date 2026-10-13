"""SQLAlchemy 数据库驱动模块 (SQLAlchemyDriver)

提供基于 SQLAlchemy 的统一数据库连接接口，支持多种数据库类型，
封装了数据库连接管理、查询执行、连接池配置和错误处理等功能，
提供线程安全的数据库操作接口。

Example:
>>> from db_connector_tool import SQLAlchemyDriver
>>>
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
>>> with driver:
...     results = driver.execute_query("SELECT * FROM users")
...     print(results)
>>>
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

from sqlalchemy import create_engine, inspect, select, text
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from ..core.exceptions import DBConnectionError, DriverError, QueryError
from ..utils.logging_utils import get_logger
from .result_fetch import ResultFetchMixin, run_sql
from .sql_validator import validate_sql

logger = get_logger(__name__)

BASIC_PARAMS = [
    "type",
    "host",
    "port",
    "username",
    "password",
    "database",
    "service_name",
    "server",
    "sqlmode",
]

POOL_PARAMS = {
    "pool_size",
    "max_overflow",
    "pool_timeout",
    "pool_recycle",
    "pool_pre_ping",
    "echo",
    "poolclass",
    "pool_reset_on_return",
    "pool_use_lifo",
    "pool_logging_name",
    "pool_events",
}


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


# pylint: disable=protected-access
PGDialect._get_server_version_info = parse_kingbase_version


class SQLAlchemyDriver(ResultFetchMixin):
    """SQLAlchemy 数据库驱动类 (SQLAlchemy Driver)

    提供统一的数据库连接和操作接口，支持多种数据库类型，
    使用连接池管理数据库连接，提高性能和资源利用率，
    支持上下文管理器协议，可使用 `with` 语句自动管理连接。

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
        >>>
        >>> with driver:
        ...     results = driver.execute_query("SELECT * FROM users")
        ...     print(results)
        >>>
        >>> driver.connect()
        >>> try:
        ...     affected = driver.execute_command("UPDATE users SET status = 'active'")
        ...     print(f"更新了 {affected} 行")
        ... finally:
        ...     driver.disconnect()
        >>>
        >>> if driver.test_connection():
        ...     print("数据库连接正常")
        ... else:
        ...     print("数据库连接异常")
    """

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
        "gbase8s": {
            "url_template": (
                "gbase8s://{host}:{port}/{database}:GBASEDBTSERVER={server}"
                "?user={username}&password={password}&sqlmode={sqlmode}"
            ),
            "required_params": [
                "host",
                "database",
                "server",
                "username",
                "password",
            ],
            "default_port": 9088,
            "defaults": {"sqlmode": "oracle"},
        },
    }

    # 连接测试的语句**必须交给方言渲染**，不能硬编码成字符串：
    # GBase 8s 在 gbase/mysql 模式下不接受无 FROM 的 SELECT（真机报“发生语法错误”），
    # 其方言会按 sqlmode 补上 ``FROM DUAL`` / ``FROM sysmaster:sysdual``。
    # 渲染入口是 ``expression.select(1).compile(dialect=...)`` —— 它走正常编译路径，
    # 会调用方言的 ``default_from()``。见 ``_perform_connection_test``。
    TEST_QUERY_DEFAULT = "SELECT 1"

    def __init__(self, config: Dict[str, Any]) -> None:
        """初始化 SQLAlchemy 驱动

        创建新的 SQLAlchemy 驱动实例，自动验证配置并准备连接参数。

        Args:
            config: 数据库连接配置字典，包含以下必需字段：
                - type: 数据库类型（oracle/postgresql/mysql/sqlserver/sqlite/gbase8s）
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
        self._validate_config()

    def __str__(self) -> str:
        """返回 SQLAlchemyDriver 的用户友好字符串表示

        Returns:
            str: 格式为 "SQLAlchemyDriver('database_type', connected: True/False)" 的字符串
        """
        database_type = self.config.get("type", "unknown")
        is_connected = self.engine is not None
        return f"SQLAlchemyDriver('{database_type}', connected: {is_connected})"

    def __repr__(self) -> str:
        """返回 SQLAlchemyDriver 的详细表示，用于调试

        Returns:
            str: 包含完整配置信息的字符串，用于调试
        """
        database_type = self.config.get("type", "unknown")
        host = self.config.get("host", "N/A")
        port = self.config.get("port", "N/A")
        database = self.config.get("database", "N/A")
        is_connected = self.engine is not None
        return (
            f"SQLAlchemyDriver(type='{database_type}', "
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

        config_copy = self._prepare_base_config(database_config)

        self._encode_sensitive_params(config_copy)

        url = database_config["url_template"].format(**config_copy)

        query_params = self._build_query_params(config_copy, database_config)

        url = self._append_query_params(url, query_params)

        logger.debug("构建的数据库连接URL: %s", self._mask_sensitive_info(url))

        return url

    def _prepare_base_config(self, database_config: dict) -> dict:
        """准备基础配置，设置默认值

        Args:
            database_config: 数据库配置信息

        Returns:
            dict: 处理后的配置副本
        """
        config_copy = self.config.copy()
        config_copy.setdefault("port", database_config["default_port"])
        for key, value in database_config.get("defaults", {}).items():
            config_copy.setdefault(key, value)
        return config_copy

    def _encode_sensitive_params(self, config_copy: dict) -> None:
        """对敏感参数进行URL编码

        Args:
            config_copy: 配置字典
        """
        sensitive_params = ["host", "username", "password"]
        for param in sensitive_params:
            if param in config_copy:
                config_copy[param] = quote_plus(str(config_copy[param]))

    def _build_query_params(self, config_copy: dict, database_config: dict) -> list:
        """构建查询参数列表

        Args:
            config_copy: 配置字典
            database_config: 数据库配置信息

        Returns:
            list: 查询参数列表
        """
        custom_params = self._collect_custom_params(config_copy)

        if "defaults" in database_config:
            self._merge_default_params(custom_params, database_config["defaults"])

        return list(custom_params.values())

    def _collect_custom_params(self, config_copy: dict) -> dict:
        """收集自定义参数

        Args:
            config_copy: 配置字典

        Returns:
            dict: 自定义参数字典
        """
        query_params = {}

        for key, value in config_copy.items():
            if self._should_skip_param(key, value):
                continue

            encoded_value = quote_plus(str(value))
            query_params[key] = f"{key}={encoded_value}"

        return query_params

    def _should_skip_param(self, key: str, value: Any) -> bool:
        """判断是否应该跳过参数

        Args:
            key: 参数键
            value: 参数值

        Returns:
            bool: 是否跳过
        """
        if key in BASIC_PARAMS:
            return True

        if key in POOL_PARAMS:
            logger.debug("跳过连接池参数 '%s'，将通过SQLAlchemy配置处理", key)
            return True

        if key == "pool_config":
            logger.debug("跳过pool_config参数，将通过SQLAlchemy配置处理")
            return True

        if value is None:
            return True

        return False

    def _merge_default_params(self, query_params: dict, defaults: dict) -> None:
        """合并默认参数

        Args:
            query_params: 查询参数字典
            defaults: 默认参数字典
        """
        for key, value in defaults.items():
            if key in BASIC_PARAMS:
                continue
            if key in query_params:
                logger.debug("自定义参数 '%s' 覆盖了默认参数", key)
                continue

            encoded_value = quote_plus(str(value))
            query_params[key] = f"{key}={encoded_value}"

    def _append_query_params(self, url: str, query_params: list) -> str:
        """添加查询参数到URL

        Args:
            url: 基础URL
            query_params: 查询参数列表

        Returns:
            str: 完整的URL
        """
        if not query_params:
            return url

        separator = "?" if "?" not in url else "&"
        url += separator + "&".join(query_params)

        return url

    def _mask_sensitive_info(self, url: str) -> str:
        """
        掩码连接URL中的敏感信息

        Args:
            url: 原始连接URL

        Returns:
            str: 掩码后的连接URL，密码部分用***替换
        """
        if "@" in url:
            url = re.sub(r":([^:@]+)@", ":***@", url)
        else:
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
            >>> driver.connect()
            >>> driver.disconnect()
        """
        try:
            if self.engine:
                self.disconnect()

            connection_url = self._build_connection_url()

            database_type = self.config.get("type", "").lower()

            if database_type == "sqlite":
                pool_config = {
                    "pool_size": 5,
                    "pool_pre_ping": True,
                    "echo": False,
                }
            else:
                pool_config = {
                    "pool_size": 5,
                    "max_overflow": 10,
                    "pool_timeout": 30,
                    "pool_pre_ping": True,
                    "pool_recycle": 3600,
                    "echo": False,
                }

                if database_type == "mysql":
                    pool_config.update(
                        {
                            "pool_recycle": 280,
                        }
                    )
                elif database_type in ("postgresql", "sqlserver"):
                    pool_config.update(
                        {
                            "pool_recycle": 3600,
                        }
                    )
                elif database_type == "oracle":
                    pool_config.update(
                        {
                            "pool_recycle": 1800,
                        }
                    )

            if "pool_config" in self.config:
                user_pool_config = self.config["pool_config"]
                pool_config.update(user_pool_config)
                logger.debug("使用用户自定义连接池配置: %s", user_pool_config)

            self.engine = create_engine(connection_url, **pool_config)

            logger.info("数据库连接已建立: %s", self.config.get("type", "unknown"))

        except SQLAlchemyError as error:
            raise DBConnectionError(f"数据库连接失败: {str(error)}") from error
        except Exception as error:
            raise DBConnectionError(f"数据库连接失败: {str(error)}") from error

    def disconnect(self) -> None:
        """断开数据库连接

        释放连接池资源，确保数据库连接被正确关闭，避免资源泄漏。

        Example:
            >>> driver.connect()
            >>> driver.disconnect()
        """
        try:
            if self.engine:
                self.engine.dispose()
                self.engine = None

            logger.info("数据库连接已关闭")

        except SQLAlchemyError as error:
            logger.warning("关闭数据库连接时发生数据库错误: %s", str(error))
        except Exception as error:
            logger.error("关闭数据库连接时发生意外错误: %s", str(error))
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
            logger.warning("连接测试失败: 数据库错误 - %s", str(error))
            return False
        except OSError as error:
            logger.warning("连接测试失败: 网络/I/O错误 - %s", str(error))
            return False
        except (ValueError, TypeError, AttributeError) as error:
            logger.warning("连接测试失败: 配置错误 - %s", str(error))
            return False
        except Exception as error:  # pylint: disable=broad-except
            # 连接探测接口不抛出异常：驱动可能抛出任意类型的异常，
            # 调用方（连接池有效性检查等）依赖 False 表示不可用。
            logger.warning("连接测试失败: 未知错误 - %s", str(error))
            return False

    def _perform_connection_test(self) -> bool:
        """执行连接测试（内部方法）

        执行简单的 SQL 查询来验证数据库连接是否正常。

        查询语句由**方言自己渲染**，而不是硬编码字符串——这正是 GBase 8s 等
        方言按 ``sqlmode`` 补伪表的入口。此前用 ``text("SELECT 1")`` 原样下发，
        绕过了该逻辑，在 gbase/mysql 模式下会因缺少 FROM 而报“发生语法错误”，
        连接测试因此误判为失败。

        渲染方式是 ``expression.select(1).compile(dialect=self.engine.dialect)``，
        **不能**改用方言的 ``_dialect_specific_select_one``：那是 SQLAlchemy 内部
        供 DBAPI 层 ``do_ping()`` 使用的探活字符串（见 ``engine/default.py``），
        并非可执行的完整 SELECT —— psycopg 方言对它就返回 ``";"``，
        当成语句执行会报 “This result object does not return rows”。

        Returns:
            bool: 连接测试是否成功
        """
        if self.engine is None:
            logger.warning("连接测试失败: 数据库引擎未初始化")
            return False

        # 方言未提供编译器时回退到通用写法（自定义方言可能编译失败）
        try:
            test_query = str(select(1).compile(dialect=self.engine.dialect))
        except Exception:  # pylint: disable=broad-except
            test_query = self.TEST_QUERY_DEFAULT

        with self.engine.connect() as conn:
            result = conn.execute(text(test_query))
            result.fetchone()
            return True

    def execute_query(
        self, query: str, parameters: Dict[str, Any] | None = None
    ) -> List[Any]:
        """执行SQL查询语句并返回结果

        执行 SELECT 等查询语句，返回格式化的结果列表，
        支持参数化查询，防止 SQL 注入攻击。

        .. note::
            返回的元素是 SQLAlchemy 的 ``RowMapping``，**不是** ``dict``：
            它有 ``keys()`` / ``values()`` 可当 ``Mapping`` 用，但不是 ``dict``
            子类（``json.dumps`` 会报错）；更关键的是
            **列名重复时用 ``dict()`` / ``items()`` 消费会丢值甚至取到错值**
            （``SELECT * FROM a JOIN b`` 的常态）。

            新代码请优先用 :meth:`execute_query_result` —— 返回按位置保真的
            :class:`QueryResult`，重名列也不会塌陷。本方法为兼容保留。

        Args:
            query: SQL查询语句
            parameters: 查询参数字典，用于参数化查询

        Returns:
            List[Any]: 查询结果列表，每行为 ``RowMapping``（Mapping 接口）

        Raises:
            QueryError: 当查询执行失败时

        Example:
            >>> results = driver.execute_query("SELECT * FROM users")
            >>> for row in results:
            ...     print(row["name"], row["age"])

            >>> results = driver.execute_query(
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
        """
        return self._execute_sql(query, parameters)

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
            >>> affected = driver.execute_command(
            ...     "UPDATE users SET status = 'active' WHERE id = :id",
            ...     {"id": 1}
            ... )
            >>> print(f"更新了 {affected} 行")

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

        def _operation() -> Any:
            if not self.engine:
                self.connect()
            assert self.engine is not None, "数据库引擎应该已经初始化，执行查询"

            self._validate_sql_query(sql)

            with self.engine.connect() as connection:  # type: ignore[union-attr]
                if parameters:
                    sql_result = connection.execute(text(sql), parameters)
                else:
                    sql_result = connection.execute(text(sql))

                if commit:
                    connection.commit()
                    return sql_result.rowcount
                return sql_result.mappings().all()

        # 异常映射与其它查询入口保持一致（见 result_fetch.run_sql）
        return run_sql(_operation)

    def _validate_sql_query(self, query: str) -> None:
        """验证SQL查询语句，防止SQL注入攻击（内部方法）

        校验策略（防注入而非限制语句种类）见
        :mod:`db_connector_tool.drivers.sql_validator`：

        1. 长度上限；
        2. 在剥离字符串字面量与注释后的语句结构上匹配注入特征，合法的
           ``information_schema`` / ``UNION`` 查询与含注入文本的字面量都不误伤；
        3. 拒绝多条语句（语句走私），存储过程/触发器定义除外；
        4. 基于原始 SQL 匹配可疑注释特征。

        显式的 ``DROP`` / ``TRUNCATE`` 等管理语句是允许的（CLI 亦声明支持），
        它们只有以“第二条语句”形式出现时才会被拒绝。

        Args:
            query: SQL查询语句字符串

        Raises:
            ValueError: 当查询语句过长、包含多条语句或命中注入特征时
        """
        validate_sql(query)

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

        except SQLAlchemyError as error:
            raise QueryError(f"获取表列表失败: 数据库错误 - {str(error)}") from error
        except Exception as error:
            raise QueryError(f"获取表列表失败: {str(error)}") from error

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

        except SQLAlchemyError as error:
            raise QueryError(f"获取表结构失败: 数据库错误 - {str(error)}") from error
        except Exception as error:
            raise QueryError(f"获取表结构失败: {str(error)}") from error
