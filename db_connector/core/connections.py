"""
数据库管理器主模块

提供统一的数据库连接管理接口，支持多种数据库类型：
- Oracle
- PostgreSQL
- MySQL
- SQL Server
- SQLite

该模块封装了连接配置管理、连接池管理、查询执行等功能。
"""

from typing import Any, Dict, List, Optional

from ..drivers.sqlalchemy_driver import SQLAlchemyDriver
from ..utils.logging_utils import get_logger
from .config import ConfigManager
from .exceptions import ConfigError, ConnectionError, DatabaseError

logger = get_logger(__name__)

# 错误消息常量
PORT_MUST_BE_INTEGER_MSG = "port 参数必须是整数"


class DatabaseManager:
    """
    数据库管理器类

    提供统一的数据库连接管理功能，包括：
    - 连接配置的创建、更新、删除
    - 数据库连接的建立、测试、关闭
    - 查询和命令的执行
    - 连接池管理

    Attributes:
        app_name (str): 应用名称
        config_manager (ConfigManager): 配置管理器实例
        connections (Dict[str, SQLAlchemyDriver]): 活跃连接缓存
    """

    def __init__(self, app_name: str = "db_connector") -> None:
        """
        初始化数据库管理器

        Args:
            app_name: 应用名称，用于配置文件的命名空间

        Example:
            >>> db_manager = DatabaseManager("my_app")
        """
        self.app_name = app_name
        self.config_manager = ConfigManager(app_name)
        self.connections: Dict[str, SQLAlchemyDriver] = {}

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        创建数据库连接配置

        Args:
            name: 连接名称，用于标识不同的数据库连接
            connection_config: 连接配置字典，包含数据库类型、主机、端口等参数

        Raises:
            DatabaseError: 当创建连接配置失败时
            ConfigError: 当连接配置验证失败时

        Example:
            >>> config = {
            ...     "type": "mysql",
            ...     "host": "localhost",
            ...     "port": 3306,
            ...     "username": "root",
            ...     "password": "password",
            ...     "database": "test_db"
            ... }
            >>> db_manager.add_connection("mysql_db", config)
        """
        try:
            # 验证必需的配置项
            self._validate_connection_config(connection_config)

            # 保存到配置
            self.config_manager.add_connection(name, connection_config)
            logger.info(f"数据库连接配置已创建: {name}")

        except Exception as e:
            logger.error(f"创建连接配置失败 {name}: {str(e)}")
            if isinstance(e, (ConfigError, DatabaseError)):
                raise
            raise DatabaseError(f"创建连接配置失败: {str(e)}")

    def _validate_connection_config(self, config: Dict[str, Any]) -> None:
        """
        验证连接配置的完整性和有效性

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当配置验证失败时
        """
        required_fields = ["type"]
        missing_fields = [field for field in required_fields if field not in config]

        if missing_fields:
            raise ConfigError(f"缺少必需的连接参数: {', '.join(missing_fields)}")

        db_type = config["type"].lower()
        if db_type not in ["oracle", "postgresql", "mysql", "mssql", "sqlite"]:
            raise ConfigError(f"不支持的数据库类型: {db_type}")

        # SQLite不需要主机、用户名、密码
        if db_type != "sqlite":
            db_required = ["host", "username", "password", "database"]
            db_missing = [field for field in db_required if field not in config]
            if db_missing:
                raise ConfigError(
                    f"数据库类型 {db_type} 需要参数: {', '.join(db_missing)}"
                )

        # 验证数据库特定的可选参数
        self._validate_database_specific_params(db_type, config)

    def _validate_database_specific_params(
        self, db_type: str, config: Dict[str, Any]
    ) -> None:
        """
        验证数据库特定的可选参数

        Args:
            db_type: 数据库类型
            config: 连接配置字典
        """
        # 定义各数据库支持的自定义参数
        supported_params: Dict[str, List[str]] = {
            "mssql": ["charset", "tds_version", "driver", "trusted_connection", "port"],
            "mysql": ["charset", "collation", "ssl_ca", "ssl_cert", "ssl_key", "port"],
            "postgresql": [
                "sslmode",
                "sslrootcert",
                "sslcert",
                "sslkey",
                "port",
                "connect_timeout",
            ],
            "oracle": ["service_name", "sid", "port", "mode", "threaded"],
            "sqlite": ["timeout", "isolation_level", "check_same_thread"],
        }

        # 检查是否有不支持的参数
        base_params = ["type", "host", "username", "password", "database"]
        unsupported_params = []
        for param in config.keys():
            if param not in base_params + supported_params.get(db_type, []):
                unsupported_params.append(param)

        if unsupported_params:
            logger.warning(
                f"数据库类型 {db_type} 不支持以下参数，将被忽略: {', '.join(unsupported_params)}"
            )

        # 验证特定数据库的参数值
        if db_type == "mssql":
            self._validate_mssql_params(config)
        elif db_type == "mysql":
            self._validate_mysql_params(config)
        elif db_type == "postgresql":
            self._validate_postgresql_params(config)
        elif db_type == "oracle":
            self._validate_oracle_params(config)
        elif db_type == "sqlite":
            self._validate_sqlite_params(config)

    def _validate_mssql_params(self, config: Dict[str, Any]) -> None:
        """验证 SQL Server 特定参数"""
        if "charset" in config and not isinstance(config["charset"], str):
            raise ConfigError("charset 参数必须是字符串")

        if "tds_version" in config:
            valid_versions = ["7.0", "7.1", "7.2", "7.3", "7.4", "8.0"]
            if config["tds_version"] not in valid_versions:
                raise ConfigError(
                    f"不支持的 TDS 版本: {config['tds_version']}，支持的版本: {', '.join(valid_versions)}"
                )

        if "driver" in config and not isinstance(config["driver"], str):
            raise ConfigError("driver 参数必须是字符串")

        if "trusted_connection" in config and not isinstance(
            config["trusted_connection"], bool
        ):
            raise ConfigError("trusted_connection 参数必须是布尔值")

    def _validate_mysql_params(self, config: Dict[str, Any]) -> None:
        """验证 MySQL 特定参数"""
        if "charset" in config and not isinstance(config["charset"], str):
            raise ConfigError("charset 参数必须是字符串")

        if "collation" in config and not isinstance(config["collation"], str):
            raise ConfigError("collation 参数必须是字符串")

        if "port" in config and not isinstance(config["port"], int):
            raise ConfigError(PORT_MUST_BE_INTEGER_MSG)

    def _validate_postgresql_params(self, config: Dict[str, Any]) -> None:
        """验证 PostgreSQL 特定参数"""
        if "sslmode" in config:
            valid_modes = [
                "disable",
                "allow",
                "prefer",
                "require",
                "verify-ca",
                "verify-full",
            ]
            if config["sslmode"] not in valid_modes:
                raise ConfigError(
                    f"不支持的 SSL 模式: {config['sslmode']}，支持的模式: {', '.join(valid_modes)}"
                )

        if "port" in config and not isinstance(config["port"], int):
            raise ConfigError(PORT_MUST_BE_INTEGER_MSG)

        if "connect_timeout" in config and not isinstance(
            config["connect_timeout"], int
        ):
            raise ConfigError("connect_timeout 参数必须是整数")

    def _validate_oracle_params(self, config: Dict[str, Any]) -> None:
        """验证 Oracle 特定参数"""
        if "port" in config and not isinstance(config["port"], int):
            raise ConfigError(PORT_MUST_BE_INTEGER_MSG)

        if "service_name" in config and not isinstance(config["service_name"], str):
            raise ConfigError("service_name 参数必须是字符串")

        if "sid" in config and not isinstance(config["sid"], str):
            raise ConfigError("sid 参数必须是字符串")

    def _validate_sqlite_params(self, config: Dict[str, Any]) -> None:
        """验证 SQLite 特定参数"""
        if "timeout" in config and not isinstance(config["timeout"], (int, float)):
            raise ConfigError("timeout 参数必须是数字")

        if "isolation_level" in config:
            valid_levels = ["DEFERRED", "IMMEDIATE", "EXCLUSIVE", None]
            if config["isolation_level"] not in valid_levels:
                raise ConfigError(f"不支持的隔离级别: {config['isolation_level']}")

    def get_connection(self, name: str) -> SQLAlchemyDriver:
        """
        获取数据库连接

        Args:
            name: 连接名称

        Returns:
            SQLAlchemyDriver实例，用于执行数据库操作

        Raises:
            DatabaseError: 当获取连接失败时
            ConnectionError: 当连接建立失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> driver = db_manager.get_connection("mysql_db")
            >>> result = driver.execute_query("SELECT * FROM users")
        """
        try:
            # 如果连接已存在且有效，直接返回
            if name in self.connections:
                driver = self.connections[name]
                if driver.is_connected and driver.test_connection():
                    logger.debug(f"使用缓存的数据库连接: {name}")
                    return driver

            # 获取连接配置
            connection_config = self.config_manager.get_connection(name)

            # 创建新的驱动实例
            driver = SQLAlchemyDriver(connection_config)
            driver.connect()

            # 缓存连接
            self.connections[name] = driver

            logger.info(f"数据库连接已建立: {name}")
            return driver

        except Exception as e:
            logger.error(f"获取数据库连接失败 {name}: {str(e)}")
            if isinstance(e, (ConnectionError, ConfigError)):
                raise
            raise DatabaseError(f"获取数据库连接失败: {str(e)}")

    def list_connections(self) -> List[str]:
        """
        获取所有连接名称

        Returns:
            连接名称列表

        Example:
            >>> connections = db_manager.list_connections()
            >>> print(f"可用的连接: {', '.join(connections)}")
        """
        return self.config_manager.list_connections()

    def remove_connection(self, name: str) -> None:
        """
        删除连接配置

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当删除连接配置失败时

        Example:
            >>> db_manager.remove_connection("mysql_db")
        """
        try:
            # 先关闭连接
            if name in self.connections:
                self.close_connection(name)

            # 删除配置
            self.config_manager.remove_connection(name)
            logger.info(f"连接配置已删除: {name}")

        except Exception as e:
            logger.error(f"删除连接配置失败 {name}: {str(e)}")
            raise DatabaseError(f"删除连接配置失败: {str(e)}")

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        更新连接配置

        Args:
            name: 连接名称
            connection_config: 新的连接配置

        Raises:
            DatabaseError: 当更新连接配置失败时
            ConfigError: 当新的连接配置验证失败时

        Example:
            >>> new_config = {
            ...     "type": "mysql",
            ...     "host": "new_host",
            ...     "port": 3306,
            ...     "username": "root",
            ...     "password": "new_password",
            ...     "database": "test_db"
            ... }
            >>> db_manager.update_connection("mysql_db", new_config)
        """
        try:
            # 验证新的连接配置
            self._validate_connection_config(connection_config)

            # 检查连接是否存在
            existing_connections = self.config_manager.list_connections()
            if name not in existing_connections:
                raise ConfigError(f"连接配置不存在: {name}")

            # 如果连接已打开，先关闭它
            if name in self.connections:
                self.close_connection(name)

            # 更新配置
            self.config_manager.update_connection(name, connection_config)
            logger.info(f"连接配置已更新: {name}")

        except Exception as e:
            logger.error(f"更新连接配置失败 {name}: {str(e)}")
            if isinstance(e, (ConfigError, ConnectionError)):
                raise
            raise DatabaseError(f"更新连接配置失败: {str(e)}")

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """
        获取连接信息（不包含敏感信息）

        Args:
            name: 连接名称

        Returns:
            连接信息字典，包含数据库类型、主机、端口等

        Raises:
            DatabaseError: 当获取连接信息失败时

        Example:
            >>> info = db_manager.get_connection_info("mysql_db")
            >>> print(f"数据库类型: {info['type']}")
        """
        try:
            if name in self.connections:
                return self.connections[name].get_connection_info()
            else:
                config = self.config_manager.get_connection(name)
                # 过滤敏感信息
                info: Dict[str, Any] = {
                    "type": config.get("type"),
                    "host": config.get("host"),
                    "port": config.get("port"),
                    "database": config.get("database"),
                }
                return {k: v for k, v in info.items() if v is not None}
        except Exception as e:
            logger.error(f"获取连接信息失败 {name}: {str(e)}")
            raise DatabaseError(f"获取连接信息失败: {str(e)}")

    def test_connection(self, name: str) -> bool:
        """
        测试连接是否有效

        Args:
            name: 连接名称

        Returns:
            True表示连接成功，False表示连接失败

        Example:
            >>> if db_manager.test_connection("mysql_db"):
            ...     print("连接测试成功")
            ... else:
            ...     print("连接测试失败")
        """
        try:
            driver = self.get_connection(name)
            return driver.test_connection()
        except Exception as e:
            logger.error(f"连接测试失败 {name}: {str(e)}")
            return False

    def execute_query(
        self, connection_name: str, query: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        执行查询语句

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选

        Returns:
            查询结果列表，每行数据为字典格式

        Raises:
            DatabaseError: 当查询执行失败时

        Example:
            >>> results = db_manager.execute_query(
            ...     "mysql_db",
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
        """
        try:
            driver = self.get_connection(connection_name)
            return driver.execute_query(query, params)
        except Exception as e:
            logger.error(f"执行查询失败: {str(e)}")
            raise DatabaseError(f"执行查询失败: {str(e)}")

    def execute_command(
        self,
        connection_name: str,
        command: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        执行非查询命令（INSERT/UPDATE/DELETE等）

        Args:
            connection_name: 连接名称
            command: SQL命令语句
            params: 命令参数字典，可选

        Returns:
            影响的行数

        Raises:
            DatabaseError: 当命令执行失败时

        Example:
            >>> affected_rows = db_manager.execute_command(
            ...     "mysql_db",
            ...     "UPDATE users SET name = :name WHERE id = :id",
            ...     {"name": "Bob", "id": 1}
            ... )
        """
        try:
            driver = self.get_connection(connection_name)
            return driver.execute_command(command, params)
        except Exception as e:
            logger.error(f"执行命令失败: {str(e)}")
            raise DatabaseError(f"执行命令失败: {str(e)}")

    def close_connection(self, name: str) -> None:
        """
        关闭数据库连接

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当关闭连接失败时

        Example:
            >>> db_manager.close_connection("mysql_db")
        """
        try:
            if name in self.connections:
                self.connections[name].disconnect()
                del self.connections[name]
                logger.info(f"数据库连接已关闭: {name}")
        except Exception as e:
            logger.error(f"关闭连接失败 {name}: {str(e)}")
            raise DatabaseError(f"关闭连接失败: {str(e)}")

    def close_all_connections(self) -> None:
        """
        关闭所有数据库连接

        清理所有活跃的连接资源。

        Raises:
            DatabaseError: 当关闭所有连接失败时

        Example:
            >>> db_manager.close_all_connections()
        """
        try:
            connection_names = list(self.connections.keys())
            for name in connection_names:
                self.close_connection(name)
            logger.info("所有数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭所有连接失败: {str(e)}")
            raise DatabaseError(f"关闭所有连接失败: {str(e)}")

    def __del__(self) -> None:
        """
        析构函数

        在对象销毁时自动关闭所有数据库连接。
        """
        try:
            self.close_all_connections()
        except Exception as e:
            # 记录析构时的异常，避免程序崩溃但保留调试信息
            logger.warning(f"析构时关闭所有连接失败: {e}")
