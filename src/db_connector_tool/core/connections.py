"""数据库连接管理模块 (DatabaseManager)

提供统一的数据库连接管理接口，实现连接池管理和生命周期控制。

Example:
>>> from db_connector_tool.core.connections import DatabaseManager
>>> db_manager = DatabaseManager("my_app")
>>> config = {
...     "type": "mysql",
...     "host": "localhost",
...     "port": 3306,
...     "username": "root",
...     "password": "password",
...     "database": "test_db"
... }
>>> db_manager.add_connection("mysql_db", config)
>>> results = db_manager.execute_query("mysql_db", "SELECT * FROM users")
>>> db_manager.close_connection("mysql_db")
>>> with DatabaseManager("my_app") as dbm:
...     dbm.add_connection("postgres_db", {"host": "localhost", "port": 5432})
...     results = dbm.execute_query("postgres_db", "SELECT * FROM products")
"""

import threading
import time
from typing import Any, Dict, List

from ..drivers.sqlalchemy_driver import SQLAlchemyDriver
from ..utils.logging_utils import get_logger
from .config import ConfigManager
from .connection_pool import ConnectionPoolManager
from .exceptions import ConfigError, DatabaseError, DBConnectionError

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class DatabaseManager:
    """数据库管理器类 (Database Manager)

    提供统一的数据库连接管理接口，实现连接池管理和生命周期控制。
    支持上下文管理器协议，可使用 `with` 语句自动管理连接的关闭。

    Attributes:
        app_name (str): 应用名称，用于配置文件的命名空间和日志标识
        config_file (str): 配置文件名，默认为"connections.toml"
        config_manager (ConfigManager): 配置管理器实例
        pool_manager (ConnectionPoolManager): 连接池管理器实例
        _lock (threading.RLock): 可重入锁，确保线程安全

    Example:
        >>> db_manager = DatabaseManager("my_application")
        >>> config = {
        ...     "type": "mysql",
        ...     "host": "localhost",
        ...     "port": 3306,
        ...     "username": "user",
        ...     "password": "pass",
        ...     "database": "test_db"
        ... }
        >>> db_manager.add_connection("mysql_db", config)
        >>> results = db_manager.execute_query("mysql_db", "SELECT * FROM users")
        >>> db_manager.close_connection("mysql_db")
        >>> with DatabaseManager("my_app") as dbm:
        ...     dbm.add_connection("postgres_db", {"host": "localhost", "port": 5432})
        ...     results = dbm.execute_query("postgres_db", "SELECT * FROM products")
        >>> db_manager = DatabaseManager("my_app")
        >>> try:
        ...     db_manager.add_connection("test_db", {"host": "localhost", "port": 5432})
        ... finally:
        ...     db_manager.close_all_connections()
    """

    def __init__(
        self, app_name: str = "db_connector_tool", config_file: str = "connections.toml"
    ) -> None:
        """初始化数据库管理器

        创建新的数据库管理器实例，自动初始化配置系统和连接池。

        Args:
            app_name: 应用名称，用于配置文件的命名空间和日志标识
            config_file: 配置文件名，默认为"connections.toml"

        Raises:
            DatabaseError: 数据库管理器初始化失败
            OSError: 文件系统操作失败

        Example:
            >>> db_manager = DatabaseManager("my_app", "database.toml")
            >>> print(f"应用名称: {db_manager.app_name}")
            >>> db_manager = DatabaseManager()  # 使用默认参数
            >>> with DatabaseManager("my_app") as dbm:
            ...     dbm.add_connection("test_db", {"host": "localhost", "port": 5432})
        """

        try:
            self.app_name = app_name
            self.config_file = config_file
            self.config_manager = ConfigManager(app_name, config_file)
            self.pool_manager = ConnectionPoolManager()
            self._lock = threading.RLock()
            logger.info("数据库管理器初始化成功: %s", app_name)
        except (OSError, DatabaseError) as error:
            logger.error("初始化数据库管理器失败: %s", str(error))
            raise DatabaseError(f"数据库管理器初始化失败: {str(error)}") from error

    def __str__(self) -> str:
        """返回数据库管理器的用户友好字符串表示

        Returns:
            str: 格式为 "DatabaseManager('app_name', N connections)" 的字符串
        """
        try:
            connection_count = len(self.list_connections())
            return f"DatabaseManager('{self.app_name}', {connection_count} connections)"
        except (DatabaseError, ConfigError):
            # 如果获取连接数量失败，返回基本表示
            return f"DatabaseManager('{self.app_name}', '{self.config_file}')"

    def __repr__(self) -> str:
        """返回数据库管理器的详细表示，用于调试

        Returns:
            str: 包含完整配置信息的字符串，用于调试

        Example:
            >>> repr(db_manager)
            "DatabaseManager(app_name='my_app', config_file='connections.toml', \
            connection_count=3, pool_size=5, statistics={...})"
        """

        stats = self.pool_manager.get_statistics()
        # 从配置管理器获取连接名称列表
        connection_names = self.list_connections()

        return (
            f"DatabaseManager(app_name={repr(self.app_name)}, "
            f"config_file={repr(self.config_file)}, "
            f"connection_count={len(connection_names)}, "
            f"connection_names={repr(connection_names)}, "
            f"pool_size={stats['connection_pool_size']}, "
            f"active_connections={stats['active_connections']}, "
            f"connections_created={stats['connections_created']}, "
            f"uptime={stats['uptime']:.2f}s)"
        )

    def __enter__(self):
        """上下文管理器入口，返回自身实例

        Returns:
            DatabaseManager: 当前数据库管理器实例
        """
        return self

    def __exit__(
        self, exc_type: type | None, exc_val: Exception | None, exc_tb: Any | None
    ) -> None:
        """退出上下文管理器，清理所有连接

        Args:
            exc_type: 异常类型（如果有异常发生）
            exc_val: 异常值（如果有异常发生）
            exc_tb: 异常回溯（如果有异常发生）
        """
        self.close_all_connections()
        logger.info("数据库管理器上下文已退出")

    def close_all_connections(self) -> None:
        """安全关闭所有数据库连接

        Raises:
            DatabaseError: 当关闭所有连接失败时

        Example:
            >>> db_manager.close_all_connections()
        """

        try:
            success_count, error_count = self.pool_manager.close_all_connections()
            total_connections = success_count + error_count

            if total_connections == 0:
                logger.info("连接池为空，无需关闭连接")
                return

            logger.info("所有数据库连接已安全关闭，共 %s 个连接", success_count)

        except (OSError, DatabaseError) as error:
            logger.error("关闭所有连接时发生严重异常: %s", str(error))
            raise DatabaseError(f"关闭所有连接失败: {str(error)}") from error

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """添加数据库连接配置

        Args:
            name: 连接名称，用于标识不同的数据库连接
            connection_config: 连接配置字典，包含数据库连接所需的参数

        Raises:
            DatabaseError: 当创建连接配置失败时
            ConfigError: 当连接配置验证失败或连接已存在时

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
            >>> with DatabaseManager("my_app") as dbm:
            ...     dbm.add_connection("postgres_db", {"host": "localhost", "port": 5432})
        """

        def _add_connection():
            # 检查连接是否已存在
            existing_connections = self.list_connections()
            if name in existing_connections:
                raise ConfigError(f"连接配置已存在: {name}")

            # 保存到配置管理器
            self.config_manager.add_config(name, connection_config)
            logger.info("数据库连接配置已创建: %s", name)

        with self._lock:
            self._safe_operation("数据库连接配置创建", name, _add_connection)

    def list_connections(self) -> List[str]:
        """获取所有可用的连接名称

        Returns:
            List[str]: 连接名称列表，按配置文件中的顺序排列

        Raises:
            DatabaseError: 列出连接失败

        Example:
            >>> connections = db_manager.list_connections()
            >>> print(f"可用的连接: {', '.join(connections)}")
        """

        return self.config_manager.list_configs()

    def _safe_operation(self, operation: str, name: str, func, *args, **kwargs) -> Any:
        """安全执行操作的辅助方法，处理异常并转换为适当的错误类型

        Args:
            operation: 操作名称，用于日志和错误消息
            name: 连接名称
            func: 要执行的函数
            *args: 函数位置参数
            **kwargs: 函数关键字参数

        Returns:
            Any: 函数执行的结果

        Raises:
            ConfigError: 如果执行过程中发生ConfigError
            DBConnectionError: 如果执行过程中发生DBConnectionError
            DatabaseError: 其他所有异常都会被转换为DatabaseError
        """

        try:
            return func(*args, **kwargs)
        except (OSError, DatabaseError) as error:
            error_message = f"{operation}失败 {name}: {str(error)}"
            logger.error(error_message)
            raise DatabaseError(f"{operation}失败: {str(error)}") from error

    def remove_connection(self, name: str) -> None:
        """删除连接配置

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当删除连接配置失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> db_manager.remove_connection("mysql_db")
            >>> with DatabaseManager("my_app") as dbm:
            ...     dbm.remove_connection("postgres_db")
        """

        def _remove_connection():
            # 检查连接是否存在
            self._validate_connection_exists(name)

            # 先关闭连接
            self.pool_manager.remove_connection(name)

            # 删除配置
            self.config_manager.remove_config(name)
            logger.info("连接配置已删除: %s", name)

        with self._lock:
            self._safe_operation("连接配置删除", name, _remove_connection)

    def _validate_connection_exists(self, name: str) -> None:
        """验证连接配置是否存在

        Args:
            name: 连接名称

        Raises:
            ConfigError: 当连接配置不存在时
        """

        existing_connections = self.list_connections()
        if name not in existing_connections:
            raise ConfigError(f"连接配置不存在: {name}")

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """更新连接配置

        Args:
            name: 连接名称
            connection_config: 新的连接配置

        Raises:
            DatabaseError: 当更新连接配置失败时
            ConfigError: 当新的连接配置验证失败或连接不存在时

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
            >>> with DatabaseManager("my_app") as dbm:
            ...     dbm.update_connection("postgres_db", {"host": "new_host", "port": 5432})
        """

        def _update_connection():
            # 检查连接是否存在
            self._validate_connection_exists(name)

            # 关闭现有连接
            self.pool_manager.remove_connection(name)

            # 更新配置
            self.config_manager.update_config(name, connection_config)
            logger.info("连接配置已更新: %s", name)

        with self._lock:
            self._safe_operation("连接配置更新", name, _update_connection)

    def show_connection(self, name: str) -> Dict[str, Any]:
        """显示指定连接的配置信息

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 连接配置信息字典

        Raises:
            ConfigError: 当连接配置不存在时

        Example:
            >>> config = db_manager.show_connection("mysql_db")
            >>> print(f"主机: {config['host']}")
            >>> with DatabaseManager("my_app") as dbm:
            ...     config = dbm.show_connection("postgres_db")
        """

        return self.config_manager.get_config(name)

    def get_connection(
        self, name: str, config_overrides: Dict[str, Any] | None = None
    ) -> Any:
        """获取数据库连接（连接池管理）

        Args:
            name: 连接名称
            config_overrides: 可选的配置覆盖字典，用于临时修改连接配置
                            例如：{"host": "127.0.0.1", "port": 3306}

        Returns:
            Any: 数据库驱动实例，用于执行数据库操作

        Raises:
            DatabaseError: 当获取连接失败时
            DBConnectionError: 当连接建立失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> driver = db_manager.get_connection("mysql_db")
            >>> result = driver.execute_query("SELECT * FROM users")
            >>> # 使用配置覆盖
            >>> driver = db_manager.get_connection("mysql_db", {"host": "127.0.0.1"})
            >>> with DatabaseManager("my_app") as dbm:
            ...     driver = dbm.get_connection("postgres_db")
        """

        with self._lock:
            try:
                # 检查连接配置是否存在
                self._validate_connection_exists(name)

                # 处理配置覆盖情况
                if config_overrides:
                    return self._get_connection_with_overrides(name, config_overrides)

                # 处理连接池管理逻辑
                return self._get_connection_from_pool(name)

            except (OSError, DBConnectionError) as error:
                # 使用pool_manager记录错误
                self.pool_manager.record_connection_error(name, error)
                logger.error("获取数据库连接失败 %s: %s", name, str(error))
                raise DBConnectionError(f"数据库连接获取失败: {str(error)}") from error

    def _get_connection_with_overrides(
        self, name: str, config_overrides: Dict[str, Any]
    ) -> Any:
        """使用配置覆盖创建临时连接

        Args:
            name: 连接名称
            config_overrides: 配置覆盖字典

        Returns:
            Any: 数据库驱动实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """

        # 清理可能存在的缓存连接
        self.pool_manager.remove_connection(name)

        # 获取基础配置并应用覆盖
        base_config = self.show_connection(name)
        connection_config = {**base_config, **config_overrides}

        # 根据数据库类型选择合适的驱动
        driver = self._create_driver_for_type(connection_config)


        try:
            driver.connect()
        except (OSError, DBConnectionError) as connect_error:
            logger.error("使用临时配置建立连接失败 %s: %s", name, str(connect_error))
            raise DBConnectionError(
                f"连接建立失败: {str(connect_error)}"
            ) from connect_error

        # 临时配置的连接也加入连接池，但使用特殊标记
        temp_connection_name = f"{name}_temp_{hash(str(config_overrides))}"
        self.pool_manager.add_connection(temp_connection_name, driver)
        logger.info("使用临时配置建立数据库连接: %s (临时连接: %s)", name, temp_connection_name)
        return driver

    def _get_connection_from_pool(self, name: str) -> Any:
        """从连接池获取或创建连接

        Args:
            name: 连接名称

        Returns:
            Any: 数据库驱动实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """

        # 尝试从连接池获取连接
        driver = self.pool_manager.get_connection(name)
        if driver:
            logger.debug("使用缓存的数据库连接: %s", name)
            return driver

        # 创建新连接
        return self._create_new_connection(name)

    def _create_driver_for_type(self, connection_config: Dict[str, Any]) -> Any:
        """根据数据库类型创建相应的驱动实例

        Args:
            connection_config: 连接配置字典

        Returns:
            Any: 数据库驱动实例

        Raises:
            DBConnectionError: 当数据库类型不支持时
        """
        # 根据数据库类型选择合适的驱动
        db_type = connection_config.get("type", "mysql")
        if db_type in ["mysql", "postgresql", "oracle", "sqlserver", "sqlite", "gbase"]:
            from ..drivers.sqlalchemy_driver import SQLAlchemyDriver
            return SQLAlchemyDriver(connection_config)
        else:
            raise DBConnectionError(f"不支持的数据库类型: {db_type}")

    def _create_new_connection(self, name: str) -> Any:
        """创建新的数据库连接

        Args:
            name: 连接名称

        Returns:
            Any: 数据库驱动实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """

        # 创建新连接，处理网络超时和服务不可用
        connection_config = self.show_connection(name)
        
        # 根据数据库类型选择合适的驱动
        driver = self._create_driver_for_type(connection_config)
        
        try:
            driver.connect()
        except (OSError, DBConnectionError) as connect_error:
            self.pool_manager.record_connection_error(name, connect_error)
            logger.error("建立数据库连接失败 %s: %s", name, str(connect_error))
            # 分析错误类型，提供更详细的错误信息
            self._handle_connection_error(connect_error)

        # 加入连接池
        self.pool_manager.add_connection(name, driver)

        logger.info("数据库连接已建立: %s", name)
        return driver

    def _handle_connection_error(self, connect_error: Exception) -> None:
        """处理连接错误，根据错误类型抛出相应的异常

        Args:
            connect_error: 连接错误异常

        Raises:
            DBConnectionError: 根据错误类型抛出相应的异常
        """

        error_msg = str(connect_error).lower()
        if "timeout" in error_msg:
            raise DBConnectionError(
                f"连接超时: {str(connect_error)}"
            ) from connect_error
        if "refused" in error_msg:
            raise DBConnectionError(
                f"连接被拒绝: {str(connect_error)}"
            ) from connect_error
        if "unreachable" in error_msg:
            raise DBConnectionError(
                f"主机不可达: {str(connect_error)}"
            ) from connect_error
        if "permission" in error_msg or "access denied" in error_msg:
            raise DBConnectionError(
                f"连接失败（权限错误）: {str(connect_error)}"
            ) from connect_error
        if "database" in error_msg and "not found" in error_msg:
            raise DBConnectionError(
                f"连接失败（数据库不存在）: {str(connect_error)}"
            ) from connect_error
        raise DBConnectionError(
            f"连接失败（未知错误）: {str(connect_error)}"
        ) from connect_error

    def test_connection(self, name: str) -> bool:
        """测试连接是否有效

        Args:
            name: 连接名称

        Returns:
            bool: True表示连接成功，False表示连接失败

        Example:
            >>> if db_manager.test_connection("mysql_db"):
            ...     print("连接测试成功")
            ... else:
            ...     print("连接测试失败")
            >>> with DatabaseManager("my_app") as dbm:
            ...     success = dbm.test_connection("postgres_db")
        """

        try:
            driver = self.get_connection(name)
            success = driver.test_connection()
            if success:
                logger.info("连接测试成功: %s", name)
            else:
                logger.warning("连接测试失败: %s", name)

            # 测试完成后不立即清理连接，保留在连接池中供后续使用
            # 连接池会通过空闲连接清理机制自动管理

            return success
        except (OSError, DBConnectionError) as connect_error:
            self.pool_manager.record_connection_error(name, connect_error)
            logger.error("连接测试失败 %s: %s", name, str(connect_error))
            # 测试失败时清理连接，避免连接池中有无效连接
            self.pool_manager.remove_connection(name)
            return False

    def execute_query(
        self, connection_name: str, query: str, params: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """执行SQL查询语句

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选

        Returns:
            List[Dict[str, Any]]: 查询结果列表，每行数据为字典格式

        Raises:
            DatabaseError: 当查询执行失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> results = db_manager.execute_query(
            ...     "mysql_db",
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
            >>> with DatabaseManager("my_app") as dbm:
            ...     results = dbm.execute_query("postgres_db", "SELECT * FROM products")
        """

        def _execute_query():
            driver = self.get_connection(connection_name)
            # 记录查询开始时间
            start_time = time.time()
            # 执行查询
            result = driver.execute_query(query, params)
            # 计算响应时间
            response_time = time.time() - start_time

            # 更新连接元数据
            self.pool_manager.update_query_metadata(connection_name, response_time)

            return result

        try:
            return _execute_query()
        except (OSError, DatabaseError) as error:
            # 记录错误信息
            self.pool_manager.record_connection_error(connection_name, error)
            error_message = f"查询执行失败 {connection_name}: {str(error)}"
            logger.error(error_message)
            raise DatabaseError(f"查询执行失败: {str(error)}") from error

    def execute_command(
        self,
        connection_name: str,
        command: str,
        params: Dict[str, Any] | None = None,
    ) -> int:
        """执行非查询SQL命令（INSERT/UPDATE/DELETE等）

        Args:
            connection_name: 连接名称
            command: SQL命令语句
            params: 命令参数字典，可选

        Returns:
            int: 影响的行数

        Raises:
            DatabaseError: 当命令执行失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> affected_rows = db_manager.execute_command(
            ...     "mysql_db",
            ...     "UPDATE users SET name = :name WHERE id = :id",
            ...     {"name": "Bob", "id": 1}
            ... )
            >>> with DatabaseManager("my_app") as dbm:
            ...     affected_rows = dbm.execute_command(
            ...         "postgres_db",
            ...         "DELETE FROM products WHERE id = 1"
            ...     )
        """

        def _execute_command():
            driver = self.get_connection(connection_name)
            # 记录命令开始时间
            start_time = time.time()
            # 执行命令
            result = driver.execute_command(command, params)
            # 计算响应时间
            response_time = time.time() - start_time

            # 更新连接元数据
            self.pool_manager.update_command_metadata(connection_name, response_time)

            return result

        try:
            return _execute_command()
        except (OSError, DatabaseError) as error:
            # 记录错误信息
            self.pool_manager.record_connection_error(connection_name, error)
            error_message = f"命令执行失败 {connection_name}: {str(error)}"
            logger.error(error_message)
            raise DatabaseError(f"命令执行失败: {str(error)}") from error

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """获取连接详细信息（包含统计信息）

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 连接信息字典，包含基本配置和统计信息

        Raises:
            DatabaseError: 当获取连接信息失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> info = db_manager.get_connection_info("mysql_db")
            >>> print(f"使用次数: {info['use_count']}")
            >>> print(f"最后使用时间: {info['last_used']}")
            >>> print(f"错误次数: {info['connection_errors']}")
            >>> with DatabaseManager("my_app") as dbm:
            ...     info = dbm.get_connection_info("postgres_db")
        """

        def _get_connection_info():
            # 检查连接是否存在
            self._validate_connection_exists(name)

            # 获取基本配置信息
            config = self.show_connection(name)
            info: Dict[str, Any] = {
                "type": config.get("type"),
                "host": config.get("host"),
                "port": config.get("port"),
                "database": config.get("database"),
            }

            # 添加连接池统计信息
            pool_info = self.pool_manager.get_connection_info(name)
            info.update(pool_info)

            return {k: v for k, v in info.items() if v is not None}

        with self._lock:
            return self._safe_operation("连接信息获取", name, _get_connection_info)

    def cleanup_idle_connections(self, max_idle_time: int = 300) -> int:
        """清理空闲时间过长的连接

        Args:
            max_idle_time: 最大空闲时间（秒），默认5分钟

        Returns:
            int: 清理的连接数量

        Raises:
            DatabaseError: 当清理过程发生严重错误时

        Example:
            >>> cleaned_count = db_manager.cleanup_idle_connections(600)  # 10分钟
            >>> print(f"清理了 {cleaned_count} 个空闲连接")
            >>> with DatabaseManager("my_app") as dbm:
            ...     cleaned_count = dbm.cleanup_idle_connections()
        """

        with self._lock:
            try:
                cleaned_count = self.pool_manager.cleanup_idle_connections(
                    max_idle_time
                )
                return cleaned_count
            except (OSError, DatabaseError) as error:
                logger.error("清理空闲连接失败: %s", str(error))
                raise DatabaseError(f"清理空闲连接失败: {str(error)}") from error

    def diagnose_connection(self, name: str) -> Dict[str, Any]:
        """诊断连接问题，提供详细的连接诊断信息

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 连接诊断信息字典

        Example:
            >>> diagnosis = db_manager.diagnose_connection("mysql_db")
            >>> print(f"诊断结果: {diagnosis['status']}")
            >>> print(f"详细信息: {diagnosis['details']}")
            >>> with DatabaseManager("my_app") as dbm:
            ...     diagnosis = dbm.diagnose_connection("postgres_db")
        """

        diagnosis: Dict[str, Any] = {
            "connection_name": name,
            "status": "unknown",
            "details": {},
            "timestamp": time.time(),
        }

        try:
            # 1. 检查连接配置
            self._diagnose_config(name, diagnosis)

            # 2. 尝试获取连接
            self._diagnose_connection(name, diagnosis)

        except ConfigError as error:
            self._diagnose_config_error(name, diagnosis, error)
        except (OSError, DBConnectionError) as error:
            self._diagnose_general_error(name, diagnosis, error)

        # 5. 添加连接池信息
        self._add_pool_info(name, diagnosis)

        return diagnosis

    def _diagnose_config(self, name: str, diagnosis: Dict[str, Any]) -> None:
        """诊断连接配置

        Args:
            name: 连接名称
            diagnosis: 诊断信息字典
        """

        config = self.show_connection(name)
        diagnosis["details"]["config"] = {
            "valid": True,
            "type": config.get("type"),
            "host": config.get("host"),
            "port": config.get("port"),
            "database": config.get("database"),
        }

    def _diagnose_connection(self, name: str, diagnosis: Dict[str, Any]) -> None:
        """诊断连接

        Args:
            name: 连接名称
            diagnosis: 诊断信息字典
        """

        try:
            driver = self.get_connection(name)
            diagnosis["details"]["connection"] = {
                "established": True,
                "driver_type": type(driver).__name__,
            }

            # 3. 测试连接
            self._diagnose_connection_test(driver, diagnosis)

            # 4. 清理连接
            self.pool_manager.remove_connection(name)
        except (OSError, DBConnectionError) as connect_error:
            diagnosis["status"] = "unhealthy"
            diagnosis["details"]["connection"] = {
                "established": False,
                "error": str(connect_error),
            }

    def _diagnose_connection_test(
        self, driver: Any, diagnosis: Dict[str, Any]
    ) -> None:
        """诊断连接测试

        Args:
            driver: 数据库驱动实例
            diagnosis: 诊断信息字典
        """

        try:
            test_result = driver.test_connection()
            diagnosis["details"]["test"] = {
                "success": test_result,
            }
            if test_result:
                diagnosis["status"] = "healthy"
            else:
                diagnosis["status"] = "unhealthy"
                diagnosis["details"]["test"]["message"] = "连接测试失败"
        except (OSError, DBConnectionError) as test_error:
            diagnosis["status"] = "unhealthy"
            diagnosis["details"]["test"] = {
                "success": False,
                "error": str(test_error),
            }

    def _diagnose_config_error(
        self, _name: str, diagnosis: Dict[str, Any], error: ConfigError
    ) -> None:
        """诊断配置错误

        Args:
            _name: 连接名称（未使用）
            diagnosis: 诊断信息字典
            error: 配置错误
        """

        diagnosis["status"] = "error"
        diagnosis["details"]["config"] = {
            "valid": False,
            "error": str(error),
        }

    def _diagnose_general_error(
        self, _name: str, diagnosis: Dict[str, Any], error: Exception
    ) -> None:
        """诊断一般错误

        Args:
            _name: 连接名称（未使用）
            diagnosis: 诊断信息字典
            error: 错误对象
        """

        diagnosis["status"] = "error"
        diagnosis["details"]["general_error"] = str(error)

    def _add_pool_info(self, name: str, diagnosis: Dict[str, Any]) -> None:
        """添加连接池信息

        Args:
            name: 连接名称
            diagnosis: 诊断信息字典
        """

        pool_info = self.pool_manager.get_connection_info(name)
        if pool_info:
            diagnosis["details"]["pool_info"] = {
                "is_active": True,
                "use_count": pool_info.get("use_count", 0),
                "connection_errors": pool_info.get("connection_errors", 0),
                "last_error": pool_info.get("last_error"),
                "response_time": pool_info.get("response_time", 0.0),
            }
        else:
            diagnosis["details"]["pool_info"] = {
                "is_active": False,
                "use_count": 0,
                "connection_errors": 0,
                "last_error": None,
                "response_time": 0.0,
            }
