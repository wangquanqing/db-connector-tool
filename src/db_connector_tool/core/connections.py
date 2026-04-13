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
    """

    def __init__(
        self, app_name: str = "db_connector_tool", config_file: str = "connections.toml"
    ) -> None:
        """初始化数据库管理器

        Args:
            app_name: 应用名称，用于配置文件的命名空间和日志标识
            config_file: 配置文件名，默认为"connections.toml"

        Raises:
            ConfigError: 当配置管理器初始化失败时

        Example:
            >>> db_manager = DatabaseManager("my_app", "database.toml")
            >>> print(f"应用名称: {db_manager.app_name}")
            >>> db_manager = DatabaseManager()  # 使用默认参数
        """

        try:
            self.app_name = app_name
            self.config_file = config_file
            self.config_manager = ConfigManager(app_name, config_file)
            self.pool_manager = ConnectionPoolManager()
            self._lock = threading.RLock()
            logger.info("数据库管理器初始化成功: %s", app_name)
        except (ConfigError, OSError) as e:
            logger.error("初始化数据库管理器失败: %s", str(e))
            raise ConfigError(f"数据库管理器初始化失败: {str(e)}") from e

    def __str__(self) -> str:
        """返回数据库管理器的字符串表示

        Returns:
            str: 格式化的数据库管理器字符串表示
        """
        stats = self.pool_manager.get_statistics()
        return (
            f"DatabaseManager(app_name='{self.app_name}', "
            f"active_connections={stats['active_connections']}, "
            f"pool_size={stats['connection_pool_size']})"
        )

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
            f"total_connections_created={stats['total_connections_created']}, "
            f"uptime={stats['uptime']:.2f}s)"
        )

    def list_connections(self) -> List[str]:
        """获取所有可用的连接名称

        Returns:
            List[str]: 连接名称列表

        Example:
            >>> connections = db_manager.list_connections()
            >>> print(f"可用的连接: {', '.join(connections)}")
        """

        return self.config_manager.list_configs()

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

    def _safe_operation(self, operation: str, name: str, func, *args, **kwargs):
        """安全执行操作的辅助方法，处理异常并转换为适当的错误类型

        Args:
            operation: 操作名称，用于日志和错误消息
            name: 连接名称
            func: 要执行的函数
            *args: 函数位置参数
            **kwargs: 函数关键字参数

        Returns:
            函数执行的结果

        Raises:
            ConfigError: 如果执行过程中发生ConfigError
            DBConnectionError: 如果执行过程中发生DBConnectionError
            DatabaseError: 其他所有异常都会被转换为DatabaseError
        """

        try:
            return func(*args, **kwargs)
        except (OSError, DatabaseError) as e:
            self._handle_exception(operation, name, e)
            # 确保即使_handle_exception没有抛出异常，也会抛出异常
            raise DatabaseError(f"{operation}失败") from e

    def _handle_exception(
        self, operation: str, name: str, exception: Exception
    ) -> None:
        """通用异常处理方法

        Args:
            operation: 操作名称，用于日志记录
            name: 连接名称
            exception: 捕获到的异常

        Raises:
            ConfigError: 如果原始异常是ConfigError
            DBConnectionError: 如果原始异常是DBConnectionError
            DatabaseError: 其他所有异常都会被转换为DatabaseError
        """

        if isinstance(exception, (ConfigError)):
            raise exception

        error_message = f"{operation}失败 {name}: {str(exception)}"
        logger.error(error_message)
        raise DatabaseError(f"{operation}失败: {str(exception)}")

    def remove_connection(self, name: str) -> None:
        """删除连接配置

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当删除连接配置失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> db_manager.remove_connection("mysql_db")
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
        """

        return self.config_manager.get_config(name)

    def get_connection(
        self, name: str, config_overrides: Dict[str, Any] | None = None
    ) -> SQLAlchemyDriver:
        """获取数据库连接（连接池管理）

        Args:
            name: 连接名称
            config_overrides: 可选的配置覆盖字典，用于临时修改连接配置
                            例如：{"host": "127.0.0.1", "port": 3306}

        Returns:
            SQLAlchemyDriver: SQLAlchemyDriver实例，用于执行数据库操作

        Raises:
            DatabaseError: 当获取连接失败时
            DBConnectionError: 当连接建立失败时
            ConfigError: 当连接配置不存在时

        Example:
            >>> driver = db_manager.get_connection("mysql_db")
            >>> result = driver.execute_query("SELECT * FROM users")
            >>> # 使用配置覆盖
            >>> driver = db_manager.get_connection("mysql_db", {"host": "127.0.0.1"})
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

            except (OSError, DatabaseError) as e:
                # 使用pool_manager记录错误
                self.pool_manager.record_connection_error(name, e)
                logger.error("获取数据库连接失败 %s: %s", name, str(e))
                raise DatabaseError(f"数据库连接获取失败: {str(e)}") from e

    def _get_connection_with_overrides(
        self, name: str, config_overrides: Dict[str, Any]
    ) -> SQLAlchemyDriver:
        """使用配置覆盖创建临时连接

        Args:
            name: 连接名称
            config_overrides: 配置覆盖字典

        Returns:
            SQLAlchemyDriver: SQLAlchemyDriver实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """

        # 清理可能存在的缓存连接
        if self.pool_manager.get_connection(name):
            self.pool_manager.remove_connection(name)

        # 获取基础配置并应用覆盖
        base_config = self.show_connection(name)
        connection_config = {**base_config, **config_overrides}

        # 创建新连接，处理连接超时
        driver = SQLAlchemyDriver(connection_config)
        try:
            driver.connect()
        except (OSError, DatabaseError) as connect_error:
            logger.error("使用临时配置建立连接失败 %s: %s", name, str(connect_error))
            raise DBConnectionError(
                f"连接建立失败: {str(connect_error)}"
            ) from connect_error

        # 注意：临时配置的连接不加入连接池，避免配置冲突
        logger.info("使用临时配置建立数据库连接: %s", name)
        return driver

    def _get_connection_from_pool(self, name: str) -> SQLAlchemyDriver:
        """从连接池获取或创建连接

        Args:
            name: 连接名称

        Returns:
            SQLAlchemyDriver: SQLAlchemyDriver实例

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

    def _is_connection_valid(self, driver: SQLAlchemyDriver) -> bool:
        """全面的连接有效性检查

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 连接是否有效
        """

        try:
            # 检查驱动实例的基本状态
            if not self._check_driver_basic_status(driver):
                return False

            # 执行实际查询测试
            return self._test_connection_query(driver)

        except (OSError, DatabaseError) as e:
            logger.debug("连接有效性检查失败: %s", str(e))
            return False

    def _check_driver_basic_status(self, driver: SQLAlchemyDriver) -> bool:
        """检查驱动实例的基本状态

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 驱动实例状态是否有效
        """

        # 检查驱动实例是否有engine属性
        if not hasattr(driver, "engine"):
            logger.debug("驱动实例缺少engine属性")
            return False

        # 检查engine是否存在
        if not driver.engine:
            logger.debug("驱动实例标记为未连接状态")
            return False

        return True

    def _test_connection_query(self, driver: SQLAlchemyDriver) -> bool:
        """执行连接查询测试

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 查询测试是否成功
        """

        try:
            return driver.test_connection()
        except (OSError, DatabaseError) as query_error:
            logger.debug("连接查询测试失败: %s", str(query_error))
            return False

    def _create_new_connection(self, name: str) -> SQLAlchemyDriver:
        """创建新的数据库连接

        Args:
            name: 连接名称

        Returns:
            SQLAlchemyDriver: SQLAlchemyDriver实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """

        # 创建新连接，处理网络超时和服务不可用
        connection_config = self.show_connection(name)
        driver = SQLAlchemyDriver(connection_config)
        try:
            driver.connect()
        except (OSError, DatabaseError) as connect_error:
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
        raise DBConnectionError(
            f"连接建立失败: {str(connect_error)}"
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
        """

        try:
            driver = self.get_connection(name)
            success = driver.test_connection()
            self._log_test_result(name, success)

            # 测试完成后立即清理连接，避免连接池污染
            self.pool_manager.remove_connection(name)

            return success
        except DBConnectionError as e:
            # 连接错误，已经在get_connection中处理过
            self._handle_test_exception(name, e)
            return False
        except ConfigError as e:
            # 配置错误
            logger.error("连接测试失败（配置错误） %s: %s", name, str(e))
            return False
        except (OSError, DatabaseError) as e:
            # 确保异常情况下也清理连接
            self.pool_manager.remove_connection(name)
            # 分析错误类型，提供更详细的错误信息
            self._log_test_error(name, e)
            return False

    def _log_test_result(self, name: str, success: bool) -> None:
        """记录测试结果

        Args:
            name: 连接名称
            success: 测试是否成功
        """

        if success:
            logger.info("连接测试成功: %s", name)
        else:
            logger.warning("连接测试失败: %s", name)

    def _handle_test_exception(self, name: str, exception: Exception) -> None:
        """处理测试异常

        Args:
            name: 连接名称
            exception: 异常对象
        """

        # 确保异常情况下也清理连接
        self.pool_manager.remove_connection(name)
        logger.error("连接测试失败 %s: %s", name, str(exception))

    def _log_test_error(self, name: str, error: Exception) -> None:
        """记录测试错误，根据错误类型提供详细信息

        Args:
            name: 连接名称
            error: 错误对象
        """

        error_message = str(error)
        error_lower = error_message.lower()

        if "timeout" in error_lower:
            logger.error("连接测试失败（连接超时） %s: %s", name, error_message)
        elif "refused" in error_lower:
            logger.error("连接测试失败（连接被拒绝） %s: %s", name, error_message)
        elif "unreachable" in error_lower:
            logger.error("连接测试失败（主机不可达） %s: %s", name, error_message)
        elif "permission" in error_lower or "access denied" in error_lower:
            logger.error("连接测试失败（权限错误） %s: %s", name, error_message)
        elif "database" in error_lower and "not found" in error_lower:
            logger.error("连接测试失败（数据库不存在） %s: %s", name, error_message)
        else:
            logger.error("连接测试失败（未知错误） %s: %s", name, error_message)

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
        except (OSError, DatabaseError) as e:
            # 记录错误信息
            self.pool_manager.record_connection_error(connection_name, e)
            self._handle_exception("查询执行", connection_name, e)
            # 确保即使_handle_exception没有抛出异常，也会抛出异常
            raise DatabaseError("查询执行失败") from e

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
        except (OSError, DatabaseError) as e:
            # 记录错误信息
            self.pool_manager.record_connection_error(connection_name, e)
            self._handle_exception("命令执行", connection_name, e)
            # 确保即使_handle_exception没有抛出异常，也会抛出异常
            raise DatabaseError("命令执行失败") from e

    def close_connection(self, name: str) -> None:
        """关闭数据库连接

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当关闭连接失败时

        Example:
            >>> db_manager.close_connection("mysql_db")
        """

        def _close_connection():
            self.pool_manager.remove_connection(name)
            logger.info("数据库连接已关闭: %s", name)

        with self._lock:
            self._safe_operation("连接关闭", name, _close_connection)

    def close_all_connections(self) -> None:
        """安全关闭所有数据库连接

        Raises:
            DatabaseError: 当关闭所有连接失败时

        Example:
            >>> db_manager.close_all_connections()
        """

        with self._lock:
            try:
                success_count, error_count = self.pool_manager.close_all_connections()
                total_connections = success_count + error_count

                if total_connections == 0:
                    logger.info("连接池为空，无需关闭连接")
                    return

                # 记录详细汇总信息
                if error_count > 0:
                    logger.warning(
                        "关闭所有连接完成，成功: %s, 失败: %s, 总数: %s",
                        success_count,
                        error_count,
                        total_connections,
                    )
                else:
                    logger.info("所有数据库连接已安全关闭，共 %s 个连接", success_count)

            except (OSError, DatabaseError) as e:
                logger.error("关闭所有连接时发生严重异常: %s", str(e))
                raise DatabaseError(f"关闭所有连接失败: {str(e)}") from e

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
        """

        with self._lock:
            try:
                cleaned_count = self.pool_manager.cleanup_idle_connections(
                    max_idle_time
                )
                return cleaned_count
            except (OSError, DatabaseError) as e:
                logger.error("清理空闲连接失败: %s", str(e))
                raise DatabaseError(f"清理空闲连接失败: {str(e)}") from e

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

        except ConfigError as e:
            self._diagnose_config_error(name, diagnosis, e)
        except (OSError, DatabaseError) as e:
            self._diagnose_general_error(name, diagnosis, e)

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
        except (OSError, DatabaseError) as connect_error:
            diagnosis["status"] = "unhealthy"
            diagnosis["details"]["connection"] = {
                "established": False,
                "error": str(connect_error),
            }

    def _diagnose_connection_test(
        self, driver: SQLAlchemyDriver, diagnosis: Dict[str, Any]
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
        except (OSError, DatabaseError) as test_error:
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
