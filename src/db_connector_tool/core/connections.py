"""数据库连接管理模块 (DatabaseManager)

提供统一的数据库连接管理接口，专注于连接生命周期管理和统一API设计。
采用连接池技术实现连接复用，提高性能和资源利用率。

主要特性：
- 连接池管理和连接复用
- 统一的错误处理和日志记录
- 线程安全的连接操作
- 连接生命周期管理
- 性能监控和统计信息
- 配置覆盖支持（临时连接配置）

设计原则：
- 简化配置验证，详细验证逻辑下放到SQLAlchemyDriver
- 增强连接池管理功能
- 明确职责边界，避免与底层组件功能重叠
- 提供简洁易用的用户接口

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
from .exceptions import ConfigError, DatabaseError, DBConnectionError
from .validators import ConnectionValidator

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class DatabaseManager:
    """数据库管理器类 (Database Manager)

    提供统一的数据库连接管理接口，实现连接池管理和生命周期控制。
    支持多种数据库类型，提供线程安全的连接操作。

    Attributes:
        app_name (str): 应用名称，用于配置文件的命名空间和日志标识
        config_file (str): 配置文件名，默认为"connections.toml"
        config_manager (ConfigManager): 配置管理器实例
        connection_pool (Dict[str, SQLAlchemyDriver]): 连接池字典
        _lock (threading.RLock): 可重入锁，确保线程安全
        _statistics (Dict[str, Any]): 连接统计信息

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

    # 错误消息常量
    CONNECTION_NOT_FOUND_MSG = "连接配置不存在: {}"
    CONNECTION_ALREADY_EXISTS_MSG = "连接配置已存在: {}"

    # 默认配置参数
    DEFAULT_MAX_IDLE_TIME = 300  # 5分钟
    DEFAULT_CONNECTION_TIMEOUT = 30  # 30秒连接超时
    DEFAULT_POOL_RECYCLE = 3600  # 1小时连接回收

    def __init__(
        self, app_name: str = "db_connector_tool", config_file: str = "connections.toml"
    ) -> None:
        """初始化数据库管理器

        创建新的数据库管理器实例，自动初始化配置管理器和连接池。

        Args:
            app_name: 应用名称，用于配置文件的命名空间和日志标识
            config_file: 配置文件名，默认为"connections.toml"

        Raises:
            ConfigError: 当配置管理器初始化失败时

        Note:
            建议为每个应用使用唯一的 app_name，避免配置冲突和日志混淆

        Example:
            >>> db_manager = DatabaseManager("my_app", "database.toml")
            >>> print(f"应用名称: {db_manager.app_name}")
            >>> db_manager = DatabaseManager()  # 使用默认参数
        """

        try:
            self.app_name = app_name
            self.config_file = config_file
            self.config_manager = ConfigManager(app_name, config_file)
            self.connection_pool: Dict[str, SQLAlchemyDriver] = {}
            self._lock = threading.RLock()
            self._statistics = {
                "total_connections_created": 0,
                "total_connections_closed": 0,
                "connection_errors": 0,
                "idle_connections_cleaned": 0,
                "start_time": time.time(),
                "last_cleanup_time": time.time(),
            }
            # 用于跟踪连接的使用情况
            self._connection_metadata: Dict[str, Dict[str, Any]] = {}
            logger.info("数据库管理器初始化成功: %s", app_name)
        except (ConfigError, OSError) as e:
            logger.error("初始化数据库管理器失败: %s", str(e))
            raise ConfigError(f"数据库管理器初始化失败: {str(e)}") from e

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """添加数据库连接配置

        创建新的数据库连接配置并保存到配置管理器。

        Args:
            name: 连接名称，用于标识不同的数据库连接
            connection_config: 连接配置字典，包含数据库连接所需的参数

        Raises:
            DatabaseError: 当创建连接配置失败时
            ConfigError: 当连接配置验证失败或连接已存在时

        Process:
            1. 检查连接名称是否已存在
            2. 验证基本配置
            3. 保存到配置管理器
            4. 记录日志

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
                raise ConfigError(self.CONNECTION_ALREADY_EXISTS_MSG.format(name))

            # 简化配置验证
            ConnectionValidator.validate_basic_config(connection_config)

            # 保存到配置管理器
            self.config_manager.add_config(name, connection_config)
            logger.info("数据库连接配置已创建: %s", name)

        with self._lock:
            self._safe_operation("数据库连接配置创建", name, _add_connection)

    def remove_connection(self, name: str) -> None:
        """删除连接配置

        删除指定的数据库连接配置，同时清理相关的连接资源。

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当删除连接配置失败时
            ConfigError: 当连接配置不存在时

        Process:
            1. 验证连接存在
            2. 清理连接池中的连接
            3. 删除配置信息
            4. 记录日志

        Note:
            删除前会自动关闭对应的连接并清理缓存
            此操作不可逆，建议谨慎使用

        Example:
            >>> db_manager.remove_connection("mysql_db")
        """

        def _remove_connection():
            # 检查连接是否存在
            self._validate_connection_exists(name)

            # 先关闭连接
            self._cleanup_connection(name)

            # 删除配置
            self.config_manager.remove_config(name)
            logger.info("连接配置已删除: %s", name)

        with self._lock:
            self._safe_operation("连接配置删除", name, _remove_connection)

    def _validate_connection_exists(self, name: str) -> None:
        """验证连接配置是否存在

        验证指定的连接配置是否存在，不存在则抛出异常。

        Args:
            name: 连接名称

        Raises:
            ConfigError: 当连接配置不存在时

        Note:
            此方法用于内部验证，确保操作前连接配置存在
        """
        existing_connections = self.list_connections()
        if name not in existing_connections:
            raise ConfigError(self.CONNECTION_NOT_FOUND_MSG.format(name))

    def _cleanup_connection(self, name: str) -> None:
        """安全清理连接资源

        安全地关闭连接并清理相关资源，确保无资源泄漏。

        Args:
            name: 连接名称

        Note:
            安全地关闭连接并清理相关资源，确保无资源泄漏
            即使发生异常也不会影响其他连接，但会记录详细错误
        """
        with self._lock:
            if not self._is_connection_in_pool(name):
                return

            driver = self.connection_pool[name]

            try:
                # 安全关闭连接
                self._safe_disconnect_driver(driver, name)
            except OSError as e:
                logger.error("清理连接 %s 时发生严重异常: %s", name, str(e))
            finally:
                # 确保从连接池中移除，避免内存泄漏
                self._remove_connection_from_pool(name)

    def _is_connection_in_pool(self, name: str) -> bool:
        """检查连接是否在连接池中

        检查指定的连接是否存在于连接池中。

        Args:
            name: 连接名称

        Returns:
            bool: 连接是否在连接池中
        """
        if name not in self.connection_pool:
            logger.debug("连接 %s 不在连接池中，无需清理", name)
            return False
        return True

    def _safe_disconnect_driver(self, driver: SQLAlchemyDriver, name: str) -> None:
        """安全断开驱动连接

        安全地断开数据库驱动连接，处理可能的异常。

        Args:
            driver: 数据库驱动实例
            name: 连接名称
        """
        if hasattr(driver, "engine") and driver.engine:
            try:
                driver.disconnect()
                logger.debug("连接 %s 已安全关闭", name)
            except OSError as disconnect_error:
                logger.warning(
                    "关闭连接 %s 时发生异常: %s", name, str(disconnect_error)
                )
        else:
            logger.debug("连接 %s 未连接或已关闭", name)

    def _remove_connection_from_pool(self, name: str) -> None:
        """从连接池中移除连接

        从连接池中移除指定的连接，并清理相关元数据。

        Args:
            name: 连接名称
        """
        try:
            del self.connection_pool[name]
            # 清理元数据
            self._cleanup_connection_metadata(name)
            self._statistics["total_connections_closed"] += 1
            logger.debug("连接 %s 已从连接池中移除", name)
        except KeyError:
            logger.debug("连接 %s 已从连接池中移除", name)
        except OSError as e:
            logger.error("从连接池中移除连接 %s 时发生异常: %s", name, str(e))

    def _cleanup_connection_metadata(self, name: str) -> None:
        """清理连接元数据

        清理指定连接的元数据信息。

        Args:
            name: 连接名称
        """
        if name in self._connection_metadata:
            del self._connection_metadata[name]

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """更新连接配置

        更新指定的数据库连接配置，同时关闭现有连接。

        Args:
            name: 连接名称
            connection_config: 新的连接配置

        Raises:
            DatabaseError: 当更新连接配置失败时
            ConfigError: 当新的连接配置验证失败或连接不存在时

        Process:
            1. 验证连接存在和新配置
            2. 关闭现有连接
            3. 更新配置信息
            4. 记录日志

        Note:
            更新配置前会关闭现有连接，新连接将在下次获取时创建
            确保配置更新不会影响正在进行的操作

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

            # 简化配置验证
            ConnectionValidator.validate_basic_config(connection_config)

            # 关闭现有连接
            self._cleanup_connection(name)

            # 更新配置
            self.config_manager.update_config(name, connection_config)
            logger.info("连接配置已更新: %s", name)

        with self._lock:
            self._safe_operation("连接配置更新", name, _update_connection)

    def show_connection(self, name: str) -> Dict[str, Any]:
        """显示指定连接的配置信息

        获取并返回指定连接的配置信息。

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

    def list_connections(self) -> List[str]:
        """获取所有可用的连接名称

        获取并返回所有可用的数据库连接名称列表。

        Returns:
            List[str]: 连接名称列表

        Example:
            >>> connections = db_manager.list_connections()
            >>> print(f"可用的连接: {', '.join(connections)}")
        """
        return self.config_manager.list_configs()

    def get_connection(
        self, name: str, config_overrides: Dict[str, Any] | None = None
    ) -> SQLAlchemyDriver:
        """获取数据库连接（连接池管理）

        获取数据库连接，支持连接池管理和临时配置覆盖。

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

        Process:
            1. 验证连接配置存在
            2. 检查连接池中是否有有效连接
            3. 如果连接无效，清理并重新创建
            4. 处理网络超时和连接失败的边界情况
            5. 返回可用的驱动实例

        Note:
            如果连接已存在且有效，直接返回缓存的连接，否则创建新连接
            连接池机制提高性能，避免频繁创建和销毁连接
            当提供config_overrides时，会创建新的连接而不是使用缓存
            添加了超时处理和边界情况处理，提高连接可靠性

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
                self._statistics["connection_errors"] += 1
                logger.error("获取数据库连接失败 %s: %s", name, str(e))
                raise DatabaseError(f"数据库连接获取失败: {str(e)}") from e

    def _get_connection_with_overrides(
        self, name: str, config_overrides: Dict[str, Any]
    ) -> SQLAlchemyDriver:
        """使用配置覆盖创建临时连接

        使用指定的配置覆盖创建临时数据库连接。

        Args:
            name: 连接名称
            config_overrides: 配置覆盖字典

        Returns:
            SQLAlchemyDriver: SQLAlchemyDriver实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """
        # 清理可能存在的缓存连接
        if name in self.connection_pool:
            self._cleanup_connection(name)

        # 获取基础配置并应用覆盖
        base_config = self.show_connection(name)
        connection_config = {**base_config, **config_overrides}

        # 验证修改后的配置
        ConnectionValidator.validate_basic_config(connection_config)

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

        从连接池获取现有连接或创建新连接。

        Args:
            name: 连接名称

        Returns:
            SQLAlchemyDriver: SQLAlchemyDriver实例

        Raises:
            DBConnectionError: 当连接建立失败时
        """
        # 检查连接池中是否有有效连接
        if name in self.connection_pool:
            driver = self.connection_pool[name]

            # 检查连接是否有效
            if self._is_connection_valid(driver):
                # 更新使用时间
                if name in self._connection_metadata:
                    self._connection_metadata[name]["last_used"] = time.time()
                    self._connection_metadata[name]["use_count"] += 1
                logger.debug("使用缓存的数据库连接: %s", name)
                return driver
            # 连接无效，清理并重新创建
            self._cleanup_connection(name)

        # 创建新连接
        return self._create_new_connection(name)

    def _create_new_connection(self, name: str) -> SQLAlchemyDriver:
        """创建新的数据库连接

        创建新的数据库连接并添加到连接池。

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
            self._statistics["connection_errors"] += 1
            logger.error("建立数据库连接失败 %s: %s", name, str(connect_error))
            # 分析错误类型，提供更详细的错误信息
            self._handle_connection_error(connect_error)

        # 加入连接池
        self.connection_pool[name] = driver
        # 初始化连接元数据
        self._initialize_connection_metadata(name)
        self._statistics["total_connections_created"] += 1

        logger.info("数据库连接已建立: %s", name)
        return driver

    def _handle_connection_error(self, connect_error: Exception) -> None:
        """处理连接错误，根据错误类型抛出相应的异常

        根据连接错误的类型，抛出相应的DBConnectionError异常。

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

    def _initialize_connection_metadata(self, name: str) -> None:
        """初始化连接元数据

        初始化指定连接的元数据信息。

        Args:
            name: 连接名称
        """
        self._connection_metadata[name] = {
            "last_used": time.time(),
            "use_count": 0,
            "created_at": time.time(),
            "connection_errors": 0,
            "last_error": None,
            "response_time": 0.0,
            "transaction_count": 0,
            "query_count": 0,
            "last_query_time": None,
        }

    def _is_connection_valid(self, driver: SQLAlchemyDriver) -> bool:
        """全面的连接有效性检查

        检查数据库连接是否有效，包括基本状态检查和实际查询测试。

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 连接是否有效

        Note:
            提供多层次的连接有效性验证，包括基本状态检查和实际查询测试
            详细记录连接失败的原因，便于故障诊断
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

        检查数据库驱动实例的基本状态是否有效。

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

        执行实际的查询测试来验证连接是否有效。

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

    def test_connection(self, name: str) -> bool:
        """测试连接是否有效

        测试指定的数据库连接是否有效，返回测试结果。

        Args:
            name: 连接名称

        Returns:
            bool: True表示连接成功，False表示连接失败

        Note:
            测试失败时会记录详细的错误信息，但不会抛出异常
            适合用于健康检查或连接监控
            测试完成后会自动清理连接，避免连接池污染
            对不同类型的连接错误进行分类处理，提供更详细的错误信息

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
            self._cleanup_connection(name)

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
            self._cleanup_connection_safe(name)
            # 分析错误类型，提供更详细的错误信息
            self._log_test_error(name, e)
            return False

    def _log_test_result(self, name: str, success: bool) -> None:
        """记录测试结果

        记录连接测试的结果信息。

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

        处理连接测试过程中发生的异常。

        Args:
            name: 连接名称
            exception: 异常对象
        """
        # 确保异常情况下也清理连接
        self._cleanup_connection_safe(name)
        logger.error("连接测试失败 %s: %s", name, str(exception))

    def _cleanup_connection_safe(self, name: str) -> None:
        """安全清理连接，忽略清理过程中的异常

        安全地清理连接资源，忽略清理过程中的异常。

        Args:
            name: 连接名称
        """
        try:
            self._cleanup_connection(name)
        except (OSError, DatabaseError):
            pass  # 忽略清理过程中的异常

    def _log_test_error(self, name: str, error: Exception) -> None:
        """记录测试错误，根据错误类型提供详细信息

        根据错误类型记录详细的连接测试错误信息。

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

        执行指定的SQL查询语句并返回结果。

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选

        Returns:
            List[Dict[str, Any]]: 查询结果列表，每行数据为字典格式

        Raises:
            DatabaseError: 当查询执行失败时
            ConfigError: 当连接配置不存在时

        Process:
            1. 获取数据库连接
            2. 执行查询语句并记录响应时间
            3. 返回格式化结果

        Note:
            此方法封装了连接获取和错误处理，提供统一的查询接口
            适合执行SELECT等查询操作
            连接由连接池管理，无需手动管理连接生命周期
            记录查询执行信息和响应时间，用于连接状态监控

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
            self._update_query_metadata(connection_name, response_time)

            return result

        try:
            return _execute_query()
        except (OSError, DatabaseError) as e:
            # 记录错误信息
            self._record_connection_error(connection_name, e)
            self._handle_exception("查询执行", connection_name, e)
            # 确保即使_handle_exception没有抛出异常，也会抛出异常
            raise DatabaseError("查询执行失败") from e

    def _update_query_metadata(
        self, connection_name: str, response_time: float
    ) -> None:
        """更新查询元数据

        更新指定连接的查询元数据信息。

        Args:
            connection_name: 连接名称
            response_time: 响应时间
        """
        if connection_name in self._connection_metadata:
            self._connection_metadata[connection_name]["last_used"] = time.time()
            self._connection_metadata[connection_name]["last_query_time"] = time.time()
            self._connection_metadata[connection_name]["response_time"] = response_time
            self._connection_metadata[connection_name]["query_count"] += 1

    def _record_connection_error(self, connection_name: str, error: Exception) -> None:
        """记录连接错误

        记录指定连接的错误信息。

        Args:
            connection_name: 连接名称
            error: 错误对象
        """
        if connection_name in self._connection_metadata:
            self._connection_metadata[connection_name]["connection_errors"] += 1
            self._connection_metadata[connection_name]["last_error"] = str(error)

    def execute_command(
        self,
        connection_name: str,
        command: str,
        params: Dict[str, Any] | None = None,
    ) -> int:
        """执行非查询SQL命令（INSERT/UPDATE/DELETE等）

        执行指定的非查询SQL命令并返回影响的行数。

        Args:
            connection_name: 连接名称
            command: SQL命令语句
            params: 命令参数字典，可选

        Returns:
            int: 影响的行数

        Raises:
            DatabaseError: 当命令执行失败时
            ConfigError: 当连接配置不存在时

        Process:
            1. 获取数据库连接
            2. 执行命令语句并记录响应时间
            3. 返回影响行数

        Note:
            适合执行数据修改操作，返回受影响的行数
            提供事务安全的命令执行环境
            连接由连接池管理，无需手动管理连接生命周期
            记录命令执行信息和响应时间，用于连接状态监控

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
            self._update_command_metadata(connection_name, response_time)

            return result

        try:
            return _execute_command()
        except (OSError, DatabaseError) as e:
            # 记录错误信息
            self._record_connection_error(connection_name, e)
            self._handle_exception("命令执行", connection_name, e)
            # 确保即使_handle_exception没有抛出异常，也会抛出异常
            raise DatabaseError("命令执行失败") from e

    def _update_command_metadata(
        self, connection_name: str, response_time: float
    ) -> None:
        """更新命令元数据

        更新指定连接的命令元数据信息。

        Args:
            connection_name: 连接名称
            response_time: 响应时间
        """
        if connection_name in self._connection_metadata:
            self._connection_metadata[connection_name]["last_used"] = time.time()
            self._connection_metadata[connection_name]["last_query_time"] = time.time()
            self._connection_metadata[connection_name]["response_time"] = response_time
            self._connection_metadata[connection_name]["transaction_count"] += 1

    def close_connection(self, name: str) -> None:
        """关闭数据库连接

        关闭指定的数据库连接，释放相关资源。

        Args:
            name: 连接名称

        Raises:
            DatabaseError: 当关闭连接失败时

        Note:
            关闭连接会释放相关资源，但保留连接配置
            连接可以在需要时重新建立

        Example:
            >>> db_manager.close_connection("mysql_db")
        """

        def _close_connection():
            self._cleanup_connection(name)
            logger.info("数据库连接已关闭: %s", name)

        with self._lock:
            self._safe_operation("连接关闭", name, _close_connection)

    def close_all_connections(self) -> None:
        """安全关闭所有数据库连接

        安全关闭所有数据库连接，彻底清理所有活跃的连接资源。

        Raises:
            DatabaseError: 当关闭所有连接失败时

        Process:
            1. 获取所有连接名称的副本
            2. 逐个安全关闭连接
            3. 统计成功和失败数量
            4. 记录详细汇总日志
            5. 清理连接池字典

        Note:
            使用list()创建副本确保线程安全，避免在迭代过程中修改字典
            适合在应用关闭或维护时调用

        Example:
            >>> db_manager.close_all_connections()
        """
        with self._lock:
            try:
                # 创建连接名称副本，避免迭代过程中字典修改
                connection_names = list(self.connection_pool.keys())
                total_connections = len(connection_names)

                if total_connections == 0:
                    logger.info("连接池为空，无需关闭连接")
                    return

                logger.info("开始关闭所有连接，共 %s 个连接", total_connections)

                success_count, error_count = self._close_all_connections(
                    connection_names
                )

                # 最终检查连接池是否完全清空
                self._ensure_pool_cleanup()

                # 记录详细汇总信息
                self._log_close_all_connections_result(
                    success_count, error_count, total_connections
                )

            except (OSError, DatabaseError) as e:
                self._handle_close_all_connections_error(e)

    def _close_all_connections(self, connection_names: List[str]) -> tuple[int, int]:
        """关闭所有连接并返回成功和失败的数量

        关闭所有指定的连接并返回成功和失败的数量。

        Args:
            connection_names: 连接名称列表

        Returns:
            tuple[int, int]: (成功数量, 失败数量)
        """
        success_count = 0
        error_count = 0

        for name in connection_names:
            try:
                # 使用内部清理方法，避免递归调用close_connection
                self._cleanup_connection(name)
                success_count += 1
                logger.debug("连接 %s 关闭成功", name)
            except (OSError, DatabaseError) as e:
                error_count += 1
                logger.error("关闭连接 %s 失败: %s", name, str(e))

        return success_count, error_count

    def _ensure_pool_cleanup(self) -> None:
        """确保连接池被完全清空

        确保连接池被完全清空，处理可能的清理不完整情况。
        """
        remaining_connections = len(self.connection_pool)
        if remaining_connections > 0:
            logger.warning(
                "连接池清理不完整，仍有 %s 个连接未清理", remaining_connections
            )
            # 强制清空连接池
            self.connection_pool.clear()
            logger.info("已强制清空连接池")

    def _log_close_all_connections_result(
        self, success_count: int, error_count: int, total_connections: int
    ) -> None:
        """记录关闭所有连接的结果

        记录关闭所有连接的结果信息。

        Args:
            success_count: 成功关闭的连接数量
            error_count: 关闭失败的连接数量
            total_connections: 总连接数量
        """
        if error_count > 0:
            logger.warning(
                "关闭所有连接完成，成功: %s, 失败: %s, 总数: %s",
                success_count,
                error_count,
                total_connections,
            )
        else:
            logger.info("所有数据库连接已安全关闭，共 %s 个连接", success_count)

    def _handle_close_all_connections_error(self, error: Exception) -> None:
        """处理关闭所有连接时的错误

        处理关闭所有连接时发生的错误，包装并重新抛出异常。

        Args:
            error: 错误对象

        Raises:
            DatabaseError: 包装后的错误
        """
        logger.error("关闭所有连接时发生严重异常: %s", str(error))
        # 即使发生异常也要尝试清理连接池
        try:
            self.connection_pool.clear()
            logger.info("异常情况下已强制清空连接池")
        except (OSError, DatabaseError):
            pass
        raise DatabaseError(f"关闭所有连接失败: {str(error)}") from error

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """获取连接详细信息（包含统计信息）

        获取指定连接的详细信息，包含基本配置和统计信息。

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
            if name in self._connection_metadata:
                metadata = self._connection_metadata[name]
                info.update(
                    {
                        "use_count": metadata["use_count"],
                        "last_used": metadata["last_used"],
                        "created_at": metadata["created_at"],
                        "is_active": name in self.connection_pool,
                        "connection_errors": metadata["connection_errors"],
                        "last_error": metadata["last_error"],
                        "response_time": metadata["response_time"],
                        "transaction_count": metadata["transaction_count"],
                        "query_count": metadata["query_count"],
                        "last_query_time": metadata["last_query_time"],
                    }
                )

            return {k: v for k, v in info.items() if v is not None}

        with self._lock:
            return self._safe_operation("连接信息获取", name, _get_connection_info)

    def get_statistics(self) -> Dict[str, Any]:
        """获取连接统计信息

        获取数据库连接的统计信息，包含连接创建、关闭、错误等统计数据。

        Returns:
            Dict[str, Any]: 统计信息字典，包含连接创建、关闭、错误等统计

        Statistics Include:
            - 总连接创建次数
            - 总连接关闭次数
            - 连接错误次数
            - 系统运行时间
            - 当前活跃连接数
            - 连接池大小

        Example:
            >>> stats = db_manager.get_statistics()
            >>> print(f"总连接数: {stats['total_connections_created']}")
            >>> print(f"运行时间: {stats['uptime']}秒")
        """
        with self._lock:
            stats = self._statistics.copy()
            stats["current_time"] = time.time()
            stats["uptime"] = stats["current_time"] - stats["start_time"]
            stats["active_connections"] = len(
                [
                    conn
                    for conn in self.connection_pool.values()
                    if self._is_connection_valid(conn)
                ]
            )
            stats["connection_pool_size"] = len(self.connection_pool)
            return stats

    def cleanup_idle_connections(
        self, max_idle_time: int = DEFAULT_MAX_IDLE_TIME
    ) -> int:
        """清理空闲时间过长的连接

        清理空闲时间超过指定阈值的数据库连接。

        Args:
            max_idle_time: 最大空闲时间（秒），默认5分钟

        Returns:
            int: 清理的连接数量

        Raises:
            DatabaseError: 当清理过程发生严重错误时

        Process:
            1. 检查所有连接的空闲时间
            2. 清理超过最大空闲时间的连接
            3. 记录清理统计信息

        Example:
            >>> cleaned_count = db_manager.cleanup_idle_connections(600)  # 10分钟
            >>> print(f"清理了 {cleaned_count} 个空闲连接")
        """

        def _cleanup_idle_connections():
            current_time = time.time()
            connection_names = list(self.connection_pool.keys())

            if not connection_names:
                logger.debug("连接池为空，无需清理空闲连接")
                return 0

            logger.info("开始清理空闲连接，最大空闲时间: %s秒", max_idle_time)

            cleaned_count = self._process_idle_connections(
                connection_names, current_time, max_idle_time
            )
            self._update_cleanup_stats(current_time, cleaned_count)

            return cleaned_count

        with self._lock:
            return self._safe_operation(
                "空闲连接清理", "all", _cleanup_idle_connections
            )

    def _process_idle_connections(
        self, connection_names: List[str], current_time: float, max_idle_time: int
    ) -> int:
        """处理空闲连接

        处理指定列表中的空闲连接，清理超过最大空闲时间的连接。

        Args:
            connection_names: 连接名称列表
            current_time: 当前时间
            max_idle_time: 最大空闲时间

        Returns:
            int: 清理的连接数量
        """
        cleaned_count = 0

        for name in connection_names:
            if not self._is_connection_in_pool(name):
                continue

            idle_time = self._calculate_idle_time(name, current_time)

            if idle_time > max_idle_time:
                cleaned_count += self._cleanup_idle_connection(name, idle_time)

        return cleaned_count

    def _calculate_idle_time(self, name: str, current_time: float) -> float:
        """计算连接的空闲时间

        计算指定连接的空闲时间。

        Args:
            name: 连接名称
            current_time: 当前时间

        Returns:
            float: 空闲时间（秒）
        """
        if name in self._connection_metadata:
            return current_time - self._connection_metadata[name]["last_used"]
        # 如果没有元数据，使用当前时间作为默认值
        return 0

    def _cleanup_idle_connection(self, name: str, idle_time: float) -> int:
        """清理空闲连接

        清理指定的空闲连接。

        Args:
            name: 连接名称
            idle_time: 空闲时间

        Returns:
            int: 清理是否成功（1成功，0失败）
        """
        try:
            logger.debug("连接 %s 空闲时间 %.1f秒超过限制，执行清理", name, idle_time)
            self._cleanup_connection(name)
            self._statistics["idle_connections_cleaned"] += 1
            return 1
        except (OSError, DatabaseError) as e:
            logger.warning("清理空闲连接 %s 失败: %s", name, str(e))
            return 0

    def _update_cleanup_stats(self, current_time: float, cleaned_count: int) -> None:
        """更新清理统计信息

        更新空闲连接清理的统计信息。

        Args:
            current_time: 当前时间
            cleaned_count: 清理的连接数量
        """
        self._statistics["last_cleanup_time"] = current_time

        if cleaned_count > 0:
            logger.info("空闲连接清理完成，共清理 %s 个连接", cleaned_count)
        else:
            logger.debug("未发现需要清理的空闲连接")

    def get_connection_pool_status(self) -> Dict[str, Any]:
        """获取连接池状态信息

        获取数据库连接池的状态信息，包含活跃连接数、平均响应时间等。

        Returns:
            Dict[str, Any]: 连接池状态信息字典

        Example:
            >>> status = db_manager.get_connection_pool_status()
            >>> print(f"活跃连接数: {status['active_connections']}")
            >>> print(f"总创建连接数: {status['statistics']['total_connections_created']}")
            >>> print(f"平均响应时间: {status['average_response_time']:.3f}s")
        """
        with self._lock:
            current_time = time.time()

            # 计算连接池统计信息
            stats = self._calculate_pool_stats(current_time)
            connection_details = self._get_connection_details(current_time)

            # 计算平均值
            pool_size = len(self.connection_pool)
            average_response_time = self._calculate_average_response_time(
                stats["total_response_time"], pool_size
            )
            error_rate = self._calculate_error_rate(
                stats["total_errors"], stats["total_query_count"]
            )

            status_data = {
                "current_time": current_time,
                "stats": stats,
                "connection_details": connection_details,
                "pool_size": pool_size,
                "average_response_time": average_response_time,
                "error_rate": error_rate,
            }
            return self._build_pool_status_response(status_data)

    def _calculate_pool_stats(self, current_time: float) -> Dict[str, Any]:
        """计算连接池统计信息

        计算连接池的统计信息，包含活跃连接数、总使用次数等。

        Args:
            current_time: 当前时间

        Returns:
            Dict[str, Any]: 连接池统计信息字典
        """
        stats = {
            "active_connections": 0,
            "total_use_count": 0,
            "max_idle_time": 0,
            "total_query_count": 0,
            "total_transaction_count": 0,
            "total_errors": 0,
            "total_response_time": 0.0,
        }

        for name, _ in self.connection_pool.items():
            stats["active_connections"] += 1

            if name in self._connection_metadata:
                metadata = self._connection_metadata[name]
                stats["total_use_count"] += metadata["use_count"]
                stats["total_query_count"] += metadata["query_count"]
                stats["total_transaction_count"] += metadata["transaction_count"]
                stats["total_errors"] += metadata["connection_errors"]
                stats["total_response_time"] += metadata["response_time"]
                idle_time = current_time - metadata["last_used"]
                stats["max_idle_time"] = max(stats["max_idle_time"], idle_time)

        return stats

    def _get_connection_details(self, current_time: float) -> Dict[str, Any]:
        """获取连接详细信息

        获取连接池中所有连接的详细信息。

        Args:
            current_time: 当前时间

        Returns:
            Dict[str, Any]: 连接详细信息字典
        """
        connection_details = {}

        for name, _ in self.connection_pool.items():
            if name in self._connection_metadata:
                metadata = self._connection_metadata[name]
                idle_time = current_time - metadata["last_used"]
                connection_details[name] = {
                    "is_active": True,
                    "use_count": metadata["use_count"],
                    "query_count": metadata["query_count"],
                    "transaction_count": metadata["transaction_count"],
                    "connection_errors": metadata["connection_errors"],
                    "response_time": metadata["response_time"],
                    "last_used": metadata["last_used"],
                    "idle_time": idle_time,
                    "last_error": metadata["last_error"],
                }
            else:
                # 如果没有元数据，使用默认值
                connection_details[name] = {
                    "is_active": True,
                    "use_count": 0,
                    "query_count": 0,
                    "transaction_count": 0,
                    "connection_errors": 0,
                    "response_time": 0.0,
                    "last_used": current_time,
                    "idle_time": 0,
                    "last_error": None,
                }

        return connection_details

    def _calculate_average_response_time(
        self, total_response_time: float, pool_size: int
    ) -> float:
        """计算平均响应时间

        计算连接池的平均响应时间。

        Args:
            total_response_time: 总响应时间
            pool_size: 连接池大小

        Returns:
            float: 平均响应时间
        """
        return total_response_time / pool_size if pool_size > 0 else 0.0

    def _calculate_error_rate(self, total_errors: int, total_query_count: int) -> float:
        """计算错误率

        计算连接池的错误率。

        Args:
            total_errors: 总错误数
            total_query_count: 总查询数

        Returns:
            float: 错误率
        """
        return total_errors / (total_query_count + 1) if total_query_count > 0 else 0.0

    def _build_pool_status_response(
        self,
        status_data: Dict[str, Any],
    ) -> Dict[str, Any]:
        """构建连接池状态响应

        构建连接池状态响应字典，包含活跃连接数、总连接数等信息。

        Args:
            status_data: 状态数据字典，包含以下键：
                current_time: 当前时间
                stats: 连接池统计信息
                connection_details: 连接详细信息
                pool_size: 连接池大小
                average_response_time: 平均响应时间
                error_rate: 错误率

        Returns:
            Dict[str, Any]: 连接池状态响应字典
        """
        return {
            "active_connections": status_data["stats"]["active_connections"],
            "total_connections": status_data["pool_size"],
            "total_use_count": status_data["stats"]["total_use_count"],
            "total_query_count": status_data["stats"]["total_query_count"],
            "total_transaction_count": status_data["stats"]["total_transaction_count"],
            "total_errors": status_data["stats"]["total_errors"],
            "average_response_time": status_data["average_response_time"],
            "error_rate": status_data["error_rate"],
            "max_idle_time": status_data["stats"]["max_idle_time"],
            "statistics": self._statistics.copy(),
            "uptime": status_data["current_time"] - self._statistics["start_time"],
            "last_cleanup": status_data["current_time"]
            - self._statistics["last_cleanup_time"],
            "connection_details": status_data["connection_details"],
        }

    def diagnose_connection(self, name: str) -> Dict[str, Any]:
        """
        诊断连接问题，提供详细的连接诊断信息

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 连接诊断信息字典

        Note:
            提供详细的连接诊断信息，包括配置验证、网络测试、连接测试等
            适合用于故障排查和连接问题分析

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
        """
        诊断连接配置

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
        """
        诊断连接

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
            self._cleanup_connection(name)
        except (OSError, DatabaseError) as connect_error:
            diagnosis["status"] = "unhealthy"
            diagnosis["details"]["connection"] = {
                "established": False,
                "error": str(connect_error),
            }

    def _diagnose_connection_test(
        self, driver: SQLAlchemyDriver, diagnosis: Dict[str, Any]
    ) -> None:
        """
        诊断连接测试

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
        """
        诊断配置错误

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
        """
        诊断一般错误

        Args:
            _name: 连接名称（未使用）
            diagnosis: 诊断信息字典
            error: 错误对象
        """
        diagnosis["status"] = "error"
        diagnosis["details"]["general_error"] = str(error)

    def _add_pool_info(self, name: str, diagnosis: Dict[str, Any]) -> None:
        """
        添加连接池信息

        Args:
            name: 连接名称
            diagnosis: 诊断信息字典
        """
        if name in self.connection_pool:
            # 从元数据中获取连接池信息
            if name in self._connection_metadata:
                metadata = self._connection_metadata[name]
                diagnosis["details"]["pool_info"] = {
                    "is_active": True,
                    "use_count": metadata["use_count"],
                    "connection_errors": metadata["connection_errors"],
                    "last_error": metadata["last_error"],
                    "response_time": metadata["response_time"],
                }
            else:
                # 如果没有元数据，使用默认值
                diagnosis["details"]["pool_info"] = {
                    "is_active": True,
                    "use_count": 0,
                    "connection_errors": 0,
                    "last_error": None,
                    "response_time": 0.0,
                }

    def _handle_exception(
        self, operation: str, name: str, exception: Exception
    ) -> None:
        """
        通用异常处理方法

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

    def _safe_operation(self, operation: str, name: str, func, *args, **kwargs):
        """
        安全执行操作的辅助方法，处理异常并转换为适当的错误类型

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

    def __str__(self) -> str:
        """返回数据库管理器的字符串表示"""
        stats = self.get_statistics()
        return (
            f"DatabaseManager(app_name='{self.app_name}', "
            f"active_connections={stats['active_connections']}, "
            f"pool_size={stats['connection_pool_size']})"
        )
