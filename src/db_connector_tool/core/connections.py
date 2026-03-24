"""
数据库连接管理器模块

提供统一的数据库连接管理接口， 专注于连接生命周期管理和统一API设计。
采用连接池技术实现连接复用， 提高性能和资源利用率。

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
"""

import threading
import time
from typing import Any, Dict, List, Set

from ..drivers.sqlalchemy_driver import SQLAlchemyDriver
from ..utils.logging_utils import get_logger
from .config import ConfigManager
from .exceptions import ConfigError, ConnectionError, DatabaseError

# 获取模块级别的日志记录器
logger = get_logger(__name__)

# 错误消息常量
CONNECTION_NOT_FOUND_MSG = "连接配置不存在: {}"
CONNECTION_ALREADY_EXISTS_MSG = "连接配置已存在: {}"

# 支持的数据库类型
SUPPORTED_DATABASE_TYPES: Set[str] = {
    "oracle",
    "postgresql",
    "mysql",
    "mssql",
    "sqlite",
    "gbasedbt",
}

# 默认配置参数
DEFAULT_MAX_IDLE_TIME = 300  # 5分钟
DEFAULT_CONNECTION_TIMEOUT = 30  # 30秒连接超时
DEFAULT_POOL_RECYCLE = 3600  # 1小时连接回收


class ConnectionInfo:
    """
    连接信息封装类

    封装连接的统计信息和状态，用于连接池管理和监控。

    Attributes:
        driver (SQLAlchemyDriver): 数据库驱动实例
        last_used (float): 最后使用时间戳
        use_count (int): 使用次数统计
        created_at (float): 创建时间戳
        is_active (bool): 连接是否活跃状态
        connection_errors (int): 连接错误次数
        last_error (str): 最后一次错误信息
        response_time (float): 最近的响应时间（秒）
        transaction_count (int): 事务计数
        query_count (int): 查询计数
        last_query_time (float): 最后一次查询时间

    Example:
        >>> connection_info = ConnectionInfo(driver)
        >>> connection_info.mark_used()
        >>> print(f"使用次数: {connection_info.use_count}")
        >>> print(f"错误次数: {connection_info.connection_errors}")
    """

    def __init__(self, driver: SQLAlchemyDriver) -> None:
        """
        初始化连接信息

        Args:
            driver: 数据库驱动实例，必须已建立连接
        """
        self.driver = driver
        self.last_used = time.time()
        self.use_count = 0
        self.created_at = time.time()
        self.is_active = True
        self.connection_errors = 0
        self.last_error = None
        self.response_time = 0.0
        self.transaction_count = 0
        self.query_count = 0
        self.last_query_time = None

    def mark_used(self) -> None:
        """标记连接被使用，更新最后使用时间和使用次数"""
        self.last_used = time.time()
        self.use_count += 1

    def mark_query(self, response_time: float = 0.0) -> None:
        """标记查询执行，更新查询计数和响应时间"""
        self.query_count += 1
        self.last_query_time = time.time()
        self.response_time = response_time

    def mark_transaction(self) -> None:
        """标记事务执行，更新事务计数"""
        self.transaction_count += 1

    def mark_error(self, error_message: str) -> None:
        """标记连接错误，更新错误计数和最后错误信息"""
        self.connection_errors += 1
        self.last_error = error_message

    def invalidate(self) -> None:
        """标记连接为无效状态"""
        self.is_active = False

    def __str__(self) -> str:
        """返回连接的字符串表示"""
        last_used_str = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(self.last_used)
        )
        created_str = time.strftime(
            "%Y-%m-%d %H:%M:%S", time.localtime(self.created_at)
        )
        last_query_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(self.last_query_time)) if self.last_query_time else "N/A"
        return (
            f"ConnectionInfo(use_count={self.use_count}, "
            f"last_used={last_used_str}, created_at={created_str}, "
            f"is_active={self.is_active}, errors={self.connection_errors}, "
            f"response_time={self.response_time:.3f}s, queries={self.query_count}, "
            f"transactions={self.transaction_count}, last_query={last_query_str})"
        )


class DatabaseManager:
    """
    数据库管理器类

    提供统一的数据库连接管理接口，实现连接池管理和生命周期控制。
    支持多种数据库类型，提供线程安全的连接操作。

    Attributes:
        app_name (str): 应用名称，用于配置文件的命名空间
        config_file (str): 配置文件名
        config_manager (ConfigManager): 配置管理器实例
        connection_pool (Dict[str, ConnectionInfo]): 连接池字典
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
    """

    def __init__(
        self, app_name: str = "db_connector_tool", config_file: str = "connections.toml"
    ) -> None:
        """
        初始化数据库管理器

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
        """
        try:
            self.app_name = app_name
            self.config_file = config_file
            self.config_manager = ConfigManager(app_name, config_file)
            self.connection_pool: Dict[str, ConnectionInfo] = {}
            self._lock = threading.RLock()
            self._statistics = {
                "total_connections_created": 0,
                "total_connections_closed": 0,
                "connection_errors": 0,
                "idle_connections_cleaned": 0,
                "start_time": time.time(),
                "last_cleanup_time": time.time(),
            }
            logger.info(f"数据库管理器初始化成功: {app_name}")
        except Exception as e:
            logger.error(f"初始化数据库管理器失败: {str(e)}")
            raise ConfigError(f"数据库管理器初始化失败: {str(e)}")

    def add_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        创建数据库连接配置

        Args:
            name: 连接名称，用于标识不同的数据库连接
            connection_config: 连接配置字典

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
                raise ConfigError(CONNECTION_ALREADY_EXISTS_MSG.format(name))

            # 简化配置验证
            self._validate_basic_config(connection_config)

            # 保存到配置管理器
            self.config_manager.add_connection(name, connection_config)
            logger.info(f"数据库连接配置已创建: {name}")

        with self._lock:
            self._safe_operation("数据库连接配置创建", name, _add_connection)

    def _validate_basic_config(self, config: Dict[str, Any]) -> None:
        """
        基本配置验证

        Args:
            config: 连接配置字典

        Raises:
            ConfigError: 当基本配置验证失败时

        Note:
            详细验证逻辑由SQLAlchemyDriver处理，这里只做基本检查
            确保配置结构正确，避免无效配置进入系统

        Validation Rules:
            - 配置不能为空且必须是字典
            - 数据库类型必须在支持列表中
            - 非SQLite数据库需要基本连接参数
            - 端口号必须是有效的整数
            - 连接超时等参数必须是有效的数值
            - 字符串参数长度限制和特殊字符过滤
        """
        if not config or not isinstance(config, dict):
            raise ConfigError("连接配置不能为空且必须是字典")

        # 验证数据库类型
        db_type = config.get("type", "").lower()
        if db_type not in SUPPORTED_DATABASE_TYPES:
            supported_types = ", ".join(sorted(SUPPORTED_DATABASE_TYPES))
            raise ConfigError(
                f"不支持的数据库类型: {db_type}，支持的类型: {supported_types}"
            )

        # 字符串参数验证函数
        def validate_string_param(param_name, value, max_length=100):
            if not isinstance(value, str):
                logger.warning(f"{param_name}类型无效: {type(value).__name__}")
                raise ConfigError(f"{param_name}必须是字符串类型")
            if len(value) > max_length:
                logger.warning(f"{param_name}长度超过限制: {len(value)} > {max_length}")
                raise ConfigError(f"{param_name}长度不能超过{max_length}个字符")
            # 检查特殊字符，防止注入攻击
            import re
            if re.search(r'[;\\\'\"\"]', value):
                logger.warning(f"{param_name}包含潜在的恶意字符: {value}")
                raise ConfigError(f"{param_name}包含不允许的特殊字符")

        # 验证字符串参数
        string_params = ["username", "password", "host", "database", "service_name", "sid", "server"]
        for param in string_params:
            if param in config:
                validate_string_param(param, config[param])

        # SQLite数据库特殊处理
        if db_type == "sqlite":
            if "database" not in config:
                config["database"] = ":memory:"
            return

        # 应用特定数据库类型的默认配置
        self._apply_default_config(config)

        # 验证必需参数
        required_fields = ["username", "password", "host"]
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise ConfigError(f"缺少必需的连接参数: {', '.join(missing_fields)}")

        # 验证端口号
        if "port" in config:
            port = config["port"]
            if not isinstance(port, int) or port <= 0 or port > 65535:
                raise ConfigError("端口号必须是1-65535之间的整数")

        # 验证连接超时参数
        if "timeout" in config:
            timeout = config["timeout"]
            if not isinstance(timeout, (int, float)) or timeout <= 0:
                raise ConfigError("连接超时必须是大于0的数值")

        # 验证连接池参数
        if "pool_size" in config:
            pool_size = config["pool_size"]
            if not isinstance(pool_size, int) or pool_size <= 0:
                raise ConfigError("连接池大小必须是大于0的整数")

        # 验证特定数据库类型的参数
        if db_type == "oracle":
            if "service_name" not in config and "sid" not in config:
                raise ConfigError("Oracle数据库必须提供service_name或sid")
        elif db_type == "postgresql":
            if "database" not in config:
                raise ConfigError("PostgreSQL数据库必须提供database参数")
        elif db_type == "mysql":
            if "database" not in config:
                raise ConfigError("MySQL数据库必须提供database参数")
        elif db_type == "mssql":
            if "database" not in config:
                raise ConfigError("SQL Server数据库必须提供database参数")
        elif db_type == "gbasedbt":
            if "database" not in config:
                raise ConfigError("GBase数据库必须提供database参数")

    def _apply_default_config(self, config: Dict[str, Any]) -> None:
        """
        应用特定数据库类型的默认配置

        Args:
            config: 连接配置字典
        """
        db_type = config.get("type", "").lower()

        if db_type == "oracle" and "service_name" not in config:
            config["service_name"] = "XE"
        elif db_type == "postgresql" and "gssencmode" not in config:
            config["gssencmode"] = "disable"
        elif db_type == "mssql":
            if "charset" not in config:
                config["charset"] = "cp936"
            if "tds_version" not in config:
                config["tds_version"] = "7.0"
        elif db_type == "gbasedbt" and "server" not in config:
            config["server"] = "gbase01"

    def remove_connection(self, name: str) -> None:
        """
        删除连接配置

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
            self.config_manager.remove_connection(name)
            logger.info(f"连接配置已删除: {name}")

        with self._lock:
            self._safe_operation("连接配置删除", name, _remove_connection)

    def _validate_connection_exists(self, name: str) -> None:
        """
        验证连接配置是否存在

        Args:
            name: 连接名称

        Raises:
            ConfigError: 当连接配置不存在时

        Note:
            此方法用于内部验证，确保操作前连接配置存在
        """
        existing_connections = self.list_connections()
        if name not in existing_connections:
            raise ConfigError(CONNECTION_NOT_FOUND_MSG.format(name))

    def _cleanup_connection(self, name: str) -> None:
        """
        安全清理连接资源

        Args:
            name: 连接名称

        Note:
            安全地关闭连接并清理相关资源，确保无资源泄漏
            即使发生异常也不会影响其他连接，但会记录详细错误
        """
        with self._lock:
            if name not in self.connection_pool:
                logger.debug(f"连接 {name} 不在连接池中，无需清理")
                return

            connection_info = self.connection_pool[name]

            try:
                # 标记连接为无效状态
                connection_info.invalidate()

                # 安全关闭连接
                if (
                    hasattr(connection_info.driver, "is_connected")
                    and connection_info.driver.is_connected
                ):
                    try:
                        connection_info.driver.disconnect()
                        logger.debug(f"连接 {name} 已安全关闭")
                    except Exception as disconnect_error:
                        logger.warning(
                            f"关闭连接 {name} 时发生异常: {str(disconnect_error)}"
                        )
                else:
                    logger.debug(f"连接 {name} 未连接或已关闭")

            except Exception as e:
                logger.error(f"清理连接 {name} 时发生严重异常: {str(e)}")
            finally:
                # 确保从连接池中移除，避免内存泄漏
                try:
                    del self.connection_pool[name]
                    self._statistics["total_connections_closed"] += 1
                    logger.debug(f"连接 {name} 已从连接池中移除")
                except KeyError:
                    logger.debug(f"连接 {name} 已从连接池中移除")
                except Exception as e:
                    logger.error(f"从连接池中移除连接 {name} 时发生异常: {str(e)}")

    def update_connection(self, name: str, connection_config: Dict[str, Any]) -> None:
        """
        更新连接配置

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
            self._validate_basic_config(connection_config)

            # 关闭现有连接
            self._cleanup_connection(name)

            # 更新配置
            self.config_manager.update_connection(name, connection_config)
            logger.info(f"连接配置已更新: {name}")

        with self._lock:
            self._safe_operation("连接配置更新", name, _update_connection)

    def show_connection(self, name: str) -> Dict[str, Any]:
        """
        显示指定连接的配置信息

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
        return self.config_manager.get_connection(name)

    def list_connections(self) -> List[str]:
        """
        获取所有可用的连接名称

        Returns:
            连接名称列表

        Example:
            >>> connections = db_manager.list_connections()
            >>> print(f"可用的连接: {', '.join(connections)}")
        """
        return self.config_manager.list_connections()

    def get_connection(
        self, name: str, config_overrides: Dict[str, Any] | None = None
    ) -> SQLAlchemyDriver:
        """
        获取数据库连接（连接池管理）

        Args:
            name: 连接名称
            config_overrides: 可选的配置覆盖字典，用于临时修改连接配置
                            例如：{"host": "127.0.0.1", "port": 3306}

        Returns:
            SQLAlchemyDriver实例，用于执行数据库操作

        Raises:
            DatabaseError: 当获取连接失败时
            ConnectionError: 当连接建立失败时
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

                # 如果提供了配置覆盖，则强制创建新连接
                if config_overrides:
                    # 清理可能存在的缓存连接
                    if name in self.connection_pool:
                        self._cleanup_connection(name)

                    # 获取基础配置并应用覆盖
                    base_config = self.show_connection(name)
                    connection_config = {**base_config, **config_overrides}

                    # 验证修改后的配置
                    self._validate_basic_config(connection_config)

                    # 创建新连接，处理连接超时
                    driver = SQLAlchemyDriver(connection_config)
                    try:
                        driver.connect()
                    except Exception as connect_error:
                        logger.error(f"使用临时配置建立连接失败 {name}: {str(connect_error)}")
                        raise ConnectionError(f"连接建立失败: {str(connect_error)}")

                    # 创建连接信息但不加入连接池（因为是临时配置）
                    connection_info = ConnectionInfo(driver)
                    # 注意：临时配置的连接不加入连接池，避免配置冲突

                    logger.info(f"使用临时配置建立数据库连接: {name}")
                    return driver

                # 连接池管理逻辑（无配置覆盖时）
                if name in self.connection_pool:
                    connection_info = self.connection_pool[name]

                    # 检查连接是否有效
                    if connection_info.is_active and self._is_connection_valid(
                        connection_info.driver
                    ):
                        connection_info.mark_used()
                        logger.debug(f"使用缓存的数据库连接: {name}")
                        return connection_info.driver
                    else:
                        # 连接无效，清理并重新创建
                        self._cleanup_connection(name)

                # 创建新连接，处理网络超时和服务不可用
                connection_config = self.show_connection(name)
                driver = SQLAlchemyDriver(connection_config)
                try:
                    driver.connect()
                except Exception as connect_error:
                    self._statistics["connection_errors"] += 1
                    logger.error(f"建立数据库连接失败 {name}: {str(connect_error)}")
                    # 分析错误类型，提供更详细的错误信息
                    if "timeout" in str(connect_error).lower():
                        raise ConnectionError(f"连接超时: {str(connect_error)}")
                    elif "refused" in str(connect_error).lower():
                        raise ConnectionError(f"连接被拒绝: {str(connect_error)}")
                    elif "unreachable" in str(connect_error).lower():
                        raise ConnectionError(f"主机不可达: {str(connect_error)}")
                    else:
                        raise ConnectionError(f"连接建立失败: {str(connect_error)}")

                # 创建连接信息并加入连接池
                connection_info = ConnectionInfo(driver)
                self.connection_pool[name] = connection_info
                self._statistics["total_connections_created"] += 1

                logger.info(f"数据库连接已建立: {name}")
                return driver

            except (ConfigError, ConnectionError):
                raise
            except Exception as e:
                self._statistics["connection_errors"] += 1
                logger.error(f"获取数据库连接失败 {name}: {str(e)}")
                raise DatabaseError(f"数据库连接获取失败: {str(e)}")

    def _is_connection_valid(self, driver: SQLAlchemyDriver) -> bool:
        """
        全面的连接有效性检查

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 连接是否有效

        Note:
            提供多层次的连接有效性验证，包括基本状态检查和实际查询测试
            详细记录连接失败的原因，便于故障诊断
        """
        try:
            # 第一层检查：基本属性验证
            if not hasattr(driver, "is_connected"):
                logger.debug("驱动实例缺少is_connected属性")
                return False

            if not driver.is_connected:
                logger.debug("驱动实例标记为未连接状态")
                return False

            # 第二层检查：连接对象存在性检查
            if not hasattr(driver, "engine") or driver.engine is None:
                logger.debug("驱动实例缺少数据库引擎对象")
                return False

            # 第三层检查：实际查询测试
            try:
                return driver.test_connection()
            except Exception as query_error:
                logger.debug(f"连接查询测试失败: {str(query_error)}")
                return False

        except Exception as e:
            logger.debug(f"连接有效性检查失败: {str(e)}")
            return False

    def test_connection(self, name: str) -> bool:
        """
        测试连接是否有效

        Args:
            name: 连接名称

        Returns:
            True表示连接成功，False表示连接失败

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
            if success:
                logger.info(f"连接测试成功: {name}")
            else:
                logger.warning(f"连接测试失败: {name}")

            # 测试完成后立即清理连接，避免连接池污染
            self._cleanup_connection(name)

            return success
        except ConnectionError as e:
            # 连接错误，已经在get_connection中处理过
            try:
                self._cleanup_connection(name)
            except Exception:
                pass  # 忽略清理过程中的异常
            logger.error(f"连接测试失败 {name}: {str(e)}")
            return False
        except ConfigError as e:
            # 配置错误
            logger.error(f"连接测试失败（配置错误） {name}: {str(e)}")
            return False
        except Exception as e:
            # 其他错误
            # 确保异常情况下也清理连接
            try:
                self._cleanup_connection(name)
            except Exception:
                pass  # 忽略清理过程中的异常
            
            # 分析错误类型，提供更详细的错误信息
            error_message = str(e)
            error_lower = error_message.lower()
            
            if "timeout" in error_lower:
                logger.error(f"连接测试失败（连接超时） {name}: {error_message}")
            elif "refused" in error_lower:
                logger.error(f"连接测试失败（连接被拒绝） {name}: {error_message}")
            elif "unreachable" in error_lower:
                logger.error(f"连接测试失败（主机不可达） {name}: {error_message}")
            elif "permission" in error_lower or "access denied" in error_lower:
                logger.error(f"连接测试失败（权限错误） {name}: {error_message}")
            elif "database" in error_lower and "not found" in error_lower:
                logger.error(f"连接测试失败（数据库不存在） {name}: {error_message}")
            else:
                logger.error(f"连接测试失败（未知错误） {name}: {error_message}")
            
            return False

    def execute_query(
        self, connection_name: str, query: str, params: Dict[str, Any] | None = None
    ) -> List[Dict[str, Any]]:
        """
        执行SQL查询语句

        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数字典，可选

        Returns:
            查询结果列表，每行数据为字典格式

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
            
            # 更新连接信息
            if connection_name in self.connection_pool:
                connection_info = self.connection_pool[connection_name]
                connection_info.mark_query(response_time)
                connection_info.mark_used()
            
            return result

        try:
            return _execute_query()
        except (ConfigError, ConnectionError):
            raise
        except Exception as e:
            # 记录错误信息
            if connection_name in self.connection_pool:
                connection_info = self.connection_pool[connection_name]
                connection_info.mark_error(str(e))
            self._handle_exception("查询执行", connection_name, e)

    def execute_command(
        self,
        connection_name: str,
        command: str,
        params: Dict[str, Any] | None = None,
    ) -> int:
        """
        执行非查询SQL命令（INSERT/UPDATE/DELETE等）

        Args:
            connection_name: 连接名称
            command: SQL命令语句
            params: 命令参数字典，可选

        Returns:
            影响的行数

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
            
            # 更新连接信息
            if connection_name in self.connection_pool:
                connection_info = self.connection_pool[connection_name]
                connection_info.mark_query(response_time)
                connection_info.mark_transaction()
                connection_info.mark_used()
            
            return result

        try:
            return _execute_command()
        except (ConfigError, ConnectionError):
            raise
        except Exception as e:
            # 记录错误信息
            if connection_name in self.connection_pool:
                connection_info = self.connection_pool[connection_name]
                connection_info.mark_error(str(e))
            self._handle_exception("命令执行", connection_name, e)

    def close_connection(self, name: str) -> None:
        """
        关闭数据库连接

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
            logger.info(f"数据库连接已关闭: {name}")

        with self._lock:
            self._safe_operation("连接关闭", name, _close_connection)

    def close_all_connections(self) -> None:
        """
        安全关闭所有数据库连接

        彻底清理所有活跃的连接资源，确保无资源泄漏。

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
                success_count = 0
                error_count = 0

                if total_connections == 0:
                    logger.info("连接池为空，无需关闭连接")
                    return

                logger.info(f"开始关闭所有连接，共 {total_connections} 个连接")

                for name in connection_names:
                    try:
                        # 使用内部清理方法，避免递归调用close_connection
                        self._cleanup_connection(name)
                        success_count += 1
                        logger.debug(f"连接 {name} 关闭成功")
                    except Exception as e:
                        error_count += 1
                        logger.error(f"关闭连接 {name} 失败: {str(e)}")

                # 最终检查连接池是否完全清空
                remaining_connections = len(self.connection_pool)
                if remaining_connections > 0:
                    logger.warning(
                        f"连接池清理不完整，仍有 {remaining_connections} 个连接未清理"
                    )
                    # 强制清空连接池
                    self.connection_pool.clear()
                    logger.info("已强制清空连接池")

                # 记录详细汇总信息
                if error_count > 0:
                    logger.warning(
                        f"关闭所有连接完成，成功: {success_count}, 失败: {error_count}, 总数: {total_connections}"
                    )
                else:
                    logger.info(f"所有数据库连接已安全关闭，共 {success_count} 个连接")

            except Exception as e:
                logger.error(f"关闭所有连接时发生严重异常: {str(e)}")
                # 即使发生异常也要尝试清理连接池
                try:
                    self.connection_pool.clear()
                    logger.info("异常情况下已强制清空连接池")
                except Exception:
                    pass
                raise DatabaseError(f"关闭所有连接失败: {str(e)}")

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """
        获取连接详细信息（包含统计信息）

        Args:
            name: 连接名称

        Returns:
            连接信息字典，包含基本配置和统计信息

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
            if name in self.connection_pool:
                connection_info = self.connection_pool[name]
                info.update(
                    {
                        "use_count": connection_info.use_count,
                        "last_used": connection_info.last_used,
                        "created_at": connection_info.created_at,
                        "is_active": connection_info.is_active,
                        "connection_errors": connection_info.connection_errors,
                        "last_error": connection_info.last_error,
                        "response_time": connection_info.response_time,
                        "transaction_count": connection_info.transaction_count,
                        "query_count": connection_info.query_count,
                        "last_query_time": connection_info.last_query_time,
                    }
                )

            return {k: v for k, v in info.items() if v is not None}

        with self._lock:
            return self._safe_operation("连接信息获取", name, _get_connection_info)

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取连接统计信息

        Returns:
            统计信息字典，包含连接创建、关闭、错误等统计

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
                [conn for conn in self.connection_pool.values() if conn.is_active]
            )
            stats["connection_pool_size"] = len(self.connection_pool)
            return stats

    def cleanup_idle_connections(
        self, max_idle_time: int = DEFAULT_MAX_IDLE_TIME
    ) -> int:
        """
        清理空闲时间过长的连接

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
            cleaned_count = 0

            if not connection_names:
                logger.debug("连接池为空，无需清理空闲连接")
                return 0

            logger.info(f"开始清理空闲连接，最大空闲时间: {max_idle_time}秒")

            for name in connection_names:
                if name not in self.connection_pool:
                    continue

                connection_info = self.connection_pool[name]
                idle_time = current_time - connection_info.last_used

                if idle_time > max_idle_time:
                    try:
                        logger.debug(
                            f"连接 {name} 空闲时间 {idle_time:.1f}秒超过限制，执行清理"
                        )
                        self._cleanup_connection(name)
                        cleaned_count += 1
                        self._statistics["idle_connections_cleaned"] += 1
                    except Exception as e:
                        logger.warning(f"清理空闲连接 {name} 失败: {str(e)}")

            self._statistics["last_cleanup_time"] = current_time

            if cleaned_count > 0:
                logger.info(f"空闲连接清理完成，共清理 {cleaned_count} 个连接")
            else:
                logger.debug("未发现需要清理的空闲连接")

            return cleaned_count

        with self._lock:
            return self._safe_operation("空闲连接清理", "all", _cleanup_idle_connections)

    def get_connection_pool_status(self) -> Dict[str, Any]:
        """
        获取连接池状态信息

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
            active_connections = 0
            total_use_count = 0
            max_idle_time = 0
            total_query_count = 0
            total_transaction_count = 0
            total_errors = 0
            total_response_time = 0.0
            connection_details = {}

            for name, connection_info in self.connection_pool.items():
                if connection_info.is_active:
                    active_connections += 1
                    total_use_count += connection_info.use_count
                    total_query_count += connection_info.query_count
                    total_transaction_count += connection_info.transaction_count
                    total_errors += connection_info.connection_errors
                    total_response_time += connection_info.response_time
                    idle_time = current_time - connection_info.last_used
                    max_idle_time = max(max_idle_time, idle_time)
                
                # 记录每个连接的详细信息
                connection_details[name] = {
                    "is_active": connection_info.is_active,
                    "use_count": connection_info.use_count,
                    "query_count": connection_info.query_count,
                    "transaction_count": connection_info.transaction_count,
                    "connection_errors": connection_info.connection_errors,
                    "response_time": connection_info.response_time,
                    "last_used": connection_info.last_used,
                    "idle_time": current_time - connection_info.last_used,
                    "last_error": connection_info.last_error,
                }

            # 计算平均值
            pool_size = len(self.connection_pool)
            average_response_time = total_response_time / pool_size if pool_size > 0 else 0.0
            error_rate = total_errors / (total_query_count + 1) if total_query_count > 0 else 0.0

            return {
                "active_connections": active_connections,
                "total_connections": pool_size,
                "total_use_count": total_use_count,
                "total_query_count": total_query_count,
                "total_transaction_count": total_transaction_count,
                "total_errors": total_errors,
                "average_response_time": average_response_time,
                "error_rate": error_rate,
                "max_idle_time": max_idle_time,
                "statistics": self._statistics.copy(),
                "uptime": current_time - self._statistics["start_time"],
                "last_cleanup": current_time - self._statistics["last_cleanup_time"],
                "connection_details": connection_details,
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
            config = self.show_connection(name)
            diagnosis["details"]["config"] = {
                "valid": True,
                "type": config.get("type"),
                "host": config.get("host"),
                "port": config.get("port"),
                "database": config.get("database"),
            }

            # 2. 尝试获取连接
            try:
                driver = self.get_connection(name)
                diagnosis["details"]["connection"] = {
                    "established": True,
                    "driver_type": type(driver).__name__,
                }

                # 3. 测试连接
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
                except Exception as test_error:
                    diagnosis["status"] = "unhealthy"
                    diagnosis["details"]["test"] = {
                        "success": False,
                        "error": str(test_error),
                    }

                # 4. 清理连接
                self._cleanup_connection(name)
            except Exception as connect_error:
                diagnosis["status"] = "unhealthy"
                diagnosis["details"]["connection"] = {
                    "established": False,
                    "error": str(connect_error),
                }

        except ConfigError as e:
            diagnosis["status"] = "error"
            diagnosis["details"]["config"] = {
                "valid": False,
                "error": str(e),
            }
        except Exception as e:
            diagnosis["status"] = "error"
            diagnosis["details"]["general_error"] = str(e)

        # 5. 添加连接池信息
        if name in self.connection_pool:
            connection_info = self.connection_pool[name]
            diagnosis["details"]["pool_info"] = {
                "is_active": connection_info.is_active,
                "use_count": connection_info.use_count,
                "connection_errors": connection_info.connection_errors,
                "last_error": connection_info.last_error,
                "response_time": connection_info.response_time,
            }

        return diagnosis

    def _handle_exception(self, operation: str, name: str, exception: Exception) -> None:
        """
        通用异常处理方法

        Args:
            operation: 操作名称，用于日志记录
            name: 连接名称
            exception: 捕获到的异常

        Raises:
            ConfigError: 如果原始异常是ConfigError
            ConnectionError: 如果原始异常是ConnectionError
            DatabaseError: 其他所有异常都会被转换为DatabaseError
        """
        if isinstance(exception, (ConfigError, ConnectionError)):
            raise
        
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
            ConnectionError: 如果执行过程中发生ConnectionError
            DatabaseError: 其他所有异常都会被转换为DatabaseError
        """
        try:
            return func(*args, **kwargs)
        except (ConfigError, ConnectionError):
            raise
        except Exception as e:
            self._handle_exception(operation, name, e)

    def __str__(self) -> str:
        """返回数据库管理器的字符串表示"""
        stats = self.get_statistics()
        return (
            f"DatabaseManager(app_name='{self.app_name}', "
            f"active_connections={stats['active_connections']}, "
            f"pool_size={stats['connection_pool_size']})"
        )
