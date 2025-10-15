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
}

# 默认配置参数
DEFAULT_MAX_IDLE_TIME = 300  # 5分钟


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

    Example:
        >>> connection_info = ConnectionInfo(driver)
        >>> connection_info.mark_used()
        >>> print(f"使用次数: {connection_info.use_count}")
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

    def mark_used(self) -> None:
        """标记连接被使用，更新最后使用时间和使用次数"""
        self.last_used = time.time()
        self.use_count += 1

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
        return (
            f"ConnectionInfo(use_count={self.use_count}, "
            f"last_used={last_used_str}, created_at={created_str}, "
            f"is_active={self.is_active})"
        )


class DatabaseManager:
    """
    数据库管理器类

    提供统一的数据库连接管理接口，实现连接池管理和生命周期控制。
    支持多种数据库类型，提供线程安全的连接操作。

    Attributes:
        app_name (str): 应用名称，用于配置文件的命名空间
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

    def __init__(self, app_name: str = "db_connector") -> None:
        """
        初始化数据库管理器

        Args:
            app_name: 应用名称，用于配置文件的命名空间和日志标识

        Raises:
            ConfigError: 当配置管理器初始化失败时

        Note:
            建议为每个应用使用唯一的 app_name，避免配置冲突和日志混淆

        Example:
            >>> db_manager = DatabaseManager("my_app")
            >>> print(f"应用名称: {db_manager.app_name}")
        """
        try:
            self.app_name = app_name
            self.config_manager = ConfigManager(app_name)
            self.connection_pool: Dict[str, ConnectionInfo] = {}
            self._lock = threading.RLock()
            self._statistics = {
                "total_connections_created": 0,
                "total_connections_closed": 0,
                "connection_errors": 0,
                "start_time": time.time(),
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
        with self._lock:
            try:
                # 检查连接是否已存在
                existing_connections = self.list_connections()
                if name in existing_connections:
                    raise ConfigError(CONNECTION_ALREADY_EXISTS_MSG.format(name))

                # 简化配置验证
                self._validate_basic_config(connection_config)

                # 保存到配置管理器
                self.config_manager.add_connection(name, connection_config)
                logger.info(f"数据库连接配置已创建: {name}")

            except ConfigError:
                raise
            except Exception as e:
                logger.error(f"创建数据库连接配置失败 {name}: {str(e)}")
                raise DatabaseError(f"数据库连接配置创建失败: {str(e)}")

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

        # SQLite数据库特殊处理
        if db_type == "sqlite" and "database" not in config:
            config["database"] = ":memory:"
            return

        # 为特定数据库类型设置默认值
        if db_type == "oracle" and "service_name" not in config:
            config["service_name"] = "XE"
        elif db_type == "postgresql" and "gssencmode" not in config:
            config["gssencmode"] = "disable"
        elif db_type == "mssql":
            if "charset" not in config:
                config["charset"] = "cp936"
            if "tds_version" not in config:
                config["tds_version"] = "7.0"

        # 验证必需参数
        required_fields = ["username", "password", "host"]
        missing_fields = [field for field in required_fields if field not in config]
        if missing_fields:
            raise ConfigError(f"缺少必需的连接参数: {', '.join(missing_fields)}")

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
        with self._lock:
            try:
                # 检查连接是否存在
                self._validate_connection_exists(name)

                # 先关闭连接
                self._cleanup_connection(name)

                # 删除配置
                self.config_manager.remove_connection(name)
                logger.info(f"连接配置已删除: {name}")

            except ConfigError:
                raise
            except Exception as e:
                logger.error(f"删除连接配置失败 {name}: {str(e)}")
                raise DatabaseError(f"连接配置删除失败: {str(e)}")

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
        清理连接资源

        Args:
            name: 连接名称

        Note:
            安全地关闭连接并清理相关资源
            即使发生异常也不会影响其他连接
        """
        try:
            if name in self.connection_pool:
                connection_info = self.connection_pool[name]
                if connection_info.driver.is_connected:
                    connection_info.driver.disconnect()
                del self.connection_pool[name]
                self._statistics["total_connections_closed"] += 1
                logger.debug(f"清理连接: {name}")
        except Exception as e:
            logger.warning(f"清理连接时发生异常 {name}: {str(e)}")

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
        with self._lock:
            try:
                # 检查连接是否存在
                self._validate_connection_exists(name)

                # 简化配置验证
                self._validate_basic_config(connection_config)

                # 关闭现有连接
                self._cleanup_connection(name)

                # 更新配置
                self.config_manager.update_connection(name, connection_config)
                logger.info(f"连接配置已更新: {name}")

            except ConfigError:
                raise
            except Exception as e:
                logger.error(f"更新连接配置失败 {name}: {str(e)}")
                raise DatabaseError(f"连接配置更新失败: {str(e)}")

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
            4. 返回可用的驱动实例

        Note:
            如果连接已存在且有效，直接返回缓存的连接，否则创建新连接
            连接池机制提高性能，避免频繁创建和销毁连接
            当提供config_overrides时，会创建新的连接而不是使用缓存

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

                    # 创建新连接
                    driver = SQLAlchemyDriver(connection_config)
                    driver.connect()

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

                # 创建新连接
                connection_config = self.show_connection(name)
                driver = SQLAlchemyDriver(connection_config)
                driver.connect()

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
        检查连接是否有效

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 连接是否有效

        Note:
            使用轻量级的测试查询验证连接状态
            避免使用复杂的验证逻辑影响性能
        """
        try:
            return driver.test_connection()
        except Exception:
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
                logger.debug(f"连接测试成功: {name}")
            else:
                logger.warning(f"连接测试失败: {name}")
            return success
        except Exception as e:
            logger.error(f"连接测试失败 {name}: {str(e)}")
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
            2. 执行查询语句
            3. 返回格式化结果

        Note:
            此方法封装了连接获取和错误处理，提供统一的查询接口
            适合执行SELECT等查询操作

        Example:
            >>> results = db_manager.execute_query(
            ...     "mysql_db",
            ...     "SELECT * FROM users WHERE age > :age",
            ...     {"age": 18}
            ... )
        """
        try:
            driver = self.get_connection(connection_name)
            with driver:
                return driver.execute_query(query, params)
        except (ConfigError, ConnectionError):
            raise
        except Exception as e:
            logger.error(f"执行查询失败: {str(e)}")
            raise DatabaseError(f"查询执行失败: {str(e)}")

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
            2. 执行命令语句
            3. 返回影响行数

        Note:
            适合执行数据修改操作，返回受影响的行数
            提供事务安全的命令执行环境

        Example:
            >>> affected_rows = db_manager.execute_command(
            ...     "mysql_db",
            ...     "UPDATE users SET name = :name WHERE id = :id",
            ...     {"name": "Bob", "id": 1}
            ... )
        """
        try:
            driver = self.get_connection(connection_name)
            with driver:
                return driver.execute_command(command, params)
        except (ConfigError, ConnectionError):
            raise
        except Exception as e:
            logger.error(f"执行命令失败: {str(e)}")
            raise DatabaseError(f"命令执行失败: {str(e)}")

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
        with self._lock:
            try:
                self._cleanup_connection(name)
                logger.info(f"数据库连接已关闭: {name}")
            except Exception as e:
                logger.error(f"关闭连接失败 {name}: {str(e)}")
                raise DatabaseError(f"连接关闭失败: {str(e)}")

    def close_all_connections(self) -> None:
        """
        关闭所有数据库连接

        清理所有活跃的连接资源，释放数据库连接池。

        Raises:
            DatabaseError: 当关闭所有连接失败时

        Process:
            1. 获取所有连接名称的副本
            2. 逐个关闭连接
            3. 统计成功和失败数量
            4. 记录汇总日志

        Note:
            使用list()创建副本确保线程安全，避免在迭代过程中修改字典
            适合在应用关闭或维护时调用

        Example:
            >>> db_manager.close_all_connections()
        """
        with self._lock:
            try:
                # 注意：这里必须使用list()创建副本，因为在迭代过程中会调用close_connection
                # 这可能会修改connection_pool字典，导致RuntimeError
                connection_names = list(self.connection_pool.keys())
                success_count = 0
                error_count = 0

                for name in connection_names:
                    try:
                        self.close_connection(name)
                        success_count += 1
                    except Exception:
                        error_count += 1

                if error_count > 0:
                    logger.warning(
                        f"关闭所有连接完成，成功: {success_count}, 失败: {error_count}"
                    )
                else:
                    logger.info(f"所有数据库连接已关闭，共 {success_count} 个连接")
            except Exception as e:
                logger.error(f"关闭所有连接失败: {str(e)}")
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
        """
        with self._lock:
            try:
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
                        }
                    )

                return {k: v for k, v in info.items() if v is not None}

            except ConfigError:
                raise
            except Exception as e:
                logger.error(f"获取连接信息失败 {name}: {str(e)}")
                raise DatabaseError(f"连接信息获取失败: {str(e)}")

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
        清理空闲连接

        Args:
            max_idle_time: 最大空闲时间（秒），默认5分钟

        Returns:
            清理的连接数量

        Process:
            1. 获取当前时间
            2. 遍历连接池，检查空闲时间
            3. 清理超时的空闲连接
            4. 返回清理数量

        Note:
            定期调用此方法可以释放长时间未使用的连接资源
            避免连接泄漏和资源浪费

        Example:
            >>> cleaned_count = db_manager.cleanup_idle_connections(600)  # 10分钟
        """
        with self._lock:
            current_time = time.time()
            cleaned_count = 0

            # 注意：这里必须使用list()创建副本，因为在迭代过程中会调用_cleanup_connection
            # 这会删除字典项，导致RuntimeError
            connection_pool = list(self.connection_pool.items())
            for name, connection_info in connection_pool:
                if (current_time - connection_info.last_used) > max_idle_time:
                    try:
                        self._cleanup_connection(name)
                        cleaned_count += 1
                        logger.debug(f"清理空闲连接: {name}")
                    except Exception as e:
                        logger.warning(f"清理空闲连接失败 {name}: {str(e)}")

            if cleaned_count > 0:
                logger.info(f"清理了 {cleaned_count} 个空闲连接")

            return cleaned_count

    def __str__(self) -> str:
        """返回数据库管理器的字符串表示"""
        stats = self.get_statistics()
        return (
            f"DatabaseManager(app_name='{self.app_name}', "
            f"active_connections={stats['active_connections']}, "
            f"pool_size={stats['connection_pool_size']})"
        )
