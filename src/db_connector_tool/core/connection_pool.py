"""连接池管理模块 (ConnectionPoolManager)

提供连接池管理、优化和统计信息收集功能，
负责连接的创建、复用、清理和性能监控。

Example:
>>> from db_connector_tool.core.connection_pool import ConnectionPoolManager
>>> pool_manager = ConnectionPoolManager()
>>> pool_manager.add_connection('mysql_db', driver_instance)
>>> driver = pool_manager.get_connection('mysql_db')
>>> pool_manager.cleanup_idle_connections()
>>> stats = pool_manager.get_statistics()
"""

import threading
import time
from typing import Any, Dict, List, Optional

from typing import Any
from ..utils.logging_utils import get_logger
from .exceptions import DatabaseError

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class ConnectionPoolManager:
    """连接池管理器类 (Connection Pool Manager)

    负责数据库连接池的管理、优化和统计信息收集，
    提供连接的创建、复用、清理和性能监控功能。

    Attributes:
        connection_pool (Dict[str, SQLAlchemyDriver]): 连接池字典
        _lock (threading.RLock): 可重入锁，确保线程安全
        _statistics (Dict[str, Any]): 连接统计信息
        _connection_metadata (Dict[str, Dict[str, Any]]): 连接元数据

    Example:
        >>> pool_manager = ConnectionPoolManager()
        >>> pool_manager.add_connection('mysql_db', driver_instance)
        >>> driver = pool_manager.get_connection('mysql_db')
        >>> pool_manager.cleanup_idle_connections()
        >>> stats = pool_manager.get_statistics()
    """

    def __init__(self) -> None:
        """初始化连接池管理器

        创建新的连接池管理器实例，初始化连接池和统计信息。

        Example:
            >>> pool_manager = ConnectionPoolManager()
        """
        self.connection_pool: Dict[str, Any] = {}
        self._lock = threading.RLock()
        self._statistics = {
            "connections_created": 0,
            "connections_closed": 0,
            "connection_errors": 0,
            "idle_connections_cleaned": 0,
            "start_time": time.time(),
            "last_cleanup_time": time.time(),
        }
        # 用于跟踪连接的使用情况
        self._connection_metadata: Dict[str, Dict[str, Any]] = {}
        logger.info("连接池管理器初始化成功")

    def get_statistics(self) -> Dict[str, Any]:
        """获取连接统计信息

        获取数据库连接的统计信息，包含连接创建、关闭、错误等统计数据。

        Returns:
            Dict[str, Any]: 统计信息字典，包含连接创建、关闭、错误等统计

        Example:
            >>> stats = pool_manager.get_statistics()
            >>> print(f"总连接数: {stats['connections_created']}")
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

    def close_all_connections(self) -> tuple[int, int]:
        """关闭所有数据库连接

        关闭所有数据库连接，返回成功和失败的数量。

        Returns:
            tuple[int, int]: (成功数量, 失败数量)

        Example:
            >>> success_count, error_count = pool_manager.close_all_connections()
        """
        with self._lock:
            # 创建连接名称副本，避免迭代过程中字典修改
            connection_names = list(self.connection_pool.keys())
            total_connections = len(connection_names)

            if total_connections == 0:
                logger.debug("连接池为空，无需关闭连接")
                return 0, 0

            logger.debug("开始关闭所有连接，共 %s 个连接", total_connections)

            success_count, error_count = self._close_all_connections(connection_names)

            # 最终检查连接池是否完全清空
            remaining_connections = len(self.connection_pool)
            if remaining_connections > 0:
                logger.warning(
                    "连接池清理不完整，仍有 %s 个连接未清理", remaining_connections
                )
                # 强制清空连接池
                self.connection_pool.clear()
                logger.debug("已强制清空连接池")

            # 记录详细汇总信息
            if error_count > 0:
                logger.warning(
                    "关闭所有连接完成，成功: %s, 失败: %s, 总数: %s",
                    success_count,
                    error_count,
                    total_connections,
                )
            else:
                logger.debug("所有数据库连接已安全关闭，共 %s 个连接", success_count)

            return success_count, error_count

    def _close_all_connections(self, connection_names: List[str]) -> tuple[int, int]:
        """关闭所有连接并返回成功和失败的数量

        关闭所有指定的连接并返回成功和失败的数量。

        Args:
            connection_names: 连接名称列表

        Returns:
            tuple[int, int]: (成功数量, 失败数量)

        Example:
            >>> success_count, error_count = pool_manager._close_all_connections(names)
        """
        success_count = 0
        error_count = 0

        for name in connection_names:
            try:
                # 使用内部清理方法
                self.remove_connection(name)
                success_count += 1
                logger.debug("连接 %s 关闭成功", name)
            except (OSError, DatabaseError) as error:
                error_count += 1
                logger.error("关闭连接 %s 失败: %s", name, str(error))

        return success_count, error_count

    def remove_connection(self, name: str) -> None:
        """从连接池移除连接

        从连接池移除指定名称的连接，并清理相关资源。

        Args:
            name: 连接名称

        Example:
            >>> pool_manager.remove_connection('mysql_db')
        """
        with self._lock:
            if not self._is_connection_in_pool(name):
                return

            driver = self.connection_pool[name]

            try:
                # 安全关闭连接
                if self._check_driver_basic_status(driver):
                    driver.disconnect()
                    logger.debug("连接 %s 已安全关闭", name)
                else:
                    logger.debug("连接 %s 未连接或已关闭", name)
            except (OSError, DatabaseError) as error:
                logger.error("清理连接 %s 时发生严重异常: %s", name, str(error))
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

        Example:
            >>> in_pool = pool_manager._is_connection_in_pool('mysql_db')
        """
        if name not in self.connection_pool:
            logger.debug("连接 %s 不在连接池中，无需清理", name)
            return False
        return True

    def _check_driver_basic_status(self, driver: Any) -> bool:
        """检查驱动实例的基本状态

        检查数据库驱动实例的基本状态是否有效。

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 驱动实例状态是否有效

        Example:
            >>> is_valid = pool_manager._check_driver_basic_status(driver)
        """
        # 检查驱动实例是否有效
        if driver is None:
            logger.debug("驱动实例为None")
            return False

        # 检查驱动实例是否有必要的方法
        if not hasattr(driver, "test_connection"):
            logger.debug("驱动实例缺少test_connection方法")
            return False

        # 对于SQLAlchemyDriver，检查engine属性
        if hasattr(driver, "engine") and not driver.engine:
            logger.debug("驱动实例标记为未连接状态")
            return False

        return True

    def _remove_connection_from_pool(self, name: str) -> None:
        """从连接池中移除连接

        从连接池中移除指定的连接，并清理相关元数据。

        Args:
            name: 连接名称

        Example:
            >>> pool_manager._remove_connection_from_pool('mysql_db')
        """
        try:
            del self.connection_pool[name]
            # 清理元数据
            if name in self._connection_metadata:
                del self._connection_metadata[name]
            self._statistics["connections_closed"] += 1
            logger.debug("连接 %s 已从连接池中移除", name)
        except (OSError, DatabaseError) as error:
            logger.error("从连接池中移除连接 %s 时发生异常: %s", name, str(error))

    def get_connection(self, name: str) -> Optional[Any]:
        """从连接池获取连接

        从连接池获取指定名称的连接，如果连接无效则返回None。

        Args:
            name: 连接名称

        Returns:
            Optional[Any]: 数据库驱动实例，如果连接无效则返回None

        Example:
            >>> driver = pool_manager.get_connection('mysql_db')
        """
        with self._lock:
            if name not in self.connection_pool:
                logger.debug("连接 %s 不在连接池中", name)
                return None

            driver = self.connection_pool[name]

            # 检查连接是否有效
            if self._is_connection_valid(driver):
                # 更新使用时间
                if name in self._connection_metadata:
                    self._connection_metadata[name]["last_used"] = time.time()
                    self._connection_metadata[name]["use_count"] += 1
                logger.debug("使用缓存的数据库连接: %s", name)
                return driver

            # 连接无效，清理并返回None
            self._remove_connection_from_pool(name)
            return None

    def _is_connection_valid(self, driver: Any) -> bool:
        """检查连接是否有效

        检查数据库连接是否有效，包括基本状态检查和实际查询测试。

        Args:
            driver: 数据库驱动实例

        Returns:
            bool: 连接是否有效

        Example:
            >>> is_valid = pool_manager._is_connection_valid(driver)
        """
        try:
            # 检查驱动实例的基本状态
            if not self._check_driver_basic_status(driver):
                return False

            # 执行实际查询测试
            if hasattr(driver, 'test_connection'):
                try:
                    return driver.test_connection()
                except Exception as test_error:
                    logger.debug("连接测试失败: %s", str(test_error))
                    return False
            # 对于没有test_connection方法的驱动，检查基本连接状态
            # 检查是否有连接状态属性或方法
            if hasattr(driver, 'is_connected'):
                try:
                    return driver.is_connected()
                except Exception as e:
                    logger.debug("连接状态检查失败: %s", str(e))
                    return False
            # 检查是否有表示连接状态的属性
            if hasattr(driver, 'connected'):
                return bool(driver.connected)
            # 检查是否有engine属性（如SQLAlchemyDriver）
            if hasattr(driver, 'engine'):
                return bool(driver.engine)
            # 无法确定连接状态，返回False
            logger.warning("无法检查驱动连接状态，缺少必要的方法或属性")
            return False

        except (OSError, DatabaseError) as error:
            logger.debug("连接有效性检查失败: %s", str(error))
            return False
        except Exception as error:
            logger.debug("连接有效性检查发生未知错误: %s", str(error))
            return False

    def record_connection_error(self, connection_name: str, error: Exception) -> None:
        """记录连接错误

        记录指定连接的错误信息。

        Args:
            connection_name: 连接名称
            error: 错误对象

        Example:
            >>> pool_manager.record_connection_error('mysql_db', error)
        """
        if connection_name in self._connection_metadata:
            self._connection_metadata[connection_name]["connection_errors"] += 1
            self._connection_metadata[connection_name]["last_error"] = str(error)
        self._statistics["connection_errors"] += 1

    def add_connection(self, name: str, driver: Any) -> None:
        """添加连接到连接池

        将数据库驱动实例添加到连接池，并初始化元数据。

        Args:
            name: 连接名称
            driver: 数据库驱动实例

        Example:
            >>> pool_manager.add_connection('mysql_db', driver_instance)
        """
        with self._lock:
            # 验证驱动实例的有效性
            if not self._check_driver_basic_status(driver):
                logger.error("驱动实例无效，缺少必要的方法或属性")
                raise DatabaseError("驱动实例无效，缺少必要的方法或属性")

            # 验证驱动实例是否可连接
            try:
                if hasattr(driver, 'test_connection'):
                    if not driver.test_connection():
                        logger.error("驱动实例连接测试失败")
                        raise DatabaseError("驱动实例连接测试失败")
            except Exception as e:
                logger.error("驱动实例连接测试异常: %s", str(e))
                raise DatabaseError(f"驱动实例连接测试异常: {str(e)}") from e

            self.connection_pool[name] = driver

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

            self._statistics["connections_created"] += 1
            logger.info("数据库连接已添加到连接池: %s", name)

    def update_query_metadata(self, connection_name: str, response_time: float) -> None:
        """更新查询元数据

        更新指定连接的查询元数据信息。

        Args:
            connection_name: 连接名称
            response_time: 响应时间

        Example:
            >>> pool_manager.update_query_metadata('mysql_db', 0.1)
        """
        if connection_name in self._connection_metadata:
            self._connection_metadata[connection_name]["last_used"] = time.time()
            self._connection_metadata[connection_name]["last_query_time"] = time.time()
            self._connection_metadata[connection_name]["response_time"] = response_time
            self._connection_metadata[connection_name]["query_count"] += 1

    def update_command_metadata(
        self, connection_name: str, response_time: float
    ) -> None:
        """更新命令元数据

        更新指定连接的命令元数据信息。

        Args:
            connection_name: 连接名称
            response_time: 响应时间

        Example:
            >>> pool_manager.update_command_metadata('mysql_db', 0.1)
        """
        if connection_name in self._connection_metadata:
            self._connection_metadata[connection_name]["last_used"] = time.time()
            self._connection_metadata[connection_name]["last_query_time"] = time.time()
            self._connection_metadata[connection_name]["response_time"] = response_time
            self._connection_metadata[connection_name]["transaction_count"] += 1

    def get_connection_info(self, name: str) -> Dict[str, Any]:
        """获取连接详细信息（包含统计信息）

        获取指定连接的详细信息，包含基本配置和统计信息。

        Args:
            name: 连接名称

        Returns:
            Dict[str, Any]: 连接信息字典，包含基本配置和统计信息

        Example:
            >>> info = pool_manager.get_connection_info('mysql_db')
            >>> print(f"使用次数: {info['use_count']}")
            >>> print(f"最后使用时间: {info['last_used']}")
        """
        with self._lock:
            if name not in self._connection_metadata:
                return {}

            metadata = self._connection_metadata[name]
            info = {
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

            return {k: v for k, v in info.items() if v is not None}

    def cleanup_idle_connections(self, max_idle_time: int = 300) -> int:
        """清理空闲时间过长的连接

        清理空闲时间超过指定阈值的数据库连接。

        Args:
            max_idle_time: 最大空闲时间（秒），默认5分钟

        Returns:
            int: 清理的连接数量

        Example:
            >>> cleaned_count = pool_manager.cleanup_idle_connections(600)  # 10分钟
            >>> print(f"清理了 {cleaned_count} 个空闲连接")
        """

        def _cleanup_idle_connections():
            current_time = time.time()
            connection_names = list(self.connection_pool.keys())

            if not connection_names:
                logger.debug("连接池为空，无需清理空闲连接")
                return 0

            logger.debug("开始清理空闲连接，最大空闲时间: %s秒", max_idle_time)

            cleaned_count = self._process_idle_connections(
                connection_names, current_time, max_idle_time
            )
            self._statistics["last_cleanup_time"] = current_time

            if cleaned_count > 0:
                logger.info("空闲连接清理完成，共清理 %s 个连接", cleaned_count)
            else:
                logger.debug("未发现需要清理的空闲连接")

            return cleaned_count

        with self._lock:
            return _cleanup_idle_connections()

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

        Example:
            >>> cleaned_count = pool_manager._process_idle_connections(names, time.time(), 300)
        """

        cleaned_count = 0

        for name in connection_names:
            if not self._is_connection_in_pool(name):
                continue

            if name in self._connection_metadata and "last_used" in self._connection_metadata[name]:
                idle_time = current_time - self._connection_metadata[name]["last_used"]
            else:
                # 如果没有元数据或last_used字段，使用创建时间或当前时间作为默认值
                if name in self._connection_metadata and "created_at" in self._connection_metadata[name]:
                    idle_time = current_time - self._connection_metadata[name]["created_at"]
                else:
                    idle_time = 0

            if idle_time > max_idle_time:
                logger.debug(
                    "连接 %s 空闲时间 %.1f秒超过限制，执行清理", name, idle_time
                )
                self.remove_connection(name)
                self._statistics["idle_connections_cleaned"] += 1
                cleaned_count += 1

        return cleaned_count

    def get_connection_pool_status(self) -> Dict[str, Any]:
        """获取连接池状态信息

        获取数据库连接池的状态信息，包含活跃连接数、平均响应时间等。

        Returns:
            Dict[str, Any]: 连接池状态信息字典

        Example:
            >>> status = pool_manager.get_connection_pool_status()
            >>> print(f"活跃连接数: {status['active_connections']}")
            >>> print(f"平均响应时间: {status['average_response_time']:.3f}s")
        """
        with self._lock:
            current_time = time.time()

            # 计算连接池统计信息
            stats = self._calculate_pool_stats()
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

    def _calculate_pool_stats(self) -> Dict[str, Any]:
        """计算连接池统计信息

        计算连接池的统计信息，包含活跃连接数、总使用次数等。

        Returns:
            Dict[str, Any]: 连接池统计信息

        Example:
            >>> stats = pool_manager._calculate_pool_stats()
        """
        total_use_count = 0
        total_query_count = 0
        total_transaction_count = 0
        total_response_time = 0.0
        total_errors = 0

        for _, metadata in self._connection_metadata.items():
            total_use_count += metadata.get("use_count", 0)
            total_query_count += metadata.get("query_count", 0)
            total_transaction_count += metadata.get("transaction_count", 0)
            total_response_time += metadata.get("response_time", 0.0)
            total_errors += metadata.get("connection_errors", 0)

        return {
            "total_use_count": total_use_count,
            "total_query_count": total_query_count,
            "total_transaction_count": total_transaction_count,
            "total_response_time": total_response_time,
            "total_errors": total_errors,
        }

    def _get_connection_details(self, current_time: float) -> List[Dict[str, Any]]:
        """获取连接详细信息列表

        获取所有连接的详细信息列表。

        Args:
            current_time: 当前时间

        Returns:
            List[Dict[str, Any]]: 连接详细信息列表

        Example:
            >>> details = pool_manager._get_connection_details(time.time())
        """
        details = []
        for name, metadata in self._connection_metadata.items():
            idle_time = current_time - metadata.get("last_used", current_time)
            detail = {
                "name": name,
                "idle_time": idle_time,
                "use_count": metadata.get("use_count", 0),
                "response_time": metadata.get("response_time", 0.0),
                "connection_errors": metadata.get("connection_errors", 0),
                "is_active": name in self.connection_pool,
            }
            details.append(detail)
        return details

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

        Example:
            >>> avg_time = pool_manager._calculate_average_response_time(10.0, 5)
        """
        if pool_size > 0:
            return total_response_time / pool_size
        return 0.0

    def _calculate_error_rate(self, total_errors: int, total_queries: int) -> float:
        """计算错误率

        计算连接池的错误率。

        Args:
            total_errors: 总错误数
            total_queries: 总查询数

        Returns:
            float: 错误率

        Example:
            >>> error_rate = pool_manager._calculate_error_rate(5, 100)
        """
        if total_queries > 0:
            return total_errors / total_queries
        return 0.0

    def _build_pool_status_response(
        self, status_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """构建连接池状态响应

        构建连接池状态响应字典。

        Args:
            status_data: 状态数据

        Returns:
            Dict[str, Any]: 连接池状态响应

        Example:
            >>> response = pool_manager._build_pool_status_response(status_data)
        """

        active_count = 0
        for conn in self.connection_pool.values():
            if self._is_connection_valid(conn):
                active_count += 1

        return {
            "current_time": status_data["current_time"],
            "pool_size": status_data["pool_size"],
            "active_connections": active_count,
            "average_response_time": status_data["average_response_time"],
            "error_rate": status_data["error_rate"],
            "connection_details": status_data["connection_details"],
            "statistics": {
                "connections_created": self._statistics["connections_created"],
                "connections_closed": self._statistics["connections_closed"],
                "connection_errors": self._statistics["connection_errors"],
                "idle_connections_cleaned": self._statistics[
                    "idle_connections_cleaned"
                ],
                "uptime": status_data["current_time"] - self._statistics["start_time"],
            },
        }
