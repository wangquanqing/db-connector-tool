"""批量数据库连接管理器模块 (Batch Manager)

管理大量配置相似的数据库连接（临时性配置），
支持基于模板的批量配置、并发执行、表结构升级和 IP 范围生成。

Example:
>>> from db_connector_tool import BatchDatabaseManager, generate_ip_range
>>>
>>> batch_manager = BatchDatabaseManager("my_app")
>>> base_config = {
...     "type": "mysql",
...     "port": 3306,
...     "username": "admin",
...     "password": "password",
...     "database": "user_db"
... }
>>> batch_manager.set_base_config(base_config)
>>> ip_list = generate_ip_range("192.168.1.100", 50)
>>> results = batch_manager.add_batch_connections(ip_list)
"""

import ipaddress
import threading
from concurrent.futures import as_completed
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

from .core.connections import DatabaseManager
from .core.exceptions import (
    ConfigError,
    DatabaseError,
    DBConnectorError,
    FileSystemError,
    QueryError,
)
from .utils.logging_utils import get_logger
from .utils.path_utils import PathHelper

logger = get_logger(__name__)


class BatchDatabaseManager:
    """批量数据库连接管理器（临时性配置）(Batch Database Manager)

    适用于管理大量配置相似的数据库连接，支持批量操作和并发执行。
    使用独立的临时配置文件，避免与主配置文件冲突。

    Example:
    >>> batch_manager = BatchDatabaseManager("my_app")
    >>> batch_manager.set_base_config({"type": "mysql", "port": 3306})
    >>> with batch_manager:
    ...     batch_manager.add_batch_connections(["192.168.1.1"])
    """

    def __init__(self, temp_config_suffix: str = "batch"):
        """初始化批量管理器

        Args:
            temp_config_suffix: 临时配置文件后缀（如 "batch" → connections_batch.toml）

        Raises:
            ValueError: 后缀为空或与主配置文件冲突

        Example:
        >>> batch_manager = BatchDatabaseManager("my_app")
        """
        if not temp_config_suffix or temp_config_suffix.strip() == "":
            raise ValueError("临时配置后缀不能为空")

        if temp_config_suffix in ["connections", "connections.toml"]:
            raise ValueError("临时配置后缀不能为'connections'，以避免与主配置文件冲突")

        self.app_name = "db_connector_tool"
        self.temp_config_file = f"connections_{temp_config_suffix}.toml"

        self.database_manager = DatabaseManager(self.app_name, self.temp_config_file)

        self.base_config: Optional[Dict[str, Any]] = None
        self._connection_names: List[str] = []
        self._lock = threading.RLock()
        self._is_cleaned = False

        logger.info("批量管理器初始化完成 - 临时配置文件: %s", self.temp_config_file)

    def set_base_config(self, config: Dict[str, Any]) -> None:
        """设置基础配置模板

        Args:
            config: 基础数据库配置（IP 会被移除，因为 IP 是动态变化的）

        Example:
        >>> batch_manager.set_base_config({
        ...     "type": "mysql", "port": 3306,
        ...     "username": "admin", "password": "password",
        ...     "database": "user_db"
        ... })
        """
        self.base_config = config.copy()
        if "host" in self.base_config:
            del self.base_config["host"]

        logger.info("基础配置模板设置完成")

    def add_batch_connections(
        self, ip_list: List[str], connection_prefix: str = "db"
    ) -> Dict[str, bool]:
        """批量添加数据库连接

        连接名称冲突时自动覆盖已有连接。

        Args:
            ip_list: IP 地址列表
            connection_prefix: 连接名称前缀

        Returns:
            Dict[str, bool]: 每个 IP 的连接添加结果

        Raises:
            ValueError: 基础配置模板未设置

        Example:
        >>> results = batch_manager.add_batch_connections(["192.168.1.1", "192.168.1.2"])
        """
        if not self.base_config:
            raise ValueError("请先设置基础配置模板")

        results = {}
        success_count = 0

        for index, ip in enumerate(ip_list):
            connection_name = f"{connection_prefix}_{index:03d}"

            specific_config = self.base_config.copy()
            specific_config["host"] = ip

            try:
                connection_names = self.database_manager.list_connections()

                if connection_name in connection_names:
                    logger.warning("连接 %s 已存在，执行覆盖操作", connection_name)
                    self._remove_existing_connection(connection_name)

                self.database_manager.add_connection(connection_name, specific_config)

                with self._lock:
                    if connection_name not in self._connection_names:
                        self._connection_names.append(connection_name)

                results[ip] = True
                success_count += 1
                logger.info("添加连接: %s -> %s", connection_name, ip)
            except (DatabaseError, ConfigError) as error:
                results[ip] = False
                logger.error("添加连接失败 %s: %s", ip, error)

        logger.info("批量添加完成: %s/%s 个连接成功", success_count, len(ip_list))
        return results

    def cleanup(self) -> None:
        """清理批量管理器所有资源

        关闭所有数据库连接、清理连接名称列表、删除临时配置文件。

        Raises:
            DBConnectorError: 清理过程中发生严重错误

        Example:
        >>> batch_manager.cleanup()
        """
        if self._is_cleaned:
            logger.debug("批量管理器已清理，无需重复操作")
            return

        try:
            logger.info("开始清理批量管理器资源")

            connection_names = self._get_all_connection_names()
            if connection_names:
                logger.info("关闭 %s 个数据库连接", len(connection_names))
                for conn_name in connection_names:
                    try:
                        self.database_manager.pool_manager.remove_connection(conn_name)
                        logger.debug("连接 %s 已关闭", conn_name)
                    except DatabaseError as error:
                        logger.warning("关闭连接 %s 失败: %s", conn_name, error)

            with self._lock:
                self._connection_names.clear()
                logger.debug("连接名称列表已清空")

            try:
                config_path = (
                    PathHelper.get_user_config_dir(self.app_name)
                    / self.temp_config_file
                )
                if config_path.exists():
                    config_path.unlink()
                    logger.info("临时配置文件已删除: %s", config_path)
                else:
                    logger.debug("临时配置文件不存在，无需删除")
            except FileSystemError as error:
                logger.warning("删除临时配置文件失败: %s", error)

            self.base_config = None

            self._is_cleaned = True
            logger.info("批量管理器资源清理完成")

        except DBConnectorError as error:
            logger.error("批量管理器资源清理失败: %s", error)
            raise

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，自动清理资源"""
        self.cleanup()

    def _remove_existing_connection(self, connection_name: str) -> None:
        """安全删除已存在的连接配置

        Args:
            connection_name: 连接名称

        Raises:
            DBConnectorError: 删除过程中发生严重错误
        """
        with self._lock:
            try:
                try:
                    self.database_manager.remove_connection(connection_name)
                    logger.debug("已从数据库管理器中删除连接配置: %s", connection_name)

                    if connection_name in self._connection_names:
                        self._connection_names.remove(connection_name)
                        logger.debug("已从连接名称列表中移除: %s", connection_name)
                except (DatabaseError, ConfigError) as remove_error:
                    logger.warning(
                        "从数据库管理器删除连接配置失败 %s: %s",
                        connection_name,
                        remove_error,
                    )

                logger.info("连接 %s 删除完成", connection_name)

            except DBConnectorError as error:
                logger.error("删除连接 %s 时发生严重错误: %s", connection_name, error)
                raise

    def test_batch_connections(self, max_workers: int = 10) -> Dict[str, bool]:
        """批量测试数据库连接

        Args:
            max_workers: 最大并发线程数

        Returns:
            Dict[str, bool]: 每个连接的测试结果

        Example:
        >>> results = batch_manager.test_batch_connections(max_workers=5)
        """
        connection_names = self._get_all_connection_names()
        if not connection_names:
            logger.warning("没有可测试的连接")
            return {}

        results = {}
        success_count = 0

        def test_single(conn_name):
            try:
                is_connected = self.database_manager.test_connection(conn_name)
                return conn_name, is_connected
            except DatabaseError as error:
                logger.error("测试连接 %s 失败: %s", conn_name, error)
                return conn_name, False

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conn = {
                executor.submit(test_single, conn_name): conn_name
                for conn_name in connection_names
            }

            for future in as_completed(future_to_conn):
                conn_name, result = future.result()
                results[conn_name] = result
                if result:
                    success_count += 1

        logger.info(
            "批量测试完成: %s/%s 个连接正常", success_count, len(connection_names)
        )
        return results

    def execute_batch_query(
        self, sql: str, params: Optional[Dict] = None, max_workers: int = 5
    ) -> Dict[str, Any]:
        """批量执行 SQL 查询

        Args:
            sql: SQL 语句
            params: 查询参数（可选）
            max_workers: 最大并发线程数

        Returns:
            Dict[str, Any]: 每个连接的执行结果

        Example:
        >>> results = batch_manager.execute_batch_query("SELECT COUNT(*) FROM users")
        """
        connection_names = self._get_all_connection_names()
        if not connection_names:
            logger.warning("没有可执行查询的连接")
            return {}

        results = {}

        def execute_single(conn_name):
            try:
                result = self.database_manager.execute_query(
                    conn_name, sql, params or {}
                )
                return conn_name, {
                    "success": True,
                    "data": result,
                    "count": len(result),
                }
            except DatabaseError as error:
                return conn_name, {"success": False, "error": str(error), "data": []}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conn = {
                executor.submit(execute_single, conn_name): conn_name
                for conn_name in connection_names
            }

            for future in as_completed(future_to_conn):
                conn_name, result = future.result()
                results[conn_name] = result

        success_count = sum(1 for r in results.values() if r["success"])
        logger.info(
            "批量查询完成: %s/%s 个连接成功", success_count, len(connection_names)
        )
        return results

    def upgrade_table_structure(
        self,
        upgrade_sqls: List[str],
        rollback_sqls: Optional[List[str]] = None,
        max_workers: int = 3,
    ) -> Dict[str, Dict]:
        """批量升级表结构（支持事务和回滚）

        Args:
            upgrade_sqls: 升级 SQL 语句列表
            rollback_sqls: 回滚 SQL 语句列表（可选）
            max_workers: 最大并发线程数

        Returns:
            Dict[str, Dict]: 每个连接的升级结果

        Example:
        >>> results = batch_manager.upgrade_table_structure(
        ...     ["ALTER TABLE users ADD COLUMN age INT"],
        ...     ["ALTER TABLE users DROP COLUMN age"],
        ... )
        """
        connection_names = self._get_all_connection_names()
        if not connection_names:
            logger.warning("没有可升级的连接")
            return {}

        results = {}

        def upgrade_single(conn_name):
            return self._upgrade_single_database_internal(
                conn_name, upgrade_sqls, rollback_sqls
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conn = {
                executor.submit(upgrade_single, conn_name): conn_name
                for conn_name in connection_names
            }

            for future in as_completed(future_to_conn):
                conn_name, result = future.result()
                results[conn_name] = result

        success_count = sum(1 for r in results.values() if r["success"])
        logger.info(
            "批量升级完成: %s/%s 个连接成功", success_count, len(connection_names)
        )
        return results

    def _get_all_connection_names(self) -> List[str]:
        """获取所有连接名称的副本

        Returns:
            List[str]: 连接名称列表副本
        """
        with self._lock:
            return self._connection_names.copy()

    def _upgrade_single_database_internal(
        self,
        conn_name: str,
        upgrade_sqls: List[str],
        rollback_sqls: Optional[List[str]],
    ) -> Tuple[str, Dict]:
        """单个数据库升级的内部逻辑

        Args:
            conn_name: 连接名称
            upgrade_sqls: 升级 SQL 列表
            rollback_sqls: 回滚 SQL 列表

        Returns:
            Tuple[str, Dict]: 连接名称和升级结果
        """
        try:
            execution_results = self._execute_upgrade_sqls(
                conn_name, upgrade_sqls, rollback_sqls
            )
            return conn_name, {
                "success": all(r["success"] for r in execution_results),
                "executions": execution_results,
            }
        except DatabaseError as error:
            return conn_name, {"success": False, "error": str(error), "executions": []}

    def _execute_upgrade_sqls(
        self,
        conn_name: str,
        upgrade_sqls: List[str],
        rollback_sqls: Optional[List[str]],
    ) -> List[Dict]:
        """执行升级 SQL 语句

        逐条执行升级 SQL，任何失败都会尝试回滚并终止后续执行。

        Args:
            conn_name: 连接名称
            upgrade_sqls: 升级 SQL 列表
            rollback_sqls: 回滚 SQL 列表

        Returns:
            List[Dict]: 每条 SQL 的执行结果
        """
        execution_results = []

        for sql in upgrade_sqls:
            try:
                result = self.database_manager.execute_query(conn_name, sql)
                execution_results.append(
                    {
                        "sql": sql,
                        "success": True,
                        "affected_rows": len(result) if result else 0,
                    }
                )
            except QueryError as error:
                execution_results.append(
                    {"sql": sql, "success": False, "error": str(error)}
                )
                if rollback_sqls:
                    self._try_rollback(conn_name, rollback_sqls)
                break
            except DatabaseError as error:
                execution_results.append(
                    {
                        "sql": sql,
                        "success": False,
                        "error": f"数据库错误: {error}",
                    }
                )
                if rollback_sqls:
                    self._try_rollback(conn_name, rollback_sqls)
                break

        return execution_results

    def _try_rollback(self, conn_name: str, rollback_sqls: List[str]) -> None:
        """尝试回滚操作

        Args:
            conn_name: 连接名称
            rollback_sqls: 回滚 SQL 列表
        """
        try:
            self._execute_rollback(conn_name, rollback_sqls)
            logger.info("连接 %s 执行回滚成功", conn_name)
        except DatabaseError as rollback_error:
            logger.error("连接 %s 执行回滚失败: %s", conn_name, rollback_error)

    def _execute_rollback(self, conn_name: str, rollback_sqls: List[str]) -> None:
        """执行回滚操作

        Args:
            conn_name: 连接名称
            rollback_sqls: 回滚 SQL 列表
        """
        for sql in rollback_sqls:
            try:
                self.database_manager.execute_query(conn_name, sql)
            except DatabaseError as error:
                logger.warning("回滚执行失败 %s: %s", conn_name, error)

    def get_connection_stats(self) -> Dict[str, Any]:
        """获取所有连接的状态信息

        Returns:
            Dict[str, Any]: 连接名称到状态信息的映射

        Example:
        >>> stats = batch_manager.get_connection_stats()
        """
        stats = {}
        connection_names = self._get_all_connection_names()

        for conn_name in connection_names:
            try:
                stats[conn_name] = self.database_manager.get_connection_info(conn_name)
            except (DatabaseError, ConfigError) as error:
                stats[conn_name] = {"error": str(error)}

        return stats

    def close_all_connections(self) -> None:
        """关闭所有数据库连接

        Example:
        >>> batch_manager.close_all_connections()
        """
        self.database_manager.close_all_connections()
        logger.info("所有批量连接已关闭")


def generate_ip_range(base_ip: str, count: int) -> List[str]:
    """生成连续的 IP 地址列表

    Args:
        base_ip: 起始 IP 地址，如 "192.168.1.100"
        count: 需要生成的 IP 数量

    Returns:
        List[str]: IP 地址列表

    Example:
    >>> ips = generate_ip_range("192.168.1.100", 3)
    >>> ips
    ['192.168.1.100', '192.168.1.101', '192.168.1.102']
    """
    base = ipaddress.IPv4Address(base_ip)
    return [str(base + i) for i in range(count)]


def cleanup_temp_configs(app_name: str = "db_connector_tool") -> None:
    """清理所有临时批量配置文件

    Args:
        app_name: 应用名称

    Example:
    >>> cleanup_temp_configs()
    """
    config_dir = PathHelper.get_user_config_dir(app_name)

    if config_dir.exists():
        temp_files = list(config_dir.glob("connections_*.toml"))

        for temp_file in temp_files:
            try:
                temp_file.unlink()
                logger.info("清理临时配置文件: %s", temp_file)
            except FileSystemError as error:
                logger.warning("清理临时配置文件失败 %s: %s", temp_file, error)

        logger.info("共清理 %s 个临时配置文件", len(temp_files))
