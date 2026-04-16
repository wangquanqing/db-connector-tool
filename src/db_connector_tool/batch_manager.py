"""
批量数据库连接管理器模块
专门用于管理大量配置相似的数据库连接（临时性配置）

主要特性：
- 基于模板的批量连接配置
- 并发执行数据库操作
- 批量表结构升级支持
- 完善的错误处理和回滚机制
- IP范围自动生成工具

使用示例：
    >>> from db_connector_tool import BatchDatabaseManager, generate_ip_range
    >>>
    >>> # 创建批量管理器
    >>> batch_manager = BatchDatabaseManager("my_app")
    >>>
    >>> # 设置基础配置模板
    >>> base_config = {
    ...     "type": "mysql",
    ...     "port": 3306,
    ...     "username": "admin",
    ...     "password": "password",
    ...     "database": "user_db"
    ... }
    >>> batch_manager.set_base_config(base_config)
    >>>
    >>> # 批量添加连接
    >>> ip_list = generate_ip_range("192.168.1.100", 50)
    >>> results = batch_manager.add_batch_connections(ip_list)
"""

import ipaddress
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple

from .core.connections import DatabaseManager
from .core.exceptions import QueryError, DatabaseError, DBConnectorError, FileSystemError, ConfigError
from .utils.logging_utils import get_logger
from .utils.path_utils import PathHelper

# 获取模块级别的日志记录器
logger = get_logger(__name__)


class BatchDatabaseManager:
    """
    批量数据库连接管理器（临时性配置）

    适用于管理大量配置相似的数据库连接，支持批量操作和并发执行。
    使用独立的临时配置文件，避免与主配置文件冲突。

    Attributes:
        base_config (Dict[str, Any]): 基础配置模板
        db_manager (DatabaseManager): 数据库管理器实例
        connection_pool (Dict[str, Dict]): 连接池字典
        _lock (threading.RLock): 线程安全锁
    """

    def __init__(self, temp_config_suffix: str = "batch"):
        """
        初始化批量数据库管理器（临时性配置）

        Args:
            temp_config_suffix: 临时配置后缀，用于创建独立的配置文件
                            例如："batch" -> "connections_batch.toml"
                            不能为"connections.toml"或空字符串

        Raises:
            ValueError: 当配置后缀无效时
        """
        if not temp_config_suffix or temp_config_suffix.strip() == "":
            raise ValueError("临时配置后缀不能为空")

        if temp_config_suffix in ["connections", "connections.toml"]:
            raise ValueError("临时配置后缀不能为'connections'，以避免与主配置文件冲突")

        # 使用主应用名称，但创建独立的临时配置文件
        self.app_name = "db_connector_tool"
        self.temp_config_suffix = temp_config_suffix
        self.temp_config_file = f"connections_{temp_config_suffix}.toml"

        self.db_manager = DatabaseManager(self.app_name, self.temp_config_file)

        self.base_config: Optional[Dict[str, Any]] = None
        self._connection_names = []
        self._lock = threading.RLock()
        self._is_cleaned = False

        logger.info(f"批量管理器初始化完成 - 临时配置文件: {self.temp_config_file}")

    def set_base_config(self, config: Dict[str, Any]) -> None:
        """
        设置基础配置模板

        Args:
            config: 基础数据库配置，包含除IP外的所有参数
        """
        self.base_config = config.copy()
        # 移除IP字段，因为IP是动态变化的
        if "host" in self.base_config:
            del self.base_config["host"]

        logger.info("基础配置模板设置完成")

    def add_batch_connections(
        self, ip_list: List[str], connection_prefix: str = "db"
    ) -> Dict[str, bool]:
        """
        批量添加数据库连接

        如果连接名称已存在，会先删除旧连接再添加新连接。

        Args:
            ip_list: IP地址列表，如 ["192.168.1.100", "192.168.1.101", ...]
            connection_prefix: 连接名称前缀，实际名称为 {prefix}_{index}

        Returns:
            Dict[str, bool]: 每个IP的连接添加结果
        """
        if not self.base_config:
            raise ValueError("请先设置基础配置模板")

        results = {}
        success_count = 0

        for index, ip in enumerate(ip_list):
            connection_name = f"{connection_prefix}_{index:03d}"

            # 创建具体配置
            specific_config = self.base_config.copy()
            specific_config["host"] = ip

            try:
                connection_names = self.db_manager.list_connections()
                # 检查连接是否已存在，如果存在则先删除
                if connection_name in connection_names:
                    logger.warning(f"连接 {connection_name} 已存在，执行覆盖操作")
                    self._remove_existing_connection(connection_name)

                # 添加新连接
                self.db_manager.add_connection(connection_name, specific_config)

                # 添加到连接名称列表
                with self._lock:
                    if connection_name not in self._connection_names:
                        self._connection_names.append(connection_name)

                results[ip] = True
                success_count += 1
                logger.info("添加连接: %s -> %s", connection_name, ip)
            except (DatabaseError, ConfigError) as e:
                results[ip] = False
                logger.error("添加连接失败 %s: %s", ip, e)

        logger.info("批量添加完成: %s/%s 个连接成功", success_count, len(ip_list))
        return results

    def cleanup(self) -> None:
        """
        清理批量管理器所有资源

        安全关闭所有数据库连接，清理临时配置文件，释放所有资源。

        Raises:
            Exception: 当清理过程中发生严重错误时

        Example:
            >>> batch_manager.cleanup()
        """
        if self._is_cleaned:
            logger.debug("批量管理器已清理，无需重复操作")
            return

        try:
            logger.info("开始清理批量管理器资源")

            # 1. 关闭所有数据库连接
            connection_names = self._get_all_connection_names()
            if connection_names:
                logger.info("关闭 %s 个数据库连接", len(connection_names))
                for conn_name in connection_names:
                    try:
                        self.db_manager.pool_manager.remove_connection(conn_name)
                        logger.debug("连接 %s 已关闭", conn_name)
                    except DatabaseError as e:
                        logger.warning("关闭连接 %s 失败: %s", conn_name, str(e))

            # 2. 清理连接名称列表
            with self._lock:
                self._connection_names.clear()
                logger.debug("连接名称列表已清空")

            # 3. 清理临时配置文件
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
            except FileSystemError as e:
                logger.warning("删除临时配置文件失败: %s", str(e))

            # 4. 清理基础配置
            self.base_config = None

            self._is_cleaned = True
            logger.info("批量管理器资源清理完成")

        except DBConnectorError as e:
            logger.error("批量管理器资源清理失败: %s", str(e))
            raise

    def __enter__(self):
        """上下文管理器入口"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口，确保资源清理"""
        self.cleanup()

    def __del__(self):
        """析构函数，确保资源清理"""
        if hasattr(self, '_is_cleaned') and not self._is_cleaned:
            try:
                self.cleanup()
            except Exception:
                # 析构函数中忽略异常
                pass

    def _remove_existing_connection(self, connection_name: str) -> None:
        """
        安全删除已存在的连接配置

        Args:
            connection_name: 要删除的连接名称

        Note:
            确保连接被完全清理，包括关闭连接和清理配置
        """
        with self._lock:
            try:
                # 1. 先从数据库管理器中删除连接配置（会自动关闭连接）
                try:
                    self.db_manager.remove_connection(connection_name)
                    logger.debug("已从数据库管理器中删除连接配置: %s", connection_name)
                    # 只有当从数据库管理器删除成功时，才从连接名称列表中移除
                    if connection_name in self._connection_names:
                        self._connection_names.remove(connection_name)
                        logger.debug("已从连接名称列表中移除: %s", connection_name)
                except (DatabaseError, ConfigError) as remove_error:
                    logger.warning(
                        "从数据库管理器删除连接配置失败 %s: %s", connection_name, str(remove_error)
                    )

                logger.info("连接 %s 删除完成", connection_name)

            except DBConnectorError as e:
                logger.error("删除连接 %s 时发生严重错误: %s", connection_name, str(e))
                raise

    def test_batch_connections(self, max_workers: int = 10) -> Dict[str, bool]:
        """
        批量测试数据库连接

        Args:
            max_workers: 最大并发线程数

        Returns:
            Dict[str, bool]: 每个连接的测试结果
        """
        connection_names = self._get_all_connection_names()
        if not connection_names:
            logger.warning("没有可测试的连接")
            return {}

        results = {}
        success_count = 0

        def test_single_connection(conn_name):
            try:
                is_connected = self.db_manager.test_connection(conn_name)
                return conn_name, is_connected
            except (DatabaseError, DBConnectionError) as e:
                logger.error("测试连接 %s 失败: %s", conn_name, e)
                return conn_name, False

        # 使用线程池并发测试
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conn = {
                executor.submit(test_single_connection, conn_name): conn_name
                for conn_name in connection_names
            }

            for future in as_completed(future_to_conn):
                conn_name, result = future.result()
                results[conn_name] = result
                if result:
                    success_count += 1
                status = "正常" if result else "异常"
                logger.info(f"{status} - {conn_name}")

        logger.info(f"批量测试完成: {success_count}/{len(connection_names)} 个连接正常")
        return results

    def execute_batch_query(
        self, sql: str, params: Optional[Dict] = None, max_workers: int = 5
    ) -> Dict[str, Any]:
        """
        批量执行SQL查询

        Args:
            sql: 要执行的SQL语句
            params: 查询参数
            max_workers: 最大并发线程数

        Returns:
            Dict[str, Any]: 每个连接的执行结果
        """
        connection_names = self._get_all_connection_names()
        if not connection_names:
            logger.warning("没有可执行查询的连接")
            return {}

        results = {}

        def execute_single_query(conn_name):
            try:
                result = self.db_manager.execute_query(conn_name, sql, params or {})
                return conn_name, {
                    "success": True,
                    "data": result,
                    "count": len(result),
                }
            except QueryError as e:
                return conn_name, {"success": False, "error": str(e), "data": []}
            except DatabaseError as e:
                return conn_name, {"success": False, "error": str(e), "data": []}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conn = {
                executor.submit(execute_single_query, conn_name): conn_name
                for conn_name in connection_names
            }

            for future in as_completed(future_to_conn):
                conn_name, result = future.result()
                results[conn_name] = result

        success_count = sum(1 for r in results.values() if r["success"])
        logger.info(f"批量查询完成: {success_count}/{len(connection_names)} 个连接成功")
        return results

    def upgrade_table_structure(
        self,
        upgrade_sqls: List[str],
        rollback_sqls: Optional[List[str]] = None,
        max_workers: int = 3,
    ) -> Dict[str, Dict]:
        """
        批量升级表结构（支持事务和回滚）

        Args:
            upgrade_sqls: 升级SQL语句列表
            rollback_sqls: 回滚SQL语句列表（可选）
            max_workers: 最大并发线程数（建议较小，避免对数据库造成过大压力）

        Returns:
            Dict[str, Dict]: 每个连接的升级结果
        """
        connection_names = self._get_all_connection_names()
        if not connection_names:
            logger.warning("没有可升级的连接")
            return {}

        results = {}

        def upgrade_single_database(conn_name):
            return self._upgrade_single_database_internal(
                conn_name, upgrade_sqls, rollback_sqls
            )

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conn = {
                executor.submit(upgrade_single_database, conn_name): conn_name
                for conn_name in connection_names
            }

            for future in as_completed(future_to_conn):
                conn_name, result = future.result()
                results[conn_name] = result

        success_count = sum(1 for r in results.values() if r["success"])
        logger.info(f"批量升级完成: {success_count}/{len(connection_names)} 个连接成功")
        return results

    def _get_all_connection_names(self) -> List[str]:
        """获取所有连接名称"""
        with self._lock:
            return self._connection_names.copy()

    def _upgrade_single_database_internal(
        self,
        conn_name: str,
        upgrade_sqls: List[str],
        rollback_sqls: Optional[List[str]],
    ) -> Tuple[str, Dict]:
        """
        单个数据库升级的内部逻辑

        Args:
            conn_name: 连接名称
            upgrade_sqls: 升级SQL语句列表
            rollback_sqls: 回滚SQL语句列表

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
        except DatabaseError as e:
            return conn_name, {"success": False, "error": str(e), "executions": []}

    def _execute_upgrade_sqls(
        self,
        conn_name: str,
        upgrade_sqls: List[str],
        rollback_sqls: Optional[List[str]],
    ) -> List[Dict]:
        """
        执行升级SQL语句

        Args:
            conn_name: 连接名称
            upgrade_sqls: 升级SQL语句列表
            rollback_sqls: 回滚SQL语句列表

        Returns:
            List[Dict]: 执行结果列表
        """
        execution_results = []

        for sql in upgrade_sqls:
            try:
                result = self.db_manager.execute_query(conn_name, sql)
                execution_results.append(
                    {
                        "sql": sql,
                        "success": True,
                        "affected_rows": len(result) if result else 0,
                    }
                )
            except QueryError as e:
                execution_results.append(
                    {"sql": sql, "success": False, "error": str(e)}
                )
                # 如果某条SQL失败，尝试回滚
                if rollback_sqls:
                    try:
                        self._execute_rollback(conn_name, rollback_sqls)
                        logger.info("连接 %s 执行回滚成功", conn_name)
                    except DatabaseError as rollback_error:
                        logger.error("连接 %s 执行回滚失败: %s", conn_name, str(rollback_error))
                break
            except DatabaseError as e:
                execution_results.append(
                    {"sql": sql, "success": False, "error": f"数据库错误: {str(e)}"}
                )
                # 如果某条SQL失败，尝试回滚
                if rollback_sqls:
                    try:
                        self._execute_rollback(conn_name, rollback_sqls)
                        logger.info("连接 %s 执行回滚成功", conn_name)
                    except DatabaseError as rollback_error:
                        logger.error("连接 %s 执行回滚失败: %s", conn_name, str(rollback_error))
                break

        return execution_results

    def _execute_rollback(self, conn_name: str, rollback_sqls: List[str]) -> None:
        """执行回滚操作"""
        for sql in rollback_sqls:
            try:
                self.db_manager.execute_query(conn_name, sql)
            except DatabaseError as e:
                logger.warning("回滚执行失败 %s: %s", conn_name, e)

    def get_connection_stats(self) -> Dict[str, Any]:
        """获取连接统计信息"""
        stats = {}
        connection_names = self._get_all_connection_names()

        for conn_name in connection_names:
            try:
                stats[conn_name] = self.db_manager.get_connection_info(conn_name)
            except (DatabaseError, ConfigError) as e:
                stats[conn_name] = {"error": str(e)}

        return stats

    def close_all_connections(self) -> None:
        """关闭所有连接"""
        self.db_manager.close_all_connections()
        logger.info("所有批量连接已关闭")


def generate_ip_range(base_ip: str, count: int) -> List[str]:
    """
    生成连续的IP地址列表

    Args:
        base_ip: 基础IP地址，如 "192.168.1.100"
        count: 生成的IP数量

    Returns:
        List[str]: IP地址列表
    """
    base = ipaddress.IPv4Address(base_ip)
    return [str(base + i) for i in range(count)]


def cleanup_temp_configs(app_name: str = "db_connector_tool") -> None:
    """
    清理所有临时批量配置文件

    Args:
        app_name: 应用名称，默认为"db_connector_tool"
    """
    config_dir = PathHelper.get_user_config_dir(app_name)

    if config_dir.exists():
        # 查找所有临时配置文件
        temp_files = list(config_dir.glob("connections_*.toml"))

        for temp_file in temp_files:
            try:
                temp_file.unlink()
                logger.info("清理临时配置文件: %s", temp_file)
            except FileSystemError as e:
                logger.warning("清理临时配置文件失败 %s: %s", temp_file, e)

        logger.info("共清理 %s 个临时配置文件", len(temp_files))
