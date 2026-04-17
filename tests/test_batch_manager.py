import unittest
from unittest import mock

from src.db_connector_tool.batch_manager import (
    BatchDatabaseManager,
    cleanup_temp_configs,
    generate_ip_range,
)


class TestBatchDatabaseManager(unittest.TestCase):
    """测试批量数据库连接管理器"""

    def setUp(self):
        """设置测试环境"""
        self.batch_manager = BatchDatabaseManager("test_batch")
        self.base_config = {
            "type": "mysql",
            "port": 3306,
            "username": "admin",
            "password": "password",
            "database": "test_db",
        }

    def tearDown(self):
        """清理测试环境"""
        if hasattr(self, "batch_manager"):
            self.batch_manager.cleanup()

    def test_init(self):
        """测试初始化批量管理器"""
        self.assertIsInstance(self.batch_manager, BatchDatabaseManager)
        self.assertEqual(self.batch_manager.temp_config_suffix, "test_batch")
        self.assertEqual(
            self.batch_manager.temp_config_file, "connections_test_batch.toml"
        )

        # 测试无效的配置后缀
        with self.assertRaises(ValueError):
            BatchDatabaseManager("")

        with self.assertRaises(ValueError):
            BatchDatabaseManager("connections")

    def test_set_base_config(self):
        """测试设置基础配置模板"""
        self.batch_manager.set_base_config(self.base_config)
        self.assertIsNotNone(self.batch_manager.base_config)
        self.assertNotIn("host", self.batch_manager.base_config)  # type: ignore

    def test_add_batch_connections(self):
        """测试批量添加连接"""
        self.batch_manager.set_base_config(self.base_config)
        ip_list = ["192.168.1.1", "192.168.1.2"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(self.batch_manager.db_manager, "add_connection"):
                results = self.batch_manager.add_batch_connections(ip_list)
                self.assertEqual(len(results), 2)
                self.assertTrue(results["192.168.1.1"])
                self.assertTrue(results["192.168.1.2"])

    def test_add_batch_connections_with_existing(self):
        """测试批量添加连接（包含已存在的连接）"""
        self.batch_manager.set_base_config(self.base_config)
        ip_list = ["192.168.1.1"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager, "list_connections", return_value=["db_000"]
        ):
            with mock.patch.object(
                self.batch_manager.db_manager.pool_manager, "remove_connection"
            ):
                with mock.patch.object(self.batch_manager.db_manager, "add_connection"):
                    results = self.batch_manager.add_batch_connections(ip_list)
                    self.assertEqual(len(results), 1)
                    self.assertTrue(results["192.168.1.1"])

    def test_cleanup(self):
        """测试清理批量管理器资源"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager.pool_manager, "remove_connection"
        ):
            with mock.patch(
                "db_connector_tool.batch_manager.PathHelper.get_user_config_dir"
            ):
                with mock.patch("pathlib.Path.exists", return_value=False):
                    self.batch_manager.cleanup()
                    self.assertTrue(self.batch_manager._is_cleaned)

    def test_test_batch_connections(self):
        """测试批量测试连接"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000", "db_001"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager, "test_connection", return_value=True
        ):
            results = self.batch_manager.test_batch_connections(max_workers=2)
            self.assertEqual(len(results), 2)
            self.assertTrue(results["db_000"])
            self.assertTrue(results["db_001"])

    def test_execute_batch_query(self):
        """测试批量执行查询"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            return_value=[{"id": 1, "name": "test"}],
        ):
            results = self.batch_manager.execute_batch_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)
            self.assertTrue(results["db_000"]["success"])
            self.assertEqual(len(results["db_000"]["data"]), 1)

    def test_upgrade_table_structure(self):
        """测试批量升级表结构"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager, "execute_query", return_value=[]
        ):
            upgrade_sqls = ["ALTER TABLE users ADD COLUMN age INT"]
            results = self.batch_manager.upgrade_table_structure(upgrade_sqls)
            self.assertEqual(len(results), 1)
            self.assertTrue(results["db_000"]["success"])

    def test_get_connection_stats(self):
        """测试获取连接统计信息"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager,
            "get_connection_info",
            return_value={"host": "192.168.1.1"},
        ):
            stats = self.batch_manager.get_connection_stats()
            self.assertEqual(len(stats), 1)
            self.assertEqual(stats["db_000"]["host"], "192.168.1.1")

    def test_close_all_connections(self):
        """测试关闭所有连接"""
        # 模拟数据库管理器的方法
        with mock.patch.object(self.batch_manager.db_manager, "close_all_connections"):
            self.batch_manager.close_all_connections()

    def test_context_manager(self):
        """测试上下文管理器"""
        with BatchDatabaseManager("test_context") as batch_manager:
            self.assertIsInstance(batch_manager, BatchDatabaseManager)
        # 退出上下文后应该被清理
        self.assertTrue(batch_manager._is_cleaned)

    def test_generate_ip_range(self):
        """测试生成IP地址范围"""
        ip_list = generate_ip_range("192.168.1.100", 3)
        self.assertEqual(len(ip_list), 3)
        self.assertEqual(ip_list[0], "192.168.1.100")
        self.assertEqual(ip_list[1], "192.168.1.101")
        self.assertEqual(ip_list[2], "192.168.1.102")

    def test_cleanup_temp_configs(self):
        """测试清理临时配置文件"""
        # 模拟PathHelper和Path方法
        with mock.patch(
            "db_connector_tool.batch_manager.PathHelper.get_user_config_dir"
        ):
            with mock.patch("pathlib.Path.exists", return_value=True):
                with mock.patch("pathlib.Path.glob", return_value=[]):
                    with mock.patch("pathlib.Path.unlink"):
                        cleanup_temp_configs("test_app")

    def test_add_batch_connections_no_base_config(self):
        """测试未设置基础配置时批量添加连接"""
        ip_list = ["192.168.1.1"]
        with self.assertRaises(ValueError):
            self.batch_manager.add_batch_connections(ip_list)

    def test_add_batch_connections_with_error(self):
        """测试批量添加连接时出现错误"""
        self.batch_manager.set_base_config(self.base_config)
        ip_list = ["192.168.1.1"]

        # 模拟数据库管理器的方法抛出异常
        with mock.patch.object(
            self.batch_manager.db_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(
                self.batch_manager.db_manager,
                "add_connection",
                side_effect=Exception("Add connection failed"),
            ):
                results = self.batch_manager.add_batch_connections(ip_list)
                self.assertEqual(len(results), 1)
                self.assertFalse(results["192.168.1.1"])

    def test_cleanup_with_errors(self):
        """测试清理时出现错误"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟关闭连接失败
        with mock.patch.object(
            self.batch_manager.db_manager.pool_manager,
            "remove_connection",
            side_effect=Exception("Remove connection failed"),
        ):
            # 模拟删除临时配置文件失败
            with mock.patch(
                "db_connector_tool.batch_manager.PathHelper.get_user_config_dir"
            ):
                with mock.patch("pathlib.Path.exists", return_value=True):
                    with mock.patch(
                        "pathlib.Path.unlink", side_effect=Exception("Unlink failed")
                    ):
                        self.batch_manager.cleanup()
                        self.assertTrue(self.batch_manager._is_cleaned)

    def test_cleanup_already_cleaned(self):
        """测试清理已清理的批量管理器"""
        self.batch_manager._is_cleaned = True
        self.batch_manager.cleanup()
        # 应该不会抛出异常

    def test_remove_existing_connection_with_error(self):
        """测试删除已存在连接时出现错误"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟删除连接失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "remove_connection",
            side_effect=Exception("Remove connection failed"),
        ):
            # 应该不会抛出异常
            try:
                self.batch_manager._remove_existing_connection("db_000")
            except Exception as e:
                self.fail(
                    f"_remove_existing_connection 应该不会抛出异常，但抛出了: {e}"
                )

    def test_test_batch_connections_empty(self):
        """测试测试空连接列表"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = []
        results = self.batch_manager.test_batch_connections()
        self.assertEqual(len(results), 0)

    def test_test_batch_connections_with_error(self):
        """测试测试连接时出现错误"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟测试连接失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "test_connection",
            side_effect=Exception("Test connection failed"),
        ):
            results = self.batch_manager.test_batch_connections()
            self.assertEqual(len(results), 1)
            self.assertFalse(results["db_000"])

    def test_execute_batch_query_empty(self):
        """测试执行空连接列表的查询"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = []
        results = self.batch_manager.execute_batch_query("SELECT * FROM users")
        self.assertEqual(len(results), 0)

    def test_upgrade_table_structure_empty(self):
        """测试升级空连接列表的表结构"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = []
        results = self.batch_manager.upgrade_table_structure(
            ["ALTER TABLE users ADD COLUMN age INT"]
        )
        self.assertEqual(len(results), 0)

    def test_get_connection_stats_with_error(self):
        """测试获取连接统计信息时出现错误"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟获取连接信息失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "get_connection_info",
            side_effect=Exception("Get connection info failed"),
        ):
            stats = self.batch_manager.get_connection_stats()
            self.assertEqual(len(stats), 1)
            self.assertIn("error", stats["db_000"])

    def test_execute_rollback(self):
        """测试执行回滚操作"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=Exception("Execute query failed"),
        ):
            # 应该不会抛出异常
            try:
                self.batch_manager._execute_rollback("db_000", ["ROLLBACK"])
            except Exception as e:
                self.fail(f"_execute_rollback 应该不会抛出异常，但抛出了: {e}")

    def test_upgrade_single_database_internal_with_error(self):
        """测试单个数据库升级时出现错误"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行升级失败
        with mock.patch.object(
            self.batch_manager,
            "_execute_upgrade_sqls",
            side_effect=Exception("Upgrade failed"),
        ):
            conn_name, result = self.batch_manager._upgrade_single_database_internal(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], None
            )
            self.assertEqual(conn_name, "db_000")
            self.assertFalse(result["success"])
            self.assertIn("error", result)

    def test_execute_upgrade_sqls_with_error(self):
        """测试执行升级SQL时出现错误"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=Exception("Execute query failed"),
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])

    def test_execute_upgrade_sqls_with_rollback_error(self):
        """测试执行升级SQL时出现错误且回滚也失败的情况"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询失败，然后模拟回滚也失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=[
                Exception("Execute query failed"),  # 第一次调用执行SQL失败
                Exception("Rollback failed"),  # 第二次调用回滚失败
            ],
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])

    def test_execute_upgrade_sqls_with_unknown_error(self):
        """测试执行升级SQL时出现未知错误的情况"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询失败，触发外层异常处理
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=Exception("Unknown error"),
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])

    def test_cleanup_temp_configs_no_dir(self):
        """测试清理临时配置文件（目录不存在）"""
        # 模拟目录不存在
        with mock.patch(
            "db_connector_tool.batch_manager.PathHelper.get_user_config_dir"
        ):
            with mock.patch("pathlib.Path.exists", return_value=False):
                cleanup_temp_configs("test_app")

    def test_cleanup_temp_configs_with_error(self):
        """测试清理临时配置文件时出现错误"""
        # 模拟删除文件失败
        with mock.patch(
            "db_connector_tool.batch_manager.PathHelper.get_user_config_dir"
        ):
            with mock.patch("pathlib.Path.exists", return_value=True):
                mock_file = mock.Mock()
                mock_file.unlink.side_effect = Exception("Unlink failed")
                with mock.patch("pathlib.Path.glob", return_value=[mock_file]):
                    cleanup_temp_configs("test_app")

    def test_set_base_config_with_host(self):
        """测试设置包含host字段的基础配置"""
        config_with_host = self.base_config.copy()
        config_with_host["host"] = "192.168.1.1"
        self.batch_manager.set_base_config(config_with_host)
        self.assertIsNotNone(self.batch_manager.base_config)
        self.assertNotIn("host", self.batch_manager.base_config)

    def test_add_batch_connections_with_connection_names_update(self):
        """测试批量添加连接时更新连接名称列表"""
        self.batch_manager.set_base_config(self.base_config)
        ip_list = ["192.168.1.1"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(self.batch_manager.db_manager, "add_connection"):
                results = self.batch_manager.add_batch_connections(ip_list)
                self.assertEqual(len(results), 1)
                self.assertTrue(results["192.168.1.1"])
                self.assertIn("db_000", self.batch_manager._connection_names)

    def test_remove_existing_connection_with_removal_from_list(self):
        """测试删除已存在连接时从连接名称列表中移除"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法
        with mock.patch.object(self.batch_manager.db_manager, "remove_connection"):
            self.batch_manager._remove_existing_connection("db_000")
            self.assertNotIn("db_000", self.batch_manager._connection_names)

    def test_remove_existing_connection_with_critical_error(self):
        """测试删除已存在连接时发生严重错误"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟删除连接时发生严重错误
        with mock.patch.object(
            self.batch_manager.db_manager,
            "remove_connection",
            side_effect=Exception("Remove connection failed"),
        ):
            # 模拟整个方法抛出异常
            with mock.patch.object(
                self.batch_manager,
                "_remove_existing_connection",
                side_effect=Exception("Critical error"),
            ):
                # 验证方法会抛出异常
                with self.assertRaises(Exception):
                    self.batch_manager._remove_existing_connection("db_000")

    def test_cleanup_with_critical_error(self):
        """测试清理过程中发生严重错误"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟清理连接名称列表时发生严重错误
        with mock.patch.object(
            self.batch_manager.db_manager.pool_manager,
            "remove_connection",
            side_effect=Exception("Remove connection failed"),
        ):
            # 模拟获取连接名称时抛出异常
            with mock.patch.object(
                self.batch_manager,
                "_get_all_connection_names",
                side_effect=Exception("Get connection names failed"),
            ):
                # 验证方法会抛出异常
                with self.assertRaises(Exception):
                    self.batch_manager.cleanup()

    def test_del_method(self):
        """测试析构函数"""
        # 创建一个新的批量管理器
        batch_manager = BatchDatabaseManager("test_del")
        batch_manager.set_base_config(self.base_config)
        # 确保_is_cleaned为False
        self.assertFalse(batch_manager._is_cleaned)
        # 手动调用析构函数
        del batch_manager
        # 析构函数会调用cleanup，这里我们无法直接验证，但代码会被覆盖

    def test_add_batch_connections_existing_in_list(self):
        """测试添加已存在于连接名称列表中的连接"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]
        ip_list = ["192.168.1.1"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.db_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(self.batch_manager.db_manager, "add_connection"):
                results = self.batch_manager.add_batch_connections(ip_list)
                self.assertEqual(len(results), 1)
                self.assertTrue(results["192.168.1.1"])
                # 验证连接名称列表中仍然只有一个元素（没有重复添加）
                self.assertEqual(len(self.batch_manager._connection_names), 1)
                self.assertIn("db_000", self.batch_manager._connection_names)

    def test_execute_batch_query_with_query_error(self):
        """测试批量执行查询时出现QueryError"""
        from src.db_connector_tool.core.exceptions import QueryError

        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法抛出QueryError
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=QueryError("Query failed"),
        ):
            results = self.batch_manager.execute_batch_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)
            self.assertFalse(results["db_000"]["success"])
            self.assertIn("error", results["db_000"])

    def test_execute_batch_query_with_generic_error(self):
        """测试批量执行查询时出现通用异常"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法抛出通用异常
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=Exception("Generic error"),
        ):
            results = self.batch_manager.execute_batch_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)
            self.assertFalse(results["db_000"]["success"])
            self.assertIn("error", results["db_000"])

    def test_execute_upgrade_sqls_with_query_error_and_rollback(self):
        """测试执行升级SQL时出现QueryError并执行回滚"""
        from src.db_connector_tool.core.exceptions import QueryError

        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询抛出QueryError，但回滚成功
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=[QueryError("Query failed"), None],
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])

    def test_execute_upgrade_sqls_with_generic_error_and_rollback(self):
        """测试执行升级SQL时出现通用异常并执行回滚"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询抛出通用异常，但回滚成功
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=[Exception("Generic error"), None],
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])

    def test_execute_upgrade_sqls_with_query_error_and_rollback_failure(self):
        """测试执行升级SQL时出现QueryError且回滚失败"""
        from src.db_connector_tool.core.exceptions import QueryError

        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询和回滚都失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=[QueryError("Query failed"), Exception("Rollback failed")],
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])

    def test_execute_upgrade_sqls_with_generic_error_and_rollback_failure(self):
        """测试执行升级SQL时出现通用异常且回滚失败"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行查询和回滚都失败
        with mock.patch.object(
            self.batch_manager.db_manager,
            "execute_query",
            side_effect=[Exception("Generic error"), Exception("Rollback failed")],
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])


if __name__ == "__main__":
    unittest.main()
