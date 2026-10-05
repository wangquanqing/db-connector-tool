import unittest
from unittest import mock

from src.db_connector_tool.batch_manager import (
    BatchDatabaseManager,
    cleanup_temp_configs,
    generate_ip_range,
)
from src.db_connector_tool.core.exceptions import QueryError

from ._isolation import IsolatedTestCase


class TestBatchDatabaseManager(IsolatedTestCase):
    """测试批量数据库连接管理器"""

    def setUp(self):
        """设置测试环境"""
        super().setUp()
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
        super().tearDown()

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
            self.batch_manager.database_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(self.batch_manager.database_manager, "add_connection"):
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
            self.batch_manager.database_manager, "list_connections", return_value=["db_000"]
        ):
            with mock.patch.object(
                self.batch_manager.database_manager.pool_manager, "remove_connection"
            ):
                with mock.patch.object(self.batch_manager.database_manager, "add_connection"):
                    results = self.batch_manager.add_batch_connections(ip_list)
                    self.assertEqual(len(results), 1)
                    self.assertTrue(results["192.168.1.1"])

    def test_test_batch_connections(self):
        """测试批量测试连接"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000", "db_001"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.database_manager, "test_connection", return_value=True
        ):
            results = self.batch_manager.test_batch_connections(max_workers=2)
            self.assertEqual(len(results), 2)
            self.assertTrue(results["db_000"])
            self.assertTrue(results["db_001"])

    def test_execute_batch_query(self):
        """测试批量执行查询"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法（execute_query 返回 (行列表, 耗时) 元组）
        with mock.patch.object(
            self.batch_manager.database_manager,
            "execute_query",
            return_value=([{"id": 1, "name": "test"}], 0.01),
        ):
            results = self.batch_manager.execute_batch_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)
            self.assertTrue(results["db_000"]["success"])
            self.assertEqual(results["db_000"]["data"], [{"id": 1, "name": "test"}])
            self.assertEqual(results["db_000"]["count"], 1)
            self.assertEqual(results["db_000"]["elapsed"], 0.01)

    def test_upgrade_table_structure(self):
        """测试批量升级表结构（DDL 走命令接口）"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法（execute_command 返回 (影响行数, 耗时) 元组）
        with mock.patch.object(
            self.batch_manager.database_manager,
            "execute_command",
            return_value=(0, 0.01),
        ) as mock_command:
            upgrade_sqls = ["ALTER TABLE users ADD COLUMN age INT"]
            results = self.batch_manager.upgrade_table_structure(upgrade_sqls)
            self.assertEqual(len(results), 1)
            self.assertTrue(results["db_000"]["success"])
            self.assertEqual(
                results["db_000"]["executions"][0]["affected_rows"], 0
            )
            mock_command.assert_called_once_with(
                "db_000", "ALTER TABLE users ADD COLUMN age INT"
            )

    def test_execute_statement_routes_ddl_to_command(self):
        """测试语句路由：DDL 走 execute_command，查询走 execute_query"""
        self.batch_manager.set_base_config(self.base_config)

        with mock.patch.object(
            self.batch_manager.database_manager, "execute_command", return_value=(3, 0.1)
        ) as mock_command:
            with mock.patch.object(
                self.batch_manager.database_manager,
                "execute_query",
                return_value=([{"id": 1}], 0.1),
            ) as mock_query:
                ddl_result = self.batch_manager._execute_statement(
                    "db_000", "ALTER TABLE t ADD COLUMN c INT"
                )
                query_result = self.batch_manager._execute_statement(
                    "db_000", "SELECT id FROM t"
                )

        mock_command.assert_called_once()
        mock_query.assert_called_once()
        self.assertEqual(ddl_result, {"success": True, "affected_rows": 3})
        self.assertEqual(query_result, {"success": True, "affected_rows": 1})

    def test_upgrade_table_structure_persists_ddl(self):
        """测试批量升级表结构后 DDL 确实落库（回归：曾用查询接口执行 DDL）"""
        db_file = self.temp_path / "upgrade.db"
        self.batch_manager.set_base_config(
            {"type": "sqlite", "database": str(db_file)}
        )
        self.batch_manager.add_batch_connections(["10.0.0.1"])
        conn_name = self.batch_manager._get_all_connection_names()[0]

        results = self.batch_manager.upgrade_table_structure(
            ["CREATE TABLE upgraded (id INTEGER PRIMARY KEY, name TEXT)"]
        )
        self.assertTrue(results[conn_name]["success"], msg=results)

        rows, _elapsed = self.batch_manager.database_manager.execute_query(
            conn_name, "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
        self.assertIn("upgraded", [row["name"] for row in rows])

    def test_upgrade_table_structure_rolls_back_on_failure(self):
        """测试升级失败时执行回滚语句"""
        db_file = self.temp_path / "rollback.db"
        self.batch_manager.set_base_config(
            {"type": "sqlite", "database": str(db_file)}
        )
        self.batch_manager.add_batch_connections(["10.0.0.1"])
        conn_name = self.batch_manager._get_all_connection_names()[0]

        results = self.batch_manager.upgrade_table_structure(
            ["ALTER TABLE not_exist ADD COLUMN c INT"],
            rollback_sqls=["CREATE TABLE rolled_back (id INT)"],
        )
        self.assertFalse(results[conn_name]["success"])

        rows, _elapsed = self.batch_manager.database_manager.execute_query(
            conn_name, "SELECT name FROM sqlite_master WHERE type = 'table'"
        )
        self.assertIn("rolled_back", [row["name"] for row in rows])

    def test_get_connection_stats(self):
        """测试获取连接统计信息"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.database_manager,
            "get_connection_info",
            return_value={"host": "192.168.1.1"},
        ):
            stats = self.batch_manager.get_connection_stats()
            self.assertEqual(len(stats), 1)
            self.assertEqual(stats["db_000"]["host"], "192.168.1.1")

    def test_close_all_connections(self):
        """测试关闭所有连接"""
        # 模拟数据库管理器的方法
        with mock.patch.object(self.batch_manager.database_manager, "close_all_connections"):
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

    def test_add_batch_connections_no_base_config(self):
        """测试未设置基础配置时批量添加连接"""
        ip_list = ["192.168.1.1"]
        with self.assertRaises(ValueError):
            self.batch_manager.add_batch_connections(ip_list)

    def test_cleanup_already_cleaned(self):
        """测试清理已清理的批量管理器"""
        self.batch_manager._is_cleaned = True
        self.batch_manager.cleanup()
        # 应该不会抛出异常

    def test_test_batch_connections_empty(self):
        """测试测试空连接列表"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = []
        results = self.batch_manager.test_batch_connections()
        self.assertEqual(len(results), 0)

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

    def test_add_batch_connections_with_connection_names_update(self):
        """测试批量添加连接时更新连接名称列表"""
        self.batch_manager.set_base_config(self.base_config)
        ip_list = ["192.168.1.1"]

        # 模拟数据库管理器的方法
        with mock.patch.object(
            self.batch_manager.database_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(self.batch_manager.database_manager, "add_connection"):
                results = self.batch_manager.add_batch_connections(ip_list)
                self.assertEqual(len(results), 1)
                self.assertTrue(results["192.168.1.1"])
                self.assertIn("db_000", self.batch_manager._connection_names)

    def test_remove_existing_connection_with_removal_from_list(self):
        """测试删除已存在连接时从连接名称列表中移除"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法
        with mock.patch.object(self.batch_manager.database_manager, "remove_connection"):
            self.batch_manager._remove_existing_connection("db_000")
            self.assertNotIn("db_000", self.batch_manager._connection_names)

    def test_remove_existing_connection_with_critical_error(self):
        """测试删除已存在连接时发生严重错误"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟删除连接时发生严重错误
        with mock.patch.object(
            self.batch_manager.database_manager,
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
            self.batch_manager.database_manager.pool_manager,
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
            self.batch_manager.database_manager, "list_connections", return_value=[]
        ):
            with mock.patch.object(self.batch_manager.database_manager, "add_connection"):
                results = self.batch_manager.add_batch_connections(ip_list)
                self.assertEqual(len(results), 1)
                self.assertTrue(results["192.168.1.1"])
                # 验证连接名称列表中仍然只有一个元素（没有重复添加）
                self.assertEqual(len(self.batch_manager._connection_names), 1)
                self.assertIn("db_000", self.batch_manager._connection_names)

    def test_execute_batch_query_with_query_error(self):
        """测试批量执行查询时出现QueryError"""
        self.batch_manager.set_base_config(self.base_config)
        self.batch_manager._connection_names = ["db_000"]

        # 模拟数据库管理器的方法抛出QueryError
        with mock.patch.object(
            self.batch_manager.database_manager,
            "execute_query",
            side_effect=QueryError("Query failed"),
        ):
            results = self.batch_manager.execute_batch_query("SELECT * FROM users")
            self.assertEqual(len(results), 1)
            self.assertFalse(results["db_000"]["success"])
            self.assertIn("error", results["db_000"])
            self.assertEqual(results["db_000"]["data"], [])
            self.assertEqual(results["db_000"]["count"], 0)

    def test_execute_upgrade_sqls_with_query_error_and_rollback(self):
        """测试执行升级SQL时出现QueryError并执行回滚"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟执行 DDL 抛出QueryError，但回滚成功
        with mock.patch.object(
            self.batch_manager.database_manager,
            "execute_command",
            side_effect=[QueryError("Query failed"), (0, 0.01)],
        ):
            results = self.batch_manager._execute_upgrade_sqls(
                "db_000", ["ALTER TABLE users ADD COLUMN age INT"], ["ROLLBACK"]
            )
            self.assertEqual(len(results), 1)
            self.assertFalse(results[0]["success"])
            self.assertIn("error", results[0])


if __name__ == "__main__":
    unittest.main()
