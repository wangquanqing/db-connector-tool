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
            with mock.patch.object(self.batch_manager.db_manager, "close_connection"):
                with mock.patch.object(
                    self.batch_manager.db_manager, "remove_connection"
                ):
                    with mock.patch.object(
                        self.batch_manager.db_manager, "add_connection"
                    ):
                        results = self.batch_manager.add_batch_connections(ip_list)
                        self.assertEqual(len(results), 1)
                        self.assertTrue(results["192.168.1.1"])

    def test_cleanup(self):
        """测试清理批量管理器资源"""
        self.batch_manager.set_base_config(self.base_config)

        # 模拟数据库管理器的方法
        with mock.patch.object(self.batch_manager.db_manager, "close_connection"):
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


if __name__ == "__main__":
    unittest.main()
