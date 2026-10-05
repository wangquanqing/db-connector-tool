"""本轮审计新增的回归测试

覆盖此前完全或部分未被测试的公共行为：

- DatabaseManager.get_connection(config_overrides=...) 临时连接路径
- 连接失败的错误计数不重复累加
- 连接池对「没有 test_connection 的鸭子类型驱动」的诊断兼容
- BatchDatabaseManager.cleanup_temp_configs
- PathHelper 的扩展名/文件类型/重命名等分支
"""

import unittest
from unittest import mock

from src.db_connector_tool import BatchDatabaseManager, PathHelper
from src.db_connector_tool.batch_manager import cleanup_temp_configs
from src.db_connector_tool.core.connections import DatabaseManager
from src.db_connector_tool.core.exceptions import DBConnectionError

from ._isolation import IsolatedTestCase


class TestGetConnectionWithOverrides(IsolatedTestCase):
    """get_connection 的配置覆盖路径（此前零覆盖）"""

    def setUp(self) -> None:
        super().setUp()
        self.db_file = self.temp_path / "overrides.db"
        self.db_manager = DatabaseManager("override_app")
        self.db_manager.add_connection(
            "lite", {"type": "sqlite", "database": str(self.db_file)}
        )

    def tearDown(self) -> None:
        self.db_manager.close_all_connections()
        super().tearDown()

    def test_overrides_return_working_driver(self) -> None:
        """临时配置覆盖应返回可用的驱动，且不影响原连接"""
        driver = self.db_manager.get_connection("lite", {"pool_size": 1})
        self.assertIsNotNone(driver)
        # 驱动层的 execute_query 直接返回行列表（DatabaseManager 层才带耗时元组）
        rows = driver.execute_query("SELECT 1 AS one")
        self.assertEqual(rows[0]["one"], 1)

        # 原连接名依旧可用（覆盖走临时连接，不污染原池条目）
        original = self.db_manager.get_connection("lite")
        self.assertIsNotNone(original)

    def test_overrides_failure_raises_connection_error(self) -> None:
        """临时连接建立失败时抛 DBConnectionError"""
        with mock.patch(
            "src.db_connector_tool.core.connections.SQLAlchemyDriver"
        ) as mock_driver_cls:
            mock_driver_cls.return_value.connect.side_effect = OSError("boom")
            with self.assertRaises(DBConnectionError):
                self.db_manager.get_connection("lite", {"host": "unreachable"})


class TestConnectionErrorAccounting(IsolatedTestCase):
    """连接失败的错误计数只累加一次"""

    def setUp(self) -> None:
        super().setUp()
        self.db_manager = DatabaseManager("accounting_app")

    def test_single_failure_counts_once(self) -> None:
        """一次连接失败只记一次错误"""
        self.db_manager.config_manager.list_configs = mock.Mock(return_value=["bad"])
        self.db_manager.config_manager.get_config = mock.Mock(
            return_value={"type": "sqlite", "database": str(self.temp_path / "x.db")}
        )
        with mock.patch(
            "src.db_connector_tool.core.connections.SQLAlchemyDriver"
        ) as mock_driver_cls:
            mock_driver_cls.return_value.connect.side_effect = OSError("refused")

            with self.assertRaises(DBConnectionError):
                self.db_manager.get_connection("bad")

        stats = self.db_manager.pool_manager.get_statistics()
        self.assertEqual(stats["connection_errors"], 1)

    def test_connection_test_failure_counts_once(self) -> None:
        """test_connection 失败路径同样只记一次"""
        self.db_manager.config_manager.list_configs = mock.Mock(return_value=["bad"])
        self.db_manager.config_manager.get_config = mock.Mock(
            return_value={"type": "sqlite", "database": str(self.temp_path / "y.db")}
        )
        with mock.patch(
            "src.db_connector_tool.core.connections.SQLAlchemyDriver"
        ) as mock_driver_cls:
            mock_driver_cls.return_value.connect.side_effect = OSError("refused")

            self.assertFalse(self.db_manager.test_connection("bad"))

        stats = self.db_manager.pool_manager.get_statistics()
        self.assertEqual(stats["connection_errors"], 1)


class TestPoolDuckTypedDriver(IsolatedTestCase):
    """没有 test_connection 的鸭子类型驱动不应让诊断接口崩溃"""

    def test_pool_status_tolerates_driver_without_ping(self) -> None:
        """诊断接口与显式探测都要兼容缺少 test_connection 的驱动"""
        driver = mock.Mock(spec=["engine"])
        driver.engine = mock.Mock()

        pool = DatabaseManager("duck_app").pool_manager
        pool.add_connection("duck", driver)

        status = pool.get_connection_pool_status()
        self.assertEqual(status["active_connections"], 1)

        # 显式 in-process 探测也保持兼容
        self.assertTrue(
            pool._is_connection_valid(driver, ping=True)  # pylint: disable=protected-access
        )


class TestCleanupTempConfigs(IsolatedTestCase):
    """cleanup_temp_configs 清理临时批量配置"""

    def test_cleanup_removes_only_temp_configs(self) -> None:
        """只清理 connections_*.toml，不影响主配置文件"""
        (self.temp_path / "connections_batch.toml").write_text("x", encoding="utf-8")
        (self.temp_path / "connections_job2.toml").write_text("x", encoding="utf-8")
        keep = self.temp_path / "connections.toml"
        keep.write_text("x", encoding="utf-8")

        cleanup_temp_configs("cleanup_app")

        self.assertFalse((self.temp_path / "connections_batch.toml").exists())
        self.assertFalse((self.temp_path / "connections_job2.toml").exists())
        self.assertTrue(keep.exists(), "主配置文件不应被清理")

    def test_batch_manager_creates_and_removes_temp_config(self) -> None:
        """批量管理器退出上下文后临时配置文件被删除"""
        with BatchDatabaseManager("audit_batch") as batch:
            batch.set_base_config(
                {"type": "sqlite", "database": str(self.temp_path / "b.db")}
            )
            batch.add_batch_connections(["10.0.0.1"])
            temp_config = self.temp_path / "connections_audit_batch.toml"
            self.assertTrue(temp_config.exists())

        self.assertFalse(temp_config.exists())


class TestPathHelperBranches(IsolatedTestCase):
    """PathHelper 此前未覆盖的分支"""

    def test_get_file_extension_branches(self) -> None:
        """扩展名提取支持带点/不带点与无扩展名"""
        self.assertEqual(PathHelper.get_file_extension("/a/b/report.csv"), ".csv")
        self.assertEqual(
            PathHelper.get_file_extension("/a/b/report.csv", include_dot=False), "csv"
        )
        self.assertEqual(PathHelper.get_file_extension("/a/b/noext"), "")
        with self.assertRaises(ValueError):
            PathHelper.get_file_extension("")

    def test_has_file_extension(self) -> None:
        """扩展名匹配不区分大小写，支持字符串与列表"""
        self.assertTrue(PathHelper.has_file_extension("a.txt", "txt"))
        self.assertTrue(PathHelper.has_file_extension("a.TXT", ".txt"))
        self.assertTrue(PathHelper.has_file_extension("a.sql", ["py", "sql"]))
        self.assertFalse(PathHelper.has_file_extension("a.sql", ["py", "txt"]))
        with self.assertRaises(ValueError):
            PathHelper.has_file_extension("", "txt")
        with self.assertRaises(ValueError):
            PathHelper.has_file_extension("a.txt", [])

    def test_is_file_type(self) -> None:
        """按类型分组判断文件，未知类型报错"""
        self.assertTrue(PathHelper.is_file_type("dump.sql", "code"))
        self.assertTrue(PathHelper.is_file_type("photo.JPG", "image"))
        self.assertTrue(PathHelper.is_file_type("backup.tar.gz", "archive"))
        self.assertFalse(PathHelper.is_file_type("photo.jpg", "video"))
        with self.assertRaises(ValueError):
            PathHelper.is_file_type("a.txt", "unknown-type")

    def test_rename_if_exists(self) -> None:
        """重名文件按 Windows 风格追加 (n) 后缀"""
        target = self.temp_path / "report.txt"
        self.assertEqual(PathHelper.rename_if_exists(target), target)

        target.write_text("v1", encoding="utf-8")
        renamed = PathHelper.rename_if_exists(target)
        self.assertEqual(renamed.name, "report (1).txt")
        self.assertTrue(renamed.exists())
        self.assertFalse(target.exists())

        renamed.write_text("v2", encoding="utf-8")
        again = PathHelper.rename_if_exists(renamed)
        self.assertEqual(again.name, "report (1) (1).txt")

    def test_set_secure_file_permissions(self) -> None:
        """权限设置返回布尔值，文件不存在时报错"""
        secure_file = self.temp_path / "secure.key"
        secure_file.write_text("secret", encoding="utf-8")
        self.assertIsInstance(PathHelper.set_secure_file_permissions(secure_file), bool)

        with self.assertRaises(OSError):
            PathHelper.set_secure_file_permissions(self.temp_path / "missing.key")

    def test_is_valid_path_and_normalize(self) -> None:
        """路径有效性判断与规范化"""
        self.assertFalse(PathHelper.is_valid_path(""))
        self.assertTrue(PathHelper.is_valid_path(self.temp_path / "a.txt"))
        with self.assertRaises(ValueError):
            PathHelper.normalize_path("")

    def test_safe_join_rejects_traversal(self) -> None:
        """安全拼接拒绝路径穿越与非法片段"""
        base = self.temp_path
        joined = PathHelper.safe_join(base, "sub", "file.txt")
        self.assertTrue(str(joined).startswith(str(base)))

        with self.assertRaises(ValueError):
            PathHelper.safe_join(base, "..")
        with self.assertRaises(ValueError):
            PathHelper.safe_join(base, "sub/dir")
        with self.assertRaises(ValueError):
            PathHelper.safe_join("", "x")


if __name__ == "__main__":
    unittest.main()
