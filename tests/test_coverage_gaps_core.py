"""覆盖率补测：core 层与 utils 层的边界分支

本文件集中覆盖那些「正常路径不会走到、但代码里明确处理了」的分支：
异常兜底、平台差异、空输入、降级路径。

**为什么单独一个文件**：这些用例大多是「人为触发异常」或「patch 平台判断」，
与功能测试混在一起会掩盖真正的业务意图；集中放置便于审查
「每条兜底分支是否真的有意义」（若某条兜底根本不可能发生，就该删掉它，
而不是写个假测试刷覆盖率）。
"""

import gc
import logging
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.db_connector_tool.core.connection_pool import ConnectionPoolManager
from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.exceptions import DatabaseError
from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.core.result import ColumnInfo, QueryResult, row_values
from src.db_connector_tool.utils.logging_utils import (
    _configure_file_handlers_from_config,
)
from src.db_connector_tool.utils.path_utils import PathHelper

from ._isolation import IsolatedTestCase


class TestResultEdgeBranches(IsolatedTestCase):
    """core/result.py 的兜底分支"""

    def test_row_values_object_without_position_info(self) -> None:
        """既无 values() 也无可迭代内容的对象返回空元组"""

        class Opaque:
            """无位置信息"""

            __slots__ = ()

        self.assertEqual(row_values(Opaque()), ())

    def test_row_values_values_raises(self) -> None:
        """values() 抛 TypeError 时安全降级"""

        class Broken:
            """values() 直接抛异常"""

            def values(self):
                raise TypeError("boom")

        self.assertEqual(row_values(Broken()), ())

    def test_row_values_inner_mapping(self) -> None:
        """Row 形态：走 _mapping.values()"""

        class FakeMapping:
            """内部映射"""

            @staticmethod
            def values():
                """返回按位置的值"""
                return [1, 2, 3]

        class FakeRow:
            """模拟 SQLAlchemy Row"""

            _mapping = FakeMapping()

        self.assertEqual(row_values(FakeRow()), (1, 2, 3))

    def test_metadata_shorter_than_names(self) -> None:
        """元数据比实际列少时，按行宽补齐列名"""
        result = QueryResult.from_rows([(1, 2, 3)], [{"name": "a"}])
        self.assertEqual(len(result.columns), 3)
        self.assertEqual(result.raw_column_names(), ["a", "column_1", "column_2"])

    def test_metadata_longer_than_names(self) -> None:
        """元数据比实际列多时，以元数据为准补齐行宽"""
        result = QueryResult.from_rows([(1,)], [{"name": "a"}, {"name": "b"}])
        self.assertEqual(len(result.columns), 2)
        self.assertEqual(result.rows[0], (1, None))

    def test_derive_names_from_plain_dict(self) -> None:
        """纯字典输入按插入顺序推导列名"""
        result = QueryResult.from_rows([{"x": 1, "y": 2}])
        self.assertEqual(result.column_names(), ["x", "y"])

    def test_derive_names_from_sequence_fallback(self) -> None:
        """无 keys() 的序列用占位列名"""
        result = QueryResult.from_rows([[1, 2]])
        self.assertEqual(result.column_names(), ["column_0", "column_1"])

    def test_column_info_reserved_fields(self) -> None:
        """ColumnInfo 的预留字段有安全默认值"""
        column = ColumnInfo(0, "c", "c")
        self.assertIsNone(column.timezone_aware)
        self.assertFalse(column.is_binary)
        self.assertIsNone(column.table)


class TestConnectionPoolEdgeBranches(IsolatedTestCase):
    """core/connection_pool.py 的兜底分支"""

    def test_close_all_on_empty_pool(self) -> None:
        """空池直接返回 (0, 0)"""
        pool = ConnectionPoolManager()
        self.assertEqual(pool.close_all_connections(), (0, 0))

    def test_close_all_forces_clear_when_incomplete(self) -> None:
        """清理不完整时强制清空池"""
        pool = ConnectionPoolManager()
        pool.connection_pool["x"] = mock.Mock()
        # 让 _close_all_connections 假装没清掉
        with mock.patch.object(pool, "_close_all_connections", return_value=(0, 1)):
            success, errors = pool.close_all_connections()
        self.assertEqual((success, errors), (0, 1))
        self.assertFalse(pool.connection_pool)

    def test_close_all_with_errors_logs_warning(self) -> None:
        """有失败项时正常返回计数"""
        pool = ConnectionPoolManager()
        pool.connection_pool["x"] = mock.Mock()
        with mock.patch.object(pool, "_close_all_connections", return_value=(1, 1)):
            self.assertEqual(pool.close_all_connections(), (1, 1))

    def test_remove_connection_swallows_severe_errors(self) -> None:
        """清理单个连接时的严重异常不影响其它连接"""
        pool = ConnectionPoolManager()
        fake = mock.Mock()
        fake.close.side_effect = RuntimeError("boom")
        pool.connection_pool["x"] = fake
        pool.remove_connection("x")  # 不应抛异常
        self.assertNotIn("x", pool.connection_pool)

    def test_get_connection_returns_none_for_missing(self) -> None:
        """取不存在的连接返回 None"""
        pool = ConnectionPoolManager()
        self.assertIsNone(pool.get_connection("nope"))

    def test_is_connection_valid_handles_attribute_error(self) -> None:
        """有效性检查遇到属性异常按无效处理"""
        pool = ConnectionPoolManager()
        fake = mock.Mock()
        # 让基本状态检查抛 AttributeError
        with mock.patch.object(
            pool, "_check_driver_basic_status", side_effect=AttributeError("boom")
        ):
            self.assertFalse(pool._is_connection_valid(fake))  # pylint: disable=protected-access


class TestCryptoEdgeBranches(IsolatedTestCase):
    """core/crypto.py 的兜底分支"""

    def test_gc_failure_is_ignored(self) -> None:
        """垃圾回收失败不影响清理流程"""
        crypto = CryptoManager(password="My$trongP@ssw0rd123!")
        with mock.patch.object(gc, "collect", side_effect=MemoryError("boom")):
            crypto._clear_sensitive_data()  # pylint: disable=protected-access
        self.assertTrue(crypto._cleaned)  # pylint: disable=protected-access

    def test_gc_runtime_error_is_ignored(self) -> None:
        """垃圾回收抛 RuntimeError 同样被吞掉"""
        crypto = CryptoManager(password="My$trongP@ssw0rd123!")
        with mock.patch.object(gc, "collect", side_effect=RuntimeError("boom")):
            crypto._clear_sensitive_data()  # pylint: disable=protected-access
        self.assertTrue(crypto._cleaned)  # pylint: disable=protected-access


class TestKeyManagerEdgeBranches(IsolatedTestCase):
    """core/key_manager.py 的兜底分支"""

    def test_corrupted_key_file_is_replaced(self) -> None:
        """密钥文件损坏时重建而不是崩溃"""
        manager = KeyManager("cov_key_corrupt")
        key_path = manager.config_dir / "encryption.key"
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_text("这不是有效的 TOML ###", encoding="utf-8")

        # 直接走文件分支：损坏文件应触发 _create_new_key
        manager._load_or_create_key_from_file()  # pylint: disable=protected-access
        self.assertTrue(key_path.exists())

    def test_missing_key_file_is_created(self) -> None:
        """密钥文件不存在时创建"""
        manager = KeyManager("cov_key_missing")
        key_path = manager.config_dir / "encryption.key"
        if key_path.exists():
            key_path.unlink()
        manager._load_or_create_key_from_file()  # pylint: disable=protected-access
        self.assertTrue(key_path.exists())


class TestPathUtilsEdgeBranches(IsolatedTestCase):
    """utils/path_utils.py 的平台分支与异常兜底"""

    def test_is_valid_path_unix_branch(self) -> None:
        """非 Windows 平台走 unix 校验分支"""
        with mock.patch("platform.system", return_value="Linux"):
            self.assertTrue(PathHelper.is_valid_path("/tmp/a.txt"))
            self.assertFalse(PathHelper.is_valid_path(""))

    def test_is_valid_path_swallows_type_error(self) -> None:
        """传入无法转字符串的对象返回 False"""

        class Bad:
            """str() 抛异常"""

            def __str__(self):
                raise TypeError("boom")

        self.assertFalse(PathHelper.is_valid_path(Bad()))

    def test_rename_if_exists_increments_counter(self) -> None:
        """重名时不断递增序号直到可用"""
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "f.txt"
            base.write_text("x", encoding="utf-8")
            (Path(tmp) / "f (1).txt").write_text("x", encoding="utf-8")
            renamed = PathHelper.rename_if_exists(base)
            self.assertNotEqual(renamed, base)
            self.assertTrue(renamed.exists())

    def test_set_permissions_unix_branch(self) -> None:
        """非 Windows 平台走 unix 权限分支"""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "f.txt"
            target.write_text("x", encoding="utf-8")
            with mock.patch("platform.system", return_value="Linux"):
                result = PathHelper.set_secure_file_permissions(target)
            self.assertIsInstance(result, bool)

    def test_unix_permissions_swallows_oserror(self) -> None:
        """chmod 失败时返回 False 而非崩溃"""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "f.txt"
            target.write_text("x", encoding="utf-8")
            with mock.patch("os.chmod", side_effect=OSError("boom")):
                self.assertFalse(
                    PathHelper._set_unix_file_permissions(target)  # pylint: disable=protected-access
                )

    def test_windows_permissions_swallows_oserror(self) -> None:
        """icacls 调用失败时返回 False"""
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "f.txt"
            target.write_text("x", encoding="utf-8")
            with mock.patch("subprocess.run", side_effect=OSError("boom")):
                self.assertFalse(
                    PathHelper._set_windows_file_permissions(target)  # pylint: disable=protected-access
                )


class TestLoggingEdgeBranches(IsolatedTestCase):
    """utils/logging_utils.py 的异常翻译"""

    @staticmethod
    def _file_handler_config(tmp: str):
        """构造 _configure_file_handlers_from_config 所需的配置字典"""
        return {
            "logger": logging.getLogger("cov_log_test"),
            "formatter": logging.Formatter("%(message)s"),
            "log_file": Path(tmp) / "app.log",
            "log_dir_path": Path(tmp),
            "app_name": "cov_app",
            "log_level": logging.INFO,
            "separate_error_log": False,
            "max_file_size": 1024,
            "backup_count": 1,
        }

    def test_permission_error_is_wrapped(self) -> None:
        """无权限写日志时抛出带路径的 PermissionError"""
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch(
                "logging.handlers.RotatingFileHandler",
                side_effect=PermissionError("denied"),
            ):
                with self.assertRaises(PermissionError) as ctx:
                    _configure_file_handlers_from_config(self._file_handler_config(tmp))
            self.assertIn("没有权限写入日志文件", str(ctx.exception))

    def test_oserror_is_wrapped(self) -> None:
        """其它 IO 错误包装为 OSError"""
        with tempfile.TemporaryDirectory() as tmp:
            with mock.patch(
                "logging.handlers.RotatingFileHandler",
                side_effect=OSError("disk full"),
            ):
                with self.assertRaises(OSError) as ctx:
                    _configure_file_handlers_from_config(self._file_handler_config(tmp))
            self.assertIn("无法创建日志文件", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
