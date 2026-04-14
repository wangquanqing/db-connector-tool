import os
import platform
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from db_connector_tool.utils.path_utils import PathHelper


class TestPathHelper(unittest.TestCase):
    """测试路径工具模块"""

    def setUp(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()
        self.app_name = "test_app"

    def tearDown(self):
        """清理测试环境"""
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_get_user_config_dir(self):
        """测试获取用户配置目录"""
        config_dir = PathHelper.get_user_config_dir(self.app_name)
        self.assertIsInstance(config_dir, Path)
        self.assertTrue(config_dir.exists())
        
        # 测试应用名称验证
        with self.assertRaises(ValueError):
            PathHelper.get_user_config_dir("")
        
        with self.assertRaises(ValueError):
            PathHelper.get_user_config_dir(None)

    def test_get_user_home_dir(self):
        """测试获取用户主目录"""
        home_dir = PathHelper.get_user_home_dir()
        self.assertIsInstance(home_dir, Path)
        self.assertTrue(home_dir.exists())

    def test_ensure_dir_exists(self):
        """测试确保目录存在"""
        # 测试不存在的目录
        new_dir = os.path.join(self.temp_dir, "new_dir")
        self.assertFalse(os.path.exists(new_dir))
        result = PathHelper.ensure_dir_exists(new_dir)
        self.assertTrue(result)
        self.assertTrue(os.path.exists(new_dir))
        
        # 测试已存在的目录
        result = PathHelper.ensure_dir_exists(new_dir)
        self.assertTrue(result)
        
        # 测试无效参数
        result = PathHelper.ensure_dir_exists("")
        self.assertFalse(result)
        
        result = PathHelper.ensure_dir_exists(None)
        self.assertFalse(result)

    def test_normalize_path(self):
        """测试路径规范化"""
        # 测试相对路径
        relative_path = "../test"
        normalized = PathHelper.normalize_path(relative_path)
        self.assertIsInstance(normalized, Path)
        self.assertTrue(normalized.is_absolute())
        
        # 测试用户主目录
        home_path = "~"
        normalized = PathHelper.normalize_path(home_path)
        self.assertIsInstance(normalized, Path)
        self.assertTrue(normalized.is_absolute())
        
        # 测试无效参数
        with self.assertRaises(ValueError):
            PathHelper.normalize_path("")
        
        with self.assertRaises(ValueError):
            PathHelper.normalize_path(None)

    def test_is_valid_path(self):
        """测试路径有效性检查"""
        # 测试有效路径
        valid_paths = [
            "/valid/path",
            "C:\\valid\\path",  # Windows路径
            "valid/path/file.txt"
        ]
        for path in valid_paths:
            self.assertTrue(PathHelper.is_valid_path(path))
        
        # 测试无效路径（包含非法字符）
        invalid_paths = [
            "/invalid/path/file?.txt",
            "C:\\invalid\\path\\file*.txt",  # Windows路径
            ""
        ]
        for path in invalid_paths:
            self.assertFalse(PathHelper.is_valid_path(path))
        
        # 测试None
        self.assertFalse(PathHelper.is_valid_path(None))

    def test_get_absolute_path(self):
        """测试获取绝对路径"""
        # 测试相对路径
        relative_path = "test/file.txt"
        abs_path = PathHelper.get_absolute_path(relative_path, self.temp_dir)
        self.assertIsInstance(abs_path, Path)
        self.assertTrue(abs_path.is_absolute())
        self.assertTrue(str(abs_path).startswith(self.temp_dir))
        
        # 测试使用当前工作目录作为基准
        abs_path = PathHelper.get_absolute_path(relative_path)
        self.assertIsInstance(abs_path, Path)
        self.assertTrue(abs_path.is_absolute())
        
        # 测试无效参数
        with self.assertRaises(ValueError):
            PathHelper.get_absolute_path("", self.temp_dir)

    def test_safe_join(self):
        """测试安全路径连接"""
        # 测试正常路径连接
        base_path = self.temp_dir
        safe_path = PathHelper.safe_join(base_path, "subdir", "file.txt")
        self.assertIsInstance(safe_path, Path)
        self.assertTrue(str(safe_path).startswith(base_path))
        
        # 测试路径遍历攻击（应该被阻止）
        with self.assertRaises(ValueError):
            PathHelper.safe_join(base_path, "..", "etc", "passwd")
        
        # 测试包含路径分隔符的路径部分（应该被阻止）
        with self.assertRaises(ValueError):
            PathHelper.safe_join(base_path, "subdir/file.txt")
        
        # 测试无效参数
        with self.assertRaises(ValueError):
            PathHelper.safe_join("", "subdir")

    def test_is_valid_path_windows(self):
        """测试Windows路径有效性检查"""
        from db_connector_tool.utils.path_utils import PathHelper
        
        # 测试有效Windows路径
        self.assertTrue(PathHelper._is_valid_path_windows("C:\\valid\\path"))
        self.assertTrue(PathHelper._is_valid_path_windows("C:"))
        
        # 测试无效Windows路径
        self.assertFalse(PathHelper._is_valid_path_windows("C:\\invalid\\path\\file?.txt"))
        self.assertFalse(PathHelper._is_valid_path_windows("C:\\invalid\\path\\file*.txt"))

    def test_is_valid_path_unix(self):
        """测试Unix路径有效性检查"""
        from db_connector_tool.utils.path_utils import PathHelper
        
        # 测试有效Unix路径
        self.assertTrue(PathHelper._is_valid_path_unix("/valid/path"))
        self.assertTrue(PathHelper._is_valid_path_unix("valid/path"))
        
        # 测试无效Unix路径
        self.assertFalse(PathHelper._is_valid_path_unix("/invalid/path/file?.txt"))
        self.assertFalse(PathHelper._is_valid_path_unix("/invalid/path/file*.txt"))


if __name__ == "__main__":
    unittest.main()
