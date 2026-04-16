import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.db_connector_tool.utils.path_utils import PathHelper


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
            PathHelper.get_user_config_dir(None)  # type: ignore

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

        result = PathHelper.ensure_dir_exists(None)  # type: ignore
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
            PathHelper.normalize_path(None)  # type: ignore

    def test_is_valid_path(self):
        """测试路径有效性检查"""
        # 测试有效路径
        valid_paths = [
            "/valid/path",
            "C:\\valid\\path",  # Windows路径
            "valid/path/file.txt",
        ]
        for path in valid_paths:
            self.assertTrue(PathHelper.is_valid_path(path))

        # 测试无效路径（包含非法字符）
        invalid_paths = [
            "/invalid/path/file?.txt",
            "C:\\invalid\\path\\file*.txt",  # Windows路径
            "",
        ]
        for path in invalid_paths:
            self.assertFalse(PathHelper.is_valid_path(path))

        # 测试None
        self.assertFalse(PathHelper.is_valid_path(None))  # type: ignore

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
        # 测试有效Windows路径
        self.assertTrue(PathHelper._is_valid_path_windows("C:\\valid\\path"))
        self.assertTrue(PathHelper._is_valid_path_windows("C:"))

        # 测试无效Windows路径
        self.assertFalse(
            PathHelper._is_valid_path_windows("C:\\invalid\\path\\file?.txt")
        )
        self.assertFalse(
            PathHelper._is_valid_path_windows("C:\\invalid\\path\\file*.txt")
        )

    def test_is_valid_path_unix(self):
        """测试Unix路径有效性检查"""
        # 测试有效Unix路径
        self.assertTrue(PathHelper._is_valid_path_unix("/valid/path"))
        self.assertTrue(PathHelper._is_valid_path_unix("valid/path"))

        # 测试无效Unix路径
        self.assertFalse(PathHelper._is_valid_path_unix("/invalid/path/file?.txt"))
        self.assertFalse(PathHelper._is_valid_path_unix("/invalid/path/file*.txt"))

    @mock.patch("src.db_connector_tool.utils.path_utils.platform.system")
    def test_get_user_config_dir_windows(self, mock_system):
        """测试Windows系统下获取配置目录"""
        mock_system.return_value = "Windows"
        with mock.patch.dict(os.environ, {"APPDATA": self.temp_dir}):
            config_dir = PathHelper.get_user_config_dir(self.app_name)
            self.assertIsInstance(config_dir, Path)
            self.assertTrue(str(config_dir).startswith(self.temp_dir))

    @mock.patch("src.db_connector_tool.utils.path_utils.platform.system")
    def test_get_user_config_dir_macos(self, mock_system):
        """测试macOS系统下获取配置目录"""
        mock_system.return_value = "Darwin"
        config_dir = PathHelper.get_user_config_dir(self.app_name)
        self.assertIsInstance(config_dir, Path)

    @mock.patch("src.db_connector_tool.utils.path_utils.platform.system")
    def test_get_user_config_dir_fallback(self, mock_system):
        """测试获取配置目录失败时的回退机制"""
        mock_system.return_value = "Linux"
        
        # 第一次mkdir失败（主目录），第二次成功（回退目录）
        def mkdir_side_effect(**kwargs):
            if not hasattr(mkdir_side_effect, "call_count"):
                mkdir_side_effect.call_count = 0
            mkdir_side_effect.call_count += 1
            if mkdir_side_effect.call_count == 1:
                raise OSError("Permission denied")
            return None
        
        with mock.patch.object(Path, "mkdir", side_effect=mkdir_side_effect):
            config_dir = PathHelper.get_user_config_dir(self.app_name)
            self.assertIsInstance(config_dir, Path)

    @mock.patch("src.db_connector_tool.utils.path_utils.platform.system")
    def test_get_user_config_dir_fallback_failure(self, mock_system):
        """测试获取配置目录回退也失败的情况"""
        mock_system.return_value = "Linux"
        with mock.patch.object(Path, "mkdir", side_effect=OSError("Permission denied")):
            with self.assertRaises(OSError):
                PathHelper.get_user_config_dir(self.app_name)

    def test_ensure_dir_exists_file_conflict(self):
        """测试ensure_dir_exists当路径已存在但不是目录的情况"""
        test_file = os.path.join(self.temp_dir, "test_file.txt")
        with open(test_file, "w") as f:
            f.write("test")
        result = PathHelper.ensure_dir_exists(test_file)
        self.assertFalse(result)

    def test_ensure_dir_exists_type_error(self):
        """测试ensure_dir_exists处理类型错误"""
        # 让Path()构造函数抛出TypeError
        with mock.patch("src.db_connector_tool.utils.path_utils.Path", side_effect=TypeError("Bad path")):
            result = PathHelper.ensure_dir_exists("bad_path")
            self.assertFalse(result)

    def test_ensure_dir_exists_os_error(self):
        """测试ensure_dir_exists处理OSError"""
        # 测试当目录已存在但不是目录时不会报错，那我们测试在创建目录时出现OSError
        test_dir = os.path.join(self.temp_dir, "test_dir")
        with mock.patch.object(Path, "exists", return_value=False):
            with mock.patch.object(Path, "mkdir", side_effect=OSError("Permission denied")):
                with self.assertRaises(OSError) as cm:
                    PathHelper.ensure_dir_exists(test_dir)
                self.assertIn("无法创建目录", str(cm.exception))

    @mock.patch.object(Path, "expanduser")
    def test_normalize_path_os_error(self, mock_expanduser):
        """测试normalize_path处理OSError"""
        mock_expanduser.side_effect = OSError("Broken link")
        with self.assertRaises(OSError):
            PathHelper.normalize_path("broken_link")

    def test_normalize_path_type_error(self):
        """测试normalize_path处理TypeError"""
        # 让Path()构造函数抛出TypeError
        with mock.patch("src.db_connector_tool.utils.path_utils.Path", side_effect=TypeError("Bad path")):
            with self.assertRaises(ValueError):
                PathHelper.normalize_path("bad_path")

    def test_is_valid_path_whitespace(self):
        """测试is_valid_path处理只包含空白字符的路径"""
        self.assertFalse(PathHelper.is_valid_path("   "))

    @mock.patch("src.db_connector_tool.utils.path_utils.platform.system")
    def test_is_valid_path_windows_system(self, mock_system):
        """测试在Windows系统下调用is_valid_path"""
        mock_system.return_value = "Windows"
        self.assertTrue(PathHelper.is_valid_path("C:\\valid\\path"))
        self.assertFalse(PathHelper.is_valid_path("C:\\invalid\\path\\file?.txt"))

    def test_is_valid_path_type_error(self):
        """测试is_valid_path处理类型错误"""
        class BadPath:
            def __str__(self):
                raise ValueError("Cannot convert to string")
        self.assertFalse(PathHelper.is_valid_path(BadPath()))

    def test_is_valid_path_windows_no_drive(self):
        """测试Windows路径无驱动器字母的情况"""
        self.assertTrue(PathHelper._is_valid_path_windows("valid\\path"))

    @mock.patch.object(Path, "resolve")
    def test_get_absolute_path_os_error(self, mock_resolve):
        """测试get_absolute_path处理OSError"""
        mock_resolve.side_effect = OSError("Permission denied")
        with self.assertRaises(OSError):
            PathHelper.get_absolute_path("test", self.temp_dir)

    def test_safe_join_skip_empty_and_dot(self):
        """测试safe_join跳过空路径和点路径"""
        safe_path = PathHelper.safe_join(self.temp_dir, "", ".", "subdir")
        self.assertIsInstance(safe_path, Path)
        self.assertTrue(str(safe_path).endswith("subdir"))

    def test_safe_join_invalid_characters(self):
        """测试safe_join处理包含非法字符的路径部分"""
        with self.assertRaises(ValueError) as cm:
            PathHelper.safe_join(self.temp_dir, "file?.txt")
        self.assertIn("路径部分包含非法字符", str(cm.exception))

    @mock.patch.object(Path, "resolve")
    def test_safe_join_final_check_failure(self, mock_resolve):
        """测试safe_join最终安全检查失败的情况"""
        mock_resolve.side_effect = [Path(self.temp_dir), Path("/etc/passwd")]
        with self.assertRaises(ValueError) as cm:
            PathHelper.safe_join(self.temp_dir, "file.txt")
        self.assertIn("路径遍历检测到安全违规", str(cm.exception))

    def test_safe_join_value_error_on_is_relative_to(self):
        """测试safe_join中is_relative_to抛出ValueError的情况"""
        # 我们可以直接模拟一个简单的场景来覆盖这个异常处理
        base_path = Path(self.temp_dir)
        # 先创建一个模拟的Path类
        with mock.patch.object(Path, "resolve") as mock_resolve:
            # 让第一次调用返回base_path，第二次调用返回一个模拟对象
            mock_result = mock.Mock(spec=Path)
            mock_result.is_relative_to.side_effect = ValueError("Not relative")
            mock_resolve.side_effect = [base_path, mock_result]
            with self.assertRaises(ValueError) as cm:
                PathHelper.safe_join(base_path, "file.txt")
            self.assertIn("路径遍历检测到安全违规", str(cm.exception))


if __name__ == "__main__":
    unittest.main()
