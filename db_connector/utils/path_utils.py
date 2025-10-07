"""
跨平台路径处理工具模块

提供跨平台的路径处理功能，包括配置目录获取、目录创建等工具方法。
支持 Windows、macOS 和 Linux 系统。
"""

import os
import platform
from pathlib import Path
from typing import Optional, Union


class PathHelper:
    """
    路径辅助类

    提供跨平台的路径处理功能，封装了常见的路径操作和系统特定的路径获取逻辑。
    """

    @staticmethod
    def get_user_config_dir(app_name: str = "db_connector") -> Path:
        """
        获取用户配置目录

        根据操作系统类型获取标准的用户配置目录，并创建应用特定的子目录。

        Args:
            app_name: 应用名称，默认为"db_connector"

        Returns:
            Path: 配置目录的Path对象

        Raises:
            OSError: 当无法创建目录时抛出（仅在回退方案也失败时）

        Example:
            >>> config_dir = PathHelper.get_user_config_dir("my_app")
            >>> print(config_dir)
            Windows: C:\\Users\\username\\AppData\\Roaming\\my_app
            macOS: /Users/username/Library/Application Support/my_app
            Linux: /home/username/.config/my_app
        """
        if not app_name or not isinstance(app_name, str):
            raise ValueError("应用名称不能为空且必须是字符串")

        system = platform.system().lower()

        try:
            if system == "windows":
                base_dir = Path(os.environ.get("APPDATA", Path.home()))
            elif system == "darwin":  # macOS
                base_dir = Path.home() / "Library" / "Application Support"
            else:  # Linux和其他Unix系统
                base_dir = Path.home() / ".config"

            config_dir = base_dir / app_name
            config_dir.mkdir(parents=True, exist_ok=True)

            return config_dir

        except OSError as e:
            # 回退到当前目录
            fallback_dir = Path.cwd() / f".{app_name}"
            try:
                fallback_dir.mkdir(exist_ok=True)
                return fallback_dir
            except OSError:
                raise OSError(f"无法创建配置目录: {str(e)}")

    @staticmethod
    def get_user_home_dir() -> Path:
        """
        获取用户主目录

        Returns:
            Path: 用户主目录的Path对象

        Example:
            >>> home_dir = PathHelper.get_user_home_dir()
            >>> print(home_dir)
            /home/username  # 或 C:\\Users\\username
        """
        return Path.home()

    @staticmethod
    def ensure_dir_exists(dir_path: Union[str, Path]) -> bool:
        """
        确保目录存在，如果不存在则创建

        Args:
            dir_path: 需要确保存在的目录路径

        Returns:
            bool: 目录是否存在或是否成功创建

        Example:
            >>> success = PathHelper.ensure_dir_exists(Path("/path/to/dir"))
            >>> print(success)
            True
        """
        if not dir_path:
            return False

        try:
            dir_path_obj = Path(dir_path) if isinstance(dir_path, str) else dir_path

            if dir_path_obj.exists():
                if dir_path_obj.is_dir():
                    return True
                else:
                    return False

            dir_path_obj.mkdir(parents=True, exist_ok=True)
            return True
        except (OSError, TypeError, ValueError):
            return False

    @staticmethod
    def normalize_path(path: Union[str, Path]) -> Path:
        """
        规范化路径字符串

        Args:
            path: 需要规范化的路径字符串或Path对象

        Returns:
            Path: 规范化后的Path对象

        Raises:
            ValueError: 当路径为空或无效时

        Example:
            >>> normalized = PathHelper.normalize_path("~/documents/../downloads")
            >>> print(normalized)
            /home/username/downloads
        """
        if not path:
            raise ValueError("路径不能为空")

        try:
            path_obj = Path(path) if isinstance(path, str) else path
            return path_obj.expanduser().resolve()
        except (OSError, TypeError, ValueError) as e:
            raise ValueError(f"无法规范化路径 '{path}': {str(e)}")

    @staticmethod
    def is_valid_path(path: Union[str, Path]) -> bool:
        """
        检查路径是否有效（不包含非法字符）

        Args:
            path: 需要检查的路径

        Returns:
            bool: 路径是否有效

        Note:
            此方法仅检查路径格式，不检查路径是否存在或可访问
        """
        if not path:
            return False

        try:
            path_str = str(path)
            # 检查常见非法字符（Windows和Unix通用）
            illegal_chars = ["<", ">", ":", '"', "|", "?", "*", "\0"]

            # 检查路径是否包含非法字符
            if any(char in path_str for char in illegal_chars):
                return False

            # 检查路径是否为空或只包含空白字符
            if not path_str.strip():
                return False

            return True
        except (TypeError, ValueError):
            return False

    @staticmethod
    def get_absolute_path(
        relative_path: Union[str, Path], base_dir: Optional[Union[str, Path]] = None
    ) -> Path:
        """
        获取相对路径的绝对路径

        Args:
            relative_path: 相对路径
            base_dir: 基准目录，如果为None则使用当前工作目录

        Returns:
            Path: 绝对路径

        Example:
            >>> abs_path = PathHelper.get_absolute_path("config/app.conf")
            >>> print(abs_path)
            /current/working/directory/config/app.conf
        """
        if not relative_path:
            raise ValueError("相对路径不能为空")

        base_path = Path(base_dir) if base_dir else Path.cwd()
        relative_path_obj = (
            Path(relative_path) if isinstance(relative_path, str) else relative_path
        )

        return (base_path / relative_path_obj).resolve()

    @staticmethod
    def safe_join(base_path: Union[str, Path], *paths: str) -> Path:
        """
        安全地连接路径，防止路径遍历攻击

        Args:
            base_path: 基础路径
            *paths: 要连接的路径部分

        Returns:
            Path: 连接后的安全路径

        Raises:
            ValueError: 当路径尝试遍历到基础路径之外时

        Example:
            >>> safe_path = PathHelper.safe_join("/safe/base", "subdir", "file.txt")
            >>> print(safe_path)
            /safe/base/subdir/file.txt
        """
        if not base_path:
            raise ValueError("基础路径不能为空")

        base_path_obj = Path(base_path) if isinstance(base_path, str) else base_path
        base_path_resolved = base_path_obj.resolve()
        result_path = base_path_resolved

        for path_part in paths:
            if not path_part or path_part == ".":
                continue

            if path_part == "..":
                raise ValueError("路径遍历不被允许")

            # 检查路径部分是否包含路径分隔符或非法字符
            if "/" in path_part or "\\" in path_part:
                raise ValueError(f"路径部分包含非法字符: {path_part}")

            result_path = result_path / path_part

        # 最终检查结果路径是否在基础路径内
        try:
            resolved_result = result_path.resolve()
            if not resolved_result.is_relative_to(base_path_resolved):
                raise ValueError("路径遍历检测到安全违规")
        except ValueError:
            raise ValueError("路径遍历检测到安全违规")

        return result_path
