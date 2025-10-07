"""
跨平台路径处理工具模块

提供跨平台的路径处理功能，包括配置目录获取、目录创建等工具方法。
支持 Windows、macOS 和 Linux 系统。
"""

import logging
import os
import platform
from pathlib import Path

logger = logging.getLogger(__name__)


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

            logger.debug(f"配置目录已创建: {config_dir}")
            return config_dir

        except OSError as e:
            logger.error(f"创建配置目录失败: {str(e)}")
            # 回退到当前目录
            fallback_dir = Path.cwd() / f".{app_name}"
            try:
                fallback_dir.mkdir(exist_ok=True)
                logger.warning(f"使用回退目录: {fallback_dir}")
                return fallback_dir
            except OSError:
                logger.error(f"回退目录创建也失败: {fallback_dir}")
                raise

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
    def ensure_dir_exists(dir_path: Path) -> bool:
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
        try:
            if dir_path.exists():
                if dir_path.is_dir():
                    logger.debug(f"目录已存在: {dir_path}")
                    return True
                else:
                    logger.error(f"路径存在但不是目录: {dir_path}")
                    return False

            dir_path.mkdir(parents=True, exist_ok=True)
            logger.info(f"目录创建成功: {dir_path}")
            return True
        except OSError as e:
            logger.error(f"创建目录失败 {dir_path}: {str(e)}")
            return False

    @staticmethod
    def normalize_path(path: str) -> Path:
        """
        规范化路径字符串

        Args:
            path: 需要规范化的路径字符串

        Returns:
            Path: 规范化后的Path对象

        Example:
            >>> normalized = PathHelper.normalize_path("~/documents/../downloads")
            >>> print(normalized)
            /home/username/downloads
        """
        return Path(path).expanduser().resolve()

    @staticmethod
    def is_valid_path(path: Path) -> bool:
        """
        检查路径是否有效（不包含非法字符）

        Args:
            path: 需要检查的路径

        Returns:
            bool: 路径是否有效

        Note:
            此方法仅检查路径格式，不检查路径是否存在或可访问
        """
        try:
            path_str = str(path)
            # 检查常见非法字符（Windows和Unix通用）
            illegal_chars = ["<", ">", ":", '"', "|", "?", "*"]
            return not any(char in path_str for char in illegal_chars)
        except (TypeError, ValueError):
            return False
