"""
数据库连接器路径处理工具模块

提供跨平台的路径处理功能，包括配置目录获取、目录创建、路径验证、安全连接等工具方法。
支持 Windows、macOS 和 Linux 系统，确保路径操作的安全性和一致性。

主要功能：
- 跨平台配置目录获取
- 路径规范化与验证
- 安全路径连接（防止路径遍历攻击）
- 目录存在性检查与自动创建
"""

import os
import platform
from pathlib import Path


class PathHelper:
    """
    路径辅助类 - 提供跨平台的路径处理功能

    封装了常见的路径操作和系统特定的路径获取逻辑，所有方法均为静态方法，
    无需实例化即可使用。支持线程安全的路径操作。

    Attributes:
        所有方法均为静态方法，无实例属性

    Example:
        >>> # 获取用户配置目录
        >>> config_dir = PathHelper.get_user_config_dir("my_app")
        >>>
        >>> # 安全连接路径
        >>> safe_path = PathHelper.safe_join("/base", "subdir", "file.txt")
        >>>
        >>> # 验证路径有效性
        >>> is_valid = PathHelper.is_valid_path("/valid/path")
    """

    @staticmethod
    def get_user_config_dir(app_name: str = "db_connector") -> Path:
        """
        获取用户配置目录路径

        根据操作系统类型获取标准的用户配置目录，并创建应用特定的子目录。
        支持自动回退机制，当标准目录创建失败时回退到当前工作目录。

        Args:
            app_name (str): 应用名称，默认为"db_connector"

        Returns:
            Path: 配置目录的Path对象

        Raises:
            ValueError: 当应用名称为空或不是字符串时
            OSError: 当无法创建目录时（仅在回退方案也失败时）

        Note:
            - Windows: %APPDATA%\\{app_name}
            - macOS: ~/Library/Application Support/{app_name}
            - Linux: ~/.config/{app_name}

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
            # 根据操作系统选择基础配置目录
            if system == "windows":
                base_dir = Path(os.environ.get("APPDATA", Path.home()))
            elif system == "darwin":  # macOS
                base_dir = Path.home() / "Library" / "Application Support"
            else:  # Linux和其他Unix系统
                base_dir = Path.home() / ".config"

            # 创建应用特定的配置目录
            config_dir = base_dir / app_name
            config_dir.mkdir(parents=True, exist_ok=True)

            return config_dir

        except OSError as e:
            # 回退到当前目录（隐藏目录）
            fallback_dir = Path.cwd() / f".{app_name}"
            try:
                fallback_dir.mkdir(exist_ok=True)
                return fallback_dir
            except OSError:
                raise OSError(f"无法创建配置目录: {str(e)}")

    @staticmethod
    def get_user_home_dir() -> Path:
        """
        获取用户主目录路径

        返回当前用户的主目录路径，跨平台兼容。

        Returns:
            Path: 用户主目录的Path对象

        Example:
            >>> home_dir = PathHelper.get_user_home_dir()
            >>> print(home_dir)
            Windows: C:\\Users\\username
            Unix: /home/username
        """
        return Path.home()

    @staticmethod
    def ensure_dir_exists(dir_path: str | Path) -> bool:
        """
        确保目录存在，如果不存在则递归创建

        此方法会递归创建所有必要的父目录，提供安全的目录创建功能。

        Args:
            dir_path (str | Path): 需要确保存在的目录路径

        Returns:
            bool: 目录是否存在或是否成功创建

        Raises:
            OSError: 当目录创建失败时（权限不足等）

        Example:
            >>> # 创建目录（如果不存在）
            >>> success = PathHelper.ensure_dir_exists("/path/to/dir")
            >>> print(success)  # True
            >>>
            >>> # 检查已存在的目录
            >>> success = PathHelper.ensure_dir_exists("/existing/dir")
            >>> print(success)  # True
        """
        if not dir_path:
            return False

        try:
            dir_path_obj = Path(dir_path) if isinstance(dir_path, str) else dir_path

            # 检查目录是否已存在
            if dir_path_obj.exists():
                return dir_path_obj.is_dir()

            # 递归创建目录（包括所有父目录）
            dir_path_obj.mkdir(parents=True, exist_ok=True)
            return True

        except OSError as e:
            # 重新抛出更详细的错误信息
            raise OSError(f"无法创建目录 '{dir_path}': {str(e)}")
        except (TypeError, ValueError):
            return False

    @staticmethod
    def normalize_path(path: str | Path) -> Path:
        """
        规范化路径字符串

        将路径转换为绝对路径，解析符号链接，展开用户主目录(~)，
        并处理路径中的相对引用(.., .)。

        Args:
            path (str | Path): 需要规范化的路径字符串或Path对象

        Returns:
            Path: 规范化后的Path对象

        Raises:
            ValueError: 当路径为空或无效时
            OSError: 当路径解析失败时（如符号链接损坏）

        Example:
            >>> # 展开用户主目录
            >>> normalized = PathHelper.normalize_path("~/documents")
            >>> print(normalized)  # /home/username/documents
            >>>
            >>> # 解析相对路径
            >>> normalized = PathHelper.normalize_path("../parent/file.txt")
            >>> print(normalized)  # /absolute/path/to/parent/file.txt
        """
        if not path:
            raise ValueError("路径不能为空")

        try:
            path_obj = Path(path) if isinstance(path, str) else path
            # 展开用户主目录并解析为绝对路径
            return path_obj.expanduser().resolve()
        except OSError as e:
            raise OSError(f"无法解析路径 '{path}': {str(e)}")
        except (TypeError, ValueError) as e:
            raise ValueError(f"无效的路径格式 '{path}': {str(e)}")

    @staticmethod
    def is_valid_path(path: str | Path) -> bool:
        """
        检查路径是否有效（不包含非法字符）

        此方法仅检查路径格式的字符有效性，不检查路径是否存在或可访问。

        Args:
            path (str | Path): 需要检查的路径

        Returns:
            bool: 路径是否有效（不包含非法字符）

        Note:
            - 对于 Windows 系统，会特殊处理驱动器字母
            - 此方法不验证路径是否存在或可访问

        Example:
            >>> # 有效路径
            >>> PathHelper.is_valid_path("/valid/path/file.txt")  # True
            >>>
            >>> # 无效路径（包含非法字符）
            >>> PathHelper.is_valid_path("/invalid/path/file?.txt")  # False
        """
        if not path:
            return False

        try:
            path_str = str(path)

            # 检查路径是否为空或只包含空白字符
            if not path_str.strip():
                return False

            # 根据操作系统选择验证方法
            system = platform.system().lower()
            if system == "windows":
                return PathHelper._is_valid_path_windows(path_str)
            else:
                return PathHelper._is_valid_path_unix(path_str)

        except (TypeError, ValueError):
            return False

    @staticmethod
    def _is_valid_path_windows(path_str: str) -> bool:
        """
        检查 Windows 系统下的路径有效性（内部方法）

        Windows 路径的特殊处理：允许驱动器字母（如 C:），
        但检查路径部分的非法字符。

        Args:
            path_str (str): 路径字符串

        Returns:
            bool: 路径是否有效（不包含非法字符）

        Note:
            此方法仅检查字符有效性，不检查路径是否存在
        """
        # 处理驱动器字母（如 C:\path\to\file）
        if ":" in path_str and path_str.index(":") == 1 and path_str[0].isalpha():
            check_str = path_str[2:] if len(path_str) > 2 else ""
        else:
            check_str = path_str

        # Windows 非法字符（完整列表）
        illegal_chars = [
            "<",
            ">",
            '"',
            "|",
            "?",
            "*",
            ":",  # 基本非法字符
            "\0",
            "\n",
            "\r",
            "\t",
            "\b",
            "\f",  # 控制字符
        ]
        return not any(char in check_str for char in illegal_chars)

    @staticmethod
    def _is_valid_path_unix(path_str: str) -> bool:
        """
        检查 Unix/Linux/macOS 系统下的路径有效性（内部方法）

        Args:
            path_str (str): 路径字符串

        Returns:
            bool: 路径是否有效（不包含非法字符）

        Note:
            此方法仅检查字符有效性，不检查路径是否存在
        """
        # Unix 系统非法字符（完整列表）
        illegal_chars = [
            "<",
            ">",
            ":",
            '"',
            "|",
            "?",
            "*",  # 基本非法字符
            "\\",  # 路径分隔符（Unix使用正斜杠）
            "\0",
            "\n",
            "\r",
            "\t",
            "\b",
            "\f",  # 控制字符
        ]
        return not any(char in path_str for char in illegal_chars)

    @staticmethod
    def get_absolute_path(
        relative_path: str | Path, base_dir: str | Path | None = None
    ) -> Path:
        """
        获取相对路径的绝对路径

        将相对路径转换为基于指定基准目录的绝对路径。

        Args:
            relative_path (str | Path): 相对路径
            base_dir (str | Path | None): 基准目录，如果为None则使用当前工作目录

        Returns:
            Path: 绝对路径

        Raises:
            ValueError: 当相对路径为空时
            OSError: 当路径解析失败时

        Example:
            >>> # 使用当前工作目录作为基准
            >>> abs_path = PathHelper.get_absolute_path("config/app.conf")
            >>> print(abs_path)  # /current/working/directory/config/app.conf
            >>>
            >>> # 指定基准目录
            >>> abs_path = PathHelper.get_absolute_path("file.txt", "/base/dir")
            >>> print(abs_path)  # /base/dir/file.txt
        """
        if not relative_path:
            raise ValueError("相对路径不能为空")

        base_path = Path(base_dir) if base_dir else Path.cwd()
        relative_path_obj = (
            Path(relative_path) if isinstance(relative_path, str) else relative_path
        )

        try:
            return (base_path / relative_path_obj).resolve()
        except OSError as e:
            raise OSError(f"无法解析路径 '{relative_path}': {str(e)}")

    @staticmethod
    def safe_join(base_path: str | Path, *paths: str) -> Path:
        """
        安全地连接路径，防止路径遍历攻击

        此方法提供多层安全验证，确保最终路径不会超出基础路径范围。
        防止常见的路径遍历攻击（如使用".."跳出基础目录）。

        Args:
            base_path (str | Path): 基础路径
            *paths (str): 要连接的路径部分

        Returns:
            Path: 连接后的安全路径

        Raises:
            ValueError:
                - 当基础路径为空时
                - 当路径尝试遍历到基础路径之外时
                - 当路径部分包含非法字符时
                - 当路径部分包含路径分隔符时（防止路径注入）

        Example:
            >>> # 正常路径连接
            >>> safe_path = PathHelper.safe_join("/safe/base", "subdir", "file.txt")
            >>> print(safe_path)  # /safe/base/subdir/file.txt
            >>>
            >>> # 路径遍历攻击（会被阻止）
            >>> try:
            ...     PathHelper.safe_join("/safe/base", "..", "etc", "passwd")
            ... except ValueError as e:
            ...     print(f"安全阻止: {e}")
            ... # 安全阻止: 路径遍历不被允许
        """
        if not base_path:
            raise ValueError("基础路径不能为空")

        base_path_obj = Path(base_path) if isinstance(base_path, str) else base_path
        base_path_resolved = base_path_obj.resolve()
        result_path = base_path_resolved

        for path_part in paths:
            if not path_part or path_part == ".":
                continue

            # 防止路径遍历攻击（禁止".."）
            if path_part == "..":
                raise ValueError("路径遍历不被允许")

            # 复用完整的非法字符检查逻辑
            if not PathHelper.is_valid_path(path_part):
                raise ValueError(f"路径部分包含非法字符: {path_part}")

            # 额外检查路径分隔符（防止路径注入）
            if "/" in path_part or "\\" in path_part:
                raise ValueError(f"路径部分包含路径分隔符: {path_part}")

            result_path = result_path / path_part

        # 最终安全检查：确保结果路径在基础路径内
        try:
            resolved_result = result_path.resolve()
            if not resolved_result.is_relative_to(base_path_resolved):
                raise ValueError("路径遍历检测到安全违规")
        except ValueError:
            raise ValueError("路径遍历检测到安全违规")

        return result_path
