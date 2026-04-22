"""路径处理工具模块 (Path Utilities)

Example:
>>> from db_connector_tool import PathHelper
>>>
>>> # 获取用户配置目录
>>> config_dir = PathHelper.get_user_config_dir("my_app")
>>> print(config_dir)
Windows: C:/Users/username/AppData/Roaming/my_app
macOS: /Users/username/Library/Application Support/my_app
Linux: /home/username/.config/my_app
>>>
>>> # 安全连接路径
>>> safe_path = PathHelper.safe_join("/base", "subdir", "file.txt")
>>> print(safe_path)  # /base/subdir/file.txt
>>>
>>> # 验证路径有效性
>>> is_valid = PathHelper.is_valid_path("/valid/path")
>>> print(is_valid)  # True
"""

import getpass
import os
import platform
import stat
import subprocess
from pathlib import Path


class PathHelper:
    """路径辅助类 (Path Helper)

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
    def ensure_dir_exists(dir_path: str | Path) -> bool:
        """确保目录存在，如果不存在则递归创建

        Args:
            dir_path: 需要确保存在的目录路径

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
            raise OSError(f"无法创建目录 '{dir_path}': {str(e)}") from e
        except (TypeError, ValueError):
            return False

    @staticmethod
    def is_valid_path(path: str | Path) -> bool:
        """检查路径是否有效（不包含非法字符）

        Args:
            path: 需要检查的路径

        Returns:
            bool: 路径是否有效（不包含非法字符）

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
            return PathHelper._is_valid_path_unix(path_str)

        except (TypeError, ValueError):
            return False

    @staticmethod
    def _is_valid_path_windows(path_str: str) -> bool:
        """检查 Windows 系统下的路径有效性（内部方法）

        Args:
            path_str: 路径字符串

        Returns:
            bool: 路径是否有效（不包含非法字符）

        Example:
            >>> # 内部使用，由 is_valid_path 方法调用
            >>> # 无需手动调用
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
        """检查 Unix/Linux/macOS 系统下的路径有效性（内部方法）

        Args:
            path_str: 路径字符串

        Returns:
            bool: 路径是否有效（不包含非法字符）

        Example:
            >>> # 内部使用，由 is_valid_path 方法调用
            >>> # 无需手动调用
        """
        # Unix 系统的非法字符
        # 虽然技术上某些字符是允许的，但为了跨平台一致性和安全性，我们限制一些特殊字符
        illegal_chars = [
            "\0",  # NUL字符
            "?",  # 通配符
            "*",  # 通配符
        ]
        return not any(char in path_str for char in illegal_chars)

    @staticmethod
    def get_absolute_path(
        relative_path: str | Path, base_dir: str | Path | None = None
    ) -> Path:
        """获取相对路径的绝对路径

        Args:
            relative_path: 相对路径
            base_dir: 基准目录，如果为None则使用当前工作目录

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
            raise OSError(f"无法解析路径 '{relative_path}': {str(e)}") from e

    @staticmethod
    def get_user_config_dir(app_name: str) -> Path:
        """获取用户配置目录路径

        Args:
            app_name: 应用名称

        Returns:
            Path: 配置目录的Path对象

        Raises:
            ValueError: 当应用名称为空或不是字符串时
            OSError: 当无法创建目录时（仅在回退方案也失败时）

        Example:
            >>> config_dir = PathHelper.get_user_config_dir("my_app")
            >>> print(config_dir)
            Windows: C:/Users/username/AppData/Roaming/my_app
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

        except OSError:
            # 回退到当前目录（隐藏目录）
            fallback_dir = Path.cwd() / f".{app_name}"
            try:
                fallback_dir.mkdir(exist_ok=True)
                return fallback_dir
            except OSError as exc:
                raise OSError(f"无法创建配置目录: {str(exc)}") from exc

    @staticmethod
    def get_user_home_dir() -> Path:
        """获取用户主目录路径

        Returns:
            Path: 用户主目录的Path对象

        Example:
            >>> home_dir = PathHelper.get_user_home_dir()
            >>> print(home_dir)
            Windows: C:/Users/username
            Unix: /home/username
        """
        return Path.home()

    @staticmethod
    def normalize_path(path: str | Path) -> Path:
        """规范化路径字符串

        Args:
            path: 需要规范化的路径字符串或Path对象

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
            raise OSError(f"无法解析路径 '{path}': {str(e)}") from e
        except (TypeError, ValueError) as e:
            raise ValueError(f"无效的路径格式 '{path}': {str(e)}") from e

    @staticmethod
    def rename_if_exists(file_path: str | Path) -> Path:
        """如果文件存在，自动重命名文件（Windows风格）

        采用Windows系统的重命名逻辑，当文件存在时，在文件名后添加 (1), (2) 等后缀。

        Args:
            file_path: 文件路径

        Returns:
            Path: 重命名后的文件路径，如果文件不存在则返回原路径

        Example:
            >>> # 重命名已存在的文件
            >>> new_path = PathHelper.rename_if_exists("/path/to/file.txt")
            >>> print(new_path)  # /path/to/file (1).txt
        """
        file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path

        # 检查文件是否存在
        if not file_path_obj.exists():
            return file_path_obj

        # 分离文件名和扩展名
        stem = file_path_obj.stem
        suffix = file_path_obj.suffix
        parent = file_path_obj.parent

        # 生成新文件名，直到找到不存在的文件名
        counter = 1
        while True:
            new_name = f"{stem} ({counter}){suffix}"
            new_path = parent / new_name
            if not new_path.exists():
                # 重命名文件
                file_path_obj.rename(new_path)
                return new_path
            counter += 1

    @staticmethod
    def safe_join(base_path: str | Path, *paths: str) -> Path:
        """安全地连接路径，防止路径遍历攻击

        Args:
            base_path: 基础路径
            *paths: 要连接的路径部分

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
        except ValueError as exc:
            raise ValueError("路径遍历检测到安全违规") from exc

        return result_path

    @staticmethod
    def set_secure_file_permissions(file_path: Path | str) -> bool:
        """设置文件的安全权限（最小权限原则）

        Args:
            file_path: 需要设置权限的文件路径

        Returns:
            bool: 权限设置是否成功

        Raises:
            ValueError: 当文件路径为空或无效时
            OSError: 当文件不存在或无法访问时

        Example:
            >>> # 设置配置文件权限
            >>> PathHelper.set_secure_file_permissions("/path/to/config.toml")
            True
            >>>
            >>> # 设置密钥文件权限
            >>> key_file = Path("/path/to/encryption.key")
            >>> success = PathHelper.set_secure_file_permissions(key_file)
            >>> print(f"权限设置{'成功' if success else '失败'}")
        """
        if not file_path:
            raise ValueError("文件路径不能为空")

        # 转换为Path对象
        file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path

        # 检查文件是否存在
        if not file_path_obj.exists():
            raise OSError(f"文件不存在: {file_path_obj}")

        system = platform.system().lower()

        if system == "windows":
            return PathHelper._set_windows_file_permissions(file_path_obj)
        return PathHelper._set_unix_file_permissions(file_path_obj)

    @staticmethod
    def _set_windows_file_permissions(file_path: Path) -> bool:
        """设置Windows系统文件权限（内部方法）

        Args:
            file_path: 文件路径

        Returns:
            bool: 权限设置是否成功

        Example:
            >>> # 内部使用，由 set_secure_file_permissions 方法调用
            >>> # 无需手动调用
        """
        try:
            username = getpass.getuser()

            # 使用icacls设置权限
            result = subprocess.run(
                [
                    "icacls",
                    str(file_path),
                    "/inheritance:r",
                    "/grant:r",
                    f"{username}:(R,W)",  # 仅读写权限
                    "/remove",
                    "*S-1-1-0",  # 移除Everyone组
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            if result.returncode != 0:
                return False

            return True

        except (OSError, subprocess.SubprocessError):
            return False

    @staticmethod
    def _set_unix_file_permissions(file_path: Path) -> bool:
        """设置Unix/Linux系统文件权限（内部方法）

        Args:
            file_path: 文件路径

        Returns:
            bool: 权限设置是否成功

        Example:
            >>> # 内部使用，由 set_secure_file_permissions 方法调用
            >>> # 无需手动调用
        """
        try:
            # 设置权限为600：仅所有者可读写
            file_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            return True

        except OSError:
            return False
