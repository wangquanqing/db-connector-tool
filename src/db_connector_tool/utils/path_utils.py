"""路径处理工具模块 (Path Utilities)

提供跨平台的路径操作工具，包括安全连接、文件权限设置、
扩展名检查和用户配置目录管理。

Example:
>>> from db_connector_tool import PathHelper
>>>
>>> config_dir = PathHelper.get_user_config_dir("my_app")
>>> print(config_dir)
Windows: C:/Users/username/AppData/Roaming/my_app
macOS: /Users/username/Library/Application Support/my_app
Linux: /home/username/.config/my_app
>>>
>>> safe_path = PathHelper.safe_join("/base", "subdir", "file.txt")
>>> print(safe_path)
/base/subdir/file.txt
>>>
>>> is_valid = PathHelper.is_valid_path("/valid/path")
>>> print(is_valid)
True
"""

import getpass
import os
import platform
import stat
import subprocess
from pathlib import Path

_ERROR_MSG_PATH_EMPTY = "路径不能为空"
_ERROR_MSG_FILE_PATH_EMPTY = "文件路径不能为空"
_ERROR_MSG_RELATIVE_PATH_EMPTY = "相对路径不能为空"
_ERROR_MSG_BASE_PATH_EMPTY = "基础路径不能为空"
_ERROR_MSG_APP_NAME_EMPTY = "应用名称不能为空且必须是字符串"
_ERROR_MSG_EXTENSIONS_EMPTY = "扩展名列表不能为空"
_ERROR_MSG_FILE_TYPE_EMPTY = "文件类型不能为空"


class PathHelper:
    """路径辅助类 (Path Helper)

    提供所有路径相关操作的静态方法，包括目录创建、路径验证、
    安全连接、权限设置和扩展名检查。

    Example:
    >>> config_dir = PathHelper.get_user_config_dir("my_app")
    >>> safe_path = PathHelper.safe_join("/base", "subdir", "file.txt")
    >>> is_valid = PathHelper.is_valid_path("/valid/path")
    """

    @staticmethod
    def ensure_dir_exists(dir_path: str | Path) -> bool:
        """确保目录存在，不存在则递归创建

        Args:
            dir_path: 目标目录路径

        Returns:
            bool: 目录是否已存在或成功创建

        Raises:
            OSError: 目录创建失败（权限不足等）

        Example:
        >>> PathHelper.ensure_dir_exists("/path/to/dir")
        True
        """
        if not dir_path:
            return False

        try:
            dir_path_obj = Path(dir_path) if isinstance(dir_path, str) else dir_path

            if dir_path_obj.exists():
                return dir_path_obj.is_dir()

            dir_path_obj.mkdir(parents=True, exist_ok=True)
            return True

        except OSError as error:
            raise OSError(f"无法创建目录 '{dir_path}': {error}") from error
        except (TypeError, ValueError):
            return False

    @staticmethod
    def is_valid_path(path: str | Path) -> bool:
        """检查路径是否有效（不包含非法字符）

        Args:
            path: 需要检查的路径

        Returns:
            bool: 路径是否有效

        Example:
        >>> PathHelper.is_valid_path("/valid/path/file.txt")
        True
        >>> PathHelper.is_valid_path("/invalid/path/file?.txt")
        False
        """
        if not path:
            return False

        try:
            path_str = str(path)

            if not path_str.strip():
                return False

            system = platform.system().lower()
            if system == "windows":
                return PathHelper._is_valid_path_windows(path_str)
            return PathHelper._is_valid_path_unix(path_str)

        except (TypeError, ValueError):
            return False

    @staticmethod
    def _is_valid_path_windows(path_str: str) -> bool:
        """Windows 系统路径有效性检查

        Args:
            path_str: 路径字符串

        Returns:
            bool: 路径是否有效
        """
        if ":" in path_str and path_str.index(":") == 1 and path_str[0].isalpha():
            check_str = path_str[2:] if len(path_str) > 2 else ""
        else:
            check_str = path_str

        illegal_chars = [
            "<",
            ">",
            '"',
            "|",
            "?",
            "*",
            ":",
            "\0",
            "\n",
            "\r",
            "\t",
            "\b",
            "\f",
        ]
        return not any(char in check_str for char in illegal_chars)

    @staticmethod
    def _is_valid_path_unix(path_str: str) -> bool:
        """Unix/Linux/macOS 系统路径有效性检查

        Args:
            path_str: 路径字符串

        Returns:
            bool: 路径是否有效
        """
        illegal_chars = ["\0", "?", "*"]
        return not any(char in path_str for char in illegal_chars)

    @staticmethod
    def get_absolute_path(
        relative_path: str | Path, base_dir: str | Path | None = None
    ) -> Path:
        """获取相对路径的绝对路径

        Args:
            relative_path: 相对路径
            base_dir: 基准目录，None 则使用当前工作目录

        Returns:
            Path: 绝对路径

        Raises:
            ValueError: 相对路径为空
            OSError: 路径解析失败

        Example:
        >>> abs_path = PathHelper.get_absolute_path("config/app.conf")
        >>> print(isinstance(abs_path, Path))
        True
        """
        if not relative_path:
            raise ValueError(_ERROR_MSG_RELATIVE_PATH_EMPTY)

        base_path = Path(base_dir) if base_dir else Path.cwd()
        relative_path_obj = (
            Path(relative_path) if isinstance(relative_path, str) else relative_path
        )

        try:
            return (base_path / relative_path_obj).resolve()
        except OSError as error:
            raise OSError(f"无法解析路径 '{relative_path}': {error}") from error

    @staticmethod
    def get_user_config_dir(app_name: str) -> Path:
        """获取用户配置目录路径

        Args:
            app_name: 应用名称

        Returns:
            Path: 配置目录路径

        Raises:
            ValueError: 应用名为空或非字符串
            OSError: 无法创建目录

        Example:
        >>> config_dir = PathHelper.get_user_config_dir("my_app")
        >>> print(config_dir)
        Windows: C:/Users/username/AppData/Roaming/my_app
        macOS: /Users/username/Library/Application Support/my_app
        Linux: /home/username/.config/my_app
        """
        if not app_name or not isinstance(app_name, str):
            raise ValueError(_ERROR_MSG_APP_NAME_EMPTY)

        system = platform.system().lower()

        try:
            if system == "windows":
                base_dir = Path(os.environ.get("APPDATA", Path.home()))
            elif system == "darwin":
                base_dir = Path.home() / "Library" / "Application Support"
            else:
                base_dir = Path.home() / ".config"

            config_dir = base_dir / app_name
            config_dir.mkdir(parents=True, exist_ok=True)

            return config_dir

        except OSError:
            fallback_dir = Path.cwd() / f".{app_name}"
            try:
                fallback_dir.mkdir(exist_ok=True)
                return fallback_dir
            except OSError as error:
                raise OSError(f"无法创建配置目录: {error}") from error

    @staticmethod
    def get_user_home_dir() -> Path:
        """获取用户主目录路径

        Returns:
            Path: 主目录路径

        Example:
        >>> home_dir = PathHelper.get_user_home_dir()
        >>> home_dir == Path.home()
        True
        """
        return Path.home()

    @staticmethod
    def normalize_path(path: str | Path) -> Path:
        """规范化路径

        Args:
            path: 路径字符串或 Path 对象

        Returns:
            Path: 规范化后的绝对路径

        Raises:
            ValueError: 路径为空或无效
            OSError: 路径解析失败

        Example:
        >>> normalized = PathHelper.normalize_path("~/documents")
        >>> print("~" not in str(normalized))
        True
        """
        if not path:
            raise ValueError(_ERROR_MSG_PATH_EMPTY)

        try:
            path_obj = Path(path) if isinstance(path, str) else path
            return path_obj.expanduser().resolve()
        except OSError as error:
            raise OSError(f"无法解析路径 '{path}': {error}") from error
        except (TypeError, ValueError) as error:
            raise ValueError(f"无效的路径格式 '{path}': {error}") from error

    @staticmethod
    def rename_if_exists(file_path: str | Path) -> Path:
        """文件存在时自动重命名

        采用 Windows 风格的重命名逻辑，在文件名后添加 (1), (2) 等后缀。

        Args:
            file_path: 文件路径

        Returns:
            Path: 重命名后的文件路径（文件不存在则返回原路径）

        Example:
        >>> new_path = PathHelper.rename_if_exists("/path/to/file.txt")
        >>> print(new_path)
        """
        file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path

        if not file_path_obj.exists():
            return file_path_obj

        stem = file_path_obj.stem
        suffix = file_path_obj.suffix
        parent = file_path_obj.parent

        counter = 1
        while True:
            new_name = f"{stem} ({counter}){suffix}"
            new_path = parent / new_name
            if not new_path.exists():
                file_path_obj.rename(new_path)
                return new_path
            counter += 1

    @staticmethod
    def safe_join(base_path: str | Path, *paths: str) -> Path:
        """安全连接路径，防止路径遍历攻击

        Args:
            base_path: 基础路径
            *paths: 要连接的路径部分

        Returns:
            Path: 连接后的安全路径

        Raises:
            ValueError: 基础路径为空、路径遍历攻击或非法字符

        Example:
        >>> safe_path = PathHelper.safe_join("/safe/base", "subdir", "file.txt")
        >>> print(safe_path)
        /safe/base/subdir/file.txt
        """
        if not base_path:
            raise ValueError(_ERROR_MSG_BASE_PATH_EMPTY)

        base_path_obj = Path(base_path) if isinstance(base_path, str) else base_path
        base_path_resolved = base_path_obj.resolve()
        result_path = base_path_resolved

        for path_part in paths:
            if not path_part or path_part == ".":
                continue

            if path_part == "..":
                raise ValueError("路径遍历不被允许")

            if not PathHelper.is_valid_path(path_part):
                raise ValueError(f"路径部分包含非法字符: {path_part}")

            if "/" in path_part or "\\" in path_part:
                raise ValueError(f"路径部分包含路径分隔符: {path_part}")

            result_path = result_path / path_part

        try:
            resolved_result = result_path.resolve()
            if not resolved_result.is_relative_to(base_path_resolved):
                raise ValueError("路径遍历检测到安全违规")
        except ValueError as error:
            raise ValueError("路径遍历检测到安全违规") from error

        return result_path

    @staticmethod
    def get_file_extension(file_path: str | Path, include_dot: bool = True) -> str:
        """获取文件的扩展名

        Args:
            file_path: 文件路径
            include_dot: 是否包含点号，默认 True

        Returns:
            str: 文件扩展名（空字符串表示无扩展名）

        Raises:
            ValueError: 文件路径为空

        Example:
        >>> PathHelper.get_file_extension("/path/to/file.txt")
        '.txt'
        >>> PathHelper.get_file_extension("/path/to/file.txt", False)
        'txt'
        """
        if not file_path:
            raise ValueError(_ERROR_MSG_FILE_PATH_EMPTY)

        file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path
        suffix = file_path_obj.suffix

        if not include_dot and suffix.startswith("."):
            return suffix[1:]

        return suffix

    @staticmethod
    def has_file_extension(file_path: str | Path, extensions: str | list[str]) -> bool:
        """检查文件扩展名是否匹配

        Args:
            file_path: 文件路径
            extensions: 单个扩展名或扩展名列表（不区分大小写）

        Returns:
            bool: 是否匹配

        Raises:
            ValueError: 文件路径或扩展名为空

        Example:
        >>> PathHelper.has_file_extension("file.txt", "txt")
        True
        >>> PathHelper.has_file_extension("photo.jpg", ["jpg", "png", "gif"])
        True
        """
        if not file_path:
            raise ValueError(_ERROR_MSG_FILE_PATH_EMPTY)
        if not extensions:
            raise ValueError(_ERROR_MSG_EXTENSIONS_EMPTY)

        if isinstance(extensions, str):
            extensions_to_check = [extensions]
        else:
            extensions_to_check = extensions

        file_extension = PathHelper.get_file_extension(
            file_path, include_dot=False
        ).lower()

        for ext in extensions_to_check:
            clean_extension = ext.lstrip(".").lower()
            if file_extension == clean_extension:
                return True

        return False

    @staticmethod
    def is_file_type(file_path: str | Path, file_type: str) -> bool:
        """检查文件是否属于指定类型

        Args:
            file_path: 文件路径
            file_type: 文件类型（image, text, archive, audio, video, document, code, executable）

        Returns:
            bool: 是否匹配

        Raises:
            ValueError: 文件路径或文件类型为空

        Example:
        >>> PathHelper.is_file_type("photo.jpg", "image")
        True
        >>> PathHelper.is_file_type("document.txt", "text")
        True
        """
        if not file_path:
            raise ValueError(_ERROR_MSG_FILE_PATH_EMPTY)
        if not file_type:
            raise ValueError(_ERROR_MSG_FILE_TYPE_EMPTY)

        file_type_extensions = {
            "image": ["jpg", "jpeg", "png", "gif", "bmp", "webp", "svg", "ico", "tiff"],
            "text": [
                "txt",
                "md",
                "rst",
                "log",
                "ini",
                "conf",
                "cfg",
                "yml",
                "yaml",
                "xml",
                "json",
                "csv",
            ],
            "archive": ["zip", "tar", "gz", "bz2", "7z", "rar", "xz"],
            "audio": ["mp3", "wav", "flac", "aac", "ogg", "wma", "m4a"],
            "video": ["mp4", "avi", "mkv", "mov", "wmv", "flv", "webm", "m4v"],
            "document": [
                "pdf",
                "doc",
                "docx",
                "ppt",
                "pptx",
                "xls",
                "xlsx",
                "odt",
                "ods",
            ],
            "code": [
                "py",
                "js",
                "ts",
                "java",
                "cpp",
                "c",
                "h",
                "html",
                "css",
                "php",
                "rb",
                "go",
                "rs",
                "sql",
            ],
            "executable": ["exe", "dll", "so", "dylib", "bin", "app", "msi"],
        }

        if file_type not in file_type_extensions:
            supported_types = ", ".join(file_type_extensions.keys())
            raise ValueError(
                f"不支持的文件类型 '{file_type}'，支持的类型: {supported_types}"
            )

        return PathHelper.has_file_extension(file_path, file_type_extensions[file_type])

    @staticmethod
    def set_secure_file_permissions(file_path: Path | str) -> bool:
        """设置文件的安全权限（最小权限原则）

        Windows 下通过 icacls 限制为当前用户读写；
        Unix 下设置为 600（仅所有者读写）。

        Args:
            file_path: 文件路径

        Returns:
            bool: 权限设置是否成功

        Raises:
            ValueError: 文件路径为空
            OSError: 文件不存在或无法访问

        Example:
        >>> PathHelper.set_secure_file_permissions("/path/to/config.toml")
        True
        """
        if not file_path:
            raise ValueError(_ERROR_MSG_FILE_PATH_EMPTY)

        file_path_obj = Path(file_path) if isinstance(file_path, str) else file_path

        if not file_path_obj.exists():
            raise OSError(f"文件不存在: {file_path_obj}")

        system = platform.system().lower()

        if system == "windows":
            return PathHelper._set_windows_file_permissions(file_path_obj)
        return PathHelper._set_unix_file_permissions(file_path_obj)

    @staticmethod
    def _set_windows_file_permissions(file_path: Path) -> bool:
        """Windows 系统文件权限设置

        Args:
            file_path: 文件路径

        Returns:
            bool: 是否成功
        """
        try:
            username = getpass.getuser()

            result = subprocess.run(
                [
                    "icacls",
                    str(file_path),
                    "/inheritance:r",
                    "/grant:r",
                    f"{username}:(R,W)",
                    "/remove",
                    "*S-1-1-0",
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            return result.returncode == 0

        except (OSError, subprocess.SubprocessError):
            return False

    @staticmethod
    def _set_unix_file_permissions(file_path: Path) -> bool:
        """Unix/Linux 系统文件权限设置

        Args:
            file_path: 文件路径

        Returns:
            bool: 是否成功
        """
        try:
            file_path.chmod(stat.S_IRUSR | stat.S_IWUSR)
            return True

        except OSError:
            return False
