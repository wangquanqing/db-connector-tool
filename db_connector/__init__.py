"""
DB Connector - 跨平台数据库连接管理模块
======================================

一个功能强大的数据库连接管理工具，支持多种数据库类型。

主要特性:
- 支持 Oracle, PostgreSQL, SQL Server, MySQL, SQLite
- 统一的连接配置管理
- 安全的密码加密存储
- 命令行界面和API接口
- 跨平台兼容

使用示例:
    >>> from db_connector import DatabaseManager, ConfigManager
    >>> config = ConfigManager()
    >>> db_manager = DatabaseManager(config)
    >>> connection = db_manager.get_connection('oracle')
"""

__version__ = "0.1.0"
__author__ = "wangquanqing <wangquanqing1636@sina.com>"
__license__ = "MIT"

# 核心模块导入
from .core.config import ConfigManager
from .core.connections import DatabaseManager
from .core.exceptions import (
    ConfigError,
    ConnectionError,
    CryptoError,
    DatabaseError,
    DBConnectorError,
)

# CLI模块导入（可选，安装时可能不可用）
try:
    from .cli import DBConnectorCLI
    from .cli import main as cli_main
except ImportError:
    # 在安装过程中或缺少依赖时，CLI模块可能不可用
    DBConnectorCLI = None
    cli_main = None

# 公共API导出列表
__all__ = [
    # 核心管理器
    "ConfigManager",
    "DatabaseManager",
    # 异常类
    "DBConnectorError",
    "ConfigError",
    "CryptoError",
    "DatabaseError",
    "ConnectionError",
    # CLI相关（可选）
    "DBConnectorCLI",
    "cli_main",
]


def get_version() -> str:
    """
    获取当前模块版本号。

    Returns:
        str: 版本号字符串，格式为 'x.y.z'

    Example:
        >>> from db_connector import get_version
        >>> print(get_version())
        '0.0.5'
    """
    return __version__


def get_supported_databases() -> list[str]:
    """
    获取支持的数据库类型列表。

    Returns:
        list[str]: 支持的数据库类型名称列表

    Example:
        >>> from db_connector import get_supported_databases
        >>> print(get_supported_databases())
        ['oracle', 'postgresql', 'sqlserver', 'mysql', 'sqlite']
    """
    return ["oracle", "postgresql", "sqlserver", "mysql", "sqlite"]


def _check_dependencies() -> None:
    """
    检查必要的依赖是否可用。

    Raises:
        ImportError: 如果核心依赖不可用
    """
    try:
        import sqlalchemy
    except ImportError:
        raise ImportError("SQLAlchemy是必需的依赖，请安装: pip install sqlalchemy")


# 在导入时执行依赖检查
try:
    _check_dependencies()
except ImportError as e:
    # 记录警告但不阻止导入
    import warnings

    warnings.warn(f"依赖检查警告: {e}", ImportWarning)

# 包信息字典
__package_info__ = {
    "name": "db_connector",
    "version": __version__,
    "author": __author__,
    "license": __license__,
    "description": "跨平台数据库连接管理工具",
    "supported_databases": get_supported_databases(),
}
