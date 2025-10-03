"""
DB Connector - 跨平台数据库连接管理模块
支持 Oracle, PostgreSQL, SQL Server, MySQL, SQLite
"""

__version__ = "1.0.0"
__author__ = "Your Name"
__license__ = "MIT"

from .core.config_manager import ConfigManager
from .core.database import DatabaseManager
from .core.exceptions import (
    ConfigError,
    ConnectionError,
    CryptoError,
    DatabaseError,
    DBConnectorError,
)

__all__ = [
    "ConfigManager",
    "DatabaseManager",
    "DBConnectorError",
    "ConfigError",
    "CryptoError",
    "DatabaseError",
    "ConnectionError",
]
