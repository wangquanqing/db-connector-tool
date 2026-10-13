"""数据库连接器核心模块 (Core)"""

from .config import ConfigManager
from .config_security import ConfigSecurityManager
from .connection_pool import ConnectionPoolManager
from .connections import DatabaseManager
from .crypto import CryptoManager
from .exceptions import (
    ConfigError,
    CryptoError,
    DatabaseError,
    DBConnectionError,
    DBConnectorError,
    DBTimeoutError,
    DriverError,
    FileSystemError,
    QueryError,
    ValidationError,
)
from .key_manager import KeyManager
from .result import ColumnInfo, QueryChunk, QueryResult

# 公共API导出列表
__all__ = [
    "ConfigManager",
    "ConfigSecurityManager",
    "ConnectionPoolManager",
    "CryptoManager",
    "DatabaseManager",
    "KeyManager",
    "ColumnInfo",
    "QueryChunk",
    "QueryResult",
    "ConfigError",
    "CryptoError",
    "DatabaseError",
    "DBConnectionError",
    "DBConnectorError",
    "DBTimeoutError",
    "DriverError",
    "FileSystemError",
    "QueryError",
    "ValidationError",
]
