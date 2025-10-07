"""
数据库连接器核心模块

提供数据库连接管理的核心功能， 包括配置管理、连接管理、异常处理和加密功能。
"""

from .config import ConfigManager
from .connections import DatabaseManager
from .crypto import CryptoManager
from .exceptions import (
    ConfigError,
    ConnectionError,
    CryptoError,
    DatabaseError,
    DBConnectorError,
    DriverError,
    FileSystemError,
    QueryError,
    TimeoutError,
    ValidationError,
)

__all__ = [
    # 配置管理
    "ConfigManager",
    # 连接管理
    "DatabaseManager",
    # 加密管理
    "CryptoManager",
    # 异常类
    "DBConnectorError",
    "ConfigError",
    "CryptoError",
    "DatabaseError",
    "ConnectionError",
    "DriverError",
    "FileSystemError",
    "QueryError",
    "TimeoutError",
    "ValidationError",
]

# 包元数据
__version__ = "1.0.0"
__author__ = "wangquanqing wangquanqing1636@sina.com"
__description__ = "数据库连接器核心模块"
