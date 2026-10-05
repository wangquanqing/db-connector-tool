# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""数据库连接管理模块 (DB Connector)

模块根导出核心管理类、异常体系、验证器与工具函数，
使 ``from db_connector_tool import ConfigError`` 这类文档用法可直接成立。
"""

from .batch_manager import BatchDatabaseManager, cleanup_temp_configs, generate_ip_range

# 核心模块导入
from .core.config import ConfigManager
from .core.connections import DatabaseManager
from .core.crypto import CryptoManager
from .core.exceptions import (
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
from .core.key_manager import KeyManager
from .core.validators import ConfigValidator, GenericValidator, PasswordValidator
from .drivers.sqlalchemy_driver import SQLAlchemyDriver
from .utils.logging_utils import LogManager, get_logger, set_log_level, setup_logging
from .utils.path_utils import PathHelper

# 公共API导出列表
__all__ = [
    # 批量管理
    "BatchDatabaseManager",
    "cleanup_temp_configs",
    "generate_ip_range",
    # 核心管理类
    "ConfigManager",
    "CryptoManager",
    "DatabaseManager",
    "KeyManager",
    "SQLAlchemyDriver",
    # 异常体系
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
    # 验证器
    "ConfigValidator",
    "GenericValidator",
    "PasswordValidator",
    # 工具
    "LogManager",
    "PathHelper",
    "get_logger",
    "set_log_level",
    "setup_logging",
]
