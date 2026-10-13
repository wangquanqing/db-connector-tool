# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""数据库连接管理模块 (DB Connector)

模块根导出核心管理类、异常体系、验证器与工具函数，
使 ``from db_connector_tool import ConfigError`` 这类文档用法可直接成立。
"""

from .batch_manager import BatchDatabaseManager, cleanup_temp_configs, generate_ip_range

# 核心模块导入
from .__about__ import __version__
from .core.config import ConfigManager
from .core.config_security import ConfigSecurityManager
from .core.connection_pool import ConnectionPoolManager
from .core.connections import DatabaseManager
from .core.crypto import CryptoManager
# pylint: disable=duplicate-code
# 包根的再导出必然与 core/__init__ 的导出清单重合，这是有意的 API 汇总，
# 并非可以抽公共函数消除的重复代码。
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
from .core.result import ColumnInfo, QueryChunk, QueryResult
from .core.validators import ConfigValidator, GenericValidator, PasswordValidator
from .drivers.sqlalchemy_driver import SQLAlchemyDriver
from .utils.logging_utils import LogManager, get_logger, set_log_level, setup_logging
from .utils.path_utils import PathHelper

# 公共API导出列表
__all__ = [
    # 元数据
    "__version__",
    # 批量管理
    "BatchDatabaseManager",
    "cleanup_temp_configs",
    "generate_ip_range",
    # 核心管理类
    "ConfigManager",
    "ConfigSecurityManager",
    "ConnectionPoolManager",
    "CryptoManager",
    "DatabaseManager",
    "KeyManager",
    "SQLAlchemyDriver",
    # 结果载体（保真的查询结果：列身份与显示名分离）
    "ColumnInfo",
    "QueryChunk",
    "QueryResult",
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
