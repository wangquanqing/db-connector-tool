# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
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
