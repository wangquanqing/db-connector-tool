# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""数据库连接管理模块 (DB Connector)"""

from .batch_manager import BatchDatabaseManager, cleanup_temp_configs, generate_ip_range

# 核心模块导入
from .core.config import ConfigManager
from .core.connections import DatabaseManager
from .core.crypto import CryptoManager
from .core.key_manager import KeyManager
from .drivers.sqlalchemy_driver import SQLAlchemyDriver

# 公共API导出列表
__all__ = [
    "cleanup_temp_configs",
    "generate_ip_range",
    "BatchDatabaseManager",
    "ConfigManager",
    "CryptoManager",
    "DatabaseManager",
    "KeyManager",
    "SQLAlchemyDriver",
]
