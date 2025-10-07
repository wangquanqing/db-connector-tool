"""
数据库驱动模块包

提供统一的数据库驱动接口，支持多种数据库类型。
当前包含SQLAlchemy驱动实现，支持Oracle、PostgreSQL、MySQL、SQL Server和SQLite。
"""

from .sqlalchemy_driver import SQLAlchemyDriver

__all__ = [
    "SQLAlchemyDriver",
]

# 包版本信息
__version__ = "1.0.0"
__author__ = "wangquanqing <wangquanqing1636@sina.com>"
__description__ = "数据库连接器驱动模块包"
