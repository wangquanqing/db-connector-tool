"""
数据库连接器工具模块

提供日志记录、 路径处理等通用工具功能。
包含日志配置、 路径操作等实用工具。
"""

# 统一在顶部导入所需模块和函数
from .logging_utils import (
    DEFAULT_LOG_FORMAT,
    LogManager,
    get_logger,
    set_log_level,
    setup_logging,
)
from .path_utils import PathHelper

__all__ = [
    # 日志相关
    "setup_logging",
    "get_logger",
    "set_log_level",
    "LogManager",
    "DEFAULT_LOG_FORMAT",
    # 路径相关
    "PathHelper",
]

# 包版本信息
__version__ = "1.0.0"
__author__ = "wangquanqing <wangquanqing1636@sina.com>"
__description__ = "数据库连接器工具模块"
