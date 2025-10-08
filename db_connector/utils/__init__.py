"""
数据库连接器工具模块

提供数据库连接器项目所需的通用工具功能，采用模块化设计支持可扩展的工具集合。
该模块封装了日志记录、路径处理、配置验证等常用功能，为整个项目提供一致的工具接口。

主要功能模块：
- 日志管理：提供统一的日志配置、记录和管理功能
- 路径处理：提供跨平台的路径操作和文件系统工具
- 工具函数：提供各种通用的辅助函数和工具类

工具特性：
- 统一的日志格式和级别管理
- 跨平台的路径操作支持
- 线程安全的工具函数实现
- 可配置的工具行为选项

日志管理功能：
- LogManager: 日志管理器类，支持多日志器配置和动态级别调整
- get_logger(): 获取模块级别的日志记录器
- set_log_level(): 动态设置日志级别
- setup_logging(): 初始化日志系统配置

路径处理功能：
- PathHelper: 路径助手类，提供跨平台的路径操作和文件系统工具
  - 支持相对路径和绝对路径的转换
  - 提供文件存在性检查和目录创建功能
  - 支持路径的安全验证和规范化

版本信息：
- 模块版本: 1.0.0
- Python要求: >= 3.8
- 核心依赖: 标准库 (logging, os, pathlib等)

使用示例：
    >>> from db_connector.utils import get_logger, setup_logging, PathHelper
    >>>
    >>> # 初始化日志系统
    >>> setup_logging(level="INFO", log_file="db_connector.log")
    >>>
    >>> # 获取模块日志器
    >>> logger = get_logger(__name__)
    >>> logger.info("数据库连接器初始化完成")
    >>>
    >>> # 使用路径工具
    >>> path_helper = PathHelper()
    >>> config_path = path_helper.get_config_path("my_app")
    >>>
    >>> # 检查配置文件是否存在
    >>> if path_helper.exists(config_path):
    ...     logger.info(f"配置文件存在: {config_path}")
    ... else:
    ...     logger.warning(f"配置文件不存在: {config_path}")

扩展说明：
- 未来计划支持更多工具功能（如性能监控、缓存管理、数据验证等）
- 支持自定义工具插件的动态加载
- 提供工具性能优化和诊断功能
"""

# 统一在顶部导入所需模块和函数
from .logging_utils import LogManager, get_logger, set_log_level, setup_logging
from .path_utils import PathHelper

# 公共API导出列表

# 按功能模块分组，便于用户理解和导入
__all__ = [
    # ==================== 日志管理模块 ====================
    "setup_logging",  # 初始化日志系统配置
    "get_logger",  # 获取模块级别的日志记录器
    "set_log_level",  # 动态设置日志级别
    "LogManager",  # 日志管理器类
    # ==================== 路径处理模块 ====================
    "PathHelper",  # 路径助手类
]

# 工具类型常量定义

# 这些常量用于标识工具的功能类型
TOOL_TYPE_LOGGING = "logging"
TOOL_TYPE_PATH = "path"
TOOL_TYPE_VALIDATION = "validation"
SUPPORTED_TOOL_TYPES = {TOOL_TYPE_LOGGING, TOOL_TYPE_PATH, TOOL_TYPE_VALIDATION}

# 日志级别常量（与标准logging模块保持一致）
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"
LOG_LEVEL_CRITICAL = "CRITICAL"

# 路径类型常量
PATH_TYPE_CONFIG = "config"
PATH_TYPE_LOG = "log"
PATH_TYPE_CACHE = "cache"
PATH_TYPE_TEMP = "temp"

# 模块级别的类型别名

# 这些别名可以简化用户的导入语句
LogManagerType = LogManager
PathHelperType = PathHelper

# 向后兼容性信息

# 这些常量用于标识模块的兼容性要求
REQUIRED_PYTHON_VERSION = (3, 8)
SUPPORTED_PLATFORMS = {"windows", "linux", "darwin"}

# 工具配置常量

# 这些常量用于定义工具的默认配置
DEFAULT_LOG_LEVEL = LOG_LEVEL_INFO
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
