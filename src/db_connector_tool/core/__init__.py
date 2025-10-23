"""
数据库连接器核心模块

提供数据库连接管理的核心功能，采用模块化设计支持多种数据库类型。
该模块封装了配置管理、连接管理、异常处理和加密功能，提供统一的API接口。

主要功能模块：
- ConfigManager: 配置管理，支持配置的持久化、加密和验证
- DatabaseManager: 连接管理，支持多种数据库的连接池管理
- CryptoManager: 加密管理，提供敏感数据的加密解密功能
- 异常处理: 统一的异常体系，支持精确的错误分类和处理

支持的数据库类型：
- Oracle, PostgreSQL, MySQL, SQL Server, SQLite

版本信息：
- 模块版本: 1.0.0
- Python要求: >= 3.8
- 依赖: SQLAlchemy, cryptography

使用示例：
    >>> from db_connector.core import DatabaseManager, ConfigManager
    >>>
    >>> # 创建数据库管理器
    >>> db_manager = DatabaseManager("my_app")
    >>>
    >>> # 添加数据库连接
    >>> config = {
    ...     "type": "mysql",
    ...     "host": "localhost",
    ...     "port": 3306,
    ...     "username": "user",
    ...     "password": "pass",
    ...     "database": "test_db"
    ... }
    >>> db_manager.add_connection("mysql_db", config)
    >>>
    >>> # 执行查询
    >>> results = db_manager.execute_query("mysql_db", "SELECT * FROM users")
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

# 公共API导出列表

# 按功能模块分组，便于用户理解和导入
__all__ = [
    # ==================== 配置管理模块 ====================
    "ConfigManager",
    # ==================== 连接管理模块 ====================
    "DatabaseManager",
    # ==================== 加密管理模块 ====================
    "CryptoManager",
    # ==================== 异常处理体系 ====================
    # 基础异常类
    "DBConnectorError",
    # 配置相关异常
    "ConfigError",
    "ValidationError",
    # 连接相关异常
    "ConnectionError",
    "TimeoutError",
    # 数据库操作异常
    "DatabaseError",
    "QueryError",
    # 驱动相关异常
    "DriverError",
    # 文件系统异常
    "FileSystemError",
    # 加密相关异常
    "CryptoError",
]

# 模块级别的类型别名和便利导入

# 这些别名可以简化用户的导入语句
ConfigManagerType = ConfigManager
DatabaseManagerType = DatabaseManager
CryptoManagerType = CryptoManager

# 向后兼容性信息

# 这些常量用于标识模块的兼容性要求
REQUIRED_PYTHON_VERSION = (3, 8)
SUPPORTED_DATABASE_TYPES = {"oracle", "postgresql", "mysql", "mssql", "sqlite"}
