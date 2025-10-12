"""
数据库驱动模块包

提供统一的数据库驱动接口，采用抽象工厂模式和策略模式支持多种数据库类型。
该模块封装了数据库连接的底层实现，为上层的连接管理器提供一致的API接口。

主要特性：
- 统一的驱动接口设计，支持多种数据库后端
- 基于SQLAlchemy的通用驱动实现
- 支持连接池管理、事务处理和错误重试
- 提供数据库特定的参数验证和优化

支持的数据库类型：
- Oracle (oracledb)
- PostgreSQL (psycopg)
- MySQL (PyMySQL)
- SQL Server (pymssql)
- SQLite (内置支持)

驱动架构：
- SQLAlchemyDriver: 基于SQLAlchemy的通用数据库驱动
  - 支持所有SQLAlchemy兼容的数据库后端
  - 提供连接池、事务管理和查询执行功能
  - 自动处理数据库方言差异和参数绑定

版本信息：
- 模块版本: 1.0.0
- Python要求: >= 3.8
- 核心依赖: SQLAlchemy >= 1.4
- 可选依赖: 各数据库的Python客户端库

使用示例：
    >>> from db_connector.drivers import SQLAlchemyDriver
    >>>
    >>> # 创建数据库驱动实例
    >>> config = {
    ...     "type": "mysql",
    ...     "host": "localhost",
    ...     "port": 3306,
    ...     "username": "user",
    ...     "password": "pass",
    ...     "database": "test_db"
    ... }
    >>> driver = SQLAlchemyDriver(config)
    >>>
    >>> # 建立数据库连接
    >>> driver.connect()
    >>>
    >>> # 执行查询
    >>> results = driver.execute_query("SELECT * FROM users")
    >>>
    >>> # 关闭连接
    >>> driver.disconnect()

扩展说明：
- 未来计划支持更多数据库驱动（如异步驱动、NoSQL驱动等）
- 支持自定义驱动插件的动态加载
- 提供驱动性能监控和诊断功能
"""

from .sqlalchemy_driver import SQLAlchemyDriver

# 公共API导出列表

# 按驱动类型分组，便于用户理解和导入
__all__ = [
    # ==================== SQLAlchemy驱动 ====================
    "SQLAlchemyDriver",
]

# 驱动类型常量定义

# 这些常量用于标识支持的数据库类型
DRIVER_TYPE_SQLALCHEMY = "sqlalchemy"
SUPPORTED_DRIVER_TYPES = {DRIVER_TYPE_SQLALCHEMY}

# 数据库类型到驱动类型的映射

# 用于自动选择合适的驱动实现
DATABASE_TO_DRIVER_MAPPING = {
    "oracle": DRIVER_TYPE_SQLALCHEMY,
    "postgresql": DRIVER_TYPE_SQLALCHEMY,
    "mysql": DRIVER_TYPE_SQLALCHEMY,
    "mssql": DRIVER_TYPE_SQLALCHEMY,
    "sqlite": DRIVER_TYPE_SQLALCHEMY,
}

# 向后兼容性信息

# 这些常量用于标识模块的兼容性要求
REQUIRED_PYTHON_VERSION = (3, 8)
REQUIRED_SQLALCHEMY_VERSION = (1, 4)

# 模块级别的类型别名

# 这些别名可以简化用户的导入语句
SQLAlchemyDriverType = SQLAlchemyDriver
