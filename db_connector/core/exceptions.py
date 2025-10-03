"""
自定义异常类
"""


class DBConnectorError(Exception):
    """基础异常类"""

    pass


class ConfigError(DBConnectorError):
    """配置相关异常"""

    pass


class CryptoError(DBConnectorError):
    """加密解密异常"""

    pass


class DatabaseError(DBConnectorError):
    """数据库操作异常"""

    pass


class ConnectionError(DatabaseError):
    """数据库连接异常"""

    pass


class DriverError(DatabaseError):
    """数据库驱动异常"""

    pass


class QueryError(DatabaseError):
    """查询执行异常"""

    pass
