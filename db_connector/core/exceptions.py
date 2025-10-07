"""
数据库连接器自定义异常模块

提供项目专用的异常类层次结构， 用于更精确地处理不同类型的错误。
异常类按照功能模块进行组织，便于错误分类和处理。
"""

from typing import Any, Dict, Optional


class DBConnectorError(Exception):
    """
    数据库连接器基础异常类

    所有自定义异常的基类，提供统一的异常处理接口。

    Attributes:
        message (str): 异常描述信息
        error_code (Optional[str]): 错误代码，用于错误分类
        details (Optional[Dict[str, Any]]): 详细的错误信息
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化基础异常

        Args:
            message: 异常描述信息
            error_code: 错误代码，用于错误分类和识别
            details: 详细的错误信息字典
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def __str__(self) -> str:
        """返回异常的字符串表示"""
        base_str = f"{self.__class__.__name__}: {self.message}"
        if self.error_code:
            base_str += f" (错误代码: {self.error_code})"
        return base_str

    def to_dict(self) -> Dict[str, Any]:
        """
        将异常信息转换为字典格式

        Returns:
            包含异常信息的字典
        """
        return {
            "error_type": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "details": self.details,
        }


class ConfigError(DBConnectorError):
    """
    配置相关异常

    处理配置文件读取、解析、验证等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        config_file: Optional[str] = None,
        config_section: Optional[str] = None,
        config_key: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化配置异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            config_file: 相关的配置文件路径
            config_section: 相关的配置节
            config_key: 相关的配置键
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details)
        self.config_file = config_file
        self.config_section = config_section
        self.config_key = config_key

        # 自动填充详细信息
        if config_file:
            self.details["config_file"] = config_file
        if config_section:
            self.details["config_section"] = config_section
        if config_key:
            self.details["config_key"] = config_key


class CryptoError(DBConnectorError):
    """
    加密解密相关异常

    处理密钥生成、数据加密、数据解密等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        operation: Optional[str] = None,
        algorithm: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化加密异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            operation: 加密操作类型（如：encrypt, decrypt, generate_key）
            algorithm: 使用的加密算法
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details)
        self.operation = operation
        self.algorithm = algorithm

        # 自动填充详细信息
        if operation:
            self.details["operation"] = operation
        if algorithm:
            self.details["algorithm"] = algorithm


class DatabaseError(DBConnectorError):
    """
    数据库操作基础异常

    处理所有数据库相关操作的通用错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        database_type: Optional[str] = None,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化数据库异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            database_type: 数据库类型（如：mysql, postgresql, oracle等）
            operation: 数据库操作类型（如：connect, query, execute等）
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details)
        self.database_type = database_type
        self.operation = operation

        # 自动填充详细信息
        if database_type:
            self.details["database_type"] = database_type
        if operation:
            self.details["operation"] = operation


class ConnectionError(DatabaseError):
    """
    数据库连接异常

    处理数据库连接建立、断开、测试等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        connection_name: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化连接异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            connection_name: 连接名称
            host: 数据库主机地址
            port: 数据库端口
            database: 数据库名称
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details=details)
        self.connection_name = connection_name
        self.host = host
        self.port = port
        self.database = database

        # 自动填充详细信息
        if connection_name:
            self.details["connection_name"] = connection_name
        if host:
            self.details["host"] = host
        if port:
            self.details["port"] = port
        if database:
            self.details["database"] = database


class DriverError(DatabaseError):
    """
    数据库驱动异常

    处理数据库驱动加载、初始化、操作等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        driver_name: Optional[str] = None,
        driver_version: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化驱动异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            driver_name: 驱动名称
            driver_version: 驱动版本
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details=details)
        self.driver_name = driver_name
        self.driver_version = driver_version

        # 自动填充详细信息
        if driver_name:
            self.details["driver_name"] = driver_name
        if driver_version:
            self.details["driver_version"] = driver_version


class QueryError(DatabaseError):
    """
    查询执行异常

    处理SQL查询解析、执行、结果处理等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        query: Optional[str] = None,
        query_type: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化查询异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            query: 执行的SQL查询语句
            query_type: 查询类型（如：SELECT, INSERT, UPDATE, DELETE等）
            parameters: 查询参数
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details=details)
        self.query = query
        self.query_type = query_type
        self.parameters = parameters

        # 自动填充详细信息（注意：不包含敏感参数信息）
        if query:
            self.details["query_preview"] = self._get_query_preview(query)
        if query_type:
            self.details["query_type"] = query_type
        if parameters:
            # 只记录参数键名，不记录参数值（避免泄露敏感信息）
            self.details["parameter_keys"] = list(parameters.keys())

    def _get_query_preview(self, query: str, max_length: int = 100) -> str:
        """
        获取查询语句的预览（避免泄露完整SQL）

        Args:
            query: 原始查询语句
            max_length: 最大预览长度

        Returns:
            查询预览字符串
        """
        if len(query) <= max_length:
            return query
        return query[:max_length] + "..."


class ValidationError(DBConnectorError):
    """
    数据验证异常

    处理配置验证、参数验证、数据格式验证等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        field_name: Optional[str] = None,
        expected_type: Optional[str] = None,
        actual_value: Optional[Any] = None,
        validation_rules: Optional[Dict[str, Any]] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化验证异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            field_name: 验证失败的字段名
            expected_type: 期望的数据类型
            actual_value: 实际的值（注意：可能包含敏感信息）
            validation_rules: 验证规则
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details)
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_value = actual_value
        self.validation_rules = validation_rules

        # 自动填充详细信息（注意：不包含敏感的实际值）
        if field_name:
            self.details["field_name"] = field_name
        if expected_type:
            self.details["expected_type"] = expected_type
        if validation_rules:
            self.details["validation_rules"] = validation_rules


class FileSystemError(DBConnectorError):
    """
    文件系统操作异常

    处理文件读写、目录操作、权限检查等过程中出现的错误。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        file_path: Optional[str] = None,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化文件系统异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            file_path: 相关的文件路径
            operation: 文件操作类型（如：read, write, delete等）
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details)
        self.file_path = file_path
        self.operation = operation

        # 自动填充详细信息
        if file_path:
            self.details["file_path"] = file_path
        if operation:
            self.details["operation"] = operation


class TimeoutError(DBConnectorError):
    """
    超时异常

    处理各种操作超时的情况。
    """

    def __init__(
        self,
        message: str,
        error_code: Optional[str] = None,
        timeout_seconds: Optional[float] = None,
        operation: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        初始化超时异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            timeout_seconds: 超时时间（秒）
            operation: 超时的操作类型
            details: 详细的错误信息
        """
        super().__init__(message, error_code, details)
        self.timeout_seconds = timeout_seconds
        self.operation = operation

        # 自动填充详细信息
        if timeout_seconds:
            self.details["timeout_seconds"] = timeout_seconds
        if operation:
            self.details["operation"] = operation
