"""
数据库连接器自定义异常模块

提供项目专用的异常类层次结构，用于更精确地处理不同类型的错误。
异常类按照功能模块进行组织，便于错误分类和处理。

主要特性：
- 统一的异常基类，提供标准化的错误处理接口
- 详细的错误信息和上下文信息
- 支持错误代码和分类
- 自动填充相关上下文信息
- 安全的敏感信息处理

异常类层次结构：
DBConnectorError
├── ConfigError (配置相关异常)
├── CryptoError (加密解密相关异常)
├── DatabaseError (数据库操作基础异常)
│   ├── ConnectionError (数据库连接异常)
│   ├── DriverError (数据库驱动异常)
│   └── QueryError (查询执行异常)
├── ValidationError (数据验证异常)
├── FileSystemError (文件系统操作异常)
└── TimeoutError (超时异常)
"""

from typing import Any, Dict


class DBConnectorError(Exception):
    """
    数据库连接器基础异常类

    所有自定义异常的基类，提供统一的异常处理接口。
    支持错误代码、详细信息和字典格式转换。

    Attributes:
        message (str): 异常描述信息
        error_code (str | None): 错误代码，用于错误分类和识别
        details (Dict[str, Any] | None): 详细的错误信息字典

    Example:
        >>> try:
        ...     raise DBConnectorError("测试异常", "TEST_001", {"key": "value"})
        ... except DBConnectorError as e:
        ...     print(e.to_dict())
        {'error_type': 'DBConnectorError', 'message': '测试异常',
         'error_code': 'TEST_001', 'details': {'key': 'value'}}
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化基础异常

        Args:
            message: 异常描述信息，应清晰描述错误原因
            error_code: 错误代码，用于错误分类和识别，格式建议为"模块_编号"
            details: 详细的错误信息字典，包含相关上下文信息

        Notes:
            - 错误代码应保持唯一性和一致性
            - 详细信息字典应包含有助于调试的相关数据
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def __str__(self) -> str:
        """
        返回异常的字符串表示

        Returns:
            str: 格式化的异常信息字符串

        Example:
            >>> str(DBConnectorError("连接失败", "CONN_001"))
            'DBConnectorError: 连接失败 (错误代码: CONN_001)'
        """
        base_str = f"{self.__class__.__name__}: {self.message}"
        if self.error_code:
            base_str += f" (错误代码: {self.error_code})"
        return base_str

    def to_dict(self) -> Dict[str, Any]:
        """
        将异常信息转换为字典格式

        便于序列化和日志记录，包含完整的异常信息。

        Returns:
            Dict[str, Any]: 包含异常信息的字典

        Example:
            >>> error = DBConnectorError("测试错误", "TEST_001", {"key": "value"})
            >>> error.to_dict()
            {
                'error_type': 'DBConnectorError',
                'message': '测试错误',
                'error_code': 'TEST_001',
                'details': {'key': 'value'}
            }
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
    包含配置文件路径、配置节和配置键等上下文信息。

    Attributes:
        config_file (str | None): 相关的配置文件路径
        config_section (str | None): 相关的配置节名称
        config_key (str | None): 相关的配置键名称

    Example:
        >>> raise ConfigError(
        ...     "配置文件格式错误",
        ...     "CONFIG_001",
        ...     config_file="config.ini",
        ...     config_section="database",
        ...     config_key="host"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        config_file: str | None = None,
        config_section: str | None = None,
        config_key: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化配置异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            config_file: 相关的配置文件路径
            config_section: 相关的配置节名称
            config_key: 相关的配置键名称
            details: 详细的错误信息

        Notes:
            - 自动将配置相关信息填充到details字典中
            - 便于日志记录和错误分析
        """
        super().__init__(message, error_code, details)
        self.config_file = config_file
        self.config_section = config_section
        self.config_key = config_key

        # 自动填充配置相关的详细信息
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
    包含加密操作类型和算法信息。

    Attributes:
        operation (str | None): 加密操作类型（encrypt/decrypt/generate_key等）
        algorithm (str | None): 使用的加密算法名称

    Example:
        >>> raise CryptoError(
        ...     "加密失败",
        ...     "CRYPTO_001",
        ...     operation="encrypt",
        ...     algorithm="AES-256"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        operation: str | None = None,
        algorithm: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化加密异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            operation: 加密操作类型
            algorithm: 使用的加密算法
            details: 详细的错误信息

        Notes:
            - 便于识别加密操作的具体失败环节
            - 算法信息有助于调试兼容性问题
        """
        super().__init__(message, error_code, details)
        self.operation = operation
        self.algorithm = algorithm

        # 自动填充加密相关的详细信息
        if operation:
            self.details["operation"] = operation
        if algorithm:
            self.details["algorithm"] = algorithm


class DatabaseError(DBConnectorError):
    """
    数据库操作基础异常

    处理所有数据库相关操作的通用错误。
    包含数据库类型和操作类型信息。

    Attributes:
        database_type (str | None): 数据库类型（mysql/postgresql/oracle等）
        operation (str | None): 数据库操作类型（connect/query/execute等）

    Example:
        >>> raise DatabaseError(
        ...     "数据库操作失败",
        ...     "DB_001",
        ...     database_type="mysql",
        ...     operation="query"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        database_type: str | None = None,
        operation: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化数据库异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            database_type: 数据库类型
            operation: 数据库操作类型
            details: 详细的错误信息

        Notes:
            - 便于区分不同数据库类型的错误处理
            - 操作类型信息有助于定位问题环节
        """
        super().__init__(message, error_code, details)
        self.database_type = database_type
        self.operation = operation

        # 自动填充数据库相关的详细信息
        if database_type:
            self.details["database_type"] = database_type
        if operation:
            self.details["operation"] = operation


class ConnectionError(DatabaseError):
    """
    数据库连接异常

    处理数据库连接建立、断开、测试等过程中出现的错误。
    包含连接名称、主机、端口和数据库等连接信息。

    Attributes:
        connection_name (str | None): 连接名称或标识
        host (str | None): 数据库主机地址
        port (int | None): 数据库端口号
        database (str | None): 数据库名称

    Example:
        >>> raise ConnectionError(
        ...     "数据库连接失败",
        ...     "CONN_001",
        ...     connection_name="main_db",
        ...     host="localhost",
        ...     port=3306,
        ...     database="test_db"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        connection_name: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化连接异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            connection_name: 连接名称或标识
            host: 数据库主机地址
            port: 数据库端口号
            database: 数据库名称
            details: 详细的错误信息

        Notes:
            - 包含完整的连接信息，便于问题诊断
            - 支持多连接环境的错误区分
        """
        super().__init__(message, error_code, details=details)
        self.connection_name = connection_name
        self.host = host
        self.port = port
        self.database = database

        # 自动填充连接相关的详细信息
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
    包含驱动名称和版本信息。

    Attributes:
        driver_name (str | None): 驱动名称
        driver_version (str | None): 驱动版本

    Example:
        >>> raise DriverError(
        ...     "驱动加载失败",
        ...     "DRIVER_001",
        ...     driver_name="mysql-connector",
        ...     driver_version="8.0.0"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        driver_name: str | None = None,
        driver_version: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化驱动异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            driver_name: 驱动名称
            driver_version: 驱动版本
            details: 详细的错误信息

        Notes:
            - 便于识别驱动兼容性问题
            - 版本信息有助于调试版本相关的错误
        """
        super().__init__(message, error_code, details=details)
        self.driver_name = driver_name
        self.driver_version = driver_version

        # 自动填充驱动相关的详细信息
        if driver_name:
            self.details["driver_name"] = driver_name
        if driver_version:
            self.details["driver_version"] = driver_version


class QueryError(DatabaseError):
    """
    查询执行异常

    处理SQL查询解析、执行、结果处理等过程中出现的错误。
    包含查询语句、查询类型和参数信息（安全处理）。

    Attributes:
        query (str | None): 执行的SQL查询语句
        query_type (str | None): 查询类型（SELECT/INSERT/UPDATE/DELETE等）
        parameters (Dict[str, Any] | None): 查询参数

    Example:
        >>> raise QueryError(
        ...     "查询语法错误",
        ...     "QUERY_001",
        ...     query="SELECT * FROM users WHERE id = ?",
        ...     query_type="SELECT",
        ...     parameters={"id": 1}
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        query: str | None = None,
        query_type: str | None = None,
        parameters: Dict[str, Any] | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化查询异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            query: 执行的SQL查询语句
            query_type: 查询类型
            parameters: 查询参数
            details: 详细的错误信息

        Notes:
            - 查询语句进行预览处理，避免泄露完整SQL
            - 参数信息只记录键名，不记录值（安全考虑）
        """
        super().__init__(message, error_code, details=details)
        self.query = query
        self.query_type = query_type
        self.parameters = parameters

        # 自动填充查询相关的详细信息（安全处理）
        if query:
            self.details["query_preview"] = self._get_query_preview(query)
        if query_type:
            self.details["query_type"] = query_type
        if parameters:
            # 只记录参数键名，不记录参数值（避免泄露敏感信息）
            self.details["parameter_keys"] = list(parameters.keys())

    def _get_query_preview(self, query: str, max_length: int = 100) -> str:
        """
        获取查询语句的预览（安全处理）

        Args:
            query: 原始查询语句
            max_length: 最大预览长度

        Returns:
            str: 查询预览字符串

        Notes:
            - 避免在日志中泄露完整的SQL语句
            - 保留足够的上下文信息用于调试
        """
        if len(query) <= max_length:
            return query
        return query[:max_length] + "..."


class ValidationError(DBConnectorError):
    """
    数据验证异常

    处理配置验证、参数验证、数据格式验证等过程中出现的错误。
    包含字段名、期望类型和验证规则等信息。

    Attributes:
        field_name (str | None): 验证失败的字段名
        expected_type (str | None): 期望的数据类型
        actual_value (Any | None): 实际的值（注意安全）
        validation_rules (Dict[str, Any] | None): 验证规则

    Example:
        >>> raise ValidationError(
        ...     "参数验证失败",
        ...     "VALID_001",
        ...     field_name="username",
        ...     expected_type="str",
        ...     validation_rules={"min_length": 3, "max_length": 20}
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        field_name: str | None = None,
        expected_type: str | None = None,
        actual_value: Any | None = None,
        validation_rules: Dict[str, Any] | None = None,
        details: Dict[str, Any] | None = None,
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

        Notes:
            - 实际值可能包含敏感信息，需谨慎处理
            - 验证规则信息有助于理解验证失败原因
        """
        super().__init__(message, error_code, details)
        self.field_name = field_name
        self.expected_type = expected_type
        self.actual_value = actual_value
        self.validation_rules = validation_rules

        # 自动填充验证相关的详细信息（安全处理）
        if field_name:
            self.details["field_name"] = field_name
        if expected_type:
            self.details["expected_type"] = expected_type
        if validation_rules:
            self.details["validation_rules"] = validation_rules
        # 注意：不自动填充actual_value，避免泄露敏感信息


class FileSystemError(DBConnectorError):
    """
    文件系统操作异常

    处理文件读写、目录操作、权限检查等过程中出现的错误。
    包含文件路径和操作类型信息。

    Attributes:
        file_path (str | None): 相关的文件路径
        operation (str | None): 文件操作类型（read/write/delete等）

    Example:
        >>> raise FileSystemError(
        ...     "文件读取失败",
        ...     "FS_001",
        ...     file_path="/path/to/file.txt",
        ...     operation="read"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        file_path: str | None = None,
        operation: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化文件系统异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            file_path: 相关的文件路径
            operation: 文件操作类型
            details: 详细的错误信息

        Notes:
            - 文件路径信息便于定位问题文件
            - 操作类型有助于理解错误发生的环节
        """
        super().__init__(message, error_code, details)
        self.file_path = file_path
        self.operation = operation

        # 自动填充文件系统相关的详细信息
        if file_path:
            self.details["file_path"] = file_path
        if operation:
            self.details["operation"] = operation


class TimeoutError(DBConnectorError):
    """
    超时异常

    处理各种操作超时的情况。
    包含超时时间和操作类型信息。

    Attributes:
        timeout_seconds (float | None): 超时时间（秒）
        operation (str | None): 超时的操作类型

    Example:
        >>> raise TimeoutError(
        ...     "数据库查询超时",
        ...     "TIMEOUT_001",
        ...     timeout_seconds=30.0,
        ...     operation="query"
        ... )
    """

    def __init__(
        self,
        message: str,
        error_code: str | None = None,
        timeout_seconds: float | None = None,
        operation: str | None = None,
        details: Dict[str, Any] | None = None,
    ) -> None:
        """
        初始化超时异常

        Args:
            message: 异常描述信息
            error_code: 错误代码
            timeout_seconds: 超时时间（秒）
            operation: 超时的操作类型
            details: 详细的错误信息

        Notes:
            - 超时时间信息有助于调整配置参数
            - 操作类型便于识别性能瓶颈
        """
        super().__init__(message, error_code, details)
        self.timeout_seconds = timeout_seconds
        self.operation = operation

        # 自动填充超时相关的详细信息
        if timeout_seconds:
            self.details["timeout_seconds"] = timeout_seconds
        if operation:
            self.details["operation"] = operation
