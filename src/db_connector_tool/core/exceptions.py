"""数据库连接器自定义异常模块 (Exceptions)

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
│   ├── DBConnectionError (数据库连接异常)
│   ├── DriverError (数据库驱动异常)
│   └── QueryError (查询执行异常)
├── ValidationError (数据验证异常)
├── FileSystemError (文件系统操作异常)
└── DBTimeoutError (超时异常)

Example:
>>> from db_connector_tool import DBConnectorError, ConfigError
>>> try:
...     # 执行可能抛出异常的操作
...     raise ConfigError("配置文件格式错误", "CONFIG_001", config_file="connections.toml")
... except DBConnectorError as e:
...     print(e)
...     error_dict = e.to_dict()
>>> # 捕获特定类型的异常
>>> try:
...     # 执行数据库连接操作
...     pass
... except DBConnectionError as e:
...     print(f"连接错误: {e.message}")
"""

from typing import Any, Dict


class DBConnectorError(Exception):
    """数据库连接器基础异常类 (DB Connector Error)

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
        """初始化基础异常

        初始化数据库连接器基础异常，设置错误信息、错误代码和详细信息。

        Args:
            message: 异常描述信息，应清晰描述错误原因
            error_code: 错误代码，用于错误分类和识别，格式建议为"模块_编号"
            details: 详细的错误信息字典，包含相关上下文信息

        Notes:
            - 错误代码应保持唯一性和一致性
            - 详细信息字典应包含有助于调试的相关数据

        Example:
            >>> error = DBConnectorError("连接失败", "CONN_001", {"host": "localhost"})
            >>> print(error.message)
            "连接失败"
        """

        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def __str__(self) -> str:
        """返回异常的字符串表示

        返回格式化的异常信息字符串，包含异常类型、错误信息和错误代码（如果有）。

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
        """将异常信息转换为字典格式

        将异常信息转换为字典格式，便于序列化和日志记录，包含完整的异常信息。

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
    """配置相关异常 (Config Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化配置异常

        初始化配置相关异常，设置错误信息、错误代码和配置相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他配置相关信息，如 config_file, config_section, config_key

        Notes:
            - 自动将配置相关信息填充到details字典中
            - 便于日志记录和错误分析

        Example:
            >>> error = ConfigError(
            ...     "配置文件格式错误",
            ...     "CONFIG_001",
            ...     config_file="connections.toml"
            ... )
            >>> print(error.message)
            "配置文件格式错误"
        """

        super().__init__(message, error_code, details)
        self.config_file = kwargs.get("config_file")
        self.config_section = kwargs.get("config_section")
        self.config_key = kwargs.get("config_key")

        # 自动填充配置相关的详细信息
        if self.config_file:
            self.details["config_file"] = self.config_file
        if self.config_section:
            self.details["config_section"] = self.config_section
        if self.config_key:
            self.details["config_key"] = self.config_key


class CryptoError(DBConnectorError):
    """加密解密相关异常 (Crypto Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化加密异常

        初始化加密解密相关异常，设置错误信息、错误代码和加密相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他加密相关信息，如 operation, algorithm

        Notes:
            - 便于识别加密操作的具体失败环节
            - 算法信息有助于调试兼容性问题

        Example:
            >>> error = CryptoError(
            ...     "加密失败",
            ...     "CRYPTO_001",
            ...     operation="encrypt",
            ...     algorithm="AES-256"
            ... )
            >>> print(error.message)
            "加密失败"
        """

        super().__init__(message, error_code, details)
        self.operation = kwargs.get("operation")
        self.algorithm = kwargs.get("algorithm")

        # 自动填充加密相关的详细信息
        if self.operation:
            self.details["operation"] = self.operation
        if self.algorithm:
            self.details["algorithm"] = self.algorithm


class DatabaseError(DBConnectorError):
    """数据库操作基础异常 (Database Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化数据库异常

        初始化数据库操作基础异常，设置错误信息、错误代码和数据库相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他数据库相关信息，如 database_type, operation

        Notes:
            - 便于区分不同数据库类型的错误处理
            - 操作类型信息有助于定位问题环节

        Example:
            >>> error = DatabaseError(
            ...     "数据库操作失败",
            ...     "DB_001",
            ...     database_type="mysql",
            ...     operation="query"
            ... )
            >>> print(error.message)
            "数据库操作失败"
        """

        super().__init__(message, error_code, details)
        self.database_type = kwargs.get("database_type")
        self.operation = kwargs.get("operation")

        # 自动填充数据库相关的详细信息
        if self.database_type:
            self.details["database_type"] = self.database_type
        if self.operation:
            self.details["operation"] = self.operation


class DBConnectionError(DatabaseError):
    """数据库连接异常 (DB Connection Error)

    处理数据库连接建立、断开、测试等过程中出现的错误。
    包含连接名称、主机、端口和数据库等连接信息。

    Attributes:
        connection_name (str | None): 连接名称或标识
        host (str | None): 数据库主机地址
        port (int | None): 数据库端口号
        database (str | None): 数据库名称

    Example:
        >>> raise DBConnectionError(
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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化连接异常

        初始化数据库连接异常，设置错误信息、错误代码和连接相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他连接相关信息，如 connection_name, host, port, database

        Notes:
            - 包含完整的连接信息，便于问题诊断
            - 支持多连接环境的错误区分

        Example:
            >>> error = DBConnectionError(
            ...     "数据库连接失败",
            ...     "CONN_001",
            ...     connection_name="main_db",
            ...     host="localhost",
            ...     port=3306,
            ...     database="test_db"
            ... )
            >>> print(error.message)
            "数据库连接失败"
        """

        super().__init__(message, error_code, details=details)
        self.connection_name = kwargs.get("connection_name")
        self.host = kwargs.get("host")
        self.port = kwargs.get("port")
        self.database = kwargs.get("database")

        # 自动填充连接相关的详细信息
        if self.connection_name:
            self.details["connection_name"] = self.connection_name
        if self.host:
            self.details["host"] = self.host
        if self.port:
            self.details["port"] = self.port
        if self.database:
            self.details["database"] = self.database


class DriverError(DatabaseError):
    """数据库驱动异常 (Driver Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化驱动异常

        初始化数据库驱动异常，设置错误信息、错误代码和驱动相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他驱动相关信息，如 driver_name, driver_version

        Notes:
            - 便于识别驱动兼容性问题
            - 版本信息有助于调试版本相关的错误

        Example:
            >>> error = DriverError(
            ...     "驱动加载失败",
            ...     "DRIVER_001",
            ...     driver_name="mysql-connector",
            ...     driver_version="8.0.0"
            ... )
            >>> print(error.message)
            "驱动加载失败"
        """

        super().__init__(message, error_code, details=details)
        self.driver_name = kwargs.get("driver_name")
        self.driver_version = kwargs.get("driver_version")

        # 自动填充驱动相关的详细信息
        if self.driver_name:
            self.details["driver_name"] = self.driver_name
        if self.driver_version:
            self.details["driver_version"] = self.driver_version


class QueryError(DatabaseError):
    """查询执行异常 (Query Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化查询异常

        初始化查询执行异常，设置错误信息、错误代码和查询相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他查询相关信息，如 query, query_type, parameters

        Notes:
            - 查询语句进行预览处理，避免泄露完整SQL
            - 参数信息只记录键名，不记录值（安全考虑）

        Example:
            >>> error = QueryError(
            ...     "查询语法错误",
            ...     "QUERY_001",
            ...     query="SELECT * FROM users WHERE id = ?",
            ...     query_type="SELECT",
            ...     parameters={"id": 1}
            ... )
            >>> print(error.message)
            "查询语法错误"
        """

        super().__init__(message, error_code, details=details)
        self.query = kwargs.get("query")
        self.query_type = kwargs.get("query_type")
        self.parameters = kwargs.get("parameters")

        # 自动填充查询相关的详细信息（安全处理）
        if self.query:
            self.details["query_preview"] = self._get_query_preview(self.query)
        if self.query_type:
            self.details["query_type"] = self.query_type
        if self.parameters:
            # 只记录参数键名，不记录参数值（避免泄露敏感信息）
            self.details["parameter_keys"] = list(self.parameters.keys())

    def _get_query_preview(self, query: str, max_length: int = 100) -> str:
        """获取查询语句的预览（安全处理）

        获取查询语句的预览，避免在日志中泄露完整的SQL语句，同时保留足够的上下文信息用于调试。

        Args:
            query: 原始查询语句
            max_length: 最大预览长度

        Returns:
            str: 查询预览字符串

        Notes:
            - 避免在日志中泄露完整的SQL语句
            - 保留足够的上下文信息用于调试

        Example:
            >>> error = QueryError("查询错误", "QUERY_001", query="SELECT * FROM users WHERE id = 1")
            >>> error._get_query_preview("SELECT * FROM users WHERE id = 1", 20)
            "SELECT * FROM user..."
        """

        if len(query) <= max_length:
            return query
        return query[:max_length] + "..."


class ValidationError(DBConnectorError):
    """数据验证异常 (Validation Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化验证异常

        初始化数据验证异常，设置错误信息、错误代码和验证相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他验证相关信息，如 field_name, expected_type, actual_value, validation_rules

        Notes:
            - 实际值可能包含敏感信息，需谨慎处理
            - 验证规则信息有助于理解验证失败原因

        Example:
            >>> error = ValidationError(
            ...     "参数验证失败",
            ...     "VALID_001",
            ...     field_name="username",
            ...     expected_type="str",
            ...     validation_rules={"min_length": 3, "max_length": 20}
            ... )
            >>> print(error.message)
            "参数验证失败"
        """

        super().__init__(message, error_code, details)
        self.field_name = kwargs.get("field_name")
        self.expected_type = kwargs.get("expected_type")
        self.actual_value = kwargs.get("actual_value")
        self.validation_rules = kwargs.get("validation_rules")

        # 自动填充验证相关的详细信息（安全处理）
        if self.field_name:
            self.details["field_name"] = self.field_name
        if self.expected_type:
            self.details["expected_type"] = self.expected_type
        if self.validation_rules:
            self.details["validation_rules"] = self.validation_rules
        # 注意：不自动填充actual_value，避免泄露敏感信息


class FileSystemError(DBConnectorError):
    """文件系统操作异常 (File System Error)

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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化文件系统异常

        初始化文件系统操作异常，设置错误信息、错误代码和文件系统相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他文件系统相关信息，如 file_path, operation

        Notes:
            - 文件路径信息便于定位问题文件
            - 操作类型有助于理解错误发生的环节

        Example:
            >>> error = FileSystemError(
            ...     "文件读取失败",
            ...     "FS_001",
            ...     file_path="/path/to/file.txt",
            ...     operation="read"
            ... )
            >>> print(error.message)
            "文件读取失败"
        """

        super().__init__(message, error_code, details)
        self.file_path = kwargs.get("file_path")
        self.operation = kwargs.get("operation")

        # 自动填充文件系统相关的详细信息
        if self.file_path:
            self.details["file_path"] = self.file_path
        if self.operation:
            self.details["operation"] = self.operation


class DBTimeoutError(DBConnectorError):
    """超时异常 (DB Timeout Error)

    处理各种操作超时的情况，包含超时时间和操作类型信息。

    Attributes:
        timeout_seconds (float | None): 超时时间（秒）
        operation (str | None): 超时的操作类型

    Example:
        >>> raise DBTimeoutError(
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
        details: Dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        """初始化超时异常

        初始化超时异常，设置错误信息、错误代码和超时相关信息。

        Args:
            message: 异常描述信息
            error_code: 错误代码
            details: 详细的错误信息
            **kwargs: 其他超时相关信息，如 timeout_seconds, operation

        Notes:
            - 超时时间信息有助于调整配置参数
            - 操作类型便于识别性能瓶颈

        Example:
            >>> error = DBTimeoutError(
            ...     "数据库查询超时",
            ...     "TIMEOUT_001",
            ...     timeout_seconds=30.0,
            ...     operation="query"
            ... )
            >>> print(error.message)
            "数据库查询超时"
        """

        super().__init__(message, error_code, details)
        self.timeout_seconds = kwargs.get("timeout_seconds")
        self.operation = kwargs.get("operation")

        # 自动填充超时相关的详细信息
        if self.timeout_seconds:
            self.details["timeout_seconds"] = self.timeout_seconds
        if self.operation:
            self.details["operation"] = self.operation
