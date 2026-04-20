"""GBase 8s JDBC 驱动模块 (GBase8sJDBCDialect)

Example:
>>> from db_connector_tool.drivers.gbase8s_jdbc import GBase8sJDBCDialect
>>> from sqlalchemy import create_engine
>>>
>>> # 创建 GBase 8s 连接引擎
>>> engine = create_engine(
...     "gbase8s+jdbc://username:password@host:port/database"
... )
>>> with engine.connect() as conn:
...     result = conn.execute("SELECT * FROM users")
...     for row in result:
...         print(row)
"""

import os
import re
import warnings
from abc import ABC
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Tuple

import jaydebeapi
import jpype
from dateutil import parser
from sqlalchemy import CHAR, TIMESTAMP, VARCHAR, TypeDecorator, exc, sql, util
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine.interfaces import DBAPIModule
from sqlalchemy.engine.url import make_url

from ..utils.path_utils import PathHelper


class GBase8sCursor(jaydebeapi.Cursor):
    """GBase 8s JDBC 游标类 (GBase8s Cursor)

    Example:
        >>> # 内部使用，由 SQLAlchemy 自动创建
        >>> # 无需手动实例化
    """

    def __init__(self, connection: Any, converters: Any) -> None:
        """初始化 GBase 8s 游标

        Args:
            connection: JDBC 连接对象
            converters: 类型转换器字典

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动创建
            >>> # 无需手动实例化
        """
        super().__init__(connection, converters)
        jaydebeapi._unknownSqlTypeConverter = self._unknown_sql_type_converter

    def _unknown_sql_type_converter(self, result_set: Any, column_index: int) -> Any:
        """处理未知 SQL 类型的转换

        Args:
            result_set: JDBC 结果集对象
            column_index: 列索引

        Returns:
            转换后的值，如果是 GBaseClob2 类型则转换为字符串

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        value = result_set.getObject(column_index)

        # 检查是否为 GBaseClob2 类型
        if str(type(value)) == "<java class 'com.gbasedbt.jdbc.GBaseClob2'>":
            string = ""
            reader = value.getCharacterStream()
            while True:
                char = reader.read()
                if char == -1:  # 到达流末尾
                    break
                string += chr(char)
            value = string

        return value


# pylint: disable=too-many-ancestors
class ObTimestamp(TypeDecorator):
    """GBase 8s 时间戳类型装饰器 (GBase8s Timestamp Decorator)

    Example:
        >>> # 内部使用，由 SQLAlchemy 自动应用
        >>> # 无需手动使用
    """

    impl = TIMESTAMP

    @property
    def python_type(self):
        """返回此类型对应的 Python 类型

        Returns:
            type: Python 类型，即 datetime 类型

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        return datetime

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        """处理绑定参数

        Args:
            value: 输入值
            dialect: SQLAlchemy 方言对象

        Returns:
            转换后的时间戳值

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        if isinstance(value, datetime):
            timestamp_class = jpype.JClass("java.sql.Timestamp")
            value = timestamp_class.valueOf(value.strftime("%Y-%m-%d %H:%M:%S.%f"))
        return value

    def process_literal_param(self, value: Any, dialect: Any) -> str:
        """处理字面量参数

        Args:
            value: 输入值
            dialect: SQLAlchemy 方言对象

        Returns:
            转换后的 SQL 字面量字符串

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        if value is None:
            return "NULL"
        if isinstance(value, datetime):
            # 将 datetime 转换为 SQL 标准的时间戳格式
            return f"TIMESTAMP '{value.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
        # 对于其他类型，使用默认的字符串表示
        return str(value)

    def process_result_value(self, value: Any, dialect: Any) -> datetime | None:
        """处理结果值

        Args:
            value: 数据库返回的值
            dialect: SQLAlchemy 方言对象

        Returns:
            解析后的 datetime 对象，如果值为 None 则返回 None

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        if value is not None:
            return parser.parse(value)
        return None


# 更新列规范，将 TIMESTAMP 类型替换为自定义的 ObTimestamp
colspecs = util.update_copy(
    dict(OracleDialect.colspecs),
    {
        TIMESTAMP: ObTimestamp,
        VARCHAR: VARCHAR,  # 确保VARCHAR类型正确映射
        CHAR: CHAR,  # CHAR类型映射
    },
)


class GBase8sJDBCDialect(OracleDialect, ABC):
    """GBase 8s JDBC 方言实现 (GBase8s JDBC Dialect)

    Example:
        >>> from db_connector_tool.drivers.gbase8s_jdbc import GBase8sJDBCDialect
        >>> from sqlalchemy import create_engine
        >>>
        >>> # 创建 GBase 8s 连接引擎
        >>> engine = create_engine(
        ...     "gbase8s+jdbc://username:password@host:port/database"
        ... )
        >>> with engine.connect() as conn:
        ...     result = conn.execute("SELECT * FROM users")
        ...     for row in result:
        ...         print(row)
    """

    name = "gbasedbt-sqli"
    driver = "com.gbasedbt.jdbc.Driver"
    colspecs = colspecs

    # 数据库特性支持配置
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    supports_statement_cache = True
    description_encoding = None

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """导入数据库 API 模块

        Returns:
            导入的 jaydebeapi 模块

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        return __import__("jaydebeapi")

    def create_connect_args(self, url: Any) -> Tuple[Tuple, dict]:
        """创建连接参数

        Args:
            url: SQLAlchemy URL 对象

        Returns:
            包含连接参数的元组和字典

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        url_obj = make_url(url)

        # 构建 JDBC URL
        jdbc_url = self._build_jdbc_url(url_obj)

        # 构建连接参数
        connect_args = self._build_connect_args(url_obj)

        kwargs = {
            "jclassname": self.driver,
            "url": jdbc_url,
            "driver_args": connect_args,
        }

        # 处理 JAR 路径
        self._handle_jar_path(kwargs)

        return (), kwargs

    def _build_jdbc_url(self, url_obj: Any) -> str:
        """构建 JDBC 连接 URL 字符串

        Args:
            url_obj: SQLAlchemy URL 对象

        Returns:
            str: GBase 8s JDBC 连接 URL

        Example:
            >>> # 内部使用，由 create_connect_args 方法调用
            >>> # 无需手动调用
        """
        jdbc_url_parts = [f"jdbc:{self.name}://{url_obj.host}"]
        if url_obj.port:
            jdbc_url_parts.append(f":{url_obj.port}")
        if url_obj.database:
            jdbc_url_parts.append(f"/{url_obj.database}")
        return "".join(jdbc_url_parts)

    def _build_connect_args(self, url_obj: Any) -> dict:
        """构建数据库连接参数字典

        Args:
            url_obj: SQLAlchemy URL 对象

        Returns:
            dict: 连接参数字典

        Example:
            >>> # 内部使用，由 create_connect_args 方法调用
            >>> # 无需手动调用
        """
        connect_args = {}
        if url_obj.username:
            connect_args["user"] = url_obj.username
        if url_obj.password:
            connect_args["password"] = url_obj.password

        if url_obj.query:
            for key, value in url_obj.query.items():
                connect_args[key] = value if isinstance(value, str) else str(value)

        if "rewriteBatchedStatements" not in connect_args:
            connect_args["rewriteBatchedStatements"] = "true"

        return connect_args

    def _handle_jar_path(self, kwargs: dict) -> None:
        """处理 JDBC 驱动 JAR 文件路径

        Args:
            kwargs: 连接参数字典

        Example:
            >>> # 内部使用，由 create_connect_args 方法调用
            >>> # 无需手动调用
        """

        jar_path = None

        # 1. 优先使用环境变量
        if "GBASE8S_JDBC_JARPATH" in os.environ:
            jar_path = os.environ["GBASE8S_JDBC_JARPATH"]

        # # 2. 其次使用URL参数（需修改sqlalchemy_driver.py的URL）
        # elif url_obj.query and "jarpath" in url_obj.query:
        #     jar_path = url_obj.query["jarpath"]

        # 3. 尝试常见默认路径
        else:
            app_name = "db_connector_tool"
            config_dir = PathHelper.get_user_config_dir(app_name)
            config_path = os.path.join(config_dir, "jars")
            path = Path(config_path)
            # 使用 glob 递归获取所有文件
            files = [p for p in path.rglob("*gbase*.jar") if p.is_file()]
            for file in files:
                jar_path = str(file)
                break

        if isinstance(jar_path, str) and os.path.exists(jar_path):
            kwargs["jars"] = jar_path
        else:
            # 处理jar_path为None的情况，提供更友好的显示
            path_display = jar_path if jar_path is not None else "未找到任何搜索路径"
            # 获取正确的默认目录路径，使用os.path.join确保路径分隔符正确
            default_jar_dir = PathHelper.get_user_config_dir("db_connector_tool")
            default_jar_path = os.path.join(default_jar_dir, "jars")
            warnings.warn(
                f"GBase 8s JDBC驱动jar文件未找到。\n"
                f"当前搜索路径: {path_display}\n"
                f"解决方案:\n"
                f"1. 设置环境变量: GBASE8S_JDBC_JARPATH=/path/to/gbasedbtjdbc_xxx.jar\n"
                f"2. 将jar文件放置在默认目录: {default_jar_path}",
                UserWarning,
            )
            kwargs["jars"] = jar_path

    @property
    def _is_oracle_8(self) -> bool:
        """检查是否为 Oracle 8 数据库

        Returns:
            bool: False，因为这是 GBase 8s 方言

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        return False

    def _check_max_identifier_length(self, connection: Any) -> Literal[30] | None:
        """检查最大标识符长度

        Args:
            connection: 数据库连接对象

        Returns:
            最大标识符长度，如果无法确定则返回 None

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        return None

    def get_default_schema_name(self, connection: Any) -> str | None:
        """获取默认 schema 名称

        Args:
            connection: 数据库连接对象

        Returns:
            大写的用户名作为默认 schema，如果用户名不存在则返回 None

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        username = connection.engine.url.username
        return username.upper() if username else None

    def _get_default_schema_name(self, connection: Any) -> str | None:
        """内部方法：获取默认 schema 名称

        Args:
            connection: 数据库连接对象

        Returns:
            默认 schema 名称

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        return self.get_default_schema_name(connection)

    def _get_server_version_info(self, connection: Any) -> Tuple[int, ...] | None:
        """获取服务器版本信息

        Args:
            connection: 数据库连接对象

        Returns:
            版本号元组，如果获取失败则返回 None

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        try:
            # 尝试多种版本查询方式
            version_queries = [
                "select dbinfo('version_gbase','full') from dual",
                "select dbinfo('version','full') from dual",
            ]

            banner = None
            for query in version_queries:
                try:
                    version_sql = sql.text(query)
                    banner = connection.execute(version_sql).scalar()
                    if banner:
                        break
                except exc.DBAPIError:
                    continue

            if isinstance(banner, str):
                # 正则表达式匹配 GBase 8s 版本格式
                version_pattern = (
                    r"GBase8sV?([\d.]+)"  # 主版本号
                    r"(?:_TL_([\d.]+))?"  # TL 版本（可选）
                    r"(?:_(\d+X\d+))?"  # 特殊版本格式（可选）
                    r"(?:_(\d+))?"  # 构建号（可选）
                    r"(?:_([a-f0-9]+))?"  # 提交哈希（可选）
                )
                match = re.search(version_pattern, banner)

                if match:
                    version_str = match.group(1)
                    return tuple(int(part) for part in version_str.split("."))
                # 尝试简单版本号提取
                simple_match = re.search(r"(\d+)\.(\d+)", banner)
                if simple_match:
                    return (int(simple_match.group(1)), int(simple_match.group(2)))

            return None

        except exc.DBAPIError:
            # 版本查询失败，返回 None
            return None
        except (ValueError, re.error, AttributeError):
            # 正则表达式解析错误或属性访问错误，返回 None
            return None

    def is_disconnect(self, e: Exception, connection: Any, cursor: Any) -> bool:
        """检查异常是否为连接断开错误

        Args:
            e: 异常对象
            connection: 连接对象
            cursor: 游标对象

        Returns:
            bool: 如果是连接断开错误返回 True，否则返回 False

        Example:
            >>> # 内部使用，由 SQLAlchemy 自动调用
            >>> # 无需手动调用
        """
        error_str = str(e).lower()
        disconnect_indicators = [
            "connection closed",
            "socket closed",
            "broken pipe",
            "connection reset",
            "jdbc connection",
            "network error",
        ]

        return any(indicator in error_str for indicator in disconnect_indicators)


# 注册方言到 SQLAlchemy
registry.register(
    "jdbcgbase8s", "db_connector_tool.drivers.gbase8s_jdbc", "GBase8sJDBCDialect"
)
