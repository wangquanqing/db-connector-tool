import re
from abc import ABC
from datetime import datetime
from typing import Any, Literal, Tuple

import jaydebeapi
from dateutil import parser
from sqlalchemy import TIMESTAMP, TypeDecorator, exc, sql, util
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine.interfaces import DBAPIModule
from sqlalchemy.engine.url import make_url


class GBase8sCursor(jaydebeapi.Cursor):
    """
    GBase8s JDBC 游标类，用于处理 GBaseClob2 类型的特殊转换。

    继承自 jaydebeapi.Cursor，重写未知 SQL 类型转换器以正确处理 GBaseClob2 类型。
    """

    def __init__(self, connection: Any, converters: Any) -> None:
        """
        初始化 GBase8s 游标。

        Args:
            connection: JDBC 连接对象
            converters: 类型转换器字典
        """
        super().__init__(connection, converters)
        jaydebeapi._unknownSqlTypeConverter = self._unknownSqlTypeConverter

    def _unknownSqlTypeConverter(self, result_set: Any, column_index: int) -> Any:
        """
        处理未知 SQL 类型的转换，特别处理 GBaseClob2 类型。

        Args:
            result_set: JDBC 结果集对象
            column_index: 列索引

        Returns:
            转换后的值，如果是 GBaseClob2 类型则转换为字符串
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


class ObTimestamp(TypeDecorator):
    """
    GBase8s 时间戳类型装饰器。

    处理 Python datetime 对象与 GBase8s 时间戳类型之间的转换。
    """

    impl = TIMESTAMP

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        """
        处理绑定参数，将 Python datetime 转换为 Java Timestamp。

        Args:
            value: 输入值
            dialect: SQLAlchemy 方言对象

        Returns:
            转换后的时间戳值
        """
        if isinstance(value, datetime):
            import jpype

            timestamp_class = jpype.JClass("java.sql.Timestamp")
            value = timestamp_class.valueOf(value.strftime("%Y-%m-%d %H:%M:%S.%f"))
        return value

    def process_result_value(self, value: Any, dialect: Any) -> datetime | None:
        """
        处理结果值，将字符串解析为 Python datetime 对象。

        Args:
            value: 数据库返回的值
            dialect: SQLAlchemy 方言对象

        Returns:
            解析后的 datetime 对象，如果值为 None 则返回 None
        """
        if value is not None:
            return parser.parse(value)
        return None


# 更新列规范，将 TIMESTAMP 类型替换为自定义的 ObTimestamp
colspecs = util.update_copy(dict(OracleDialect.colspecs), {TIMESTAMP: ObTimestamp})


class GBase8sJDBCDialect(OracleDialect, ABC):
    """
    GBase8s JDBC 方言实现。

    基于 OracleDialect 实现，适配 GBase8s 数据库的特性。
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

    def initialize(self, connection: Any) -> None:
        """
        初始化方言，避免调用 OracleDialect 的初始化方法。

        Args:
            connection: SQLAlchemy 连接对象
        """
        # 由于 GBase8s 与 Oracle 有差异，不调用父类初始化
        pass

    @classmethod
    def dbapi(cls) -> Any:
        """
        返回数据库 API 模块，使用自定义的 GBase8sCursor。

        Returns:
            jaydebeapi 模块，使用自定义游标类
        """
        import jaydebeapi

        jaydebeapi.Cursor = GBase8sCursor
        return jaydebeapi

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """
        导入数据库 API 模块。

        Returns:
            导入的 jaydebeapi 模块
        """
        return __import__("jaydebeapi")

    def do_rollback(self, dbapi_connection: Any) -> None:
        """
        执行回滚操作。

        GBase8s 不支持事务回滚操作，因此该方法为空实现。

        Args:
            dbapi_connection: 数据库 API 连接对象
        """
        # GBase8s 不支持事务回滚，留空实现
        pass

    def create_connect_args(self, url: Any) -> Tuple[Tuple, dict]:
        """
        创建连接参数。

        Args:
            url: SQLAlchemy URL 对象

        Returns:
            包含连接参数的元组和字典
        """
        url_obj = make_url(url)
        jdbc_url = (
            f"jdbc:{self.name}://{url_obj.host}:{url_obj.port}/{url_obj.database}"
        )

        # 基础连接参数
        connect_args = {"user": url_obj.username, "password": url_obj.password}

        # 添加查询字符串参数
        if url_obj.query:
            for key, value in url_obj.query.items():
                connect_args[key] = value if isinstance(value, str) else str(value)

        kwargs = {
            "jclassname": self.driver,
            "url": jdbc_url,
            "driver_args": connect_args,
        }

        return (), kwargs

    @property
    def _is_oracle_8(self) -> bool:
        """
        检查是否为 Oracle 8 数据库。

        Returns:
            False，因为这是 GBase8s 方言
        """
        return False

    def _check_max_identifier_length(self, connection: Any) -> Literal[30] | None:
        """
        检查最大标识符长度。

        Args:
            connection: 数据库连接对象

        Returns:
            最大标识符长度，如果无法确定则返回 None
        """
        return None

    def get_default_schema_name(self, connection: Any) -> str | None:
        """
        获取默认 schema 名称。

        Args:
            connection: 数据库连接对象

        Returns:
            大写的用户名作为默认 schema，如果用户名不存在则返回 None
        """
        username = connection.engine.url.username
        return username.upper() if username else None

    def _get_default_schema_name(self, connection: Any) -> str | None:
        """
        内部方法：获取默认 schema 名称。

        Args:
            connection: 数据库连接对象

        Returns:
            默认 schema 名称
        """
        return self.get_default_schema_name(connection)

    def _get_server_version_info(self, connection: Any) -> Tuple[int, ...] | None:
        """
        获取服务器版本信息。

        Args:
            connection: 数据库连接对象

        Returns:
            版本号元组，如果获取失败则返回 None
        """
        try:
            version_sql = sql.text("select dbinfo('version_gbase','full') from dual")
            banner = connection.execute(version_sql).scalar()

            if isinstance(banner, str):
                # 正则表达式匹配 GBase8s 版本格式
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

            return None

        except exc.DBAPIError:
            # 版本查询失败，返回 None
            return None


# 注册方言到 SQLAlchemy
registry.register(
    "jdbc.gbase8s_jdbc", "db_connector_tool.drivers.gbase8s_jdbc", "GBase8sJDBCDialect"
)
