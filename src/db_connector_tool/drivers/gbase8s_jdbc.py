import re
from abc import ABC
from datetime import datetime
from types import ModuleType

import jaydebeapi
from dateutil import parser
from sqlalchemy import TIMESTAMP, TypeDecorator, exc, sql, util
from sqlalchemy.dialects import registry
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.engine.url import make_url


class GBase8sCursor(jaydebeapi.Cursor):
    """Defined private Cursor modify the Clob object value return."""

    def __init__(self, connection, converters):
        super(GBase8sCursor, self).__init__(connection, converters)
        jaydebeapi._unknownSqlTypeConverter = self._unknownSqlTypeConverter

    def _unknownSqlTypeConverter(self, rs, col):
        value = rs.getObject(col)
        if str(type(value)) == "<java class 'com.gbasedbt.jdbc.GBaseClob2'>":
            string, reader = "", value.getCharacterStream()
            while True:
                char = reader.read()
                if char == -1:
                    break
                string += chr(char)
            value = string
        return value


class ObTimestamp(TypeDecorator):
    impl = TIMESTAMP

    def process_bind_param(self, value, dialect):
        if isinstance(value, datetime):
            import jpype

            timestamp = jpype.JClass("java.sql.Timestamp")
            value = timestamp.valueOf(value.strftime("%Y-%m-%d %H:%M:%S.%f"))
        return value

    def process_result_value(self, value, dialect):
        if value is not None:
            return parser.parse(value)
        return None


colspecs = util.update_copy(dict(OracleDialect.colspecs), {TIMESTAMP: ObTimestamp})


class GBase8sJDBCDialect(OracleDialect, ABC):
    name = "gbasedbt-sqli"
    driver = "com.gbasedbt.jdbc.Driver"
    colspecs = colspecs

    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    supports_statement_cache = True
    description_encoding = None

    def initialize(self, connection):
        """重写initialize方法，避免调用OracleDialect的初始化"""
        # super(GBase8sJDBCDialect, self).initialize(connection)
        pass

    @classmethod
    def dbapi(cls):
        import jaydebeapi

        jaydebeapi.Cursor = GBase8sCursor
        return jaydebeapi

    @classmethod
    def import_dbapi(cls):
        import jaydebeapi

        jaydebeapi.Cursor = GBase8sCursor
        return jaydebeapi

    def do_rollback(self, dbapi_connection):
        # GBase8s 不支持事务回滚操作，因此该方法留空
        pass

    def create_connect_args(self, url):
        url = make_url(url)
        jdbc_url = f"jdbc:{self.name}://{url.host}:{url.port}/{url.database}"
        connect_args = {"user": url.username, "password": url.password}
        if url.query:
            for key, value in url.query.items():
                connect_args[key] = value if isinstance(value, str) else str(value)
        kwargs = {
            "jclassname": self.driver,
            "url": jdbc_url,
            "driver_args": connect_args,
        }
        return (), kwargs

    @property
    def _is_oracle_8(self):
        return False

    def _check_max_identifier_length(self, connection):
        return None

    def get_default_schema_name(self, connection):
        """重写此方法以避免使用Oracle的sys_context函数"""
        # 对于Gbase数据库，返回连接的用户名作为默认schema
        return (
            connection.engine.url.username.upper()
            if connection.engine.url.username
            else None
        )

    def _get_default_schema_name(self, connection):
        """重写内部方法，避免Oracle特定查询"""
        return self.get_default_schema_name(connection)

    def _get_server_version_info(self, connection):
        try:
            ver_sql = sql.text("select dbinfo('version_gbase','full') from dual")
            banner = connection.execute(ver_sql).scalar()
            if isinstance(banner, str):
                match = re.search(r"gbase ([\d+.]+\d+)", banner)
                if match:
                    version = match.group(1)
                    return tuple(int(x) for x in version.split("."))
            # 如果 banner 无效或正则匹配失败，返回默认版本或 None
            return None
        except exc.DBAPIError:
            return None


registry.register(
    "jdbcapi.gbase8sjdbc", "sqlalchemy_jdbcapi.gbase8sjdbc", "GBase8sJDBCDialect"
)
