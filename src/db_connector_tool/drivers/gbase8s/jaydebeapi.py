"""GBase 8s JayDeBeApi 适配层 (GBase8s JayDeBeApi)

对齐 SQLAlchemy 官方 Oracle 方言中 ``cx_oracle.py`` 的定位：
本模块只负责「连接怎么建立、取值怎么转换、异常怎么识别」，
SQL 编译与反射等 DBAPI 无关逻辑全部在 ``base.py``。

Example:
>>> from db_connector_tool.drivers.gbase8s.jaydebeapi import GBase8sDialect_jaydebeapi
>>> GBase8sDialect_jaydebeapi.driver
'com.gbasedbt.jdbc.Driver'
"""

import os
import warnings
from typing import Any, Tuple

import jaydebeapi
from sqlalchemy.engine.interfaces import DBAPIModule
from sqlalchemy.engine.url import make_url

from ...utils.logging_utils import get_logger
from . import provision
from .base import GBase8sDialect

logger = get_logger(__name__)


class GBase8sCursor(jaydebeapi.Cursor):
    """GBase 8s JDBC 游标类 (GBase8s Cursor)

    扩展 jaydebeapi 游标，支持 GBaseClob2 类型的自动转换。

    Example:
        >>> import jaydebeapi
        >>> cursor = GBase8sCursor(connection, converters)
    """

    def __init__(self, connection: Any, converters: Any) -> None:
        """初始化 GBase 8s 游标

        Args:
            connection: JDBC 连接对象
            converters: 类型转换器字典
        """
        super().__init__(connection, converters)
        jaydebeapi._unknownSqlTypeConverter = self._unknown_sql_type_converter

    def _unknown_sql_type_converter(self, result_set: Any, column_index: int) -> Any:
        """处理未知 SQL 类型的转换

        将 GBaseClob2 类型的值通过字符流逐字符读出并转为字符串。

        Args:
            result_set: JDBC 结果集对象
            column_index: 列索引

        Returns:
            转换后的值，GBaseClob2 类型转为字符串
        """
        value = result_set.getObject(column_index)

        if str(type(value)) == "<java class 'com.gbasedbt.jdbc.GBaseClob2'>":
            string = ""
            reader = value.getCharacterStream()
            while True:
                char = reader.read()
                if char == -1:
                    break
                string += chr(char)
            value = string

        return value


class _GBase8sConnectArgs:
    """连接参数构建（JayDeBeApi kwargs）

    本类是 mixin，``name`` 与 ``driver`` 由最终的方言类提供，此处仅作类型声明。
    """

    name: str
    driver: str

    def create_connect_args(self, url: Any) -> Tuple[Tuple, dict]:
        """创建连接参数

        Args:
            url: SQLAlchemy URL 对象

        Returns:
            空位置参数与 jaydebeapi 关键字参数字典
        """
        url_obj = make_url(url)

        kwargs = {
            "jclassname": self.driver,
            "url": self._build_jdbc_url(url_obj),
            "driver_args": self._build_connect_args(url_obj),
        }

        self._handle_jar_path(kwargs)

        return (), kwargs

    def _build_jdbc_url(self, url_obj: Any) -> str:
        """构建 JDBC 连接 URL 字符串

        Args:
            url_obj: SQLAlchemy URL 对象

        Returns:
            GBase 8s JDBC 连接 URL
        """
        jdbc_url_parts = ["jdbc:%s://%s" % (self.name, url_obj.host)]
        if url_obj.port:
            jdbc_url_parts.append(":%s" % url_obj.port)
        if url_obj.database:
            jdbc_url_parts.append("/%s" % url_obj.database)
        return "".join(jdbc_url_parts)

    def _build_connect_args(self, url_obj: Any) -> dict:
        """构建数据库连接参数字典

        Args:
            url_obj: SQLAlchemy URL 对象

        Returns:
            连接参数字典（用户/密码 + URL 查询参数 + 批处理改写开关）
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

        按优先级查找 GBase JDBC 驱动 JAR 文件：
        1. 环境变量 ``GBASE8S_JDBC_JARPATH``
        2. 用户配置目录 ``jars`` 下的 ``*gbase*.jar`` 文件
        未找到时告警并把 ``jars`` 置为 None。

        Args:
            kwargs: 连接参数字典
        """
        jar_path = provision.find_jdbc_jar()

        if isinstance(jar_path, str):
            kwargs["jars"] = jar_path
            return

        warnings.warn(
            "GBase 8s JDBC驱动jar文件未找到。\n"
            "当前搜索路径: %s\n"
            "解决方案:\n"
            "1. 设置环境变量: GBASE8S_JDBC_JARPATH=/path/to/gbasedbtjdbc_xxx.jar\n"
            "2. 将jar文件放置在默认目录: %s"
            % (os.environ.get(provision.JAR_ENV_VAR), provision.jar_search_dir()),
            UserWarning,
        )
        kwargs["jars"] = None


class _GBase8sConnectionHooks:
    """连接生命周期钩子（回滚与断连识别）"""

    def do_rollback(self, dbapi_connection: Any) -> None:
        """执行回滚操作

        jaydebeapi 的 ``Connection.rollback()`` 直通 JDBC ``Connection.rollback()``；
        对不支持回滚的匿名/异常连接，捕获异常并降级，避免连接池归还时报错。

        Args:
            dbapi_connection: 数据库 API 连接对象
        """
        try:
            dbapi_connection.rollback()
        except Exception as err:  # pylint: disable=broad-except
            logger.warning("GBase 8s 连接回滚失败，已忽略: %s", err)

    def is_disconnect(self, e: Exception, connection: Any, cursor: Any) -> bool:
        """检查异常是否为连接断开错误

        Args:
            e: 异常对象
            connection: 连接对象
            cursor: 游标对象

        Returns:
            bool: 是连接断开错误时返回 True
        """
        error_str = str(e).lower()
        disconnect_indicators = [
            "connection closed",
            "socket closed",
            "broken pipe",
            "connection reset",
            "jdbc connection",
            "network error",
            "sqlnontransientconnectionexception",
        ]

        return any(indicator in error_str for indicator in disconnect_indicators)


# pylint: disable=too-many-ancestors
class GBase8sDialect_jaydebeapi(
    _GBase8sConnectArgs, _GBase8sConnectionHooks, GBase8sDialect
):
    """GBase 8s 方言（JayDeBeApi 驱动） (GBase8s JayDeBeApi Dialect)

    通过 jaydebeapi + GBase JDBC 驱动连接 GBase 8s 数据库，
    与 ``pyproject.toml`` 中 ``jdbcgbase8s`` 入口点及注册名保持一致。

    Example:
        >>> from sqlalchemy import create_engine
        >>> engine = create_engine(
        ...     "jdbcgbase8s://username:password@host:port/database"
        ... )
    """

    driver = "com.gbasedbt.jdbc.Driver"

    @classmethod
    def import_dbapi(cls) -> DBAPIModule:
        """导入数据库 API 模块

        Returns:
            导入的 jaydebeapi 模块
        """
        return __import__("jaydebeapi")


dialect = GBase8sDialect_jaydebeapi