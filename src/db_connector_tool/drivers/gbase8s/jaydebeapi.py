"""GBase 8s 的 JayDeBeApi（JPype + JDBC）连接层

本模块把 SQLAlchemy 连接 URL 翻译为 :func:`jaydebeapi.connect` 的调用参数，
并处理 JDBC 驱动 jar 定位、断开连接判定与事务回滚等 DBAPI 细节，
从而绕开 gbase8sdb / DbtPy 对 GBase 8s 版本的依赖，获得更好的版本兼容性。

连接 URL 形式（由 :class:`SQLAlchemyDriver` 的 ``gbase8s`` 配置生成）::

    gbase8s://host:9088/dbname:GBASEDBTSERVER=server?user=u&password=p&sqlmode=mysql

其中 URL 路径中 ``:`` 之后的部分为 JDBC 连接属性（``key=value``），
查询串中除 ``user``/``password``/``sqlmode``/``jars`` 外的键也会追加为 JDBC 属性。
"""

from __future__ import annotations

import os
from typing import Any, Dict, List, Optional, Tuple

from .base import GBase8sDialect, normalize_sqlmode

# pylint: disable=invalid-name
# ``GBase8sDialect_jaydebeapi`` / ``dialect`` 沿用 SQLAlchemy 各 DBAPI 方言模块的
# 命名习惯（如 ``OracleDialect_cx_oracle``），便于与框架约定保持一致。
# pylint: disable=too-many-ancestors,abstract-method
# 方言由 4 个反射 mixin 与 DefaultDialect 组合而成，仅实现 GBase 8s 用到的接口。

# GBase 8s JDBC 驱动主类、URL 子协议与默认端口
JDBC_DRIVER_CLASS = "com.gbasedbt.jdbc.Driver"
JDBC_SUBPROTOCOL = "gbasedbt-sqli"
DEFAULT_PORT = 9088

# 方言级查询参数，不会被写入 JDBC 连接属性
_DIALECT_QUERY_KEYS = ("user", "password", "sqlmode", "jars")

# 定位 JDBC 驱动 jar 的环境变量
_JAR_ENV_VARS = ("GBASEDBT_JDBC_JAR", "GBASE8S_JDBC_JAR")

# jar 文件名关键字（不区分大小写）
_JAR_NAME_KEYWORDS = ("gbasedbt", "gbase")

# 断开连接的典型错误信息片段
_DISCONNECT_MARKERS = (
    "connection is closed",
    "connection closed",
    "connection has been closed",
    "connection was closed",
    "no current connection",
)


def _jar_search_dirs() -> List[str]:
    """返回 JDBC 驱动 jar 的候选搜索目录

    额外搜索路径可通过环境变量 ``GBASEDBT_JDBC_SEARCH_DIRS`` 以
    ``os.pathsep`` 分隔追加。
    """
    home = os.path.expanduser("~")
    dirs = [
        os.path.join(home, ".sqlalchemy-jdbcapi", "drivers"),
        os.path.join(home, "db_connector_tool", "jars"),
        os.path.join(os.getcwd(), "jars"),
    ]
    extra = os.environ.get("GBASEDBT_JDBC_SEARCH_DIRS", "")
    if extra:
        dirs.extend(extra.split(os.pathsep))
    return dirs


def _first_jar_in(directory: str) -> Optional[str]:
    """返回目录中第一个 GBase JDBC jar 的完整路径"""
    try:
        if not os.path.isdir(directory):
            return None
        for name in sorted(os.listdir(directory)):
            lowered = name.lower()
            if lowered.endswith(".jar") and any(
                keyword in lowered for keyword in _JAR_NAME_KEYWORDS
            ):
                return os.path.join(directory, name)
    except OSError:
        pass
    return None


def find_jdbc_jar(explicit: Optional[str] = None) -> Optional[str]:
    """定位 GBase 8s 的 JDBC 驱动 jar

    按“显式配置 -> 环境变量 -> 约定目录”的顺序查找。

    Args:
        explicit: 通过 URL 查询参数 ``jars`` 显式指定的 jar 路径

    Returns:
        Optional[str]: jar 文件路径；未找到时返回 ``None``，此时由 ``CLASSPATH`` 决定

    Example:
        >>> find_jdbc_jar()
        'C:\\\\Users\\\\me\\\\.sqlalchemy-jdbcapi\\\\drivers\\\\gbasedbt-jdbc-3.5.1.jar'
    """
    candidates = [explicit]
    candidates.extend(os.environ.get(name) for name in _JAR_ENV_VARS)
    for candidate in candidates:
        if candidate:
            try:
                if os.path.isfile(candidate):
                    return candidate
            except OSError:
                continue
    for directory in _jar_search_dirs():
        found = _first_jar_in(directory)
        if found:
            return found
    return None


class GBase8sDialect_jaydebeapi(GBase8sDialect):  # NOSONAR
    """GBase 8s 的 JayDeBeApi 方言实现"""

    driver = "jaydebeapi"

    # SQLAlchemy 仅在方言类自身的 ``__dict__`` 中查找该属性（不追溯基类），
    # 因此必须在具体 DBAPI 方言类上重复声明，否则会触发 cprf 警告。
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> Any:
        """返回 JayDeBeApi 模块作为 DBAPI 2.0 实现"""
        import jaydebeapi  # pylint: disable=import-outside-toplevel

        return jaydebeapi

    def create_connect_args(self, url: Any) -> Tuple[list, dict]:
        """把 SQLAlchemy URL 翻译为 ``jaydebeapi.connect`` 的位置参数"""
        if url.query.get("sqlmode"):
            self.sqlmode = normalize_sqlmode(url.query["sqlmode"])
        database, properties = self._parse_database(url.database)
        jdbc_url = (
            f"jdbc:{JDBC_SUBPROTOCOL}://{url.host or '127.0.0.1'}"
            f":{url.port or DEFAULT_PORT}/{database}"
        )
        jdbc_properties = self._jdbc_properties(url, properties)
        if jdbc_properties:
            jdbc_url = f"{jdbc_url}:{';'.join(jdbc_properties)}"
        driver_args = [
            url.query.get("user") or url.username or "",
            url.query.get("password") or url.password or "",
        ]
        jars = find_jdbc_jar(url.query.get("jars"))
        return ([JDBC_DRIVER_CLASS, jdbc_url, driver_args, jars], {})

    @staticmethod
    def _parse_database(database: Optional[str]) -> Tuple[str, Dict[str, str]]:
        """拆分 URL 中的库名与 JDBC 连接属性

        Args:
            database: URL 的 database 部分，形如 ``dbname:GBASEDBTSERVER=srv``

        Returns:
            Tuple[str, Dict[str, str]]: 库名与连接属性字典

        Example:
            >>> GBase8sDialect_jaydebeapi._parse_database("db:GBASEDBTSERVER=s1")
            ('db', {'GBASEDBTSERVER': 's1'})
        """
        if not database:
            return "", {}
        # JDBC 连接属性以 ``;`` 分隔，此处同时兼容 URL 中常用的 ``:`` 写法
        segments = str(database).replace(";", ":").split(":")
        properties: Dict[str, str] = {}
        for segment in segments[1:]:
            key, separator, value = segment.partition("=")
            if separator:
                properties[key.strip()] = value.strip()
        return segments[0], properties

    def _jdbc_properties(self, url: Any, extra: Dict[str, str]) -> List[str]:
        """汇总需要写入 JDBC 连接串的属性

        除 URL 路径中声明的属性外，``sqlmode`` 与查询串中的自定义键
        （如 ``CLIENT_LOCALE``）也会一并追加，保证一库三模的会话模式与
        编译器生成的 SQL 保持一致。
        """
        properties = dict(extra)
        properties["sqlmode"] = self.sqlmode
        for key, value in url.query.items():
            if key not in _DIALECT_QUERY_KEYS:
                properties[key] = value
        return [f"{key}={value}" for key, value in properties.items()]

    def on_connect(self) -> Any:
        """连接建立后关闭 JDBC 自动提交，交由 SQLAlchemy 管理事务"""

        def connect(dbapi_connection: Any) -> None:
            dbapi_connection.jconn.setAutoCommit(False)

        return connect

    def is_disconnect(self, e: Any, connection: Any, cursor: Any) -> bool:
        """判断异常是否表示连接已断开"""
        if isinstance(e, self.dbapi.Error) and any(  # type: ignore[union-attr]
            marker in str(e).lower() for marker in _DISCONNECT_MARKERS
        ):
            return True
        return bool(getattr(connection, "_closed", False))

    def do_rollback(self, dbapi_connection: Any) -> None:
        """回滚事务；连接已关闭时直接返回（JayDeBeApi 关闭后回滚会抛异常）"""
        if getattr(dbapi_connection, "_closed", False):
            return
        try:
            dbapi_connection.rollback()
        except self.dbapi.Error:  # type: ignore[union-attr]
            pass


# 供方言注册机制识别
dialect = GBase8sDialect_jaydebeapi
