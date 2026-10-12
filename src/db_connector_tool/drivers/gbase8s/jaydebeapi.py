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

import functools
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ...utils.logging_utils import get_logger
from ...utils.path_utils import PathHelper
from . import jar_downloader
from .base import GBase8sDialect, normalize_sqlmode

logger = get_logger(__name__)

# pylint: disable=invalid-name
# ``GBase8sDialect_jaydebeapi`` / ``dialect`` 沿用 SQLAlchemy 各 DBAPI 方言模块的
# 命名习惯（如 ``OracleDialect_cx_oracle``），便于与框架约定保持一致。
# pylint: disable=too-many-ancestors,abstract-method
# 方言由 4 个反射 mixin 与 DefaultDialect 组合而成，仅实现 GBase 8s 用到的接口。

# GBase 8s JDBC 驱动主类、URL 子协议与默认端口
JDBC_DRIVER_CLASS = "com.gbasedbt.jdbc.Driver"
JDBC_SUBPROTOCOL = "gbasedbt-sqli"
DEFAULT_PORT = 9088

# 自动下载的落盘目录：用户配置目录下的 jars（Windows 为 %APPDATA%\db_connector_tool\jars）
AUTO_DOWNLOAD_APP_NAME = "db_connector_tool"
AUTO_DOWNLOAD_SUBDIR = "jars"

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

# JPype 启动 JVM 时会调用 ``java.lang.System::load`` 加载自身原生库，
# 而 JDK 24 起该受限方法默认只发警告（JEP 472），JDK 后续版本将直接拒绝。
# 这里在启动 JVM 前补上显式授权，既消除 4 行 WARNING，也为将来做好准备。
NATIVE_ACCESS_FLAG = "--enable-native-access=ALL-UNNAMED"

# 关闭上述注入的逃生口（个别环境若因该参数启动失败可置 0）
NATIVE_ACCESS_ENV = "GBASEDBT_JDBC_NATIVE_ACCESS"

# 视作「关闭」的取值（与 jar_downloader 保持一致）
_FALSY_VALUES = {"0", "false", "no", "off", ""}

# 原生访问授权是否已注入（``startJVM`` 只包装一次）
_NATIVE_ACCESS_INSTALLED = False


def auto_download_dir() -> Path:
    """返回自动下载的落盘目录（用户配置目录下的 jars）

    Returns:
        Path: 例如 Windows 上的 ``%APPDATA%\\db_connector_tool\\jars``

    Example:
        >>> auto_download_dir().name
        'jars'
    """
    return Path(PathHelper.get_user_config_dir(AUTO_DOWNLOAD_APP_NAME)) / (
        AUTO_DOWNLOAD_SUBDIR
    )


def _jar_search_dirs() -> List[str]:
    """返回 JDBC 驱动 jar 的候选搜索目录

    **只搜索两个目录**（按顺序）：

    1. 程序运行目录下的 ``jars``（``./jars``，便于随程序分发）；
    2. 用户配置目录下的 ``jars``（Windows 为 ``%APPDATA%\\db_connector_tool\\jars``，
       也是自动下载的落盘位置）。

    显式指定路径请用连接配置的 ``jars`` 参数或环境变量
    ``GBASEDBT_JDBC_JAR`` / ``GBASE8S_JDBC_JAR``；这两者优先级高于目录搜索。
    """
    return [
        os.path.join(os.getcwd(), "jars"),
        str(auto_download_dir()),
    ]


def _is_gbase_jar(filename: str) -> bool:
    """判断文件名是否为 GBase JDBC 驱动 jar

    识别两类命名：

    * 含 gbasedbt / gbase 关键字（厂商原始名与自动下载名）；
    * Maven 规范名 ``jdbc-<数字>.jar``（``com.gbasedbt:jdbc`` 产物）。
    """
    lowered = filename.lower()
    if not lowered.endswith(".jar"):
        return False
    if any(keyword in lowered for keyword in _JAR_NAME_KEYWORDS):
        return True
    return bool(jar_downloader.MAVEN_JAR_NAME_PATTERN.match(filename))


def _looks_like_jar(path: str) -> bool:
    """校验文件头是否为 ZIP（jar），用于跳过损坏/半截文件

    只读前 4 个字节，开销可忽略；坏文件被跳过而不是交给 JDBC 报出难懂的错。
    """
    try:
        with open(path, "rb") as handle:
            return handle.read(4) == b"PK\x03\x04"
    except OSError:
        return False


def _first_jar_in(directory: str) -> Optional[str]:
    """返回目录中版本最高的**完好** GBase JDBC jar 的完整路径

    同一目录可能存在多个版本（例如自动下载的新版本与用户手工放置的旧版本），
    这里按``文件名中的数字段``取最高版本；损坏的文件（文件头不是 ZIP）会被
    跳过，从而回退到其他候选或触发重新下载。

    数字段相同时**默认包优先于 ``_nobson`` 变体**：两者版本号数字段一致
    （``3.6.5.21`` / ``3.6.5.21_nobson``），而变体缺少 ``org.bson.*``，
    若被排到前面会出现「装了却用不了」的缺类问题。
    """
    candidates: List[str] = []
    try:
        if not os.path.isdir(directory):
            return None
        for name in os.listdir(directory):
            if _is_gbase_jar(name):
                candidates.append(os.path.join(directory, name))
    except OSError:
        return None

    if not candidates:
        return None

    ordered = sorted(
        candidates,
        key=lambda path: jar_downloader.version_rank(os.path.basename(path)),
        reverse=True,
    )
    for candidate in ordered:
        if _looks_like_jar(candidate):
            return candidate
        logger.warning("跳过损坏的驱动文件（文件头不是 jar）: %s", candidate)
    return None


def _native_access_enabled() -> bool:
    """是否注入 JVM 原生访问授权（默认注入）

    Example:
        >>> os.environ[NATIVE_ACCESS_ENV] = "0"
        >>> _native_access_enabled()
        False
    """
    raw = os.environ.get(NATIVE_ACCESS_ENV)
    if raw is None:
        return True
    return raw.strip().lower() not in _FALSY_VALUES


def _install_native_access_flag() -> bool:
    """为 ``jpype.startJVM`` 补上 ``--enable-native-access``（幂等）

    JayDeBeApi 在内部自行调用 :func:`jpype.startJVM`，且不对外暴露任何 JVM
    参数入口，因此只能在此处包一层：真正启动 JVM 时若参数里还没有该开关就补上。

    该开关只对**首次**启动 JVM 有效。若 JVM 已被其他代码抢先启动，则无从生效
    （此时警告已输出），但本函数不会报错。

    兼容性：JDK 17+ 认识该参数；更早的 JDK 会视为未知参数，而 JayDeBeApi 调用
    ``startJVM`` 时带 ``ignoreUnrecognized=True``，未知参数会被忽略而非报错。

    Returns:
        bool: 本次是否完成包装（已包装过或显式关闭时返回 ``False``）

    Example:
        >>> _install_native_access_flag()
        True
    """
    global _NATIVE_ACCESS_INSTALLED  # pylint: disable=global-statement
    if _NATIVE_ACCESS_INSTALLED or not _native_access_enabled():
        return False
    try:
        import jpype  # pylint: disable=import-outside-toplevel
    except ImportError:  # pragma: no cover - 缺 jpype 时交由 jaydebeapi 报错
        return False

    original = jpype.startJVM

    @functools.wraps(original)
    def start_jvm_with_native_access(*args: Any, **kwargs: Any) -> Any:
        if not any(
            isinstance(arg, str) and arg.startswith("--enable-native-access")
            for arg in args
        ):
            args = (*args, NATIVE_ACCESS_FLAG)
        return original(*args, **kwargs)

    jpype.startJVM = start_jvm_with_native_access  # type: ignore[assignment]
    _NATIVE_ACCESS_INSTALLED = True
    logger.debug("已为 JVM 注入原生访问授权: %s", NATIVE_ACCESS_FLAG)
    return True


def find_jdbc_jar(explicit: Optional[str] = None) -> Optional[str]:
    """定位 GBase 8s 的 JDBC 驱动 jar

    查找顺序：显式配置（URL 的 ``jars`` 参数） → 环境变量
    （``GBASEDBT_JDBC_JAR`` / ``GBASE8S_JDBC_JAR``） → **两个目录**
    （程序运行目录 ``./jars``、用户配置目录 jars）。

    若仍然找不到，且未通过 ``GBASEDBT_JDBC_AUTO_DOWNLOAD`` 关闭自动下载，
    则从 Maven Central 拉取**最新版本**到用户配置目录下的 jars 并返回其路径；
    下载失败只记录告警并返回 ``None``（此时由 ``CLASSPATH`` 决定）。

    特例：目录里**只有** ``_nobson`` 变体时（该变体缺少 ``org.bson.*``，早期
    自动下载可能装过），会先尝试补下默认包；补下成功就用默认包，失败才回退到
    变体（并给出告警）。设置 ``GBASEDBT_JDBC_ALLOW_NOBSON=1`` 可跳过这个补下。

    Args:
        explicit: 通过 URL 查询参数 ``jars`` 显式指定的 jar 路径

    Returns:
        Optional[str]: jar 文件路径；未找到时返回 ``None``

    Example:
        >>> find_jdbc_jar()
        'C:\\\\dbct\\\\jars\\\\gbasedbt-jdbc-3.6.5.21.jar'
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

    local_jar: Optional[str] = None
    for directory in _jar_search_dirs():
        found = _first_jar_in(directory)
        if found:
            local_jar = found
            break

    # 只有 _nobson 变体时，先试着补下默认包（同版本默认包会排在变体之前）
    needs_plain = (
        local_jar is not None
        and jar_downloader.is_nobson_variant(os.path.basename(local_jar))
        and not jar_downloader.allow_nobson_variant()
    )
    if local_jar is not None and not needs_plain:
        return local_jar

    if needs_plain:
        logger.warning(
            "本地只有 _nobson 变体驱动（%s），该变体缺少 org.bson.*，"
            "尝试从 Maven 获取同版本默认包",
            local_jar,
        )

    downloaded = jar_downloader.download_latest_jar(auto_download_dir())
    if downloaded:
        return str(downloaded)
    return local_jar


class GBase8sDialect_jaydebeapi(GBase8sDialect):  # NOSONAR
    """GBase 8s 的 JayDeBeApi 方言实现"""

    driver = "jaydebeapi"

    # SQLAlchemy 仅在方言类自身的 ``__dict__`` 中查找该属性（不追溯基类），
    # 因此必须在具体 DBAPI 方言类上重复声明，否则会触发 cprf 警告。
    supports_statement_cache = True

    @classmethod
    def import_dbapi(cls) -> Any:
        """返回 JayDeBeApi 模块作为 DBAPI 2.0 实现

        顺带安装 JVM 原生访问授权（见 :func:`_install_native_access_flag`）：
        该函数在 SQLAlchemy 建引擎时即被调用，早于任何一次 JDBC 连接，
        因此能保证参数在 JVM 启动前就位。
        """
        import jaydebeapi  # pylint: disable=import-outside-toplevel

        _install_native_access_flag()
        return jaydebeapi

    def create_connect_args(self, url: Any) -> Tuple[list, dict]:
        """把 SQLAlchemy URL 翻译为 ``jaydebeapi.connect`` 的位置参数"""
        # 兜底：绕过 create_engine 直接构造方言时也能补上授权（幂等）
        _install_native_access_flag()
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
