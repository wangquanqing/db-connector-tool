"""GBase 8s SQLAlchemy 方言（JayDeBeApi + JDBC）

通过 JayDeBeApi（JPype + JDBC）连接 GBase 8s，原生支持一库三模
（``oracle`` / ``gbase`` / ``mysql``），并以 ``gbase8s`` 方言名注册到 SQLAlchemy：

Example:
    >>> from sqlalchemy import create_engine, text
    >>> engine = create_engine(
    ...     "gbase8s://127.0.0.1:9088/db_station:GBASEDBTSERVER=gbase01"
    ...     "?user=cvicse&password=secret&sqlmode=oracle"
    ... )
    >>> with engine.connect() as conn:
    ...     print(conn.execute(text("SELECT 1")).scalar())
"""

from sqlalchemy.dialects import registry

# pylint: disable=invalid-name
# ``dialect`` 是 SQLAlchemy 方言模块约定的模块级实例别名。
from . import jaydebeapi
from .base import DEFAULT_SQLMODE, SUPPORTED_SQLMODES, GBase8sDialect, normalize_sqlmode
from .jaydebeapi import GBase8sDialect_jaydebeapi
from .types import LVARCHAR, MONEY, SERIAL, SERIAL8

dialect = jaydebeapi.dialect


# 程序化注册：即使包不是通过 entry point 安装（例如直接引用源码目录）也能使用，
# 且注册项优先于 entry point，便于开发调试。
def _register():
    registry.register("gbase8s", __name__ + ".jaydebeapi", "GBase8sDialect_jaydebeapi")
    registry.register(
        "gbase8s.jaydebeapi",
        __name__ + ".jaydebeapi",
        "GBase8sDialect_jaydebeapi",
    )


_register()

__all__ = (
    "GBase8sDialect",
    "GBase8sDialect_jaydebeapi",
    "LVARCHAR",
    "MONEY",
    "SERIAL",
    "SERIAL8",
    "DEFAULT_SQLMODE",
    "SUPPORTED_SQLMODES",
    "normalize_sqlmode",
    "dialect",
)
