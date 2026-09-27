"""GBase 8s 类型体系 (GBase8s Types)

对齐 SQLAlchemy 官方 Oracle 方言的 ``types.py`` 组织方式，集中定义：

- ``ischema_names``：系统目录 ``syscolumns.coltype`` 到 SQLAlchemy 类型的映射，键为**数字类型码**
  （GBase 8s 沿用 Informix 血统的类型码，与官方 Oracle 方言的字符串键必然不同）；
- ``colspecs``：在 Oracle 基类 colspecs 之上叠加 GBase 8s 的时间戳与字符串映射；
- ``ObTimestamp``：``TIMESTAMP`` 的双向转换装饰器（绑定为 ``java.sql.Timestamp``）；
- ``LVARCHAR`` / ``SERIAL`` / ``SERIAL8``：GBase 8s 专属类型。

Example:
>>> from db_connector_tool.drivers.gbase8s.types import ischema_names
>>> ischema_names[43].__name__
'LVARCHAR'
"""

from datetime import datetime
from typing import Any

import jpype
from dateutil import parser
from sqlalchemy import CHAR, TIMESTAMP, VARCHAR, TypeDecorator, util
from sqlalchemy.dialects.oracle.base import OracleDialect
from sqlalchemy.sql import sqltypes


# pylint: disable=too-many-ancestors
class ObTimestamp(TypeDecorator):
    """GBase 8s 时间戳类型装饰器 (GBase8s Timestamp Decorator)

    处理 GBase 8s 时间戳类型与 Python datetime 之间的双向转换。

    Example:
        >>> from sqlalchemy import Column
        >>> col = Column("created_at", ObTimestamp())
    """

    impl = TIMESTAMP

    @property
    def python_type(self) -> type:
        """返回此类型对应的 Python 类型

        Returns:
            type: datetime 类型
        """
        return datetime

    def process_bind_param(self, value: Any, dialect: Any) -> Any:
        """处理绑定参数

        Args:
            value: 输入值
            dialect: SQLAlchemy 方言对象

        Returns:
            转换后的 java.sql.Timestamp 对象
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
        """
        if value is None:
            return "NULL"
        if isinstance(value, datetime):
            text = value.strftime("%Y-%m-%d %H:%M:%S.%f")
            return "TIMESTAMP '%s'" % text
        return str(value)

    def process_result_value(self, value: Any, dialect: Any) -> datetime | None:
        """处理结果值

        Args:
            value: 数据库返回的值
            dialect: SQLAlchemy 方言对象

        Returns:
            解析后的 datetime 对象，值为 None 时返回 None
        """
        if value is not None:
            return parser.parse(value)
        return None


class LVARCHAR(sqltypes.String):
    """GBase 8s 变长字符串类型 LVARCHAR"""

    __visit_name__ = "LVARCHAR"


class SERIAL(sqltypes.Integer):
    """GBase 8s 自增整数类型 SERIAL"""

    __visit_name__ = "SERIAL"


class SERIAL8(sqltypes.BigInteger):
    """GBase 8s 自增 8 字节整数类型 SERIAL8"""

    __visit_name__ = "SERIAL8"


# syscolumns.coltype 低字节的类型码映射（coltype // 256 == 1 表示 NOT NULL）
ischema_names: dict[int, type] = {
    0: sqltypes.CHAR,  # CHAR
    1: sqltypes.SMALLINT,  # SMALLINT
    2: sqltypes.INTEGER,  # INT
    3: sqltypes.FLOAT,  # FLOAT
    4: sqltypes.FLOAT,  # SMALLFLOAT
    5: sqltypes.Numeric,  # DECIMAL（collength 编码精度/小数位）
    6: SERIAL,  # SERIAL（自增）
    7: sqltypes.DATE,  # DATE
    8: sqltypes.Numeric,  # MONEY
    10: sqltypes.TIMESTAMP,  # DATETIME
    11: sqltypes.LargeBinary,  # BYTE
    12: sqltypes.TEXT,  # TEXT
    13: sqltypes.VARCHAR,  # VARCHAR
    14: sqltypes.Interval,  # INTERVAL
    15: sqltypes.NCHAR,  # NCHAR
    16: sqltypes.NVARCHAR,  # NVARCHAR
    17: sqltypes.BIGINT,  # INT8
    18: SERIAL8,  # SERIAL8（自增）
    43: LVARCHAR,  # LVARCHAR
    52: sqltypes.BIGINT,  # BIGINT
    65: sqltypes.TIMESTAMP,  # TIMESTAMP
}

# 自增类型码（反射时置 autoincrement=True）
AUTOINCREMENT_TYPE_CODES = frozenset({6, 18})

colspecs = util.update_copy(
    dict(OracleDialect.colspecs),
    {
        TIMESTAMP: ObTimestamp,
        VARCHAR: VARCHAR,
        CHAR: CHAR,
    },
)