"""GBase 8s 数据类型定义

GBase 8s 沿用 Informix 的类型体系，``syscolumns.coltype`` 使用数字类型号，
因此本模块以“类型号 -> SQLAlchemy 类型”的形式维护映射表，
供数据字典反射使用（见 :mod:`dictionary`）。
"""

from __future__ import annotations

from typing import Any, Dict, Type

from sqlalchemy import types as sqltypes

# pylint: disable=abstract-method
# ``TypeEngine._with_collation`` 仅在 ``String`` 中实现，派生自 Numeric/Integer 的
# 类型会被 pylint 误判为未实现抽象方法，故在此关闭该检查。


class LVARCHAR(sqltypes.String):
    """GBase 8s 的 LVARCHAR 变长字符串类型

    该类型可突破普通 VARCHAR 255 字节的长度限制，
    在 DDL 中直接渲染为 ``LVARCHAR``。
    """

    __visit_name__ = "LVARCHAR"


class MONEY(sqltypes.Numeric):
    """GBase 8s 的 MONEY 货币类型（定点数）"""

    __visit_name__ = "MONEY"


class SERIAL(sqltypes.Integer):
    """GBase 8s 的 SERIAL 自增整型"""

    __visit_name__ = "SERIAL"


class SERIAL8(sqltypes.BigInteger):
    """GBase 8s 的 SERIAL8 自增大整型"""

    __visit_name__ = "SERIAL8"


# syscolumns.coltype 低 8 位的类型号 -> SQLAlchemy 类型
ischema_names: Dict[int, Type[sqltypes.TypeEngine]] = {
    0: sqltypes.CHAR,
    1: sqltypes.SMALLINT,
    2: sqltypes.INTEGER,
    3: sqltypes.FLOAT,
    4: sqltypes.Float,
    5: sqltypes.DECIMAL,
    6: SERIAL,
    7: sqltypes.DATE,
    8: MONEY,
    10: sqltypes.DATETIME,
    11: sqltypes.LargeBinary,  # BYTE
    12: sqltypes.TEXT,
    13: sqltypes.VARCHAR,
    14: sqltypes.Interval,
    15: sqltypes.NCHAR,
    16: sqltypes.NVARCHAR,
    17: sqltypes.BIGINT,  # INT8
    18: SERIAL8,
    40: LVARCHAR,
    41: sqltypes.TEXT,  # CLOB / BLOB / BOOLEAN 共用，具体区分见 dictionary._EXTENDED_TYPE_MAP
    43: LVARCHAR,
    45: sqltypes.LargeBinary,  # BLOB
    46: sqltypes.TEXT,  # CLOB
    52: sqltypes.BIGINT,  # BIGINT（SQL 标准写法，等价于 INT8=17）
    53: SERIAL8,  # BIGSERIAL（SQL 标准写法，等价于 SERIAL8=18）
    65: sqltypes.TIMESTAMP,  # TIMESTAMP
}

# 通用类型 -> GBase 8s 方言实现的映射。
# JayDeBeApi/JDBC 已完成大部分 Python 值转换，因此这里只保留 DDL 渲染层面的差异。
colspecs: Dict[Type[Any], Type[sqltypes.TypeEngine]] = {}
