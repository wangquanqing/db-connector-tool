"""GBase 8s（JDBC/JPype）返回值类型归一化

JayDeBeApi 自带的取值转换器有两个问题，都会污染上层拿到的数据：

1. **精度丢失**：``DECIMAL``/``NUMERIC`` 走 :func:`_java_to_py_bigdecimal`，
   只要 ``scale != 0`` 就调 ``doubleValue()``，把 ``BigDecimal`` 降级为 float。
   ``12345678901234567.89`` 会变成 ``1.2345678901234568e+16``。
2. **类型不统一**：日期/时间戳被转成 ``str``，而整数/浮点则原样返回 JPype 包装的
   Java 对象（``java.lang.Integer`` / ``JDouble``），与其它数据库驱动返回的
   Python 原生类型（``int`` / ``Decimal`` / ``datetime``）不一致。

此外 JayDeBeApi 的转换器表只覆盖了部分 ``java.sql.Types``：``BIGINT``、``REAL``、
``VARCHAR``、``BLOB`` 等未注册，会走 :func:`_unknownSqlTypeConverter` 直接返回
Java 对象。

本模块在**连接建立后**用归一化版本替换这些转换器（见 :func:`install_converters`），
使 GBase 8s 的返回值与其它方言保持一致：

===========================  ==========================================
JDBC 类型                    归一化后的 Python 类型
===========================  ==========================================
INTEGER / SMALLINT / TINYINT  ``int``
BIGINT                       ``int``
DECIMAL / NUMERIC            ``decimal.Decimal``（精确保留）
DOUBLE / FLOAT / REAL        ``float``
CHAR / VARCHAR / LONGVARCHAR  ``str``
DATE                         ``datetime.date``
TIME                         ``datetime.time``
TIMESTAMP                    ``datetime.datetime``（微秒精确）
BINARY / VARBINARY / BLOB    ``bytes``
BOOLEAN / BIT                ``bool``
NULL                         ``None``
===========================  ==========================================
"""

from __future__ import annotations

import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Dict, Optional

from ...utils.logging_utils import get_logger

logger = get_logger(__name__)

# 归一化后的转换器签名：(resultset, column_index) -> Python 值
Converter = Callable[[Any, int], Any]


def _nanos_to_microseconds(nanos: int) -> int:
    """把 JDBC 的纳秒部分换算为微秒

    JayDeBeApi 原实现是 ``int(str(nanos)[:6])``，当纳秒不足 9 位时结果偏大
    （如 ``1010000`` ns 应为 ``1010`` µs，却算出 ``101000``）。这里改为整除。

    Args:
        nanos: ``Timestamp.getNanos()`` 的返回值（0 ~ 999999999）

    Returns:
        int: 微秒数（0 ~ 999999）

    Example:
        >>> _nanos_to_microseconds(1010000)
        1010
        >>> _nanos_to_microseconds(123456789)
        123456
    """
    return int(nanos) // 1000


def _to_decimal(rs: Any, col: int) -> Optional[Decimal]:
    """``DECIMAL``/``NUMERIC`` -> :class:`~decimal.Decimal`（精确）

    通过 ``toPlainString()`` 取十进制字面量再构造 ``Decimal``，避免任何二进制
    浮点中转，从而保留全部有效位。

    Example:
        >>> # DECIMAL(19,2) 的 12345678901234567.89 不再变成 float
    """
    value = rs.getObject(col)
    if value is None:
        return None
    try:
        return Decimal(value.toPlainString())
    except (AttributeError, InvalidOperation, ValueError):
        # 非 BigDecimal（部分驱动会直接给数值），退回字符串构造
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            return None


def _to_int(rs: Any, col: int) -> Optional[int]:
    """整数类型 -> :class:`int`（兼容 Java 的 Integer/Long/Short/BigInteger）"""
    value = rs.getObject(col)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        # 超出 int64 的 BigInteger：走字符串转换避免溢出
        return int(str(value))


def _to_float(rs: Any, col: int) -> Optional[float]:
    """浮点类型 -> :class:`float`"""
    value = rs.getObject(col)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(value.doubleValue())


def _to_str(rs: Any, col: int) -> Optional[str]:
    """字符串类型 -> :class:`str`"""
    value = rs.getObject(col)
    return None if value is None else str(value)


def _to_bool(rs: Any, col: int) -> Optional[bool]:
    """``BOOLEAN``/``BIT`` -> :class:`bool`"""
    value = rs.getObject(col)
    if value is None:
        return None
    try:
        return bool(value)
    except (TypeError, ValueError):
        return bool(value.booleanValue())


def _to_date(rs: Any, col: int) -> Optional[datetime.date]:
    """``DATE`` -> :class:`datetime.date`

    用 ``getDate()`` 拿到 ``java.sql.Date``，其 ``str()`` 为 ``YYYY-MM-DD``，
    再交给 ``fromisoformat`` 解析（避开 JayDeBeApi 里对 1900 年前日期的限制）。
    """
    value = rs.getDate(col)
    if value is None:
        return None
    text = str(value)
    try:
        return datetime.date.fromisoformat(text[:10])
    except ValueError:
        return None


def _to_time(rs: Any, col: int) -> Optional[datetime.time]:
    """``TIME`` -> :class:`datetime.time`"""
    value = rs.getTime(col)
    if value is None:
        return None
    text = str(value)
    try:
        return datetime.time.fromisoformat(text[:8])
    except ValueError:
        return None


def _to_datetime(rs: Any, col: int) -> Optional[datetime.datetime]:
    """``TIMESTAMP`` -> :class:`datetime.datetime`（含微秒）

    纳秒经 :func:`_nanos_to_microseconds` 换算，不再使用 JayDeBeApi 的
    ``int(str(nanos)[:6])``（该算法在纳秒不足 9 位时会放大 100 倍）。
    """
    value = rs.getTimestamp(col)
    if value is None:
        return None
    text = str(value)
    try:
        parsed = datetime.datetime.fromisoformat(text[:19])
    except ValueError:
        return None
    return parsed.replace(microsecond=_nanos_to_microseconds(value.getNanos()))


def _to_binary(rs: Any, col: int) -> Optional[bytes]:
    """``BINARY``/``VARBINARY``/``BLOB`` -> :class:`bytes`"""
    value = rs.getBytes(col)
    if value is None:
        return None
    # JPype 的 byte[] 转成 Python bytes；驱动返回字符串时按 latin-1 保字节
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return str(value).encode("latin-1", "replace")


# JDBC 类型名 -> 归一化后对应的 Python 类型名（供列元数据展示）
PYTHON_TYPE_NAMES: Dict[str, str] = {
    "TINYINT": "int",
    "SMALLINT": "int",
    "INTEGER": "int",
    "INT": "int",
    "SERIAL": "int",
    "BIGINT": "int",
    "INT8": "int",
    "SERIAL8": "int",
    "BIGSERIAL": "int",
    "DECIMAL": "Decimal",
    "NUMERIC": "Decimal",
    "MONEY": "Decimal",
    "DOUBLE": "float",
    "FLOAT": "float",
    "REAL": "float",
    "SMALLFLOAT": "float",
    "CHAR": "str",
    "VARCHAR": "str",
    "NCHAR": "str",
    "NVARCHAR": "str",
    "LONGVARCHAR": "str",
    "LVARCHAR": "str",
    "TEXT": "str",
    "CLOB": "str",
    "BOOLEAN": "bool",
    "BIT": "bool",
    "DATE": "date",
    "TIME": "time",
    "TIMESTAMP": "datetime",
    "BINARY": "bytes",
    "VARBINARY": "bytes",
    "LONGVARBINARY": "bytes",
    "BYTE": "bytes",
    "BLOB": "bytes",
}

# GBase 8s 把时间戳报成 ``DATETIME YEAR TO FRACTION(3)`` 这类 Informix 语法，
# 需要按前缀归一化后再查 PYTHON_TYPE_NAMES。
_TYPE_NAME_PREFIXES: tuple = (
    ("DATETIME", "datetime"),
    ("TIMESTAMP", "datetime"),
    ("INTERVAL", "str"),
    ("VARCHAR", "str"),
    ("LVARCHAR", "str"),
    ("NVARCHAR", "str"),
    ("DECIMAL", "Decimal"),
    ("NUMERIC", "Decimal"),
    ("MONEY", "Decimal"),
    ("INT8", "int"),
    ("SERIAL8", "int"),
    ("SMALLINT", "int"),
    ("INTEGER", "int"),
    ("INT", "int"),
    ("BIGINT", "int"),
    ("FLOAT", "float"),
    ("SMALLFLOAT", "float"),
    ("DOUBLE", "float"),
    ("REAL", "float"),
    ("CHAR", "str"),
    ("TEXT", "str"),
    ("CLOB", "str"),
    ("BLOB", "bytes"),
    ("BYTE", "bytes"),
    ("DATE", "date"),
    ("TIME", "time"),
    ("BOOLEAN", "bool"),
)


def python_type_for(type_name: Optional[str]) -> Optional[str]:
    """把 JDBC 类型名映射为归一化后的 Python 类型名

    先查精确表，再按前缀匹配——GBase 8s 会把时间戳报成
    ``DATETIME YEAR TO FRACTION(3)``、把整型报成 ``INT8``。

    Args:
        type_name: ``ResultSetMetaData.getColumnTypeName()`` 的返回值

    Returns:
        Optional[str]: 如 ``'int'`` / ``'Decimal'`` / ``'datetime'``；无法识别时为 ``None``

    Example:
        >>> python_type_for("INT8")
        'int'
        >>> python_type_for("DATETIME YEAR TO FRACTION(3)")
        'datetime'
        >>> python_type_for("WEIRD") is None
        True
    """
    if not type_name:
        return None
    upper = type_name.strip().upper()
    if upper in PYTHON_TYPE_NAMES:
        return PYTHON_TYPE_NAMES[upper]
    for prefix, python_name in _TYPE_NAME_PREFIXES:
        if upper.startswith(prefix):
            return python_name
    return None


# 未覆盖类型的兜底：原样返回 ``getObject()`` 结果
def _to_object(rs: Any, col: int) -> Any:
    """未覆盖类型的兜底：原样返回 ``getObject()`` 结果"""
    return rs.getObject(col)


# JDBC 类型名 -> 归一化转换器。键名对应 ``java.sql.Types`` 的字段名。
CONVERTERS: Dict[str, Converter] = {
    "TINYINT": _to_int,
    "SMALLINT": _to_int,
    "INTEGER": _to_int,
    "BIGINT": _to_int,
    "DECIMAL": _to_decimal,
    "NUMERIC": _to_decimal,
    "DOUBLE": _to_float,
    "FLOAT": _to_float,
    "REAL": _to_float,
    "CHAR": _to_str,
    "VARCHAR": _to_str,
    "NCHAR": _to_str,
    "NVARCHAR": _to_str,
    "LONGVARCHAR": _to_str,
    "CLOB": _to_str,
    "BOOLEAN": _to_bool,
    "BIT": _to_bool,
    "DATE": _to_date,
    "TIME": _to_time,
    "TIMESTAMP": _to_datetime,
    "BINARY": _to_binary,
    "VARBINARY": _to_binary,
    "LONGVARBINARY": _to_binary,
    "BLOB": _to_binary,
}


def build_converter_map(types_module: Any) -> Dict[int, Converter]:
    """把 :data:`CONVERTERS` 的键解析为 ``java.sql.Types`` 常量值

    ``java.sql.Types`` 的常量名在 ``types_module`` 上以属性形式暴露，
    取值即为 ``cursor.description`` / ``ResultSetMetaData.getColumnType()``
    返回的数字类型号。

    Args:
        types_module: ``jpype.java.sql.Types``（或等价的常量容器）

    Returns:
        Dict[int, Converter]: 类型号 -> 转换器；未识别的常量名会被跳过

    Example:
        >>> len(build_converter_map(jpype.java.sql.Types)) > 20
        True
    """
    resolved: Dict[int, Converter] = {}
    for name, converter in CONVERTERS.items():
        try:
            resolved[int(getattr(types_module, name))] = converter
        except (AttributeError, TypeError, ValueError):
            # 个别 JDBC 版本没有该常量，跳过即可
            logger.debug("java.sql.Types 缺少常量 %s，跳过其转换器", name)
    return resolved


def column_metadata(cursor: Any) -> list:
    """从游标读取列元数据（**必须在取值之前调用**）

    JDBC 的 ``ResultSetMetaData`` 只在结果集打开期间有效：JayDeBeApi 取完最后
    一行后会关闭 ``_rs`` 与 ``_meta``，之后 ``cursor.description`` 变为 ``None``。
    因此本函数要在 ``fetch*`` 之前调用。

    GBase 8s 的 JDBC 元数据比 ``cursor.description`` 更丰富（含类型名与 Java
    类名），故优先取 ``ResultSetMetaData``；取不到时退回 ``cursor.description``。

    Args:
        cursor: JayDeBeApi 游标

    Returns:
        List[Dict[str, Any]]: 每列一项，键为 ``name`` / ``type_name`` /
        ``jdbc_type`` / ``python_type`` / ``precision`` / ``scale`` /
        ``nullable`` / ``java_class``；信息缺失时对应值为 ``None``

    Example:
        >>> meta = column_metadata(cursor)   # 取值前
        >>> meta[0]["name"]
        'id'
    """
    columns: list = []
    meta = getattr(cursor, "_meta", None)
    if meta is not None:
        try:
            for index in range(1, meta.getColumnCount() + 1):
                type_name = str(meta.getColumnTypeName(index) or "").upper()
                columns.append(
                    {
                        "name": str(meta.getColumnLabel(index)),
                        "type_name": type_name or None,
                        "jdbc_type": int(meta.getColumnType(index)),
                        "python_type": python_type_for(type_name),
                        "precision": int(meta.getPrecision(index)),
                        "scale": int(meta.getScale(index)),
                        "nullable": bool(meta.isNullable(index)),
                        "java_class": str(meta.getColumnClassName(index) or "") or None,
                    }
                )
            return columns
        except Exception as error:  # pylint: disable=broad-except
            logger.debug("读取 JDBC 元数据失败，回退 cursor.description: %s", error)

    for description in getattr(cursor, "description", None) or ():
        type_code = description[1]
        names = getattr(type_code, "values", ()) or ()
        type_name = str(names[0]).upper() if names else None
        columns.append(
            {
                "name": str(description[0]),
                "type_name": type_name,
                "jdbc_type": None,
                "python_type": python_type_for(type_name),
                "precision": description[4],
                "scale": description[5],
                "nullable": bool(description[6]),
                "java_class": None,
            }
        )
    return columns


def install_converters(types_module: Any, target: Optional[Dict[int, Converter]] = None) -> int:
    """替换取值转换器表，返回覆盖的类型数量

    必须在 **JVM 启动之后**调用：JayDeBeApi 在首次连接时用
    ``java.sql.Types`` 的常量构造 ``_converters``，且该字典对象会被
    ``Connection``/``Cursor`` 共享引用，因此这里**原地更新**，使已建立的连接
    也能立即生效。

    重复调用是安全的（幂等）。

    Args:
        types_module: ``jpype.java.sql.Types``
        target: 待更新的转换器表；``None`` 时更新 JayDeBeApi 的全局表

    Returns:
        int: 实际注册的转换器数量（无法取得 ``jaydebeapi`` 时返回 0）

    Example:
        >>> install_converters(jpype.java.sql.Types) > 0
        True
    """
    try:
        import jaydebeapi  # pylint: disable=import-outside-toplevel
    except ImportError:  # pragma: no cover - 缺依赖时交由调用方报错
        return 0

    converters = build_converter_map(types_module)
    if not converters:
        return 0

    # 更新常量名表，使后续 _init_converters 也带上归一化转换器
    jaydebeapi._DEFAULT_CONVERTERS.update(  # pylint: disable=protected-access
        dict(CONVERTERS)
    )

    # 原地更新（不重新绑定）：Connection/Cursor 共享同一个 dict 引用
    if target is None:
        target = jaydebeapi._converters  # pylint: disable=protected-access
    if isinstance(target, dict):
        target.update(converters)

    logger.debug("已安装 GBase 8s 取值转换器: %d 种类型", len(converters))
    return len(converters)
