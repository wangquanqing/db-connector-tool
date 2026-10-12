"""GBase 8s（JDBC/JPype）返回值类型归一化单元测试

覆盖三处曾污染上层数据的缺陷：

1. ``DECIMAL``/``NUMERIC`` 被 JayDeBeApi 转成 float 导致**精度丢失**；
2. 整数/浮点原样返回 JPype 包装的 Java 对象，日期被转成 ``str``，与其它
   数据库驱动返回的 Python 原生类型不一致；
3. 时间戳微秒换算 ``int(str(nanos)[:6])`` 在纳秒不足 9 位时**放大 100 倍**。

测试用轻量假对象模拟 ``ResultSet``/``ResultSetMetaData``，无需启动 JVM。
"""

# pylint: disable=protected-access

import datetime
import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.db_connector_tool.drivers.gbase8s import converters
from src.db_connector_tool.drivers.gbase8s.converters import (
    CONVERTERS,
    _nanos_to_microseconds,
    _to_binary,
    _to_bool,
    _to_date,
    _to_datetime,
    _to_decimal,
    _to_float,
    _to_int,
    _to_str,
    _to_time,
    build_converter_map,
    column_metadata,
    python_type_for,
)


class FakeResultSet:
    """按列序号返回预置 Java 对象的假 ResultSet"""

    def __init__(self, values: dict) -> None:
        self._values = values

    def getObject(self, col: int) -> object:
        return self._values[col]

    def getDate(self, col: int) -> object:
        return self._values[col]

    def getTime(self, col: int) -> object:
        return self._values[col]

    def getTimestamp(self, col: int) -> object:
        return self._values[col]

    def getBytes(self, col: int) -> object:
        return self._values[col]


def fake_rs(value: object) -> FakeResultSet:
    """构造只含第 1 列的假 ResultSet"""
    return FakeResultSet({1: value})


class FakeBigDecimal:
    """模拟 java.math.BigDecimal（只实现被用到的接口）"""

    def __init__(self, text: str) -> None:
        self._text = text

    def toPlainString(self) -> str:
        return self._text

    def __str__(self) -> str:
        return self._text


class FakeTimestamp:
    """模拟 java.sql.Timestamp"""

    def __init__(self, text: str, nanos: int) -> None:
        self._text = text
        self._nanos = nanos

    def __str__(self) -> str:
        return self._text

    def getNanos(self) -> int:
        return self._nanos


class TestDecimalPrecision(unittest.TestCase):
    """DECIMAL / NUMERIC 必须精确，不能经 float 中转"""

    def test_large_decimal_keeps_all_digits(self) -> None:
        """回归：DECIMAL(19,2) 的大数不再失真为科学计数法"""
        rs = fake_rs(FakeBigDecimal("12345678901234567.89"))
        result = _to_decimal(rs, 1)
        self.assertEqual(result, Decimal("12345678901234567.89"))
        self.assertNotIsInstance(result, float)

    def test_scale_zero_decimal_stays_decimal(self) -> None:
        """标度为 0 的 DECIMAL 也保持 Decimal（原实现会退化为 int）"""
        rs = fake_rs(FakeBigDecimal("100"))
        self.assertEqual(_to_decimal(rs, 1), Decimal("100"))

    def test_money_like_value_exact(self) -> None:
        """金额典型值精确保留"""
        rs = fake_rs(FakeBigDecimal("1234567.89"))
        self.assertEqual(_to_decimal(rs, 1), Decimal("1234567.89"))

    def test_null_returns_none(self) -> None:
        """NULL 仍为 None"""
        self.assertIsNone(_to_decimal(fake_rs(None), 1))

    def test_plain_number_fallback(self) -> None:
        """驱动直接返回数值时走字符串构造"""
        self.assertEqual(_to_decimal(fake_rs(3), 1), Decimal("3"))

    def test_unparsable_returns_none(self) -> None:
        """无法解析的值返回 None 而不是抛异常"""

        class Broken:
            def toPlainString(self) -> str:
                return "not-a-number"

            def __str__(self) -> str:
                return "not-a-number"

        self.assertIsNone(_to_decimal(fake_rs(Broken()), 1))


class TestScalarNormalization(unittest.TestCase):
    """标量类型归一化为 Python 原生类型"""

    def test_integer_variants(self) -> None:
        """Integer / Long / Short 统一为 int"""
        for value in (1, -1, 0, 9223372036854775807):
            with self.subTest(value=value):
                result = _to_int(fake_rs(value), 1)
                self.assertIsInstance(result, int)
                self.assertEqual(result, value)

    def test_int_null(self) -> None:
        """整数 NULL 为 None"""
        self.assertIsNone(_to_int(fake_rs(None), 1))

    def test_float_from_java_double(self) -> None:
        """Java Double -> float"""
        result = _to_float(fake_rs(1.5), 1)
        self.assertIsInstance(result, float)
        self.assertEqual(result, 1.5)

    def test_float_fallback_to_double_value(self) -> None:
        """float() 不可用时退回 doubleValue()"""

        class JavaFloat:
            def doubleValue(self) -> float:
                return 2.5

        self.assertEqual(_to_float(fake_rs(JavaFloat()), 1), 2.5)

    def test_str_conversion(self) -> None:
        """字符串列转为 str"""
        self.assertEqual(_to_str(fake_rs("abc"), 1), "abc")
        self.assertIsNone(_to_str(fake_rs(None), 1))

    def test_bool_conversion(self) -> None:
        """BOOLEAN / BIT -> bool"""
        self.assertIs(_to_bool(fake_rs(True), 1), True)
        self.assertIs(_to_bool(fake_rs(False), 1), False)
        self.assertIsNone(_to_bool(fake_rs(None), 1))

    def test_binary_conversion(self) -> None:
        """BINARY / BLOB -> bytes"""
        self.assertEqual(_to_binary(fake_rs(b"\x00\x01"), 1), b"\x00\x01")
        self.assertIsNone(_to_binary(fake_rs(None), 1))


class TestDateTimeConversion(unittest.TestCase):
    """日期时间归一化为 datetime 类型"""

    def test_date(self) -> None:
        """DATE -> datetime.date"""
        result = _to_date(fake_rs("2026-10-13"), 1)
        self.assertEqual(result, datetime.date(2026, 10, 13))
        self.assertIsInstance(result, datetime.date)

    def test_time(self) -> None:
        """TIME -> datetime.time"""
        result = _to_time(fake_rs("12:34:56"), 1)
        self.assertEqual(result, datetime.time(12, 34, 56))

    def test_datetime_with_microseconds(self) -> None:
        """TIMESTAMP -> datetime.datetime，微秒精确"""
        rs = fake_rs(FakeTimestamp("2026-10-13 01:23:18.936", 936000000))
        result = _to_datetime(rs, 1)
        self.assertEqual(result, datetime.datetime(2026, 10, 13, 1, 23, 18, 936000))

    def test_nanos_below_100_million_not_scaled(self) -> None:
        """回归：纳秒不足 9 位时微秒不再放大 100 倍

        旧实现 ``int(str(1010000)[:6])`` 得到 101000（应为 1010）。
        """
        rs = fake_rs(FakeTimestamp("2026-10-13 01:23:18.001010", 1010000))
        self.assertEqual(_to_datetime(rs, 1).microsecond, 1010)

    def test_nanos_to_microseconds_boundaries(self) -> None:
        """纳秒 -> 微秒换算的边界值"""
        cases = ((0, 0), (1000, 1), (1010000, 1010), (999999999, 999999))
        for nanos, expected in cases:
            with self.subTest(nanos=nanos):
                self.assertEqual(_nanos_to_microseconds(nanos), expected)

    def test_datetime_null(self) -> None:
        """时间戳 NULL 为 None"""
        self.assertIsNone(_to_datetime(fake_rs(None), 1))


class TestConverterRegistry(unittest.TestCase):
    """转换器表的构建与安装"""

    def test_registry_covers_common_types(self) -> None:
        """常用 JDBC 类型均有归一化转换器"""
        for name in (
            "INTEGER",
            "BIGINT",
            "SMALLINT",
            "DECIMAL",
            "NUMERIC",
            "DOUBLE",
            "REAL",
            "VARCHAR",
            "DATE",
            "TIME",
            "TIMESTAMP",
            "BLOB",
            "BOOLEAN",
        ):
            with self.subTest(name=name):
                self.assertIn(name, CONVERTERS)

    def test_build_map_uses_types_constants(self) -> None:
        """类型号由 java.sql.Types 常量解析得到"""
        fake_types = MagicMock()
        fake_types.INTEGER = 4
        fake_types.DECIMAL = 3
        resolved = build_converter_map(fake_types)
        self.assertIs(resolved[4], CONVERTERS["INTEGER"])
        self.assertIs(resolved[3], CONVERTERS["DECIMAL"])

    def test_build_map_skips_missing_constants(self) -> None:
        """常量缺失时跳过，不抛异常"""
        fake_types = MagicMock(spec=["INTEGER"])
        fake_types.INTEGER = 4
        resolved = build_converter_map(fake_types)
        self.assertIn(4, resolved)


class TestPythonTypeNames(unittest.TestCase):
    """JDBC 类型名 -> Python 类型名映射"""

    def test_exact_names(self) -> None:
        """标准类型名精确匹配"""
        self.assertEqual(python_type_for("INTEGER"), "int")
        self.assertEqual(python_type_for("DECIMAL"), "Decimal")
        self.assertEqual(python_type_for("DATE"), "date")

    def test_gbase_specific_names(self) -> None:
        """GBase 特有类型名（INT8 / DATETIME YEAR TO FRACTION(n)）"""
        self.assertEqual(python_type_for("INT8"), "int")
        self.assertEqual(python_type_for("SERIAL8"), "int")
        self.assertEqual(python_type_for("MONEY"), "Decimal")
        self.assertEqual(python_type_for("DATETIME YEAR TO FRACTION(3)"), "datetime")

    def test_case_insensitive(self) -> None:
        """大小写不敏感"""
        self.assertEqual(python_type_for("int8"), "int")

    def test_unknown_and_empty(self) -> None:
        """无法识别或为空时返回 None"""
        self.assertIsNone(python_type_for("WEIRDTYPE"))
        self.assertIsNone(python_type_for(""))
        self.assertIsNone(python_type_for(None))


class FakeMetaData:
    """模拟 ResultSetMetaData"""

    def __init__(self, columns: list) -> None:
        self._columns = columns

    def getColumnCount(self) -> int:
        return len(self._columns)

    def _col(self, index: int) -> dict:
        return self._columns[index - 1]

    def getColumnLabel(self, index: int) -> str:
        return self._col(index)["label"]

    def getColumnTypeName(self, index: int) -> str:
        return self._col(index)["type_name"]

    def getColumnType(self, index: int) -> int:
        return self._col(index)["type"]

    def getPrecision(self, index: int) -> int:
        return self._col(index)["precision"]

    def getScale(self, index: int) -> int:
        return self._col(index)["scale"]

    def isNullable(self, index: int) -> int:
        return self._col(index)["nullable"]

    def getColumnClassName(self, index: int) -> str:
        return self._col(index)["java_class"]


class TestColumnMetadata(unittest.TestCase):
    """列元数据提取"""

    def test_from_jdbc_metadata(self) -> None:
        """优先使用 JDBC ResultSetMetaData（信息最全）"""
        cursor = MagicMock()
        cursor._meta = FakeMetaData(
            [
                {
                    "label": "dec",
                    "type_name": "decimal",
                    "type": 3,
                    "precision": 19,
                    "scale": 2,
                    "nullable": 1,
                    "java_class": "java.math.BigDecimal",
                }
            ]
        )
        columns = column_metadata(cursor)
        self.assertEqual(len(columns), 1)
        self.assertEqual(columns[0]["name"], "dec")
        self.assertEqual(columns[0]["precision"], 19)
        self.assertEqual(columns[0]["scale"], 2)
        self.assertEqual(columns[0]["python_type"], "Decimal")
        self.assertTrue(columns[0]["nullable"])
        self.assertEqual(columns[0]["java_class"], "java.math.BigDecimal")

    def test_gbase_datetime_type_name(self) -> None:
        """GBase 的 Informix 式类型名也能映射到 python_type"""
        cursor = MagicMock()
        cursor._meta = FakeMetaData(
            [
                {
                    "label": "dt",
                    "type_name": "datetime year to fraction(3)",
                    "type": 93,
                    "precision": 23,
                    "scale": 3,
                    "nullable": 1,
                    "java_class": "java.sql.Timestamp",
                }
            ]
        )
        columns = column_metadata(cursor)
        self.assertEqual(columns[0]["python_type"], "datetime")
        self.assertEqual(columns[0]["type_name"], "DATETIME YEAR TO FRACTION(3)")

    def test_fallback_to_description(self) -> None:
        """无 JDBC 元数据时退回 cursor.description"""

        class TypeCode:
            values = ("VARCHAR",)

        cursor = MagicMock()
        cursor._meta = None
        cursor.description = [("s", TypeCode(), 10, 10, 10, 0, 1)]
        columns = column_metadata(cursor)
        self.assertEqual(columns[0]["name"], "s")
        self.assertEqual(columns[0]["python_type"], "str")

    def test_no_metadata_returns_empty(self) -> None:
        """两者都取不到时返回空列表"""
        cursor = MagicMock()
        cursor._meta = None
        cursor.description = None
        self.assertEqual(column_metadata(cursor), [])

    def test_broken_jdbc_metadata_falls_back(self) -> None:
        """JDBC 元数据抛异常时回退 description，不向外抛"""

        class TypeCode:
            values = ("INTEGER",)

        cursor = MagicMock()
        cursor._meta.getColumnCount.side_effect = RuntimeError("boom")
        cursor.description = [("i", TypeCode(), 4, 4, 10, 0, 1)]
        columns = column_metadata(cursor)
        self.assertEqual(columns[0]["name"], "i")
        self.assertEqual(columns[0]["python_type"], "int")


class TestErrorBranches(unittest.TestCase):
    """异常与边界值不应向外抛"""

    def test_int_unparsable_falls_back_to_str(self) -> None:
        """int() 失败时经字符串转换（BigInteger 超出 int64 的情形）"""

        class HugeInt:
            def __str__(self) -> str:
                return "123456789012345678901234567890"

        result = _to_int(fake_rs(HugeInt()), 1)
        self.assertEqual(result, 123456789012345678901234567890)

    def test_bool_falls_back_to_boolean_value(self) -> None:
        """bool() 不可用时退回 booleanValue()"""

        class JavaBool:
            def booleanValue(self) -> bool:
                return True

        self.assertIs(_to_bool(fake_rs(JavaBool()), 1), True)

    def test_date_unparsable_returns_none(self) -> None:
        """无法解析的日期返回 None"""
        self.assertIsNone(_to_date(fake_rs("not-a-date"), 1))

    def test_time_unparsable_returns_none(self) -> None:
        """无法解析的时间返回 None"""
        self.assertIsNone(_to_time(fake_rs("not-a-time"), 1))

    def test_datetime_unparsable_returns_none(self) -> None:
        """无法解析的时间戳返回 None"""

        class BrokenTs:
            def __str__(self) -> str:
                return "not-a-timestamp"

            def getNanos(self) -> int:
                return 0

        self.assertIsNone(_to_datetime(fake_rs(BrokenTs()), 1))

    def test_binary_from_java_bytes(self) -> None:
        """Java byte[] 兼容 bytes/bytearray"""
        self.assertEqual(_to_binary(fake_rs(bytearray(b"\x01\x02")), 1), b"\x01\x02")

    def test_binary_from_string_encodes(self) -> None:
        """驱动返回字符串时按 latin-1 保字节"""
        self.assertEqual(_to_binary(fake_rs("ab"), 1), b"ab")


class TestInstallConverters(unittest.TestCase):
    """转换器安装（用假 jaydebeapi 模块，无需 JVM）"""

    @staticmethod
    def _fake_jaydebeapi() -> MagicMock:
        module = MagicMock()
        module._DEFAULT_CONVERTERS = {}
        module._converters = {}
        return module

    def test_install_updates_tables(self) -> None:
        """安装后常量名表与类型号表都被填充"""
        fake = self._fake_jaydebeapi()
        fake_types = MagicMock()
        fake_types.INTEGER = 4
        fake_types.DECIMAL = 3
        with patch.dict("sys.modules", {"jaydebeapi": fake}):
            count = converters.install_converters(fake_types)
        self.assertGreater(count, 0)
        self.assertIs(fake._converters[4], CONVERTERS["INTEGER"])
        self.assertIs(fake._converters[3], CONVERTERS["DECIMAL"])
        self.assertIn("BIGINT", fake._DEFAULT_CONVERTERS)

    def test_install_updates_target_in_place(self) -> None:
        """显式传入 target 时原地更新该字典（连接共享同一引用）"""
        fake = self._fake_jaydebeapi()
        fake_types = MagicMock()
        fake_types.INTEGER = 4
        target = {999: "keep"}
        with patch.dict("sys.modules", {"jaydebeapi": fake}):
            converters.install_converters(fake_types, target=target)
        self.assertEqual(target[999], "keep")
        self.assertIs(target[4], CONVERTERS["INTEGER"])

    def test_install_without_constants_returns_zero(self) -> None:
        """取不到任何常量时返回 0，不抛异常"""
        fake = self._fake_jaydebeapi()
        fake_types = MagicMock(spec=[])  # 无任何常量
        with patch.dict("sys.modules", {"jaydebeapi": fake}):
            self.assertEqual(converters.install_converters(fake_types), 0)

    def test_install_without_jaydebeapi_returns_zero(self) -> None:
        """缺少 jaydebeapi 时返回 0"""
        fake_types = MagicMock()
        fake_types.INTEGER = 4
        with patch.dict("sys.modules", {"jaydebeapi": None}):
            self.assertEqual(converters.install_converters(fake_types), 0)


if __name__ == "__main__":
    unittest.main()