"""结果保真回归测试（重名列 / JOIN / 导出 / 流式）

**背景**：查询结果原先以 ``result.mappings().all()`` 的形式流转，调用方普遍用
``dict(row)`` / ``row.items()`` / ``row.get(名字)`` 消费。这在**列名重复**时会
静默产出错误数据，而列名重复正是 ``SELECT * FROM a JOIN b`` 的常态：

.. code-block:: text

    SQL      : SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id
    真实数据 : emp(id=1, name='张三', dept_id=10) + dept(id=10, name='研发部')
    keys()   : ['id', 'name', 'dept_id', 'id', 'name']
    values() : [1, '张三', 10, 10, '研发部']            ← RowMapping 内部保真
    dict(row): {'id': 10, 'name': '研发部', 'dept_id': 10}  ← 塌成 3 键，「张三」丢失

本文件同时**钉住旧行为**（说明为什么不能用字典）与**新行为**（按位置保真），
以免将来有人出于“简化”又把行数据改回字典。
"""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout

from sqlalchemy import create_engine, text

from src.db_connector_tool.cli_core import DBConnectorCLI
from src.db_connector_tool.core.result import (
    ColumnInfo,
    QueryChunk,
    QueryResult,
    assign_display_names,
    row_values,
)
from src.db_connector_tool.drivers.sqlalchemy_driver import SQLAlchemyDriver

from ._isolation import IsolatedTestCase

# 真值：emp(id=1, name='张三', dept_id=10) JOIN dept(id=10, name='研发部')
JOIN_TRUTH = (1, "张三", 10, 10, "研发部")
JOIN_NAMES = ["id", "name", "dept_id", "id", "name"]


def _join_rows():
    """在内存库上跑一次真实 JOIN，返回 (Row 列表, RowMapping 列表, 列名)"""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE emp (id INT, name TEXT, dept_id INT)"))
        conn.execute(text("CREATE TABLE dept (id INT, name TEXT)"))
        conn.execute(text("INSERT INTO emp VALUES (1,'张三',10)"))
        conn.execute(text("INSERT INTO dept VALUES (10,'研发部')"))
        conn.commit()
        sql = "SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id"
        res = conn.execute(text(sql))
        names = list(res.keys())
        rows = res.all()
        res2 = conn.execute(text(sql))
        mappings = res2.mappings().all()
    engine.dispose()
    return rows, mappings, names


class TestDuplicateColumnDisplayNames(IsolatedTestCase):
    """重复列名的显示名生成"""

    def test_unique_names_unchanged(self) -> None:
        """无重复时显示名保持原样"""
        self.assertEqual(assign_display_names(["id", "name"]), ["id", "name"])

    def test_duplicates_get_suffix(self) -> None:
        """重复列名追加 (n)，与常见客户端一致"""
        self.assertEqual(
            assign_display_names(["id", "name", "id", "name"]),
            ["id", "name", "id (1)", "name (1)"],
        )
        self.assertEqual(assign_display_names(["a", "a", "a"]), ["a", "a (1)", "a (2)"])

    def test_result_always_unique(self) -> None:
        """即使列名本身长得像带后缀的名字，结果仍两两不同"""
        got = assign_display_names(["a", "a (1)", "a"])
        self.assertEqual(len(set(got)), len(got), f"显示名出现重复: {got}")
        self.assertEqual(got, ["a", "a (1)", "a (2)"])


class TestRowMappingInformationLoss(IsolatedTestCase):
    """钉住 ``RowMapping`` 的丢值行为（说明为何必须按位置取值）"""

    def test_dict_collapses_duplicate_columns(self) -> None:
        """``dict(row)`` 会把 5 列塌成 3 键，且值被覆盖"""
        _rows, mappings, names = _join_rows()
        row = mappings[0]
        self.assertEqual(names, JOIN_NAMES)
        # RowMapping 内部按位置保真
        self.assertEqual(tuple(row.values()), JOIN_TRUTH)
        # 字典化后丢失
        collapsed = dict(row)
        self.assertEqual(len(collapsed), 3)
        self.assertNotIn("张三", collapsed.values(), "「张三」不应出现在塌陷后的字典里")

    def test_items_gives_wrong_values(self) -> None:
        """``items()`` 不仅丢键，**值也是错的**（第一个 name 被覆盖）"""
        _rows, mappings, _names = _join_rows()
        row = mappings[0]
        item_values = [value for _key, value in row.items()]
        self.assertNotEqual(
            item_values, list(JOIN_TRUTH), "items() 不应与真值相同（这正是 bug）"
        )
        self.assertEqual(len(set(item_values)), 2, "同名列全部取到同一个值")


class TestQueryResultFidelity(IsolatedTestCase):
    """``QueryResult`` 的按位置保真"""

    def test_from_row_preserves_all_columns(self) -> None:
        """Row 输入：5 列全部保留"""
        rows, _mappings, names = _join_rows()
        result = QueryResult.from_rows(rows, [{"name": n} for n in names])
        self.assertEqual(len(result.columns), 5)
        self.assertEqual(result.rows[0], JOIN_TRUTH)

    def test_from_rowmapping_preserves_all_columns(self) -> None:
        """RowMapping 输入：同样 5 列保真（关键回归）"""
        _rows, mappings, names = _join_rows()
        result = QueryResult.from_rows(mappings, [{"name": n} for n in names])
        self.assertEqual(result.rows[0], JOIN_TRUTH)

    def test_display_names_are_unique_keys(self) -> None:
        """``as_dicts()`` 以唯一显示名建键，不丢列"""
        _rows, mappings, names = _join_rows()
        result = QueryResult.from_rows(mappings, [{"name": n} for n in names])
        as_dict = result.as_dicts()[0]
        self.assertEqual(len(as_dict), 5)
        self.assertEqual(as_dict["id"], 1)
        self.assertEqual(as_dict["name"], "张三")
        self.assertEqual(as_dict["id (1)"], 10)
        self.assertEqual(as_dict["name (1)"], "研发部")

    def test_value_at_is_positional(self) -> None:
        """按位置取值永不塌陷"""
        _rows, mappings, names = _join_rows()
        result = QueryResult.from_rows(mappings, [{"name": n} for n in names])
        self.assertEqual(
            [result.value_at(0, i) for i in range(5)], list(JOIN_TRUTH)
        )

    def test_metadata_alignment(self) -> None:
        """列元数据与行按位置严格对齐（网格渲染的前提）"""
        _rows, mappings, names = _join_rows()
        metadata = [
            {"name": n, "type_name": "INT" if n != "name" else "TEXT"} for n in names
        ]
        result = QueryResult.from_rows(mappings, metadata)
        self.assertEqual(len(result.columns), len(result.rows[0]))
        for column in result.columns:
            self.assertEqual(
                column.ordinal, result.columns.index(column), "ordinal 应与下标一致"
            )
            self.assertEqual(result.raw_column_names()[column.ordinal], column.name)

    def test_empty_result_keeps_columns(self) -> None:
        """空结果仍保留列元数据（便于渲染表头）"""
        result = QueryResult.from_rows([], [{"name": "a"}, {"name": "a"}])
        self.assertEqual(result.rows, [])
        self.assertEqual(result.column_names(), ["a", "a (1)"])

    def test_all_null_row_survives(self) -> None:
        """整行为 NULL 时不会被误判为空"""
        result = QueryResult.from_rows(
            [(None, None)], [{"name": "a"}, {"name": "a"}]
        )
        self.assertEqual(result.rows[0], (None, None))
        self.assertEqual(result.as_dicts()[0], {"a": None, "a (1)": None})

    def test_derives_names_without_metadata(self) -> None:
        """无元数据时自行推导列名（Row 走 _mapping，RowMapping 走 keys）"""
        rows, mappings, names = _join_rows()
        self.assertEqual(QueryResult.from_rows(rows).raw_column_names(), names)
        self.assertEqual(QueryResult.from_rows(mappings).raw_column_names(), names)

    def test_equality_and_repr(self) -> None:
        """按内容比较、repr 简洁"""
        result = QueryResult.from_rows([(1, 2)], [{"name": "a"}, {"name": "b"}])
        same = QueryResult.from_rows([(1, 2)], [{"name": "a"}, {"name": "b"}])
        self.assertEqual(result, same)
        self.assertIn("rows=1", repr(result))


class TestDriverFidelityApis(IsolatedTestCase):
    """驱动层的保真与流式接口（真实 SQLite，非 mock）"""

    def setUp(self) -> None:
        super().setUp()
        self.driver = SQLAlchemyDriver({"type": "sqlite", "database": ":memory:"})
        self.driver.connect()
        with self.driver.engine.begin() as conn:
            conn.execute(text("CREATE TABLE b (n INT)"))
            for i in range(5):
                conn.execute(text("INSERT INTO b VALUES (:n)"), {"n": i})

    def tearDown(self) -> None:
        if self.driver.engine is not None:
            self.driver.engine.dispose()
        super().tearDown()

    def test_execute_query_result_preserves_duplicates(self) -> None:
        """execute_query_result 面对重名列仍保真"""
        result = self.driver.execute_query_result("SELECT 1 AS a, 2 AS a, 3 AS b")
        self.assertEqual(result.rows[0], (1, 2, 3))
        self.assertEqual(result.column_names(), ["a", "a (1)", "b"])

    def test_execute_query_result_real_join(self) -> None:
        """真表 JOIN：员工姓名不被部门名覆盖"""
        with self.driver.engine.begin() as conn:
            conn.execute(text("CREATE TABLE emp (id INT, name TEXT, dept_id INT)"))
            conn.execute(text("CREATE TABLE dept (id INT, name TEXT)"))
            conn.execute(text("INSERT INTO emp VALUES (1,'张三',10)"))
            conn.execute(text("INSERT INTO dept VALUES (10,'研发部')"))
        result = self.driver.execute_query_result(
            "SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id"
        )
        self.assertEqual(result.rows[0], JOIN_TRUTH)
        self.assertEqual(len(result.columns), 5)

    def test_metadata_has_ordinal_and_display_name(self) -> None:
        """execute_query_with_metadata 的列元数据带 ordinal / display_name"""
        _rows, columns = self.driver.execute_query_with_metadata(
            "SELECT 1 AS a, 2 AS a, 3 AS b"
        )
        self.assertEqual([c["ordinal"] for c in columns], [0, 1, 2])
        self.assertEqual([c["display_name"] for c in columns], ["a", "a (1)", "b"])
        # 既有键仍在（向后兼容）
        self.assertIn("name", columns[0])
        self.assertIn("python_type", columns[0])

    def test_stream_yields_all_rows_in_batches(self) -> None:
        """流式取数分批交出行，合计与全量一致"""
        chunks = list(
            self.driver.execute_query_stream(
                "SELECT n AS x, n + 100 AS x FROM b", fetch_size=2
            )
        )
        self.assertEqual([len(c) for c in chunks], [2, 2, 1])
        self.assertEqual(chunks[-1].is_last, True)
        rows = [row for chunk in chunks for row in chunk.rows]
        self.assertEqual(rows, [(0, 100), (1, 101), (2, 102), (3, 103), (4, 104)])

    def test_stream_is_last_reliable_when_exact_multiple(self) -> None:
        """恰好整除时末批仍标记 is_last（消费方不会等不到）"""
        chunks = list(self.driver.execute_query_stream("SELECT n FROM b", fetch_size=5))
        self.assertEqual(len(chunks), 1)
        self.assertTrue(chunks[0].is_last, "恰好一批时也必须标记为末批")

    def test_stream_empty_result(self) -> None:
        """空结果交出一批空数据但带列元数据"""
        chunks = list(
            self.driver.execute_query_stream("SELECT n FROM b WHERE 1 = 0", fetch_size=10)
        )
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].rows, [])
        self.assertTrue(chunks[0].is_last)
        self.assertEqual([c.display_name for c in chunks[0].columns], ["n"])

    def test_stream_rejects_bad_fetch_size(self) -> None:
        """fetch_size 非正数报 ValueError"""
        with self.assertRaises(ValueError):
            list(self.driver.execute_query_stream("SELECT 1", fetch_size=0))

    def test_stream_chunk_as_dicts(self) -> None:
        """批次可转字典，重名列不丢列"""
        chunk = next(
            self.driver.execute_query_stream("SELECT 1 AS a, 2 AS a", fetch_size=10)
        )
        self.assertEqual(chunk.as_dicts()[0], {"a": 1, "a (1)": 2})


class TestCliExportFidelity(IsolatedTestCase):
    """CLI 导出与表格渲染不再输出错误数据（**核心回归**）"""

    def setUp(self) -> None:
        super().setUp()
        self.cli = DBConnectorCLI()
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)

    def test_csv_keeps_all_duplicate_columns(self) -> None:
        """回归：CSV 每个重名列各占一列，值不互相覆盖

        修复前实测写出 ``a,a,b / 2,2,3``（第一列真值为 1，被写成 2）。
        """
        result = QueryResult.from_rows(
            [(1, 2, 3)], [{"name": "a"}, {"name": "a"}, {"name": "b"}]
        )
        path = os.path.join(self.temp.name, "out.csv")
        self.cli._save_csv(result, path)  # pylint: disable=protected-access
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().strip().splitlines()
        self.assertEqual(lines[0], "a,a (1),b")
        self.assertEqual(lines[1], "1,2,3", "重名列的值必须各自独立")

    def test_csv_real_join_keeps_employee_name(self) -> None:
        """回归：JOIN 导出的员工姓名不再是部门名"""
        _rows, mappings, names = _join_rows()
        result = QueryResult.from_rows(mappings, [{"name": n} for n in names])
        path = os.path.join(self.temp.name, "join.csv")
        self.cli._save_csv(result, path)  # pylint: disable=protected-access
        with open(path, encoding="utf-8") as handle:
            content = handle.read()
        self.assertIn("张三", content, "员工姓名必须出现在导出里")
        self.assertIn("1,张三,10,10,研发部", content)

    def test_json_keeps_all_columns(self) -> None:
        """回归：JSON 导出 5 列不再塌成 3 键"""
        _rows, mappings, names = _join_rows()
        result = QueryResult.from_rows(mappings, [{"name": n} for n in names])
        path = os.path.join(self.temp.name, "join.json")
        self.cli._save_json(result, path)  # pylint: disable=protected-access
        with open(path, encoding="utf-8") as handle:
            data = json.load(handle)
        self.assertEqual(len(data[0]), 5)
        self.assertEqual(data[0]["name"], "张三")
        self.assertEqual(data[0]["name (1)"], "研发部")

    def test_table_shows_employee_name(self) -> None:
        """回归：终端表格不再把「张三」显示成「研发部」"""
        _rows, mappings, names = _join_rows()
        result = QueryResult.from_rows(mappings, [{"name": n} for n in names])
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.cli._show_results(result)  # pylint: disable=protected-access
        output = buffer.getvalue()
        self.assertIn("张三", output)
        self.assertIn("id (1)", output)
        self.assertIn("name (1)", output)
        # 值必须落在正确的列上：第一列 1，第二列 张三
        self.assertRegex(output, r"\|\s*1\s*\|\s*张三\s*\|")

    def test_accepts_legacy_row_objects(self) -> None:
        """兼容历史形态：直接传 RowMapping 列表也能正确渲染"""
        _rows, mappings, names = _join_rows()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            self.cli._show_results(mappings)  # pylint: disable=protected-access
        self.assertIn("张三", buffer.getvalue())

    def test_save_output_accepts_query_result(self) -> None:
        """_save_output 接受 QueryResult 与旧行列表两种输入"""
        result = QueryResult.from_rows([(1,)], [{"name": "a"}])
        csv_path = os.path.join(self.temp.name, "a.csv")
        json_path = os.path.join(self.temp.name, "a.json")
        for path in (csv_path, json_path):
            buffer = io.StringIO()
            with redirect_stdout(buffer):
                self.cli._save_output(result, path)  # pylint: disable=protected-access
            self.assertIn("结果已保存", buffer.getvalue())

    def test_column_width_uses_position_for_duplicates(self) -> None:
        """列宽按位置计算，重名列各算各的宽度"""
        result = QueryResult.from_rows(
            [("short", "a-much-longer-value")], [{"name": "x"}, {"name": "x"}]
        )
        widths = self.cli._calc_col_widths(  # pylint: disable=protected-access
            result.column_names(), result
        )
        self.assertNotEqual(
            widths["x"], widths["x (1)"], "两列宽度应分别按其真实内容计算"
        )


class TestQueryChunkShape(IsolatedTestCase):
    """``QueryChunk`` 的基本行为"""

    def test_len_and_dicts(self) -> None:
        """批次支持 len 与字典转换"""
        columns = (
            ColumnInfo(0, "a", "a"),
            ColumnInfo(1, "a", "a (1)"),
        )
        chunk = QueryChunk(columns=columns, rows=[(1, 2)], is_last=True)
        self.assertEqual(len(chunk), 1)
        self.assertEqual(chunk.as_dicts(), [{"a": 1, "a (1)": 2}])
        self.assertTrue(chunk.is_last)


class TestResultModelApi(IsolatedTestCase):
    """``ColumnInfo`` / ``QueryResult`` 的完整接口"""

    def setUp(self) -> None:
        super().setUp()
        self.columns = (
            ColumnInfo(0, "id", "id", type_name="INTEGER", python_type="int"),
            ColumnInfo(1, "id", "id (1)"),
        )
        self.result = QueryResult(columns=self.columns, rows=[(1, 10), (2, 20)])

    def test_to_mapping_round_trip(self) -> None:
        """ColumnInfo 转回字典保留既有键并新增 ordinal / display_name"""
        mapping = self.columns[0].to_mapping()
        self.assertEqual(mapping["name"], "id")
        self.assertEqual(mapping["ordinal"], 0)
        self.assertEqual(mapping["display_name"], "id")
        self.assertEqual(mapping["type_name"], "INTEGER")
        for key in ("jdbc_type", "precision", "scale", "nullable", "java_class"):
            self.assertIn(key, mapping, f"to_mapping 应保留既有键 {key}")

    def test_from_mapping_defaults(self) -> None:
        """from_mapping 在缺字段时给出安全默认值"""
        column = ColumnInfo.from_mapping(3, "c3", {})
        self.assertEqual(column.ordinal, 3)
        self.assertEqual(column.name, "column_3", "缺 name 时按下标兜底")
        self.assertEqual(column.display_name, "c3")
        self.assertIsNone(column.type_name)
        self.assertIsNone(column.precision)

    def test_len_rowcount_and_iter(self) -> None:
        """len / rowcount / 迭代都按行数"""
        self.assertEqual(len(self.result), 2)
        self.assertEqual(self.result.rowcount, 2)
        self.assertEqual(list(self.result), [(1, 10), (2, 20)])
        self.assertEqual(list(self.result.iter_rows()), [(1, 10), (2, 20)])

    def test_find_column_by_display_and_raw_name(self) -> None:
        """find_column 支持显示名与原始名"""
        self.assertEqual(self.result.find_column("id").ordinal, 0)
        self.assertEqual(self.result.find_column("id (1)").ordinal, 1)
        self.assertIsNone(self.result.find_column("nope"))

    def test_equality_with_other_types(self) -> None:
        """与其它类型比较不相等"""
        self.assertNotEqual(self.result, "不是 QueryResult")

    def test_repr_and_column_name_lists(self) -> None:
        """repr 含行数，列名列表区分显示名与原始名"""
        self.assertIn("rows=2", repr(self.result))
        self.assertEqual(self.result.column_names(), ["id", "id (1)"])
        self.assertEqual(self.result.raw_column_names(), ["id", "id"])

    def test_value_at_out_of_range(self) -> None:
        """越界取值抛 IndexError"""
        with self.assertRaises(IndexError):
            self.result.value_at(99, 0)
        with self.assertRaises(IndexError):
            self.result.value_at(0, 99)

    def test_with_columns_metadata(self) -> None:
        """用新元数据补充列属性，行数据保持不变"""
        enriched = self.result.with_columns_metadata(
            [
                {"name": "id", "python_type": "int", "precision": 10},
                {"name": "id", "python_type": "int", "scale": 0},
            ]
        )
        self.assertEqual(enriched.columns[0].python_type, "int")
        self.assertEqual(enriched.columns[0].precision, 10)
        self.assertEqual(enriched.columns[1].scale, 0)
        self.assertEqual(enriched.rows, self.result.rows, "行数据不应改变")
        # 元数据少于列数时，多出的列原样保留
        partial = self.result.with_columns_metadata([{"name": "id"}])
        self.assertEqual(partial.columns[1], self.columns[1])

    def test_metadata_longer_than_names(self) -> None:
        """元数据多于行宽时以元数据为准补齐列名"""
        result = QueryResult.from_rows([(1,)], [{"name": "a"}, {"name": "b"}])
        self.assertEqual(result.rows[0], (1, None), "缺的值补 None")
        self.assertEqual(result.column_names(), ["a", "b"])

    def test_derives_names_from_plain_dict(self) -> None:
        """纯字典输入按插入顺序推导列名"""
        result = QueryResult.from_rows([{"x": 1, "y": 2}])
        self.assertEqual(result.column_names(), ["x", "y"])
        self.assertEqual(result.rows[0], (1, 2))

    def test_tuple_input_without_metadata(self) -> None:
        """元组输入且无元数据时用占位列名"""
        result = QueryResult.from_rows([(1, 2, 3)])
        self.assertEqual(result.column_names(), ["column_0", "column_1", "column_2"])
        self.assertEqual(result.rows[0], (1, 2, 3))


class TestRowValuesHelper(IsolatedTestCase):
    """``row_values`` 对各种行形态的取值行为"""

    def test_tuple_and_list(self) -> None:
        """序列直接返回"""
        self.assertEqual(row_values((1, 2)), (1, 2))
        self.assertEqual(row_values([1, 2]), (1, 2))

    def test_dict_uses_insertion_order(self) -> None:
        """字典按插入顺序取值"""
        self.assertEqual(row_values({"b": 2, "a": 1}), (2, 1))

    def test_unknown_object_returns_empty(self) -> None:
        """无位置信息的对象返回空元组（不抛异常）"""

        class Opaque:
            """既无 values 也无 _mapping 且不可迭代"""

            __slots__ = ()

        self.assertEqual(row_values(Opaque()), ())

    def test_object_whose_values_raises(self) -> None:
        """values() 抛异常时安全降级"""

        class Broken:
            """values() 直接抛 TypeError"""

            def values(self):
                raise TypeError("boom")

        self.assertEqual(row_values(Broken()), ())


if __name__ == "__main__":
    unittest.main()
