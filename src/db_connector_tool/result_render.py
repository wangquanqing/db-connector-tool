# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""查询结果的终端渲染与文件导出（CLI 的 mixin）

从 ``cli_core`` 拆出来的原因：该模块本已接近 pylint 的 1000 行上限，而
「结果怎么显示/怎么落盘」与「命令行参数怎么处理」是两件独立的事。

**保真的关键**：本模块**一律按位置取值**（``row_values`` / ``QueryResult.rows``），
绝不用 ``row.get(列名)`` / ``dict(row)``。原因见
:mod:`db_connector_tool.core.result`：列名重复时按名取值会互相覆盖，而列名重复
正是 ``SELECT * FROM a JOIN b`` 的常态 —— 实测曾把员工姓名「张三」显示成部门名
「研发部」。

表头使用**去重后的显示名**（``id`` / ``id (1)``），因此列身份（序号）与显示名
是分离的：显示可读、取值不错。

Example:
    >>> from db_connector_tool.cli_core import DBConnectorCLI
    >>> cli = DBConnectorCLI()
    >>> cli._show_results([{"id": 1, "name": "张三"}])
    +----+------+
    | id | name |
    +----+------+
    | 1  | 张三 |
    +----+------+
"""

import csv
import datetime
import json
import shutil
import sys
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple, Union

from .core.exceptions import ConfigError, DatabaseError, FileSystemError
from .core.result import QueryResult, row_values
from .utils.logging_utils import get_logger
from .utils.path_utils import PathHelper

logger = get_logger(__name__)

__all__ = ["ResultRenderMixin"]

# mixin 有意只提供「渲染/导出」一族方法，不单独对外暴露公共 API
# pylint: disable=too-few-public-methods


class ResultRenderMixin:
    """结果渲染与导出（由 DBConnectorCLI 混入）"""

    # ------------------------------------------------------------ 入口
    @staticmethod
    def _as_query_result(results: Any) -> QueryResult:
        """把任意历史形态的查询结果统一成 :class:`QueryResult`

        这是 CLI 的**保真入口**：把「按名字取值」换成「按位置取值」。

        之所以必须统一，是因为按键消费在**列名重复**时会静默出错 ——
        而列名重复是 ``SELECT * FROM a JOIN b`` 的常态：

        .. code-block:: text

            SQL      : SELECT * FROM emp JOIN dept ON emp.dept_id = dept.id
            真值     : 1, 张三, 10, 10, 研发部
            旧做法    : dict(row) -> {'id': 10, 'name': '研发部', 'dept_id': 10}
                       └─ 员工姓名「张三」被「研发部」覆盖，且只余 3 列

        接受的输入：``QueryResult``（直接用）、行对象列表（``Row`` /
        ``RowMapping`` / 元组 / 字典）。

        Args:
            results: 查询结果，形态不限

        Returns:
            QueryResult: 按位置保真的结果对象

        Example:
            >>> DBConnectorCLI._as_query_result([{"id": 1}]).as_dicts()
            [{'id': 1}]
        """
        if isinstance(results, QueryResult):
            return results
        return QueryResult.from_rows(list(results or []))

    # ------------------------------------------------------------ 导出
    def _save_output(self, results: Any, output_path: str) -> None:
        """将查询结果保存到文件

        根据扩展名自动选择 JSON 或 CSV 格式。

        Args:
            results: 查询结果（``QueryResult`` 或行对象列表）
            output_path: 输出文件路径（.json/.csv）

        Raises:
            SystemExit: 格式不支持或保存失败时退出
        """
        try:
            result = self._as_query_result(results)
            ext = PathHelper.get_file_extension(output_path, include_dot=False)
            if ext == "json":
                self._save_json(result, output_path)
            elif ext == "csv":
                self._save_csv(result, output_path)
            else:
                raise FileSystemError(f"不支持的输出文件格式: {output_path}")
            print(f"✅ 结果已保存到: {output_path}")
        except (DatabaseError, ConfigError, FileSystemError, OSError) as error:
            # OSError 覆盖目录不存在、磁盘已满、权限不足等文件系统错误，
            # 否则用户会看到裸 traceback 而不是统一的中文提示
            logger.error("保存结果失败: %s", error)
            print(f"❌ 保存结果失败: {error}")
            sys.exit(1)

    def _save_json(self, results: Any, output_path: str) -> None:
        """将结果保存为 JSON（UTF-8，2空格缩进）

        列名重复时以去重后的显示名（``id (1)``）建键，**不会丢列**。

        Args:
            results: 查询结果（``QueryResult`` 或行对象列表）
            output_path: 输出文件路径

        Raises:
            SystemExit: 序列化失败时退出
        """
        result = self._as_query_result(results)
        if not result.rows:
            return
        try:
            with open(output_path, "w", encoding="utf-8") as fh:
                json.dump(
                    result.as_dicts(),
                    fh,
                    indent=2,
                    ensure_ascii=False,
                    default=self._to_serializable,
                )
        except (ValueError, TypeError) as error:
            logger.error("JSON序列化失败: %s", error)
            print(f"❌ JSON保存失败: {error}")
            sys.exit(1)

    def _save_csv(self, results: Any, output_path: str) -> None:
        """将结果保存为 CSV（UTF-8）

        按**位置**写出每一列，因此重复列名也能各自成列、值不会互相覆盖
        （此前 ``row.items()`` 会把同名列写成同一个值）。

        Args:
            results: 查询结果（``QueryResult`` 或行对象列表）
            output_path: 输出文件路径

        Raises:
            SystemExit: 写入失败时退出
        """
        result = self._as_query_result(results)
        if not result.rows:
            return
        with open(output_path, "w", newline="", encoding="utf-8") as fh:
            # 表头用去重后的显示名，保证 CSV 的列名唯一可辨
            writer = csv.writer(fh)
            writer.writerow(result.column_names())
            for row in result.rows:
                writer.writerow([self._to_serializable(value) for value in row])

    @staticmethod
    def _to_serializable(obj: Any) -> Any:
        """将数据库驱动对象转换为可序列化原生类型

        **标量先于容器判断**：JPype 包装的 Java 对象带一个**空的 ``__dict__``**，
        若先走 ``obj.__dict__`` 分支，``java.lang.Integer`` 之类的数值会被静默变成
        ``{}``，导出 CSV 时整列数据丢失。

        JSON 原生类型（``int`` / ``float`` / ``bool`` / ``str`` / ``None``）原样返回，
        以免 JSON 导出把数字变成字符串；``Decimal`` 与日期时间转为字符串，
        既保证可序列化，又避免 ``Decimal`` 经 float 中转丢精度。

        Args:
            obj: 原始对象（Row/ResultProxy/标量等）

        Returns:
            Any: 可序列化的 Python 原生类型

        Example:
            >>> DBConnectorCLI._to_serializable(3)
            3
            >>> DBConnectorCLI._to_serializable({"a": 1})
            {'a': 1}
            >>> DBConnectorCLI._to_serializable(Decimal("1.50"))
            '1.50'
        """
        # JSON 原生类型：直接返回，保持数字/布尔/字符串的原始语义
        if obj is None or isinstance(obj, (str, int, float, bool)):
            return obj
        # 非 JSON 原生标量：转字符串（Decimal 保持精度，不经过 float）
        if isinstance(obj, (Decimal, datetime.date, datetime.time, bytes, bytearray)):
            return str(obj)

        result = ResultRenderMixin._to_container(obj)
        if result is not None:
            return result
        # 仅当 __dict__ 确有内容时才采用，空字典视为「无结构信息」
        attributes = getattr(obj, "__dict__", None)
        if attributes:
            return attributes
        return str(obj)

    @staticmethod
    def _to_container(obj: Any) -> Any:
        """尝试把对象转为字典或列表，无法转换时返回 ``None``

        Args:
            obj: 待转换对象

        Returns:
            Any: 字典/列表，或 ``None`` 表示不是容器

        Example:
            >>> DBConnectorCLI._to_container({"a": 1})
            {'a': 1}
            >>> DBConnectorCLI._to_container(3) is None
            True
        """
        for converter in (
            dict,
            lambda item: item._asdict(),  # pylint: disable=protected-access
            list,
        ):
            if converter is list and not (
                hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes))
            ):
                continue
            if converter is dict and not hasattr(obj, "keys"):
                continue
            try:
                return converter(obj)
            except (TypeError, ValueError, AttributeError):
                continue
        return None

    # ------------------------------------------------------------ 渲染
    def _show_results(self, results: Any) -> None:
        """以表格格式在终端显示查询结果

        终端宽度不足时自动选择可容纳的列，提示使用 --output 导出完整数据。

        **按位置渲染**：表头用去重后的显示名，单元格按列序号取值。
        此前按列名取值（``row.get(header)``），在 ``SELECT * FROM a JOIN b``
        这类重名列查询下会把同名列都显示成最后一个值（实测「张三」被显示成
        「研发部」），属静默输出错误数据。

        Args:
            results: 查询结果（``QueryResult`` 或行对象列表）
        """
        result = self._as_query_result(results)
        if not result.rows:
            print("没有结果")
            return
        headers = result.column_names()
        col_widths = self._calc_col_widths(headers, result)
        table_width = sum(col_widths[header] + 3 for header in headers) - 1
        term_width = self._get_term_width()
        if table_width > term_width:
            display_headers = self._pick_display_cols(headers, term_width, col_widths)
            hidden = len(headers) - len(display_headers)
            if display_headers:
                print(f"📋 显示 {len(display_headers)}/{len(headers)} 列（终端宽度限制）")
                print(f"🔍 隐藏了 {hidden} 列，使用 --output csv/json 查看完整数据")
                print()
                indices = [headers.index(name) for name in display_headers]
                trimmed_widths = {h: col_widths[h] for h in display_headers}
                self._print_table(display_headers, result.rows, trimmed_widths, indices)
            else:
                print("❌ 终端宽度过小，无法显示任何列")
                print("💡 建议使用 --output csv/json 参数导出数据查看")
        else:
            self._print_table(headers, result.rows, col_widths)

    def _get_term_width(self) -> int:
        """获取终端宽度

        Returns:
            int: 终端字符列数，失败时返回 120
        """
        try:
            return shutil.get_terminal_size().columns
        except OSError:
            return 120

    def _calc_col_widths(self, headers: List[str], rows: Any) -> Dict[str, int]:
        """计算每列最大显示宽度（上限50）

        **按位置**取单元格值，重复列名也能各自算宽。

        Args:
            headers: 去重后的显示名列表
            rows: 行数据；``QueryResult`` 或「行对象/元组」的可迭代对象

        Returns:
            Dict[str, int]: 列名到宽度的映射
        """
        max_width = 50
        materialized = rows.rows if isinstance(rows, QueryResult) else list(rows)
        col_widths: Dict[str, int] = {}
        for header in headers:
            col_widths[header] = self._get_display_width(str(header))
        for row in materialized:
            values = row if isinstance(row, tuple) else self._row_values(row)
            for index, header in enumerate(headers):
                value = values[index] if index < len(values) else ""
                w = self._get_display_width(str(value))
                col_widths[header] = max(col_widths[header], w)
        for header in headers:
            col_widths[header] = min(col_widths[header], max_width)
        return col_widths

    @staticmethod
    def _row_values(row: Any) -> Tuple[Any, ...]:
        """按位置取出整行的值（重复列名也保真）

        转发到 :func:`db_connector_tool.core.result.row_values`，保证 CLI 与
        驱动层用的是同一套取值语义。

        Args:
            row: 行对象

        Returns:
            Tuple[Any, ...]: 该行的值元组
        """
        return row_values(row)

    def _get_display_width(self, text: str) -> int:
        """计算字符串终端显示宽度（中文2，英文1）

        Args:
            text: 输入字符串

        Returns:
            int: 终端显示宽度
        """
        return sum(self._get_char_width(char) for char in text)

    def _get_char_width(self, char: str) -> int:
        """计算单字符终端显示宽度

        Args:
            char: 单个字符

        Returns:
            int: CJK统一汉字/CJK标点/全角字符返回2，其余返回1
        """
        if (
            "\u4e00" <= char <= "\u9fff"
            or "\u3000" <= char <= "\u303f"
            or "\uff00" <= char <= "\uffef"
        ):
            return 2
        return 1

    def _pick_display_cols(
        self, headers: List[str], term_width: int, col_widths: Dict[str, int]
    ) -> List[str]:
        """按终端宽度从左到右选择可容纳的列

        Args:
            headers: 所有表头
            term_width: 终端可用宽度
            col_widths: 列宽映射

        Returns:
            List[str]: 可容纳的列名列表
        """
        if not headers:
            return []
        selected: List[str] = []
        current = 0
        for header in headers:
            col_total = col_widths[header] + 3
            if current + col_total <= term_width:
                selected.append(header)
                current += col_total
            else:
                break
        return selected

    def _print_table(
        self,
        headers: List[str],
        rows: Any,
        col_widths: Dict[str, int],
        indices: Optional[List[int]] = None,
    ) -> None:
        """打印带边框的格式化数据表格

        Args:
            headers: 表头（去重后的显示名）
            rows: 行数据（元组序列，或可转成元组的行对象序列）
            col_widths: 列宽映射
            indices: 只渲染这些列序号（终端过窄时的裁剪）；``None`` 表示全部
        """
        sep_parts = ["-" * (col_widths[header] + 2) for header in headers]
        separator = "+" + "+".join(sep_parts) + "+"
        print(separator)
        self._print_row(headers, headers, col_widths, is_header=True)
        print(separator)
        for row in rows:
            values = row if isinstance(row, tuple) else self._row_values(row)
            cells = (
                [values[i] if i < len(values) else "" for i in indices]
                if indices is not None
                else list(values)
            )
            self._print_row(cells, headers, col_widths, is_header=False)
        print(separator)

    def _print_row(
        self,
        row_data: Union[List[str], Dict[str, Any], Tuple[Any, ...]],
        headers: List[str],
        col_widths: Dict[str, int],
        is_header: bool = False,
    ) -> None:
        """打印表格单行（居中，表头不截断）

        **按位置**取值：``row_data`` 是与 ``headers`` 等长的序列，
        因此重复列名各占一格、互不覆盖。

        Args:
            row_data: 行数据（表头为 List，数据行为按位置的序列）
            headers: 当前表头列表
            col_widths: 列宽映射
            is_header: 是否为表头行
        """
        if is_header:
            values: List[Any] = list(row_data)
        elif isinstance(row_data, (tuple, list)):
            values = list(row_data)
        else:
            values = list(self._row_values(row_data))

        cells: List[str] = []
        for index, header in enumerate(headers):
            raw = values[index] if index < len(values) else ""
            if is_header:
                cell_text = str(raw)
            else:
                cell_text = self._truncate_cell(str(raw), col_widths[header])
            actual = self._get_display_width(cell_text)
            padding = col_widths[header] - actual
            left_pad = padding // 2
            right_pad = padding - left_pad
            cells.append(f"{' ' * left_pad}{cell_text}{' ' * right_pad}")
        print(f"| {' | '.join(cells)} |")

    def _truncate_cell(self, value: str, max_width: int) -> str:
        """智能截断过长单元格（考虑CJK宽度）

        Args:
            value: 原始文本
            max_width: 最大显示宽度

        Returns:
            str: 截断后文本，末尾带 ...

        Example:
        >>> cli = DBConnectorCLI()
        >>> cli._truncate_cell("长文本内容", 5)
        '长...'
        """
        if self._get_display_width(value) <= max_width:
            return value
        current_width = 0
        truncated = ""
        for char in value:
            char_width = self._get_char_width(char)
            if current_width + char_width > max_width - 3:
                break
            truncated += char
            current_width += char_width
        return truncated + "..."
