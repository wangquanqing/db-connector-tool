"""SQL 解析工具模块 (SQL Parser Utilities)

提供语法感知的 SQL 语句分割功能，正确处理以下语法边界：
- 字符串常量（单引号、双引号，含 \\' 和 '' 转义）
- 美元引号（PostgreSQL $$...$$ / $tag$...$tag$）
- Oracle Q-引用（q'[...]' / q'{...}' / q'(...)' / q'<...>'）
- SQL Server 方括号标识符（[name]）
- MySQL/GBase 反引号标识符（`name`）
- N-字符串前缀（SQL Server N'...'）
- 行注释（--）和块注释（/* ... */）
- 大括号代码块（存储过程 / PL 块）
- BEGIN-END 嵌套块（排除 END IF/LOOP/CASE/WHILE/REPEAT）
- MySQL/GBase DELIMITER 指令
- Oracle / 终结符（PL/SQL 块结尾）
- SQL Server GO 批次分隔符

支持的数据库: Oracle / PostgreSQL / MySQL / SQL Server / SQLite / GBase

Example:
>>> from db_connector_tool.utils.sqlparse_utils import SQLStatementParser  # doctest: +SKIP
>>>
>>> parser = SQLStatementParser()
>>> statements = parser.parse("SELECT 1; SELECT 2;")
>>> print(len(statements))
2
>>> statements = parser.parse("SELECT q'[it's quoted]' FROM dual;")
>>> print(len(statements))
1
>>> sql = "DELIMITER &&\\nCREATE PROCEDURE t() BEGIN SET x=1; END&&\\nDELIMITER ;"
>>> statements = parser.parse(sql)
>>> print(len(statements))
1
>>> statements = parser.parse('''CREATE OR REPLACE PACKAGE BODY pkg AS
...     PROCEDURE p1 AS BEGIN NULL; END p1;
...     PROCEDURE p2 AS BEGIN NULL; END p2;
... END pkg;
... /
... ''')
>>> print(len(statements))
1
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

logger = logging.getLogger(__name__)

# 编码回退列表
ENCODING_FALLBACKS = ["utf-8", "utf-16", "gbk", "latin-1", "cp1252"]

# GO 批次分隔符正则
_BATCH_SEPARATOR_GO = re.compile(r"^\s*GO\b", re.IGNORECASE | re.MULTILINE)

# DELIMITER 指令正则
_DELIMITER_PATTERN = re.compile(r"^\s*DELIMITER\s+(\S+)", re.IGNORECASE | re.MULTILINE)

# END 后不应递减 begin_end_level 的关键字
_END_NO_DECREMENT = frozenset(
    {
        "IF",
        "LOOP",
        "CASE",
        "WHILE",
        "REPEAT",
        "PROCEDURE",
        "FUNCTION",
        "PACKAGE",
        "FOR",
    }
)

# 可开启复合语句的 CREATE 类型
_CREATE_COMPOUND_STARTS = frozenset(
    {
        "PROCEDURE",
        "FUNCTION",
        "TRIGGER",
        "PACKAGE",
    }
)

# Oracle Q-引用配对符号
_Q_QUOTE_PAIRS = {
    "[": "]",
    "{": "}",
    "(": ")",
    "<": ">",
}


@dataclass
class _ParserState:
    """解析器内部状态数据类

    将所有可变状态字段集中在数据类中，减少 SQLStatementParser
    的实例属性数量。
    """

    # 字符串引用状态
    in_string: bool = False
    string_quote: Optional[str] = None
    _has_n_string_prefix: bool = False
    # 注释状态
    in_line_comment: bool = False
    in_block_comment: bool = False
    # 美元引号状态
    in_dollar_quote: bool = False
    dollar_tag: Optional[str] = None
    # 反引号状态 (MySQL/GBase)
    in_backtick: bool = False
    _backtick_escaped: bool = False
    # 方括号状态 (SQL Server)
    in_bracket: bool = False
    # Q-引用状态 (Oracle)
    in_q_quote: bool = False
    q_quote_close: Optional[str] = None
    # 嵌套层级
    begin_end_level: int = 0
    brace_level: int = 0
    # 复合语句深度（存储过程/函数/触发器）
    _compound_depth: int = 0
    # 包体上下文 (Oracle PACKAGE BODY)
    _in_package_body: bool = False
    # 多字符跳越计数器（处理 $$、// 等分隔符）
    _skip_count: int = 0
    # 关键字词法缓冲
    word_buffer: str = ""
    _keyword_history: List[str] = field(default_factory=list)


def read_and_split_sql_file(file_path: str) -> List[str]:
    """读取 SQL 文件并分割为独立语句 (Read and Split SQL File)

    自动尝试多种编码格式解码文件内容。

    Args:
        file_path: SQL 文件路径

    Returns:
        List[str]: 独立 SQL 语句列表

    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 所有编码尝试均失败

    Example:
    >>> statements = read_and_split_sql_file("script.sql")  # doctest: +SKIP
    >>> print(len(statements))  # doctest: +SKIP
    5
    """
    parser = SQLStatementParser()
    return parser.parse_file(file_path)


class SQLStatementParser:
    """SQL 语句解析器类 (SQL Statement Parser)

    根据语法上下文分割 SQL 多语句内容，支持 Oracle / PostgreSQL / MySQL /
    SQL Server / SQLite / GBase 全部语法，正确处理存储过程和触发器中的
    多级嵌套结构。

    Example:
    >>> parser = SQLStatementParser()
    >>> sql = '''SELECT 1;
    ... /* 块注释中的;不触发分割 */
    ... SELECT 2; -- 行注释中的;也不触发
    ... '''
    >>> statements = parser.parse(sql)
    >>> print(len(statements))
    3
    >>> parser = SQLStatementParser()
    >>> pg = "CREATE FUNCTION f() RETURNS text AS $$SELECT 'hello; world';$$ LANGUAGE sql;"
    >>> statements = parser.parse(pg)
    >>> print(len(statements))
    1
    >>> parser = SQLStatementParser()
    >>> mysql_sql = "SELECT `na;me` FROM users;"
    >>> statements = parser.parse(mysql_sql)
    >>> print(len(statements))
    1
    >>> parser = SQLStatementParser()
    >>> mssql = "SELECT [na;me] FROM users;"
    >>> statements = parser.parse(mssql)
    >>> print(len(statements))
    1
    >>> parser = SQLStatementParser()
    >>> oracle_sql = "SELECT q'[it's not; split]' FROM dual;"
    >>> statements = parser.parse(oracle_sql)
    >>> print(len(statements))
    1
    """

    # 以下类型注解仅供静态分析参考，实际通过 _state 委派访问
    in_string: bool
    string_quote: Optional[str]
    _has_n_string_prefix: bool
    in_line_comment: bool
    in_block_comment: bool
    in_dollar_quote: bool
    dollar_tag: Optional[str]
    in_backtick: bool
    _backtick_escaped: bool
    in_bracket: bool
    in_q_quote: bool
    q_quote_close: Optional[str]
    begin_end_level: int
    brace_level: int
    _compound_depth: int
    _in_package_body: bool
    _skip_count: int
    word_buffer: str
    _keyword_history: List[str]

    def __init__(self):
        """初始化 SQL 语句解析器

        设置解析器内部状态变量，包括分隔符、引用跟踪、嵌套层级等。
        """
        self._state = _ParserState()
        self.statements: List[str] = []
        self.current_statement: str = ""
        self.current_delimiter: str = ";"
        # 以下通过 __setattr__ 委派到 _state，同时满足 pylint 静态分析
        self.in_string = False
        self.string_quote = None
        self._has_n_string_prefix = False
        self.in_line_comment = False
        self.in_block_comment = False
        self.in_dollar_quote = False
        self.dollar_tag = None
        self.in_backtick = False
        self._backtick_escaped = False
        self.in_bracket = False
        self.in_q_quote = False
        self.q_quote_close = None
        self.begin_end_level = 0
        self.brace_level = 0
        self._compound_depth = 0
        self._in_package_body = False
        self._skip_count = 0
        self.word_buffer = ""
        self._keyword_history = []

    def __getattr__(self, name: str):
        """将缺失的属性访问委派到 _state 数据类"""
        state = object.__getattribute__(self, "_state")
        return getattr(state, name)

    def __setattr__(self, name: str, value):
        """将非顶层属性写入委派到 _state 数据类"""
        if name in ("statements", "current_statement", "current_delimiter", "_state"):
            object.__setattr__(self, name, value)
        else:
            state = object.__getattribute__(self, "_state")
            object.__setattr__(state, name, value)

    def parse(self, sql_content: str) -> List[str]:
        """解析 SQL 内容并分割为独立语句 (Parse SQL Content)

        Args:
            sql_content: SQL 文本内容

        Returns:
            List[str]: 独立语句列表

        Example:
        >>> parser = SQLStatementParser()
        >>> statements = parser.parse("SELECT * FROM users; SELECT * FROM products;")
        >>> print(len(statements))
        2
        """
        if not sql_content:
            return []

        self._reset_state()

        segments = self._split_by_delimiter_commands(sql_content)

        for delimiter, segment_content in segments:
            self.current_delimiter = delimiter
            self._parse_segment(segment_content)

        final_stmt = self._finalize_statement()
        if final_stmt:
            self.statements.append(final_stmt)

        statements = self._filter_statements()
        statements = self._split_batch_separators(statements)
        return statements

    def parse_file(self, file_path: str) -> List[str]:
        """解析 SQL 文件并分割为独立语句 (Parse SQL File)

        自动尝试多种编码格式解码文件内容。

        Args:
            file_path: SQL 文件路径

        Returns:
            List[str]: 独立语句列表

        Raises:
            FileNotFoundError: 文件不存在
            ValueError: 所有编码尝试均失败

        Example:
        >>> parser = SQLStatementParser()
        >>> statements = parser.parse_file("script.sql")  # doctest: +SKIP
        >>> print(len(statements))  # doctest: +SKIP
        3
        """
        last_error = None
        for encoding in ENCODING_FALLBACKS:
            try:
                with open(file_path, "r", encoding=encoding) as file:
                    sql_content = file.read()
                if (
                    encoding.startswith("utf-16")
                    and sql_content
                    and sql_content[0] == "\ufeff"
                ):
                    sql_content = sql_content[1:]
                return self.parse(sql_content)
            except UnicodeDecodeError as error:
                last_error = error
                continue

        logger.error(
            "无法解码SQL文件 %s，已尝试编码: %s", file_path, ENCODING_FALLBACKS
        )
        raise ValueError(
            f"无法解码SQL文件，已尝试编码: {ENCODING_FALLBACKS}"
        ) from last_error

    def get_statement_count(self) -> int:
        """获取解析后的语句数量 (Get Statement Count)

        Returns:
            int: 语句数量

        Example:
        >>> parser = SQLStatementParser()
        >>> _ = parser.parse("SELECT 1; SELECT 2;")
        >>> print(parser.get_statement_count())
        2
        """
        return len(self.statements)

    def get_statements_by_type(self, statement_type: Optional[str] = None) -> List[str]:
        """按类型过滤语句 (Get Statements by Type)

        Args:
            statement_type: 语句类型关键字（SELECT/INSERT 等），None 返回全部

        Returns:
            List[str]: 过滤后的语句列表

        Example:
        >>> parser = SQLStatementParser()
        >>> _ = parser.parse("SELECT 1; INSERT INTO t VALUES(1);")
        >>> print(len(parser.get_statements_by_type("SELECT")))
        1
        """
        if not statement_type:
            return self.statements.copy()

        return [
            stmt
            for stmt in self.statements
            if stmt.strip().upper().startswith(statement_type.upper())
        ]

    @staticmethod
    def _is_comment_only(stmt: str) -> bool:
        """检查语句是否仅包含注释内容

        Args:
            stmt: 待检查的语句文本

        Returns:
            bool: 是否仅为注释
        """
        stripped = stmt.strip()
        if not stripped:
            return True
        if stripped.startswith("/*") and stripped.endswith("*/"):
            return True
        return all(
            not line.strip() or line.strip().startswith("--")
            for line in stripped.splitlines()
        )

    def validate_syntax(self, sql_content: str) -> bool:
        """验证 SQL 语法解析有效性 (Validate Syntax)

        检查内容是否能被正确解析为至少一条非空、非注释语句。

        Args:
            sql_content: SQL 文本内容

        Returns:
            bool: 语法是否有效

        Example:
        >>> parser = SQLStatementParser()
        >>> parser.validate_syntax("SELECT * FROM users;")
        True
        >>> parser.validate_syntax("/* 仅是注释 */")
        False
        """
        try:
            statements = self.parse(sql_content)
            return len(statements) > 0 and all(
                stmt and not stmt.isspace() and not self._is_comment_only(stmt)
                for stmt in statements
            )
        except (ValueError, TypeError, IndexError, AttributeError):
            return False

    # ==================================================================
    # 内部状态管理
    # ==================================================================

    def _reset_state(self):
        """重置全部解析器状态"""
        object.__setattr__(self, "_state", _ParserState())
        self.statements = []
        self.current_statement = ""
        self.current_delimiter = ";"

    def _reset_segment_state(self):
        """重置段落级解析状态（保留全局 statements 列表）"""
        object.__setattr__(self, "_state", _ParserState())
        self.current_statement = ""

    # ==================================================================
    # DELIMITER 指令分段
    # ==================================================================

    @staticmethod
    def _split_by_delimiter_commands(sql_content: str) -> List[tuple]:
        """按 DELIMITER 指令将内容切分为 (分隔符, 段落) 的段落列表

        遍历 SQL 内容中的 DELIMITER 指令，将每个指令变更前的段落
        与对应的分隔符组合，用于后续逐段解析。
        """
        segments: List[tuple] = []
        current_pos = 0
        current_delimiter = ";"

        for match in _DELIMITER_PATTERN.finditer(sql_content):
            segment_content = sql_content[current_pos : match.start()]
            if segment_content.strip():
                segments.append((current_delimiter, segment_content))

            new_delimiter = match.group(1).strip()
            if new_delimiter:
                current_delimiter = new_delimiter
            current_pos = match.end()

        final_content = sql_content[current_pos:]
        if final_content.strip():
            segments.append((current_delimiter, final_content))

        return segments

    def _parse_segment(self, segment_content: str):
        """以指定分隔符解析一个段落

        逐字符遍历段落内容，通过 _process_char 处理每个字符。
        """
        self._reset_segment_state()

        for index, char in enumerate(segment_content):
            self._process_char(char, index, segment_content)

        final_stmt = self._finalize_statement()
        if final_stmt:
            self.statements.append(final_stmt)

    # ==================================================================
    # 逐字符处理 - 主调度
    # ==================================================================

    def _process_char(self, char: str, index: int, sql_content: str):
        """逐字符处理主调度方法

        根据当前状态将字符分派到对应的处理器：注释模式、引用模式
        直接追加字符；正常模式下检测各种边界并进入对应子状态。

        Args:
            char: 当前字符
            index: 字符位置索引
            sql_content: 完整 SQL 内容（用于前后视检测）
        """
        if self._skip_count > 0:
            self._skip_count -= 1
            return

        if self._handle_passive_mode(char, index, sql_content):
            return

        if self._attempt_boundary_enter(char, index, sql_content):
            return

        if char.isalnum() or char == "_":
            self.word_buffer += char
        else:
            self._process_keyword(index, sql_content)
            self.word_buffer = ""

        if self._process_delimiter(char, index, sql_content):
            return

        if self._attempt_dollar_quote(char, index, sql_content):
            return

        self.current_statement += char

    # ==================================================================
    # 被动模式处理（已在注释/引用内部时，只追加字符并检测退出）
    # ==================================================================

    def _handle_passive_mode(self, char: str, index: int, sql_content: str) -> bool:
        """处理所有被动模式中的字符追加与退出检测

        当解析器已处于注释、引用等模式内部时，只需将字符追加到
        current_statement 并检测是否满足退出条件。

        Returns:
            bool: 是否在被动模式中消费了该字符
        """
        consumed = True
        if self.in_line_comment:
            self._append_line_comment_char(char)
        elif self.in_block_comment:
            self._append_block_comment_char(char, index, sql_content)
        elif self.in_dollar_quote:
            self._append_dollar_quote_char(char, index, sql_content)
        elif self.in_backtick:
            self._append_backtick_char(char)
        elif self.in_bracket:
            self._append_bracket_char(char)
        elif self.in_q_quote:
            self._append_q_quote_char(char, index, sql_content)
        else:
            consumed = False
        return consumed

    def _append_line_comment_char(self, char: str):
        """行注释模式中追加字符"""
        self.current_statement += char
        if char == "\n":
            self.in_line_comment = False

    def _append_block_comment_char(self, char: str, index: int, sql_content: str):
        """块注释模式中追加字符"""
        self.current_statement += char
        if char == "/" and index > 0 and sql_content[index - 1] == "*":
            self.in_block_comment = False

    def _append_dollar_quote_char(self, char: str, index: int, sql_content: str):
        """美元引号模式中追加字符"""
        self.current_statement += char
        if self._detect_dollar_quote_end(index, sql_content):
            self.in_dollar_quote = False
            self.dollar_tag = None

    def _append_backtick_char(self, char: str):
        """反引号模式中追加字符"""
        self.current_statement += char
        if self._backtick_escaped:
            self._backtick_escaped = False
            return
        if char == "`":
            self.in_backtick = False

    def _append_bracket_char(self, char: str):
        """方括号模式中追加字符"""
        self.current_statement += char
        if char == "]":
            self.in_bracket = False

    def _append_q_quote_char(self, char: str, index: int, sql_content: str):
        """Q-引用模式中追加字符"""
        self.current_statement += char
        if char == self.q_quote_close:
            peek = index + 1
            if peek < len(sql_content) and sql_content[peek] == "'":
                self.current_statement += "'"
                self.in_q_quote = False
                self.q_quote_close = None

    # ==================================================================
    # 边界检测（尝试进入新的注释/引用模式）
    # ==================================================================

    def _attempt_boundary_enter(self, char: str, index: int, sql_content: str) -> bool:
        """按优先级尝试进入各种语法边界模式

        依次检测行注释、块注释、反引号、方括号、Q-引用、
        字符串常量和花括号边界。首个匹配即返回 True。

        Returns:
            bool: 是否成功进入某个边界模式
        """
        return (
            self._attempt_line_comment(char, index, sql_content)
            or self._attempt_block_comment(char, index, sql_content)
            or self._attempt_backtick(char)
            or self._attempt_bracket(char)
            or self._attempt_q_quote(char, index, sql_content)
            or self._process_string_literal(char, index, sql_content)
            or self._process_braces(char)
        )

    # ==================================================================
    # 注释处理
    # ==================================================================

    def _attempt_line_comment(self, char: str, index: int, sql_content: str) -> bool:
        """尝试进入行注释模式（--）"""
        if (
            char == "-"
            and index + 1 < len(sql_content)
            and sql_content[index + 1] == "-"
        ):
            self.in_line_comment = True
            self.current_statement += char
            return True
        return False

    def _attempt_block_comment(self, char: str, index: int, sql_content: str) -> bool:
        """尝试进入块注释模式（/* */）"""
        if (
            char == "/"
            and index + 1 < len(sql_content)
            and sql_content[index + 1] == "*"
        ):
            self.in_block_comment = True
            self.current_statement += char
            return True
        return False

    # ==================================================================
    # PostgreSQL 美元引号
    # ==================================================================

    def _attempt_dollar_quote(self, char: str, index: int, sql_content: str) -> bool:
        """尝试进入美元引号模式（$$...$$ / $tag$...$tag$）

        检测 $ 后紧接的标签内容（可为空），验证结束标签是否存在于
        剩余内容中。进入后消费整个开始标签并设置跳越计数器。
        """
        if char != "$":
            return False

        closing_tag_pos = sql_content.find("$", index + 1)
        if closing_tag_pos == -1:
            return False

        tag_content = sql_content[index + 1 : closing_tag_pos]
        if "$" in tag_content:
            return False

        start_tag = f"${tag_content}$"
        end_tag = f"${tag_content}$"

        remaining = sql_content[index + len(start_tag) :]
        if remaining.find(end_tag) == -1 and tag_content != "":
            return False

        self.in_dollar_quote = True
        self.dollar_tag = end_tag
        self.current_statement += start_tag
        if len(start_tag) > 1:
            self._skip_count = len(start_tag) - 1
        return True

    def _detect_dollar_quote_end(self, index: int, sql_content: str) -> bool:
        """检测是否到达美元引号结束标签"""
        if not self.dollar_tag:
            return False

        tag_len = len(self.dollar_tag)
        if index >= tag_len - 1:
            candidate = sql_content[index - tag_len + 1 : index + 1]
            return candidate == self.dollar_tag
        return False

    # ==================================================================
    # MySQL/GBase 反引号
    # ==================================================================

    def _attempt_backtick(self, char: str) -> bool:
        """尝试进入反引号标识符模式（`name`）"""
        if char == "`":
            self.in_backtick = True
            self.current_statement += char
            return True
        return False

    # ==================================================================
    # SQL Server 方括号
    # ==================================================================

    def _attempt_bracket(self, char: str) -> bool:
        """尝试进入方括号标识符模式（[name]）"""
        if char == "[":
            self.in_bracket = True
            self.current_statement += char
            return True
        return False

    # ==================================================================
    # Oracle Q-引用
    # ==================================================================

    def _attempt_q_quote(self, char: str, index: int, sql_content: str) -> bool:
        """尝试进入 Oracle Q-引用模式

        Q'[...]' / Q'{...}' / Q'(...)' / Q'<...>'
        同时处理 N 字符前缀 (N'...').
        """
        upper = char.upper()
        if upper not in ("Q", "N"):
            return False

        if upper == "N":
            return self._detect_n_string_prefix(index, sql_content)

        return self._detect_q_quote_prefix(index, sql_content)

    def _detect_n_string_prefix(self, index: int, sql_content: str) -> bool:
        """检测 N'...' 前缀（SQL Server Unicode 字符串）

        检测到 N' 时设置 _has_n_string_prefix 标志位、追加 N 到
        当前语句并返回 True 表示消费该字符；_process_string_literal
        在后续遇到 ' 时会利用该标志补全 N' 输出。
        """
        if index + 1 < len(sql_content) and sql_content[index + 1] == "'":
            self._has_n_string_prefix = True
            self.current_statement += "N"
            return True
        return False

    def _detect_q_quote_prefix(self, index: int, sql_content: str) -> bool:
        """检测 Q' 前缀并进入配对引号模式"""
        if index + 2 >= len(sql_content):
            return False

        if sql_content[index + 1] != "'":
            return False

        open_char = sql_content[index + 2]
        close_char = _Q_QUOTE_PAIRS.get(open_char)
        if close_char is None:
            return False

        self.in_q_quote = True
        self.q_quote_close = close_char
        self.current_statement += "q'"
        self.current_statement += open_char
        return True

    # ==================================================================
    # 字符串常量
    # ==================================================================

    def _process_string_literal(self, char: str, index: int, sql_content: str) -> bool:
        """处理字符串常量（'...' / "..."）

        进入：支持 SQL Server N 前缀自动补回。
        退出：检测 \\' 转义和 '' 加倍转义，防止提前退出。

        Returns:
            bool: 是否由本方法消费了该字符
        """
        had_n_prefix = self._has_n_string_prefix
        self._has_n_string_prefix = False

        if not self.in_string:
            if char in ("'", '"'):
                self._enter_string(char)
                if had_n_prefix:
                    self.current_statement = self.current_statement[:-1] + "N" + char
                return True
            return False

        if char != self.string_quote:
            return False

        # 反斜线转义
        if index > 0 and sql_content[index - 1] == "\\":
            return False

        # 双倍引号转义（'' 或 ""）
        if index + 1 < len(sql_content) and sql_content[index + 1] == self.string_quote:
            return False

        self.in_string = False
        self.string_quote = None
        self.current_statement += char
        return True

    def _enter_string(self, quote_char: str):
        """进入字符串模式"""
        self.in_string = True
        self.string_quote = quote_char
        self.current_statement += quote_char

    # ==================================================================
    # 大括号（PL 块）
    # ==================================================================

    def _process_braces(self, char: str) -> bool:
        """处理大括号层级（{ ... }）

        用于 PL/SQL 风格的大括号代码块追踪。
        """
        if char == "{":
            self.brace_level += 1
            self.current_statement += char
            return True
        if char == "}":
            self.brace_level = max(0, self.brace_level - 1)
            self.current_statement += char
            return True
        return False

    # ==================================================================
    # 关键字检测
    # ==================================================================

    def _process_keyword(self, index: int, sql_content: str):
        """检测词法缓冲中的关键字并分发处理

        维护最近 5 个关键字的历史记录，用于上下文判断
        （如 PACKAGE BODY 组合检测）。
        """
        keyword = self.word_buffer.strip().upper()
        if not keyword:
            return

        self._keyword_history.append(keyword)
        if len(self._keyword_history) > 5:
            self._keyword_history.pop(0)

        if keyword == "BEGIN":
            self._handle_begin_keyword(index, sql_content)
        elif keyword == "END":
            self._handle_end_keyword(index, sql_content)
        elif keyword in ("PROCEDURE", "FUNCTION", "TRIGGER"):
            self._compound_depth = max(self._compound_depth, 1)
        elif keyword == "PACKAGE":
            self._handle_package_keyword(index, sql_content)
        elif keyword == "BODY":
            self._handle_body_keyword()
        elif keyword == "DECLARE":
            self._compound_depth = max(self._compound_depth, 1)
        elif keyword == "DO":
            self._compound_depth = max(self._compound_depth, 1)

    def _handle_begin_keyword(self, index: int, sql_content: str):
        """处理 BEGIN 关键字

        排除事务控制语句（BEGIN TRANSACTION/WORK/DISTRIBUTED），
        仅对代码块 BEGIN 递增嵌套层级。
        """
        ahead = sql_content[index : index + 30].lstrip().upper()
        if not (
            ahead.startswith("TRANSACTION")
            or ahead.startswith("WORK")
            or ahead.startswith("DISTRIBUTED")
        ):
            self.begin_end_level += 1
            self._compound_depth = max(self._compound_depth, 1)

    def _handle_end_keyword(self, index: int, sql_content: str):
        """处理 END 关键字

        前视检测 END 后的单词，若为流程控制关键字
        （IF/LOOP/CASE/WHILE/REPEAT 等）则不递减 begin_end_level，
        避免存储过程内部的流程控制 END 破坏嵌套追踪。
        """
        ahead = sql_content[index : index + 30].lstrip().upper()
        next_word = self._extract_next_word(ahead)

        if next_word in _END_NO_DECREMENT:
            return

        if self.begin_end_level > 0:
            self.begin_end_level -= 1

        # 回到最外层时递减复合语句深度
        if self.begin_end_level == 0 and not self._in_package_body:
            if self._compound_depth > 0:
                self._compound_depth -= 1

    def _handle_package_keyword(self, index: int, sql_content: str):
        """处理 PACKAGE 关键字

        PACKAGE BODY 进入包体上下文（抑制所有分号分割），
        单独的 PACKAGE 声明仅递增复合深度。
        """
        ahead = sql_content[index : index + 30].lstrip().upper()
        if ahead.startswith("BODY"):
            self._in_package_body = True
        else:
            self._compound_depth = max(self._compound_depth, 1)

    def _handle_body_keyword(self):
        """处理 BODY 关键字

        结合关键字历史检测 CREATE PACKAGE BODY 组合，
        确认后进入包体上下文。
        """
        history_upper = " ".join(self._keyword_history[-3:]).upper()
        if "CREATE" in history_upper and "PACKAGE" in history_upper:
            self._in_package_body = True
            self._compound_depth = max(self._compound_depth, 1)

    @staticmethod
    def _extract_next_word(text: str) -> str:
        """从文本提取第一个单词（字母或下划线组成）

        用于 END 关键字的后续单词前视检测。

        Args:
            text: 待提取的文本

        Returns:
            str: 首个单词的大写形式
        """
        result = []
        for ch in text:
            if ch.isalpha() or ch == "_":
                result.append(ch)
            else:
                break
        return "".join(result).upper()

    # ==================================================================
    # 分隔符处理
    # ==================================================================

    def _process_delimiter(self, char: str, index: int, sql_content: str) -> bool:
        """处理当前分隔符匹配

        支持单字符（;）和多字符（&&, //, $$ 等由 DELIMITER 指定）分隔符。
        匹配到分隔符后，根据 _should_suppress_split 判断是否需要
        抑制分割（例如当前处于 BEGIN-END 块内）。

        Returns:
            bool: 是否由本方法消费了该字符
        """
        delim = self.current_delimiter
        if not delim:
            return False

        if len(delim) == 1:
            if char != delim:
                return False
            matched = delim
        else:
            peek = sql_content[index : index + len(delim)]
            if peek != delim:
                return False
            matched = delim

        self.current_statement += matched
        self.word_buffer = ""

        if len(matched) > 1:
            self._skip_count = len(matched) - 1

        if self._should_suppress_split():
            return True

        stmt = self.current_statement.strip()
        if stmt and not stmt.isspace():
            self.statements.append(stmt)
        self.current_statement = ""
        return True

    def _should_suppress_split(self) -> bool:
        """判断当前是否应抑制分隔符分割

        以下任一状态为 True 时不应分割：
        - 字符串内 / 反引号 / 方括号 / Q-引用 / 美元引号内
        - 包体上下文内（Oracle PACKAGE BODY）
        - BEGIN-END 嵌套层级内
        - 复合语句（存储过程/函数/触发器）内
        - 大括号代码块内
        """
        return (
            self.in_string
            or self.in_backtick
            or self.in_bracket
            or self.in_q_quote
            or self.in_dollar_quote
            or self._in_package_body
            or self.begin_end_level > 0
            or self._compound_depth > 0
            or self.brace_level > 0
        )

    # ==================================================================
    # 语句收集与过滤
    # ==================================================================

    def _finalize_statement(self) -> Optional[str]:
        """完成当前语句的收集

        清除 current_statement 缓冲并返回非空语句。

        Returns:
            Optional[str]: 非空语句文本，无内容时返回 None
        """
        stmt = self.current_statement.strip()
        self.current_statement = ""
        if stmt and not stmt.isspace():
            return stmt
        return None

    def _filter_statements(self) -> List[str]:
        """过滤空白语句

        去除解析结果中的空白和仅空白字符组成的条目。
        """
        return [stmt for stmt in self.statements if stmt and not stmt.isspace()]

    # ==================================================================
    # 后处理：GO 批次分隔符 与 Oracle / 终结符
    # ==================================================================

    @staticmethod
    def _split_batch_separators(statements: List[str]) -> List[str]:
        """按 GO 批次分隔符二次分割语句

        SQL Server 脚本中 GO 作为批次分隔符，一条语句内可能包含
        多个 GO 分隔的子批次。此方法将各子批次独立列出，
        并清理每段的 Oracle / 终结符。

        Args:
            statements: 初步分割后的语句列表

        Returns:
            List[str]: GO 分割并清理后的最终语句列表
        """
        result: List[str] = []

        for stmt in statements:
            parts = _BATCH_SEPARATOR_GO.split(stmt)
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                cleaned = SQLStatementParser._clean_oracle_slash(part)
                if cleaned:
                    result.append(cleaned)

        return result

    @staticmethod
    def _clean_oracle_slash(stmt: str) -> Optional[str]:
        """清理 Oracle PL/SQL 块末尾的独立 / 终结符

        Oracle PL/SQL 代码块末尾通常以单独一行的 / 结束。
        此方法移除独立行上的 / 符号。

        Args:
            stmt: 待清理的语句文本

        Returns:
            Optional[str]: 清理后的语句文本，清空后返回 None
        """
        lines = stmt.split("\n")
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped == "/":
                continue
            cleaned_lines.append(line)

        result = "\n".join(cleaned_lines).strip()
        if not result:
            return None
        return result
