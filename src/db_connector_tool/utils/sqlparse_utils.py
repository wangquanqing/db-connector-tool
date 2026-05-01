"""SQL解析工具模块 (SQL Parser Utilities)

提供 SQL 语句解析和分割功能，支持语法感知的语句分割，
能正确处理字符串常量、存储过程等复杂语法结构。

Example:
>>> from db_connector_tool.utils.sqlparse_utils import SQLStatementParser, read_and_split_sql_file
>>>
>>> parser = SQLStatementParser()
>>> statements = parser.parse("SELECT 1; SELECT 2;")
>>> print(len(statements))
2
>>>
>>> statements = read_and_split_sql_file("script.sql")
"""

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


def read_and_split_sql_file(file_path: str) -> List[str]:
    """读取 SQL 文件并分割为独立语句

    自动处理不同编码格式（utf-8, gbk）。

    Args:
        file_path: SQL 文件路径

    Returns:
        List[str]: 语句列表

    Raises:
        FileNotFoundError: 文件不存在
        ValueError: 文件编码不支持

    Example:
    >>> statements = read_and_split_sql_file("script.sql")
    >>> print(len(statements))
    5
    """
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            sql_content = file.read()
        return _split_sql_statements(sql_content)
    except UnicodeDecodeError:
        try:
            with open(file_path, "r", encoding="gbk") as file:
                sql_content = file.read()
            return _split_sql_statements(sql_content)
        except UnicodeDecodeError as error:
            logger.error("无法解码SQL文件: %s", error)
            raise ValueError(f"无法解码SQL文件，请检查文件编码: {error}") from error


def _split_sql_statements(sql_content: str) -> List[str]:
    """将 SQL 内容分割为独立语句（内部函数）

    Args:
        sql_content: 原始 SQL 内容

    Returns:
        List[str]: 语句列表
    """
    parser = SQLStatementParser()
    return parser.parse(sql_content)


class SQLStatementParser:
    """SQL 语句解析器类 (SQL Statement Parser)

    根据语法上下文分割 SQL 语句，正确处理字符串常量、
    存储过程、大括号等复杂语法结构。

    Example:
    >>> parser = SQLStatementParser()
    >>> statements = parser.parse("SELECT 1; SELECT 2; CREATE PROCEDURE p() BEGIN SELECT 3; END;")
    >>> for stmt in statements:
    ...     print(stmt)
    """

    def __init__(self):
        """初始化解析器状态"""
        self.statements: List[str] = []
        self.current_statement: str = ""
        self.in_string: bool = False
        self.string_quote: Optional[str] = None
        self.in_procedure: bool = False
        self.brace_level: int = 0

    def parse(self, sql_content: str) -> List[str]:
        """解析 SQL 内容并分割为独立语句

        Args:
            sql_content: SQL 内容

        Returns:
            List[str]: 语句列表

        Example:
        >>> parser = SQLStatementParser()
        >>> statements = parser.parse("SELECT * FROM users; SELECT * FROM products;")
        >>> print(len(statements))
        2
        """
        self._reset_state()

        for index, char in enumerate(sql_content):
            self._process_char(char, index, sql_content)

        self._add_final_statement()
        return self._filter_statements()

    def parse_file(self, file_path: str, encoding: str = "utf-8") -> List[str]:
        """解析 SQL 文件并分割为独立语句

        Args:
            file_path: SQL 文件路径
            encoding: 文件编码，默认 utf-8

        Returns:
            List[str]: 语句列表

        Raises:
            FileNotFoundError: 文件不存在
            UnicodeDecodeError: 编码不支持

        Example:
        >>> parser = SQLStatementParser()
        >>> statements = parser.parse_file("script.sql")
        >>> print(len(statements))
        3
        """
        try:
            with open(file_path, "r", encoding=encoding) as file:
                sql_content = file.read()
            return self.parse(sql_content)
        except UnicodeDecodeError:
            for alt_encoding in ["gbk", "latin-1", "cp1252"]:
                try:
                    with open(file_path, "r", encoding=alt_encoding) as file:
                        sql_content = file.read()
                    return self.parse(sql_content)
                except UnicodeDecodeError:
                    continue
            raise

    def get_statement_count(self) -> int:
        """获取解析后的语句数量

        Returns:
            int: 语句数量

        Example:
        >>> parser = SQLStatementParser()
        >>> statements = parser.parse("SELECT 1; SELECT 2;")
        >>> print(parser.get_statement_count())
        2
        """
        return len(self.statements)

    def get_statements_by_type(self, statement_type: Optional[str] = None) -> List[str]:
        """按类型过滤语句

        Args:
            statement_type: 语句类型关键字（SELECT, INSERT 等），None 返回全部

        Returns:
            List[str]: 过滤后的语句列表

        Example:
        >>> parser = SQLStatementParser()
        >>> parser.parse("SELECT 1; INSERT INTO t VALUES(1);")
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

    def validate_syntax(self, sql_content: str) -> bool:
        """验证 SQL 语法解析有效性

        Args:
            sql_content: SQL 内容

        Returns:
            bool: 语法是否有效

        Example:
        >>> parser = SQLStatementParser()
        >>> parser.validate_syntax("SELECT * FROM users;")
        True
        """
        try:
            statements = self.parse(sql_content)
            return len(statements) > 0 and all(
                stmt and not stmt.isspace() for stmt in statements
            )
        except (ValueError, TypeError, IndexError, AttributeError):
            return False

    def _reset_state(self):
        """重置解析器状态"""
        self.statements = []
        self.current_statement = ""
        self.in_string = False
        self.string_quote = None
        self.in_procedure = False
        self.brace_level = 0

    def _process_char(self, char: str, index: int, sql_content: str):
        """处理单个字符

        Args:
            char: 当前字符
            index: 字符索引
            sql_content: 完整的 SQL 内容
        """
        if self._handle_string_literal(char, index, sql_content):
            return
        if self._handle_braces(char):
            return
        if self._handle_semicolon(char):
            return

        self.current_statement += char

    def _handle_string_literal(self, char: str, index: int, sql_content: str) -> bool:
        """处理字符串常量边界

        Args:
            char: 当前字符
            index: 字符索引
            sql_content: 完整的 SQL 内容

        Returns:
            bool: 是否已处理
        """
        if char in ("'", '"') and not self.in_string:
            self._start_string(char)
            return True
        if char == self.string_quote and self.in_string:
            self._end_string(char, index, sql_content)
            return True
        return False

    def _start_string(self, quote_char: str):
        """进入字符串常量状态

        Args:
            quote_char: 引号字符
        """
        self.in_string = True
        self.string_quote = quote_char
        self.current_statement += quote_char

    def _end_string(self, quote_char: str, index: int, sql_content: str):
        """退出字符串常量状态

        Args:
            quote_char: 引号字符
            index: 字符索引
            sql_content: 完整的 SQL 内容
        """
        if index > 0 and sql_content[index - 1] == "\\":
            self.current_statement += quote_char
        else:
            self.in_string = False
            self.string_quote = None
            self.current_statement += quote_char

    def _handle_braces(self, char: str) -> bool:
        """处理大括号（存储过程边界）

        Args:
            char: 当前字符

        Returns:
            bool: 是否已处理
        """
        if char == "{" and not self.in_string:
            self._open_brace()
            return True
        if char == "}" and not self.in_string:
            self._close_brace()
            return True
        return False

    def _open_brace(self):
        """进入大括号层级"""
        self.brace_level += 1
        self.in_procedure = self.brace_level > 0
        self.current_statement += "{"

    def _close_brace(self):
        """退出大括号层级"""
        self.brace_level = max(0, self.brace_level - 1)
        self.in_procedure = self.brace_level > 0
        self.current_statement += "}"

    def _handle_semicolon(self, char: str) -> bool:
        """处理分号分割

        Args:
            char: 当前字符

        Returns:
            bool: 是否已处理
        """
        if char == ";" and not self.in_string and not self.in_procedure:
            self._split_statement()
            return True
        return False

    def _split_statement(self):
        """在分号处分割语句"""
        self.current_statement += ";"
        stmt = self.current_statement.strip()
        if stmt and not stmt.isspace():
            self.statements.append(stmt)
        self.current_statement = ""

    def _add_final_statement(self):
        """添加末尾可能无分号的语句"""
        if (
            self.current_statement.strip()
            and not self.current_statement.strip().isspace()
        ):
            self.statements.append(self.current_statement.strip())

    def _filter_statements(self) -> List[str]:
        """过滤空语句和注释行

        Returns:
            List[str]: 过滤后的语句列表
        """
        return [
            stmt
            for stmt in self.statements
            if stmt and not stmt.isspace() and not stmt.startswith("--")
        ]
