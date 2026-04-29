"""SQL解析工具模块 (SQL Parser Utilities)

提供SQL语句解析和分割功能，支持语法感知的语句分割，
能够正确处理字符串常量、存储过程等复杂语法结构。

Example:
>>> from db_connector_tool.utils.sqlparse_utils import SQLStatementParser, read_and_split_sql_file
>>>
>>> # 解析包含存储过程的SQL
>>> parser = SQLStatementParser()
>>> sql_content = '''
... CREATE PROCEDURE test()
... BEGIN
...     SELECT * FROM users;
... END;
... SELECT * FROM products;
... '''
>>> statements = parser.parse(sql_content)
>>> for stmt in statements:
...     print(stmt)
>>>
>>> # 直接读取并解析SQL文件
>>> statements = read_and_split_sql_file("script.sql")
"""

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


def read_and_split_sql_file(file_path: str) -> List[str]:
    """读取SQL文件内容并分割为独立的SQL语句

    自动处理不同编码格式（utf-8, gbk），确保文件能够正确读取。

    Args:
        file_path: SQL文件路径

    Returns:
        List[str]: 分割后的SQL语句列表

    Raises:
        FileNotFoundError: 如果文件不存在
        ValueError: 如果文件编码不支持

    Example:
        >>> statements = read_and_split_sql_file("script.sql")
        >>> print(len(statements))
        5
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            sql_content = f.read()
        return _split_sql_statements(sql_content)
    except UnicodeDecodeError:
        # 尝试其他编码
        try:
            with open(file_path, "r", encoding="gbk") as f:
                sql_content = f.read()
            return _split_sql_statements(sql_content)
        except UnicodeDecodeError as e:
            logger.error("无法解码SQL文件: %s", e)
            raise ValueError(f"无法解码SQL文件，请检查文件编码: {e}") from e


def _split_sql_statements(sql_content: str) -> List[str]:
    """将SQL内容分割为独立的语句（内部函数）

    Args:
        sql_content: 原始SQL内容

    Returns:
        List[str]: 分割后的SQL语句列表
    """
    parser = SQLStatementParser()
    return parser.parse(sql_content)


class SQLStatementParser:
    """SQL语句解析器类 (SQL Statement Parser)

    负责根据语法上下文分割SQL语句，能够正确处理字符串常量、
    存储过程、大括号等复杂语法结构，避免在字符串中间或存储过程内部错误分割。

    Example:
    >>> parser = SQLStatementParser()
    >>> sql_content = "SELECT 1; SELECT 2; CREATE PROCEDURE p() BEGIN SELECT 3; END;"
    >>> statements = parser.parse(sql_content)
    >>> for stmt in statements:
    ...     print(stmt)
    """

    def __init__(self):
        """初始化SQL语句解析器"""
        self.statements = []
        self.current_statement = ""
        self.in_string = False
        self.string_quote = None
        self.in_procedure = False
        self.brace_level = 0

    def parse(self, sql_content: str) -> List[str]:
        """解析SQL内容并分割为独立语句

        Args:
            sql_content: SQL内容

        Returns:
            List[str]: 分割后的语句列表

        Example:
            >>> parser = SQLStatementParser()
            >>> sql = "SELECT * FROM users; SELECT * FROM products;"
            >>> statements = parser.parse(sql)
            >>> print(len(statements))
            2
        """
        self._reset_state()

        for i, char in enumerate(sql_content):
            self._process_char(char, i, sql_content)

        self._add_final_statement()
        return self._filter_statements()

    def parse_file(self, file_path: str, encoding: str = "utf-8") -> List[str]:
        """解析SQL文件并分割为独立语句

        Args:
            file_path: SQL文件路径
            encoding: 文件编码，默认为utf-8

        Returns:
            List[str]: 分割后的语句列表

        Raises:
            FileNotFoundError: 如果文件不存在
            UnicodeDecodeError: 如果文件编码不支持

        Example:
            >>> parser = SQLStatementParser()
            >>> statements = parser.parse_file("script.sql")
            >>> print(len(statements))
            3
        """
        try:
            with open(file_path, "r", encoding=encoding) as f:
                sql_content = f.read()
            return self.parse(sql_content)
        except UnicodeDecodeError:
            # 尝试其他常见编码
            for alt_encoding in ["gbk", "latin-1", "cp1252"]:
                try:
                    with open(file_path, "r", encoding=alt_encoding) as f:
                        sql_content = f.read()
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
            >>> count = parser.get_statement_count()
            >>> print(count)  # 2
            2
        """
        return len(self.statements)

    def get_statements_by_type(self, statement_type: Optional[str] = None) -> List[str]:
        """按类型过滤语句

        Args:
            statement_type: 语句类型关键字（如SELECT, INSERT等），如果为None则返回所有语句

        Returns:
            List[str]: 过滤后的语句列表

        Example:
            >>> parser = SQLStatementParser()
            >>> statements = parser.parse("SELECT 1; INSERT INTO t VALUES(1);")
            >>> select_statements = parser.get_statements_by_type("SELECT")
            >>> print(len(select_statements))
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
        """验证SQL语法是否可以被正确解析

        Args:
            sql_content: SQL内容

        Returns:
            bool: 语法是否有效

        Example:
            >>> parser = SQLStatementParser()
            >>> is_valid = parser.validate_syntax("SELECT * FROM users;")
            >>> print(is_valid)  # True
            True
        """
        try:
            statements = self.parse(sql_content)
            return len(statements) > 0 and all(
                stmt and not stmt.isspace() for stmt in statements
            )
        except (ValueError, TypeError, IndexError, AttributeError):
            # 捕获解析过程中可能出现的具体异常
            return False

    def _reset_state(self):
        """重置解析器状态（内部方法）"""
        self.statements = []
        self.current_statement = ""
        self.in_string = False
        self.string_quote = None
        self.in_procedure = False
        self.brace_level = 0

    def _process_char(self, char: str, index: int, sql_content: str):
        """处理单个字符（内部方法）

        Args:
            char: 当前字符
            index: 字符索引
            sql_content: 完整的SQL内容
        """
        if self._handle_string_literal(char, index, sql_content):
            return
        if self._handle_braces(char):
            return
        if self._handle_semicolon(char):
            return

        self.current_statement += char

    def _handle_string_literal(self, char: str, index: int, sql_content: str) -> bool:
        """处理字符串常量（内部方法）

        Args:
            char: 当前字符
            index: 字符索引
            sql_content: 完整的SQL内容

        Returns:
            bool: 是否已处理字符串逻辑
        """
        if char in ("'", '"') and not self.in_string:
            self._start_string(char)
            return True
        if char == self.string_quote and self.in_string:
            self._end_string(char, index, sql_content)
            return True
        return False

    def _start_string(self, quote_char: str):
        """开始字符串常量（内部方法）

        Args:
            quote_char: 引号字符
        """
        self.in_string = True
        self.string_quote = quote_char
        self.current_statement += quote_char

    def _end_string(self, quote_char: str, index: int, sql_content: str):
        """结束字符串常量（内部方法）

        Args:
            quote_char: 引号字符
            index: 字符索引
            sql_content: 完整的SQL内容
        """
        # 检查是否是转义的引号
        if index > 0 and sql_content[index - 1] == "\\":
            self.current_statement += quote_char
        else:
            self.in_string = False
            self.string_quote = None
            self.current_statement += quote_char

    def _handle_braces(self, char: str) -> bool:
        """处理大括号（存储过程）（内部方法）

        Args:
            char: 当前字符

        Returns:
            bool: 是否已处理大括号逻辑
        """
        if char == "{" and not self.in_string:
            self._open_brace()
            return True
        if char == "}" and not self.in_string:
            self._close_brace()
            return True
        return False

    def _open_brace(self):
        """处理开大括号（内部方法）"""
        self.brace_level += 1
        self.in_procedure = self.brace_level > 0
        self.current_statement += "{"

    def _close_brace(self):
        """处理闭大括号（内部方法）"""
        self.brace_level = max(0, self.brace_level - 1)
        self.in_procedure = self.brace_level > 0
        self.current_statement += "}"

    def _handle_semicolon(self, char: str) -> bool:
        """处理分号分割（内部方法）

        Args:
            char: 当前字符

        Returns:
            bool: 是否已处理分号逻辑
        """
        if char == ";" and not self.in_string and not self.in_procedure:
            self._split_statement()
            return True
        return False

    def _split_statement(self):
        """在分号处分割语句（内部方法）"""
        self.current_statement += ";"
        stmt = self.current_statement.strip()
        if stmt and not stmt.isspace():
            self.statements.append(stmt)
        self.current_statement = ""

    def _add_final_statement(self):
        """添加最后可能没有分号的语句（内部方法）"""
        if (
            self.current_statement.strip()
            and not self.current_statement.strip().isspace()
        ):
            self.statements.append(self.current_statement.strip())

    def _filter_statements(self) -> List[str]:
        """过滤空语句和注释语句（内部方法）

        Returns:
            List[str]: 过滤后的语句列表
        """
        return [
            stmt
            for stmt in self.statements
            if stmt and not stmt.isspace() and not stmt.startswith("--")
        ]
