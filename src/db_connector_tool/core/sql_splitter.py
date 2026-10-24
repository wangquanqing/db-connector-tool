# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""方言感知的 SQL 语句切分器

把一段脚本切成可逐条执行的语句。看似只需「按分号 split」，实际必须理解
**字符串、注释、引用与过程体边界**，否则会把一条语句切碎、或把多条粘成一条。

**为什么不用现成库**（实测结论，详见 `ANALYSIS-sqlglot-evaluation.md`）：

======================  ==========================  ==========================
场景                    sqlparse                    sqlglot
======================  ==========================  ==========================
MySQL 过程体（2 条）     2 ✅                        1 ❌（BEGIN..END 内分号被当分隔）
SQL Server ``GO``（2 条） 2 ✅                        直接报错 ❌
GBase SPL（3 条）        5 ❌（SPL 边界不识别）        直接报错 ❌
======================  ==========================  ==========================

两者都无法满足「六库通用」，因此本模块自研：不做语法校验（那是数据库的事），
只做**边界识别**，且识别不准时宁可少切（把语句粘在一起交给数据库报错），
也不多切（多切会执行半条语句，可能造成破坏）。

支持的边界形态：

- 字符串：``'...'``（``''`` 转义）、``"..."``、`` `...` ``、``[...]``（T-SQL）；
  MySQL 档额外识别反斜杠转义
- 注释：``-- ...``、``# ...``（MySQL）、``/* ... */``、PG 的嵌套块注释
- 引用体：PG 的 ``$$...$$`` / ``$tag$...$tag$``、Oracle 的 ``q'[...]'``
- 过程体：``BEGIN...END``、``IF...END IF``、``LOOP...END LOOP``、
  ``CASE...END CASE``、``REPEAT...UNTIL``
- GBase SPL：``END PROCEDURE`` / ``END FUNCTION`` / ``END TRIGGER``
- 批处理分隔：T-SQL 独占一行的 ``GO``、Oracle SQL*Plus 独占一行的 ``/``

.. note::
    ``GO`` 与 ``/`` 仅作**分隔符**，不会出现在结果里 —— 它们不是 SQL 语句，
    发给数据库会报错。也就是说本模块**不保留 T-SQL 的批语义**
    （例如 ``CREATE VIEW`` 必须单独成批这类约束需调用方自行处理）。

Example:
    >>> from db_connector_tool.core.sql_splitter import split_sql
    >>> [s.text for s in split_sql("SELECT 1; SELECT 2;")]
    ['SELECT 1', 'SELECT 2']
    >>> len(split_sql("SELECT ';' ; SELECT 2"))
    2
"""

from dataclasses import dataclass
from typing import List, Optional, Set, Tuple

__all__ = [
    "SplitStatement",
    "DialectProfile",
    "split_sql",
    "resolve_profile",
    "PROFILES",
]


@dataclass(frozen=True)
class SplitStatement:
    """一条切分出来的语句

    Attributes:
        text: 语句原文（已去除首尾空白，**内容未经改写**，可直接执行）
        start: 在原始脚本中的起始偏移
        end: 结束偏移（不含语句分隔符）
        line: 起始行号（1 基，便于报错定位）
        is_complete: 是否正常闭合。字符串/注释未闭合，或过程体在脚本结束前
            未闭合时为 ``False``，调用方可据此提示「脚本似乎不完整」
    """

    text: str
    start: int
    end: int
    line: int
    is_complete: bool = True


# 数据类：字段都是独立的方言特征开关，拆成多个类反而让调用方到处拼接。
# pylint: disable=too-many-instance-attributes
@dataclass(frozen=True)
class DialectProfile:
    """方言的切分特征

    Attributes:
        name: 规范化后的方言名
        dollar_quoting: 支持 ``$$...$$`` / ``$tag$...$tag$``（PostgreSQL）
        hash_comment: ``#`` 起行注释（MySQL）
        bracket_identifier: ``[...]`` 标识符（T-SQL），``]]`` 为转义
        q_quote: Oracle 的 ``q'[...]'`` 替代引用
        nested_block_comments: 块注释可嵌套（PostgreSQL）
        backslash_escape: 字符串内 ``\\'`` 为转义（MySQL）。
            其余方言中反斜杠是普通字符（Oracle 的 ``'C:\\'`` 很常见），
            故默认关闭 —— 否则会把字符串结尾吞掉
        go_separator: 独占一行的 ``GO`` 是批处理分隔符（T-SQL）
        slash_terminator: 独占一行的 ``/`` 是语句终结符（Oracle SQL*Plus）
        end_if_style: 过程体闭合词含 ``END IF`` / ``END LOOP`` / ``END CASE``
            / ``END WHILE`` / ``END REPEAT``（PL/SQL、PL/pgSQL、MySQL、GBase）。
            T-SQL 与 SQLite **没有** ``END IF``，其 ``IF`` 也不配对，故为 ``False``
        spl_terminator: ``END PROCEDURE`` / ``END FUNCTION`` / ``END TRIGGER``
            直接结束整条语句（GBase 8s 的 SPL）
    """

    name: str
    dollar_quoting: bool = False
    hash_comment: bool = False
    bracket_identifier: bool = False
    q_quote: bool = False
    nested_block_comments: bool = False
    backslash_escape: bool = False
    go_separator: bool = False
    slash_terminator: bool = False
    end_if_style: bool = True
    spl_terminator: bool = False


#: 各方言特征表。规范化名为键，另接受常见别名（见 :func:`resolve_profile`）
PROFILES = {
    "oracle": DialectProfile(
        name="oracle",
        q_quote=True,
        slash_terminator=True,
    ),
    "mysql": DialectProfile(
        name="mysql",
        hash_comment=True,
        backslash_escape=True,
    ),
    "postgresql": DialectProfile(
        name="postgresql",
        dollar_quoting=True,
        nested_block_comments=True,
    ),
    "sqlserver": DialectProfile(
        name="sqlserver",
        bracket_identifier=True,
        go_separator=True,
        end_if_style=False,
    ),
    "sqlite": DialectProfile(name="sqlite", end_if_style=False),
    "gbase8s": DialectProfile(
        name="gbase8s",
        end_if_style=True,
        spl_terminator=True,
    ),
    # 保守通用档：只认标准 SQL 的字符串与注释形态。
    # 不开启方言语法识别（如 ``[`` 或 ``$$``），以免在非该方言的脚本上
    # 把普通字符误判成引用体开头，反而吞掉后续内容。
    "generic": DialectProfile(name="generic"),
}

#: 方言别名 -> 规范化名
_ALIASES = {
    "": "generic",
    "generic": "generic",
    "ansi": "generic",
    "oracle": "oracle",
    "ora": "oracle",
    "mysql": "mysql",
    "mariadb": "mysql",
    "postgres": "postgresql",
    "postgresql": "postgresql",
    "pg": "postgresql",
    "psql": "postgresql",
    "sqlserver": "sqlserver",
    "mssql": "sqlserver",
    "tsql": "sqlserver",
    "sqlite": "sqlite",
    "sqlite3": "sqlite",
    "gbase8s": "gbase8s",
    "gbase": "gbase8s",
    "informix": "gbase8s",
}


def resolve_profile(dialect: Optional[str]) -> DialectProfile:
    """把方言名（含别名）解析为切分特征

    Args:
        dialect: 方言名，大小写不敏感；``None`` 或未知值取通用档

    Returns:
        DialectProfile: 对应的特征；未知方言回退 ``generic``

    Example:
        >>> resolve_profile("postgres").name
        'postgresql'
        >>> resolve_profile("TSQL").name
        'sqlserver'
        >>> resolve_profile(None).name
        'generic'
    """
    key = (dialect or "").strip().lower()
    return PROFILES[_ALIASES.get(key, "generic")]


# 带 END 前缀的闭合词（``END`` 之后紧跟的词决定它闭合的是哪一层）
_END_TAILS_END_IF_STYLE: Set[str] = {"IF", "LOOP", "CASE", "WHILE", "REPEAT"}
_END_TAILS_SPL: Set[str] = {"PROCEDURE", "FUNCTION", "TRIGGER", "ROUTINE"}

# ``BEGIN`` 后紧跟这些词时是事务语句（TCL），不是过程体开头
_TCL_AFTER_BEGIN: Set[str] = {
    "WORK",
    "TRANSACTION",
    "TRAN",
    "DEFERRED",
    "IMMEDIATE",
    "EXCLUSIVE",
}

_WORD_CHARS = set(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_$"
)
_SKIP_CHARS = " \t\r\n"

# 词法处理结果：不做特殊动作 / 命中批处理分隔符 / 命中 SPL 整句终结
_ACTION_NONE = "none"
_ACTION_GO = "go"
_ACTION_SPL_END = "spl_end"


# 私有扫描器：只暴露 run() 一个入口，其余都是内部分支，
# 拆成多个类会让状态（游标/深度/未闭合标记）跨对象传递，反而更易错。
# pylint: disable=too-few-public-methods
class _Splitter:
    """一次切分的扫描状态（无外部依赖，便于单测）

    单遍字符扫描 + 过程体深度计数：

    - 字符串/注释/引用体内部的分号一律不算分隔符；
    - 仅当**过程体深度为 0** 时的分号才是语句分隔符；
    - 识别不确定时**宁可少切**（把语句粘住交给数据库报错），
      绝不多切（多切会执行半条语句，可能造成破坏）。
    """

    def __init__(self, text: str, profile: DialectProfile) -> None:
        self.text = text
        self.length = len(text)
        self.profile = profile
        self.index = 0
        self.depth = 0
        self.unclosed = False
        # GBase SPL 的过程体没有 BEGIN，单独用标志位表示「处于不透明区间」：
        # 自 CREATE PROCEDURE 起，内部的分号一律不是语句分隔符。
        self.in_spl = False

    # ------------------------------------------------------------ 主循环
    def run(self) -> List[SplitStatement]:
        """执行切分

        Returns:
            List[SplitStatement]: 语句列表（不含只有注释/空白的片段）
        """
        statements: List[SplitStatement] = []
        start = 0
        has_code = False

        while self.index < self.length:
            char = self.text[self.index]

            # 1) 注释：不影响 has_code
            if self._skip_comment():
                continue

            # 2) 其余「不透明」区间：字符串 / 引用体 / 方括号标识符
            if self._skip_opaque():
                has_code = True
                continue

            # 3) 词法：过程体深度、批处理分隔符、SPL 终结符
            if char.isalpha() or char == "_":
                action, head_end, next_start = self._handle_word()
                has_code = True
                if action in (_ACTION_GO, _ACTION_SPL_END):
                    statements.extend(self._emit(start, head_end, has_code, False))
                    start = next_start
                    has_code = False
                    self.depth = 0
                    self.in_spl = False
                continue

            # 4) 语句分隔符（仅在过程体外、且不处于 SPL 主体时生效）
            if char == ";":
                if self.depth == 0 and not self.in_spl:
                    statements.extend(self._emit(start, self.index, has_code, False))
                    start = self.index + 1
                    has_code = False
                self.index += 1
                continue

            # 5) Oracle SQL*Plus 的 ``/`` 终结符（须独占一行）
            if (
                char == "/"
                and self.profile.slash_terminator
                and self._alone_on_line(self.index, self.index + 1)
            ):
                if self.depth == 0 and not self.in_spl:
                    statements.extend(self._emit(start, self.index, has_code, False))
                    start = self.index + 1
                    has_code = False
                self.index += 1
                continue

            if not char.isspace():
                has_code = True
            self.index += 1

        statements.extend(self._emit(start, self.length, has_code, True))
        return statements

    # ------------------------------------------------------------ 片段收集
    def _emit(
        self, start: int, end: int, has_code: bool, trailing: bool
    ) -> List[SplitStatement]:
        """把 ``[start, end)`` 变成一条语句（去空白后）

        Args:
            start: 起始偏移
            end: 结束偏移
            has_code: 该片段是否含实际代码（只有注释/空白则丢弃）
            trailing: 是否为脚本末尾片段

        Returns:
            List[SplitStatement]: 空片段返回空列表
        """
        if not has_code:
            return []
        raw = self.text[start:end]
        stripped = raw.strip()
        if not stripped:
            return []
        # 反推去空白后的真实位置，便于报错定位
        real_start = start + (len(raw) - len(raw.lstrip()))
        line = self.text.count("\n", 0, real_start) + 1
        complete = not (trailing and (self.unclosed or self.depth > 0))
        return [
            SplitStatement(
                text=stripped,
                start=real_start,
                end=real_start + len(stripped),
                line=line,
                is_complete=complete,
            )
        ]

    # ------------------------------------------------------------ 跳过器
    def _skip_opaque(self) -> bool:
        """尝试跳过一个「不透明」区间（字符串 / 引用体 / 方括号）

        Returns:
            bool: 是否消费了字符
        """
        return (
            self._skip_string()
            or self._skip_dollar_quote()
            or self._skip_q_quote()
            or self._skip_bracket_identifier()
        )

    def _skip_string(self) -> bool:
        """跳过引号包裹的区间；返回是否消费了字符

        覆盖单引号字符串、双引号字符串/标识符，以及 MySQL 的反引号标识符
        （``\\``` 内可含分号，是切碎的常见原因）。
        """
        char = self.text[self.index]
        if char not in ("'", '"', "`"):
            return False
        escape = self.profile.backslash_escape and char == "'"
        self.index += 1
        while self.index < self.length:
            current = self.text[self.index]
            if current == "\\" and escape and self.index + 1 < self.length:
                # MySQL 默认开启反斜杠转义（其余方言反斜杠是普通字符）
                self.index += 2
                continue
            if current == char:
                # 连写两个引号是转义后的字面量引号
                if self.index + 1 < self.length and self.text[self.index + 1] == char:
                    self.index += 2
                    continue
                self.index += 1
                return True
            self.index += 1
        # 未闭合：标记但继续（交给数据库报错）
        self.unclosed = True
        return True

    def _skip_comment(self) -> bool:
        """跳过行注释与块注释"""
        if self.text.startswith("--", self.index):
            newline = self.text.find("\n", self.index)
            self.index = self.length if newline == -1 else newline + 1
            return True

        if self.profile.hash_comment and self.text[self.index] == "#":
            newline = self.text.find("\n", self.index)
            self.index = self.length if newline == -1 else newline + 1
            return True

        if self.text.startswith("/*", self.index):
            if self.profile.nested_block_comments:
                self._skip_nested_block_comment()
            else:
                end = self.text.find("*/", self.index + 2)
                if end == -1:
                    self.unclosed = True
                    self.index = self.length
                else:
                    self.index = end + 2
            return True

        return False

    def _skip_nested_block_comment(self) -> None:
        """跳过可嵌套的块注释（PostgreSQL）"""
        depth = 0
        while self.index < self.length:
            if self.text.startswith("/*", self.index):
                depth += 1
                self.index += 2
                continue
            if self.text.startswith("*/", self.index):
                depth -= 1
                self.index += 2
                if depth == 0:
                    return
                continue
            self.index += 1
        self.unclosed = True

    def _skip_dollar_quote(self) -> bool:
        """跳过 PG 的 ``$$...$$`` / ``$tag$...$tag$``

        注意 ``$1`` 是位置参数不是引用体，故标签须为空或以字母/下划线开头。
        """
        if not self.profile.dollar_quoting or self.text[self.index] != "$":
            return False
        tag = self._read_dollar_tag()
        if tag is None:
            return False
        end = self.text.find(tag, self.index + len(tag))
        if end == -1:
            self.unclosed = True
            self.index = self.length
        else:
            self.index = end + len(tag)
        return True

    def _read_dollar_tag(self) -> Optional[str]:
        """读取 ``$`` 起的引用标签；不是引用体时返回 ``None``"""
        cursor = self.index + 1
        if cursor < self.length and self.text[cursor] == "$":
            return "$$"
        start = cursor
        if cursor < self.length and (
            self.text[cursor].isalpha() or self.text[cursor] == "_"
        ):
            cursor += 1
            while cursor < self.length and (
                self.text[cursor].isalnum() or self.text[cursor] == "_"
            ):
                cursor += 1
            if cursor < self.length and self.text[cursor] == "$":
                return self.text[start : cursor + 1]
        return None

    def _skip_q_quote(self) -> bool:
        """跳过 Oracle 的 ``q'[...]'`` 替代引用"""
        if not self.profile.q_quote:
            return False
        if self.text[self.index] not in ("q", "Q"):
            return False
        if self.index + 2 >= self.length or self.text[self.index + 1] != "'":
            return False
        opener = self.text[self.index + 2]
        closer = {"[": "]", "{": "}", "(": ")", "<": ">"}.get(opener, opener)
        end = self.text.find(closer + "'", self.index + 3)
        if end == -1:
            self.unclosed = True
            self.index = self.length
        else:
            self.index = end + 2
        return True

    def _skip_bracket_identifier(self) -> bool:
        """跳过 T-SQL 的 ``[...]`` 标识符（``]]`` 为转义）"""
        if not self.profile.bracket_identifier or self.text[self.index] != "[":
            return False
        self.index += 1
        while self.index < self.length:
            if self.text[self.index] == "]":
                if self.index + 1 < self.length and self.text[self.index + 1] == "]":
                    self.index += 2
                    continue
                self.index += 1
                return True
            self.index += 1
        self.unclosed = True
        return True

    # ------------------------------------------------------------ 词法
    def _read_word(self) -> Tuple[str, int]:
        """从当前位置读取一个词

        **必须保证游标至少前进一个字符**：``str.isalpha()`` 对中文等非 ASCII
        字母同样为真（``'用'.isalpha()`` 是 ``True``），但这些字符不在
        :data:`_WORD_CHARS` 里。若读不到字符又不推进游标，主循环会死循环
        —— 含中文标识符的脚本（如 ``SELECT * FROM 用户表``）会直接卡住。

        Returns:
            Tuple[str, int]: ``(大写词, 词结束偏移)``
        """
        start = self.index
        while self.index < self.length and self.text[self.index] in _WORD_CHARS:
            self.index += 1
        if self.index == start:
            # 非 ASCII 字母（CJK 等）：不构成关键字，整体跳过该字符即可
            self.index += 1
        return self.text[start : self.index].upper(), self.index

    # 分支本就是「先判分隔符、再判闭合、再判开启」的扁平结构，
    # 每个 return 都是一类独立的边界情形，合并反而降低可读性。
    # pylint: disable=too-many-return-statements
    def _handle_word(self) -> Tuple[str, int, int]:
        """处理一个词：更新过程体深度、识别 ``GO`` 与 SPL 终结符

        Returns:
            Tuple[str, int, int]: ``(动作, 片段结束位置, 下一片段起点)``。
            动作为 ``none`` 时后两项等于当前游标，调用方无需处理
        """
        word_start = self.index
        word, word_end = self._read_word()

        # T-SQL 的 GO：独占一行，且本身不是 SQL 语句
        if (
            self.profile.go_separator
            and word == "GO"
            and self._alone_on_line(word_start, word_end)
        ):
            return _ACTION_GO, word_start, self._skip_blank(word_end)

        # GBase SPL：过程体内的分号不是语句分隔符 —— 若当前处于 SPL 主体，
        # 遇到 END / END PROCEDURE 才结束。深度由 spl_terminator 分支承担，
        # 因此这里必须先于通用 END 处理判定。
        # 注意：``_read_end_tail`` 会推进游标，**只能**在词为 ``END`` 时调用。
        # 早期版本对所有词都调用，导致 ``BEGIN`` 吃掉后面的 ``IF``、
        # ``CREATE`` 吃掉 ``PROCEDURE``，嵌套深度少数一层，过程体被切碎。
        if word == "END":
            tail = self._read_end_tail(word_end)
            if tail is not None:
                if self.profile.spl_terminator and tail in _END_TAILS_SPL:
                    # SPL 整句终结：吞掉可选分号，作为一条语句交出
                    end = self.index
                    if end < self.length and self.text[end] == ";":
                        end += 1
                    self.index = end
                    return _ACTION_SPL_END, end, self._skip_blank(end)
                # ``END IF`` / ``END LOOP`` 等：与对应的开头配对。
                # 由于 ``IF`` 可能因是函数调用（``IF(...)``）而未计入深度，
                # 这里做**饱和递减**：深度为 0 时不减，避免把外层过程体减掉。
                self.depth = max(0, self.depth - 1)
                return _ACTION_NONE, self.index, self.index
            self.depth = max(0, self.depth - 1)
            return _ACTION_NONE, self.index, self.index

        # SPL 过程体自 ``CREATE PROCEDURE`` 起进入不透明状态：
        # 其内部的分号不是语句分隔符（SPL 无 BEGIN，靠 END PROCEDURE 收尾）
        if self._is_spl_opener(word):
            self.in_spl = True
            return _ACTION_NONE, self.index, self.index

        if self._is_opener(word):
            if word == "BEGIN" and self._is_transaction_begin():
                return _ACTION_NONE, self.index, self.index
            self.depth += 1
        return _ACTION_NONE, self.index, self.index

    def _is_spl_opener(self, word: str) -> bool:
        """判断该词是否开启 GBase SPL 的过程体

        SPL 的过程体**没有** ``BEGIN``，形如::

            CREATE PROCEDURE p() DEFINE i INT; LET i = 1; END PROCEDURE;

        因此见到 ``CREATE PROCEDURE`` / ``CREATE FUNCTION`` 就要进入「不透明」
        状态，直到 ``END PROCEDURE`` 等终结词。

        .. note::
            **不含 ``CREATE TRIGGER``**：GBase 触发器有两种写法，一种带
            ``BEGIN...END TRIGGER``（由 ``BEGIN`` 正常处理），另一种是
            ``CREATE TRIGGER ... FOR EACH ROW (单条语句);`` —— 后者没有
            ``END TRIGGER``，若在此开启深度就永远闭不上，会把后续语句全吞掉。

        Args:
            word: 大写词

        Returns:
            bool: 开启 SPL 过程体返回 ``True``
        """
        if not self.profile.spl_terminator or word != "CREATE":
            return False
        cursor = self._skip_blank(self.index)
        head, cursor = self._read_alpha_word(cursor)
        # CREATE OR REPLACE PROCEDURE / CREATE OR REPLACE FUNCTION：
        # ``OR`` 之后必须紧跟 ``REPLACE``，再跳过它取真正的对象类型。
        if head == "OR":
            cursor = self._skip_blank(cursor)
            modifier, cursor = self._read_alpha_word(cursor)
            if modifier != "REPLACE":
                return False
            cursor = self._skip_blank(cursor)
            head, cursor = self._read_alpha_word(cursor)
        return head in ("PROCEDURE", "FUNCTION", "ROUTINE")

    def _read_alpha_word(self, cursor: int) -> Tuple[str, int]:
        """从 ``cursor`` 读一个纯字母词

        Args:
            cursor: 起始偏移

        Returns:
            Tuple[str, int]: ``(大写词, 词结束偏移)``
        """
        start = cursor
        while cursor < self.length and self.text[cursor].isalpha():
            cursor += 1
        return self.text[start:cursor].upper(), cursor

    def _read_end_tail(self, word_end: int) -> Optional[str]:
        """读取 ``END`` 之后紧跟的词（用于判定 ``END IF`` / ``END PROCEDURE``）

        仅当该方言确实存在这些形态时才消费它，避免把下一条语句的首词吃掉。

        Args:
            word_end: ``END`` 一词的结束偏移

        Returns:
            Optional[str]: 紧跟的大写词；无匹配形态时返回 ``None``
        """
        cursor = self._skip_blank(word_end)
        start = cursor
        while cursor < self.length and self.text[cursor].isalpha():
            cursor += 1
        tail = self.text[start:cursor].upper()

        known: Set[str] = set()
        if self.profile.end_if_style:
            known |= _END_TAILS_END_IF_STYLE
        if self.profile.spl_terminator:
            known |= _END_TAILS_SPL
        if tail and tail in known:
            self.index = cursor
            return tail
        return None

    def _is_opener(self, word: str) -> bool:
        """判断该词是否开启一层过程体

        需要排除两类**看起来像但不配对**的 ``IF``：

        1. ``IF(...)`` —— MySQL 的 IF 函数调用；
        2. ``IF EXISTS`` / ``IF NOT EXISTS`` —— DDL 条件词
           （``DROP TABLE IF EXISTS t``、``CREATE TABLE IF NOT EXISTS t``），
           其后没有 ``END IF``。若误计入深度，分号将不再切分，
           整段脚本会被粘成一条（实测：``DROP TABLE IF EXISTS t;``
           会把后面的建表、插入、查询全部吞进同一条语句）。

        Args:
            word: 大写词

        Returns:
            bool: 开启新一层返回 ``True``
        """
        if word in ("BEGIN", "CASE"):
            return True
        if not self.profile.end_if_style:
            return False
        if word in ("LOOP", "REPEAT"):
            return True
        if word == "IF":
            return not self._next_is_open_paren() and not self._next_is_ddl_condition()
        return False

    def _next_is_open_paren(self) -> bool:
        """判断当前游标之后（跳过空白）是否紧跟 ``(``"""
        cursor = self._skip_blank(self.index)
        return cursor < self.length and self.text[cursor] == "("

    def _next_is_ddl_condition(self) -> bool:
        """判断 ``IF`` 之后是否为 ``EXISTS`` / ``NOT EXISTS``（DDL 条件词）

        Returns:
            bool: 是 DDL 条件词返回 ``True``（此时 ``IF`` 不配对，不计深度）
        """
        head = self._peek_word_after()
        if head == "EXISTS":
            return True
        if head == "NOT":
            cursor = self._skip_blank(self.index)
            cursor = self._skip_blank(self._word_end(cursor))
            start = cursor
            while cursor < self.length and self.text[cursor].isalpha():
                cursor += 1
            return self.text[start:cursor].upper() == "EXISTS"
        return False

    def _peek_word_after(self) -> str:
        """读取当前游标之后的第一个词（不移动主游标）"""
        cursor = self._skip_blank(self.index)
        start = cursor
        while cursor < self.length and self.text[cursor].isalpha():
            cursor += 1
        return self.text[start:cursor].upper()

    def _word_end(self, cursor: int) -> int:
        """返回从 ``cursor`` 起首个词之后的偏移"""
        while cursor < self.length and self.text[cursor].isalpha():
            cursor += 1
        return cursor

    def _is_transaction_begin(self) -> bool:
        """判断 ``BEGIN`` 是否为事务语句（而非过程体开头）

        覆盖 ``BEGIN WORK`` / ``BEGIN TRANSACTION`` / ``BEGIN IMMEDIATE``
        以及孤立的 ``BEGIN;``（SQLite / PostgreSQL）。
        """
        cursor = self._skip_blank(self.index)
        if cursor < self.length and self.text[cursor] == ";":
            return True
        start = cursor
        while cursor < self.length and self.text[cursor].isalpha():
            cursor += 1
        return self.text[start:cursor].upper() in _TCL_AFTER_BEGIN

    def _skip_blank(self, cursor: int) -> int:
        """跳过空白字符

        Args:
            cursor: 起始偏移

        Returns:
            int: 首个非空白字符的偏移
        """
        while cursor < self.length and self.text[cursor] in _SKIP_CHARS:
            cursor += 1
        return cursor

    def _alone_on_line(self, start: int, end: int) -> bool:
        """判断 ``[start, end)`` 是否独占一行（前后只有空白）

        Args:
            start: 词起始偏移
            end: 词结束偏移

        Returns:
            bool: 独占一行返回 ``True``
        """
        if self.text[self._line_left(start) : start].strip():
            return False
        right = self.text.find("\n", end)
        right = self.length if right == -1 else right
        return not self.text[end:right].strip()

    def _line_left(self, position: int) -> int:
        """返回该位置所在行的起始偏移（换行符之后）"""
        return self.text.rfind("\n", 0, position) + 1


def split_sql(text: str, dialect: Optional[str] = None) -> List[SplitStatement]:
    """把 SQL 脚本切分为可逐条执行的语句

    不做语法校验（交给数据库），只做边界识别。识别不确定时**宁可少切**：
    多切会执行半条语句，可能造成破坏；少切只会让数据库报语法错误。

    Args:
        text: SQL 脚本内容
        dialect: 方言名（``oracle`` / ``mysql`` / ``postgresql`` / ``sqlserver``
            / ``sqlite`` / ``gbase8s``，别名见 :func:`resolve_profile`）；
            缺省为保守通用档

    Returns:
        List[SplitStatement]: 语句列表；只有注释或空白的片段被丢弃

    Example:
        >>> [s.text for s in split_sql("SELECT 1; SELECT 2;")]
        ['SELECT 1', 'SELECT 2']

        >>> len(split_sql("SELECT ';' ; SELECT 2"))
        2

        >>> # 过程体内的分号不是分隔符
        >>> sql = "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END"
        >>> len(split_sql(sql, "mysql"))
        1

        >>> # 行号便于报错定位
        >>> split_sql("SELECT 1;\\nSELECT 2;")[1].line
        2
    """
    if not text or not text.strip():
        return []
    return _Splitter(text, resolve_profile(dialect)).run()
