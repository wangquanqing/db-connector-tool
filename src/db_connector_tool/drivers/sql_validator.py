"""SQL 安全校验策略 (SQL Validator)

把 SQL 注入防护策略从驱动实现中独立出来，便于单独维护与测试。

校验思路是**防注入而非限制语句种类**：

1. 长度上限；
2. 在剥离字符串字面量与注释后的“语句结构”上匹配注入特征，
   因此合法查询（``information_schema``、``UNION``）以及字面量中恰好
   包含 ``OR 1=1`` 这类文本的正常数据都不会被误伤；
3. 拒绝一次执行多条语句（``SELECT 1; DROP TABLE t`` 语句走私），
   存储过程/触发器定义除外；
4. 基于原始 SQL 匹配可疑注释（``'--`` 截断、MySQL 版本注释）。

显式的管理语句（``DROP`` / ``TRUNCATE`` / ``CREATE`` / ``ALTER`` /
``GRANT`` / ``REVOKE`` 等）都是允许的：用户自己书写的语句属于明确意图，
注入的真正风险在于“拼接”与“一条语句里夹带另一条”，因此本模块只拦注入特征
与语句走私，不按语句种类设限。

已知限制（本模块是纵深防御的一层，**真正的防线是参数化查询**）：

* 只识别常见的注入特征，``OR TRUE``、``OR 0x1 = 0x1`` 等价写法不在特征库内；
* 不解析方言特有语法（PostgreSQL 的 ``$$...$$``、MySQL 的 ``#`` 注释），
  这类文本会被当作语句结构处理，可能造成偏严的拦截（不会造成放行）。
"""

import re
from typing import List, Tuple

from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

# SQL 语句长度上限
MAX_SQL_LENGTH = 10000

# 允许内嵌分号的存储过程/脚本类语句（这类语句本身就需要多语句结构）
PROCEDURAL_PREFIXES: Tuple[str, ...] = (
    "create procedure",
    "create function",
    "create trigger",
    "create or replace",
    "begin",
    "declare",
)

# 注入特征：在剥离字面量与注释后的语句结构上匹配
INJECTION_PATTERNS: Tuple[Tuple[str, str], ...] = (
    (r"(?i)\bOR\s+1\s*=\s*1\b", "OR 1=1 注入"),
    (r"(?i)\bAND\s+1\s*=\s*1\b", "AND 1=1 注入"),
    # 布尔盲注：字面量被剥离后留下 “OR =” 这种缺操作数的比较，
    # 正是 “'1' OR '1'='1'” 一类引号越界的形态；
    # 合法写法（OR b = c、OR (b = 'y')）在 OR 与 = 之间有其他 token，不会命中。
    (r"(?i)\bOR\s*=", "布尔盲注尝试"),
    (r"(?i)\bAND\s*=", "布尔盲注尝试"),
    (r'(?i)\bEXEC\s*\(\s*[\'"\@]', "动态SQL执行"),
    (r"(?i)\bEXECUTE\s+IMMEDIATE\b", "动态SQL执行"),
    (r"(?i)\bxp_cmdshell\b", "系统命令执行"),
    (r"(?i)\bsp_oamethod\b|\bsp_oacreate\b", "OLE自动化存储过程"),
    (r"(?i)\bBULK\s+INSERT\b", "批量文件导入"),
    (r"(?i)\bINTO\s+(OUTFILE|DUMPFILE)\b", "文件写入操作"),
    (r"(?i)\bLOAD_FILE\s*\(", "文件读取操作"),
    (r"(?i)\bpg_read_file\s*\(|\bpg_ls_dir\s*\(", "文件读取操作"),
    (r"(?i)\bCOPY\s+\w+\s+FROM\s+PROGRAM\b", "系统命令执行"),
    (r'(?i)WAITFOR\s+DELAY\s+[\'"]?\d+', "时间盲注"),
    (r"(?i)^\s*SHUTDOWN\b", "数据库关闭命令"),
)

# 可疑注释特征：基于原始 SQL 匹配，特征本身依赖引号与注释符号
COMMENT_PATTERNS: Tuple[str, ...] = (
    r'(?i)[\'"]\s*--\s*$',
    r"(?i)/\*!\d+\s+",
)


def strip_sql_literals_and_comments(query: str) -> str:
    """剥离字符串字面量与注释，仅保留语句结构

    Args:
        query: 原始 SQL 语句

    Returns:
        str: 字面量与注释被替换为空格后的语句结构

    Example:
        >>> strip_sql_literals_and_comments("SELECT * FROM t WHERE a = 'OR 1=1'").strip()
        'SELECT * FROM t WHERE a ='
    """
    result: List[str] = []
    index = 0
    length = len(query)

    while index < length:
        char = query[index]

        if char in ("'", '"', "`"):
            index += 1
            while index < length:
                if query[index] == char:
                    # 连续两个引号表示转义后的字面量引号
                    if index + 1 < length and query[index + 1] == char:
                        index += 2
                        continue
                    index += 1
                    break
                index += 1
            result.append(" ")
            continue

        if query.startswith("--", index):
            newline = query.find("\n", index)
            index = length if newline == -1 else newline + 1
            result.append(" ")
            continue

        if query.startswith("/*", index):
            end = query.find("*/", index + 2)
            index = length if end == -1 else end + 2
            result.append(" ")
            continue

        result.append(char)
        index += 1

    return "".join(result)


def _ensure_single_statement(query: str, structure: str) -> None:
    """拒绝一次执行多条语句（语句走私）

    Args:
        query: 原始 SQL 语句（用于日志）
        structure: 已剥离字面量与注释的语句结构

    Raises:
        ValueError: 语句结构中存在多条语句
    """
    if structure.lstrip().lower().startswith(PROCEDURAL_PREFIXES):
        logger.debug("存储过程/脚本类语句，跳过多语句检查: %s...", query[:100])
        return

    statements = [part for part in structure.split(";") if part.strip()]
    if len(statements) > 1:
        logger.warning("检测到多条语句（疑似语句注入） - %s...", query[:100])
        raise ValueError("查询语句包含多条语句，疑似语句注入")


def validate_sql(query: str) -> None:
    """校验 SQL 语句，拦截注入特征与语句走私

    Args:
        query: SQL 语句

    Raises:
        ValueError: 语句过长、命中注入特征、包含多条语句或可疑注释

    Example:
        >>> validate_sql("SELECT * FROM users WHERE id = :id")
        >>> validate_sql("SELECT 1; DROP TABLE t")
        Traceback (most recent call last):
        ...
        ValueError: 查询语句包含多条语句，疑似语句注入
    """
    if len(query) > MAX_SQL_LENGTH:
        logger.warning("查询语句长度超过限制: %d > %d", len(query), MAX_SQL_LENGTH)
        raise ValueError("查询语句长度超过限制")

    structure = strip_sql_literals_and_comments(query)

    for pattern, description in INJECTION_PATTERNS:
        if re.search(pattern, structure):
            logger.warning(
                "检测到潜在的SQL注入攻击: %s - %s...", description, query[:100]
            )
            raise ValueError(f"查询语句包含潜在的安全风险: {description}")

    _ensure_single_statement(query, structure)

    for pattern in COMMENT_PATTERNS:
        if re.search(pattern, query):
            logger.warning("检测到可疑的SQL注释模式 - %s...", query[:100])
            raise ValueError("查询语句包含可疑的注释模式")
