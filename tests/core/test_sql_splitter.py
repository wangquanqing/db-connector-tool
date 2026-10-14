"""SQL 切分器回归测试（方言感知的语句边界识别）

覆盖六库（Oracle / MySQL / PostgreSQL / SQL Server / SQLite / GBase 8s）
的脚本切分场景。核心是把三类「切错」挡在门外：

1. **切碎**：字符串、注释、引用体、过程体内部的分号被当成语句分隔符
   → 会执行半条语句，可能造成破坏；
2. **粘连**：多条语句被并成一条 → 数据库报语法错误；
3. **死循环**：扫描游标不前进 → 整个 CLI 卡死
   （`SELECT * FROM 用户表` 曾触发，见 ``test_cjk_identifier_does_not_hang``）。

参考文献见 `ANALYSIS-sqlglot-evaluation.md`：sqlparse 会把 GBase SPL 切碎，
sqlglot 对 MySQL 过程体只切出 1 条、对 SQL Server ``GO`` 直接报错，
故本模块自研。
"""

import unittest

from src.db_connector_tool.core.sql_splitter import (
    DialectProfile,
    SplitStatement,
    resolve_profile,
    split_sql,
)


def texts(sql: str, dialect=None) -> list:
    """切分并返回语句文本列表"""
    return [s.text for s in split_sql(sql, dialect)]


class TestBasicSplitting(unittest.TestCase):
    """基础切分"""

    def test_single_statement_without_semicolon(self) -> None:
        """无结尾分号也算一条"""
        self.assertEqual(texts("SELECT 1"), ["SELECT 1"])

    def test_multiple_statements(self) -> None:
        """按分号切开"""
        self.assertEqual(texts("SELECT 1; SELECT 2;"), ["SELECT 1", "SELECT 2"])

    def test_empty_and_whitespace_only(self) -> None:
        """空输入与纯空白返回空列表"""
        for value in ("", "   ", "\n\t ", "\n\n"):
            with self.subTest(value=value):
                self.assertEqual(split_sql(value), [])

    def test_comment_only_script(self) -> None:
        """只有注释的脚本不产出语句"""
        self.assertEqual(split_sql("-- 只有注释\n/* 块注释 */\n"), [])

    def test_comment_lines_are_dropped(self) -> None:
        """行注释与块注释不单独成句"""
        self.assertEqual(
            texts("-- 头部注释\nSELECT 1;\n/* 中间 */ SELECT 2;"),
            ["-- 头部注释\nSELECT 1", "/* 中间 */ SELECT 2"],
        )

    def test_text_is_stripped_but_not_rewritten(self) -> None:
        """语句文本仅去首尾空白，内容原样保留"""
        result = texts("  SELECT  1  ;\n")
        self.assertEqual(result, ["SELECT  1"])

    def test_multiline_statement(self) -> None:
        """跨行语句作为一条"""
        self.assertEqual(len(split_sql("SELECT\n  1,\n  2\nFROM t;")), 1)


class TestStringAndCommentHandling(unittest.TestCase):
    """字符串与注释中的分号不得切分"""

    def test_semicolon_in_single_quoted_string(self) -> None:
        """单引号字符串内的分号不算分隔符"""
        self.assertEqual(len(split_sql("INSERT INTO t VALUES ('a;b'); SELECT 2;")), 2)

    def test_semicolon_in_double_quoted_string(self) -> None:
        """双引号内的分号不算分隔符"""
        self.assertEqual(len(split_sql('INSERT INTO t VALUES ("a;b");')), 1)

    def test_escaped_quote(self) -> None:
        """连写引号是转义，不结束字符串"""
        self.assertEqual(len(split_sql("INSERT INTO t VALUES ('it''s;ok');")), 1)

    def test_backtick_identifier(self) -> None:
        """MySQL 反引号标识符内的分号不算分隔符"""
        self.assertEqual(len(split_sql("SELECT `a;b` FROM t;", "mysql")), 1)

    def test_semicolon_in_line_comment(self) -> None:
        """行注释内的分号不算分隔符"""
        self.assertEqual(len(split_sql("SELECT 1; -- 注释; 还是注释\nSELECT 2;")), 2)

    def test_semicolon_in_block_comment(self) -> None:
        """块注释内的分号不算分隔符"""
        self.assertEqual(len(split_sql("SELECT 1; /* 注释; */ SELECT 2;")), 2)

    def test_mysql_hash_comment(self) -> None:
        """MySQL 的 # 是行注释"""
        self.assertEqual(len(split_sql("SELECT 1; # 注释;\nSELECT 2;", "mysql")), 2)

    def test_hash_is_not_comment_outside_mysql(self) -> None:
        """非 MySQL 档下 # 不是注释（保持保守，不吞内容）

        ``# x`` 会作为下一条语句的开头被保留，而不是被当成注释删掉 ——
        宁可让数据库报错，也不静默丢掉用户写的字符。
        """
        result = split_sql("SELECT 1; # x\nSELECT 2;", None)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[1].text, "# x\nSELECT 2")

    def test_nested_block_comment_in_postgres(self) -> None:
        """PostgreSQL 块注释可嵌套"""
        self.assertEqual(
            len(split_sql("SELECT 1; /* a /* b; */ c; */ SELECT 2;", "postgres")), 2
        )

    def test_backslash_escape_in_mysql(self) -> None:
        r"""MySQL 档下 \' 是转义，不结束字符串"""
        self.assertEqual(
            len(split_sql(r"INSERT INTO t VALUES ('a\';b'); SELECT 2;", "mysql")), 2
        )

    def test_backslash_is_plain_in_oracle(self) -> None:
        r"""Oracle 档下反斜杠是普通字符（'C:\' 不应吞掉结尾引号）"""
        self.assertEqual(
            len(split_sql(r"SELECT 'C:\'; SELECT 2 FROM DUAL;", "oracle")), 2
        )


class TestProceduralBodies(unittest.TestCase):
    """过程体内的分号不得切分"""

    def test_mysql_begin_end(self) -> None:
        """MySQL BEGIN...END 整体成一条"""
        sql = "CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END;\nSELECT 3;"
        self.assertEqual(len(split_sql(sql, "mysql")), 2)

    def test_mysql_if_end_if(self) -> None:
        """IF...END IF 配对，不把外层 BEGIN 提前闭合"""
        sql = (
            "CREATE PROCEDURE p() BEGIN IF x THEN SELECT 1; END IF; END;\nSELECT 2;"
        )
        self.assertEqual(len(split_sql(sql, "mysql")), 2)

    def test_mysql_loop_end_loop(self) -> None:
        """LOOP...END LOOP 配对"""
        sql = (
            "CREATE PROCEDURE p() BEGIN LOOP SELECT 1; END LOOP; END;\nSELECT 2;"
        )
        self.assertEqual(len(split_sql(sql, "mysql")), 2)

    def test_oracle_anonymous_block(self) -> None:
        """Oracle 匿名块 BEGIN...END; 整体成一条"""
        self.assertEqual(len(split_sql("BEGIN NULL; NULL; END;\nSELECT 1;", "oracle")), 2)

    def test_oracle_case_end_case(self) -> None:
        """CASE...END CASE 配对"""
        sql = "BEGIN CASE WHEN 1=1 THEN NULL; END CASE; END;\nSELECT 1;"
        self.assertEqual(len(split_sql(sql, "oracle")), 2)

    def test_postgres_dollar_quoted_function(self) -> None:
        """PG $$ 函数体整体成一条"""
        sql = (
            "CREATE FUNCTION f() RETURNS void AS $$ BEGIN PERFORM 1; END; $$ "
            "LANGUAGE plpgsql;\nSELECT 1;"
        )
        self.assertEqual(len(split_sql(sql, "postgres")), 2)

    def test_postgres_tagged_dollar_quote(self) -> None:
        """PG $tag$ 函数体整体成一条"""
        sql = (
            "CREATE FUNCTION f() RETURNS void AS $body$ SELECT 1; SELECT 2; $body$ "
            "LANGUAGE sql;\nSELECT 3;"
        )
        self.assertEqual(len(split_sql(sql, "postgres")), 2)

    def test_postgres_positional_parameter_is_not_quote(self) -> None:
        """$1 是位置参数，不是引用体开头"""
        self.assertEqual(len(split_sql("SELECT $1; SELECT $2;", "postgres")), 2)

    def test_postgres_nested_begin(self) -> None:
        """嵌套 BEGIN...END 正确配对"""
        sql = (
            "CREATE FUNCTION f() RETURNS void AS $$ BEGIN BEGIN NULL; END; END; $$ "
            "LANGUAGE plpgsql;"
        )
        self.assertEqual(len(split_sql(sql, "postgres")), 1)

    def test_sqlite_trigger(self) -> None:
        """SQLite 触发器 BEGIN...END 整体成一条"""
        sql = (
            "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT 1; SELECT 2; END;\n"
            "SELECT 3;"
        )
        self.assertEqual(len(split_sql(sql, "sqlite")), 2)

    def test_sqlserver_begin_end(self) -> None:
        """T-SQL BEGIN...END 整体成一条"""
        sql = "CREATE PROCEDURE p AS BEGIN SELECT 1; SELECT 2; END;\nSELECT 3;"
        self.assertEqual(len(split_sql(sql, "sqlserver")), 2)

    def test_sqlserver_if_is_not_paired(self) -> None:
        """T-SQL 的 IF 不与 END 配对（无 END IF），不应少减深度"""
        sql = "IF NOT EXISTS (SELECT 1) BEGIN SELECT 2; END;\nSELECT 3;"
        self.assertEqual(len(split_sql(sql, "sqlserver")), 2)


class TestGbaseSpl(unittest.TestCase):
    """GBase 8s 的 SPL 过程体（无 BEGIN，靠 END PROCEDURE 收尾）"""

    def test_spl_procedure(self) -> None:
        """SPL 过程体整体成一条"""
        sql = (
            "CREATE PROCEDURE p() DEFINE i INT; LET i = 1; END PROCEDURE;\n"
            "SELECT 1 FROM DUAL;"
        )
        self.assertEqual(len(split_sql(sql, "gbase8s")), 2)

    def test_spl_function(self) -> None:
        """SPL 函数整体成一条"""
        sql = (
            "CREATE FUNCTION f(a INT) RETURNING INT; RETURN a + 1; END FUNCTION;\n"
            "SELECT 1 FROM DUAL;"
        )
        self.assertEqual(len(split_sql(sql, "gbase8s")), 2)

    def test_spl_trigger_without_end_trigger(self) -> None:
        """触发器写成 FOR EACH ROW (单条语句) 时不吞掉后续语句

        GBase 触发器有两种写法：带 ``BEGIN...END TRIGGER`` 的，和
        ``FOR EACH ROW (单条语句);`` 的。后者没有 ``END TRIGGER``，
        若按 SPL 过程体处理就会永远闭不上、把后续语句全部吞掉。
        """
        sql = (
            "CREATE TRIGGER tr INSERT ON t REFERENCING NEW AS n FOR EACH ROW "
            "(INSERT INTO l VALUES(1));\nSELECT 1 FROM DUAL;"
        )
        self.assertEqual(len(split_sql(sql, "gbase8s")), 2)

    def test_spl_mixed_script(self) -> None:
        """GBase 真实混合脚本：DDL + 插入 + 过程 + 查询 + 会话语句"""
        script = (
            "CREATE TABLE t (id SERIAL8, n LVARCHAR(50));\n"
            "INSERT INTO t (n) VALUES ('张三;李四');\n"
            "CREATE PROCEDURE p(c LVARCHAR(50))\n"
            "    DEFINE i INT;\n"
            "    LET i = 1;\n"
            "    INSERT INTO t (n) VALUES (c);\n"
            "END PROCEDURE;\n"
            "SELECT FIRST 10 SKIP 5 t.* FROM t;\n"
            "SET LOCK MODE TO WAIT 30;\n"
            "COMMIT WORK;\n"
        )
        result = split_sql(script, "gbase8s")
        self.assertEqual(len(result), 6)
        # 过程体必须整体保留（内含 DEFINE / LET / INSERT 三处语句分号
        # + 结尾 END PROCEDURE 的分号 = 4 个）
        procedure = [s.text for s in result if "CREATE PROCEDURE" in s.text][0]
        self.assertEqual(procedure.count(";"), 4)
        self.assertTrue(procedure.rstrip().endswith("END PROCEDURE;"))


class TestTransactionStatements(unittest.TestCase):
    """BEGIN 作为事务语句时不能当过程体开头"""

    def test_begin_work(self) -> None:
        """BEGIN WORK 是事务语句（GBase）"""
        self.assertEqual(
            len(split_sql("BEGIN WORK; SELECT 1; COMMIT WORK;", "gbase8s")), 3
        )

    def test_begin_transaction(self) -> None:
        """BEGIN TRANSACTION 是事务语句"""
        self.assertEqual(
            len(split_sql("BEGIN TRANSACTION; SELECT 1; COMMIT;", "sqlserver")), 3
        )

    def test_bare_begin(self) -> None:
        """孤立 BEGIN; 是事务语句（SQLite / PG）"""
        self.assertEqual(len(split_sql("BEGIN; SELECT 1; COMMIT;", "sqlite")), 3)

    def test_begin_immediate(self) -> None:
        """BEGIN IMMEDIATE 是事务语句"""
        self.assertEqual(
            len(split_sql("BEGIN IMMEDIATE; SELECT 1; COMMIT;", "sqlite")), 3
        )


class TestBatchSeparators(unittest.TestCase):
    """T-SQL 的 GO 与 Oracle 的 /"""

    def test_go_separates_statements(self) -> None:
        """GO 独占一行时切分，且自身不作为语句"""
        self.assertEqual(texts("SELECT 1\nGO\nSELECT 2\n", "sqlserver"),
                         ["SELECT 1", "SELECT 2"])

    def test_go_case_insensitive(self) -> None:
        """GO 大小写不敏感"""
        self.assertEqual(len(split_sql("SELECT 1\ngo\nSELECT 2", "sqlserver")), 2)

    def test_go_with_semicolons(self) -> None:
        """GO 与分号混用时都能切"""
        self.assertEqual(len(split_sql("SELECT 1;\nGO\nSELECT 2;", "sqlserver")), 2)

    def test_go_not_separator_outside_tsql(self) -> None:
        """非 T-SQL 档下 GO 只是普通词"""
        self.assertEqual(len(split_sql("SELECT 1\nGO\n", None)), 1)

    def test_goto_is_not_mistaken_for_go(self) -> None:
        """GOTO 不会被误判为分隔符"""
        self.assertEqual(len(split_sql("GOTO label;\nSELECT 1;", "sqlserver")), 2)

    def test_go_inside_string(self) -> None:
        """字符串里的 GO 不切分"""
        self.assertEqual(len(split_sql("SELECT 'GO' FROM t;", "sqlserver")), 1)

    def test_oracle_slash_terminator(self) -> None:
        """Oracle 的 / 独占一行时是终结符，不作为语句"""
        self.assertEqual(
            texts("SELECT 1 FROM DUAL\n/\nSELECT 2 FROM DUAL\n/", "oracle"),
            ["SELECT 1 FROM DUAL", "SELECT 2 FROM DUAL"],
        )

    def test_division_operator_not_terminator(self) -> None:
        """除号不误判为终结符"""
        self.assertEqual(
            len(split_sql("SELECT 10 / 2 FROM DUAL;\nSELECT 1 FROM DUAL;", "oracle")),
            2,
        )


class TestQuotingForms(unittest.TestCase):
    """各方言的引用体"""

    def test_tsql_bracket_identifier(self) -> None:
        """方括号标识符内的分号与点号不切分"""
        self.assertEqual(
            len(split_sql("SELECT [col;name] FROM [dbo].[t];\nSELECT 1;", "sqlserver")),
            2,
        )

    def test_tsql_bracket_escape(self) -> None:
        """]] 是方括号内的转义"""
        self.assertEqual(len(split_sql("SELECT [a]]b] FROM t;", "sqlserver")), 1)

    def test_oracle_q_quote(self) -> None:
        """Oracle q'[...]' 替代引用内的分号不切分"""
        self.assertEqual(
            len(split_sql("SELECT q'[a;b]' FROM DUAL;\nSELECT 1;", "oracle")), 2
        )

    def test_oracle_q_quote_custom_delimiter(self) -> None:
        """q'X...X' 自定义界符"""
        self.assertEqual(len(split_sql("SELECT q'Xa;bX' FROM DUAL;", "oracle")), 1)


class TestRobustness(unittest.TestCase):
    """容错：宁可少切，绝不多切"""

    def test_unclosed_string(self) -> None:
        """未闭合字符串不切分后续内容"""
        result = split_sql("SELECT 'abc", None)
        self.assertEqual(len(result), 1)
        self.assertFalse(result[0].is_complete)

    def test_unclosed_block_comment(self) -> None:
        """未闭合块注释被吞掉"""
        self.assertEqual(len(split_sql("SELECT 1 /* 未闭合", None)), 1)

    def test_unclosed_procedure_body(self) -> None:
        """未闭合过程体不切分（多切会执行半条语句）"""
        self.assertEqual(
            len(split_sql("CREATE PROCEDURE p() BEGIN SELECT 1;\nSELECT 2;", "mysql")),
            1,
        )

    def test_complete_statement_flag(self) -> None:
        """正常语句标记为完整"""
        self.assertTrue(split_sql("SELECT 1;", None)[0].is_complete)

    def test_cjk_identifier_does_not_hang(self) -> None:
        """回归：中文标识符不得导致死循环

        ``str.isalpha()`` 对中文为真，但中文不在词法字符集内；
        早期实现读不到字符又不推进游标，``SELECT * FROM 用户表`` 会卡死整个 CLI。
        """
        self.assertEqual(texts("SELECT * FROM 用户表"), ["SELECT * FROM 用户表"])
        self.assertEqual(len(split_sql("CREATE TABLE 表 (列 INT); SELECT 1;")), 2)
        self.assertEqual(
            len(split_sql("INSERT INTO 表 VALUES ('中文;分号'); SELECT 1;")), 2
        )

    def test_if_function_call_is_not_opener(self) -> None:
        """IF(...) 是函数调用，不计入过程体嵌套"""
        self.assertEqual(
            len(split_sql("SELECT IF(a>b, 1, 0) FROM t;\nSELECT 2;", "mysql")), 2
        )

    def test_case_expression_in_select(self) -> None:
        """SELECT 里的 CASE 表达式与过程体 CASE 都能正确配对"""
        self.assertEqual(
            len(split_sql("SELECT CASE WHEN a THEN 1 ELSE 2 END FROM t;\nSELECT 1;")),
            2,
        )

    def test_if_exists_is_not_procedural_if(self) -> None:
        """回归：``IF EXISTS`` 是 DDL 条件词，不得当作过程体 IF

        它后面没有 ``END IF``；若误计入嵌套深度，分号将不再切分，
        ``DROP TABLE IF EXISTS t;`` 会把后续建表、插入、查询全部吞成一条
        （实测会导致整段脚本只有 2 条语句，逐条执行必然失败）。
        """
        sql = "DROP TABLE IF EXISTS t;\nCREATE TABLE t (id INT);\nSELECT 1;"
        result = split_sql(sql, "gbase8s")
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0].text, "DROP TABLE IF EXISTS t")

    def test_create_table_if_not_exists(self) -> None:
        """``IF NOT EXISTS`` 同样不配对"""
        sql = "CREATE TABLE IF NOT EXISTS t (id INT);\nSELECT 1;"
        self.assertEqual(len(split_sql(sql, "mysql")), 2)


class TestMetadata(unittest.TestCase):
    """切分结果的元信息"""

    def test_line_numbers(self) -> None:
        """行号用于报错定位"""
        result = split_sql("SELECT 1;\nSELECT 2;\n\nSELECT 3;")
        self.assertEqual([s.line for s in result], [1, 2, 4])

    def test_offsets_point_into_original(self) -> None:
        """start/end 能切出原文"""
        sql = "SELECT 1;\n  SELECT 2;"
        for statement in split_sql(sql):
            self.assertEqual(sql[statement.start : statement.end], statement.text)

    def test_returns_dataclass(self) -> None:
        """返回 SplitStatement 实例"""
        result = split_sql("SELECT 1;")
        self.assertIsInstance(result[0], SplitStatement)
        self.assertEqual(result[0].text, "SELECT 1")


class TestDialectResolution(unittest.TestCase):
    """方言名解析"""

    def test_known_names(self) -> None:
        """六库都能解析到各自档位"""
        expected = {
            "oracle": "oracle",
            "mysql": "mysql",
            "postgresql": "postgresql",
            "sqlserver": "sqlserver",
            "sqlite": "sqlite",
            "gbase8s": "gbase8s",
        }
        for name, normalized in expected.items():
            with self.subTest(name=name):
                self.assertEqual(resolve_profile(name).name, normalized)

    def test_aliases(self) -> None:
        """常见别名可用"""
        self.assertEqual(resolve_profile("postgres").name, "postgresql")
        self.assertEqual(resolve_profile("pg").name, "postgresql")
        self.assertEqual(resolve_profile("TSQL").name, "sqlserver")
        self.assertEqual(resolve_profile("mssql").name, "sqlserver")
        self.assertEqual(resolve_profile("gbase").name, "gbase8s")
        self.assertEqual(resolve_profile("informix").name, "gbase8s")
        self.assertEqual(resolve_profile("mariadb").name, "mysql")

    def test_unknown_and_none_fall_back_to_generic(self) -> None:
        """未知方言与 None 都回退通用档"""
        self.assertEqual(resolve_profile(None).name, "generic")
        self.assertEqual(resolve_profile("").name, "generic")
        self.assertEqual(resolve_profile("   ").name, "generic")
        self.assertEqual(resolve_profile("不存在的方言").name, "generic")

    def test_profile_flags(self) -> None:
        """各档的特性开关符合预期"""
        self.assertTrue(resolve_profile("postgres").dollar_quoting)
        self.assertTrue(resolve_profile("postgres").nested_block_comments)
        self.assertTrue(resolve_profile("mysql").hash_comment)
        self.assertTrue(resolve_profile("mysql").backslash_escape)
        self.assertTrue(resolve_profile("sqlserver").bracket_identifier)
        self.assertTrue(resolve_profile("sqlserver").go_separator)
        self.assertFalse(resolve_profile("sqlserver").end_if_style)
        self.assertFalse(resolve_profile("sqlite").end_if_style)
        self.assertTrue(resolve_profile("gbase8s").spl_terminator)
        self.assertTrue(resolve_profile("oracle").q_quote)
        self.assertTrue(resolve_profile("oracle").slash_terminator)
        # 通用档保守：不启用任何方言特有形态
        generic = resolve_profile(None)
        self.assertFalse(generic.dollar_quoting)
        self.assertFalse(generic.bracket_identifier)
        self.assertFalse(generic.q_quote)
        self.assertFalse(generic.hash_comment)
        self.assertIsInstance(generic, DialectProfile)


if __name__ == "__main__":
    unittest.main()
