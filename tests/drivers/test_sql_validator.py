"""SQL 校验策略单元测试 (sql_validator)"""

import unittest

from src.db_connector_tool.drivers.sql_validator import (
    MAX_SQL_LENGTH,
    strip_sql_literals_and_comments,
    validate_sql,
)


class TestStripSqlLiterals(unittest.TestCase):
    """字面量与注释剥离"""

    def test_keeps_plain_structure(self) -> None:
        """无字面量时结构保持不变"""
        sql = "SELECT a FROM t WHERE b = 1"
        self.assertEqual(strip_sql_literals_and_comments(sql), sql)

    @staticmethod
    def normalized(sql: str) -> str:
        """把剥离结果的连续空白折叠为单个空格，便于断言"""
        return " ".join(strip_sql_literals_and_comments(sql).split())

    def test_replaces_string_literal(self) -> None:
        """单引号字面量整体被移除，语句骨架保留"""
        self.assertEqual(
            self.normalized("SELECT * FROM t WHERE a = 'x y'"),
            "SELECT * FROM t WHERE a =",
        )

    def test_handles_escaped_quote(self) -> None:
        """连续两个引号表示转义，不提前结束字面量"""
        self.assertEqual(self.normalized("SELECT 'it''s' FROM t"), "SELECT FROM t")

    def test_handles_line_comment(self) -> None:
        """行注释连同换行一起被剥离"""
        stripped = strip_sql_literals_and_comments("SELECT 1 -- note\nFROM t")
        self.assertNotIn("note", stripped)
        self.assertIn("FROM t", stripped)

    def test_handles_block_comment(self) -> None:
        """块注释被剥离"""
        stripped = strip_sql_literals_and_comments("SELECT /* c */ 1 FROM t")
        self.assertNotIn("c", stripped)
        self.assertIn("SELECT", stripped)

    def test_handles_backtick_identifier(self) -> None:
        """反引号标识符按字面量处理（MySQL 风格）"""
        self.assertEqual(self.normalized("SELECT `col` FROM t"), "SELECT FROM t")

    def test_unterminated_literal(self) -> None:
        """未闭合字面量不抛异常"""
        self.assertEqual(self.normalized("SELECT 'abc"), "SELECT")


class TestValidateSql(unittest.TestCase):
    """校验入口"""

    def test_length_limit(self) -> None:
        """超过长度上限被拒绝"""
        with self.assertRaises(ValueError) as context:
            validate_sql("SELECT 1 " * (MAX_SQL_LENGTH // 4))
        self.assertIn("长度超过限制", str(context.exception))

    def test_allows_common_statements(self) -> None:
        """常见语句均放行"""
        for sql in (
            "SELECT * FROM users WHERE id = :id",
            "INSERT INTO t (a) VALUES (:a)",
            "UPDATE t SET a = 1 WHERE id = 2",
            "DELETE FROM t WHERE id = 3",
            "CREATE TABLE t (id INT)",
            "ALTER TABLE t ADD COLUMN c INT",
            "DROP TABLE t",
            "TRUNCATE TABLE t",
            "SELECT table_name FROM information_schema.tables",
            "SELECT id FROM a UNION SELECT id FROM b",
        ):
            with self.subTest(sql=sql):
                validate_sql(sql)

    def test_allows_injection_text_inside_literal(self) -> None:
        """字面量中的注入文本属数据，不误报"""
        for sql in (
            "SELECT * FROM logs WHERE msg = 'OR 1=1'",
            "SELECT * FROM logs WHERE msg = 'xp_cmdshell'",
            "SELECT * FROM logs WHERE msg = 'a; DROP TABLE t'",
        ):
            with self.subTest(sql=sql):
                validate_sql(sql)

    def test_blocks_structural_injection(self) -> None:
        """结构性注入被拦截"""
        for sql in (
            "SELECT * FROM users WHERE id = 1 OR 1=1",
            "SELECT * FROM users WHERE id = '1' OR '1'='1'",
            "SELECT * FROM users WHERE id = 1 AND 1=1",
            "EXEC xp_cmdshell 'dir'",
            "SELECT * FROM t WHERE a = 1 INTO OUTFILE '/tmp/x'",
            "SELECT * FROM t WHERE a = 1; SHUTDOWN",
        ):
            with self.subTest(sql=sql):
                with self.assertRaises(ValueError):
                    validate_sql(sql)

    def test_blocks_multi_statement(self) -> None:
        """语句走私被拦截"""
        with self.assertRaises(ValueError) as context:
            validate_sql("SELECT 1; DROP TABLE t")
        self.assertIn("多条语句", str(context.exception))

    def test_allows_procedural_body_with_semicolons(self) -> None:
        """存储过程/触发器体内的分号不算走私"""
        validate_sql("CREATE PROCEDURE p() BEGIN SELECT 1; END")
        validate_sql("CREATE FUNCTION f() RETURNS INT BEGIN RETURN 1; END")
        validate_sql("CREATE TRIGGER tr AFTER INSERT ON t BEGIN END")

    def test_blocks_suspicious_comments(self) -> None:
        """可疑注释被拦截"""
        with self.assertRaises(ValueError):
            validate_sql("SELECT * FROM t WHERE id = 1' --")
        with self.assertRaises(ValueError):
            validate_sql("SELECT * FROM t /*!50000 SELECT */")


if __name__ == "__main__":
    unittest.main()
