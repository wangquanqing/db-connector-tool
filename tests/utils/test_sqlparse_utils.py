import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))
import unittest
from db_connector_tool.utils.sqlparse_utils import SQLStatementParser


class TestBasicSplitting(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_single_statement(self):
        statements = self.parser.parse("SELECT 1;")
        self.assertEqual(len(statements), 1)

    def test_multiple_statements(self):
        statements = self.parser.parse("SELECT 1; SELECT 2; SELECT 3;")
        self.assertEqual(len(statements), 3)

    def test_empty_content(self):
        self.assertEqual(self.parser.parse(""), [])

    def test_no_trailing_semicolon(self):
        statements = self.parser.parse("SELECT 1")
        self.assertEqual(len(statements), 1)


class TestStringLiterals(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_single_quoted_semicolon(self):
        statements = self.parser.parse("SELECT 'hello; world';")
        self.assertEqual(len(statements), 1)

    def test_double_quoted_semicolon(self):
        statements = self.parser.parse('SELECT "hello; world";')
        self.assertEqual(len(statements), 1)


class TestComments(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_line_comment(self):
        statements = self.parser.parse("SELECT 1; -- comment; still one\nSELECT 2;")
        self.assertEqual(len(statements), 2)

    def test_block_comment(self):
        sql = "SELECT 1; /* block; with; semicolons */ SELECT 2;"
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 2)


class TestPostgreSQL(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_dollar_quote(self):
        sql = "SELECT $$hello; world$$ AS greeting;"
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)

    def test_create_function_dollar(self):
        sql = (
            "CREATE FUNCTION add(a INT, b INT) RETURNS INT AS $$\n"
            "BEGIN\n"
            "    RETURN a + b;\n"
            "END;\n"
            "$$ LANGUAGE plpgsql;"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)


class TestMySQL(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_backtick(self):
        statements = self.parser.parse("SELECT `col;name` FROM users;")
        self.assertEqual(len(statements), 1)

    def test_delimiter_basic(self):
        sql = (
            "DELIMITER //\n"
            "CREATE PROCEDURE test_proc()\n"
            "BEGIN\n"
            "    SELECT 1;\n"
            "    SELECT 2;\n"
            "END//\n"
            "DELIMITER ;"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)

    def test_delimiter_multiple(self):
        sql = (
            "DELIMITER $$\n"
            "CREATE PROCEDURE proc1()\n"
            "BEGIN\n"
            "    SELECT 'proc1';\n"
            "END$$\n"
            "CREATE PROCEDURE proc2()\n"
            "BEGIN\n"
            "    SELECT 'proc2';\n"
            "END$$\n"
            "DELIMITER ;"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 2)

    def test_begin_transaction_no_suppression(self):
        sql = "BEGIN TRANSACTION; UPDATE t SET v=1; COMMIT;"
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 3)


class TestOracle(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_q_quote(self):
        statements = self.parser.parse("SELECT q'[it's a test]' FROM dual;")
        self.assertEqual(len(statements), 1)

    def test_create_procedure(self):
        sql = (
            "CREATE OR REPLACE PROCEDURE test_proc AS\n"
            "BEGIN\n"
            "    DBMS_OUTPUT.PUT_LINE('hello');\n"
            "END test_proc;\n"
            "/"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)

    def test_package_body_multi(self):
        sql = (
            "CREATE OR REPLACE PACKAGE BODY emp_pkg AS\n"
            "\n"
            "    PROCEDURE add_emp(name VARCHAR2, sal NUMBER) AS\n"
            "    BEGIN\n"
            "        INSERT INTO emp VALUES (seq.NEXTVAL, name, sal);\n"
            "        COMMIT;\n"
            "    END add_emp;\n"
            "\n"
            "    FUNCTION get_sal(emp_id NUMBER) RETURN NUMBER AS\n"
            "        v_sal NUMBER;\n"
            "    BEGIN\n"
            "        SELECT salary INTO v_sal FROM emp WHERE id = emp_id;\n"
            "        RETURN v_sal;\n"
            "    END get_sal;\n"
            "\n"
            "END emp_pkg;\n"
            "/"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)


class TestSQLServer(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_bracket(self):
        statements = self.parser.parse("SELECT [col;name] FROM users;")
        self.assertEqual(len(statements), 1)

    def test_n_string(self):
        statements = self.parser.parse("SELECT N'unicode; text';")
        self.assertEqual(len(statements), 1)

    def test_go_batch(self):
        sql = "SELECT 1;\nGO\nSELECT 2;"
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 2)


class TestDeepNesting(unittest.TestCase):

    def setUp(self):
        self.parser = SQLStatementParser()

    def test_mysql_deeply_nested(self):
        sql = (
            "DELIMITER $$\n"
            "CREATE PROCEDURE deeply_nested()\n"
            "BEGIN\n"
            "    DECLARE i INT DEFAULT 0;\n"
            "    outer_loop: LOOP\n"
            "        SET i = i + 1;\n"
            "        IF i > 100 THEN\n"
            "            LEAVE outer_loop;\n"
            "        END IF;\n"
            "        BEGIN\n"
            "            DECLARE j INT DEFAULT 0;\n"
            "            inner_loop: LOOP\n"
            "                SET j = j + 1;\n"
            "                IF j > 10 THEN\n"
            "                    LEAVE inner_loop;\n"
            "                END IF;\n"
            "                INSERT INTO log VALUES (i, j);\n"
            "            END LOOP inner_loop;\n"
            "        END;\n"
            "    END LOOP outer_loop;\n"
            "END$$\n"
            "DELIMITER ;"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)

    def test_postgresql_complex_trigger(self):
        sql = (
            "CREATE OR REPLACE FUNCTION complex_audit()\n"
            "RETURNS TRIGGER AS $$\n"
            "DECLARE\n"
            "    v_count INT;\n"
            "BEGIN\n"
            "    IF TG_OP = 'INSERT' THEN\n"
            "        BEGIN\n"
            "            SELECT COUNT(*) INTO v_count\n"
            "            FROM audit_log WHERE table_name = TG_TABLE_NAME;\n"
            "            IF v_count > 1000 THEN\n"
            "                BEGIN\n"
            "                    DELETE FROM audit_log\n"
            "                    WHERE created_at < now() - INTERVAL '30 days';\n"
            "                END;\n"
            "            END IF;\n"
            "        END;\n"
            "    END IF;\n"
            "    INSERT INTO audit_log VALUES (TG_TABLE_NAME, TG_OP, NEW::TEXT, now());\n"
            "    RETURN NEW;\n"
            "END;\n"
            "$$ LANGUAGE plpgsql;"
        )
        statements = self.parser.parse(sql)
        self.assertEqual(len(statements), 1)


if __name__ == "__main__":
    unittest.main()
