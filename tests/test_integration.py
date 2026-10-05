"""集成测试模块

测试系统各组件之间的协作，确保整个系统能够正常工作。
"""

import sqlite3
import unittest
from contextlib import closing

from src.db_connector_tool import (
    BatchDatabaseManager,
    DatabaseManager,
    generate_ip_range,
)
from src.db_connector_tool.core.config import ConfigManager
from src.db_connector_tool.core.crypto import CryptoManager
from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.core.validators import PasswordValidator

from ._isolation import IsolatedTestCase


class TestIntegration(IsolatedTestCase):
    """集成测试类"""

    def setUp(self):
        """设置测试环境"""
        super().setUp()
        # 创建配置管理器和相关实例（配置目录已被隔离到临时目录）
        self.config_manager = ConfigManager("test_app")
        self.key_manager = KeyManager("test_app")
        self.db_manager = DatabaseManager("test_app")

    def tearDown(self):
        """清理测试环境"""
        self.db_manager.close_all_connections()
        super().tearDown()

    def test_full_workflow(self):
        """测试完整的工作流程"""
        # 1. 测试密钥管理
        self.key_manager.load_or_create_key()
        self.assertTrue(self.key_manager.crypto is not None)

        # 2. 测试配置管理
        test_config = {"type": "sqlite", "database": ":memory:"}
        self.config_manager.add_config("test_conn", test_config)
        configs = self.config_manager.list_configs()
        self.assertIn("test_conn", configs)

        # 3. 测试数据库管理
        # 先检查连接是否存在，如果存在则删除
        if "test_conn" in self.db_manager.list_connections():
            self.db_manager.remove_connection("test_conn")
        self.db_manager.add_connection("test_conn", test_config)
        connections = self.db_manager.list_connections()
        self.assertIn("test_conn", connections)

        # 4. 测试连接测试
        result = self.db_manager.test_connection("test_conn")
        self.assertTrue(result)

        # 5. 测试执行查询
        create_table_sql = "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
        insert_sql = "INSERT INTO test (name) VALUES ('test')"
        select_sql = "SELECT * FROM test"

        # 执行创建表
        self.db_manager.execute_command("test_conn", create_table_sql)
        # 执行插入（返回 (影响行数, 耗时) 元组）
        affected_rows, _elapsed = self.db_manager.execute_command(
            "test_conn", insert_sql
        )
        self.assertEqual(affected_rows, 1)
        # 执行查询（返回 (行列表, 耗时) 元组）
        results, _elapsed = self.db_manager.execute_query("test_conn", select_sql)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "test")

        # 6. 测试删除连接
        self.db_manager.remove_connection("test_conn")
        connections = self.db_manager.list_connections()
        self.assertNotIn("test_conn", connections)

    def test_batch_manager_integration(self):
        """测试批处理管理器集成"""
        # 设置基础配置
        # SQLite 连接有线程亲和性，而批量管理器内部使用线程池，
        # 因此这里用文件库并只做一次批量操作（同一连接不跨线程复用）
        db_file = self.temp_path / "batch.db"
        with closing(sqlite3.connect(db_file)) as raw_connection:
            raw_connection.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"
            )
            raw_connection.execute("INSERT INTO test (name) VALUES ('test')")
            raw_connection.commit()

        base_config = {"type": "sqlite", "database": str(db_file)}

        # 创建批量管理器
        with BatchDatabaseManager("test_batch") as batch_manager:
            batch_manager.set_base_config(base_config)

            # 生成IP范围
            ip_list = generate_ip_range("127.0.0.1", 2)

            # 批量添加连接
            results = batch_manager.add_batch_connections(ip_list)
            self.assertEqual(len(results), 2)
            self.assertEqual(len(batch_manager._get_all_connection_names()), 2)

            # 执行批量查询（结果为 {连接名: {"success", "data", "count", ...}}）
            query_results = batch_manager.execute_batch_query(
                "SELECT COUNT(*) AS total FROM test", max_workers=1
            )
            self.assertEqual(len(query_results), 2)
            for conn_result in query_results.values():
                self.assertTrue(conn_result["success"], msg=conn_result)
                self.assertEqual(conn_result["count"], 1)
                self.assertEqual(conn_result["data"][0]["total"], 1)

    def test_crypto_integration(self):
        """测试加密模块集成"""
        # 测试加密解密
        crypto = CryptoManager()
        test_data = "敏感数据"
        encrypted = crypto.encrypt(test_data)
        decrypted = crypto.decrypt(encrypted)
        self.assertEqual(decrypted, test_data)

        # 测试密码强度验证
        strong_password = "StrongPassword123!"
        weak_password = "weak"
        self.assertTrue(PasswordValidator.validate_strength(strong_password))
        self.assertFalse(PasswordValidator.validate_strength(weak_password))


if __name__ == "__main__":
    unittest.main()
