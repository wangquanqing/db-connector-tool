"""
数据库管理测试
"""

import pytest

from db_connector.core.connections import DatabaseManager
from db_connector.core.exceptions import ConfigError


class TestDatabaseManager:
    """DatabaseManager测试类"""

    def setup_method(self):
        """测试方法 setup"""
        self.db_manager = DatabaseManager(app_name="test_db_manager")

    def test_create_connection_config(self):
        """测试创建连接配置"""
        mysql_config = {
            "type": "mysql",
            "host": "localhost",
            "port": "3306",
            "username": "test_user",
            "password": "test_pass",
            "database": "test_db",
        }

        self.db_manager.create_connection("test_mysql", mysql_config)

        connections = self.db_manager.list_connections()
        assert "test_mysql" in connections

    def test_invalid_config(self):
        """测试无效配置"""
        invalid_config = {"type": "invalid_db"}

        with pytest.raises(ConfigError):
            self.db_manager.create_connection("invalid", invalid_config)

    def test_sqlite_connection(self):
        """测试SQLite连接配置"""
        sqlite_config = {"type": "sqlite", "database": ":memory:"}

        self.db_manager.create_connection("test_sqlite", sqlite_config)

        # 测试获取连接（虽然:memory:数据库不能真正测试连接，但可以测试配置）
        connections = self.db_manager.list_connections()
        assert "test_sqlite" in connections


if __name__ == "__main__":
    pytest.main()
