"""
配置管理测试
"""

import tempfile

import pytest

from db_connector.core.config import ConfigManager
from db_connector.core.exceptions import ConfigError


class TestConfigManager:
    """ConfigManager测试类"""

    def setup_method(self):
        """测试方法 setup"""
        self.test_dir = tempfile.mkdtemp()
        self.config_manager = ConfigManager(
            app_name="test_db_connector", config_file="test_connections.toml"
        )

    def teardown_method(self):
        """测试方法 teardown"""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_add_get_connection(self):
        """测试添加和获取连接"""
        test_config = {
            "type": "mysql",
            "host": "localhost",
            "port": "3306",
            "username": "test_user",
            "password": "test_password",
            "database": "test_db",
        }

        self.config_manager.add_connection("test_conn", test_config)
        retrieved_config = self.config_manager.get_connection("test_conn")

        assert retrieved_config["type"] == test_config["type"]
        assert retrieved_config["host"] == test_config["host"]
        assert retrieved_config["password"] == test_config["password"]

    def test_list_connections(self):
        """测试列出连接"""
        test_config = {"type": "sqlite", "database": ":memory:"}

        self.config_manager.add_connection("conn1", test_config)
        self.config_manager.add_connection("conn2", test_config)

        connections = self.config_manager.list_connections()

        assert "conn1" in connections
        assert "conn2" in connections
        assert len(connections) == 2

    def test_remove_connection(self):
        """测试删除连接"""
        test_config = {
            "type": "postgresql",
            "host": "localhost",
            "username": "user",
            "password": "pass",
            "database": "db",
        }

        self.config_manager.add_connection("temp_conn", test_config)

        # 确认连接存在
        connections = self.config_manager.list_connections()
        assert "temp_conn" in connections

        # 删除连接
        self.config_manager.remove_connection("temp_conn")

        # 确认连接已删除
        connections = self.config_manager.list_connections()
        assert "temp_conn" not in connections

    def test_get_nonexistent_connection(self):
        """测试获取不存在的连接"""
        with pytest.raises(ConfigError):
            self.config_manager.get_connection("nonexistent")


if __name__ == "__main__":
    pytest.main()
