"""
CLI工具测试
"""

import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from db_connector.cli import DBConnectorCLI


class TestCLI:
    """CLI测试类"""

    def setup_method(self):
        """测试方法 setup"""
        self.cli = DBConnectorCLI()
        self.test_dir = tempfile.mkdtemp()

    def teardown_method(self):
        """测试方法 teardown"""
        import shutil

        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_cli_initialization(self):
        """测试CLI初始化"""
        assert self.cli.db_manager is None
        self.cli.init_db_manager()
        assert self.cli.db_manager is not None

    def test_display_results_table(self):
        """测试表格显示"""
        test_data = [
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
        ]

        # 直接调用方法，确保不抛出异常即可
        self.cli._display_results(test_data, "table")

    def test_display_results_json(self):
        """测试JSON显示"""
        test_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        # 确保不抛出异常
        self.cli._display_results(test_data, "json")

    def test_save_output(self):
        """测试输出保存"""
        test_data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        # 测试JSON输出
        json_file = Path(self.test_dir) / "test.json"
        self.cli._save_output(test_data, str(json_file), "json")
        assert json_file.exists()

        # 测试CSV输出
        csv_file = Path(self.test_dir) / "test.csv"
        self.cli._save_output(test_data, str(csv_file), "csv")
        assert csv_file.exists()

    def test_command_line_help(self):
        """测试命令行帮助"""
        result = subprocess.run(
            [sys.executable, "-m", "db_connector.cli", "--help"],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "DB Connector" in result.stdout


def test_cli_integration():
    """CLI集成测试（需要真实数据库）"""
    # 这个测试需要真实的数据库连接，可以根据实际情况启用
    pass


if __name__ == "__main__":
    pytest.main()
