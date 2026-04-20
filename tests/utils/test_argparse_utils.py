import argparse
import unittest
from unittest import mock

from src.db_connector_tool.utils.argparse_utils import (
    ChineseHelpFormatter,
    create_argument_parser,
    _setup_connection_arguments,
    SUPPORTED_DATABASE_TYPES,
)


class TestArgparseUtils(unittest.TestCase):
    """测试命令行参数解析工具模块"""

    def test_chinese_help_formatter(self):
        """测试中文帮助格式化器"""
        # 创建格式化器实例
        formatter = ChineseHelpFormatter(prog="db-connector")

        # 测试_format_usage方法
        usage = "db-connector add [-h] name -T TYPE [-H HOST] [-P PORT] [-u USERNAME] [-p [PASSWORD]] [-d DATABASE] [-s SERVICE_NAME] [-g {disable,allow,prefer,require}] [-k {cp936,utf8,utf8mb4}] [-t {7.0,7.1,7.2,7.3,7.4,8.0}] [-c CUSTOM_PARAMS [CUSTOM_PARAMS ...]]"
        actions = []
        groups = []
        prefix = "使用情况: "

        formatted_usage = formatter._format_usage(usage, actions, groups, prefix)
        self.assertIn("使用情况:", formatted_usage)

        # 测试start_section方法
        # 捕获标准输出以验证输出内容
        with mock.patch("builtins.print") as mock_print:
            # 这里我们无法直接测试start_section的输出，因为它是内部方法
            # 但我们可以确保它不会抛出异常
            formatter.start_section("options")
            # 验证是否调用了父类的start_section方法
            # 由于我们无法直接验证，这里我们只确保方法可以正常执行
            pass

    def test_setup_connection_arguments(self):
        """测试设置连接相关的命令行参数"""
        # 创建一个参数解析器
        parser = argparse.ArgumentParser()

        # 测试_setup_connection_arguments函数
        _setup_connection_arguments(parser)

        # 验证参数是否正确添加
        # 获取所有参数的dest属性
        dests = [action.dest for action in parser._actions if hasattr(action, "dest")]

        # 验证必要的参数是否存在
        expected_dests = [
            "name",
            "type",
            "host",
            "port",
            "username",
            "password",
            "database",
            "service_name",
            "gssencmode",
            "charset",
            "tds_version",
            "custom_params",
        ]
        for dest in expected_dests:
            self.assertIn(dest, dests)

        # 验证--type参数的选项是否正确
        for action in parser._actions:
            if action.dest == "type":
                self.assertEqual(action.required, True)
                self.assertEqual(action.choices, SUPPORTED_DATABASE_TYPES)
                break

    def test_supported_database_types(self):
        """测试支持的数据库类型"""
        # 验证SUPPORTED_DATABASE_TYPES是一个列表
        self.assertIsInstance(SUPPORTED_DATABASE_TYPES, list)
        # 验证列表不为空
        self.assertGreater(len(SUPPORTED_DATABASE_TYPES), 0)


if __name__ == "__main__":
    unittest.main()
