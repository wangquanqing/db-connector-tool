import logging
import os
import tempfile
import unittest
from pathlib import Path

from src.db_connector_tool.utils.logging_utils import (
    LOG_LEVEL_MAP,
    VALID_LOG_LEVELS,
    LogManager,
    _validate_log_level,
    get_logger,
    set_log_level,
    setup_logging,
)


class TestLoggingUtils(unittest.TestCase):
    """测试日志工具模块"""

    def setUp(self):
        """设置测试环境"""
        self.temp_dir = tempfile.mkdtemp()
        self.app_name = "test_app"

    def tearDown(self):
        """清理测试环境"""
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_validate_log_level(self):
        """测试日志级别验证"""
        # 测试有效日志级别
        for level in VALID_LOG_LEVELS:
            self.assertEqual(_validate_log_level(level), LOG_LEVEL_MAP[level])
            # 测试大小写不敏感
            self.assertEqual(_validate_log_level(level.lower()), LOG_LEVEL_MAP[level])

        # 测试无效日志级别
        with self.assertRaises(ValueError):
            _validate_log_level("INVALID_LEVEL")

    def test_setup_logging(self):
        """测试日志系统配置"""
        # 测试基本配置
        logger = setup_logging(
            app_name=self.app_name,
            level="DEBUG",
            log_to_console=False,
            log_to_file=True,
            log_dir=self.temp_dir,
        )

        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, self.app_name)
        self.assertEqual(logger.level, logging.DEBUG)

        # 测试文件输出
        log_file = Path(self.temp_dir) / f"{self.app_name}.log"
        self.assertTrue(log_file.exists())

        # 测试控制台输出
        logger2 = setup_logging(
            app_name=self.app_name + "_console",
            level="INFO",
            log_to_console=True,
            log_to_file=False,
        )
        self.assertEqual(logger2.level, logging.INFO)

        # 测试错误：未启用任何输出方式
        with self.assertRaises(ValueError):
            setup_logging(
                app_name=self.app_name + "_error",
                log_to_console=False,
                log_to_file=False,
            )

    def test_get_logger(self):
        """测试获取logger实例"""
        logger = get_logger("test_module")
        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, "test_module")

    def test_set_log_level(self):
        """测试动态设置日志级别"""
        logger_name = "test_logger"
        logger = get_logger(logger_name)
        logger.setLevel(logging.INFO)

        # 设置为DEBUG级别
        set_log_level(logger_name, "DEBUG")
        self.assertEqual(logger.level, logging.DEBUG)

        # 设置为ERROR级别
        set_log_level(logger_name, "ERROR")
        self.assertEqual(logger.level, logging.ERROR)

        # 测试无效级别
        with self.assertRaises(ValueError):
            set_log_level(logger_name, "INVALID")

    def test_log_manager_setup(self):
        """测试LogManager的setup方法"""
        log_manager = LogManager(self.app_name)
        logger = log_manager.setup(
            level="DEBUG", log_to_file=True, log_dir=self.temp_dir
        )

        self.assertIsInstance(logger, logging.Logger)
        self.assertEqual(logger.name, self.app_name)

    def test_log_manager_add_file_handler(self):
        """测试LogManager添加文件handler"""
        log_manager = LogManager(self.app_name)
        log_manager.setup(level="INFO", log_to_file=False, log_to_console=False)

        # 添加文件handler
        test_log_file = os.path.join(self.temp_dir, "test.log")
        log_manager.add_file_handler(test_log_file, level="DEBUG")

        # 验证文件创建
        self.assertTrue(os.path.exists(test_log_file))

    def test_log_manager_remove_handler(self):
        """测试LogManager移除handler"""
        log_manager = LogManager(self.app_name)
        logger = log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 获取handler并移除
        if logger.handlers:
            handler = logger.handlers[0]
            log_manager.remove_handler(handler)
            self.assertNotIn(handler, logger.handlers)

    def test_log_manager_cleanup(self):
        """测试LogManager清理资源"""
        log_manager = LogManager(self.app_name)
        log_manager.setup(level="INFO", log_to_file=True, log_dir=self.temp_dir)

        # 清理
        log_manager.cleanup()

    def test_log_manager_get_loggers_info(self):
        """测试LogManager获取logger信息"""
        log_manager = LogManager(self.app_name)
        info = log_manager.get_loggers_info()
        self.assertIsInstance(info, dict)

    def test_log_manager_quick_setup(self):
        """测试LogManager快速设置"""
        log_manager = LogManager.quick_setup(self.app_name, "DEBUG")
        self.assertIsInstance(log_manager, LogManager)
        self.assertEqual(log_manager.app_name, self.app_name)


if __name__ == "__main__":
    unittest.main()
