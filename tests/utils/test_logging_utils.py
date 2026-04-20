import logging
import os
import shutil
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

    def test_setup_logging_with_separate_error_log(self):
        """测试单独的错误日志配置"""
        # 测试启用单独错误日志且级别为ERROR或更低
        logger = setup_logging(
            app_name=self.app_name + "_error_log",
            level="ERROR",
            log_to_console=False,
            log_to_file=True,
            log_dir=self.temp_dir,
            separate_error_log=True,
        )
        self.assertEqual(logger.level, logging.ERROR)

        # 验证错误日志文件创建
        error_log_file = Path(self.temp_dir) / f"{self.app_name}_error_log_error.log"
        self.assertTrue(error_log_file.exists())

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

        # 添加一个handler以便测试handler级别更新
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        # 设置为DEBUG级别
        set_log_level(logger_name, "DEBUG")
        self.assertEqual(logger.level, logging.DEBUG)
        # 验证handler级别也被更新
        self.assertEqual(handler.level, logging.DEBUG)

        # 设置为ERROR级别
        set_log_level(logger_name, "ERROR")
        self.assertEqual(logger.level, logging.ERROR)
        # 验证handler级别也被更新
        self.assertEqual(handler.level, logging.ERROR)

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
        log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 添加文件handler
        test_log_file = os.path.join(self.temp_dir, "test.log")
        log_manager.add_file_handler(test_log_file, level="DEBUG")

        # 验证文件创建
        self.assertTrue(os.path.exists(test_log_file))

    def test_log_manager_add_timed_file_handler(self):
        """测试LogManager添加基于时间轮转的文件handler"""
        log_manager = LogManager(self.app_name)
        log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 添加基于时间轮转的文件handler
        test_log_file = os.path.join(self.temp_dir, "timed_test.log")
        log_manager.add_file_handler(test_log_file, when="H", level="INFO")

        # 验证文件创建
        self.assertTrue(os.path.exists(test_log_file))

    def test_log_manager_remove_handler(self):
        """测试LogManager移除handler"""
        log_manager = LogManager(self.app_name)
        logger = log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 添加一个由LogManager管理的handler
        test_log_file = os.path.join(self.temp_dir, "remove_test.log")
        log_manager.add_file_handler(test_log_file, level="DEBUG")

        # 获取handler并移除
        handler = log_manager._handlers[0]
        log_manager.remove_handler(handler)
        self.assertNotIn(handler, logger.handlers)
        self.assertNotIn(handler, log_manager._handlers)

    def test_log_manager_cleanup(self):
        """测试LogManager清理资源"""
        log_manager = LogManager(self.app_name)
        log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 添加多个handler以便清理
        test_log_file1 = os.path.join(self.temp_dir, "cleanup_test1.log")
        test_log_file2 = os.path.join(self.temp_dir, "cleanup_test2.log")
        log_manager.add_file_handler(test_log_file1, level="DEBUG")
        log_manager.add_file_handler(test_log_file2, level="INFO")

        self.assertEqual(len(log_manager._handlers), 2)

        # 清理
        log_manager.cleanup()

        # 验证所有handler都被清理
        self.assertEqual(len(log_manager._handlers), 0)

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

    def test_setup_logging_without_separate_error_log(self):
        """测试不启用单独错误日志且级别高于ERROR的情况"""
        # 测试不启用单独错误日志且级别为WARNING（高于ERROR）
        logger = setup_logging(
            app_name=self.app_name + "_no_error_log",
            level="WARNING",
            log_to_console=False,
            log_to_file=True,
            log_dir=self.temp_dir,
            separate_error_log=False,
        )
        self.assertEqual(logger.level, logging.WARNING)

    def test_log_manager_add_file_handler_without_level(self):
        """测试LogManager添加文件handler时不指定级别"""
        log_manager = LogManager(self.app_name)
        log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 添加文件handler但不指定级别
        test_log_file = os.path.join(self.temp_dir, "test_no_level.log")
        log_manager.add_file_handler(test_log_file, level=None)

        # 验证文件创建
        self.assertTrue(os.path.exists(test_log_file))

    def test_log_manager_remove_unmanaged_handler(self):
        """测试LogManager移除未管理的handler"""
        log_manager = LogManager(self.app_name)
        logger = log_manager.setup(level="INFO", log_to_file=False, log_to_console=True)

        # 创建一个未由LogManager管理的handler
        unmanaged_handler = logging.StreamHandler()
        logger.addHandler(unmanaged_handler)

        # 移除这个未管理的handler
        log_manager.remove_handler(unmanaged_handler)

        # 验证handler已被移除但没有记录移除日志（因为不在_handlers中）
        self.assertNotIn(unmanaged_handler, logger.handlers)


if __name__ == "__main__":
    unittest.main()
