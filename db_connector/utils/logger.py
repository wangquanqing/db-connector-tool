"""
日志配置模块

提供统一的日志配置和管理功能， 支持文件日志和控制台日志输出。
包含日志级别配置、日志文件轮转、格式化等功能。
"""

import logging
import logging.handlers
import sys
from typing import Optional

from .path_helper import PathHelper


def setup_logging(
    app_name: str = "db_connector",
    level: str = "INFO",
    log_to_console: bool = True,
    log_to_file: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    log_format: Optional[str] = None,
) -> logging.Logger:
    """
    配置日志系统

    初始化并配置应用程序的日志系统，支持控制台和文件日志输出。

    Args:
        app_name: 应用名称，用于创建日志目录
        level: 日志级别，可选值：DEBUG, INFO, WARNING, ERROR, CRITICAL
        log_to_console: 是否输出到控制台
        log_to_file: 是否输出到文件
        max_file_size: 单个日志文件最大大小（字节）
        backup_count: 保留的备份日志文件数量
        log_format: 自定义日志格式字符串，如果为None则使用默认格式

    Returns:
        logging.Logger: 配置好的logger实例

    Raises:
        ValueError: 当日志级别无效时
        OSError: 当无法创建日志目录或文件时

    Example:
        >>> logger = setup_logging("my_app", "DEBUG")
        >>> logger.info("应用程序启动")
    """
    # 验证日志级别
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    level_upper = level.upper()
    if level_upper not in valid_levels:
        raise ValueError(f"无效的日志级别: {level}，有效值为: {valid_levels}")

    log_level = getattr(logging, level_upper)

    # 获取日志目录
    log_dir = PathHelper.get_user_config_dir(app_name) / "logs"
    if not PathHelper.ensure_dir_exists(log_dir):
        raise OSError(f"无法创建日志目录: {log_dir}")

    log_file = log_dir / f"{app_name}.log"

    # 设置日志格式
    if log_format is None:
        log_format = (
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
            "[%(filename)s:%(lineno)d]"
        )

    formatter = logging.Formatter(log_format)

    # 配置root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # 清除已有的handler，避免重复配置
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    handlers_added = 0

    # 文件handler - 滚动日志
    if log_to_file:
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                log_file,
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            root_logger.addHandler(file_handler)
            handlers_added += 1
        except OSError as e:
            raise OSError(f"无法创建日志文件 {log_file}: {str(e)}")

    # 控制台handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        root_logger.addHandler(console_handler)
        handlers_added += 1

    if handlers_added == 0:
        raise ValueError("至少需要启用一种日志输出方式（控制台或文件）")

    logger = logging.getLogger(__name__)
    logger.info(f"日志系统初始化完成 - 级别: {level_upper}, 日志文件: {log_file}")

    return logger


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的logger

    如果日志系统尚未配置，会返回一个基本的logger实例。

    Args:
        name: logger名称，通常使用模块名（如：__name__）

    Returns:
        logging.Logger: logger实例

    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("模块初始化完成")
    """
    return logging.getLogger(name)


def set_log_level(logger_name: str, level: str) -> None:
    """
    动态设置指定logger的日志级别

    Args:
        logger_name: logger名称
        level: 新的日志级别

    Raises:
        ValueError: 当日志级别无效时

    Example:
        >>> set_log_level("db_connector.core", "DEBUG")
    """
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    level_upper = level.upper()
    if level_upper not in valid_levels:
        raise ValueError(f"无效的日志级别: {level}，有效值为: {valid_levels}")

    logger = logging.getLogger(logger_name)
    logger.setLevel(getattr(logging, level_upper))

    # 同时更新所有handler的级别
    for handler in logger.handlers:
        handler.setLevel(getattr(logging, level_upper))


class LogManager:
    """
    日志管理器类

    提供更高级的日志管理功能，包括日志级别动态调整、handler管理等。
    """

    def __init__(self, app_name: str = "db_connector"):
        """
        初始化日志管理器

        Args:
            app_name: 应用名称
        """
        self.app_name = app_name
        self.logger = get_logger(f"{__name__}.LogManager")

    def setup(self, **kwargs) -> logging.Logger:
        """
        设置日志系统（封装setup_logging函数）

        Args:
            **kwargs: 传递给setup_logging的参数

        Returns:
            logging.Logger: 配置好的logger实例
        """
        return setup_logging(self.app_name, **kwargs)

    def add_file_handler(
        self, log_file: str, max_size: int = 10 * 1024 * 1024, backup_count: int = 5
    ) -> None:
        """
        添加额外的文件handler

        Args:
            log_file: 日志文件路径
            max_size: 最大文件大小
            backup_count: 备份文件数量
        """
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_size, backupCount=backup_count, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)

        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)

        self.logger.info(f"添加文件handler: {log_file}")

    def get_loggers_info(self) -> dict:
        """
        获取所有logger的信息

        Returns:
            dict: logger信息字典
        """
        loggers_info = {}
        manager = logging.getLogger().manager
        for name in manager.loggerDict:
            logger = logging.getLogger(name)
            loggers_info[name] = {
                "level": logging.getLevelName(logger.level),
                "handlers": len(logger.handlers),
                "propagate": logger.propagate,
            }
        return loggers_info
