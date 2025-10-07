"""
日志配置模块

提供统一的日志配置和管理功能， 支持文件日志和控制台日志输出。
包含日志级别配置、日志文件轮转、格式化等功能。
"""

import logging
import logging.handlers
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List, Optional

from .path_utils import PathHelper

# 默认日志格式
DEFAULT_LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
    + "[%(filename)s:%(lineno)d]"
)


def setup_logging(
    app_name: str = "db_connector",
    level: str = "INFO",
    log_to_console: bool = True,
    log_to_file: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    log_format: Optional[str] = None,
    log_dir: Optional[str] = None,
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
        log_dir: 自定义日志目录，如果为None则使用默认配置目录

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
    if log_dir is None:
        base_dir = PathHelper.get_user_config_dir(app_name)
        log_dir_path = base_dir / "logs"
    else:
        log_dir_path = Path(log_dir)

    # 确保日志目录存在
    try:
        log_dir_path.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"无法创建日志目录 {log_dir_path}: {str(e)}")

    log_file = log_dir_path / f"{app_name}.log"

    # 设置日志格式
    if log_format is None:
        log_format = DEFAULT_LOG_FORMAT

    formatter = logging.Formatter(log_format)

    # 获取应用专用logger而非root logger
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)

    # 清除已有的handler，避免重复配置
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    handlers_added = 0

    # 文件handler - 滚动日志
    if log_to_file:
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                str(log_file),
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            logger.addHandler(file_handler)
            handlers_added += 1
        except OSError as e:
            raise OSError(f"无法创建日志文件 {log_file}: {str(e)}")

    # 控制台handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        logger.addHandler(console_handler)
        handlers_added += 1

    if handlers_added == 0:
        raise ValueError("至少需要启用一种日志输出方式（控制台或文件）")

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

    # 注意：不修改handler级别，因为handler级别通常应该独立于logger级别


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
        self._handlers: List[logging.Handler] = []
        self._lock = threading.Lock()

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
        self,
        log_file: str,
        max_size: int = 10 * 1024 * 1024,
        backup_count: int = 5,
        level: Optional[str] = None,
        when: Optional[str] = None,
    ) -> None:
        """
        添加额外的文件handler

        Args:
            log_file: 日志文件路径
            max_size: 最大文件大小
            backup_count: 备份文件数量
            level: 可选的日志级别，如果为None则使用root logger的级别
            when: 时间轮转规则（如 'midnight', 'H', 'D' 等），如果指定则使用时间轮转
        """
        with self._lock:
            formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

            if when is not None:
                # 使用时间轮转
                file_handler = logging.handlers.TimedRotatingFileHandler(
                    log_file, when=when, backupCount=backup_count, encoding="utf-8"
                )
            else:
                # 使用大小轮转
                file_handler = logging.handlers.RotatingFileHandler(
                    log_file,
                    maxBytes=max_size,
                    backupCount=backup_count,
                    encoding="utf-8",
                )

            file_handler.setFormatter(formatter)

            if level is not None:
                valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
                level_upper = level.upper()
                if level_upper not in valid_levels:
                    raise ValueError(f"无效的日志级别: {level}")
                file_handler.setLevel(getattr(logging, level_upper))

            logger = logging.getLogger(self.app_name)
            logger.addHandler(file_handler)
            self._handlers.append(file_handler)

            self.logger.info(f"添加文件handler: {log_file}")

    def remove_handler(self, handler: logging.Handler) -> None:
        """
        移除指定的handler

        Args:
            handler: 要移除的handler实例
        """
        with self._lock:
            logger = logging.getLogger(self.app_name)
            logger.removeHandler(handler)
            handler.close()
            if handler in self._handlers:
                self._handlers.remove(handler)

    def cleanup(self) -> None:
        """
        清理所有由LogManager创建的handler
        """
        with self._lock:
            logger = logging.getLogger(self.app_name)
            for handler in self._handlers[:]:
                logger.removeHandler(handler)
                handler.close()
                self._handlers.remove(handler)

    def get_loggers_info(self) -> Dict[str, Dict[str, Any]]:
        """
        获取所有logger的信息

        Returns:
            dict: logger信息字典
        """
        with self._lock:
            loggers_info: Dict[str, Dict[str, Any]] = {}
            manager = logging.getLogger().manager
            for name in manager.loggerDict:
                logger = logging.getLogger(name)
                loggers_info[name] = {
                    "level": logging.getLevelName(logger.level),
                    "handlers": len(logger.handlers),
                    "propagate": logger.propagate,
                }
            return loggers_info
