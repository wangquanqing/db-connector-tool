"""
数据库连接器日志配置模块

提供统一的日志配置和管理功能， 支持多级别日志输出、文件轮转、格式化等高级特性。
本模块封装了Python标准库logging模块， 提供更友好的API和线程安全的操作。

主要功能：
- setup_logging: 快速配置日志系统
- get_logger: 获取指定名称的logger
- set_log_level: 动态调整日志级别
- LogManager: 高级日志管理类，支持handler管理和线程安全操作
"""

import logging
import logging.handlers
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List

from .path_utils import PathHelper

# 默认日志格式 - 包含时间、模块名、级别、消息和源码位置
DEFAULT_LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
    + "[%(filename)s:%(lineno)d]"
)

# 支持的日志级别映射
VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def setup_logging(
    app_name: str = "db_connector",
    level: str = "INFO",
    log_to_console: bool = False,
    log_to_file: bool = True,
    max_file_size: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5,
    log_format: str | None = None,
    log_dir: str | None = None,
) -> logging.Logger:
    """
    配置并初始化应用程序的日志系统

    提供灵活的日志配置选项，支持控制台和文件输出，自动创建日志目录，
    并确保线程安全的日志操作。

    Args:
        app_name (str): 应用名称，用于创建日志目录和logger名称，默认"db_connector"
        level (str): 日志级别，可选值：DEBUG, INFO, WARNING, ERROR, CRITICAL，默认"INFO"
        log_to_console (bool): 是否输出到控制台，默认True
        log_to_file (bool): 是否输出到文件，默认True
        max_file_size (int): 单个日志文件最大大小（字节），默认10MB
        backup_count (int): 保留的备份日志文件数量，默认5个
        log_format (str | None): 自定义日志格式字符串，如果为None则使用默认格式
        log_dir (str | None): 自定义日志目录，如果为None则使用默认配置目录

    Returns:
        logging.Logger: 配置好的logger实例，可直接用于日志记录

    Raises:
        ValueError: 当日志级别无效或未启用任何输出方式时
        OSError: 当无法创建日志目录或文件时
        PermissionError: 当没有权限访问日志目录时

    Example:
        >>> # 基本用法
        >>> logger = setup_logging("my_app", "DEBUG")
        >>> logger.info("应用程序启动成功")

        >>> # 高级配置
        >>> logger = setup_logging(
        ...     app_name="my_app",
        ...     level="DEBUG",
        ...     log_to_file=True,
        ...     max_file_size=5*1024*1024,  # 5MB
        ...     backup_count=10
        ... )
    """
    # 验证日志级别
    log_level = _validate_log_level(level)

    # 获取日志目录路径
    if log_dir is None:
        base_dir = PathHelper.get_user_config_dir(app_name)
        log_dir_path = base_dir / "logs"
    else:
        log_dir_path = Path(log_dir)

    # 确保日志目录存在
    try:
        log_dir_path.mkdir(parents=True, exist_ok=True)
    except PermissionError as e:
        raise PermissionError(f"没有权限创建日志目录 {log_dir_path}: {str(e)}")
    except OSError as e:
        raise OSError(f"无法创建日志目录 {log_dir_path}: {str(e)}")

    log_file = log_dir_path / f"{app_name}.log"
    log_file_exists = os.path.exists(log_file)

    # 设置日志格式
    format_to_use = log_format if log_format is not None else DEFAULT_LOG_FORMAT
    formatter = logging.Formatter(format_to_use)

    # 获取应用专用logger（避免使用root logger）
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)

    # 清除已有的handler，避免重复配置导致的重复日志
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    handlers_added = 0

    # 配置文件handler（滚动日志）
    if log_to_file:
        try:
            file_handler = logging.handlers.RotatingFileHandler(
                filename=str(log_file),
                maxBytes=max_file_size,
                backupCount=backup_count,
                encoding="utf-8",
            )
            file_handler.setFormatter(formatter)
            file_handler.setLevel(log_level)
            logger.addHandler(file_handler)
            handlers_added += 1
        except PermissionError as e:
            raise PermissionError(f"没有权限写入日志文件 {log_file}: {str(e)}")
        except OSError as e:
            raise OSError(f"无法创建日志文件 {log_file}: {str(e)}")

    # 配置控制台handler
    if log_to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        logger.addHandler(console_handler)
        handlers_added += 1

    # 验证至少配置了一个输出handler
    if handlers_added == 0:
        raise ValueError("至少需要启用一种日志输出方式（控制台或文件）")

    # 只在文件不存在时才打印初始化信息，避免重复日志
    if not log_file_exists:
        logger.info(
            f"日志系统初始化完成 - 应用: {app_name}, "
            f"级别: {level.upper()}, 日志文件: {log_file}"
        )

    return logger


def _validate_log_level(level: str) -> int:
    """
    验证并转换日志级别字符串为对应的logging常量

    Args:
        level (str): 日志级别字符串（不区分大小写）

    Returns:
        int: 对应的logging级别常量

    Raises:
        ValueError: 当日志级别无效时抛出

    Example:
        >>> _validate_log_level("debug")  # 返回 logging.DEBUG
        >>> _validate_log_level("invalid")  # 抛出 ValueError
    """
    level_upper = level.upper()
    if level_upper not in VALID_LOG_LEVELS:
        raise ValueError(f"无效的日志级别: '{level}'，有效值为: {VALID_LOG_LEVELS}")
    return LOG_LEVEL_MAP[level_upper]


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的logger实例

    如果日志系统尚未配置，会返回一个基本的logger实例。
    建议在模块级别使用此函数获取logger。

    Args:
        name (str): logger名称，通常使用模块名（如：__name__）

    Returns:
        logging.Logger: logger实例

    Note:
        返回的logger如果没有配置handler，会继承root logger的配置

    Example:
        >>> # 在模块中使用
        >>> logger = get_logger(__name__)
        >>> logger.info("模块初始化完成")

        >>> # 在类中使用
        >>> class MyClass:
        ...     def __init__(self):
        ...         self.logger = get_logger(f"{__name__}.MyClass")
    """
    return logging.getLogger(name)


def set_log_level(logger_name: str, level: str) -> None:
    """
    动态设置指定logger的日志级别

    同时更新logger和所有关联handler的级别，确保立即生效。

    Args:
        logger_name (str): logger名称
        level (str): 新的日志级别字符串

    Raises:
        ValueError: 当日志级别无效时

    Example:
        >>> # 设置特定模块的日志级别
        >>> set_log_level("db_connector.core", "DEBUG")

        >>> # 设置整个应用的日志级别
        >>> set_log_level("db_connector", "WARNING")
    """
    log_level = _validate_log_level(level)
    logger = get_logger(logger_name)
    logger.setLevel(log_level)

    # 同时更新所有handler的级别，确保输出一致性
    for handler in logger.handlers:
        handler.setLevel(log_level)


class LogManager:
    """
    高级日志管理器类

    提供线程安全的日志管理功能，包括动态handler管理、级别调整、
    多输出配置等高级特性。适用于需要精细控制日志行为的复杂应用。

    Attributes:
        app_name (str): 应用名称，用于标识日志配置
        logger (logging.Logger): LogManager自身的logger，用于记录管理操作
        _handlers (List[logging.Handler]): 管理的handler列表
        _lock (threading.Lock): 线程锁，确保线程安全操作

    Example:
        >>> # 创建日志管理器
        >>> log_manager = LogManager("my_app")
        >>> logger = log_manager.setup(level="DEBUG")

        >>> # 添加额外handler
        >>> log_manager.add_file_handler("debug.log", level="DEBUG")

        >>> # 清理资源
        >>> log_manager.cleanup()
    """

    def __init__(self, app_name: str = "db_connector") -> None:
        """
        初始化日志管理器实例

        Args:
            app_name (str): 应用名称，用于标识不同的日志配置，默认"db_connector"
        """
        self.app_name = app_name
        # 创建LogManager自身的logger，用于记录管理操作
        self.logger = get_logger(f"{__name__}.LogManager")
        self._handlers: List[logging.Handler] = []
        self._lock = threading.Lock()

    def setup(self, **kwargs: Any) -> logging.Logger:
        """
        设置并配置日志系统

        封装setup_logging函数，提供更简洁的API，并记录配置操作。

        Args:
            **kwargs: 传递给setup_logging的参数

        Returns:
            logging.Logger: 配置好的logger实例

        Example:
            >>> log_manager = LogManager("my_app")
            >>> logger = log_manager.setup(level="DEBUG", log_to_file=True)
        """
        logger = setup_logging(self.app_name, **kwargs)
        self.logger.info(f"LogManager为应用 '{self.app_name}' 配置了日志系统")
        return logger

    def add_file_handler(
        self,
        log_file: str,
        max_size: int = 10 * 1024 * 1024,
        backup_count: int = 5,
        level: str | None = None,
        when: str | None = None,
    ) -> None:
        """
        添加额外的文件handler到应用logger

        支持基于时间或大小的日志轮转策略，提供灵活的日志输出配置。

        Args:
            log_file (str): 日志文件路径
            max_size (int): 最大文件大小（字节），默认10MB
            backup_count (int): 备份文件数量，默认5个
            level (str | None): 可选的日志级别，如果为None则使用logger的级别
            when (str | None): 时间轮转规则，如 'midnight', 'H', 'D' 等

        Raises:
            ValueError: 当日志级别无效时
            OSError: 当无法创建日志文件时
            PermissionError: 当没有文件写入权限时

        Note:
            - 如果指定when参数，使用TimedRotatingFileHandler
            - 否则使用RotatingFileHandler（基于大小轮转）

        Example:
            >>> # 添加调试日志文件
            >>> log_manager.add_file_handler("debug.log", level="DEBUG")

            >>> # 添加每小时轮转的日志
            >>> log_manager.add_file_handler("hourly.log", when="H")
        """
        with self._lock:
            formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

            # 根据轮转策略选择handler类型
            if when is not None:
                file_handler = logging.handlers.TimedRotatingFileHandler(
                    filename=log_file,
                    when=when,
                    backupCount=backup_count,
                    encoding="utf-8",
                )
            else:
                file_handler = logging.handlers.RotatingFileHandler(
                    filename=log_file,
                    maxBytes=max_size,
                    backupCount=backup_count,
                    encoding="utf-8",
                )

            file_handler.setFormatter(formatter)

            # 设置handler级别（如果指定）
            if level is not None:
                handler_level = _validate_log_level(level)
                file_handler.setLevel(handler_level)

            # 添加到应用logger和管理列表
            logger = logging.getLogger(self.app_name)
            logger.addHandler(file_handler)
            self._handlers.append(file_handler)

            self.logger.info(f"添加文件handler: {log_file}")

    def remove_handler(self, handler: logging.Handler) -> None:
        """
        移除指定的handler并清理资源

        线程安全地移除handler，确保不会影响正在进行的日志操作。

        Args:
            handler (logging.Handler): 要移除的handler实例

        Example:
            >>> # 移除特定的handler
            >>> handler = logger.handlers[0]
            >>> log_manager.remove_handler(handler)
        """
        with self._lock:
            logger = logging.getLogger(self.app_name)
            logger.removeHandler(handler)
            handler.close()

            if handler in self._handlers:
                self._handlers.remove(handler)
                self.logger.info(f"已移除handler: {handler}")

    def cleanup(self) -> None:
        """
        清理所有由LogManager创建的handler

        线程安全地清理所有管理的handler，释放资源。
        通常在应用程序退出时调用。

        Example:
            >>> # 应用程序退出前清理
            >>> log_manager.cleanup()
        """
        with self._lock:
            logger = logging.getLogger(self.app_name)
            handler_count = len(self._handlers)

            # 使用副本遍历避免迭代时修改列表
            for handler in self._handlers[:]:
                logger.removeHandler(handler)
                handler.close()
                self._handlers.remove(handler)

            self.logger.info(f"已清理所有handler，共清理 {handler_count} 个")

    def get_loggers_info(self) -> Dict[str, Dict[str, Any]]:
        """
        获取系统中所有logger的配置信息

        返回详细的logger配置信息，用于调试和监控。

        Returns:
            Dict[str, Dict[str, Any]]:
                - 键: logger名称
                - 值: 包含级别、handler数量、传播设置的信息字典

        Example:
            >>> info = log_manager.get_loggers_info()
            >>> print(info["db_connector.core"])
            {'level': 'DEBUG', 'handlers': 2, 'propagate': True}
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

    @staticmethod
    def quick_setup(
        app_name: str = "db_connector", level: str = "INFO"
    ) -> "LogManager":
        """
        快速创建并配置LogManager实例

        提供便捷的静态方法，简化日志管理器的创建和配置过程。

        Args:
            app_name (str): 应用名称，默认"db_connector"
            level (str): 日志级别，默认"INFO"

        Returns:
            LogManager: 配置好的LogManager实例

        Example:
            >>> # 快速配置
            >>> log_manager = LogManager.quick_setup("my_app", "DEBUG")
        """
        log_manager = LogManager(app_name)
        log_manager.setup(level=level)
        return log_manager
