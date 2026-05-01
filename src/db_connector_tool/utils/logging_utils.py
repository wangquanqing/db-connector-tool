"""日志配置管理模块 (Logging Utilities)

提供应用日志系统的配置和管理功能，支持控制台/文件输出、
日志轮转、多级别过滤和动态级别调整。

Example:
>>> from db_connector_tool import setup_logging, get_logger
>>>
>>> logger = setup_logging(app_name="my_app", level="DEBUG")
>>> logger.info("应用程序启动成功")
>>>
>>> logger = setup_logging(
...     app_name="my_app",
...     level="DEBUG",
...     log_to_file=True,
...     max_file_size=5 * 1024 * 1024,
...     backup_count=10,
... )
>>>
>>> module_logger = get_logger(__name__)
>>> module_logger.debug("模块初始化完成")
"""

import logging
import logging.handlers
import os
import sys
import threading
from pathlib import Path
from typing import Any, Dict, List

from .path_utils import PathHelper

DEFAULT_LOG_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s "
    + "[%(filename)s:%(lineno)d]"
)

VALID_LOG_LEVELS = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


def setup_logging(**kwargs) -> logging.Logger:
    """配置并初始化应用程序的日志系统

    Args:
        app_name: 应用名称
        level: 日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL），默认 INFO
        log_to_console: 是否输出到控制台，默认 False
        log_to_file: 是否输出到文件，默认 True
        max_file_size: 单个日志文件最大字节数，默认 10MB
        backup_count: 保留的备份日志文件数量，默认 5
        log_format: 自定义格式字符串，None 使用默认
        log_dir: 自定义日志目录，None 使用默认配置目录
        separate_error_log: 是否分离错误日志，默认 True

    Returns:
        logging.Logger: 配置好的 logger 实例

    Raises:
        ValueError: 日志级别无效或未启用输出方式
        OSError: 无法创建日志目录
        PermissionError: 无权限访问日志目录

    Example:
    >>> logger = setup_logging(app_name="my_app", level="DEBUG")
    >>> logger.info("应用程序启动成功")
    """
    default_config = {
        "app_name": "",
        "level": "INFO",
        "log_to_console": False,
        "log_to_file": True,
        "max_file_size": 10 * 1024 * 1024,
        "backup_count": 5,
        "log_format": None,
        "log_dir": None,
        "separate_error_log": True,
    }

    config = {**default_config, **kwargs}

    return _setup_logging_from_config(config)


def _setup_logging_from_config(config: dict) -> logging.Logger:
    """从配置字典初始化日志系统

    Args:
        config: 日志配置字典

    Returns:
        logging.Logger: 配置好的 logger 实例
    """
    log_level = _validate_log_level(config["level"])

    log_dir_path = _get_log_dir_path(config["app_name"], config["log_dir"])
    PathHelper.ensure_dir_exists(log_dir_path)

    log_file = log_dir_path / f"{config['app_name']}.log"
    log_file_exists = os.path.exists(log_file)

    formatter = _create_formatter(config["log_format"])
    logger = _setup_logger(config["app_name"], log_level)

    handler_config = {
        "logger": logger,
        "formatter": formatter,
        "log_to_file": config["log_to_file"],
        "log_to_console": config["log_to_console"],
        "log_file": log_file,
        "log_dir_path": log_dir_path,
        "app_name": config["app_name"],
        "log_level": log_level,
        "separate_error_log": config["separate_error_log"],
        "max_file_size": config["max_file_size"],
        "backup_count": config["backup_count"],
    }

    handlers_added = _configure_handlers_from_config(handler_config)

    if handlers_added == 0:
        raise ValueError("至少需要启用一种日志输出方式（控制台或文件）")
    if not log_file_exists:
        logger.info(
            "日志系统初始化完成 - 应用: %s, 级别: %s, 日志文件: %s",
            config["app_name"],
            config["level"].upper(),
            log_file,
        )

    return logger


def _validate_log_level(level: str) -> int:
    """验证并转换日志级别

    Args:
        level: 日志级别字符串（不区分大小写）

    Returns:
        int: logging 级别常量

    Raises:
        ValueError: 日志级别无效

    Example:
    >>> _validate_log_level("debug")  # 返回 logging.DEBUG
    """
    level_upper = level.upper()
    if level_upper not in VALID_LOG_LEVELS:
        raise ValueError(f"无效的日志级别: '{level}'，有效值为: {VALID_LOG_LEVELS}")
    return LOG_LEVEL_MAP[level_upper]


def _get_log_dir_path(app_name: str, log_dir: str | None) -> Path:
    """获取日志目录路径

    Args:
        app_name: 应用名称
        log_dir: 自定义目录，None 使用默认配置目录

    Returns:
        Path: 日志目录路径
    """
    if log_dir is None:
        base_dir = PathHelper.get_user_config_dir(app_name)
        return base_dir / "logs"
    return Path(log_dir)


def _create_formatter(log_format: str | None) -> logging.Formatter:
    """创建日志格式化器

    Args:
        log_format: 自定义格式，None 使用默认

    Returns:
        logging.Formatter: 格式化器实例
    """
    format_to_use = log_format if log_format is not None else DEFAULT_LOG_FORMAT
    return logging.Formatter(format_to_use)


def _setup_logger(app_name: str, log_level: int) -> logging.Logger:
    """初始化 logger 并清除已有 handlers

    Args:
        app_name: 应用名称
        log_level: 日志级别常量

    Returns:
        logging.Logger: 配置好的 logger 实例
    """
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)

    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    return logger


def _configure_handlers_from_config(config: dict) -> int:
    """按配置注册所有 handlers

    Args:
        config: handler 配置字典

    Returns:
        int: 添加的 handler 数量
    """
    logger = config["logger"]
    formatter = config["formatter"]
    log_to_file = config["log_to_file"]
    log_to_console = config["log_to_console"]
    log_file = config["log_file"]
    log_dir_path = config["log_dir_path"]
    app_name = config["app_name"]
    log_level = config["log_level"]
    separate_error_log = config["separate_error_log"]
    max_file_size = config["max_file_size"]
    backup_count = config["backup_count"]

    handlers_added = 0

    if log_to_file:
        file_handler_config = {
            "logger": logger,
            "formatter": formatter,
            "log_file": log_file,
            "log_dir_path": log_dir_path,
            "app_name": app_name,
            "log_level": log_level,
            "separate_error_log": separate_error_log,
            "max_file_size": max_file_size,
            "backup_count": backup_count,
        }
        handlers_added += _configure_file_handlers_from_config(file_handler_config)

    if log_to_console:
        handlers_added += _configure_console_handler(logger, formatter, log_level)

    return handlers_added


def _configure_file_handlers_from_config(config: dict) -> int:
    """按配置文件 handlers

    Args:
        config: file handler 配置字典

    Returns:
        int: 添加的 handler 数量

    Raises:
        PermissionError: 无权限写入日志文件
        OSError: 无法创建日志文件
    """
    logger = config["logger"]
    formatter = config["formatter"]
    log_file = config["log_file"]
    log_dir_path = config["log_dir_path"]
    app_name = config["app_name"]
    log_level = config["log_level"]
    separate_error_log = config["separate_error_log"]
    max_file_size = config["max_file_size"]
    backup_count = config["backup_count"]

    try:
        handlers_added = 0

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

        if separate_error_log and log_level <= logging.ERROR:
            error_handler = _create_error_file_handler(
                log_dir_path=log_dir_path,
                app_name=app_name,
                formatter=formatter,
                max_file_size=max_file_size,
                backup_count=backup_count,
            )
            logger.addHandler(error_handler)
            handlers_added += 1

        return handlers_added

    except PermissionError as error:
        raise PermissionError(f"没有权限写入日志文件 {log_file}: {error}") from error
    except OSError as error:
        raise OSError(f"无法创建日志文件 {log_file}: {error}") from error


def _create_error_file_handler(**kwargs) -> logging.Handler:
    """创建错误日志文件 handler

    Args:
        log_dir_path: 日志目录路径
        app_name: 应用名称
        formatter: 格式化器
        max_file_size: 最大文件大小
        backup_count: 备份数量

    Returns:
        logging.Handler: 错误日志 handler 实例
    """
    log_dir_path = kwargs["log_dir_path"]
    app_name = kwargs["app_name"]
    formatter = kwargs["formatter"]
    max_file_size = kwargs["max_file_size"]
    backup_count = kwargs["backup_count"]

    error_log_file = log_dir_path / f"{app_name}_error.log"
    error_handler = logging.handlers.RotatingFileHandler(
        filename=str(error_log_file),
        maxBytes=max_file_size,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_handler.setFormatter(formatter)
    error_handler.setLevel(logging.ERROR)
    return error_handler


def _configure_console_handler(
    logger: logging.Logger, formatter: logging.Formatter, log_level: int
) -> int:
    """配置控制台 handler

    Args:
        logger: logger 实例
        formatter: 格式化器
        log_level: 日志级别常量

    Returns:
        int: 添加的 handler 数量（始终为 1）
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)
    return 1


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的 logger 实例

    Args:
        name: logger 名称（通常使用 __name__）

    Returns:
        logging.Logger: logger 实例

    Example:
    >>> logger = get_logger(__name__)
    >>> logger.info("模块初始化完成")
    """
    return logging.getLogger(name)


def set_log_level(logger_name: str, level: str) -> None:
    """动态设置指定 logger 的日志级别

    Args:
        logger_name: logger 名称
        level: 日志级别字符串

    Raises:
        ValueError: 日志级别无效

    Example:
    >>> set_log_level("db_connector_tool.core", "DEBUG")
    """
    log_level = _validate_log_level(level)
    logger = get_logger(logger_name)
    logger.setLevel(log_level)

    for handler in logger.handlers:
        handler.setLevel(log_level)


class LogManager:
    """高级日志管理器类 (Log Manager)

    提供日志系统的面向对象管理接口，支持动态添加/移除 handler、
    资源清理和日志状态查询。

    Example:
    >>> log_manager = LogManager("my_app")
    >>> logger = log_manager.setup(level="DEBUG")
    >>> log_manager.add_file_handler("debug.log", level="DEBUG")
    >>> log_manager.cleanup()
    """

    def __init__(self, app_name: str) -> None:
        """初始化日志管理器

        Args:
            app_name: 应用名称

        Example:
        >>> log_manager = LogManager("my_app")
        """
        self.app_name = app_name
        self.logger = get_logger(f"{__name__}.LogManager")
        self._handlers: List[logging.Handler] = []
        self._lock = threading.Lock()

    def setup(self, **kwargs: Any) -> logging.Logger:
        """配置日志系统

        Args:
            **kwargs: 传递给 setup_logging 的参数

        Returns:
            logging.Logger: 配置好的 logger 实例

        Example:
        >>> log_manager = LogManager("my_app")
        >>> logger = log_manager.setup(level="DEBUG", log_to_file=True)
        """
        logger = setup_logging(app_name=self.app_name, **kwargs)
        self.logger.info("LogManager为应用 '%s' 配置了日志系统", self.app_name)
        return logger

    def add_file_handler(self, log_file: str, **kwargs) -> None:
        """添加额外的文件 handler

        Args:
            log_file: 日志文件路径
            max_size: 最大文件大小（字节），默认 10MB
            backup_count: 备份数量，默认 5
            level: 日志级别（可选）
            when: 时间轮转规则（'midnight', 'H', 'D' 等）

        Raises:
            ValueError: 日志级别无效
            OSError: 无法创建日志文件
            PermissionError: 无权限

        Example:
        >>> log_manager.add_file_handler("debug.log", level="DEBUG")
        >>> log_manager.add_file_handler("hourly.log", when="H")
        """
        max_size = kwargs.get("max_size", 10 * 1024 * 1024)
        backup_count = kwargs.get("backup_count", 5)
        level = kwargs.get("level")
        when = kwargs.get("when")

        with self._lock:
            formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

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

            if level is not None:
                handler_level = _validate_log_level(level)
                file_handler.setLevel(handler_level)

            logger = logging.getLogger(self.app_name)
            logger.addHandler(file_handler)
            self._handlers.append(file_handler)

            self.logger.info("添加文件handler: %s", log_file)

    def remove_handler(self, handler: logging.Handler) -> None:
        """移除指定的 handler

        Args:
            handler: 要移除的 handler 实例

        Example:
        >>> handler = logger.handlers[0]
        >>> log_manager.remove_handler(handler)
        """
        with self._lock:
            logger = logging.getLogger(self.app_name)
            logger.removeHandler(handler)
            handler.close()

            if handler in self._handlers:
                self._handlers.remove(handler)
                self.logger.info("已移除handler: %s", handler)

    def cleanup(self) -> None:
        """清理所有 LogManager 创建的 handler

        Example:
        >>> log_manager.cleanup()
        """
        with self._lock:
            logger = logging.getLogger(self.app_name)
            handler_count = len(self._handlers)

            for handler in self._handlers[:]:
                logger.removeHandler(handler)
                handler.close()
                self._handlers.remove(handler)

            self.logger.info("已清理所有handler，共清理 %s 个", handler_count)

    def get_loggers_info(self) -> Dict[str, Dict[str, Any]]:
        """获取系统中所有 logger 的配置信息

        Returns:
            Dict[str, Dict[str, Any]]: logger 名 → {level, handlers, propagate}

        Example:
        >>> info = log_manager.get_loggers_info()
        >>> print(info["db_connector_tool.core"])
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
    def quick_setup(app_name: str, level: str = "INFO") -> "LogManager":
        """快速创建并配置 LogManager 实例

        Args:
            app_name: 应用名称
            level: 日志级别，默认 INFO

        Returns:
            LogManager: 配置好的 LogManager 实例

        Example:
        >>> log_manager = LogManager.quick_setup("my_app", "DEBUG")
        """
        log_manager = LogManager(app_name)
        log_manager.setup(level=level)
        return log_manager
