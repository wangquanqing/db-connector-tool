"""日志配置管理模块 (Logging Utilities)

Example:
>>> from db_connector_tool.utils.logging_utils import setup_logging, get_logger
>>>
>>> # 基本配置
>>> logger = setup_logging("my_app", "DEBUG")
>>> logger.info("应用程序启动成功")
>>>
>>> # 高级配置
>>> logger = setup_logging(
...     app_name="my_app",
...     level="DEBUG",
...     log_to_file=True,
...     max_file_size=5*1024*1024,  # 5MB
...     backup_count=10
... )
>>>
>>> # 获取模块级logger
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


def setup_logging(**kwargs) -> logging.Logger:
    """配置并初始化应用程序的日志系统

    Args:
        app_name: 应用名称，用于创建日志目录和logger名称
        level: 日志级别，可选值：DEBUG, INFO, WARNING, ERROR, CRITICAL，默认"INFO"
        log_to_console: 是否输出到控制台，默认False
        log_to_file: 是否输出到文件，默认True
        max_file_size: 单个日志文件最大大小（字节），默认10MB
        backup_count: 保留的备份日志文件数量，默认5个
        log_format: 自定义日志格式字符串，如果为None则使用默认格式
        log_dir: 自定义日志目录，如果为None则使用默认配置目录
        separate_error_log: 是否分离错误日志，默认True

    Returns:
        logging.Logger: 配置好的logger实例，可直接用于日志记录

    Raises:
        ValueError: 当日志级别无效或未启用任何输出方式时
        OSError: 当无法创建日志目录或文件时
        PermissionError: 当没有权限访问日志目录时

    Example:
        >>> # 基本用法
        >>> logger = setup_logging(app_name="my_app", level="DEBUG")
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
    # 默认配置
    default_config = {
        "app_name": "",
        "level": "INFO",
        "log_to_console": False,
        "log_to_file": True,
        "max_file_size": 10 * 1024 * 1024,  # 10MB
        "backup_count": 5,
        "log_format": None,
        "log_dir": None,
        "separate_error_log": True,
    }

    # 合并配置
    config = {**default_config, **kwargs}

    return _setup_logging_from_config(config)


def _setup_logging_from_config(config: dict) -> logging.Logger:
    """从配置字典设置日志系统

    Args:
        config: 日志配置字典

    Returns:
        logging.Logger: 配置好的logger实例

    Example:
        >>> # 内部使用，由 setup_logging 函数调用
        >>> # 无需手动调用
    """
    # 验证日志级别
    log_level = _validate_log_level(config["level"])

    # 获取并创建日志目录
    log_dir_path = _get_log_dir_path(config["app_name"], config["log_dir"])
    PathHelper.ensure_dir_exists(log_dir_path)

    # 准备日志文件
    log_file = log_dir_path / f"{config['app_name']}.log"
    log_file_exists = os.path.exists(log_file)

    # 创建格式化器和logger
    formatter = _create_formatter(config["log_format"])
    logger = _setup_logger(config["app_name"], log_level)

    # 构建并配置handlers
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

    # 验证handlers并记录初始化信息
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
    """验证并转换日志级别字符串为对应的logging常量

    Args:
        level: 日志级别字符串（不区分大小写）

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


def _get_log_dir_path(app_name: str, log_dir: str | None) -> Path:
    """获取日志目录路径

    Args:
        app_name: 应用名称
        log_dir: 自定义日志目录，如果为None则使用默认配置目录

    Returns:
        Path: 日志目录路径

    Example:
        >>> # 内部使用，由 setup_logging 函数调用
        >>> # 无需手动调用
    """
    if log_dir is None:
        base_dir = PathHelper.get_user_config_dir(app_name)
        return base_dir / "logs"
    return Path(log_dir)


def _create_formatter(log_format: str | None) -> logging.Formatter:
    """创建日志格式化器

    Args:
        log_format: 自定义日志格式字符串，如果为None则使用默认格式

    Returns:
        logging.Formatter: 日志格式化器实例

    Example:
        >>> # 内部使用，由 setup_logging 函数调用
        >>> # 无需手动调用
    """
    format_to_use = log_format if log_format is not None else DEFAULT_LOG_FORMAT
    return logging.Formatter(format_to_use)


def _setup_logger(app_name: str, log_level: int) -> logging.Logger:
    """设置logger并清除已有handlers

    Args:
        app_name: 应用名称
        log_level: 日志级别常量

    Returns:
        logging.Logger: 配置好的logger实例

    Example:
        >>> # 内部使用，由 setup_logging 函数调用
        >>> # 无需手动调用
    """
    logger = logging.getLogger(app_name)
    logger.setLevel(log_level)

    # 清除已有的handler，避免重复配置导致的重复日志
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
        handler.close()

    return logger


def _configure_handlers_from_config(config: dict) -> int:
    """从配置字典配置所有handlers并返回添加的handler数量

    Args:
        config: handler配置字典

    Returns:
        int: 添加的handler数量

    Example:
        >>> # 内部使用，由 _setup_logging_from_config 函数调用
        >>> # 无需手动调用
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
        # 构建file handler配置字典
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
    """从配置字典配置文件handlers

    Args:
        config: file handler配置字典

    Returns:
        int: 添加的handler数量

    Raises:
        PermissionError: 当没有权限写入日志文件时
        OSError: 当无法创建日志文件时

    Example:
        >>> # 内部使用，由 _configure_handlers_from_config 函数调用
        >>> # 无需手动调用
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

        # 主日志文件handler
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

        # 错误日志文件handler
        if separate_error_log and log_level <= logging.ERROR:
            error_file_handler = _create_error_file_handler(
                log_dir_path=log_dir_path,
                app_name=app_name,
                formatter=formatter,
                max_file_size=max_file_size,
                backup_count=backup_count,
            )
            logger.addHandler(error_file_handler)
            handlers_added += 1

        return handlers_added

    except PermissionError as e:
        raise PermissionError(f"没有权限写入日志文件 {log_file}: {str(e)}") from e
    except OSError as e:
        raise OSError(f"无法创建日志文件 {log_file}: {str(e)}") from e


def _create_error_file_handler(**kwargs) -> logging.Handler:
    """创建错误日志文件handler

    Args:
        log_dir_path: 日志目录路径
        app_name: 应用名称
        formatter: 日志格式化器
        max_file_size: 单个日志文件最大大小
        backup_count: 保留的备份日志文件数量

    Returns:
        logging.Handler: 错误日志文件handler实例

    Example:
        >>> # 内部使用，由 _configure_file_handlers 函数调用
        >>> # 无需手动调用
    """
    log_dir_path = kwargs["log_dir_path"]
    app_name = kwargs["app_name"]
    formatter = kwargs["formatter"]
    max_file_size = kwargs["max_file_size"]
    backup_count = kwargs["backup_count"]

    error_log_file = log_dir_path / f"{app_name}_error.log"
    error_file_handler = logging.handlers.RotatingFileHandler(
        filename=str(error_log_file),
        maxBytes=max_file_size,
        backupCount=backup_count,
        encoding="utf-8",
    )
    error_file_handler.setFormatter(formatter)
    error_file_handler.setLevel(logging.ERROR)
    return error_file_handler


def _configure_console_handler(
    logger: logging.Logger, formatter: logging.Formatter, log_level: int
) -> int:
    """配置控制台handler

    Args:
        logger: logger实例
        formatter: 日志格式化器
        log_level: 日志级别常量

    Returns:
        int: 添加的handler数量（始终为1）

    Example:
        >>> # 内部使用，由 _configure_handlers 函数调用
        >>> # 无需手动调用
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger.addHandler(console_handler)
    return 1


def get_logger(name: str) -> logging.Logger:
    """获取指定名称的logger实例

    Args:
        name: logger名称，通常使用模块名（如：__name__）

    Returns:
        logging.Logger: logger实例

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
    """动态设置指定logger的日志级别

    Args:
        logger_name: logger名称
        level: 新的日志级别字符串

    Raises:
        ValueError: 当日志级别无效时

    Example:
        >>> # 设置特定模块的日志级别
        >>> set_log_level("db_connector_tool.core", "DEBUG")

        >>> # 设置整个应用的日志级别
        >>> set_log_level("db_connector_tool", "WARNING")
    """
    log_level = _validate_log_level(level)
    logger = get_logger(logger_name)
    logger.setLevel(log_level)

    # 同时更新所有handler的级别，确保输出一致性
    for handler in logger.handlers:
        handler.setLevel(log_level)


class LogManager:
    """高级日志管理器类 (Log Manager)

    Example:
        >>> # 创建日志管理器
        >>> log_manager = LogManager("my_app")
        >>> logger = log_manager.setup(level="DEBUG")

        >>> # 添加额外handler
        >>> log_manager.add_file_handler("debug.log", level="DEBUG")

        >>> # 清理资源
        >>> log_manager.cleanup()
    """

    def __init__(self, app_name: str) -> None:
        """初始化日志管理器实例

        Args:
            app_name: 应用名称，用于标识不同的日志配置

        Example:
            >>> # 创建日志管理器实例
            >>> log_manager = LogManager("my_app")
        """
        self.app_name = app_name
        # 创建LogManager自身的logger，用于记录管理操作
        self.logger = get_logger(f"{__name__}.LogManager")
        self._handlers: List[logging.Handler] = []
        self._lock = threading.Lock()

    def setup(self, **kwargs: Any) -> logging.Logger:
        """设置并配置日志系统

        Args:
            **kwargs: 传递给setup_logging的参数

        Returns:
            logging.Logger: 配置好的logger实例

        Example:
            >>> log_manager = LogManager("my_app")
            >>> logger = log_manager.setup(level="DEBUG", log_to_file=True)
        """
        logger = setup_logging(app_name=self.app_name, **kwargs)
        self.logger.info("LogManager为应用 '%s' 配置了日志系统", self.app_name)
        return logger

    def add_file_handler(self, log_file: str, **kwargs) -> None:
        """添加额外的文件handler到应用logger

        Args:
            log_file: 日志文件路径
            max_size: 最大文件大小（字节），默认10MB
            backup_count: 备份文件数量，默认5个
            level: 可选的日志级别，如果为None则使用logger的级别
            when: 时间轮转规则，如 'midnight', 'H', 'D' 等

        Raises:
            ValueError: 当日志级别无效时
            OSError: 当无法创建日志文件时
            PermissionError: 当没有文件写入权限时

        Example:
            >>> # 添加调试日志文件
            >>> log_manager.add_file_handler("debug.log", level="DEBUG")

            >>> # 添加每小时轮转的日志
            >>> log_manager.add_file_handler("hourly.log", when="H")
        """
        # 默认参数
        max_size = kwargs.get("max_size", 10 * 1024 * 1024)
        backup_count = kwargs.get("backup_count", 5)
        level = kwargs.get("level")
        when = kwargs.get("when")

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

            self.logger.info("添加文件handler: %s", log_file)

    def remove_handler(self, handler: logging.Handler) -> None:
        """移除指定的handler并清理资源

        Args:
            handler: 要移除的handler实例

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
                self.logger.info("已移除handler: %s", handler)

    def cleanup(self) -> None:
        """清理所有由LogManager创建的handler

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

            self.logger.info("已清理所有handler，共清理 %s 个", handler_count)

    def get_loggers_info(self) -> Dict[str, Dict[str, Any]]:
        """获取系统中所有logger的配置信息

        Returns:
            Dict[str, Dict[str, Any]]:
                - 键: logger名称
                - 值: 包含级别、handler数量、传播设置的信息字典

        Example:
            >>> info = log_manager.get_loggers_info()
            >>> print(info["db_connector_tool.core"])
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
    def quick_setup(app_name: str, level: str = "INFO") -> "LogManager":
        """快速创建并配置LogManager实例

        Args:
            app_name: 应用名称
            level: 日志级别，默认"INFO"

        Returns:
            LogManager: 配置好的LogManager实例

        Example:
            >>> # 快速配置
            >>> log_manager = LogManager.quick_setup("my_app", "DEBUG")
        """
        log_manager = LogManager(app_name)
        log_manager.setup(level=level)
        return log_manager
