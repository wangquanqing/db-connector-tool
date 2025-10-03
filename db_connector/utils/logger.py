"""
日志配置模块
"""

import logging
import logging.handlers
import sys
from pathlib import Path
from .path_helper import PathHelper
import os

def setup_logging(app_name: str = "db_connector", level: str = "INFO") -> logging.Logger:
    """
    配置日志系统
    
    Args:
        app_name: 应用名称
        level: 日志级别
        
    Returns:
        配置好的logger实例
    """
    # 获取日志目录
    log_dir = PathHelper.get_user_config_dir(app_name) / "logs"
    PathHelper.ensure_dir_exists(log_dir)
    
    log_file = log_dir / f"{app_name}.log"
    
    # 创建formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s [%(filename)s:%(lineno)d]'
    )
    
    # 配置root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))
    
    # 清除已有的handler
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 文件handler - 滚动日志
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)
    
    # 控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    logger = logging.getLogger(__name__)
    logger.info(f"日志系统初始化完成，日志文件: {log_file}")
    
    return logger

def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的logger
    
    Args:
        name: logger名称
        
    Returns:
        logger实例
    """
    return logging.getLogger(name)
