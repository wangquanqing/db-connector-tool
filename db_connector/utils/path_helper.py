"""
跨平台路径处理工具
"""

import os
import platform
from pathlib import Path
from ..exceptions import ConfigError
import logging

logger = logging.getLogger(__name__)

class PathHelper:
    """路径辅助类"""
    
    @staticmethod
    def get_user_config_dir(app_name: str = "db_connector") -> Path:
        """
        获取用户配置目录
        
        Args:
            app_name: 应用名称
            
        Returns:
            配置目录Path对象
        """
        system = platform.system().lower()
        
        try:
            if system == "windows":
                base_dir = Path(os.environ.get('APPDATA', Path.home()))
            elif system == "darwin":  # macOS
                base_dir = Path.home() / "Library" / "Application Support"
            else:  # Linux和其他Unix系统
                base_dir = Path.home() / ".config"
            
            config_dir = base_dir / app_name
            config_dir.mkdir(parents=True, exist_ok=True)
            
            logger.debug(f"配置目录: {config_dir}")
            return config_dir
            
        except Exception as e:
            logger.error(f"创建配置目录失败: {str(e)}")
            # 回退到当前目录
            fallback_dir = Path.cwd() / f".{app_name}"
            fallback_dir.mkdir(exist_ok=True)
            return fallback_dir
    
    @staticmethod
    def get_user_home_dir() -> Path:
        """
        获取用户主目录
        
        Returns:
            用户主目录Path对象
        """
        return Path.home()
    
    @staticmethod
    def ensure_dir_exists(dir_path: Path) -> bool:
        """
        确保目录存在
        
        Args:
            dir_path: 目录路径
            
        Returns:
            是否成功创建或目录已存在
        """
        try:
            dir_path.mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            logger.error(f"创建目录失败 {dir_path}: {str(e)}")
            return False
