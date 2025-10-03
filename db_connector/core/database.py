"""
数据库管理器主类
"""

from ..drivers.sqlalchemy_driver import SQLAlchemyDriver
from ..exceptions import DatabaseError, ConnectionError, ConfigError
from .config_manager import ConfigManager
from ..utils.logger import get_logger

logger = get_logger(__name__)

class DatabaseManager:
    """数据库管理器"""
    
    def __init__(self, app_name: str = "db_connector"):
        """
        初始化数据库管理器
        
        Args:
            app_name: 应用名称
        """
        self.app_name = app_name
        self.config_manager = ConfigManager(app_name)
        self.connections = {}
        
    def create_connection(self, name: str, connection_config: dict):
        """
        创建数据库连接配置
        
        Args:
            name: 连接名称
            connection_config: 连接配置
        """
        try:
            # 验证必需的配置项
            self._validate_connection_config(connection_config)
            
            # 保存到配置
            self.config_manager.add_connection(name, connection_config)
            logger.info(f"数据库连接配置已创建: {name}")
            
        except Exception as e:
            logger.error(f"创建连接配置失败 {name}: {str(e)}")
            raise DatabaseError(f"创建连接配置失败: {str(e)}")
    
    def _validate_connection_config(self, config: dict):
        """验证连接配置"""
        required_fields = ['type']
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            raise ConfigError(f"缺少必需的连接参数: {', '.join(missing_fields)}")
        
        db_type = config['type'].lower()
        if db_type not in ['oracle', 'postgresql', 'mysql', 'mssql', 'sqlite']:
            raise ConfigError(f"不支持的数据库类型: {db_type}")
        
        # SQLite不需要主机、用户名、密码
        if db_type != 'sqlite':
            db_required = ['host', 'username', 'password', 'database']
            db_missing = [field for field in db_required if field not in config]
            if db_missing:
                raise ConfigError(f"数据库类型 {db_type} 需要参数: {', '.join(db_missing)}")
    
    def get_connection(self, name: str) -> SQLAlchemyDriver:
        """
        获取数据库连接
        
        Args:
            name: 连接名称
            
        Returns:
            SQLAlchemyDriver实例
        """
        try:
            # 如果连接已存在且有效，直接返回
            if name in self.connections:
                driver = self.connections[name]
                if driver.is_connected and driver.test_connection():
                    return driver
            
            # 获取连接配置
            connection_config = self.config_manager.get_connection(name)
            
            # 创建新的驱动实例
            driver = SQLAlchemyDriver(connection_config)
            driver.connect()
            
            # 缓存连接
            self.connections[name] = driver
            
            logger.info(f"数据库连接已建立: {name}")
            return driver
            
        except Exception as e:
            logger.error(f"获取数据库连接失败 {name}: {str(e)}")
            if isinstance(e, (ConnectionError, ConfigError)):
                raise
            raise DatabaseError(f"获取数据库连接失败: {str(e)}")
    
    def test_connection(self, name: str) -> bool:
        """
        测试连接是否有效
        
        Args:
            name: 连接名称
            
        Returns:
            连接是否成功
        """
        try:
            driver = self.get_connection(name)
            return driver.test_connection()
        except Exception as e:
            logger.error(f"连接测试失败 {name}: {str(e)}")
            return False
    
    def execute_query(self, connection_name: str, query: str, params: dict = None) -> list:
        """
        执行查询
        
        Args:
            connection_name: 连接名称
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果
        """
        try:
            driver = self.get_connection(connection_name)
            return driver.execute_query(query, params)
        except Exception as e:
            logger.error(f"执行查询失败: {str(e)}")
            raise DatabaseError(f"执行查询失败: {str(e)}")
    
    def execute_command(self, connection_name: str, command: str, params: dict = None) -> int:
        """
        执行非查询命令
        
        Args:
            connection_name: 连接名称
            command: SQL命令
            params: 命令参数
            
        Returns:
            影响的行数
        """
        try:
            driver = self.get_connection(connection_name)
            return driver.execute_command(command, params)
        except Exception as e:
            logger.error(f"执行命令失败: {str(e)}")
            raise DatabaseError(f"执行命令失败: {str(e)}")
    
    def close_connection(self, name: str):
        """
        关闭数据库连接
        
        Args:
            name: 连接名称
        """
        try:
            if name in self.connections:
                self.connections[name].disconnect()
                del self.connections[name]
                logger.info(f"数据库连接已关闭: {name}")
        except Exception as e:
            logger.error(f"关闭连接失败 {name}: {str(e)}")
            raise DatabaseError(f"关闭连接失败: {str(e)}")
    
    def close_all_connections(self):
        """关闭所有数据库连接"""
        try:
            for name in list(self.connections.keys()):
                self.close_connection(name)
            logger.info("所有数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭所有连接失败: {str(e)}")
            raise DatabaseError(f"关闭所有连接失败: {str(e)}")
    
    def list_connections(self) -> list:
        """
        获取所有连接名称
        
        Returns:
            连接名称列表
        """
        return self.config_manager.list_connections()
    
    def remove_connection(self, name: str):
        """
        删除连接配置
        
        Args:
            name: 连接名称
        """
        try:
            # 先关闭连接
            if name in self.connections:
                self.close_connection(name)
            
            # 删除配置
            self.config_manager.remove_connection(name)
            logger.info(f"连接配置已删除: {name}")
            
        except Exception as e:
            logger.error(f"删除连接配置失败 {name}: {str(e)}")
            raise DatabaseError(f"删除连接配置失败: {str(e)}")
    
    def __del__(self):
        """析构函数"""
        try:
            self.close_all_connections()
        except:
            pass  # 忽略析构过程中的错误
