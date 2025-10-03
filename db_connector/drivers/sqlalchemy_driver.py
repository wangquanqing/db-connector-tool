"""
SQLAlchemy 数据库驱动
支持多种数据库
"""

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.exc import SQLAlchemyError
from ..core.exceptions import ConnectionError, QueryError, DriverError
from ..utils.logger import get_logger
import logging

logger = get_logger(__name__)

class SQLAlchemyDriver:
    """SQLAlchemy 数据库驱动"""
    
    # 数据库驱动映射
    DRIVER_MAP = {
        'oracle': 'oracledb',
        'postgresql': 'psycopg2',
        'mysql': 'pymysql',
        'mssql': 'pymssql',
        'sqlite': 'sqlite3'
    }
    
    # 连接URL模板
    URL_TEMPLATES = {
        'oracle': 'oracle+oracledb://{username}:{password}@{host}:{port}/{database}',
        'postgresql': 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}',
        'mysql': 'mysql+pymysql://{username}:{password}@{host}:{port}/{database}',
        'mssql': 'mssql+pymssql://{username}:{password}@{host}:{port}/{database}',
        'sqlite': 'sqlite:///{database}'
    }
    
    def __init__(self, connection_config: dict):
        """
        初始化数据库驱动
        
        Args:
            connection_config: 数据库连接配置
        """
        self.connection_config = connection_config
        self.engine = None
        self.session_factory = None
        self._connected = False
        
    def _build_connection_url(self) -> str:
        """构建数据库连接URL"""
        db_type = self.connection_config.get('type', '').lower()
        
        if db_type not in self.URL_TEMPLATES:
            raise DriverError(f"不支持的数据库类型: {db_type}")
        
        template = self.URL_TEMPLATES[db_type]
        config = self.connection_config.copy()
        
        # SQLite特殊处理
        if db_type == 'sqlite':
            if 'database' not in config:
                config['database'] = ':memory:'
            return template.format(**config)
        
        # 其他数据库
        required_fields = ['username', 'password', 'host', 'database']
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            raise DriverError(f"缺少必需的连接参数: {', '.join(missing_fields)}")
        
        # 设置默认端口
        if 'port' not in config:
            config['port'] = self._get_default_port(db_type)
        
        return template.format(**config)
    
    def _get_default_port(self, db_type: str) -> str:
        """获取数据库默认端口"""
        default_ports = {
            'oracle': '1521',
            'postgresql': '5432',
            'mysql': '3306',
            'mssql': '1433'
        }
        return default_ports.get(db_type, '')
    
    def connect(self):
        """连接到数据库"""
        try:
            if self._connected:
                return
                
            connection_url = self._build_connection_url()
            
            # 创建引擎
            self.engine = create_engine(
                connection_url,
                echo=False,  # 设置为True可显示SQL日志
                pool_pre_ping=True,  # 连接池预检查
                pool_recycle=3600,  # 连接回收时间
            )
            
            # 创建会话工厂
            self.session_factory = scoped_session(
                sessionmaker(bind=self.engine)
            )
            
            self._connected = True
            logger.info(f"数据库连接成功: {self.connection_config.get('type')}")
            
        except SQLAlchemyError as e:
            logger.error(f"数据库连接失败: {str(e)}")
            raise ConnectionError(f"数据库连接失败: {str(e)}")
        except Exception as e:
            logger.error(f"连接过程中发生未知错误: {str(e)}")
            raise ConnectionError(f"连接错误: {str(e)}")
    
    def disconnect(self):
        """断开数据库连接"""
        try:
            if self.session_factory:
                self.session_factory.remove()
                self.session_factory = None
            
            if self.engine:
                self.engine.dispose()
                self.engine = None
            
            self._connected = False
            logger.info("数据库连接已断开")
            
        except Exception as e:
            logger.error(f"断开数据库连接失败: {str(e)}")
            raise ConnectionError(f"断开连接失败: {str(e)}")
    
    def execute_query(self, query: str, params: dict = None) -> list:
        """
        执行查询语句
        
        Args:
            query: SQL查询语句
            params: 查询参数
            
        Returns:
            查询结果列表
        """
        if not self._connected:
            raise ConnectionError("数据库未连接")
        
        try:
            with self.engine.connect() as connection:
                result = connection.execute(text(query), params or {})
                return [dict(row._mapping) for row in result]
                
        except SQLAlchemyError as e:
            logger.error(f"查询执行失败: {str(e)}")
            raise QueryError(f"查询执行失败: {str(e)}")
    
    def execute_command(self, command: str, params: dict = None) -> int:
        """
        执行非查询语句
        
        Args:
            command: SQL命令语句
            params: 命令参数
            
        Returns:
            影响的行数
        """
        if not self._connected:
            raise ConnectionError("数据库未连接")
        
        try:
            with self.engine.begin() as connection:
                result = connection.execute(text(command), params or {})
                return result.rowcount
                
        except SQLAlchemyError as e:
            logger.error(f"命令执行失败: {str(e)}")
            raise QueryError(f"命令执行失败: {str(e)}")
    
    def test_connection(self) -> bool:
        """
        测试连接是否有效
        
        Returns:
            连接是否成功
        """
        try:
            self.connect()
            test_query = self._get_test_query()
            self.execute_query(test_query)
            return True
        except Exception as e:
            logger.error(f"连接测试失败: {str(e)}")
            return False
    
    def _get_test_query(self) -> str:
        """获取测试连接用的查询语句"""
        db_type = self.connection_config.get('type', '').lower()
        
        test_queries = {
            'oracle': 'SELECT 1 FROM DUAL',
            'postgresql': 'SELECT 1',
            'mysql': 'SELECT 1',
            'mssql': 'SELECT 1',
            'sqlite': 'SELECT 1'
        }
        
        return test_queries.get(db_type, 'SELECT 1')
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
    
    @property
    def is_connected(self) -> bool:
        """获取连接状态"""
        return self._connected
