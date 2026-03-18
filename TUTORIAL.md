# DB Connector Tool 使用教程

本教程将详细介绍 DB Connector Tool 的所有公共方法，通过实际示例展示如何使用这个强大的数据库连接管理工具。

## 📚 目录

1. [快速开始](#快速开始)
2. [核心管理类](#核心管理类)
   - [DatabaseManager](#databasemanager)
   - [BatchDatabaseManager](#batchdatabasemanager)
   - [ConfigManager](#configmanager)
   - [CryptoManager](#cryptomanager)
3. [异常处理](#异常处理)
4. [工具函数](#工具函数)
5. [命令行工具](#命令行工具)
6. [最佳实践](#最佳实践)

## 🚀 快速开始

### 安装

```bash
# 从PyPI安装
pip install db-connector-tool

# 或从源码安装
git clone https://github.com/wangquanqing/db-connector-tool.git
cd db-connector-tool
pip install -e .
```

### 基础示例

```python
from db_connector_tool import DatabaseManager

# 创建数据库管理器
db_manager = DatabaseManager()

# 添加MySQL连接
mysql_config = {
    'type': 'mysql',
    'host': 'localhost',
    'port': 3306,
    'username': 'user',
    'password': 'password',
    'database': 'test_db'
}

db_manager.add_connection('mysql_db', mysql_config)

# 执行查询
results = db_manager.execute_query('mysql_db', 'SELECT * FROM users')
print(results)

# 关闭连接
db_manager.close_all_connections()
```

## 🔧 核心管理类

### DatabaseManager

DatabaseManager 是主要的数据库连接管理类，提供完整的连接生命周期管理。

#### 初始化

```python
from db_connector_tool import DatabaseManager

# 使用默认配置
db_manager = DatabaseManager()

# 自定义应用名称和配置文件
db_manager = DatabaseManager(
    app_name="my_application",
    config_file="database.toml"
)
```

#### 添加连接配置

```python
# MySQL连接
mysql_config = {
    'type': 'mysql',
    'host': 'localhost',
    'port': 3306,
    'username': 'user',
    'password': 'password',
    'database': 'test_db',
    'charset': 'utf8mb4'
}
db_manager.add_connection('mysql_db', mysql_config)

# PostgreSQL连接
pg_config = {
    'type': 'postgresql',
    'host': 'localhost',
    'port': 5432,
    'username': 'user',
    'password': 'password',
    'database': 'test_db'
}
db_manager.add_connection('postgres_db', pg_config)

# Oracle连接
oracle_config = {
    'type': 'oracle',
    'host': 'localhost',
    'port': 1521,
    'username': 'user',
    'password': 'password',
    'service_name': 'ORCL'
}
db_manager.add_connection('oracle_db', oracle_config)

# SQLite连接
sqlite_config = {
    'type': 'sqlite',
    'database': '/path/to/database.db'
}
db_manager.add_connection('sqlite_db', sqlite_config)
```

#### 执行查询

```python
# 执行SELECT查询
results = db_manager.execute_query('mysql_db', 'SELECT * FROM users WHERE age > ?', (18,))

# 执行INSERT/UPDATE/DELETE命令
affected_rows = db_manager.execute_command(
    'mysql_db', 
    'INSERT INTO users (name, email) VALUES (?, ?)',
    ('张三', 'zhangsan@example.com')
)

# 执行事务操作
with db_manager.get_connection('mysql_db') as conn:
    # 在事务中执行多个操作
    conn.execute('UPDATE accounts SET balance = balance - 100 WHERE id = 1')
    conn.execute('UPDATE accounts SET balance = balance + 100 WHERE id = 2')
    # 事务会自动提交或回滚
```

#### 连接管理

```python
# 测试连接
if db_manager.test_connection('mysql_db'):
    print("连接正常")
else:
    print("连接失败")

# 列出所有连接
connections = db_manager.list_connections()
print("可用连接:", connections)

# 获取连接统计信息
stats = db_manager.get_statistics()
print("连接统计:", stats)

# 关闭指定连接
db_manager.close_connection('mysql_db')

# 关闭所有连接
db_manager.close_all_connections()

# 删除连接配置
db_manager.remove_connection('mysql_db')
```

### BatchDatabaseManager

BatchDatabaseManager 用于管理大量配置相似的数据库连接，支持批量操作。

#### 初始化

```python
from db_connector_tool import BatchDatabaseManager, generate_ip_range

# 创建批量管理器
batch_manager = BatchDatabaseManager("batch_operation")

# 设置基础配置模板
base_config = {
    "type": "mysql",
    "port": 3306,
    "username": "admin",
    "password": "password",
    "database": "user_db"
}
batch_manager.set_base_config(base_config)
```

#### 批量添加连接

```python
# 生成IP地址范围
ip_list = generate_ip_range("192.168.1.100", 10)
print("IP范围:", ip_list)

# 批量添加连接
results = batch_manager.add_batch_connections(ip_list)
print("批量添加结果:", results)

# 检查成功和失败的连接
successful = [ip for ip, result in results.items() if result["success"]]
failed = [ip for ip, result in results.items() if not result["success"]]

print(f"成功: {len(successful)}, 失败: {len(failed)}")
```

#### 批量查询操作

```python
# 批量执行查询
query_results = batch_manager.execute_batch_query("SELECT COUNT(*) FROM users")

# 处理查询结果
for host, result in query_results.items():
    if result["success"]:
        print(f"{host}: {result['data']}")
    else:
        print(f"{host}: 查询失败 - {result['error']}")

# 带参数的批量查询
param_results = batch_manager.execute_batch_query(
    "SELECT * FROM users WHERE age > ?",
    params=(18,)
)
```

#### 表结构升级

```python
# 批量升级表结构
upgrade_sql = """
ALTER TABLE users 
ADD COLUMN last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP
"""

upgrade_results = batch_manager.upgrade_table_structure("users", upgrade_sql)

for host, result in upgrade_results.items():
    if result["success"]:
        print(f"{host}: 表结构升级成功")
    else:
        print(f"{host}: 升级失败 - {result['error']}")
```

#### 清理资源

```python
# 清理批量管理器创建的临时配置
batch_manager.cleanup()

# 或者使用工具函数清理所有临时配置
from db_connector_tool import cleanup_temp_configs
cleanup_temp_configs()
```

### ConfigManager

ConfigManager 负责配置文件的加密存储和管理。

#### 基本操作

```python
from db_connector_tool import ConfigManager

# 创建配置管理器
config_manager = ConfigManager("my_app")

# 添加连接配置
config = {
    "type": "mysql",
    "host": "localhost",
    "username": "user",
    "password": "secret"
}
config_manager.add_connection("db1", config)

# 获取连接配置
saved_config = config_manager.get_connection("db1")
print("保存的配置:", saved_config)

# 列出所有连接
connections = config_manager.list_connections()
print("所有连接:", connections)

# 删除连接配置
config_manager.remove_connection("db1")

# 备份配置文件
config_manager.backup_config()
```

#### 配置验证

```python
# 验证配置格式
try:
    config_manager._validate_config({
        "version": "1.0.0",
        "app_name": "test",
        "connections": {},
        "metadata": {}
    })
    print("配置格式正确")
except Exception as e:
    print("配置格式错误:", e)
```

### CryptoManager

CryptoManager 提供数据加密解密功能。

#### 基本使用

```python
from db_connector_tool.core import CryptoManager

# 创建加密管理器
crypto = CryptoManager()

# 加密字符串数据
plaintext = "这是敏感数据"
encrypted = crypto.encrypt(plaintext)
print("加密结果:", encrypted)

# 解密数据
decrypted = crypto.decrypt(encrypted)
print("解密结果:", decrypted)

# 加密字节数据
binary_data = b"binary sensitive data"
encrypted_binary = crypto.encrypt_bytes(binary_data)

# 解密字节数据
decrypted_binary = crypto.decrypt_bytes(encrypted_binary)
```

#### 密钥管理

```python
# 获取密钥信息（用于持久化）
key_info = crypto.get_key_info()
print("密钥信息:", key_info)

# 从保存的密钥恢复加密管理器
from_saved = CryptoManager.from_saved_key(
    key_info["password"], 
    key_info["salt"]
)

# 验证恢复的加密器是否能正确解密
test_data = "测试数据"
encrypted_test = from_saved.encrypt(test_data)
decrypted_test = from_saved.decrypt(encrypted_test)
assert test_data == decrypted_test
print("密钥恢复验证成功")
```

## ⚠️ 异常处理

DB Connector Tool 提供了完整的异常处理体系。

### 基础异常处理

```python
from db_connector_tool.core.exceptions import (
    DBConnectorError, ConfigError, CryptoError, DatabaseError,
    ConnectionError, DriverError, QueryError
)

try:
    # 尝试执行可能失败的操作
    db_manager.execute_query("nonexistent_db", "SELECT 1")
    
except ConnectionError as e:
    print(f"连接错误: {e}")
    print(f"错误详情: {e.to_dict()}")
    
except QueryError as e:
    print(f"查询错误: {e}")
    
except DBConnectorError as e:
    print(f"数据库连接器错误: {e}")
    
except Exception as e:
    print(f"未知错误: {e}")
```

### 配置异常处理

```python
try:
    config_manager.add_connection("", {})  # 空名称会触发异常
    
except ConfigError as e:
    print(f"配置错误: {e}")
    print(f"错误代码: {e.error_code}")
    print(f"详细信息: {e.details}")
```

### 加密异常处理

```python
try:
    # 尝试解密无效数据
    crypto.decrypt("invalid_encrypted_data")
    
except CryptoError as e:
    print(f"加密错误: {e}")
    
except InvalidToken:
    print("加密令牌无效，可能被篡改")
```

## 🛠️ 工具函数

### 路径工具

```python
from db_connector_tool.utils.path_utils import PathHelper

# 创建路径助手
path_helper = PathHelper()

# 获取用户配置目录
config_dir = PathHelper.get_user_config_dir("my_app")
print("配置目录:", config_dir)

# 检查路径存在性
if path_helper.exists(config_dir):
    print("配置目录存在")

# 创建目录（如果不存在）
path_helper.ensure_dir_exists(config_dir)
```

### 日志工具

```python
from db_connector_tool.utils.logging_utils import (
    setup_logging, get_logger, set_log_level
)

# 初始化日志系统
logger = setup_logging(
    app_name="my_app",
    level="DEBUG",
    log_to_console=True,
    log_to_file=True
)

# 获取模块日志器
module_logger = get_logger(__name__)
module_logger.info("模块初始化完成")

# 动态调整日志级别
set_log_level("INFO")

# 记录不同级别的日志
module_logger.debug("调试信息")
module_logger.info("普通信息")
module_logger.warning("警告信息")
module_logger.error("错误信息")
module_logger.critical("严重错误")
```

### IP范围生成

```python
from db_connector_tool import generate_ip_range

# 生成连续IP范围
ip_list = generate_ip_range("192.168.1.100", 5)
print("IP范围:", ip_list)
# 输出: ['192.168.1.100', '192.168.1.101', '192.168.1.102', '192.168.1.103', '192.168.1.104']

# 生成大量IP地址
large_range = generate_ip_range("10.0.0.1", 1000)
print(f"生成 {len(large_range)} 个IP地址")
```

## 💻 命令行工具

### 基本命令

```bash
# 查看帮助
db-connector --help

# 显示版本
db-connector --version

# 列出所有连接
db-connector list

# 添加MySQL连接
db-connector add mysql-dev --type mysql --host localhost --username root --password 123456 --database test_db

# 测试连接
db-connector test mysql-dev

# 执行查询
db-connector query mysql-dev "SELECT * FROM users"

# 进入交互式SQL Shell
db-connector shell mysql-dev

# 删除连接
db-connector remove mysql-dev
```

### 高级用法

```bash
# 使用JSON格式输出
db-connector query mysql-dev "SELECT * FROM users" --format json

# 导出查询结果为CSV
db-connector query mysql-dev "SELECT * FROM users" --format csv --output users.csv

# 执行SQL文件
db-connector exec mysql-dev --file init.sql

# 批量执行SQL文件
db-connector batch-exec --dir sql_scripts/
```

## 🏆 最佳实践

### 1. 连接管理最佳实践

```python
# 使用上下文管理器确保连接正确关闭
with DatabaseManager() as db_manager:
    db_manager.add_connection('db1', config)
    results = db_manager.execute_query('db1', 'SELECT 1')
    # 连接会在退出时自动关闭

# 或者显式管理连接生命周期
try:
    db_manager = DatabaseManager()
    # ... 执行操作 ...
finally:
    db_manager.close_all_connections()
```

### 2. 错误处理最佳实践

```python
def safe_database_operation(db_name, query, params=None):
    """安全的数据库操作函数"""
    try:
        db_manager = DatabaseManager()
        
        if not db_manager.test_connection(db_name):
            raise ConnectionError(f"数据库 {db_name} 连接失败")
        
        return db_manager.execute_query(db_name, query, params)
        
    except QueryError as e:
        logger.error(f"查询执行失败: {e}")
        # 返回空结果或重试
        return []
        
    except ConnectionError as e:
        logger.error(f"连接错误: {e}")
        # 尝试重连或通知管理员
        raise
        
    except Exception as e:
        logger.error(f"未知错误: {e}")
        raise
        
    finally:
        if 'db_manager' in locals():
            db_manager.close_all_connections()
```

### 3. 批量操作最佳实践

```python
def batch_database_maintenance():
    """批量数据库维护操作"""
    
    # 创建批量管理器
    batch_manager = BatchDatabaseManager("maintenance")
    
    try:
        # 设置基础配置
        base_config = {
            "type": "mysql",
            "port": 3306,
            "username": "admin",
            "password": "secure_password",
            "database": "app_db"
        }
        batch_manager.set_base_config(base_config)
        
        # 生成维护IP列表
        maintenance_ips = generate_ip_range("192.168.1.100", 50)
        
        # 批量添加连接
        add_results = batch_manager.add_batch_connections(maintenance_ips)
        
        # 分析结果
        successful_ips = [ip for ip, result in add_results.items() if result["success"]]
        
        if successful_ips:
            # 执行批量维护操作
            maintenance_results = batch_manager.execute_batch_query(
                "OPTIMIZE TABLE important_table"
            )
            
            # 记录结果
            for ip, result in maintenance_results.items():
                if result["success"]:
                    logger.info(f"{ip}: 维护操作成功")
                else:
                    logger.warning(f"{ip}: 维护操作失败: {result['error']}")
        
        return successful_ips
        
    except Exception as e:
        logger.error(f"批量维护操作失败: {e}")
        raise
        
    finally:
        # 清理临时配置
        batch_manager.cleanup()
```

### 4. 安全最佳实践

```python
def secure_configuration():
    """安全配置管理"""
    
    # 使用应用特定的配置名称
    config_manager = ConfigManager("my_secure_app")
    
    # 敏感配置加密存储
    sensitive_config = {
        "type": "mysql",
        "host": "db.example.com",
        "username": "app_user",
        "password": "very_secure_password_123",
        "database": "sensitive_data"
    }
    
    # 配置会自动加密
    config_manager.add_connection("secure_db", sensitive_config)
    
    # 定期备份配置
    config_manager.backup_config()
    
    # 使用操作系统密钥环（如果可用）
    try:
        import keyring
        # 密钥会自动使用系统密钥环存储
    except ImportError:
        logger.warning("keyring不可用，使用文件权限保护")
```

## 🔍 故障排除

### 常见问题

1. **连接失败**
   ```python
   # 检查网络连接和防火墙设置
   # 验证数据库服务是否运行
   # 检查用户名密码是否正确
   ```

2. **加密错误**
   ```python
   # 检查密钥文件权限
   # 验证加密密钥是否损坏
   # 尝试重新生成密钥
   ```

3. **配置问题**
   ```python
   # 检查配置文件格式
   # 验证配置字段是否完整
   # 查看详细错误日志
   ```

### 调试技巧

```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 使用调试模式
from db_connector_tool.utils.logging_utils import setup_logging
logger = setup_logging(level="DEBUG")

# 检查连接状态
if db_manager.test_connection('db_name'):
    print("连接正常")
else:
    print("连接失败，检查配置和网络")
```

## 📞 获取帮助

- 查看完整文档: [GitHub Repository](https://github.com/wangquanqing/db-connector-tool)
- 提交问题: [GitHub Issues](https://github.com/wangquanqing/db-connector-tool/issues)
- 联系维护者: wangquanqing1636@sina.com

---

**恭喜！** 您已经完成了 DB Connector Tool 的完整教程。现在您可以充分利用这个强大的工具来管理您的数据库连接了。
