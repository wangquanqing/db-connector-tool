# DB Connector - 跨平台数据库连接管理模块

一个安全、跨平台的Python数据库连接管理模块，支持主流数据库并提供加密存储功能。

## 特性

- 🔐 **安全加密**: 使用 `cryptography.fernet` 加密敏感连接信息
- 📁 **配置管理**: 基于 TOML 的配置文件（读取：`tomllib`，写入：`tomli-w`）
- 🗄️ **多数据库支持**: 
  - Oracle (oracledb)
  - PostgreSQL (psycopg2/psycopg3)
  - SQL Server (pymssql)
  - MySQL (pymysql)
  - SQLite (sqlite3)
- 🏗️ **ORM 集成**: 基于 SQLAlchemy 2.0+
- 📊 **完整日志**: 使用 logging 模块记录操作日志
- 🌐 **跨平台**: 支持 Windows、Linux、macOS
- 🧪 **完整测试**: 包含单元测试和集成测试

## 安装

### 要求

- Python >= 3.8
- 参见 `requirements.txt` 了解依赖详情

### 从源码安装

```bash
git clone https://github.com/yourusername/db-connector.git
cd db-connector
pip install -e .
```

### 从 PyPI 安装（未来计划）

```bash
pip install db-connector
```

## 快速开始

### 基础用法

```python
from db_connector import DatabaseManager

# 创建数据库管理器
db_manager = DatabaseManager()

# 添加数据库连接
mysql_config = {
    'type': 'mysql',
    'host': 'localhost',
    'port': '3306',
    'username': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}

db_manager.create_connection('my_mysql', mysql_config)

# 执行查询
results = db_manager.execute_query('my_mysql', 'SELECT * FROM users LIMIT 10')
print(results)

# 关闭连接
db_manager.close_all_connections()
```

### 多数据库操作

```python
from db_connector import DatabaseManager

db_manager = DatabaseManager()

# 配置多个数据库
databases = {
    'app_db': {
        'type': 'postgresql',
        'host': 'db.server.com',
        'username': 'user',
        'password': 'pass',
        'database': 'application'
    },
    'log_db': {
        'type': 'sqlite',
        'database': '/path/to/logs.db'
    }
}

for name, config in databases.items():
    db_manager.create_connection(name, config)

# 跨数据库操作
users = db_manager.execute_query('app_db', 'SELECT * FROM users')
db_manager.execute_command('log_db', 'INSERT INTO access_log VALUES (?, ?)', (user_id, 'login'))

db_manager.close_all_connections()
```

## 配置说明

### 连接配置格式

每个数据库连接支持以下配置：

**MySQL/PostgreSQL/SQL Server/Oracle:**
```python
{
    'type': 'mysql',  # mysql, postgresql, mssql, oracle
    'host': 'localhost',
    'port': '3306',
    'username': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}
```

**SQLite:**
```python
{
    'type': 'sqlite',
    'database': '/path/to/database.db'  # 或 ':memory:' 用于内存数据库
}
```

### 配置文件位置

- **配置文件**: `~/.config/db_connector/connections.toml`
- **日志文件**: `~/.config/db_connector/logs/db_connector.log`
- **加密密钥**: `~/.config/db_connector/encryption.key`

## API 参考

### DatabaseManager

主要管理类，提供以下方法：

- `create_connection(name, config)`: 创建连接配置
- `get_connection(name)`: 获取数据库连接
- `execute_query(connection_name, query, params)`: 执行查询
- `execute_command(connection_name, command, params)`: 执行命令
- `test_connection(name)`: 测试连接
- `list_connections()`: 列出所有连接
- `remove_connection(name)`: 删除连接
- `close_connection(name)`: 关闭连接
- `close_all_connections()`: 关闭所有连接

## 开发

### 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试模块
pytest tests/test_database.py

# 带覆盖率的测试
pytest --cov=db_connector tests/
```

### 代码风格

本项目使用 Black 代码格式化工具：

```bash
black db_connector/ tests/ examples/
```

## 许可证

MIT License - 详见 LICENSE 文件

## 贡献

欢迎提交 Issue 和 Pull Request！

## 支持

如有问题请：
1. 查看文档和示例
2. 提交 GitHub Issue
3. 联系维护者
