# DB Connector

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

一个安全、跨平台的Python数据库连接管理工具，支持多种主流数据库并提供加密存储功能。

## ✨ 特性

- 🔐 **安全加密**: 使用 `cryptography.fernet` 加密敏感连接信息
- 📁 **配置管理**: 基于 TOML 的配置文件，支持备份功能
- 🗄️ **多数据库支持**:
  - Oracle (oracledb)
  - PostgreSQL (psycopg3)
  - SQL Server (pymssql)
  - MySQL (pymysql)
  - SQLite (sqlite3)
- 🏗️ **ORM 集成**: 基于 SQLAlchemy 2.0+ 的现代化ORM支持
- 📊 **完整日志**: 使用 logging 模块记录详细操作日志
- 🌐 **跨平台**: 支持 Windows、Linux、macOS
- 🧪 **完整测试**: 包含单元测试和集成测试
- 💻 **CLI 工具**: 提供命令行界面进行连接管理

## 📦 安装

### 从源码安装

```bash
git clone https://github.com/wangquanqing/db-connector-tool.git
cd db-connector-tool
pip install -e .
```

### 从 PyPI 安装

```bash
pip install db-connector-tool
```

## 🚀 快速开始

### 基础用法

```python
from db_connector import DatabaseManager

# 创建数据库管理器
db_manager = DatabaseManager()

# 添加MySQL连接配置
mysql_config = {
    'type': 'mysql',
    'host': 'localhost',
    'port': 3306,
    'username': 'your_username',
    'password': 'your_password',
    'database': 'your_database'
}

db_manager.add_connection('mysql_db', mysql_config)

# 执行查询
results = db_manager.execute_query('mysql_db', 'SELECT * FROM users LIMIT 10')
print(results)

# 关闭所有连接
db_manager.close_all_connections()
```

### 多数据库操作示例

```python
from db_connector import DatabaseManager

db_manager = DatabaseManager()

# 配置多个数据库连接
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
    db_manager.add_connection(name, config)

# 跨数据库操作
users = db_manager.execute_query('app_db', 'SELECT * FROM users')
db_manager.execute_command('log_db', 'INSERT INTO access_log VALUES (?, ?)', (user_id, 'login'))

db_manager.close_all_connections()
```

## 🔧 命令行工具

### 基本命令

```bash
# 查看帮助
db-connector --help

# 列出所有连接
db-connector list

# 添加新连接
db-connector add mysql-dev --type mysql --host localhost --username root --password your_password --database app_db

# 测试连接
db-connector test mysql-dev

# 执行查询
db-connector query mysql-dev "SELECT * FROM users"

# 进入交互式SQL Shell
db-connector shell mysql-dev
```

## 📋 API 参考

### DatabaseManager 类

主要管理类，提供以下方法：

- `add_connection(name, config)` - 添加数据库连接配置
- `get_connection(name)` - 获取数据库连接
- `execute_query(connection_name, query, params)` - 执行查询语句
- `execute_command(connection_name, command, params)` - 执行命令语句
- `test_connection(name)` - 测试连接是否正常
- `list_connections()` - 列出所有连接配置
- `remove_connection(name)` - 删除连接配置
- `close_connection(name)` - 关闭指定连接
- `close_all_connections()` - 关闭所有连接

### ConfigManager 类

配置管理类，提供以下方法：

- `get_connection(name)` - 获取连接配置
- `remove_connection(name)` - 删除连接配置
- `list_connections()` - 列出所有连接配置

## 🔒 安全特性

### 加密存储

所有敏感信息（密码、连接字符串等）都会自动加密存储：

```python
# 配置会自动加密存储
config = {
    'type': 'mysql',
    'host': 'localhost',
    'username': 'user',
    'password': 'secret_password'
}
```

### 配置文件位置

- **配置文件**: `~/.config/db_connector/connections.toml`
- **日志文件**: `~/.config/db_connector/logs/db_connector.log`
- **加密密钥**: `~/.config/db_connector/encryption.key`

## 🧪 开发

### 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试模块
pytest tests/test_database.py

# 带覆盖率的测试
pytest --cov=db_connector tests/

# 运行快速测试（跳过慢速测试）
pytest -m "not slow"
```

### 代码质量

```bash
# 代码格式化
black db_connector/ tests/

# 代码检查
flake8 db_connector/ tests/

# 类型检查
mypy db_connector/
```

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE](LICENSE) 文件。

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

1. Fork 本项目
2. 创建功能分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 📞 支持

如有问题请：

1. 查看文档和示例代码
2. 提交 [GitHub Issue](https://github.com/wangquanqing/db-connector-tool/issues)
3. 联系维护者: wangquanqing1636@sina.com

## 📚 相关项目

- [SQLAlchemy](https://www.sqlalchemy.org/) - Python SQL 工具包和 ORM
- [cryptography](https://cryptography.io/) - Python 加密库
