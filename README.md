# DB Connector Tool

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

一个安全、跨平台的Python数据库连接管理工具，支持多种主流数据库并提供加密存储功能。

## ✨ 特性

### 🔐 安全特性

- **全字段加密**: 使用 `cryptography.fernet` 加密所有敏感连接信息
- **密钥管理**: 支持操作系统密钥环和文件权限保护双重方案
- **PBKDF2派生**: 使用480,000次迭代的PBKDF2密钥派生算法
- **安全随机数**: 基于 `secrets`模块的密码学安全随机数生成

### 🏗️ 架构设计

- **模块化架构**: 清晰的职责分离（核心、驱动、工具模块）
- **连接池管理**: 线程安全的连接池，支持连接复用和生命周期管理
- **异常体系**: 统一的异常处理，支持精确错误分类和上下文信息
- **批量操作**: 支持基于模板的批量连接配置和并发执行

### 🗄️ 多数据库支持

- **Oracle**: 通过 oracledb 驱动（原生支持）
- **PostgreSQL**: 通过 psycopg3 驱动（异步支持）
- **MySQL**: 通过 pymysql 驱动（纯Python实现）
- **SQL Server**: 通过 pymssql 驱动（FreeTDS后端）
- **SQLite**: 内置支持，无需额外依赖
- **GBase 8s**: 通过 JDBC 驱动（完整SQLAlchemy方言支持）

### 📊 运维特性

- **配置管理**: 基于 TOML 的配置文件，支持自动备份和版本控制
- **完整日志**: 多级别日志输出，支持文件轮转和格式化
- **CLI工具**: 完整的命令行界面，支持交互式SQL Shell
- **跨平台**: 支持 Windows、Linux、macOS，路径自动适配

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

> 📚 **完整教程**: 查看 [TUTORIAL.md](TUTORIAL.md) 获取详细的使用指南、API参考和最佳实践

### 基础用法

```python
from db_connector_tool import DatabaseManager

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
from db_connector_tool import DatabaseManager

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
db_manager.execute_command('log_db', 'INSERT INTO access_log VALUES (?, ?)', {"user_id": "user_id", "action": "login"})

db_manager.close_all_connections()
```

### 批量连接管理示例

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

# 生成IP范围并批量添加连接
ip_list = generate_ip_range("192.168.1.100", 50)
results = batch_manager.add_batch_connections(ip_list)

# 批量执行查询
query_results = batch_manager.execute_batch_query("SELECT COUNT(*) FROM users")

# 清理临时配置
batch_manager.cleanup()
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

详细API文档和使用示例请查看 [TUTORIAL.md](TUTORIAL.md)，包含：

### 核心管理类

- **DatabaseManager**: 数据库连接生命周期管理
- **BatchDatabaseManager**: 批量连接配置和并发操作
- **ConfigManager**: 配置文件加密存储和管理
- **CryptoManager**: 数据加密解密功能

### 功能模块

- 完整的异常处理体系和使用示例
- 工具函数和辅助类详细说明
- 命令行工具(CLI)完整指南
- 最佳实践和故障排除

### 快速查找

在教程文件中可以找到：

- 每个方法的详细参数说明
- 实际使用代码示例
- 错误处理最佳实践
- 生产环境部署建议

## 🔒 安全特性

### 多层安全保护

#### 1. 数据加密

- **全字段加密**: 所有连接配置字段自动加密存储
- **强加密算法**: 使用 AES-256 加密和 PBKDF2 密钥派生
- **安全迭代**: 480,000次PBKDF2迭代，符合OWASP安全标准

#### 2. 密钥管理

- **操作系统密钥环**: 优先使用系统密钥存储服务（keyring）
- **文件权限保护**: 回退方案使用严格的文件权限控制
- **密钥派生**: 基于用户密码和随机盐值动态派生加密密钥

#### 3. 配置安全

- **配置文件完整性**: TOML格式验证和版本兼容性检查
- **自动备份**: 配置文件变更时自动创建备份
- **敏感信息处理**: 日志中自动屏蔽密码等敏感信息

### 安全配置示例

```python
# 配置会自动加密存储，支持操作系统密钥环
config = {
    'type': 'mysql',
    'host': 'localhost',
    'username': 'user',
    'password': 'secret_password'  # 自动加密存储
}

# 使用加密管理器直接操作
from db_connector_tool.core import CryptoManager
crypto = CryptoManager()
encrypted = crypto.encrypt("sensitive_data")
```

### 配置文件位置

- **主配置文件**: `~/.config/db_connector_tool/connections.toml`
- **临时配置文件**: `~/.config/db_connector_tool/connections_*.toml`（批量管理）
- **日志文件**: `~/.config/db_connector_tool/logs/db_connector_tool.log`
- **加密密钥**: 操作系统密钥环或 `~/.config/db_connector_tool/encryption.key`

## 🧪 开发

### 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试模块
pytest tests/test_database.py

# 带覆盖率的测试
pytest --cov=db_connector_tool tests/

# 运行快速测试（跳过慢速测试）
pytest -m "not slow"
```

### 代码质量

```bash
# 代码格式化
black db_connector_tool/ tests/

# 代码检查
flake8 db_connector_tool/ tests/

# 类型检查
mypy db_connector_tool/
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
