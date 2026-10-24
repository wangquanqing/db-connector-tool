# DB Connector Tool

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

一个安全、跨平台的Python数据库连接管理工具，支持多种主流数据库并提供加密存储功能。

## ✨ 特性

### 🔐 安全特性

- **全字段加密**: 使用 `cryptography.fernet` 加密所有敏感连接信息
- **密钥管理**: 支持操作系统密钥环和文件权限保护双重方案
- **PBKDF2派生**: PBKDF2-HMAC-SHA256 密钥派生（默认按硬件自适应 10 万~100 万次，`create_secure_instance()` 固定 48 万次）
- **安全随机数**: 基于 `secrets` 模块的密码学安全随机数生成

### 🏗️ 架构设计

- **模块化架构**: 清晰的职责分离（核心、驱动、工具模块）
- **连接池管理**: 线程安全的连接池，支持连接复用和生命周期管理
- **异常体系**: 统一的异常处理，支持精确错误分类和上下文信息
- **批量操作**: 支持基于模板的批量连接配置和并发执行

### 🗄️ 多数据库支持

- **Oracle**: 通过 oracledb 驱动（原生支持）
- **PostgreSQL**: 通过 psycopg3 驱动（同步使用）
- **MySQL**: 通过 pymysql 驱动（纯Python实现）
- **SQL Server**: 通过 pymssql 驱动（FreeTDS后端）
- **SQLite**: 内置支持，无需额外依赖
- **GBase 8s**: 通过 JayDeBeApi+JDBC 驱动（自研 SQLAlchemy 方言，支持一库三模，见 [设计说明](docs/gbase8s-dialect-design.md)）

### 📊 运维特性

- **配置管理**: 基于 TOML 的配置文件，支持自动备份和版本控制
- **完整日志**: 多级别日志输出，支持文件轮转和格式化
- **CLI工具**: 完整的命令行界面，支持交互式SQL Shell
- **跨平台**: 支持 Windows、Linux、macOS，路径自动适配

## 📦 安装

> ⚠️ **数据库驱动是可选依赖**：除 SQLite 外，各数据库驱动通过 extras 安装，
> 只装主包无法连接 Oracle/PostgreSQL/MySQL/SQL Server/GBase 8s。

### 从源码安装

```bash
git clone https://github.com/wangquanqing/db-connector-tool.git
cd db-connector-tool
pip install -e ".[all]"     # 安装全部驱动；也可按需选择 extras
```

### 从 PyPI 安装

```bash
pip install db-connector-tool            # 仅核心 + SQLite
pip install "db-connector-tool[all]"     # 全部驱动
pip install "db-connector-tool[mysql]"   # 按需：secure/oracle/postgresql/mysql/sqlserver/gbase8s
```

| extra | 用途 |
| --- | --- |
| `secure` | keyring，把主密钥存进操作系统密钥环（推荐） |
| `oracle` / `postgresql` / `mysql` / `sqlserver` | 对应数据库驱动 |
| `gbase8s` | jaydebeapi + python-dateutil（另需自备 GBase JDBC jar） |
| `all` | 以上全部 |

## 🚀 快速开始

> 📚 **完整教程**: 查看 [TUTORIAL.md](TUTORIAL.md) 获取详细的使用指南、API参考和最佳实践

### 基础用法

```python
from db_connector_tool import DatabaseManager

# 创建数据库管理器
db = DatabaseManager()

# 添加MySQL连接配置
db.add_connection("mysql_db", {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "your_username",
    "password": "your_password",
    "database": "your_database",
})

# 执行查询（使用命名参数）
results = db.execute_query(
    "mysql_db",
    "SELECT * FROM users WHERE age > :age",
    {"age": 18},
)
print(results)

# 执行增删改操作
affected = db.execute_command(
    "mysql_db",
    "UPDATE users SET name = :name WHERE id = :id",
    {"name": "Bob", "id": 1},
)
print(f"影响行数: {affected[0]}")

# 关闭所有连接
db.close_all_connections()
```

### 多数据库操作示例

```python
from db_connector_tool import DatabaseManager

db = DatabaseManager()

db.add_connection("app_db", {
    "type": "postgresql",
    "host": "db.server.com",
    "port": 5432,
    "username": "user",
    "password": "pass",
    "database": "application",
})
db.add_connection("log_db", {
    "type": "sqlite",
    "database": "/path/to/logs.db",
})

# 跨数据库操作（使用命名参数 :name 风格）
users = db.execute_query("app_db", "SELECT * FROM users")
db.execute_command(
    "log_db",
    "INSERT INTO access_log (user_id, action) VALUES (:user_id, :action)",
    {"user_id": 123, "action": "login"},
)

db.close_all_connections()
```

### GBase 8s 连接示例

GBase 8s 走 JDBC，需要额外的实例名与兼容模式参数（`sqlmode` 支持 `oracle`/`gbase`/`mysql`，
默认 `oracle`，默认端口 `9088`）：

```python
db.add_connection("gb8s", {
    "type": "gbase8s",
    "host": "127.0.0.1",
    "port": 9088,
    "server": "gbase01",          # GBASEDBTSERVER 实例名
    "database": "db_station",
    "username": "cvicse",
    "password": "secret",
    "sqlmode": "oracle",          # oracle / gbase / mysql
    # "jars": r"C:\path\to\gbasedbt-jdbc.jar",  # 未指定时按环境变量与约定目录查找
    # "CLIENT_LOCALE": "zh_CN.GB18030",          # 其他键会作为 JDBC 连接属性透传
})

rows, elapsed = db.execute_query("gb8s", "SELECT FIRST 10 * FROM systables")
```

CLI 等价写法：

```bash
db-connector add gb8s -T gbase8s -H 127.0.0.1 -P 9088 -S gbase01 \
    -d db_station -u cvicse -p -c sqlmode=oracle
```

JDBC jar 查找顺序：连接配置的 `jars` → 环境变量 `GBASEDBT_JDBC_JAR` /
`GBASE8S_JDBC_JAR` → **只搜索两个目录**（程序运行目录下的 `./jars`；
用户配置目录下的 `jars`，即 `%APPDATA%\db_connector_tool\jars`）
→ 由 `CLASSPATH` 决定。

**两个目录都找不到时会自动从 Maven Central 下载最新版驱动**（坐标 `com.gbasedbt:jdbc`）
到用户配置目录下的 `jars` 并直接使用；同一目录存在多个版本时，按文件名中的
版本号取最高者。

> **默认包优先于 `_nobson` 变体**：厂商对同一版本会同时发布默认包与 `_nobson` 包，
> 后者剥掉了内置的 `org.bson.*`（供自备 mongo-java-driver 的场景使用），而
> `maven-metadata.xml` 的 `<release>` 恰好指向 `_nobson`。本工具不引入 mongo 驱动，
> 因此**同版本存在默认包时一律选默认包**；本地只装了旧 `_nobson` 包时会自动补下默认包
> （否则遇到 BSON/JSON 列会抛 `NoClassDefFoundError`）。确需变体时置
> `GBASEDBT_JDBC_ALLOW_NOBSON=1`。

相关环境变量：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GBASEDBT_JDBC_AUTO_DOWNLOAD` | 启用 | 置 `0`/`false`/`no`/`off` 可关闭自动下载 |
| `GBASEDBT_JDBC_MAVEN_REPO` | `https://repo1.maven.org/maven2` | 镜像、私服，或本地目录 / `file://` 路径（便于内网离线分发） |
| `GBASEDBT_JDBC_ALLOW_NOBSON` | 关闭 | 置 `1` 才允许使用缺 `org.bson.*` 的 `_nobson` 变体 |
| `GBASEDBT_JDBC_NATIVE_ACCESS` | 启用 | 置 `0` 可关闭 `--enable-native-access` 注入（见下方说明） |
| `GBASEDBT_JDBC_DOWNLOAD_TIMEOUT` | `20` | 单次请求超时（秒） |
| `GBASEDBT_JDBC_DOWNLOAD_MAX_SIZE` | `41943040` | 下载大小上限（字节） |

> **JDK 24+ 的 JNI 警告**：JPype 启动 JVM 时调用 `java.lang.System::load`，而该方法自
> JDK 24 起属受限原生方法（[JEP 472](https://openjdk.org/jeps/472)），默认会打印 4 行
> `WARNING: A restricted method in java.lang.System has been called`。本工具在启动 JVM 前
> 自动补上 `--enable-native-access=ALL-UNNAMED`，警告随之消失，也避免将来被直接拒绝。
> 该参数对 JDK 17+ 有效，更早的 JDK 会被 JPype 忽略（`ignoreUnrecognized=True`）。
> 个别环境若因此启动失败，可置 `GBASEDBT_JDBC_NATIVE_ACCESS=0` 关闭注入。

下载会校验官方 `.sha1` 与 ZIP 文件头，先写临时文件再原子替换；失败只记录告警
（不影响 `CLASSPATH` 方案），且同一版本在同一进程内不会反复重试。
方言的设计与真机验证结论见 [docs/gbase8s-dialect-design.md](docs/gbase8s-dialect-design.md)。

### 批量连接管理示例

```python
from db_connector_tool import BatchDatabaseManager, generate_ip_range

batch = BatchDatabaseManager("batch_operation")

# 设置基础配置模板
batch.set_base_config({
    "type": "mysql",
    "port": 3306,
    "username": "admin",
    "password": "password",
    "database": "user_db",
})

# 生成IP范围并批量添加连接（返回 {IP: 是否成功}）
ip_list = generate_ip_range("192.168.1.100", 50)
results = batch.add_batch_connections(ip_list)
failed_ips = [ip for ip, ok in results.items() if not ok]

# 批量执行查询（返回 {连接名: {"success", "data", "count", "elapsed"}}）
query_results = batch.execute_batch_query("SELECT COUNT(*) FROM users")
for conn_name, result in query_results.items():
    if result["success"]:
        print(conn_name, result["count"], "行", result["elapsed"], "秒")

# 批量升级表结构（DDL 走命令接口并提交事务，失败时按 rollback_sqls 回滚）
upgrade_results = batch.upgrade_table_structure(
    ["ALTER TABLE users ADD COLUMN age INT"],
    ["ALTER TABLE users DROP COLUMN age"],
)

# 清理临时配置
batch.cleanup()
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

# 执行增删改
db-connector command mysql-dev "INSERT INTO users (name) VALUES ('John')"

# 从文件执行 SQL
db-connector file mysql-dev init.sql --continue-on-error

# 进入交互式SQL Shell
db-connector shell mysql-dev

# 查看连接详情
db-connector show mysql-dev

# 更新连接配置
db-connector update mysql-dev --host new_host --port 3307

# 删除连接
db-connector remove mysql-dev

# 导出查询结果为文件
db-connector query mysql-dev "SELECT * FROM users" --output users.json
```

## 📋 API 参考

详细API文档和使用示例请查看 [TUTORIAL.md](TUTORIAL.md)，包含：

### 核心管理类

- **DatabaseManager**: 数据库连接生命周期管理
- **BatchDatabaseManager**: 批量连接配置和并发操作
- **ConfigManager**: 配置文件加密存储和管理
- **CryptoManager**: 数据加密解密功能
- **KeyManager**: 加密密钥生成与管理

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

- **连接配置加密**: 所有连接配置字段自动加密存储
- **强加密算法**: 使用 Fernet（AES-128-CBC + HMAC-SHA256）加密和 PBKDF2 密钥派生
- **自适应迭代**: `CryptoManager()` 以 0.15 秒为目标自动调整 PBKDF2 迭代次数（10 万~100 万，按万取整）；`create_secure_instance()` 使用 OWASP 建议的 48 万次

#### 2. 密钥管理

- **操作系统密钥环**: 优先使用系统密钥存储服务（keyring）
- **自动降级**: 密钥环存在但不可用（服务未启动、凭据库拒绝写入等）时，自动回退到
  环境变量、再到文件存储，并在日志中给出告警，不会直接失败
- **文件权限保护**: 回退方案使用严格的文件权限控制
- **密钥派生**: 基于用户密码和随机盐值动态派生加密密钥

#### 3. 配置安全

- **配置文件完整性**: 每次保存都以 HMAC-SHA256 对配置内容签名；加载时校验，
  签名缺失或不匹配都会被拒绝（配置文件被外部编辑即视为不可信）
- **迁移逃生口**: 历史遗留的无签名配置可用
  `ConfigManager(app, file, require_signature=False)` 加载后重新保存以补上签名
- **自动备份**: 配置文件变更时自动创建备份
- **敏感信息处理**: 日志中自动屏蔽密码等敏感信息

#### 4. SQL 执行安全

- **参数化查询**: `execute_query` / `execute_command` 通过 `text(sql) + 参数`
  执行，参数不参与 SQL 拼接
- **注入特征检测**: 在剥离字符串字面量与注释后的语句结构上匹配
  `OR 1=1`、`xp_cmdshell`、`INTO OUTFILE`、动态 SQL 等特征；
  字面量里恰好含有这类文本的正常数据不会误报
- **拒绝多条语句**: `SELECT 1; DROP TABLE t` 这类语句走私会被拒绝，
  存储过程/触发器定义除外
- **不按语句种类设限**: 用户直接书写的管理语句——`DROP` / `TRUNCATE` /
  `CREATE` / `ALTER` / `GRANT` / `REVOKE` 等——正常执行；
  校验只针对注入特征与语句走私，不限制语句类型

### 安全配置示例

```python
# 配置会自动加密存储，支持操作系统密钥环
config = {
    "type": "mysql",
    "host": "localhost",
    "username": "user",
    "password": "secret_password",  # 自动加密存储
}

# 使用加密管理器直接操作
from db_connector_tool import CryptoManager
crypto = CryptoManager()
encrypted = crypto.encrypt("sensitive_data")
decrypted = crypto.decrypt(encrypted)
```

### 配置文件位置

配置目录由 `PathHelper.get_user_config_dir()` 按平台自动选择：

| 平台 | 目录 |
| --- | --- |
| Windows | `%APPDATA%\db_connector_tool\` |
| macOS | `~/Library/Application Support/db_connector_tool/` |
| Linux | `~/.config/db_connector_tool/` |

其中的文件：

- **主配置文件**: `connections.toml`
- **临时配置文件**: `connections_*.toml`（批量管理自动生成与清理）
- **日志文件**: `logs/db_connector_tool.log`（错误另存 `logs/db_connector_tool_error.log`）
- **加密密钥**: 操作系统密钥环（推荐）、环境变量，或回退文件 `encryption.key`

## 🧪 开发

### 运行测试

本项目使用标准库 `unittest`（不是 pytest），需在仓库根目录执行，以便 `src.` 包可导入：

```bash
# 运行所有测试（单元 + 集成）
python -m unittest discover -s tests -t .

# 运行特定测试模块
python -m unittest tests.core.test_config

# 带覆盖率（含分支）
coverage run -m unittest discover -s tests -t . && coverage report -m
```

### 覆盖率

| 指标 | 数值 |
|---|---|
| 语句覆盖 | **100%** |
| 分支覆盖 | **99.76%** |
| 用例总数 | **1112** 项 |

门槛配置在 `pyproject.toml` 的 `[tool.coverage.report] fail_under = 99`，
低于该值 `coverage report` 会返回非零。

**未覆盖的 24 个分支**都是「依赖缺失 / 单平台 / 双重失败」三类，
在源码中以 `# pragma: no cover` 标注并写明理由，
完整清单见 **[docs/testing.md](docs/testing.md)**。

### 真实数据库集成测试

默认跳过。要跑真库就指定连接名：

```bash
# PostgreSQL
DCT_IT_DB=db_kes python -m unittest tests.test_integration_full.TestRealDatabase -v

# GBase 8s
DCT_IT_DB=db_gb python -m unittest tests.test_integration_full.TestRealDatabase -v
```

连接不可用时整类跳过（不判失败）——环境问题不该让测试红。

详见 [docs/testing.md](docs/testing.md)：分层设计、写测试的约定（含几个真实踩坑记录）、
回归守卫清单、已知局限。

### 代码质量

```bash
# 代码检查
pylint src/db_connector_tool/

# 类型检查
pyright src/db_connector_tool/
```

## 📚 相关项目

- [SQLAlchemy](https://www.sqlalchemy.org/) - Python SQL 工具包和 ORM
- [cryptography](https://cryptography.io/) - Python 加密库
- [tomli-w](https://github.com/hukkin/tomli-w) - TOML 序列化库

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE.txt](LICENSE.txt) 文件。

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
