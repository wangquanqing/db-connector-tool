# DB Connector Tool 使用教程

本教程将详细介绍 DB Connector Tool 的所有公共方法，通过实际示例展示如何使用这个强大的数据库连接管理工具。

## 📚 目录

1. [快速开始](#快速开始)
2. [核心管理类](#核心管理类)
   - [DatabaseManager](#databasemanager)
   - [BatchDatabaseManager](#batchdatabasemanager)
   - [ConfigManager](#configmanager)
   - [CryptoManager](#cryptomanager)
   - [KeyManager](#keymanager)
   - [SQLAlchemyDriver](#sqlalchemydriver)
   - [LogManager](#logmanager)
3. [异常处理](#异常处理)
4. [工具函数](#工具函数)
   - [查询超时](#查询超时)
   - [SQL 切分器（方言感知）](#sql-切分器方言感知)
   - [查询结果（保真）](#查询结果保真)
   - [路径工具](#路径工具)
   - [日志工具](#日志工具)
5. [命令行工具](#命令行工具)
6. [最佳实践](#最佳实践)
7. [故障排除](#故障排除)
8. [获取帮助](#获取帮助)

## 🚀 快速开始

### 安装

```bash
# 从源码安装（推荐装全部驱动）
git clone https://github.com/wangquanqing/db-connector-tool.git
cd db-connector-tool
pip install -e ".[all]"

# 或从 PyPI 安装
pip install db-connector-tool            # 仅核心 + SQLite
pip install "db-connector-tool[all]"     # 全部数据库驱动
pip install "db-connector-tool[mysql]"   # 按需：secure/oracle/postgresql/mysql/sqlserver/gbase8s
```

> ⚠️ 除 SQLite 外，各数据库驱动都是可选依赖（extras），只装主包无法连接
> Oracle/PostgreSQL/MySQL/SQL Server/GBase 8s；`secure` extra 安装 keyring，
> 可把主密钥存入操作系统密钥环（推荐）。

### 基础示例

```python
from db_connector_tool import DatabaseManager

db = DatabaseManager()

# 添加MySQL连接
db.add_connection("mysql_db", {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "user",
    "password": "password",
    "database": "test_db",
})

# 执行查询
results = db.execute_query("mysql_db", "SELECT * FROM users")

# 执行增删改
affected = db.execute_command(
    "mysql_db",
    "UPDATE users SET name = :name WHERE id = :id",
    {"name": "张三", "id": 1},
)

# 关闭连接
db.close_all_connections()
```

## 🔧 核心管理类

### DatabaseManager

DatabaseManager 是主要的数据库连接管理类，提供完整的连接生命周期管理。

#### 初始化

```python
from db_connector_tool import DatabaseManager

# 使用默认配置
db = DatabaseManager()

# 自定义应用名称和配置文件
db = DatabaseManager(
    app_name="my_application",
    config_file="database.toml",
)
```

#### 添加连接配置

```python
# MySQL连接
db.add_connection("mysql_db", {
    "type": "mysql",
    "host": "localhost",
    "port": 3306,
    "username": "user",
    "password": "password",
    "database": "test_db",
    "charset": "utf8mb4",
})

# PostgreSQL连接
db.add_connection("postgres_db", {
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "username": "user",
    "password": "password",
    "database": "test_db",
})

# Oracle连接
db.add_connection("oracle_db", {
    "type": "oracle",
    "host": "localhost",
    "port": 1521,
    "username": "user",
    "password": "password",
    "service_name": "ORCL",
})

# SQLite连接
db.add_connection("sqlite_db", {
    "type": "sqlite",
    "database": "/path/to/database.db",
})

# SQL Server连接
db.add_connection("mssql_db", {
    "type": "sqlserver",
    "host": "localhost",
    "port": 1433,
    "username": "sa",
    "password": "password",
    "database": "test_db",
})

# GBase 8s连接（JDBC，驱动 jar 会自动获取，详见 docs/gbase8s-dialect-design.md）
db.add_connection("gb8s_db", {
    "type": "gbase8s",
    "host": "127.0.0.1",
    "port": 9088,
    "server": "gbase01",      # GBASEDBTSERVER 实例名
    "database": "db_station",
    "username": "cvicse",
    "password": "password",
    "sqlmode": "oracle",      # oracle / gbase / mysql，默认 oracle
    # "jars": r"C:\path\to\gbasedbt-jdbc.jar",   # 显式指定时优先使用
})
```

GBase 8s 的一库三模会影响分页与伪表写法：`oracle` 模式用 `ROWNUM`/`DUAL`，
`gbase` 模式用 `FIRST n SKIP m`/`sysmaster:sysdual`，`mysql` 模式用 `LIMIT ... OFFSET ...`。

#### GBase 8s 驱动 jar 的获取

按「连接配置 `jars` → 环境变量 `GBASEDBT_JDBC_JAR`/`GBASE8S_JDBC_JAR` →
**只搜索两个目录**：程序运行目录 `./jars`、用户配置目录 `jars`
（`%APPDATA%\db_connector_tool\jars`）」的顺序查找；**两个目录都没有时自动从
Maven Central 下载最新版**（`com.gbasedbt:jdbc`）到用户配置目录下的 `jars` 并直接使用。

> 需要把 jar 放在别处时，用连接配置的 `jars` 参数或上面两个环境变量显式指定，
> 它们优先于目录搜索。

```bash
# 关闭自动下载（例如内网环境自行分发 jar）
set GBASEDBT_JDBC_AUTO_DOWNLOAD=0

# 换用镜像/私服，或指向本地目录/ file:// 路径实现离线分发
set GBASEDBT_JDBC_MAVEN_REPO=https://mirror.example.com/maven

# 调整超时（秒）与大小上限（字节）
set GBASEDBT_JDBC_DOWNLOAD_TIMEOUT=30
set GBASEDBT_JDBC_DOWNLOAD_MAX_SIZE=41943040

# 关闭 JVM 原生访问授权注入（仅在因此启动失败时需要）
set GBASEDBT_JDBC_NATIVE_ACCESS=0
```

> **JDK 24+ 的 JNI 警告**：连接时若看到
> `WARNING: A restricted method in java.lang.System has been called`，那是 JPype 启动 JVM
> 时调用受限的 `System::load`（[JEP 472](https://openjdk.org/jeps/472)）所致。本工具会在
> 启动 JVM 前自动补上 `--enable-native-access=ALL-UNNAMED`，无需手工处理。

下载会校验官方 `.sha1` 与 ZIP 文件头，先写临时文件再原子替换；失败只在日志中告警，
不影响 `CLASSPATH` 方案。同一目录存在多个版本时按版本号取最高者。

> **默认包优先于 `_nobson` 变体**：厂商对同一版本会同时发布默认包与 `_nobson` 包，
> 后者剥掉了内置的 `org.bson.*`（供自备 mongo-java-driver 的场景使用），而
> `maven-metadata.xml` 的 `<release>` 恰好指向 `_nobson`。本工具不引入 mongo 驱动，
> 因此**同版本存在默认包时一律选默认包**；本地只装了旧 `_nobson` 包时会自动补下默认包
> （否则遇到 BSON/JSON 列会抛 `NoClassDefFoundError`）。确需变体时置
> `GBASEDBT_JDBC_ALLOW_NOBSON=1`。

#### 执行查询与命令

SQL 参数统一使用 **命名参数** 风格（`:name` 占位符 + 字典传参）：

```python
# 执行SELECT查询
results = db.execute_query(
    "mysql_db",
    "SELECT * FROM users WHERE age > :age",
    {"age": 18},
)

# 执行INSERT/UPDATE/DELETE命令（返回 (影响行数, 耗时秒数)）
affected, elapsed = db.execute_command(
    "mysql_db",
    "INSERT INTO users (name, email) VALUES (:name, :email)",
    {"name": "张三", "email": "zhangsan@example.com"},
)
print(f"影响 {affected} 行，耗时 {elapsed:.3f}s")

# 事务操作
# execute_query / execute_command 各自独立提交事务；
# 需要「多条语句同一事务」时，可直接使用驱动暴露的 SQLAlchemy 引擎：
from sqlalchemy import text

driver = db.get_connection("mysql_db")
with driver.engine.begin() as conn:  # 退出时提交，异常自动回滚
    conn.execute(text("UPDATE accounts SET balance = balance - 100 WHERE id = 1"))
    conn.execute(text("UPDATE accounts SET balance = balance + 100 WHERE id = 2"))
```

#### 连接管理

```python
# 测试连接
if db.test_connection("mysql_db"):
    print("连接正常")
else:
    print("连接失败")

# 列出所有连接
connections = db.list_connections()
print("可用连接:", connections)

# 获取连接信息（含统计信息）
info = db.get_connection_info("mysql_db")
print("连接信息:", info)

# 关闭所有连接
db.close_all_connections()

# 删除连接配置
db.remove_connection("mysql_db")
```

### BatchDatabaseManager

BatchDatabaseManager 用于管理大量配置相似的数据库连接，支持批量操作。

#### 初始化

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
```

#### 批量添加连接

```python
# 生成IP地址范围
ip_list = generate_ip_range("192.168.1.100", 10)
print("IP范围:", ip_list)

# 批量添加连接（返回 {IP: 是否成功}）
results = batch.add_batch_connections(ip_list)
print("批量添加结果:", results)

# 检查成功和失败的连接
successful = [ip for ip, ok in results.items() if ok]
failed = [ip for ip, ok in results.items() if not ok]
print(f"成功: {len(successful)}, 失败: {len(failed)}")
```

#### 批量查询操作

```python
# 批量执行查询（返回 {连接名: {"success", "data", "count", "elapsed"}}）
query_results = batch.execute_batch_query("SELECT COUNT(*) FROM users")

for conn_name, result in query_results.items():
    if result["success"]:
        print(f"{conn_name}: {result['count']} 行, {result['elapsed']:.3f}s")
    else:
        print(f"{conn_name}: 查询失败 - {result['error']}")

# 带参数的批量查询
param_results = batch.execute_batch_query(
    "SELECT * FROM users WHERE age > :age",
    params={"age": 18},
)
```

#### 表结构升级

```python
# 批量升级表结构（参数为 SQL 列表，支持回滚）
results = batch.upgrade_table_structure(
    ["ALTER TABLE users ADD COLUMN last_login TIMESTAMP DEFAULT CURRENT_TIMESTAMP"],
    ["ALTER TABLE users DROP COLUMN last_login"],  # 回滚SQL（可选）
)

for conn_name, result in results.items():
    if result["success"]:
        print(f"{conn_name}: 表结构升级成功")
    else:
        print(f"{conn_name}: 升级失败 - {result['error']}")
```

#### 清理资源

```python
# 清理批量管理器创建的临时配置
batch.cleanup()

# 或者使用工具函数清理所有临时配置
from db_connector_tool import cleanup_temp_configs
cleanup_temp_configs()
```

### ConfigManager

ConfigManager 负责配置文件的加密存储和管理。

#### 基本操作

```python
from db_connector_tool import ConfigManager

cm = ConfigManager("my_app")

# 添加连接配置
cm.add_config("db1", {
    "type": "mysql",
    "host": "localhost",
    "username": "user",
    "password": "secret",
})

# 获取连接配置
saved = cm.get_config("db1")
print("保存的配置:", saved)

# 列出所有连接
connections = cm.list_configs()
print("所有连接:", connections)

# 删除连接配置
cm.remove_config("db1")

# 备份配置文件
backup_path = cm.backup_config()
print("备份路径:", backup_path)
```

#### 配置验证

```python
# 验证配置格式
from db_connector_tool.core.validators import ConfigValidator

try:
    ConfigValidator.validate_config({
        "version": "1.0.0",
        "app_name": "test",
        "connections": {},
        "metadata": {
            "created": "2025-01-01T00:00:00+08:00",
            "last_modified": "2025-01-01T00:00:00+08:00",
            "config_file": "",
            "key_version": "1",
            "signature": "",
            "audit_log": [],
        },
    })
    print("配置格式正确")
except Exception as error:
    print("配置格式错误:", error)
```

### CryptoManager

CryptoManager 提供数据加密解密功能。

#### 基本使用

```python
from db_connector_tool import CryptoManager

crypto = CryptoManager()

# 加密字符串数据
encrypted = crypto.encrypt("这是敏感数据")
print("加密结果:", encrypted)

# 解密数据
decrypted = crypto.decrypt(encrypted)
print("解密结果:", decrypted)

# 加密字节数据
encrypted_binary = crypto.encrypt_bytes(b"binary sensitive data")

# 解密字节数据
decrypted_binary = crypto.decrypt_bytes(encrypted_binary)
```

#### 密钥管理

```python
# 获取密钥信息（用于持久化恢复）
key_info = crypto.get_key_info()

# 从保存的密钥恢复加密管理器
from_saved = CryptoManager.from_saved_key(
    key_info["password"],
    key_info["salt"],
    key_info["iterations"],
)

# 验证恢复的加密器
test_data = "测试数据"
encrypted_test = from_saved.encrypt(test_data)
decrypted_test = from_saved.decrypt(encrypted_test)
assert test_data == decrypted_test
print("密钥恢复验证成功")
```

### KeyManager

KeyManager 负责主加密密钥的**存储与轮换**，按「操作系统密钥环 → 环境变量 →
文件」的优先级选择方案；某一层不可用（例如密钥环服务未启动、凭据库拒绝写入）
时会自动降级到下一层，并在日志中给出告警，不会直接失败。

#### 加载与使用密钥

```python
from db_connector_tool import KeyManager

key_manager = KeyManager("my_app")

# 加载已有密钥，不存在则创建
key_manager.load_or_create_key()

# 取得加密器（ConfigManager 内部同样通过它加解密配置）
crypto = key_manager.get_crypto_manager()
encrypted = crypto.encrypt("敏感数据")

# 关闭并清理内存中的密钥材料
key_manager.close()
```

#### 轮换密钥

```python
key_manager = KeyManager("my_app")
key_manager.load_or_create_key()

# 生成新密钥并按同样的优先级保存；已有配置需要随后重新加密
new_key_info = key_manager.rotate_key()
print("新密钥字段:", sorted(new_key_info))
key_manager.close()
```

> 配置文件的密钥轮换请优先使用 `ConfigManager.rotate_encryption_key()`：
> 它会先备份配置、解密全部连接、换钥后重新加密，失败时自动回滚。

#### 派生 HMAC 密钥

```python
from db_connector_tool import KeyManager

key_manager = KeyManager("my_app")
key_manager.load_or_create_key()

# 32 字节，用于配置签名等场景（由主密钥派生，不落盘）
hmac_key = key_manager.get_secure_hmac_key()
print(len(hmac_key))  # 32
key_manager.close()
```

环境变量方案：`DB_CONNECTOR_TOOL_ENCRYPTION_KEY`（JSON，含 password/salt/iterations）
与 `DB_CONNECTOR_TOOL_HMAC_KEY`（32 字节十六进制）。

### SQLAlchemyDriver

需要绕开配置管理层、直接操作单个数据库连接时，可单独使用驱动。

#### 直接连接与执行

```python
from db_connector_tool import SQLAlchemyDriver

config = {
    "type": "mysql",          # oracle/postgresql/mysql/sqlserver/sqlite/gbase8s
    "host": "localhost",
    "port": 3306,
    "username": "user",
    "password": "password",
    "database": "test_db",
}

driver = SQLAlchemyDriver(config)

# 上下文管理器：进入时连接，退出时自动断开
with driver:
    rows = driver.execute_query(
        "SELECT * FROM users WHERE age > :age", {"age": 18}
    )                       # -> List[Dict[str, Any]]
    affected = driver.execute_command(
        "UPDATE users SET status = :s WHERE id = :id", {"s": "active", "id": 1}
    )                       # -> int，受影响行数（自动提交）
    print(len(rows), affected)
```

#### 连接池与探测

```python
driver = SQLAlchemyDriver(config)

# 建立连接（内部使用 SQLAlchemy 连接池，pool_pre_ping=True）
driver.connect()

# 连通性探测：任何异常都返回 False，不抛出
print(driver.test_connection())

# 自定义连接池参数（可选）
driver = SQLAlchemyDriver({**config, "pool_config": {"pool_size": 10, "max_overflow": 20}})

driver.disconnect()
```

#### 元数据反射

```python
driver.connect()

# 表名列表
tables = driver.get_tables()

# 列定义：name/type/nullable/default
schema = driver.get_table_schema("users")
for column in schema:
    print(column["name"], column["type"], column["nullable"])

driver.disconnect()
```

> 传入的 SQL 会经过安全校验：参数化查询、注入特征检测、拒绝多条语句；
> 显式的 `DROP`/`TRUNCATE`/`CREATE`/`ALTER` 等语句正常执行。
> 详见 README「SQL 执行安全」。

### LogManager

LogManager 是日志系统的面向对象封装，适合在应用里集中管理 handler。

```python
from db_connector_tool import LogManager

# 一步完成初始化
log_manager = LogManager.quick_setup("my_app", "DEBUG")

# 追加自定义文件 handler（支持按大小或按时间轮转）
log_manager.add_file_handler("debug.log", level="DEBUG", max_size=5 * 1024 * 1024)
log_manager.add_file_handler("hourly.log", when="H", backup_count=24)

# 查看当前所有 logger 的级别、handler 数量与传播设置
for name, info in log_manager.get_loggers_info().items():
    if name.startswith("my_app"):
        print(name, info)

# 移除单个 handler / 清理全部 handler（会关闭文件句柄）
log_manager.cleanup()
```

底层函数式接口同样可用：

```python
from db_connector_tool import get_logger, set_log_level, setup_logging

logger = setup_logging(
    app_name="my_app", level="INFO", log_to_console=True, log_to_file=True
)
set_log_level("my_app", "DEBUG")   # 需要指定 logger 名称
get_logger(__name__).info("模块初始化完成")
```

## ⚠️ 异常处理

DB Connector Tool 提供了完整的异常处理体系。

### 基础异常处理

```python
from db_connector_tool.core.exceptions import (
    DBConnectorError, ConfigError, CryptoError, DatabaseError,
    DBConnectionError, DriverError, QueryError, DBTimeoutError,
)

try:
    db.execute_query("nonexistent_db", "SELECT 1")
except DBConnectionError as error:
    print(f"连接错误: {error}")
    print(f"错误详情: {error.to_dict()}")
except QueryError as error:
    print(f"查询错误: {error}")
except DBConnectorError as error:
    print(f"数据库连接器错误: {error}")
except Exception as error:
    print(f"未知错误: {error}")
```

### 配置异常处理

```python
try:
    cm.add_config("", {})  # 空名称会触发异常
except ConfigError as error:
    print(f"配置错误: {error}")
    print(f"错误代码: {error.error_code}")
    print(f"详细信息: {error.details}")
```

### 加密异常处理

```python
from cryptography.fernet import InvalidToken

try:
    crypto.decrypt("invalid_encrypted_data")
except InvalidToken:
    print("加密令牌无效，可能被篡改")
except CryptoError as error:
    print(f"加密错误: {error}")
```

## 🛠️ 工具函数

### 查询超时

给一次查询加**时间上限**，超时立刻中断。适用于「报表跑太久」「锁等待卡死」
这类场景 —— 没有超时的话，查询会一直挂着直到数据库自己算完。

```python
from db_connector_tool import DatabaseManager, DBTimeoutError

db = DatabaseManager("my_app")

try:
    result = db.execute_query_result(
        "db_kes", "SELECT * FROM huge_table", timeout=30
    )
except DBTimeoutError as error:
    print(f"超时 {error.timeout_seconds} 秒")
    print(f"操作: {error.operation}")
    print(f"错误码: {error.error_code}")     # TIMEOUT_001
```

除 `execute_query_result` 外，`execute_query` / `execute_command` /
`execute_query_stream` / `execute_query_with_metadata` 都支持 `timeout`。

#### 各方言的支持情况

| 方言 | 实现机制 | 查询级超时 |
|---|---|---|
| PostgreSQL | `SET LOCAL statement_timeout` | ✅ 事务内有效，自动失效 |
| MySQL | `SET SESSION max_execution_time` | ✅ 用完自动复位 |
| MariaDB | `SET SESSION max_statement_time` | ✅ |
| SQLite | `set_progress_handler` | ✅ 按指令数回调 |
| Oracle | `connection.call_timeout` | ✅ 客户端 OCI 层 |
| GBase 8s | —（JDBC 无法安全中断） | ❌ 明确报错 |
| SQL Server | —（无语句级机制） | ❌ 明确报错 |

不支持的方言会抛 `TimeoutNotSupportedError`（继承 `QueryError`），
文案里说明原因和替代方案：

```python
from db_connector_tool.drivers.timeout_control import (
    TimeoutNotSupportedError, supports_query_timeout,
)

supports_query_timeout("postgresql")   # True
supports_query_timeout("gbase8s")      # False

try:
    db.execute_query_result("db_gb", "SELECT ...", timeout=5)
except TimeoutNotSupportedError as error:
    print(error)   # 该连接（gbase8s）不支持查询级超时：...
```

#### 为什么 GBase 8s / SQL Server 做不到

这不是实现偷懒，而是**驱动的硬限制**，实测确认：

- **GBase 8s**：JDBC 调用不可重入。从另一个线程调 `invalidate()` 会让
  **整个进程挂死**（连 `thread.join()` 都返回不了）。
- **SQL Server**：pymssql 只有**连接级** `timeout`，没有语句级机制；
  `invalidate()` 行为不确定，不敢用。

SQL Server 可在连接配置里设统一上限：

```python
{
    "type": "sqlserver",
    "pool_config": {"connect_args": {"timeout": 30}},   # 所有查询统一 30 秒
}
```

#### 已知限制

- **MySQL 的 `max_execution_time` 不覆盖 `SLEEP()`**
  （MySQL 文档明示），`SELECT SLEEP(100)` 不会被超时打断。
- 超时靠**服务端**杀语句时，客户端一定能返回；若退回客户端看门狗，
  连接会被作废（这是必要代价，也比卡死好）。
- 超时后连接状态：服务端机制下连接可继续用（`SET LOCAL` 自动失效、
  会话级设置会被复位）；看门狗机制下连接已作废，会从池中重建。

### Ctrl+C 即时取消查询

超时是「到点自动停」，取消是「用户想停就停」。两者互补。

```python
from db_connector_tool import DatabaseManager, QueryCancelledError

db = DatabaseManager("my_app")
try:
    result = db.execute_query_result_cancellable(
        "db_kes", "SELECT * FROM huge_table"
    )
except QueryCancelledError:
    print("用户取消了查询")
```

命令行默认就支持，按 Ctrl+C 立即中断（退出码 130）：

```bash
db-connector query db_kes "SELECT * FROM huge_table"
# 运行中按 Ctrl+C
# ⏹️  查询已被用户取消
```

不想启用可加 `--no-cancel`。

#### 各方言的取消机制

| 方言 | 机制 | 实测耗时 | 连接可复用 |
|---|---|---|---|
| PostgreSQL | `dbapi.cancel()` | 1.51s | ✅ |
| MySQL / MariaDB | 另开连接 `KILL <id>` | 1.58s | ✅ |
| SQLite | `dbapi.interrupt()` | 1.50s | ✅ |
| GBase 8s | — | ❌ 立即报错 | — |
| SQL Server | — | ❌ 立即报错 | — |

#### 为什么必须把查询放到工作线程

这是实现里最关键、也最容易踩错的一点。

Python 的信号处理器只在**字节码边界**执行。当驱动在 C 扩展里阻塞时：

- 若该扩展**释放 GIL**（psycopg 会），信号能及时送达；
- 若**不释放 GIL**（pymysql / pymssql / sqlite3 大多如此），信号会被
  推迟到查询返回之后 —— 此时再取消已经毫无意义。

实测（1.5s 时发 SIGINT，看 handler 何时执行）：

| 方言 | 主线程直接执行 | 工作线程执行 |
|---|---|---|
| PostgreSQL | 1.51s ✅ | 1.90s ✅ |
| SQLite | **8.30s** ❌ | 1.90s ✅ |

所以本实现一律把查询放到工作线程，主线程只做**短间隔轮询**
（`stop.wait(0.1)`），保证每次都能返回解释器处理信号。

```python
from db_connector_tool.drivers.query_cancel import (
    run_cancellable, cancel_query, supports_cancel,
)

supports_cancel("postgresql")   # True
supports_cancel("gbase8s")      # False

# 手动用：查询放 worker，主线程可被打断
value, cancelled = run_cancellable(
    lambda: do_query(),
    lambda: cancel_query(connection, engine),
)
```

#### 为什么 GBase 8s / SQL Server 不能取消

**这是驱动的硬限制，实测确认，不是没实现**：

- **GBase 8s**：JDBC 调用不可重入。从另一线程调 `jconn.abort()` 会让
  **整个进程挂死**（连 `thread.join()` 都返回不了）。
- **SQL Server**：`pymssql` 没有 `cancel()`；另开连接执行 `KILL <spid>`
  被服务端拒绝（错误 6115：KILL command cannot be used inside user transactions）；
  调 `close()` 同样会挂死。

两者都会抛 `QueryCancelledError` 并说明「查询会在后台继续跑完」，
**不会假装成功**。

#### 取消后的连接状态

- 取消成功时，连接可能停在事务中间，因此该连接会被关闭并归还连接池，
  下次查询自动重建；
- 实测 PostgreSQL / MySQL / SQLite 取消后连接仍可正常复用。

### SQL 切分器（方言感知）


把一段脚本切成可逐条执行的语句。**不是简单的 `split(";")`** ——
它理解字符串、注释、引用体与**过程体边界**，因此 GBase SPL、
MySQL `BEGIN...END`、SQL Server `GO` 都能正确切分。

```python
from db_connector_tool.core.sql_splitter import split_sql

# 基础用法（不传方言 = 保守通用档）
for stmt in split_sql("SELECT 1; SELECT 2;"):
    print(stmt.text)        # 'SELECT 1' / 'SELECT 2'
    print(stmt.line)        # 行号，便于报错定位

# 指定方言，启用该方言的边界识别
sql = """
CREATE PROCEDURE p()
    DEFINE i INT;
    LET i = 1;
END PROCEDURE;
SELECT FIRST 10 * FROM t;
"""
stmts = split_sql(sql, "gbase8s")
assert len(stmts) == 2        # 过程体整体一条，不会按内部分号切碎
```

支持的方言（含别名）：

| 方言 | 传入值 | 特有能力 |
|---|---|---|
| GBase 8s | `gbase8s` / `gbase` / `informix` | `END PROCEDURE` 整句终结 |
| Oracle | `oracle` / `ora` | `q'[...]'` 引用、独占一行的 `/` 终结符 |
| MySQL | `mysql` / `mariadb` | `#` 行注释、反斜杠转义 |
| PostgreSQL | `postgresql` / `postgres` / `pg` | `$$..$$` / `$tag$..$tag$`、嵌套块注释 |
| SQL Server | `sqlserver` / `mssql` / `tsql` | `[...]` 标识符、独占一行的 `GO` |
| SQLite | `sqlite` / `sqlite3` | — |
| 通用 | `None` / 未知值 | 只认标准 SQL 形态 |

**设计原则：宁可少切，绝不多切。** 多切会执行半条语句（可能造成破坏），
少切只会让数据库报语法错误。因此未闭合时会标记：

```python
stmt = split_sql("SELECT 'unclosed", None)[0]
print(stmt.is_complete)     # False —— 脚本似乎不完整
```

> 为什么不直接用 `sqlparse` / `sqlglot`：实测 sqlparse 会把 GBase SPL 切成 5 条，
> sqlglot 对 MySQL 过程体只切出 1 条、对 `GO` 直接报错。
> 对比数据见仓库外 `ANALYSIS-sqlglot-evaluation.md`。

### 查询结果（保真）

查询结果默认按**位置**存放，避免重名列互相覆盖
（`SELECT * FROM a JOIN b` 的常态）。

```python
from db_connector_tool import DatabaseManager

db = DatabaseManager("my_app")
result = db.execute_query_result("mysql_db", "SELECT * FROM emp JOIN dept USING (id)")

# 列元数据：ordinal 是列身份，display_name 是去重后的显示名
for col in result.columns:
    print(col.ordinal, col.name, col.display_name, col.python_type)

# 按位置取值 —— 重名列也安全
first_row = result.rows[0]
print(first_row[0], first_row[1])

# 转字典（键为唯一显示名，不丢列）
print(result.as_dicts())
# [{'id': 1, 'name': '张三', 'id (1)': 10, 'name (1)': '研发部'}]
```

流式取数（大表不爆内存）：

```python
for chunk in db.execute_query_stream("mysql_db", "SELECT * FROM big_table",
                                     fetch_size=1000):
    for row in chunk.rows:
        process(row)
    if chunk.is_last:
        break
```

> **为什么不能直接 `dict(row)`**：`RowMapping` 内部按位置保真，
> 但 `dict()` 会让同名列互相覆盖 —— 实测曾把员工姓名「张三」
> 覆盖成部门名「研发部」。

### 路径工具

```python
from db_connector_tool.utils.path_utils import PathHelper

# 获取用户配置目录
config_dir = PathHelper.get_user_config_dir("my_app")
print("配置目录:", config_dir)

# 创建目录（如果不存在）
PathHelper.ensure_dir_exists(config_dir)

# 安全路径拼接
safe_path = PathHelper.safe_join("/base", "subdir", "file.txt")
```

### 日志工具

```python
from db_connector_tool.utils.logging_utils import (
    setup_logging, get_logger, set_log_level,
)

# 初始化日志系统
logger = setup_logging(
    app_name="my_app",
    level="DEBUG",
    log_to_console=True,
    log_to_file=True,
)

# 获取模块日志器
module_logger = get_logger(__name__)
module_logger.info("模块初始化完成")

# 动态调整日志级别（logger 名称 + 级别）
set_log_level("my_app", "INFO")

# 记录不同级别的日志（使用惰性格式化）
module_logger.debug("调试信息: %s", "详情")
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
# ['192.168.1.100', '192.168.1.101', '192.168.1.102', '192.168.1.103', '192.168.1.104']

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

# 执行命令
db-connector command mysql-dev "INSERT INTO users (name) VALUES ('John')"

# 进入交互式SQL Shell
db-connector shell mysql-dev

# 查看连接详情
db-connector show mysql-dev

# 更新连接
db-connector update mysql-dev --host new_host --port 3307

# 删除连接
db-connector remove mysql-dev
```

### 高级用法

```bash
# 从文件执行 SQL（遇错可继续）
db-connector file mysql-dev init.sql --continue-on-error

# 导出查询结果为 JSON
db-connector query mysql-dev "SELECT * FROM users" --output users.json

# 导出查询结果为 CSV
db-connector query mysql-dev "SELECT * FROM users" --output users.csv
```

## 🏆 最佳实践

### 1. 连接管理最佳实践

```python
# 使用上下文管理器确保连接正确关闭
with DatabaseManager() as db:
    db.add_connection("db1", config)
    results = db.execute_query("db1", "SELECT 1")
    # 连接会在退出时自动关闭

# 或者显式管理连接生命周期
try:
    db = DatabaseManager()
    results = db.execute_query("db1", "SELECT 1")
finally:
    db.close_all_connections()
```

### 2. 错误处理最佳实践

```python
def safe_query(db_name, query, params=None):
    """安全的数据库查询函数"""
    db = DatabaseManager()
    try:
        if not db.test_connection(db_name):
            raise DBConnectionError(f"数据库 {db_name} 连接失败")
        return db.execute_query(db_name, query, params)
    except QueryError as error:
        logger.error("查询执行失败: %s", error)
        return []
    except DBConnectionError as error:
        logger.error("连接错误: %s", error)
        raise
    except Exception as error:
        logger.error("未知错误: %s", error)
        raise
    finally:
        db.close_all_connections()
```

### 3. 批量操作最佳实践

```python
def batch_maintenance():
    """批量数据库维护操作"""
    batch = BatchDatabaseManager("maintenance")

    try:
        batch.set_base_config({
            "type": "mysql",
            "port": 3306,
            "username": "admin",
            "password": "secure_password",
            "database": "app_db",
        })

        maintenance_ips = generate_ip_range("192.168.1.100", 50)
        add_results = batch.add_batch_connections(maintenance_ips)

        successful = [ip for ip, ok in add_results.items() if ok]
        if successful:
            results = batch.execute_batch_query("OPTIMIZE TABLE important_table")
            for conn_name, result in results.items():
                if result["success"]:
                    logger.info("%s: 维护成功", conn_name)
                else:
                    logger.warning("%s: 维护失败: %s", conn_name, result["error"])
        return successful
    except Exception as error:
        logger.error("批量维护失败: %s", error)
        raise
    finally:
        batch.cleanup()
```

### 4. 安全最佳实践

```python
def secure_configuration():
    """安全配置管理"""
    cm = ConfigManager("my_secure_app")

    # 敏感配置自动加密存储
    cm.add_config("secure_db", {
        "type": "mysql",
        "host": "db.example.com",
        "username": "app_user",
        "password": "very_secure_password_123",
        "database": "sensitive_data",
    })

    # 定期备份配置
    cm.backup_config()

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
   - 检查网络连接和防火墙设置
   - 验证数据库服务是否运行
   - 检查用户名密码是否正确

2. **加密错误**
   - 检查密钥文件权限
   - 验证加密密钥是否损坏
   - 尝试重新生成密钥

3. **配置问题**
   - 检查配置文件格式
   - 验证配置字段是否完整
   - 查看详细错误日志

4. **Windows 控制台下中文/图标显示异常**
   - **1.5.1 起已自动处理**，无需手动设置。CLI 会自动把打印不出的
     emoji 降级为空白，中文保持正常。
   - 如果你用的是 **1.5.0 或更早**，可能出现
     `UnicodeEncodeError: 'gbk' codec can't encode character` 直接崩溃
     （Windows 中文版控制台默认 GBK）。升级到 1.5.1+ 即可。
   - 若仍想看完整 emoji，可临时设 `set PYTHONUTF8=1`（会影响该窗口所有程序），
     或改用 Windows Terminal。

5. **SQL Server 里中文/emoji 存不进去**
   - **列必须是 `NVARCHAR` 而不是 `VARCHAR`**：`VARCHAR` 是非 Unicode 类型，
     按服务器排序规则编码，中文可能丢失（实测存成 `??èy`）。
   - 连接配置的 `charset` 保持默认 `utf8`（1.5.0 起已修正，早期是 `cp936`
     导致 emoji / 德文变音直接抛 `UnicodeEncodeError`）。

### 调试技巧

```python
# 启用详细日志
import logging
logging.basicConfig(level=logging.DEBUG)

# 使用调试模式
from db_connector_tool.utils.logging_utils import setup_logging
logger = setup_logging(level="DEBUG")

# 检查连接状态
if db.test_connection("db_name"):
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
