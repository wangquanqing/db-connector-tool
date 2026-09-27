# GBase 8s（JayDeBeApi + JDBC）SQLAlchemy 方言设计说明

## 1. 目标与范围

把现有单文件方言 `src/db_connector_tool/drivers/gbase8s_jdbc.py` 替换为**按 SQLAlchemy 官方 Oracle 方言的文件组织形式**拆分的方言包，
连接通道为 **JayDeBeApi + GBase JDBC 驱动**，以获得比 `gbase8sdb`（≥ V8.8_3.6.2）和 `DbtPy`（< V8.8_3.6.2）原生驱动更宽的版本兼容性。

范围之外：不改动 `SQLAlchemyDriver` 的连接池/校验逻辑；不改动 3 个根目录临时脚本；不引入新的第三方依赖（`jaydebeapi`、`python-dateutil` 已在 `pyproject.toml` 中声明）。

## 2. 已确认的设计决策（用户对齐结果）

| 编号 | 决策 | 结论 |
| --- | --- | --- |
| D1 | 包组织形式 | 新建 `drivers/gbase8s/` 包，同步修改 `pyproject.toml` 的 entry point 与 `tests/` 的导入路径 |
| D2 | 反射深度 | 完整系统表反射：表名、视图、列、主键、唯一约束、外键、索引、检查约束、注释、视图定义、序列 |
| D3 | 缺陷修正 | ① `do_rollback` 改为真实回滚；② 关闭 RETURNING；③ 统一 URL 前缀与注册名为 `jdbcgbase8s` |
| D4 | 验收方式 | 离线验收：`unittest` 全绿 + SQL 编译快照 + 导入级验证；不连真实库 |

## 3. 包结构与文件职责

```
src/db_connector_tool/drivers/gbase8s/
├── __init__.py      # 注册方言 + 公共导出（触发 base/jaydebeapi 的导入顺序）
├── base.py          # DBAPI 无关层：colspecs、类型编译、SQL 编译、DDL 编译、标识符准备、GBase8sDialect（版本/schema/隔离级别/反射）
├── jaydebeapi.py    # JayDeBeApi 适配层：GBase8sCursor、GBase8sDialect_jaydebeapi（create_connect_args / jar 查找 / is_disconnect / do_rollback / import_dbapi）
├── types.py         # GBase 8s 类型：ischema_names（数字键）、ObTimestamp、LVARCHAR、SERIAL、SERIAL8
├── dictionary.py    # 系统目录 SQL 常量与类型码常量（systables/syscolumns/...）
└── provision.py     # 验收与环境自检辅助：jar 自检、连通性 SQL、建/删测试表 DDL
```

分层原则（对齐 Oracle 方言）：`base.py` 不感知 DBAPI；`jaydebeapi.py` 只处理「连接怎么建立、取值怎么转换、异常怎么识别」。

## 4. 类命名与注册

| 名称 | 位置 | 说明 |
| --- | --- | --- |
| `GBase8sDialect` | `base.py` | DBAPI 无关方言类，继承 `OracleDialect`（`from sqlalchemy.dialects.oracle.base import OracleDialect`） |
| `GBase8sDialect_jaydebeapi` | `jaydebeapi.py` | 注册目标（类比 Oracle 的 `OracleDialect_cx_oracle`）；`dialect = GBase8sDialect_jaydebeapi` |
| `GBase8sCursor` | `jaydebeapi.py` | 继承 `jaydebeapi.Cursor`，处理 `com.gbasedbt.jdbc.GBaseClob2` 未知类型 |
| `ObTimestamp` | `types.py` | 保留原名（既有测试与潜在外部引用），`TIMESTAMP` 的双向转换装饰器 |
| `LVARCHAR` / `SERIAL` / `SERIAL8` | `types.py` | GBase 8s 专属类型 |

注册（`__init__.py` 末尾）与入口点保持一致：

```python
registry.register("jdbcgbase8s", "db_connector_tool.drivers.gbase8s", "GBase8sDialect_jaydebeapi")
```

`pyproject.toml`：

```toml
[project.entry-points."sqlalchemy.dialects"]
jdbcgbase8s = "db_connector_tool.drivers.gbase8s:GBase8sDialect_jaydebeapi"
```

URL 形态（与 `SQLAlchemyDriver.DB_CONFIGS["gbase"]["url_template"]` 完全一致，**不改该文件**）：

```
jdbcgbase8s://host:9088/dbname:GBASEDBTSERVER=servername?user=xxx&password=xxx
```

JDBC URL 生成规则（沿用现有实现，测试已覆盖）：`jdbc:{name}://{host}[:{port}][/{database}]`，其中 `name = "gbasedbt-sqli"`，
`database` 允许形如 `dbname:GBASEDBTSERVER=servername` 的复合串（这正是 GBase JDBC 的合法写法）。

## 5. 方言能力矩阵（显式声明，逐条有理由）

| 属性 | 取值 | 理由 |
| --- | --- | --- |
| `name` | `"gbasedbt-sqli"` | 与 JDBC 子协议同名（沿用现有值） |
| `driver` | `"com.gbasedbt.jdbc.Driver"` | GBase JDBC 驱动类 |
| `default_paramstyle` | `"qmark"` | jaydebeapi 声明 `paramstyle = 'qmark'`；实际 `paramstyle` 由 DBAPI 决定，此处显式声明以防 DBAPI 缺失 |
| `insert_returning` / `update_returning` / `delete_returning` | `False` | Oracle 基类默认 `True`，会生成 `RETURNING ... INTO :ret_0`（`sql.outparam`），而 jaydebeapi **没有 `cursor.var()`**，out 参数无法绑定 → 必须关闭 |
| `use_insertmanyvalues` / `use_insertmanyvalues_wo_returning` | `False` | Informix/GBase 8s **不支持多行 VALUES 语法**，insertmanyvalues 会生成 `VALUES (...), (...)` |
| `postfetch_lastrowid` | `False` | Oracle 基类已为 `False`；jaydebeapi 无 `lastrowid`，不引入 `dbinfo('sqlca.sqlerrd1')` 额外查询（避免「无为 0 被当成主键」的错误），作为已知限制记录 |
| `supports_sane_rowcount` / `supports_sane_multi_rowcount` | `False` | 沿用现有值 |
| `supports_unicode_binds` | `True` | 沿用现有值 |
| `supports_statement_cache` | `True` | 沿用现有值 |
| `description_encoding` | `None` | JDBC 已解码，返回 str |
| `supports_sequences` | `True` | GBase 8s 有 `syssequences` / `tabtype='Q'` |
| `max_identifier_length` | 继承 Oracle 的 `128` | GBase 8s 标识符上限 128 字节 |
| `requires_name_normalize` | 继承 `True` | oracle 兼容模式标识符大小写不敏感 |

## 6. 类型体系（`types.py`）

### 6.1 类型码映射

`syscolumns.coltype` 的编码：**低字节 = 类型码，`coltype // 256 == 1` 表示 NOT NULL**（`divmod(coltype, 256)`）。
故 `ischema_names` 用**数字键**（与官方 Oracle 方言的字符串键不同，这是 Informix 血统的必然差异）：

```python
ischema_names = {
    0: sqltypes.CHAR,        # CHAR
    1: sqltypes.SMALLINT,    # SMALLINT
    2: sqltypes.INTEGER,     # INT
    3: sqltypes.FLOAT,       # FLOAT
    4: sqltypes.FLOAT,       # SMALLFLOAT
    5: sqltypes.Numeric,     # DECIMAL（collength 编码精度/小数位）
    6: SERIAL,               # SERIAL（自增）
    7: sqltypes.DATE,        # DATE
    8: sqltypes.Numeric,     # MONEY
    10: sqltypes.TIMESTAMP,  # DATETIME
    11: sqltypes.LargeBinary,# BYTE
    12: sqltypes.TEXT,       # TEXT
    13: sqltypes.VARCHAR,    # VARCHAR
    14: sqltypes.Interval,   # INTERVAL
    15: sqltypes.NCHAR,      # NCHAR
    16: sqltypes.NVARCHAR,   # NVARCHAR
    17: sqltypes.BIGINT,     # INT8
    18: SERIAL8,             # SERIAL8（自增）
    43: LVARCHAR,            # LVARCHAR
    52: sqltypes.BIGINT,     # BIGINT
    65: sqltypes.TIMESTAMP,  # TIMESTAMP
}
```

`collength` 解析规则（`dictionary.parse_column_type` 实现）：

- `0`(CHAR) / `15`(NCHAR)：长度 = `collength`
- `13`(VARCHAR) / `16`(NVARCHAR)：长度 = `collength // 256`（Informix 编码：高字节为最大长度、低字节为最小占用）
- `43`(LVARCHAR)：`collength > 255` 时取 `collength // 256`，否则取 `collength`
- `5`(DECIMAL)：精度 = `(collength & 0xFF00) >> 8`，小数位 = `collength & 0xFF`；小数位为 255 时视为 INTEGER
- `6`/`18`：`autoincrement=True`
- 其他：`collength` 不参与

### 6.2 colspecs

在 Oracle 的 colspecs 基础上叠加，并**显式保留 `VARCHAR`/`CHAR` 的恒等映射**（用于抵消 Oracle 基类把它们映射为 `VARCHAR2`/`CHAR` 的语义）：

```python
colspecs = util.update_copy(
    dict(OracleDialect.colspecs),   # Boolean/Interval/DateTime/Date
    {TIMESTAMP: ObTimestamp, VARCHAR: VARCHAR, CHAR: CHAR},
)
```

已知风险（记录、不在本次修正）：Oracle 的 `colspecs` 含 `DateTime -> Oracle DATE`，对 GBase 8s 的 `DATETIME` 语义并非最优；因既有实现如此，本次保持行为一致。

## 7. 反射（`dictionary.py` 提供 SQL，`base.py` 提供逻辑）

统一约定：

- 所有系统目录都来自**当前数据库**，表名前缀 `{schema}.`（`schema` 语义沿用厂商方言：可为 `dbname`、`dbname:owner` 形式的跨库限定串；为 `None` 时不加前缀）。
- 过滤系统目录：`tabid > 99`（Informix 血统中 tabid < 100 为系统目录），比厂商的 `flags = 16384` 更稳。
- **不使用** `syscolumns.colattr`（厂商用它识别虚拟列；该列在新老版本中不一定存在，与「更好的版本兼容性」目标冲突，故放弃 computed 列反射）。
- 可选元数据（默认值、注释、视图定义）用 `try/except exc.DBAPIError` 包裹，缺失时降级为 `None`，避免因单个目录视图缺失导致整体反射失败。

### 7.1 SQL 清单（逐字，占位符用 `?`）

```sql
-- has_table
select count(*) from {p}systables where tabname = ? and tabtype in ('T', 'V')

-- get_table_names
SELECT tabname FROM {p}systables WHERE tabtype = ? AND tabid > 99 ORDER BY tabname

-- get_temp_table_names（oracle 兼容模式；flags 取自厂商实测值）
SELECT tabname FROM {p}systables WHERE tabtype = ? AND flags = 28672 ORDER BY tabname

-- get_sequence_names
SELECT tabname FROM {p}systables WHERE tabtype = 'Q' AND tabid > 99 ORDER BY tabname

-- get_schema_names
SELECT DBS_DBSNAME FROM SYSMASTER.SYSDBSLOCALE

-- get_columns（主查询）
SELECT t1.colname, t1.coltype, t1.collength, t1.colno
FROM {p}syscolumns AS t1, {p}systables AS t2
WHERE t1.tabid = t2.tabid AND t2.tabname = ?
ORDER BY t1.colno

-- get_columns（可选：默认值。sysdefaultsexpr 在部分版本/GBase 分支才存在）
SELECT t3.colno, t3.default
FROM {p}sysdefaultsexpr AS t3, {p}systables AS t2
WHERE t3.tabid = t2.tabid AND t2.tabname = ? AND t3.type = 'T'

-- get_columns（可选：列注释）
select colname, comments from {p}syscolcomments where tabname = ?

-- get_pk_constraint
SELECT t1.tabid, t2.idxname, t2.part1, t2.part2, t2.part3, t2.part4, t2.part5,
       t2.part6, t2.part7, t2.part8, t2.part9, t2.part10, t2.part11, t2.part12,
       t2.part13, t2.part14, t2.part15, t2.part16
FROM {p}systables AS t1, {p}sysindexes AS t2, {p}sysconstraints AS t3
WHERE t1.tabid = t2.tabid AND t1.tabname = ?
  AND t2.idxname = t3.idxname AND t3.constrtype = 'P'

-- get_indexes
SELECT t1.idxname, t1.idxtype, t1.clustered, t1.part1, t1.part2, t1.part3, t1.part4,
       t1.part5, t1.part6, t1.part7, t1.part8, t1.part9, t1.part10, t1.part11,
       t1.part12, t1.part13, t1.part14, t1.part15, t1.part16,
       t2.constrtype, t2.constrname
FROM {p}sysindexes AS t1
LEFT JOIN {p}sysconstraints AS t2 ON (t1.tabid = t2.tabid AND t1.idxname = t2.idxname)
WHERE t1.tabid = (SELECT tabid FROM {p}systables WHERE tabname = ?)
ORDER BY t1.idxname ASC

-- 列号 → 列名（批量，供 pk/index/fk 复用）
SELECT colno, colname FROM {p}syscolumns WHERE tabid = ? AND colno IN ({占位符列表})

-- get_foreign_keys 第一步（本地约束列）
SELECT t1.tabid, t2.idxname, t3.constrname, t3.constrid,
       t2.part1, ... , t2.part16
FROM {p}systables AS t1, {p}sysindexes AS t2, {p}sysconstraints AS t3
WHERE t1.tabid = t2.tabid AND t1.tabname = ?
  AND t2.idxname = t3.idxname AND t3.constrtype = 'R'

-- get_foreign_keys 第二步（被引用表与列）
SELECT t4.tabname, t4.tabid, t1.delrule, t2.part1, ... , t2.part16
FROM {p}sysreferences AS t1, {p}sysindexes AS t2, {p}sysconstraints AS t3, {p}systables AS t4
WHERE t1.constrid = ? AND t3.constrid = t1.primary
  AND t2.idxname = t3.idxname AND t4.tabid = t1.ptabid

-- get_check_constraints
SELECT t1.constrname, t2.checktext, t2.seqno
FROM {p}sysconstraints AS t1, {p}syschecks AS t2
WHERE t1.tabid = (SELECT tabid FROM {p}systables WHERE tabname = ?)
  AND t1.constrid = t2.constrid AND t2.type = 'T' AND t1.constrtype = 'C'
ORDER BY t1.constrname, t2.seqno

-- get_table_comment（可选）
select comments from {p}syscomments where tabname = ?

-- get_view_definition（按 seqno 拼接 viewtext）
SELECT t1.viewtext
FROM {p}sysviews AS t1, {p}systables AS t2
WHERE t1.tabid = t2.tabid AND t2.tabname = ?
ORDER BY seqno

-- get_isolation_level
select scs_isolationlevel from sysmaster:syssqlcurses
```

### 7.2 解析规则

- `get_columns`：`divmod(coltype, 256)` → `(not_null, base_type)`；主键列集合来自 `get_pk_constraint`；`nullable = not bool(not_null)`；`autoincrement` 由类型码 6/18 决定；无列且表不存在时抛 `exc.NoSuchTableError`。
- `get_pk_constraint`：`part1..part16` 中 `> 0` 的项即列号，按顺序批量换名；返回 `{name, constrained_columns}`。
- `get_indexes`：`idxtype.strip().upper().startswith('U')` → `unique=True`；`constrtype` 为 `P`/`R` 的索引跳过；返回 `{name, column_names, unique, dialect_options}`。
- `get_unique_constraints`：复用 `get_indexes`，保留 `unique` 的项，返回 `{name, column_names}`。
- `get_foreign_keys`：本地列取第一步的 `part*`；被引用列取第二步的 `part*`；`delrule`：`C→CASCADE`、`N→SET NULL`，其余为 `None`；返回 `{name, constrained_columns, referred_schema, referred_table, referred_columns, options}`。
- `get_check_constraints`：按 `constrname` 分组，把同组 `checktext` 按 `seqno` 拼接；返回 `[{name, sqltext}]`。
- `get_view_definition`：**拼接所有 seqno 行**（厂商实现只取首行，是缺陷）。
- `get_table_comment`：`{'text': comments.rstrip()}`；无注释返回 `reflection.ReflectionDefaults.table_comment()`。

## 8. 连接层（`jaydebeapi.py`）

1. `import_dbapi()` → `__import__("jaydebeapi")`。
2. `create_connect_args(url)`：`make_url` → `_build_jdbc_url` + `_build_connect_args` + `_handle_jar_path`，返回 `((), {jclassname, url, driver_args, jars})`（**保持既有签名与返回结构**，测试已覆盖）。
3. `_handle_jar_path`：优先环境变量 `GBASE8S_JDBC_JARPATH`，否则在 `PathHelper.get_user_config_dir("db_connector_tool")/jars` 下 `rglob("*gbase*.jar")`；找不到时 `warnings.warn` 并把 `jars` 置为 `None`。
4. `do_rollback(dbapi_connection)`：**改为调用 `dbapi_connection.rollback()`**（jaydebeapi 的 `Connection.rollback` 直通 JDBC `Connection.rollback()`）；对不支持回滚的匿名/异常连接，捕获异常并降级为 `pass`，避免连接池归还时报错。
5. `GBase8sCursor`：保留 `GBaseClob2` 未知类型转换逻辑（逐字符 `getCharacterStream()` 读取）。
6. `is_disconnect`：保留关键字匹配（connection closed / socket closed / broken pipe / connection reset / jdbc connection / network error），并追加对 `java.sql.SQLNonTransientConnectionException` 类名文本的判断。
7. 连接池回收：`do_rollback` 生效后，`pool_reset_on_return` 的默认行为（回滚未提交事务）得以正确工作。

## 9. 版本、schema 与隔离级别

- `_get_server_version_info`：沿用现有实现（依次尝试 `dbinfo('version_gbase','full')` / `dbinfo('version','full')`，正则解析），返回形如 `(8, 8, 3, 6, 2)`。
- `get_default_schema_name` / `_get_default_schema_name`：沿用现有实现（`connection.engine.url.username.upper()`，`_get_default_schema_name` 转调前者）。
- `get_isolation_level_values`：`["DIRTY READ", "COMMITTED READ", "COMMITTED READ LAST COMMITTED", "CURSOR STABILITY", "REPEATABLE READ"]`（不含 `AUTOCOMMIT`，GBase 8s 的自动提交由 JDBC 层 `setAutoCommit` 控制，不走 `SET ISOLATION`）。
- `set_isolation_level`：**白名单校验后**执行 `SET ISOLATION TO {level}`（`SET ISOLATION` 不支持绑定参数，白名单即注入防护）；非白名单值抛 `exc.ArgumentError`。
- `get_isolation_level`：执行 `dictionary.GET_ISOLATION_LEVEL`，失败时返回 `None`。
- Oracle 基类的 `get_isolation_level`（`v$parameter`）与 `set_isolation_level`（`ALTER SESSION`）在 GBase 8s 上会失败，必须覆盖。

## 10. 已知限制与风险（须在真机验证时确认）

| 项 | 说明 |
| --- | --- |
| 无法自动取回自增主键 | 无 RETURNING、无 `lastrowid` → ORM 插入后主键为 `None`；如需可用 `SELECT dbinfo('sqlca.sqlerrd1')` 手工获取 |
| `VARCHAR`/`NVARCHAR` 长度 | 采用 `collength // 256`（Informix 标准编码）；若 GBase 8s 分支按字面值存储 `collength`，长度会偏小 256 倍，需真机核对 |
| 分页语法 | 继承 Oracle 基类：服务端版本 < 12 时用 `ROWNUM` 包装。依赖 oracle 兼容模式的 `ROWNUM` 支持；若不支持，应改为 Informix 原生 `SKIP n FIRST m` |
| `sysdefaultsexpr` / `syscomments` | 部分版本可能不存在，已用 `try/except` 降级 |
| `schema` 语义 | 仅作为表名前缀（跨库限定），不按 owner 过滤 |
| 临时表反射 | 沿用厂商 `flags = 28672`，未实测 |

## 11. 验收清单与结果（离线）

测试文件：`tests/drivers/test_gbase8s.py`（替换 `tests/drivers/test_gbase8s_jdbc.py`，原有用例全部保留并更新导入路径）。

| 测试类 | 覆盖点 |
| --- | --- |
| `TestGBase8sCursor` | 游标构造 |
| `TestObTimestamp` | `None`/非 datetime/`datetime` 绑定与结果解析 |
| `TestGBase8sTypes` | `LVARCHAR`/`SERIAL`/`SERIAL8` 的 DDL 渲染；`ischema_names` 类型码完整性 |
| `TestColumnTypeParsing` | `collength` 解析：CHAR/VARCHAR/NVARCHAR/LVARCHAR/DECIMAL/SERIAL/未知类型告警 |
| `TestGBase8sDialectCapability` | `name`/`driver`/`paramstyle`/`insert_returning`/`enable_offset_fetch` 等能力矩阵 |
| `TestGBase8sJdbcUrl` / `TestGBase8sConnectArgs` | JDBC URL 拼接（4 种）与 `driver_args` 组装（5 种） |
| `TestGBase8sJarPath` | jar 自动搜索命中与缺失告警 |
| `TestGBase8sConnectionHooks` | `import_dbapi`、`do_rollback` 调用 DBAPI、`is_disconnect` |
| `TestGBase8sVersionAndSchema` | `_get_server_version_info`(4)、`get_default_schema_name`(2) |
| `TestGBase8sIsolationLevel` / `TestGBase8sIsolationLevelWrite` | 白名单、默认值回退、非法值抛错、发出的 SQL |
| `TestGBase8sDictionarySql` | `format_sql` 前缀规则、系统表过滤、索引 part 列数（16） |
| `TestGBase8sNameReflection` / `TestGBase8sTempAndSchemaReflection` | `has_table`/`has_sequence`/表名/临时表名/视图名/序列名/schema 名 |
| `TestGBase8sColumnReflection` / `TestGBase8sColumnMetadata` | 列定义、无表报错、可选目录降级、表注释、视图定义拼接 |
| `TestGBase8sPkAndIndexReflection` | 主键、索引（过滤 P/R）、唯一约束 |
| `TestGBase8sForeignKeyReflection` | 外键（含 `SET NULL`/未知规则/被引用表缺失）、检查约束片段拼接 |
| `TestGBase8sProvision` | jar 自检、建/删测试表 DDL、标识符校验 |
| `TestGBase8sCompileSnapshot` | `create_engine("jdbcgbase8s://...")` 解析、`CreateTable`、无 RETURNING 的 INSERT、`limit/offset` 走 `ROWNUM` |

验收结果：

- `python -m unittest tests.drivers.test_gbase8s` → **Ran 100 tests, OK**
- `python -m unittest discover -s tests -t .` → **Ran 545 tests, FAILED (failures=9, errors=25)**；已用 `git worktree` 在 HEAD 基线复跑确认该 34 处失败为**既有问题**（属性缺失、Windows 日志文件占用、mock 漂移），与本次改动无关
- `pylint --max-line-length=100`（E/W0611/W0621/W1203/C0301/C0116/C0411-C0413）→ **10.00/10**
- 全部函数与类均 ≤ 100 行（AST 扫描 0 超限）
- 编译快照（离线）：
  - `CREATE TABLE t (id SERIAL NOT NULL, name LVARCHAR(64), b SERIAL8, ts TIMESTAMP, PRIMARY KEY (id))`
  - `select(t).limit(5).offset(2)` → 两层 `ROWNUM` 包装（无 `FETCH FIRST`）
  - `create_connect_args` → `jclassname=com.gbasedbt.jdbc.Driver`、`url=jdbc:gbasedbt-sqli://host:9088/db`