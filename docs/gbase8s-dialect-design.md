# GBase 8s SQLAlchemy 方言设计说明（JayDeBeApi + JDBC）

## 1. 目标

以 **JayDeBeApi（JPype + JDBC）** 替代 `gbase8sdb` / `DbtPy` 直连，统一通过 JDBC 驱动
`com.gbasedbt.jdbc.Driver` 连接 GBase 8s，获得更好的服务端版本兼容性，并在同一方言内
**原生支持一库三模**（`oracle` / `gbase` / `mysql`）。

## 2. 文件组织（对照 SQLAlchemy Oracle 方言）

| 文件 | 职责 |
| --- | --- |
| `drivers/gbase8s/__init__.py` | 汇总导出，程序化注册方言名 `gbase8s` |
| `drivers/gbase8s/base.py` | 类型编译器 / 语句编译器 / DDL 编译器 / 方言类 / 5 个反射 Mixin（与 DBAPI 无关） |
| `drivers/gbase8s/types.py` | GBase 8s 专用类型（`SERIAL` / `SERIAL8` / `MONEY` / `LVARCHAR`）与 `ischema_names` 类型号映射 |
| `drivers/gbase8s/dictionary.py` | 系统目录 SQL 与结果解析（`systables` / `syscolumns` / `sysconstraints` / `sysindexes` 等） |
| `drivers/gbase8s/jaydebeapi.py` | DBAPI 层：URL → `jaydebeapi.connect` 参数、jar 定位、事务与断连判定 |
| `drivers/gbase8s/converters.py` | 取值类型归一化（JDBC → Python 原生类型）与列元数据提取 |

## 3. 注册与连接 URL

方言名：`gbase8s`（同时注册 `gbase8s.jaydebeapi`），驱动 `jaydebeapi`。

```
gbase8s://host:9088/dbname:GBASEDBTSERVER=server?user=u&password=p&sqlmode=mysql
```

- URL 路径中 `:` 之后为 JDBC 连接属性（`key=value`，`;` 亦可）；
- 查询串中 `user` / `password` / `sqlmode` / `jars` 为方言级参数，其余键（如
  `CLIENT_LOCALE`）一并追加为 JDBC 属性；
- `sqlmode` 既写入 JDBC 连接串，也同步到方言实例，保证会话模式与生成的 SQL 一致；
- jar 定位顺序：URL `jars` 参数 → 环境变量 `GBASEDBT_JDBC_JAR` / `GBASE8S_JDBC_JAR`
  → **只搜索两个目录**（程序运行目录 `./jars`、用户配置目录 `jars`）
  → 由 `CLASSPATH` 决定。

### 3.1 驱动 jar 的自动获取（`jar_downloader.py`）

两个搜索目录都找不到驱动时，从 Maven Central 拉取最新版到用户配置目录下的 `jars`
（Windows 为 `%APPDATA%\db_connector_tool\jars`）：

| 项 | 值 |
| --- | --- |
| 坐标 | `com.gbasedbt:jdbc` |
| 版本来源 | `maven-metadata.xml` 的 `<release>`；**为 `_nobson` 变体时改用同数字段的默认包**（缺失时按数字段取最大，同数字段默认包优先） |
| 元数据 | `…/com/gbasedbt/jdbc/maven-metadata.xml` |
| 产物 | `…/com/gbasedbt/jdbc/<version>/jdbc-<version>.jar` + `.sha1` |
| 落盘名 | `gbasedbt-jdbc-<version>.jar`（带 gbase 关键字，确保能被搜索逻辑识别） |

**`_nobson` 变体的处理**（重要）：厂商对同一版本同时发布两个产物，默认包内置
`org.bson.*`，`_nobson` 包把这些类剥掉（供调用方自备 mongo-java-driver）。实测
`3.6.5.21.jar` 含 67 个 `org/bson/*` 类、`3.6.5.21_nobson.jar` 为 0 个，但后者的
`com/gbasedbt/jdbc/IfxBSONObject.class` **仍引用** `org/bson/BSONObject` 等 4 个类 ——
即遇到 BSON/JSON 列会抛 `NoClassDefFoundError`。而 Maven 元数据的 `<release>`
恰好指向 `3.6.5.21_nobson`，故必须显式规避：

- 选择阶段：`release` 是变体且列表里有**同数字段**的默认包 → 改用默认包；
- 排序阶段：`version_rank()` 在数字段相同时让默认包排在变体之前（`_first_jar_in`
  也用它），避免目录遍历顺序决定命中哪个；
- 自愈：本地只装了变体时，`find_jdbc_jar()` 会尝试补下同版本默认包，失败才回退到变体；
- 逃生口：`GBASEDBT_JDBC_ALLOW_NOBSON=1` 显式允许变体（此时不做上述替换与补下）。

实现要点：

- **校验**：先比对官方 `.sha1`（缺失时降级为仅校验 ZIP 文件头），再原子替换；
- **原子性**：写临时文件（`.gbasedbt-*.part`）后 `os.replace`，避免半截文件被加载；
- **限流**：默认 20 秒超时、40MB 上限，均可由环境变量覆盖；
- **失败策略**：只告警不抛出，返回 `None` 交由 `CLASSPATH` 处理；版本解析按仓库
  缓存、失败版本按进程记忆，避免每次建连都重试；
- **可关闭/可换源**：`GBASEDBT_JDBC_AUTO_DOWNLOAD`、
  `GBASEDBT_JDBC_MAVEN_REPO`（支持镜像、私服、本地目录与 `file://`）、
  `GBASEDBT_JDBC_ALLOW_NOBSON`；
- **多版本共存**：同目录按文件名数字段取最高版本，数字段相同则默认包优先。

### 3.2 JVM 原生访问授权（JEP 472）

JPype 启动 JVM 时会调用 `java.lang.System::load` 加载 `_jpype` 原生库，而该方法自
JDK 24 起被 [JEP 472](https://openjdk.org/jeps/472) 列为**受限方法**：默认只打印
4 行 `WARNING`，后续 JDK 版本将直接拒绝。该警告对功能无影响，但会污染日志。

JayDeBeApi 在 `_jdbc_connect_jpype()` 内部自行调用 `jpype.startJVM`，**不对外暴露
任何 JVM 参数入口**，因此无法通过 `create_connect_args` 传参。本工具的做法是在
JVM 启动前**包装 `jpype.startJVM`**，调用时若参数里还没有该开关就补上：

| 项 | 值 |
| --- | --- |
| 注入参数 | `--enable-native-access=ALL-UNNAMED` |
| 注入时机 | `import_dbapi()`（建引擎时，早于首次连接）；`create_connect_args()` 兜底 |
| 幂等性 | 模块级 `_NATIVE_ACCESS_INSTALLED` 标志 + 参数存在性检查，避免重复包装/重复追加 |
| 逃生口 | `GBASEDBT_JDBC_NATIVE_ACCESS=0` 关闭注入 |

兼容性：`--enable-native-access` 自 JDK 17 起被识别；更早的 JDK 视为未知参数，而
JayDeBeApi 调用 `startJVM` 时带 `ignoreUnrecognized=True`，未知参数会被忽略而非报错，
故老环境不会因此启动失败。若 JVM 已被其他代码抢先启动，则注入无从生效（警告已输出），
但不报错。

### 3.3 取值类型归一化（`converters.py`）

JayDeBeApi 自带的取值转换器有两个问题，会把 GBase 8s 与其它数据库的返回类型割裂：

1. **精度丢失**：`DECIMAL`/`NUMERIC` 走 `_java_to_py_bigdecimal()`，只要 `scale != 0`
   就调 `doubleValue()`，把 `BigDecimal` 降级为 float。真机实测
   `DECIMAL(19,2)` 的 `12345678901234567.89` 变成 `1.2345678901234568e+16`；
2. **类型不统一**：日期/时间戳被转成 `str`，整数/浮点则原样返回 JPype 包装的
   Java 对象（`java.lang.Integer` / `JDouble`），而其它驱动返回 Python 原生类型。

此外其转换器表只覆盖部分 `java.sql.Types`：`BIGINT`、`REAL`、`VARCHAR`、`BLOB` 等
未注册，会走兜底直接返回 Java 对象。

**做法**：在连接建立后（`on_connect`）原地更新 JayDeBeApi 的 `_converters` 字典——
该字典被 `Connection`/`Cursor` 共享引用，原地更新即可让已建立的连接立即生效。

归一化映射：

| JDBC 类型 | Python 类型 | 说明 |
| --- | --- | --- |
| `INTEGER` / `SMALLINT` / `TINYINT` / `BIGINT` / `INT8` | `int` | 含 Java 的 `Long`/`Short`/`BigInteger` |
| `DECIMAL` / `NUMERIC` / `MONEY` | `decimal.Decimal` | 经 `toPlainString()`，不经 float |
| `DOUBLE` / `FLOAT` / `REAL` / `SMALLFLOAT` | `float` | |
| `CHAR` / `VARCHAR` / `LVARCHAR` / `TEXT` / `CLOB` | `str` | |
| `DATE` | `datetime.date` | |
| `TIME` | `datetime.time` | |
| `TIMESTAMP` / `DATETIME ...` | `datetime.datetime` | 微秒精确 |
| `BINARY` / `VARBINARY` / `BYTE` / `BLOB` | `bytes` | |
| `BOOLEAN` / `BIT` | `bool` | |

**微秒换算修正**：JayDeBeApi 用 `int(str(nanos)[:6])`，当纳秒不足 9 位时结果偏大
（`1010000` ns 应为 `1010` µs，却算出 `101000`，放大 100 倍）。本实现改为整除
（见 `_nanos_to_microseconds`）。

### 3.4 列元数据

`ResultSetMetaData` 只在结果集打开期间有效——取完最后一行后 JayDeBeApi 会关闭
`_rs`/`_meta`，`cursor.description` 随即变为 `None`。因此元数据**必须在取值之前**
读取，`SQLAlchemyDriver.execute_query_with_metadata()` 即按此顺序实现。

GBase 8s 的 JDBC 元数据比 DBAPI 的 `description` 更丰富，提供类型名、精度、标度、
可空性与 Java 类名：

| 字段 | 示例 |
| --- | --- |
| `name` | `dec` |
| `type_name` | `DECIMAL` / `INT8` / `DATETIME YEAR TO FRACTION(3)` |
| `python_type` | `Decimal` / `int` / `datetime` |
| `precision` / `scale` | `19` / `2` |
| `nullable` | `True` |
| `java_class` | `java.math.BigDecimal` |

GBase 把时间戳报成 Informix 语法 `DATETIME YEAR TO FRACTION(3)`、整型报成 `INT8`，
故 `python_type_for()` 先精确匹配再按前缀匹配。

## 4. 一库三模实现

| 能力 | oracle 模式 | gbase 模式 | mysql 模式 |
| --- | --- | --- | --- |
| 无 FROM 时的伪表 | `FROM DUAL` | `FROM sysmaster:sysdual` | `FROM sysmaster:sysdual` |
| 分页 | `ROWNUM` 双层子查询 | `FIRST n SKIP m` | `LIMIT m OFFSET n` |
| 布尔字面量 | `'t'` / `'f'` | `'t'` / `'f'` | `'t'` / `'f'` |

实现要点：

- `GBase8sCompiler.default_from` / `visit_empty_set_expr` 按模式补伪表；
- oracle 模式在 `translate_select_structure` 中把 LIMIT/OFFSET 改写为 ROWNUM 上限 +
  外层 `gbase8s_rn` 过滤，简单整型立即数以 `render_literal_execute()` 内联；
- gbase 模式在 `get_select_precolumns` 中输出 `FIRST n SKIP m`；
- mysql 模式在 `limit_clause` 中输出 `LIMIT ... OFFSET ...`；仅 OFFSET 时取 BIGINT
  最大值并以字面量内联 OFFSET（详见第 5 节实测结论）；
- `_dialect_specific_select_one` 沿默认实现自动套用 `default_from`，因此 pre-ping 语句
  随模式自动变化，无需覆写。

## 5. 真机验证结论

测试环境：WSL 内 GBase 8s（`db_station`），方言经 `SQLAlchemyDriver` 正规路径连接。

| 验证项 | 结论 |
| --- | --- |
| 伪表 | oracle 仅 `DUAL` 可用；gbase 两者皆可；mysql 仅 `sysmaster:sysdual` 可用 |
| 分页 | 三模式 limit / limit+offset / offset-only 结果均正确 |
| `LIMIT` 边界 | `LIMIT -1` 与 `18446744073709551615` 均报语法错误，`9223372036854775807` 合法 |
| `LIMIT <max> OFFSET ?` | 字面量 + 绑定参数组合在 `prepareStatement` 阶段报错，OFFSET 必须内联 |
| `库:表` 限定语法 | oracle 模式不可用；gbase 可用；mysql 用于用户表需 `db:owner.表` |
| 不带限定符的系统目录 | oracle / gbase 可用；mysql 语法错误 |
| 反射 | oracle 与 gbase 模式结果一致（schemas / default schema / 列 / 主键 / DDL 往返） |
| 填充空格 | CHAR 列（`owner` / `USER` / `idxname` / `constrname`）需 `.strip()` |

类型号实测（建表后读 `syscolumns`）：

| SQLAlchemy 类型 | coltype | 备注 |
| --- | --- | --- |
| `String(n)` | 13 | VARCHAR |
| `Text()` | 41 | extended_id=11（CLOB） |
| `Boolean()` | 41 | extended_id=5 |
| `LargeBinary()` | 41 | extended_id=10（BLOB） |
| `Integer()` | 2 | 自增主键低 8 位为 6（SERIAL） |
| `BigInteger()` | 17 | INT8 |
| `Float()` | 3 | |
| `Numeric(p,s)` | 5 | |
| `Date()` / `DateTime()` / `Time()` | 7 / 10 / 10 | |
| `MONEY()` | 8 | |
| `LVARCHAR(n)` | 40 | |

因 CLOB / BLOB / BOOLEAN 共用 `coltype=41`，反射需借助 `syscolumns.extended_id` 区分
（见 `dictionary._EXTENDED_TYPE_MAP`）。

## 6. 已知限制

- 不支持 `lastrowid` 与 `RETURNING`（JayDeBeApi 无 `cursor.var()`）；
- oracle 模式的 ROWNUM 分页依赖服务器 Oracle 兼容模式；
- mysql 模式反射受名称解析限制，不带限定符的系统目录不可用；
- 尚未接入 `sqlalchemy.testing` 官方方言一致性测试套件。

## 7. 测试与验收

- 单元测试：`tests/drivers/test_gbase8s.py`（56 项），覆盖三模式 SQL 生成、类型与 DDL
  编译、URL 翻译与 jar 定位、数据字典解析、模拟连接下的反射；
- 代码质量：`pylint src/db_connector_tool/drivers/gbase8s tests/drivers/test_gbase8s.py`
  评分 10.00/10（另有 1 条历史遗留的 `too-many-locals` 提示，位于
  `get_foreign_keys`，尚未拆分）；
- 回归：`python -m unittest discover -s tests -t .` 全量用例全绿（用例数随迭代增长，此处不写死）。