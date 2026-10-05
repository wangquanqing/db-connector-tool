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
  → `~/.sqlalchemy-jdbcapi/drivers` 等约定目录 → 由 `CLASSPATH` 决定。

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
  评分 10.00/10；
- 回归：`python -m unittest discover -s tests -t .` 全绿（507 项）。