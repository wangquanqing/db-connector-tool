# 测试策略与覆盖率

> 本文说明测试的分层、如何运行、覆盖率口径，以及**哪些分支刻意不覆盖**及其理由。
> 最后一项尤其重要 —— 覆盖率的价值在于「剩下的没覆盖是否可接受」，而不是数字本身。

---

## 1. 分层

| 层 | 文件 | 特点 | 数量 |
|---|---|---|---|
| **单元测试** | `tests/*/test_*.py`、`tests/test_*.py` | 单函数/类，大量 mock，毫秒级 | 约 1080 项 |
| **集成测试** | `tests/test_integration.py`、`tests/test_integration_full.py` | 真实 CLI → 管理器 → 驱动 → SQLite 串起来，只 mock 外部不可控因素 | 34 项 |
| **真库集成** | 同上（`TestRealDatabase`，环境变量启用） | 连真实 PostgreSQL / GBase 8s 等 | 3 项（默认跳过） |
| **覆盖率补测** | `tests/test_coverage_gaps_*.py`、`test_coverage_branches*.py` | 专门覆盖异常/边界分支，每条注明**触发条件** | 约 180 项 |

### 为什么把「覆盖率补测」单独放

这些用例大多是「人为触发异常」或「patch 平台判断」，与功能测试混在一起会掩盖真正的业务意图。
集中放置便于审查：**每条兜底分支是否真的有意义**（若某条根本不可能发生，就该删掉代码，而不是写个假测试刷数字）。

---

## 2. 运行方式

```bash
cd <仓库根目录>          # 必须在根目录，否则 src. 包导不进来

# 全量单元 + 集成测试
python -m unittest discover -s tests -t .

# 单个模块
python -m unittest tests.core.test_sql_splitter -v

# 单个用例
python -m unittest tests.test_integration_full.TestCliEndToEnd.test_query_renders_table

# 覆盖率（含分支）
coverage run -m unittest discover -s tests -t . && coverage report -m

# HTML 报告
coverage html && start htmlcov/index.html
```

### 真库集成测试

默认跳过（CI 里没有这些库）。要跑就指定连接名：

```bash
# PostgreSQL
DCT_IT_DB=db_kes python -m unittest tests.test_integration_full.TestRealDatabase -v

# GBase 8s（注意：oracle 模式需要伪表，测试内部已按方言处理）
DCT_IT_DB=db_gb python -m unittest tests.test_integration_full.TestRealDatabase -v

# 若连接配置不在默认 app 名下，可另指定
DCT_IT_DB=my_conn DCT_IT_APP=my_app python -m unittest ...
```

**连接不可用时整类跳过**（不是失败）—— 环境问题不该让测试红。

---

## 3. 当前覆盖率

| 指标 | 数值 |
|---|---|
| 语句覆盖 | **100%**（4135 / 4135） |
| 分支覆盖 | **99.76%**（978 / 1002） |
| 用例总数 | **1112** 项（3 项真库默认跳过） |

门槛写在 `pyproject.toml`：

```toml
[tool.coverage.report]
fail_under = 99
```

即低于 99% 会让 `coverage report` 返回非零，防止退化。

---

## 4. 刻意不覆盖的分支（24 个）

这些分支**不是测不了，而是测它们的成本 > 收益**。分三类：

### A. 依赖缺失（装了就永远走不到）

| 位置 | 分支 | 说明 |
|---|---|---|
| `core/key_manager.py:43` | `except ImportError` | 仅在未安装可选依赖 `keyring` 时触发。CI 装了 keyring，走不到。 |

### B. 单平台 / 环境特定（只能在另一个 OS 上走到）

| 位置 | 分支 | 说明 |
|---|---|---|
| `utils/path_utils.py` 平台分支 | Windows / Unix 二选一 | 一次运行只能命中一个平台的分支。跨平台 CI 可覆盖另一半。 |
| `core/config.py` 等处 | 同上 | 同上。 |

### C. 双重失败 / 运维级故障（异常处理里再出异常）

| 位置 | 分支 | 说明 |
|---|---|---|
| `core/config_security.py:381` | 密钥轮换失败后「恢复原密钥」也失败 | 需要两次独立故障叠加。仅记日志后继续抛原异常，行为已由「轮换失败」用例间接保证。 |
| `batch_manager.py:187` | 关闭连接时抛 `DatabaseError` | 连接已损坏的极端情况；关闭失败不影响整体清理（`finally` 会从池中移除）。 |
| `batch_manager.py:521/624` | 升级失败且无回滚语句 / 清理中的 OSError | 需要模拟磁盘/权限故障。 |
| `core/connections.py:451-455` | `atexit` 回调里清理临时连接失败 | 回调在进程退出时才执行，单测无法稳定捕获；且失败只记 debug。 |
| `core/result_render.py:486` | `_print_row` 的 Mapping 形态分支之一 | 该形态已被 `_show_results` 主路径覆盖，此处为防御性冗余。 |
| `drivers/gbase8s/base.py` 8 处 | 反射时的空值/异常组合 | 需要真实库返回畸形目录数据（如外键缺远端属主），属数据损坏场景。 |
| `drivers/gbase8s/jaydebeapi.py:264` | `os.path.isfile` 抛 `OSError` | 路径含非法字符等极端情况。 |
| `drivers/gbase8s/jaydebeapi.py:446` | `do_rollback` 吞 `dbapi.Error` | 需要构造真实的 JDBC 异常实例（需 JVM + 连接）。 |
| `drivers/sqlalchemy_driver.py:396/549` | 非 `SQLAlchemyError` 的驱动层异常 | 属驱动意外行为，防御性兜底。 |

### 处理方式

这些行在源码里用 `# pragma: no cover` 标注，**并附一行说明**，例如：

```python
except ImportError:  # pragma: no cover - 仅在未安装可选依赖 keyring 时触发
    pass
```

> **原则**：不用 `# pragma: no cover` 掩盖「懒得写测试」。判断标准是
> 「能否在不 mock 到失真的前提下稳定复现」—— 不能，才标注。

---

## 5. 写测试时的约定

### 5.1 必须用 `IsolatedTestCase`

它会隔离配置目录与密钥存储，避免污染真实的 `%APPDATA%`：

```python
from ._isolation import IsolatedTestCase

class TestSomething(IsolatedTestCase):
    def setUp(self):
        super().setUp()          # 必须调用，否则 self.temp_path 不存在
        self.path = self.temp_path / "x.db"
```

> **踩坑记录**：曾经有测试类忘了 `super().setUp()`，导致 `self._temp_dir` 缺失、tearDown 报错。

### 5.2 不要 mock 到「失去真实性」

反面教材（已修）：连接测试的用例曾用 `MagicMock` 硬塞
`_dialect_specific_select_one = "SELECT 1"`，于是**任何字符串都"编译成功"**，
真实方言差异（psycopg 返回 `";"`）被完全掩盖，导致 PostgreSQL 连接测试
误报失败的回归**没被发现**。

正确做法：构造**真实对象**（如 `_dialect_stub()` 用真实方言子类），
只 mock 外部边界（网络、JVM、文件系统）。

### 5.3 避免真实网络 / 真实数据库

- 网络：mock `urllib.request.urlopen`；
- 数据库：SQLite `:memory:` 或临时文件；真库走 `DCT_IT_DB` 开关；
- 不要在测试里 `input()` 等用户输入 —— 用 `mock.patch("builtins.input", side_effect=[...])`，
  且**提供终止条件**（否则循环不退出会卡死）。

> **踩坑记录**：曾有测试让 `interactive_shell` 一直读 input，
> 而 `_run_single_sql` 又去连真实数据库，导致整个测试套件挂起 5 分钟。

### 5.4 Windows 文件占用

SQLite 文件库在 Windows 上被连接占用时，`TemporaryDirectory` 清理会失败。
用完记得 `engine.dispose()` 或 `close_all_connections()`。

### 5.5 超时测试要用「真在算」的查询

测查询超时时，**不要用 `SLEEP()` 一类函数**：

| 库 | 踩的坑 |
|---|---|
| MySQL | `max_execution_time` **明确不覆盖** `SLEEP()`（文档写明），实测不会被打断 → 改用笛卡尔积 |
| PostgreSQL | `pg_sleep()` 会被 `statement_timeout` 打断，可以用 |
| SQL Server | `WAITFOR DELAY` 可以模拟，但该方言不支持查询级超时 |

### 5.6 绝不要在 GBase 8s 上测「看门狗打断」

实测：用另一线程对 JDBC 连接调 `invalidate()` 会让**整个进程挂死**
（JVM 调用不可重入）。这类测试请加方言判断跳过，或放到子进程里设硬超时：

```python
proc = multiprocessing.Process(target=worker)
proc.start()
proc.join(timeout=25)
if proc.is_alive():
    proc.terminate()      # 挂死时强制清理
    proc.join(timeout=3)
```

`drivers/timeout_control.py` 已用 `WATCHDOG_SAFE_DIALECTS` 白名单把这类方言
挡在门外，测试只需验证「它确实被拒绝」。

---

## 6. 测试覆盖了哪些关键行为

除常规功能外，以下**回归守卫**值得注意（都是真实发生过的问题）：

| 守卫 | 文件 | 防的是 |
|---|---|---|
| `test_cjk_identifier_does_not_hang` | `test_sql_splitter.py` | 中文标识符导致切分器死循环（`'用'.isalpha()` 为真但不在词法集内） |
| `test_if_exists_is_not_procedural_if` | `test_sql_splitter.py` | `DROP TABLE IF EXISTS` 被当成过程体 IF，整段脚本粘连 |
| `test_duplicate_columns_preserved_to_csv` | `test_integration_full.py` | 重名列导出 CSV 时丢列/写错值（曾把「张三」写成「研发部」） |
| `test_connection_test_ignores_dbapi_ping_string` | `test_sqlalchemy_driver.py` | 误用 `_dialect_specific_select_one`（psycopg 返回 `";"`），PG 连接测试误报 |
| `test_docstring_promised_root_names` | `test_public_api.py` | docstring 承诺的导入实际导不通 |
| `test_set_then_get_is_consistent` | `test_gbase8s.py` | GBase 隔离级别 set/get 不自洽 |
| `test_timeout_does_not_leak_to_next_query` | `test_integration_full.py` | MySQL 的 SESSION 级超时未复位，**后续查询被莫名打断** |
| `TestWatchdogSafety` | `test_query_timeout.py` | 在不安全方言上启动看门狗 → **进程挂死** |

---

## 7. 已知测试局限

| 局限 | 影响 | 缓解 |
|---|---|---|
| GBase 8s 的真机测试默认跳过 | 驱动行为变化可能漏检 | 发版前手动跑 `DCT_IT_DB=db_gb` |
| 无跨平台 CI | Unix-only 分支未验证 | 建议后续在 GitHub Actions 加 Linux/macOS job |
| 无性能/压力测试 | 大表流式取数只用小数据验证 | `execute_query_stream` 的分批逻辑有单测，但未压测 |
| 无并发竞态测试 | 连接池的线程安全靠代码审查 | 池操作有锁，但未做多线程压力验证 |
| GBase 8s / SQL Server 不支持查询级超时 | 这两类库无法限制单条查询的时长 | 实测受驱动限制（JDBC 不可重入 / pymssql 无语句级机制），已明确报错而非静默忽略；SQL Server 可在**连接级**配 `connect_args.timeout` |
