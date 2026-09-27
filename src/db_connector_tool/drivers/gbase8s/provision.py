"""GBase 8s 方言验收与环境自检辅助 (GBase8s Provision)

提供**离线可用**的自检与验收辅助：

- JDBC 驱动 JAR 定位（环境变量 ``GBASE8S_JDBC_JARPATH`` 优先，其次用户配置目录 ``jars``）；
- 连通性与版本探测 SQL；
- 建/删测试表的 DDL（供验收时连真库使用）。

Example:
>>> from db_connector_tool.drivers.gbase8s import provision
>>> provision.create_test_table_sql("dbct_probe")
'CREATE TABLE dbct_probe (id SERIAL PRIMARY KEY, name VARCHAR(64), created TIMESTAMP)'
"""

import os
import re
from pathlib import Path

from ...utils.path_utils import PathHelper

# 应用名（用户配置目录）
APP_NAME = "db_connector_tool"
# 驱动 JAR 环境变量
JAR_ENV_VAR = "GBASE8S_JDBC_JARPATH"
# 驱动 JAR 搜索目录名与通配符
JARS_DIR_NAME = "jars"
JAR_GLOB = "*gbase*.jar"
# 默认验收测试表名
DEFAULT_TEST_TABLE = "dbct_gbase8s_probe"

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def jar_search_dir() -> Path:
    """返回默认的驱动 JAR 搜索目录

    Returns:
        用户配置目录下的 ``jars`` 目录
    """
    return PathHelper.get_user_config_dir(APP_NAME) / JARS_DIR_NAME


def find_jdbc_jar() -> str | None:
    """定位 GBase JDBC 驱动 JAR 文件

    Returns:
        JAR 文件绝对路径；未找到时返回 None
    """
    jar_path = os.environ.get(JAR_ENV_VAR)
    if jar_path and os.path.exists(jar_path):
        return jar_path

    files = [p for p in jar_search_dir().rglob(JAR_GLOB) if p.is_file()]
    return str(files[0]) if files else None


def probe_sql() -> str:
    """返回连通性与版本探测 SQL

    Returns:
        可在 oracle 兼容模式下直接执行的探测 SQL
    """
    return "select dbinfo('version','full') from dual"


def _validate_identifier(identifier: str) -> str:
    """校验表名标识符

    Args:
        identifier: 表名

    Returns:
        校验通过的表名

    Raises:
        ValueError: 表名不是合法的标识符
    """
    if not _IDENTIFIER_PATTERN.match(identifier):
        raise ValueError("非法表名: %r" % identifier)
    return identifier


def create_test_table_sql(table_name: str = DEFAULT_TEST_TABLE) -> str:
    """返回建测试表的 DDL

    Args:
        table_name: 表名（仅允许字母/数字/下划线）

    Returns:
        CREATE TABLE 语句
    """
    table = _validate_identifier(table_name)
    return (
        "CREATE TABLE %s (id SERIAL PRIMARY KEY, name VARCHAR(64), "
        "created TIMESTAMP)" % table
    )


def drop_test_table_sql(table_name: str = DEFAULT_TEST_TABLE) -> str:
    """返回删测试表的 DDL

    Args:
        table_name: 表名（仅允许字母/数字/下划线）

    Returns:
        DROP TABLE 语句
    """
    return "DROP TABLE %s" % _validate_identifier(table_name)