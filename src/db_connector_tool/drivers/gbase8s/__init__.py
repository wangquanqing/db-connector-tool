"""GBase 8s（JayDeBeApi + JDBC）SQLAlchemy 方言包

按 SQLAlchemy 官方 Oracle 方言的文件组织形式拆分：

- ``base.py``：DBAPI 无关层（类型编译、能力矩阵、版本/schema/隔离级别、系统表反射）；
- ``jaydebeapi.py``：JayDeBeApi 适配层（连接参数、游标、回滚、断连识别）；
- ``types.py``：GBase 8s 类型体系（数字键 ischema_names、ObTimestamp、LVARCHAR/SERIAL/SERIAL8）；
- ``dictionary.py``：系统目录 SQL 与类型码解析；
- ``provision.py``：环境自检与验收辅助。

相对 ``gbase8sdb``（≥ V8.8_3.6.2）与 ``DbtPy``（< V8.8_3.6.2）原生驱动，
JDBC 通道可覆盖更宽的 GBase 8s 版本范围。

Example:
>>> from sqlalchemy import create_engine
>>> engine = create_engine("jdbcgbase8s://user:pwd@host:9088/db")
"""

from sqlalchemy.dialects import registry

from .base import GBase8sDialect, GBase8sTypeCompiler
from .jaydebeapi import GBase8sCursor, GBase8sDialect_jaydebeapi
from .types import LVARCHAR, SERIAL, SERIAL8, ObTimestamp

__all__ = [
    "GBase8sDialect",
    "GBase8sDialect_jaydebeapi",
    "GBase8sCursor",
    "GBase8sTypeCompiler",
    "LVARCHAR",
    "SERIAL",
    "SERIAL8",
    "ObTimestamp",
]

registry.register(
    "jdbcgbase8s",
    "db_connector_tool.drivers.gbase8s",
    "GBase8sDialect_jaydebeapi",
)