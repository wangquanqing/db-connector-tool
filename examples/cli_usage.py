"""
CLI工具使用示例
"""

import sys
import subprocess


def cli_usage_example():
    """CLI使用示例"""

    print("🚀 DB Connector CLI 使用示例")
    print("=" * 50)

    # 示例命令
    commands = [
        # 显示帮助
        ["db-connector", "--help"],
        # 列出命令
        ["db-connector", "list", "--help"],
        # 添加连接示例（这些只是示例，需要真实数据库）
        # ["db-connector", "add", "demo-mysql",
        #  "--type", "mysql",
        #  "--host", "localhost",
        #  "--user", "root",
        #  "--password", "password123",
        #  "--database", "testdb"],
        # ["db-connector", "add", "demo-sqlite",
        #  "--type", "sqlite",
        #  "--database", "/tmp/test.db"],
    ]

    for cmd in commands:
        print(f"\n💻 执行命令: {' '.join(cmd)}")
        print("-" * 40)

        try:
            result = subprocess.run(cmd, capture_output=True, text=True)
            print(f"退出码: {result.returncode}")
            if result.stdout:
                print("输出:")
                print(result.stdout)
            if result.stderr:
                print("错误:")
                print(result.stderr)
        except FileNotFoundError:
            print("❌ 找不到 db-connector 命令")
            print("请先安装包: pip install -e .")
            break
        except Exception as e:
            print(f"❌ 执行命令失败: {e}")


def generate_cli_cheatsheet():
    """生成CLI速查表"""

    cheatsheet = """
DB Connector CLI 速查表
=====================

连接管理:
--------
db-connector add <name> [options]      # 添加连接
db-connector list                      # 列出所有连接  
db-connector test <name>               # 测试连接
db-connector show <name>               # 显示连接详情
db-connector remove <name>             # 删除连接

查询执行:
--------
db-connector query <conn> <sql>        # 执行SQL查询
db-connector file <conn> <file>        # 执行SQL文件
db-connector shell <conn>              # 交互式SQL Shell

输出选项:
--------
--format table|json|csv               # 输出格式（默认: table）
--output <file>                       # 输出到文件

添加连接示例:
------------
# MySQL
db-connector add mysql-dev --type mysql --host localhost --user root --password 123456 --database testdb

# PostgreSQL  
db-connector add pg-dev --type postgresql --host localhost --user postgres --password 123456 --database testdb

# SQLite
db-connector add sqlite-dev --type sqlite --database /path/to/db.sqlite

查询示例:
--------
db-connector query mysql-dev "SELECT * FROM users"
db-connector query pg-dev "SELECT version()" --format json
db-connector query sqlite-dev "SELECT name FROM sqlite_master" --output tables.csv
    """

    print(cheatsheet)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "cheatsheet":
        generate_cli_cheatsheet()
    else:
        cli_usage_example()
