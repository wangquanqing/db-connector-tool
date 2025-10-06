"""
增强版 CLI 工具（可选）
使用rich库提供更美观的输出
"""

from .cli import create_parser

try:
    import rich

    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False

import getpass
import sys

from .cli import DBConnectorCLI


class EnhancedDBConnectorCLI(DBConnectorCLI):
    """增强版CLI，使用rich库提供更好的用户体验"""

    def __init__(self):
        super().__init__()
        if RICH_AVAILABLE:
            from rich.console import Console  # 局部导入确保安全

            self.console = Console()
        else:
            self.console = None

    def add_connection(self, args):
        """增强的添加连接功能"""
        if self.console and not args.password:
            # 交互式输入密码
            args.password = getpass.getpass("请输入密码: ")

        super().add_connection(args)

        if self.console:
            self.console.print("✅ [green]连接添加成功[/green]")

    def list_connections(self, args):
        """增强的连接列表显示"""
        self.init_db_manager()

        # 确保 db_manager 初始化成功
        if self.db_manager is None:
            if self.console:
                self.console.print("❌ [red]数据库管理器未初始化[/red]")
            else:
                print("❌ 数据库管理器未初始化")
            sys.exit(1)

        try:
            connections = self.db_manager.list_connections(args)

            if self.console:
                # 延迟导入 Table 以避免 Pylance 报错
                from rich.table import Table

                table = Table(title="数据库连接列表")
                table.add_column("序号", style="cyan")
                table.add_column("连接名称", style="magenta")
                table.add_column("状态", style="green")

                for i, conn in enumerate(connections, 1):
                    # 测试连接状态
                    status = (
                        "✅ 正常"
                        if self.db_manager.test_connection(conn)
                        else "❌ 失败"
                    )
                    table.add_row(str(i), conn, status)

                self.console.print(table)
            else:
                super().list_connections(args)

        except Exception as e:
            if self.console:
                self.console.print(f"❌ [red]列出连接失败: {e}[/red]")
            else:
                print(f"❌ 列出连接失败: {e}")
            sys.exit(1)

    def _display_results(self, results: list, format: str = "table"):
        """增强的结果显示"""
        if not results:
            if self.console:
                self.console.print("ℹ️ [yellow]没有结果[/yellow]")
            else:
                print("没有结果")
            return

        if self.console and format == "table":
            self._display_rich_table(results)
        else:
            super()._display_results(results, format)

    def _display_rich_table(self, results: list):
        """使用rich显示表格"""
        if not results or not self.console:
            return

        # 延迟导入 Table 以避免 Pylance 报错
        from rich.table import Table

        headers = list(results[0].keys())
        table = Table(title=f"查询结果 ({len(results)} 行)")

        # 添加列
        for header in headers:
            table.add_column(header, style="cyan")

        # 添加行（限制显示行数避免过大）
        for row in results[:100]:  # 只显示前100行
            table.add_row(*[str(row.get(header, "")) for header in headers])

        self.console.print(table)

        if len(results) > 100:
            self.console.print(
                f"📋 [yellow]只显示前100行，总共{len(results)}行[/yellow]"
            )


def main_enhanced():
    """增强版CLI入口点"""
    if not RICH_AVAILABLE:
        print("⚠️  rich库未安装，使用标准CLI")
        print("   安装增强功能: pip install rich")
        from .cli import main

        main()
    else:
        cli = EnhancedDBConnectorCLI()
        parser = create_parser()

        if len(sys.argv) == 1:
            parser.print_help()
            return

        args = parser.parse_args()

        if hasattr(args, "func"):
            args.func(cli, args)
        else:
            parser.print_help()


if __name__ == "__main__":
    main_enhanced()
