"""
增强版 CLI 工具（可选）
========================

使用 rich 库提供更美观的终端输出和交互体验。

特性:
- 彩色表格显示
- 进度指示器
- 更好的错误提示
- 交互式密码输入

依赖:
    pip install rich

版本: 1.0.0
作者: DB Connector Team
"""

import getpass
import sys
from typing import Any, List, Optional

from .cli import DBConnectorCLI, create_parser

# 检查 rich 库是否可用
try:
    import rich  # noqa: F401
    RICH_AVAILABLE = True
except ImportError:
    RICH_AVAILABLE = False


class EnhancedDBConnectorCLI(DBConnectorCLI):
    """
    增强版数据库连接器 CLI
    
    继承自基础 CLI 类，使用 rich 库提供更好的用户体验。
    
    Attributes:
        console (Optional[rich.console.Console]): rich 控制台实例，如果 rich 可用
    """

    def __init__(self) -> None:
        """
        初始化增强版 CLI
        
        如果 rich 库可用，则创建控制台实例；否则使用标准输出。
        """
        super().__init__()
        self.console: Optional[Any] = None
        
        if RICH_AVAILABLE:
            from rich.console import Console
            self.console = Console()

    def add_connection(self, args: Any) -> None:
        """
        增强的添加连接功能
        
        提供交互式密码输入和彩色成功提示。
        
        Args:
            args: 命令行参数对象
            
        Raises:
            SystemExit: 如果添加连接失败
        """
        # 如果 rich 可用且密码未提供，则交互式输入密码
        if self.console and not getattr(args, 'password', None):
            args.password = getpass.getpass("请输入密码: ")

        # 调用父类方法执行实际添加操作
        super().add_connection(args)

        # 显示成功提示
        if self.console:
            self.console.print("✅ [bold green]连接添加成功[/bold green]")

    def list_connections(self, args: Any) -> None:
        """
        增强的连接列表显示功能
        
        使用 rich 表格显示连接列表，包含连接状态测试。
        
        Args:
            args: 命令行参数对象
            
        Raises:
            SystemExit: 如果列出连接失败或数据库管理器未初始化
        """
        # 初始化数据库管理器
        self._ensure_db_manager_initialized()

        try:
            connections = self.db_manager.list_connections(args)

            if self.console:
                self._display_connections_with_rich(connections)
            else:
                super().list_connections(args)

        except Exception as e:
            error_msg = f"列出连接失败: {e}"
            self._handle_error(error_msg)

    def _display_connections_with_rich(self, connections: List[str]) -> None:
        """
        使用 rich 表格显示连接列表
        
        Args:
            connections: 连接名称列表
        """
        from rich.table import Table

        table = Table(title="📋 数据库连接列表", show_header=True, header_style="bold magenta")
        table.add_column("序号", style="cyan", justify="center")
        table.add_column("连接名称", style="magenta")
        table.add_column("状态", style="green", justify="center")

        for i, conn_name in enumerate(connections, 1):
            # 测试连接状态
            try:
                is_connected = self.db_manager.test_connection(conn_name)
                status = "✅ [bold green]正常[/bold green]" if is_connected else "❌ [bold red]失败[/bold red]"
            except Exception:
                status = "❓ [yellow]未知[/yellow]"
            
            table.add_row(str(i), conn_name, status)

        self.console.print(table)
        
        # 显示统计信息
        if connections:
            self.console.print(f"📊 总共 {len(connections)} 个连接配置")

    def _display_results(self, results: List[dict], format: str = "table") -> None:
        """
        增强的查询结果显示功能
        
        Args:
            results: 查询结果列表
            format: 输出格式，支持 'table' 或 'json'
        """
        if not results:
            self._display_no_results()
            return

        if self.console and format == "table":
            self._display_rich_table(results)
        else:
            super()._display_results(results, format)

    def _display_no_results(self) -> None:
        """显示无结果的提示信息"""
        if self.console:
            self.console.print("ℹ️  [yellow]没有查询到结果[/yellow]")
        else:
            print("ℹ️  没有查询到结果")

    def _display_rich_table(self, results: List[dict]) -> None:
        """
        使用 rich 表格显示查询结果
        
        Args:
            results: 查询结果字典列表
        """
        if not results or not self.console:
            return

        from rich.table import Table

        # 获取表头
        headers = list(results[0].keys())
        
        # 创建表格
        table = Table(
            title=f"🔍 查询结果 ({len(results)} 行)", 
            show_header=True, 
            header_style="bold cyan"
        )

        # 添加列
        for header in headers:
            table.add_column(header, style="cyan", overflow="fold")

        # 添加数据行（限制显示数量避免终端溢出）
        max_display_rows = 100
        display_results = results[:max_display_rows]
        
        for row in display_results:
            table.add_row(*[str(row.get(header, "")) for header in headers])

        self.console.print(table)

        # 如果结果超过显示限制，显示提示信息
        if len(results) > max_display_rows:
            self.console.print(
                f"📋 [yellow]只显示前 {max_display_rows} 行，总共 {len(results)} 行[/yellow]"
            )

    def _handle_error(self, error_message: str) -> None:
        """
        统一的错误处理
        
        Args:
            error_message: 错误消息
            
        Raises:
            SystemExit: 总是退出程序
        """
        if self.console:
            self.console.print(f"❌ [bold red]{error_message}[/bold red]")
        else:
            print(f"❌ {error_message}")
        sys.exit(1)


def main_enhanced() -> None:
    """
    增强版 CLI 入口点
    
    如果 rich 库可用，使用增强版 CLI；否则回退到标准 CLI。
    """
    if not RICH_AVAILABLE:
        print("⚠️  rich 库未安装，使用标准 CLI")
        print("   安装增强功能: pip install rich")
        
        # 回退到标准 CLI
        from .cli import main
        main()
    else:
        # 使用增强版 CLI
        cli = EnhancedDBConnectorCLI()
        parser = create_parser()

        # 如果没有参数，显示帮助信息
        if len(sys.argv) == 1:
            parser.print_help()
            return

        # 解析参数并执行相应功能
        args = parser.parse_args()
        
        if hasattr(args, "func"):
            args.func(cli, args)
        else:
            parser.print_help()


if __name__ == "__main__":
    main_enhanced()