"""控制台输出编码降级的测试

**守的问题**：Windows 中文版控制台默认编码是 GBK，CLI 打印 emoji 时
（``📋`` / ``✅`` / ``❌``）会抛 ``UnicodeEncodeError`` 直接崩溃。
实测 ``list`` / ``query`` / ``test`` 三个命令全部退出码 1。

修法：入口处把 stdout/stderr 包一层「编码失败就丢弃该字符」的流。
关键要求：**中文必须保留**（GBK 支持），只丢 emoji。
"""

import io
import sys
import unittest
from unittest import mock

from src.db_connector_tool.utils.console import (
    _SafeStream,
    enable_safe_output,
    safe_stream,
)


def _gbk_stream() -> tuple:
    """构造一个 GBK 编码的文本流，返回 (包装后的流, 底层 BytesIO)"""
    buffer = io.BytesIO()
    text_stream = io.TextIOWrapper(buffer, encoding="gbk")
    return _SafeStream(text_stream), buffer


class TestSafeStream(unittest.TestCase):
    """``_SafeStream`` 的编码降级行为"""

    def test_chinese_preserved(self) -> None:
        """中文必须完整保留（GBK 支持中文）"""
        safe, buffer = _gbk_stream()
        safe.write("已配置的连接")
        safe.flush()
        self.assertEqual(buffer.getvalue().decode("gbk"), "已配置的连接")

    def test_emoji_dropped_not_crash(self) -> None:
        """emoji 被丢弃，而不是抛异常"""
        safe, buffer = _gbk_stream()
        # 不应抛 UnicodeEncodeError
        safe.write("📋")
        safe.flush()
        self.assertEqual(buffer.getvalue().decode("gbk"), "")

    def test_mixed_text_keeps_chinese(self) -> None:
        """中英混排 + emoji：保留可编码部分"""
        safe, buffer = _gbk_stream()
        safe.write("📋 已配置的连接: 3 个")
        safe.flush()
        decoded = buffer.getvalue().decode("gbk")
        self.assertIn("已配置的连接", decoded)
        self.assertIn("3", decoded)
        self.assertNotIn("📋", decoded)

    def test_checkmark_dropped(self) -> None:
        """✅ 被丢弃，后面的文字保留"""
        safe, buffer = _gbk_stream()
        safe.write("✅ 连接 'db_kes' 测试成功")
        safe.flush()
        decoded = buffer.getvalue().decode("gbk")
        self.assertIn("测试成功", decoded)
        self.assertNotIn("✅", decoded)

    def test_ascii_unchanged(self) -> None:
        """纯 ASCII 完全不受影响"""
        safe, buffer = _gbk_stream()
        safe.write("OK 200 rows")
        safe.flush()
        self.assertEqual(buffer.getvalue().decode("gbk"), "OK 200 rows")

    def test_write_returns_char_count(self) -> None:
        """``write`` 返回原文本长度（符合文件对象语义）"""
        safe, _ = _gbk_stream()
        text = "✅ 中文"
        self.assertEqual(safe.write(text), len(text))

    def test_attribute_passthrough(self) -> None:
        """其余属性透传给原流（如 isatty / fileno）"""
        safe, _ = _gbk_stream()
        self.assertFalse(safe.isatty())
        self.assertEqual(safe.encoding, "gbk")

    def test_flush_failure_swallowed(self) -> None:
        """原流 flush 失败时静默（不打断输出）"""
        stream = mock.Mock()
        stream.flush.side_effect = OSError("管道已关闭")
        safe = _SafeStream(stream)
        safe.flush()  # 不应抛异常

    def test_write_failure_swallowed(self) -> None:
        """原流写入失败时静默"""
        stream = mock.Mock()
        stream.write.side_effect = OSError("管道已关闭")
        stream.encoding = "gbk"
        safe = _SafeStream(stream)
        safe.write("中文")  # 不应抛异常


class TestSafeStreamHelper(unittest.TestCase):
    """``safe_stream`` / ``enable_safe_output`` 的行为"""

    def test_safe_stream_none(self) -> None:
        """None 原样返回"""
        self.assertIsNone(safe_stream(None))

    def test_safe_stream_idempotent(self) -> None:
        """已包装的流不再重复包装"""
        safe, _ = _gbk_stream()
        self.assertIs(safe_stream(safe), safe)

    def test_safe_stream_wraps_plain(self) -> None:
        """普通流会被包装"""
        stream = io.StringIO()
        wrapped = safe_stream(stream)
        self.assertIsInstance(wrapped, _SafeStream)

    def test_enable_safe_output_is_idempotent(self) -> None:
        """重复调用不会重复包装

        注意：不假设初始状态 —— 其它测试可能已经包装过 ``sys.stdout``。
        因此先复位到原始流，再验证「第一次包装、第二次不重复」。
        """
        original_out = sys.stdout
        original_err = sys.stderr
        try:
            # 复位为未包装的原始流，保证起点确定
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()

            first = enable_safe_output()
            self.assertTrue(first, msg="首次调用应执行包装")
            first_out = sys.stdout
            self.assertIsInstance(first_out, _SafeStream)

            second = enable_safe_output()
            self.assertFalse(second, msg="已包装后应返回 False")
            self.assertIs(sys.stdout, first_out, msg="不应重复包装")
        finally:
            sys.stdout = original_out
            sys.stderr = original_err

    def test_enable_safe_output_survives_missing_stream(self) -> None:
        """stdout 为 None 时不崩"""

        class FakeSys:
            """模拟 sys 模块"""

            stdout = None
            stderr = None

        with mock.patch.object(sys, "stdout", None), mock.patch.object(
            sys, "stderr", None
        ):
            # 不应抛异常
            enable_safe_output()


class TestCliEntryUsesSafeOutput(unittest.TestCase):
    """CLI 入口必须启用该保护（回归守卫）"""

    def test_main_calls_enable_safe_output(self) -> None:
        """``main()`` 一开始就调用 ``enable_safe_output``"""
        from src.db_connector_tool import cli

        called = []

        def _fake_enable() -> bool:
            called.append(1)
            return True

        # 让 main 在启用后立刻退出（无参数 → print_help + SystemExit(0)）
        with mock.patch.object(cli, "enable_safe_output", _fake_enable), \
                mock.patch.object(sys, "argv", ["db-connector"]):
            try:
                cli.main()
            except SystemExit:
                pass

        self.assertEqual(len(called), 1, msg="main() 必须在输出前启用编码降级")

    def test_cli_module_imports_helper(self) -> None:
        """``cli`` 模块确实导入了该函数"""
        from src.db_connector_tool import cli

        self.assertTrue(hasattr(cli, "enable_safe_output"))


if __name__ == "__main__":
    unittest.main()
