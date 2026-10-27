# SPDX-FileCopyrightText: 2025-present wangquanqing <wangquanqing1636@sina.com>
#
# SPDX-License-Identifier: MIT
"""控制台输出编码适配

让 CLI 的 emoji 在**非 UTF-8 控制台**（Windows 中文版的 GBK/cp936、
部分 CI 的 ASCII）上也能正常工作，而不是直接崩溃。

**问题**

CLI 到处用 emoji 做视觉标记（``📋`` / ``✅`` / ``❌`` / ``⏱️`` / ``⏹️``），
体验很好，但 Windows 中文版控制台的默认编码是 **GBK**，写不出这些字符：

.. code-block:: text

    UnicodeEncodeError: 'gbk' codec can't encode character '\\U0001f4cb'

实测受影响命令：``list`` / ``query`` / ``test``（都会打印 emoji），
用户一执行就抛异常退出 —— 这是**开箱即崩**级别的问题。

**解决思路**

不删 emoji（那是产品体验），而是在入口处把 ``stdout`` / ``stderr``
重新包装成「**编码失败不崩溃**」的流：

1. 目标编码优先用控制台真实编码（``sys.stdout.encoding``）；
2. 编码失败时退化为**去掉不可编码字符**（``errors="replace"`` 会产生
   ``?``，对中文友好；纯 emoji 场景用 ``ignore`` 更干净）；
3. 若整个流不可用（如被重定向到已关闭的管道），静默兜底。

**为什么不用 ``PYTHONIOENCODING``**

那是**用户侧**环境变量，不能让用户为了用工具去设；
库/CLI 应该自己处理好。也不能改 ``chcp 65001`` —— 那会改变用户控制台状态。

Example:
    >>> from db_connector_tool.utils.console import enable_safe_output
    >>> enable_safe_output()          # 幂等，可重复调用
"""

import sys
from typing import Any, Optional

from ..utils.logging_utils import get_logger

logger = get_logger(__name__)

__all__ = ["enable_safe_output", "safe_stream", "SUPPORTED_EMOJI_HINT"]


#: 供文档/提示使用的说明
SUPPORTED_EMOJI_HINT = "若控制台不支持 emoji，会自动降级为纯文本标记"


class _SafeStream:
    """包装 stdout/stderr：编码不了的字符**丢弃而不抛异常**

    只重写 ``write`` / ``flush``，其余属性透传给原流，
    因此 ``isatty()`` / ``encoding`` / ``fileno()`` 等行为保持不变。

    Attributes:
        _stream: 原始流
    """

    __slots__ = ("_stream",)

    def __init__(self, stream: Any) -> None:
        self._stream = stream

    def write(self, text: str) -> int:
        """写入文本；不可编码的字符被丢弃

        Args:
            text: 待写文本

        Returns:
            int: 声称写入的字符数（与原流语义一致）
        """
        stream = self._stream
        try:
            return stream.write(text)
        except UnicodeEncodeError:
            # 编码失败：把编不了的字符丢掉再写一次。
            # 中文在 GBK 下可编码，会被保留；只有 emoji 等被丢弃。
            encoding = getattr(stream, "encoding", None) or "ascii"
            safe = text.encode(encoding, errors="ignore").decode(
                encoding, errors="ignore"
            )
            try:
                stream.write(safe)
            except Exception:  # pylint: disable=broad-except
                pass
            return len(text)
        except Exception:  # pylint: disable=broad-except
            # 流已关闭 / 管道断开等：输出失败不该让业务崩溃
            return len(text)

    def flush(self) -> None:
        """刷新原流（失败静默）"""
        try:
            self._stream.flush()
        except Exception:  # pylint: disable=broad-except
            pass

    def __getattr__(self, name: str) -> Any:
        """其余属性透传给原流"""
        return getattr(self._stream, name)


def safe_stream(stream: Optional[Any]) -> Any:
    """把流包装为安全版本（已是安全版本或为 None 时原样返回）

    Args:
        stream: 原始流

    Returns:
        Any: 包装后的流；``None`` 原样返回
    """
    if stream is None or isinstance(stream, _SafeStream):
        return stream
    return _SafeStream(stream)


def enable_safe_output() -> bool:
    """让 ``stdout`` / ``stderr`` 在编码不支持时降级而非崩溃

    幂等：重复调用不会重复包装。

    Returns:
        bool: 是否做了包装（已包装或流不可用时返回 ``False``）

    Example:
        >>> enable_safe_output()      # doctest: +ELLIPSIS
        True
    """
    changed = False
    for name in ("stdout", "stderr"):
        stream = getattr(sys, name, None)
        if stream is None or isinstance(stream, _SafeStream):
            continue
        try:
            setattr(sys, name, _SafeStream(stream))
            changed = True
        except Exception as error:  # pylint: disable=broad-except
            logger.debug("包装 %s 失败: %s", name, error)
    if changed:
        logger.debug("已启用控制台输出编码降级")
    return changed
