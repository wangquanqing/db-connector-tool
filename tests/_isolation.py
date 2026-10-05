"""测试公共辅助模块 (Test Isolation Helpers)

测试必须与宿主环境完全隔离，否则会出现两类问题：

1. **污染用户数据**：``ConfigManager`` / ``KeyManager`` 默认把配置与密钥写进
   真实用户配置目录，测试运行会在 ``%APPDATA%``（或 ``~/.config``）里留下
   大量 ``test_app_*`` 目录；
2. **依赖宿主状态而随机失败**：``KeyManager`` 默认优先使用操作系统密钥环，
   在凭据库不可用（服务未启动、拒绝写入等）的环境下测试会随机报错。

本模块提供 :class:`IsolatedTestCase`：把配置目录重定向到临时目录，
并强制关闭真实 keyring，让测试只依赖文件密钥方案。
"""

import logging
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from src.db_connector_tool.core import key_manager as key_manager_module
from src.db_connector_tool.core.key_manager import KeyManager
from src.db_connector_tool.utils.path_utils import PathHelper

# 需要在测试期间清空的环境变量（避免继承宿主机的密钥配置）
_ENV_KEYS = ("DB_CONNECTOR_TOOL_ENCRYPTION_KEY", "DB_CONNECTOR_TOOL_HMAC_KEY")


def close_file_handlers() -> None:
    """关闭所有 logger 上仍然打开的文件 handler

    Windows 上被占用的日志文件无法删除，会让依赖 ``rmtree`` 的 tearDown 失败。
    """
    for name in list(logging.Logger.manager.loggerDict):
        logger = logging.getLogger(name)
        for handler in logger.handlers[:]:
            if isinstance(handler, logging.FileHandler):
                logger.removeHandler(handler)
                handler.close()


class IsolatedTestCase(unittest.TestCase):
    """隔离配置目录与密钥存储的测试基类

    子类可以直接使用 ``self.temp_path``（指向本次测试专属的临时目录），
    所有配置、密钥、日志都会落在该目录内，测试结束后自动删除。
    """

    def setUp(self) -> None:
        """创建临时配置目录并启用隔离补丁"""
        super().setUp()
        # 生命周期跨越整个用例，由 tearDown 中的 cleanup() 负责释放
        self._temp_dir = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
            prefix="dct_test_"
        )
        self.temp_path = Path(self._temp_dir.name)
        self._start_patches()

    def _start_patches(self) -> None:
        """启用隔离补丁，并注册到 cleanup 以便自动恢复"""
        patchers = [
            mock.patch.object(
                PathHelper,
                "get_user_config_dir",
                staticmethod(lambda app_name: self.temp_path),
            ),
            # 强制走文件密钥方案：keyring 在 CI/受限环境下不可靠
            mock.patch.object(key_manager_module, "keyring_available", False),
            mock.patch.object(key_manager_module, "keyring_module", None),
            # 重置 KeyManager 的类级依赖检查缓存
            mock.patch.object(KeyManager, "_dependencies_checked", False),
            mock.patch.object(KeyManager, "_env_key", None),
            mock.patch.object(KeyManager, "_env_key_available", False),
            mock.patch.dict(
                os.environ, {key: "" for key in _ENV_KEYS}, clear=False
            ),
            # 静音被测代码的日志（负向用例会主动制造告警，输出噪声极大）
            mock.patch.object(
                logging.getLogger("src.db_connector_tool"), "level", logging.CRITICAL
            ),
            mock.patch.object(
                logging.getLogger("src.db_connector_tool"), "propagate", False
            ),
            mock.patch.object(
                logging.getLogger("db_connector_tool"), "level", logging.CRITICAL
            ),
            mock.patch.object(
                logging.getLogger("db_connector_tool"), "propagate", False
            ),
        ]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

    def tearDown(self) -> None:
        """关闭日志文件句柄并删除临时目录"""
        close_file_handlers()
        self._temp_dir.cleanup()
        super().tearDown()
