"""GBase 8s JDBC 驱动自动下载测试 (jar_downloader)

全部离线：用临时目录伪装 Maven 仓库（``file://`` 与本地路径两种形式），
真实走一遍元数据解析、SHA-1 校验、ZIP 头校验与原子写入。
"""

import hashlib
import os
import tempfile
import unittest
import urllib.error
from pathlib import Path
from unittest import mock

from src.db_connector_tool.drivers.gbase8s import jar_downloader
from src.db_connector_tool.drivers.gbase8s.jar_downloader import (
    JarDownloadError,
    auto_download_enabled,
    download_latest_jar,
    download_status,
    maven_repo_url,
    parse_version_key,
    reset_download_state,
    select_latest_version,
)
FAKE_JAR = b"PK\x03\x04" + b"fake-jdbc-driver-payload" * 8
VERSION = "3.6.5.21_nobson"
OLD_VERSION = "3.6.3.34"


def build_repo(root: Path, version: str = VERSION, payload: bytes = FAKE_JAR) -> Path:
    """在临时目录里搭一个 Maven 仓库布局"""
    group_dir = root / "com" / "gbasedbt" / "jdbc"
    artifact_dir = group_dir / version
    artifact_dir.mkdir(parents=True, exist_ok=True)

    (group_dir / "maven-metadata.xml").write_text(
        f"""<?xml version="1.0" encoding="UTF-8"?>
<metadata>
  <groupId>com.gbasedbt</groupId>
  <artifactId>jdbc</artifactId>
  <versioning>
    <latest>{version}</latest>
    <release>{version}</release>
    <versions>
      <version>{OLD_VERSION}</version>
      <version>{version}</version>
    </versions>
  </versioning>
</metadata>
""",
        encoding="utf-8",
    )
    (artifact_dir / f"jdbc-{version}.jar").write_bytes(payload)
    (artifact_dir / f"jdbc-{version}.jar.sha1").write_text(
        hashlib.sha1(payload).hexdigest(), encoding="utf-8"
    )
    return root


class TestConfiguration(unittest.TestCase):
    """环境变量开关与取值"""

    def setUp(self) -> None:
        reset_download_state()

    def test_enabled_by_default(self) -> None:
        """默认启用自动下载"""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(jar_downloader.AUTO_DOWNLOAD_ENV, None)
            self.assertTrue(auto_download_enabled())

    def test_disable_values(self) -> None:
        """0/false/no/off 均视为关闭"""
        for value in ("0", "false", "FALSE", "no", "off"):
            with self.subTest(value=value):
                with mock.patch.dict(
                    os.environ, {jar_downloader.AUTO_DOWNLOAD_ENV: value}
                ):
                    self.assertFalse(auto_download_enabled())

    def test_repo_url_override_and_trailing_slash(self) -> None:
        """仓库地址可覆盖，并去掉尾部斜杠"""
        with mock.patch.dict(
            os.environ, {jar_downloader.MAVEN_REPO_ENV: "https://mirror.local/maven/"}
        ):
            self.assertEqual(maven_repo_url(), "https://mirror.local/maven")

    def test_repo_url_default(self) -> None:
        """未配置时使用 Maven Central"""
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(jar_downloader.MAVEN_REPO_ENV, None)
            self.assertEqual(maven_repo_url(), jar_downloader.DEFAULT_MAVEN_REPO)

    def test_invalid_numeric_env_falls_back(self) -> None:
        """非法的超时/大小取值回落到默认值"""
        with mock.patch.dict(
            os.environ, {jar_downloader.DOWNLOAD_TIMEOUT_ENV: "abc"}
        ):
            self.assertEqual(
                jar_downloader._env_number(  # pylint: disable=protected-access
                    jar_downloader.DOWNLOAD_TIMEOUT_ENV,
                    jar_downloader.DEFAULT_TIMEOUT_SECONDS,
                ),
                jar_downloader.DEFAULT_TIMEOUT_SECONDS,
            )


class TestVersionSelection(unittest.TestCase):
    """版本解析与选择"""

    def test_parse_version_key(self) -> None:
        """版本号按数字段比较，非数字后缀被忽略"""
        self.assertEqual(parse_version_key("3.6.5.21_nobson"), (3, 6, 5, 21))
        self.assertGreater(
            parse_version_key("3.6.5.21_nobson"), parse_version_key("3.6.3.34")
        )
        self.assertGreater(parse_version_key("3.10.0"), parse_version_key("3.9.9"))

    def test_select_latest_prefers_release(self) -> None:
        """优先采用 metadata 中的 release 字段"""
        self.assertEqual(select_latest_version(["1.0"], "2.0"), "2.0")

    def test_select_latest_without_release(self) -> None:
        """没有 release 时按数字段取最大"""
        self.assertEqual(
            select_latest_version(["3.6.3.34", "3.6.5.21_nobson", "2.0.1.26"]),
            "3.6.5.21_nobson",
        )

    def test_select_latest_empty(self) -> None:
        """空版本列表报错"""
        with self.assertRaises(JarDownloadError):
            select_latest_version([])


class TestVersionValidation(unittest.TestCase):
    """版本号必须能安全地用作文件名

    Maven 元数据来自仓库（可能是镜像/私服），``<release>`` 不可信：
    含路径分隔符或 ``..`` 时会逃逸出目标目录，必须拒绝。
    """

    MALICIOUS = (
        "../../../../escaped-version",
        "..",
        "3.6.5/../../evil",
        "3.6.5\\..\\evil",
        "3.6.5:21*?",  # Windows 非法字符
        "",
        "   ",
        "3.6.5\n21",
    )

    def test_rejects_malicious_versions(self) -> None:
        """含路径穿越、分隔符或非法字符的版本号被拒绝"""
        for version in self.MALICIOUS:
            with self.subTest(version=version):
                self.assertFalse(
                    jar_downloader.is_safe_version(version),
                    f"{version!r} 不应被视为安全版本号",
                )

    def test_accepts_real_versions(self) -> None:
        """真实版本号通过校验"""
        for version in ("3.6.5.21_nobson", "3.6.3.34", "2.0.1.26", "3.5.1"):
            with self.subTest(version=version):
                self.assertTrue(jar_downloader.is_safe_version(version))

    def test_select_latest_version_skips_unsafe(self) -> None:
        """挑选最新版本时跳过不安全的版本号"""
        versions = ["3.6.3.34", "../../../../evil"]
        self.assertEqual(select_latest_version(versions), "3.6.3.34")

    def test_select_latest_version_rejects_all_unsafe(self) -> None:
        """全部不安全时抛错"""
        with self.assertRaises(JarDownloadError):
            select_latest_version(["../../../evil", "3.6.5/.."])

    def test_download_refuses_traversal_release(self) -> None:
        """元数据 release 含穿越时既不下载也不越权落盘"""
        with tempfile.TemporaryDirectory(prefix="dct_trav_") as tmp:
            root = Path(tmp)
            repo = root / "repo"
            group = repo / "com" / "gbasedbt" / "jdbc"
            group.mkdir(parents=True)
            evil = "../../../../escaped-version"
            (group / "maven-metadata.xml").write_text(
                f"<metadata><versioning><release>{evil}</release>"
                f"<versions><version>{evil}</version></versions>"
                "</versioning></metadata>",
                encoding="utf-8",
            )
            target = root / "jars"

            with mock.patch.dict(
                os.environ,
                {
                    jar_downloader.MAVEN_REPO_ENV: str(repo),
                    jar_downloader.AUTO_DOWNLOAD_ENV: "1",
                },
            ):
                reset_download_state()
                self.assertIsNone(download_latest_jar(target))

            escaped = [
                path
                for path in root.rglob("*")
                if path.is_file() and "escaped-version" in path.name
            ]
            self.assertEqual(escaped, [], "不应在目标目录之外落盘")

    def test_illegal_filename_is_handled(self) -> None:
        """Windows 非法字符不会导致异常逃逸，只记录失败"""
        with tempfile.TemporaryDirectory(prefix="dct_badname_") as tmp:
            root = Path(tmp)
            repo = root / "repo"
            group = repo / "com" / "gbasedbt" / "jdbc"
            group.mkdir(parents=True)
            bad = "3.6.5:21*?"
            (group / "maven-metadata.xml").write_text(
                f"<metadata><versioning><release>{bad}</release>"
                f"<versions><version>{bad}</version></versions>"
                "</versioning></metadata>",
                encoding="utf-8",
            )

            with mock.patch.dict(
                os.environ,
                {
                    jar_downloader.MAVEN_REPO_ENV: str(repo),
                    jar_downloader.AUTO_DOWNLOAD_ENV: "1",
                },
            ):
                reset_download_state()
                self.assertIsNone(download_latest_jar(root / "jars"))


class TestDownloadFromLocalRepo(unittest.TestCase):
    """用本地目录伪装仓库，完整走一遍下载流程"""

    def setUp(self) -> None:
        reset_download_state()
        # 生命周期覆盖整个用例，tearDown 中统一释放
        self.temp_dir = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
            prefix="dct_jar_repo_"
        )
        self.root = Path(self.temp_dir.name)
        self.repo = build_repo(self.root / "repo")
        self.target = self.root / "jars"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _env(self, repo_url: str) -> dict:
        return {
            jar_downloader.MAVEN_REPO_ENV: repo_url,
            jar_downloader.AUTO_DOWNLOAD_ENV: "1",
        }

    def test_download_with_local_path(self) -> None:
        """本地路径形式的仓库可完成下载与校验"""
        with mock.patch.dict(os.environ, self._env(str(self.repo))):
            result = download_latest_jar(self.target)

        self.assertIsNotNone(result)
        assert result is not None
        # 落盘名带 gbasedbt 关键字，确保能被驱动搜索逻辑识别
        self.assertEqual(result.name, f"gbasedbt-jdbc-{VERSION}.jar")
        self.assertEqual(result.read_bytes(), FAKE_JAR)
        self.assertEqual(result.parent, self.target)

    def test_download_with_file_uri(self) -> None:
        """file:// 形式的仓库地址同样可用"""
        with mock.patch.dict(os.environ, self._env(self.repo.as_uri())):
            result = download_latest_jar(self.target)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.read_bytes(), FAKE_JAR)

    def test_second_call_reuses_existing_file(self) -> None:
        """已下载过则复用现有文件，不重复写入"""
        with mock.patch.dict(os.environ, self._env(str(self.repo))):
            first = download_latest_jar(self.target)
            assert first is not None
            stamp = first.stat().st_mtime_ns

            second = download_latest_jar(self.target)

        self.assertEqual(first, second)
        self.assertEqual(second.stat().st_mtime_ns, stamp, "不应重写已有文件")

    def test_disabled_skips_everything(self) -> None:
        """关闭开关时完全不访问仓库"""
        with mock.patch.dict(
            os.environ,
            {
                jar_downloader.MAVEN_REPO_ENV: str(self.repo),
                jar_downloader.AUTO_DOWNLOAD_ENV: "off",
            },
        ):
            self.assertIsNone(download_latest_jar(self.target))

        self.assertFalse(self.target.exists() and any(self.target.iterdir()))

    def test_no_temp_files_left(self) -> None:
        """原子写入不留下 .part 临时文件"""
        with mock.patch.dict(os.environ, self._env(str(self.repo))):
            download_latest_jar(self.target)
        leftovers = [p.name for p in self.target.iterdir() if p.suffix == ".part"]
        self.assertEqual(leftovers, [])


class TestDownloadFailures(unittest.TestCase):
    """各类失败路径都不留坏文件，且返回 None"""

    def setUp(self) -> None:
        reset_download_state()
        # 生命周期覆盖整个用例，tearDown 中统一释放
        self.temp_dir = tempfile.TemporaryDirectory(  # pylint: disable=consider-using-with
            prefix="dct_jar_fail_"
        )
        self.root = Path(self.temp_dir.name)
        self.target = self.root / "jars"

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _download(self, repo: Path) -> None:
        with mock.patch.dict(
            os.environ,
            {
                jar_downloader.MAVEN_REPO_ENV: str(repo),
                jar_downloader.AUTO_DOWNLOAD_ENV: "1",
            },
        ):
            self.assertIsNone(download_latest_jar(self.target))

    def test_sha1_mismatch_rejected(self) -> None:
        """SHA-1 不匹配时拒绝落盘"""
        repo = build_repo(self.root / "bad_sha")
        sha_file = repo / "com" / "gbasedbt" / "jdbc" / VERSION / f"jdbc-{VERSION}.jar.sha1"
        sha_file.write_text("0" * 40, encoding="utf-8")

        self._download(repo)

        self.assertFalse(self.target.exists() and any(self.target.iterdir()))
        self.assertIn("SHA-1", download_status()["last_error"] or "")

    def test_non_zip_payload_rejected(self) -> None:
        """非 ZIP 内容（缺少 jar 文件头）被拒绝"""
        repo = build_repo(self.root / "bad_zip", payload=b"not-a-jar")
        self._download(repo)
        self.assertIn("ZIP", download_status()["last_error"] or "")

    def test_missing_metadata(self) -> None:
        """元数据缺失时报错但不抛异常"""
        empty_repo = self.root / "empty"
        empty_repo.mkdir()
        self._download(empty_repo)
        self.assertIsNotNone(download_status()["last_error"])

    def test_malformed_metadata(self) -> None:
        """元数据不是合法 XML"""
        repo = self.root / "malformed"
        group_dir = repo / "com" / "gbasedbt" / "jdbc"
        group_dir.mkdir(parents=True)
        (group_dir / "maven-metadata.xml").write_text("<metadata>", encoding="utf-8")

        self._download(repo)
        self.assertIn("解析失败", download_status()["last_error"] or "")

    def test_size_limit(self) -> None:
        """超过大小上限时中止下载"""
        repo = build_repo(self.root / "big")
        with mock.patch.dict(
            os.environ,
            {
                jar_downloader.MAVEN_REPO_ENV: str(repo),
                jar_downloader.AUTO_DOWNLOAD_ENV: "1",
                jar_downloader.DOWNLOAD_MAX_SIZE_ENV: "8",
            },
        ):
            self.assertIsNone(download_latest_jar(self.target))
        self.assertIn("大小上限", download_status()["last_error"] or "")

    def test_network_error_is_swallowed(self) -> None:
        """网络异常只记录日志并返回 None"""
        with mock.patch.dict(
            os.environ,
            {
                jar_downloader.MAVEN_REPO_ENV: "https://unreachable.invalid/maven",
                jar_downloader.AUTO_DOWNLOAD_ENV: "1",
            },
        ):
            with mock.patch(
                "src.db_connector_tool.drivers.gbase8s.jar_downloader._open_url",
                side_effect=urllib.error.URLError("boom"),
            ):
                self.assertIsNone(download_latest_jar(self.target))

        self.assertIsNotNone(download_status()["last_error"])

    def test_failed_version_not_retried_in_process(self) -> None:
        """同一版本在本进程内只尝试一次，避免反复重试"""
        repo = build_repo(self.root / "retry")
        sha_file = (
            repo / "com" / "gbasedbt" / "jdbc" / VERSION / f"jdbc-{VERSION}.jar.sha1"
        )
        sha_file.write_text("0" * 40, encoding="utf-8")

        with mock.patch.dict(
            os.environ,
            {
                jar_downloader.MAVEN_REPO_ENV: str(repo),
                jar_downloader.AUTO_DOWNLOAD_ENV: "1",
            },
        ):
            self.assertIsNone(download_latest_jar(self.target))
            with mock.patch(
                "src.db_connector_tool.drivers.gbase8s.jar_downloader._open_url"
            ) as open_url:
                open_url.side_effect = AssertionError("不应再次访问仓库")
                self.assertIsNone(download_latest_jar(self.target))

        self.assertEqual(download_status()["attempted_versions"], [VERSION])

    def test_metadata_parsed_once_per_repo(self) -> None:
        """版本解析按仓库缓存，多次调用只读一次元数据"""
        repo = build_repo(self.root / "cached")

        with mock.patch.dict(
            os.environ,
            {
                jar_downloader.MAVEN_REPO_ENV: str(repo),
                jar_downloader.AUTO_DOWNLOAD_ENV: "1",
            },
        ):
            self.assertEqual(
                jar_downloader.resolve_latest_version(str(repo), 5.0), VERSION
            )
            with mock.patch(
                "src.db_connector_tool.drivers.gbase8s.jar_downloader._open_url"
            ) as open_url:
                open_url.side_effect = AssertionError("不应再次读取元数据")
                self.assertEqual(
                    jar_downloader.resolve_latest_version(str(repo), 5.0), VERSION
                )

        self.assertEqual(download_status()["resolved_versions"][str(repo)], VERSION)


if __name__ == "__main__":
    unittest.main()
