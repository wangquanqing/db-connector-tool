"""GBase 8s JDBC 驱动 jar 的自动获取 (Maven Central)

本地各搜索目录都找不到 JDBC 驱动时，可从 Maven Central 拉取最新版本到
用户配置目录（Windows 为 ``%APPDATA%\\db_connector_tool\\jars``）。

行为约束：

* 默认开启；设置 ``GBASEDBT_JDBC_AUTO_DOWNLOAD=0/false/no/off`` 关闭；
* 可用 ``GBASEDBT_JDBC_MAVEN_REPO`` 指定镜像、私有仓库或本地目录
  （``file://`` 形式亦可），便于离线/内网分发；
* 下载先写临时文件，校验 **SHA-1** 与 **ZIP 魔数**后再原子替换，
  避免半截文件被后续连接误用；
* 限制响应体大小（默认 40MB）与超时（默认 20 秒），失败只记录日志，
  并在进程内缓存失败结果，避免每次建连都重试。

Maven 坐标：``com.gbasedbt:jdbc``，最新版本取 ``maven-metadata.xml`` 的
``<release>``（例如 ``3.6.5.21_nobson``）。
"""

from __future__ import annotations

import hashlib
import os
import re
import tempfile
import threading
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from xml.etree import ElementTree

from ...utils.logging_utils import get_logger

logger = get_logger(__name__)

# Maven 坐标与默认仓库
MAVEN_GROUP_ID = "com.gbasedbt"
MAVEN_ARTIFACT_ID = "jdbc"
DEFAULT_MAVEN_REPO = "https://repo1.maven.org/maven2"

# 环境变量
AUTO_DOWNLOAD_ENV = "GBASEDBT_JDBC_AUTO_DOWNLOAD"
MAVEN_REPO_ENV = "GBASEDBT_JDBC_MAVEN_REPO"
DOWNLOAD_TIMEOUT_ENV = "GBASEDBT_JDBC_DOWNLOAD_TIMEOUT"
DOWNLOAD_MAX_SIZE_ENV = "GBASEDBT_JDBC_DOWNLOAD_MAX_SIZE"

# 下载参数
# 默认超时偏保守：这是**建连路径**上的操作，首次离线访问最多等待该时间。
DEFAULT_TIMEOUT_SECONDS = 8.0
DEFAULT_MAX_SIZE_BYTES = 40 * 1024 * 1024  # GBase JDBC jar 约 3MB，留足余量

# 视为「关闭」的环境变量取值
_FALSY_VALUES = {"0", "false", "no", "off", ""}

# jar 文件头（ZIP 魔数）
_ZIP_MAGIC = b"PK\x03\x04"

# 版本号解析：取数字段用于比较，例如 3.6.5.21_nobson -> (3, 6, 5, 21)
_VERSION_NUMBER_PATTERN = re.compile(r"\d+")

# 下载落盘使用的文件名前缀。刻意带上 gbasedbt 关键字，确保能被
# jaydebeapi._first_jar_in 的关键字识别到（Maven 原始名
# ``jdbc-3.6.5.21_nobson.jar`` 不含 gbase 关键字，会被过滤掉）。
LOCAL_JAR_PREFIX = "gbasedbt-jdbc"

# 兼容识别 Maven 规范命名 ``jdbc-<数字...>.jar``
MAVEN_JAR_NAME_PATTERN = re.compile(r"^jdbc-\d[\w.\-]*\.jar$", re.IGNORECASE)

# 版本号白名单：字母数字与 . _ + -（Maven 常见字符集）。
# 版本号会被拼进 URL 与**文件名**，而元数据来自仓库（可能是镜像/私服），
# 因此必须拒绝路径分隔符、``..``、驱动保留字符等，避免路径穿越。
_SAFE_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]*$")

# 单个版本号长度上限（防御性）
_MAX_VERSION_LENGTH = 64

# 进程内状态：按仓库缓存解析出的版本（None 表示该仓库解析失败）、
# 已尝试过的版本与最后一次失败原因。缓存的意义是避免每次建连都去访问仓库。
_VERSION_CACHE: Dict[str, Optional[str]] = {}
_ATTEMPTED_VERSIONS: Set[str] = set()
_LAST_ERROR: Optional[str] = None

# 串行化下载过程：多线程同时建连时，避免重复下载与重复写盘
_DOWNLOAD_LOCK = threading.Lock()


class JarDownloadError(RuntimeError):
    """驱动 jar 自动下载失败"""


def _env_flag(name: str, default: bool) -> bool:
    """读取布尔型环境变量"""
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in _FALSY_VALUES


def _env_number(name: str, default: float) -> float:
    """读取数值型环境变量，非法值回落到默认值"""
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning("环境变量 %s 取值非法(%s)，使用默认值 %s", name, raw, default)
        return default
    return value if value > 0 else default


def auto_download_enabled() -> bool:
    """自动下载是否启用（默认启用）

    Example:
        >>> os.environ[AUTO_DOWNLOAD_ENV] = "0"
        >>> auto_download_enabled()
        False
    """
    return _env_flag(AUTO_DOWNLOAD_ENV, True)


def maven_repo_url() -> str:
    """返回 Maven 仓库基地址（可被环境变量覆盖）

    使用非 https 的远程仓库时会给出告警：驱动 jar 会随程序加载执行，
    明文传输存在被篡改的风险（本地路径与 ``file://`` 不受影响）。
    """
    raw = os.environ.get(MAVEN_REPO_ENV, "").strip()
    base = raw or DEFAULT_MAVEN_REPO
    base = base.rstrip("/")

    parsed = urllib.parse.urlparse(base)
    if parsed.scheme == "http":
        logger.warning(
            "GBase JDBC 驱动仓库使用明文 HTTP（%s），存在被篡改风险，"
            "建议改用 https 或本地路径",
            base,
        )
    return base


def is_safe_version(version: str) -> bool:
    """判断版本号是否可安全地用于 URL 与文件名

    Maven 元数据由仓库提供（可能是镜像/私服，不完全可信），版本号会被拼进
    下载 URL 与本地文件名，因此只接受字母数字与 ``. _ + -``，拒绝路径分隔符、
    ``..``、Windows 保留字符等。

    Args:
        version: 待校验的版本号

    Returns:
        bool: 是否安全

    Example:
        >>> is_safe_version("3.6.5.21_nobson")
        True
        >>> is_safe_version("../../evil")
        False
    """
    if not version or len(version) > _MAX_VERSION_LENGTH:
        return False
    if not _SAFE_VERSION_PATTERN.match(version):
        return False
    # 纯点名（. / ..）虽符合字符集，但不能作为文件名
    return bool(version.strip("._-+"))


def parse_version_key(version: str) -> Tuple[int, ...]:
    """把版本号转成可比较的数字元组

    Args:
        version: Maven 版本号，如 ``3.6.5.21_nobson``

    Returns:
        Tuple[int, ...]: 数字段元组，如 ``(3, 6, 5, 21)``

    Example:
        >>> parse_version_key("3.6.5.21_nobson") > parse_version_key("3.6.3.34")
        True
    """
    numbers = _VERSION_NUMBER_PATTERN.findall(version or "")
    return tuple(int(part) for part in numbers)


def select_latest_version(versions: List[str], release: Optional[str] = None) -> str:
    """从版本列表中挑出最新版本

    优先使用 ``maven-metadata.xml`` 的 ``<release>``，但它可能指向
    ``_nobson`` 之类的变体；其余情况按数字段比较取最大者。

    只接受通过 :func:`is_safe_version` 的版本号（元数据不可信）。

    Args:
        versions: ``<versions>`` 中列出的全部版本
        release: ``<release>`` 字段（可选）

    Returns:
        str: 最新的版本号

    Raises:
        JarDownloadError: 版本列表为空，或没有任何安全可用版本
    """
    candidates = [item.strip() for item in versions if item and item.strip()]
    safe_candidates = [item for item in candidates if is_safe_version(item)]
    rejected = [item for item in candidates if not is_safe_version(item)]
    if rejected:
        logger.warning("忽略不安全的版本号: %s", rejected)

    if release and release.strip() and is_safe_version(release.strip()):
        return release.strip()
    if release and release.strip():
        logger.warning("忽略不安全的 release 字段: %s", release.strip())

    if not safe_candidates:
        raise JarDownloadError("Maven 元数据中没有安全可用的版本号")
    return max(safe_candidates, key=parse_version_key)


def _metadata_url(repo_url: str) -> str:
    return f"{repo_url}/{MAVEN_GROUP_ID.replace('.', '/')}/{MAVEN_ARTIFACT_ID}/maven-metadata.xml"


def _jar_url(repo_url: str, version: str) -> str:
    group_path = MAVEN_GROUP_ID.replace(".", "/")
    filename = f"{MAVEN_ARTIFACT_ID}-{version}.jar"
    return f"{repo_url}/{group_path}/{MAVEN_ARTIFACT_ID}/{version}/{filename}"


def _jar_filename(version: str) -> str:
    """下载后落盘使用的文件名

    使用 ``gbasedbt-jdbc-<version>.jar`` 而非 Maven 原始名
    ``jdbc-<version>.jar``：后者不含 gbase 关键字，会被驱动搜索逻辑
    （按 gbasedbt/gbase 关键字过滤）忽略，导致「下载了却用不上」。

    调用方需保证 ``version`` 已通过 :func:`is_safe_version`。
    """
    return f"{LOCAL_JAR_PREFIX}-{version}.jar"


def _open_url(url: str, timeout: float) -> Any:
    """打开 URL，支持 http(s)、``file://`` 与本地路径

    注意：Windows 盘符路径（如 ``C:\\repo\\...``）会被 :func:`urlparse`
    解析出 ``c`` 这样的 scheme，因此这里只把 http/https 交给 urlopen，
    其余一律按文件系统路径处理。
    """
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme in ("http", "https"):
        request = urllib.request.Request(  # noqa: S310 - 仅访问用户显式配置的仓库
            url, headers={"User-Agent": "db-connector-tool"}
        )
        return urllib.request.urlopen(  # noqa: S310  # pylint: disable=consider-using-with
            request, timeout=timeout
        )

    path = urllib.request.url2pathname(parsed.path) if parsed.scheme == "file" else url
    return open(path, "rb")  # pylint: disable=consider-using-with


def _read_limited(handle: Any, max_bytes: int) -> bytes:
    """读取响应体并限制大小

    对返回非字节内容的异常对象直接报错——否则 ``while`` 循环拿到的永远是
    同一个“非空”对象，会一直读下去。
    """
    chunks: List[bytes] = []
    total = 0
    while True:
        chunk = handle.read(64 * 1024)
        if not chunk:
            break
        if not isinstance(chunk, (bytes, bytearray)):
            raise JarDownloadError(
                f"读取驱动内容失败: 期望字节流，实际得到 {type(chunk).__name__}"
            )
        total += len(chunk)
        if total > max_bytes:
            raise JarDownloadError(f"驱动文件超过大小上限 {max_bytes} 字节")
        chunks.append(bytes(chunk))
    return b"".join(chunks)


def _fetch_metadata(repo_url: str, timeout: float, max_bytes: int) -> Tuple[str, List[str]]:
    """读取 maven-metadata.xml，返回 (release, versions)"""
    url = _metadata_url(repo_url)
    try:
        with _open_url(url, timeout) as handle:
            payload = _read_limited(handle, max_bytes)
    except (urllib.error.URLError, OSError, ValueError, JarDownloadError) as error:
        raise JarDownloadError(f"读取 Maven 元数据失败: {url} ({error})") from error

    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError as error:
        raise JarDownloadError(f"Maven 元数据解析失败: {url} ({error})") from error

    versioning = root.find("versioning")
    if versioning is None:
        raise JarDownloadError(f"Maven 元数据缺少 versioning 节点: {url}")

    release = versioning.findtext("release")
    versions = [
        (node.text or "").strip()
        for node in versioning.findall("./versions/version")
        if (node.text or "").strip()
    ]
    return (release or "").strip(), versions


def _fetch_sha1(repo_url: str, version: str, timeout: float) -> Optional[str]:
    """读取官方 .sha1 校验值；缺失时返回 None（不做硬失败）"""
    url = f"{_jar_url(repo_url, version)}.sha1"
    try:
        with _open_url(url, timeout) as handle:
            payload = _read_limited(handle, 1024).decode("utf-8", "ignore")
    except (urllib.error.URLError, OSError, ValueError) as error:
        logger.debug("读取 SHA-1 失败(%s)，跳过校验: %s", url, error)
        return None

    match = re.search(r"\b([0-9a-fA-F]{40})\b", payload)
    return match.group(1).lower() if match else None


def _verify_jar(payload: bytes, expected_sha1: Optional[str], version: str) -> None:
    """校验 ZIP 魔数与 SHA-1"""
    if not payload.startswith(_ZIP_MAGIC):
        raise JarDownloadError("下载内容不是有效的 jar（缺少 ZIP 文件头）")

    if expected_sha1:
        actual = hashlib.sha1(payload).hexdigest()  # noqa: S324 - 与 Maven 侧校验值一致
        if actual != expected_sha1:
            raise JarDownloadError(
                f"SHA-1 校验失败: 期望 {expected_sha1}, 实际 {actual}"
            )
    else:
        logger.warning("未取到 %s 的官方 SHA-1，仅完成文件头校验", version)


def _is_within(path: Path, directory: Path) -> bool:
    """判断 path 归一化后是否位于 directory 之内（防目录穿越的第二道校验）

    Args:
        path: 待检查路径
        directory: 期望的父目录

    Returns:
        bool: 是否在目录之内

    Example:
        >>> _is_within(Path("/tmp/jars/a.jar"), Path("/tmp/jars"))
        True
    """
    try:
        resolved_parent = directory.resolve()
        resolved_path = path.resolve()
    except OSError:
        return False
    return resolved_path == resolved_parent or resolved_parent in resolved_path.parents


def resolve_latest_version(repo_url: str, timeout: float) -> Optional[str]:
    """解析仓库中的最新版本号，结果按仓库缓存

    缓存的意义：版本解析每个进程只需一次，避免每次建连都去访问 Maven。
    解析失败同样会被缓存（值为 ``None``），并记录到 :func:`download_status`。

    Args:
        repo_url: Maven 仓库基地址
        timeout: 单次请求超时（秒）

    Returns:
        Optional[str]: 最新版本号；解析失败返回 None
    """
    global _LAST_ERROR  # pylint: disable=global-statement

    if repo_url in _VERSION_CACHE:
        return _VERSION_CACHE[repo_url]

    try:
        release, versions = _fetch_metadata(repo_url, timeout, 1024 * 1024)
        version = select_latest_version(versions, release)
    except JarDownloadError as error:
        _VERSION_CACHE[repo_url] = None
        _LAST_ERROR = str(error)
        logger.warning("获取 GBase JDBC 驱动版本信息失败: %s", error)
        return None

    _VERSION_CACHE[repo_url] = version
    return version


@dataclass(frozen=True)
class DownloadContext:
    """一次下载所需的仓库与目标信息"""

    repo_url: str
    version: str
    target_dir: Path
    timeout: float
    max_bytes: int

    @property
    def target_path(self) -> Path:
        """本次下载的落盘路径"""
        return self.target_dir / _jar_filename(self.version)


def download_latest_jar(target_dir: Path) -> Optional[Path]:
    """下载最新版 GBase JDBC 驱动到目标目录

    Args:
        target_dir: 目标目录，不存在时自动创建

    Returns:
        Optional[Path]: 下载成功的 jar 路径；未启用或失败时返回 None

    Example:
        >>> path = download_latest_jar(Path.home() / "db_connector_tool" / "jars")
        >>> path is None or path.exists()
        True
    """
    global _LAST_ERROR  # pylint: disable=global-statement

    if not auto_download_enabled():
        logger.debug("已通过 %s 关闭自动下载", AUTO_DOWNLOAD_ENV)
        return None

    context = DownloadContext(
        repo_url=maven_repo_url(),
        version="",
        target_dir=Path(target_dir),
        timeout=_env_number(DOWNLOAD_TIMEOUT_ENV, DEFAULT_TIMEOUT_SECONDS),
        max_bytes=int(_env_number(DOWNLOAD_MAX_SIZE_ENV, DEFAULT_MAX_SIZE_BYTES)),
    )

    version = resolve_latest_version(context.repo_url, context.timeout)
    if version is None:
        return None
    context = replace(context, version=version)

    if not _is_within(context.target_path, context.target_dir):
        # 双保险：即便版本号校验被绕过，也不允许写出目标目录
        _ATTEMPTED_VERSIONS.add(version)
        _LAST_ERROR = f"拒绝写入目标目录之外的路径: {context.target_path}"
        logger.warning("拒绝写入目标目录之外的路径: %s", context.target_path)
        return None

    if context.target_path.exists():
        logger.debug("目标文件已存在，跳过下载: %s", context.target_path)
        return context.target_path

    if version in _ATTEMPTED_VERSIONS:
        logger.debug("版本 %s 本进程已尝试过，跳过重复下载", version)
        return None

    # 串行化：多线程同时建连时只下载一次，避免重复写盘
    with _DOWNLOAD_LOCK:
        logger.debug("已获得下载锁: %s", context.target_path)
        return _download_version(context)


def _download_version(
    context: DownloadContext,
) -> Optional[Path]:
    """实际执行下载、校验与落盘（调用方需持有下载锁）"""
    global _LAST_ERROR  # pylint: disable=global-statement

    target_path = context.target_path
    try:
        context.target_dir.mkdir(parents=True, exist_ok=True)
        expected_sha1 = _fetch_sha1(context.repo_url, context.version, context.timeout)
        url = _jar_url(context.repo_url, context.version)
        logger.info("正在从 Maven 下载 GBase JDBC 驱动: %s", url)
        with _open_url(url, context.timeout) as handle:
            payload = _read_limited(handle, context.max_bytes)
        _verify_jar(payload, expected_sha1, context.version)
        _atomic_write(target_path, payload)
    except (JarDownloadError, urllib.error.URLError, OSError, ValueError) as error:
        _ATTEMPTED_VERSIONS.add(context.version)
        _LAST_ERROR = f"{type(error).__name__}: {error}"
        logger.warning(
            "自动下载 GBase JDBC 驱动失败，请手动放置 jar 或设置 "
            "GBASEDBT_JDBC_JAR / %s: %s",
            MAVEN_REPO_ENV,
            error,
        )
        return None

    logger.info("GBase JDBC 驱动已下载: %s (%s 字节)", target_path, len(payload))
    return target_path


def _atomic_write(target_path: Path, payload: bytes) -> None:
    """先写临时文件再原子替换，避免半截文件被使用"""
    handle = tempfile.NamedTemporaryFile(
        dir=str(target_path.parent), prefix=".gbasedbt-", suffix=".part", delete=False
    )
    temp_path = Path(handle.name)
    try:
        with handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_path, target_path)
    except OSError:
        temp_path.unlink(missing_ok=True)
        raise


def download_status() -> Dict[str, Any]:
    """返回自动下载的当前状态（便于诊断）

    Returns:
        Dict[str, Any]: 启用状态、仓库地址、本进程已尝试版本与最近失败原因

    Example:
        >>> status = download_status()
        >>> status["enabled"]
        True
    """
    return {
        "enabled": auto_download_enabled(),
        "repo_url": maven_repo_url(),
        "resolved_versions": dict(_VERSION_CACHE),
        "attempted_versions": sorted(_ATTEMPTED_VERSIONS),
        "last_error": _LAST_ERROR,
    }


def reset_download_state() -> None:
    """清空进程内下载状态（供测试与手动重试使用）"""
    global _LAST_ERROR  # pylint: disable=global-statement
    _VERSION_CACHE.clear()
    _ATTEMPTED_VERSIONS.clear()
    _LAST_ERROR = None
