from __future__ import annotations

import os
import platform
import shutil
import signal
import subprocess
import tarfile
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib import error, request

from seccloud.storage import read_json, write_json

DEFAULT_QUICKWIT_PORT = 7280
DEFAULT_QUICKWIT_INDEX_ID = "seccloud-events-v2"
DEFAULT_QUICKWIT_VERSION = "0.7.1"


def _now_timestamp() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def quickwit_version() -> str:
    return os.environ.get("SECCLOUD_QUICKWIT_VERSION", DEFAULT_QUICKWIT_VERSION)


def _quickwit_release_target() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()

    if machine in {"arm64", "aarch64"}:
        cputype = "aarch64"
    elif machine in {"x86_64", "amd64", "x64"}:
        cputype = "x86_64"
    else:
        raise RuntimeError(f"Unsupported Quickwit CPU architecture for managed install: {machine}")

    if system == "darwin":
        ostype = "apple-darwin"
    elif system == "linux":
        ostype = "unknown-linux-gnu"
    else:
        raise RuntimeError(f"Unsupported Quickwit OS for managed install: {system}")

    return f"{cputype}-{ostype}"


def _quickwit_download_url(*, version: str, target: str) -> str:
    explicit = os.environ.get("SECCLOUD_QUICKWIT_DOWNLOAD_URL")
    if explicit:
        return explicit.format(version=version, target=target)
    return f"https://github.com/quickwit-oss/quickwit/releases/download/v{version}/quickwit-v{version}-{target}.tar.gz"


def quickwit_paths(root: str | Path) -> dict[str, Path]:
    root_path = Path(root).resolve()
    seccloud_root = root_path / ".seccloud"
    base = root_path / ".seccloud" / "quickwit"
    return {
        "base": base,
        "bin_dir": seccloud_root / "bin",
        "managed_bin": seccloud_root / "bin" / "quickwit",
        "data": base / "data",
        "metastore": base / "metastore",
        "indexes": base / "indexes",
        "config": base / "quickwit.yaml",
        "log": base / "quickwit.log",
        "pid": base / "quickwit.pid",
        "runtime": base / "runtime.json",
    }


def local_quickwit_url(root: str | Path) -> str:
    return f"http://127.0.0.1:{DEFAULT_QUICKWIT_PORT}"


def local_quickwit_index_id() -> str:
    return os.environ.get("SECCLOUD_QUICKWIT_INDEX", DEFAULT_QUICKWIT_INDEX_ID)


def _quickwit_config_payload(paths: dict[str, Path]) -> str:
    metastore_uri = paths["metastore"].as_uri()
    default_index_root_uri = paths["indexes"].as_uri()
    return (
        "version: 0.7\n"
        "cluster_id: seccloud-local\n"
        "node_id: seccloud-local\n"
        "listen_address: 127.0.0.1\n"
        f"data_dir: {paths['data']}\n"
        f"metastore_uri: {metastore_uri}\n"
        f"default_index_root_uri: {default_index_root_uri}\n"
        "rest:\n"
        f"  listen_port: {DEFAULT_QUICKWIT_PORT}\n"
        "  cors_allow_origins: '*'\n"
    )


def _write_runtime_manifest(paths: dict[str, Path], payload: dict[str, Any]) -> None:
    write_json(paths["runtime"], payload)


def _read_runtime_manifest(paths: dict[str, Path]) -> dict[str, Any]:
    defaults = {
        "config_path": str(paths["config"]),
        "url": local_quickwit_url(paths["base"].parents[1]),
        "index_id": local_quickwit_index_id(),
        "binary_path": str(paths["managed_bin"]),
        "binary_source": None,
        "version": quickwit_version(),
        "last_start_attempted_at": None,
        "last_start_completed_at": None,
        "last_start_duration_ms": None,
        "last_start_status": None,
        "last_start_error": None,
    }
    payload = read_json(paths["runtime"], defaults)
    if not isinstance(payload, dict):
        return defaults
    return {**defaults, **payload}


def _download_quickwit_archive(url: str, destination: Path) -> None:
    with request.urlopen(url, timeout=30) as response:  # noqa: S310 - operator-managed download
        with destination.open("wb") as handle:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                handle.write(chunk)


def _extract_quickwit_binary(archive_path: Path, destination: Path) -> None:
    with tarfile.open(archive_path, mode="r:gz") as archive:
        members = [
            member for member in archive.getmembers() if member.isfile() and Path(member.name).name == "quickwit"
        ]
        if not members:
            raise RuntimeError(f"Quickwit archive did not contain a `quickwit` binary: {archive_path}")
        extracted = archive.extractfile(members[0])
        if extracted is None:
            raise RuntimeError(f"Failed to extract Quickwit binary from archive: {archive_path}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(dir=destination.parent, delete=False) as tmp:
            shutil.copyfileobj(extracted, tmp)
            temp_path = Path(tmp.name)
    temp_path.chmod(0o755)
    temp_path.replace(destination)


def ensure_local_quickwit(root: str | Path, *, download_if_missing: bool = True) -> dict[str, Any]:
    paths = quickwit_paths(root)
    version = quickwit_version()
    explicit = os.environ.get("SECCLOUD_QUICKWIT_BIN")
    if explicit:
        explicit_path = Path(explicit).expanduser()
        if not explicit_path.exists():
            raise FileNotFoundError(f"SECCLOUD_QUICKWIT_BIN points to a missing file: {explicit_path}")
        return {
            "status": "explicit_env",
            "binary_path": str(explicit_path),
            "binary_source": "explicit_env",
            "managed": False,
            "version": version,
        }

    managed = paths["managed_bin"]
    if managed.exists():
        return {
            "status": "already_installed",
            "binary_path": str(managed),
            "binary_source": "managed_local",
            "managed": True,
            "version": version,
        }

    resolved = shutil.which("quickwit")
    if resolved:
        return {
            "status": "system_binary",
            "binary_path": resolved,
            "binary_source": "path",
            "managed": False,
            "version": version,
        }

    if not download_if_missing:
        raise FileNotFoundError(
            "Could not find Quickwit locally. Set SECCLOUD_QUICKWIT_BIN, "
            f"place a managed binary at {managed}, or allow the local runtime to install Quickwit {version}."
        )

    target = _quickwit_release_target()
    url = _quickwit_download_url(version=version, target=target)
    paths["base"].mkdir(parents=True, exist_ok=True)
    paths["bin_dir"].mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="seccloud-quickwit-", dir=paths["base"]) as temp_dir:
        archive_path = Path(temp_dir) / "quickwit.tar.gz"
        _download_quickwit_archive(url, archive_path)
        _extract_quickwit_binary(archive_path, managed)
    return {
        "status": "installed",
        "binary_path": str(managed),
        "binary_source": "managed_local",
        "managed": True,
        "version": version,
        "download_url": url,
    }


def _read_pid(paths: dict[str, Path]) -> int | None:
    if not paths["pid"].exists():
        return None
    try:
        return int(paths["pid"].read_text(encoding="utf-8").strip())
    except ValueError:
        return None


def _process_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    else:
        return True


def _remove_stale_pid(paths: dict[str, Path]) -> bool:
    pid = _read_pid(paths)
    if pid is not None and not _process_exists(pid):
        paths["pid"].unlink(missing_ok=True)
        return True
    return False


def _is_http_ready(url: str) -> bool:
    try:
        with request.urlopen(url, timeout=0.5) as response:  # noqa: S310 - local operator-managed service
            return response.status < 500
    except error.HTTPError as exc:
        return exc.code < 500
    except Exception:
        return False


def _wait_for_http_ready(url: str, *, timeout_seconds: float = 10.0, poll_interval_seconds: float = 0.2) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        if _is_http_ready(url):
            return
        time.sleep(poll_interval_seconds)
    raise RuntimeError(f"Local Quickwit did not become ready within {timeout_seconds:.1f}s: {url}")


def _process_rss_bytes(pid: int | None) -> int | None:
    if pid is None:
        return None
    try:
        result = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            check=False,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, PermissionError):
        return None
    if result.returncode != 0:
        return None
    value = result.stdout.strip()
    if not value:
        return None
    try:
        return int(value) * 1024
    except ValueError:
        return None


def init_local_quickwit(root: str | Path) -> dict[str, Any]:
    paths = quickwit_paths(root)
    already_initialized = paths["config"].exists()
    for key in ("base", "bin_dir", "data", "metastore", "indexes"):
        paths[key].mkdir(parents=True, exist_ok=True)
    paths["config"].write_text(_quickwit_config_payload(paths), encoding="utf-8")
    runtime_manifest = _read_runtime_manifest(paths)
    runtime_manifest.update(
        {
            "config_path": str(paths["config"]),
            "url": local_quickwit_url(root),
            "index_id": local_quickwit_index_id(),
            "binary_path": str(paths["managed_bin"]),
            "version": quickwit_version(),
        }
    )
    _write_runtime_manifest(paths, runtime_manifest)
    return {
        "status": "already_initialized" if already_initialized else "initialized",
        "config_path": str(paths["config"]),
        "url": local_quickwit_url(root),
        "index_id": local_quickwit_index_id(),
        "binary_path": str(paths["managed_bin"]),
        "version": quickwit_version(),
    }


def start_local_quickwit(root: str | Path) -> dict[str, Any]:
    paths = quickwit_paths(root)
    init_result = init_local_quickwit(root)
    runtime_manifest = _read_runtime_manifest(paths)
    binary = ensure_local_quickwit(root)
    stale_pid_removed = _remove_stale_pid(paths)
    pid = _read_pid(paths)
    url = local_quickwit_url(root)
    if pid is not None and _process_exists(pid):
        started_at = _now_timestamp()
        _wait_for_http_ready(url, timeout_seconds=2.0)
        runtime_manifest.update(
            {
                "last_start_attempted_at": started_at,
                "last_start_completed_at": started_at,
                "last_start_duration_ms": 0,
                "last_start_status": "already_running",
                "last_start_error": None,
                "binary_path": binary["binary_path"],
                "binary_source": binary["binary_source"],
                "version": binary["version"],
            }
        )
        _write_runtime_manifest(paths, runtime_manifest)
        return {
            "status": "already_running",
            "pid": pid,
            "url": url,
            "index_id": local_quickwit_index_id(),
            "config_path": str(paths["config"]),
            "log_path": str(paths["log"]),
            "initialized": init_result["status"] == "initialized",
            "stale_pid_removed": stale_pid_removed,
            "binary_path": binary["binary_path"],
            "binary_source": binary["binary_source"],
            "version": binary["version"],
        }
    if _is_http_ready(url):
        started_at = _now_timestamp()
        runtime_manifest.update(
            {
                "last_start_attempted_at": started_at,
                "last_start_completed_at": started_at,
                "last_start_duration_ms": 0,
                "last_start_status": "already_running_external",
                "last_start_error": None,
                "binary_path": binary["binary_path"],
                "binary_source": binary["binary_source"],
                "version": binary["version"],
            }
        )
        _write_runtime_manifest(paths, runtime_manifest)
        return {
            "status": "already_running_external",
            "pid": None,
            "url": url,
            "index_id": local_quickwit_index_id(),
            "config_path": str(paths["config"]),
            "log_path": str(paths["log"]),
            "initialized": init_result["status"] == "initialized",
            "stale_pid_removed": stale_pid_removed,
            "binary_path": binary["binary_path"],
            "binary_source": binary["binary_source"],
            "version": binary["version"],
        }

    start_clock = time.monotonic()
    start_attempted_at = _now_timestamp()
    with paths["log"].open("a", encoding="utf-8") as log_handle:
        process = subprocess.Popen(  # noqa: S603 - operator-managed local binary
            [binary["binary_path"], "run", "--config", str(paths["config"])],
            stdout=log_handle,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    paths["pid"].write_text(f"{process.pid}\n", encoding="utf-8")
    try:
        _wait_for_http_ready(url)
    except Exception as exc:
        runtime_manifest.update(
            {
                "last_start_attempted_at": start_attempted_at,
                "last_start_completed_at": _now_timestamp(),
                "last_start_duration_ms": int((time.monotonic() - start_clock) * 1000),
                "last_start_status": "failed",
                "last_start_error": str(exc),
                "binary_path": binary["binary_path"],
                "binary_source": binary["binary_source"],
                "version": binary["version"],
            }
        )
        _write_runtime_manifest(paths, runtime_manifest)
        stop_local_quickwit(root)
        raise
    runtime_manifest.update(
        {
            "last_start_attempted_at": start_attempted_at,
            "last_start_completed_at": _now_timestamp(),
            "last_start_duration_ms": int((time.monotonic() - start_clock) * 1000),
            "last_start_status": "started",
            "last_start_error": None,
            "binary_path": binary["binary_path"],
            "binary_source": binary["binary_source"],
            "version": binary["version"],
        }
    )
    _write_runtime_manifest(paths, runtime_manifest)
    return {
        "status": "started",
        "pid": process.pid,
        "url": url,
        "index_id": local_quickwit_index_id(),
        "config_path": str(paths["config"]),
        "log_path": str(paths["log"]),
        "initialized": init_result["status"] == "initialized",
        "stale_pid_removed": stale_pid_removed,
        "binary_path": binary["binary_path"],
        "binary_source": binary["binary_source"],
        "version": binary["version"],
    }


def stop_local_quickwit(root: str | Path) -> dict[str, Any]:
    paths = quickwit_paths(root)
    stale_pid_removed = _remove_stale_pid(paths)
    pid = _read_pid(paths)
    if pid is None:
        return {"status": "not_running", "stale_pid_removed": stale_pid_removed}
    if not _process_exists(pid):
        paths["pid"].unlink(missing_ok=True)
        return {"status": "not_running", "stale_pid_removed": True}

    os.kill(pid, signal.SIGTERM)
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if not _process_exists(pid):
            paths["pid"].unlink(missing_ok=True)
            return {"status": "stopped"}
        time.sleep(0.1)
    os.kill(pid, signal.SIGKILL)
    paths["pid"].unlink(missing_ok=True)
    return {"status": "killed"}


def quickwit_runtime_status(root: str | Path) -> dict[str, Any]:
    paths = quickwit_paths(root)
    stale_pid_removed = _remove_stale_pid(paths)
    pid = _read_pid(paths)
    url = local_quickwit_url(root)
    ready = _is_http_ready(url)
    running = (pid is not None and _process_exists(pid)) or ready
    runtime_manifest = _read_runtime_manifest(paths)
    log_path = paths["log"]
    return {
        "root": str(Path(root).resolve()),
        "url": url,
        "index_id": local_quickwit_index_id(),
        "binary_path": runtime_manifest.get("binary_path", str(paths["managed_bin"])),
        "binary_source": runtime_manifest.get("binary_source"),
        "version": runtime_manifest.get("version", quickwit_version()),
        "config_path": str(paths["config"]),
        "log_path": str(log_path),
        "paths": {key: str(value) for key, value in paths.items()},
        "initialized": paths["config"].exists(),
        "pid": pid,
        "running": running,
        "ready": ready,
        "rss_bytes": _process_rss_bytes(pid) if running else None,
        "log_size_bytes": log_path.stat().st_size if log_path.exists() else 0,
        "stale_pid_removed": stale_pid_removed,
        "last_start_attempted_at": runtime_manifest.get("last_start_attempted_at"),
        "last_start_completed_at": runtime_manifest.get("last_start_completed_at"),
        "last_start_duration_ms": runtime_manifest.get("last_start_duration_ms"),
        "last_start_status": runtime_manifest.get("last_start_status"),
        "last_start_error": runtime_manifest.get("last_start_error"),
    }


def read_quickwit_log_tail(root: str | Path, *, max_lines: int = 80) -> list[str]:
    log_path = quickwit_paths(root)["log"]
    if not log_path.exists():
        return []
    lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    if max_lines <= 0:
        return []
    return lines[-max_lines:]
