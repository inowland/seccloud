from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Any

from seccloud.projection_store import (
    DEFAULT_PROJECTION_PGDATABASE,
    DEFAULT_PROJECTION_PGPORT,
    DEFAULT_PROJECTION_PGUSER,
)


def _postgres_binary(name: str) -> str:
    explicit = Path("/opt/homebrew/bin") / name
    if explicit.exists():
        return str(explicit)

    resolved = shutil.which(name)
    if resolved:
        return resolved

    raise FileNotFoundError(
        f"Could not find PostgreSQL binary `{name}`. "
        "Install PostgreSQL with Homebrew and ensure its bin directory is on PATH."
    )


def postgres_paths(root: str | Path) -> dict[str, Path]:
    root_path = Path(root).resolve()
    preferred = root_path / ".seccloud" / "postgres"
    legacy = root_path / ".demo" / "postgres"
    base = legacy if legacy.exists() and not preferred.exists() else preferred
    return {
        "base": base,
        "data": base / "data",
        "log": base / "postgres.log",
        "socket": base / "socket",
    }


def _run_pg_ctl(paths: dict[str, Path], action: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [_postgres_binary("pg_ctl"), "-D", str(paths["data"]), action],
        check=False,
        capture_output=True,
        text=True,
    )


def _read_postmaster_pid(pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None

    lines = pid_path.read_text(encoding="utf-8").splitlines()
    if not lines:
        return None

    try:
        return int(lines[0].strip())
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


def _process_command(pid: int) -> str | None:
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            check=False,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, PermissionError):
        return None
    if result.returncode != 0:
        return None
    command = result.stdout.strip()
    return command or None


def _cleanup_stale_runtime_files(paths: dict[str, Path]) -> list[str]:
    removed: list[str] = []
    pid_path = paths["data"] / "postmaster.pid"
    stale_files = [
        pid_path,
        paths["socket"] / f".s.PGSQL.{DEFAULT_PROJECTION_PGPORT}",
        paths["socket"] / f".s.PGSQL.{DEFAULT_PROJECTION_PGPORT}.lock",
    ]

    pid = _read_postmaster_pid(pid_path)
    if pid is not None and _process_exists(pid):
        command = _process_command(pid)
        if command and "postgres" in command and str(paths["data"]) in command:
            raise RuntimeError(
                "Local Postgres looks inconsistent: postmaster.pid points at a live "
                "process, but pg_ctl does not recognize the server as running."
            )

    for path in stale_files:
        if path.exists():
            path.unlink()
            removed.append(str(path))
    return removed


def _ensure_database(paths: dict[str, Path]) -> str:
    createdb_env = os.environ | {
        "PGHOST": str(paths["socket"]),
        "PGPORT": str(DEFAULT_PROJECTION_PGPORT),
    }
    result = subprocess.run(
        [
            _postgres_binary("createdb"),
            "-h",
            str(paths["socket"]),
            "-p",
            str(DEFAULT_PROJECTION_PGPORT),
            DEFAULT_PROJECTION_PGDATABASE,
        ],
        env=createdb_env,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        return "created"
    if "already exists" in result.stderr:
        return "already_exists"
    raise RuntimeError(result.stderr.strip() or "createdb failed")


def init_local_postgres(root: str | Path) -> dict[str, Any]:
    paths = postgres_paths(root)
    paths["base"].mkdir(parents=True, exist_ok=True)
    paths["socket"].mkdir(parents=True, exist_ok=True)
    if paths["data"].exists() and any(paths["data"].iterdir()):
        return {"status": "already_initialized", "data_dir": str(paths["data"])}
    subprocess.run(
        [_postgres_binary("initdb"), "-D", str(paths["data"]), "--auth=trust"],
        check=True,
    )
    return {"status": "initialized", "data_dir": str(paths["data"])}


def start_local_postgres(root: str | Path) -> dict[str, Any]:
    paths = postgres_paths(root)
    init_result = init_local_postgres(root)
    paths["socket"].mkdir(parents=True, exist_ok=True)
    status = _run_pg_ctl(paths, "status")
    stale_files_removed: list[str] = []

    if status.returncode == 0:
        database_status = _ensure_database(paths)
        return {
            "status": "already_running",
            "data_dir": str(paths["data"]),
            "database_status": database_status,
            "dsn": local_postgres_dsn(root),
            "initialized": init_result["status"] == "initialized",
            "log_path": str(paths["log"]),
        }

    if status.returncode == 3:
        stale_files_removed = _cleanup_stale_runtime_files(paths)

    subprocess.run(
        [
            _postgres_binary("pg_ctl"),
            "-D",
            str(paths["data"]),
            "-l",
            str(paths["log"]),
            "-o",
            f"-k {paths['socket']} -p {DEFAULT_PROJECTION_PGPORT}",
            "start",
        ],
        check=True,
    )
    database_status = _ensure_database(paths)
    return {
        "status": "started",
        "data_dir": str(paths["data"]),
        "database_status": database_status,
        "initialized": init_result["status"] == "initialized",
        "log_path": str(paths["log"]),
        "stale_files_removed": stale_files_removed,
        "dsn": (
            f"dbname={DEFAULT_PROJECTION_PGDATABASE} "
            f"user={DEFAULT_PROJECTION_PGUSER} "
            f"host={paths['socket']} "
            f"port={DEFAULT_PROJECTION_PGPORT}"
        ),
    }


def stop_local_postgres(root: str | Path) -> dict[str, Any]:
    paths = postgres_paths(root)
    status = _run_pg_ctl(paths, "status")
    if status.returncode == 3:
        stale_files_removed = _cleanup_stale_runtime_files(paths)
        return {"status": "not_running", "stale_files_removed": stale_files_removed}

    subprocess.run([_postgres_binary("pg_ctl"), "-D", str(paths["data"]), "stop"], check=True)
    return {"status": "stopped"}


def local_postgres_dsn(root: str | Path) -> str:
    paths = postgres_paths(root)
    return (
        f"dbname={DEFAULT_PROJECTION_PGDATABASE} "
        f"user={DEFAULT_PROJECTION_PGUSER} "
        f"host={paths['socket']} "
        f"port={DEFAULT_PROJECTION_PGPORT}"
    )
