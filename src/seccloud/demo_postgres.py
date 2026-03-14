from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

from seccloud.demo_projection import (
    DEFAULT_DEMO_PGDATABASE,
    DEFAULT_DEMO_PGPORT,
    DEFAULT_DEMO_PGUSER,
)


def postgres_paths(root: str | Path) -> dict[str, Path]:
    base = (Path(root) / ".demo" / "postgres").resolve()
    return {
        "base": base,
        "data": base / "data",
        "log": base / "postgres.log",
        "socket": base / "socket",
    }


def init_demo_postgres(root: str | Path) -> dict[str, Any]:
    paths = postgres_paths(root)
    paths["base"].mkdir(parents=True, exist_ok=True)
    paths["socket"].mkdir(parents=True, exist_ok=True)
    if paths["data"].exists() and any(paths["data"].iterdir()):
        return {"status": "already_initialized", "data_dir": str(paths["data"])}
    subprocess.run(
        ["/opt/homebrew/bin/initdb", "-D", str(paths["data"]), "--auth=trust"],
        check=True,
    )
    return {"status": "initialized", "data_dir": str(paths["data"])}


def start_demo_postgres(root: str | Path) -> dict[str, Any]:
    paths = postgres_paths(root)
    paths["socket"].mkdir(parents=True, exist_ok=True)
    subprocess.run(
        [
            "/opt/homebrew/bin/pg_ctl",
            "-D",
            str(paths["data"]),
            "-l",
            str(paths["log"]),
            "-o",
            f"-k {paths['socket']} -p {DEFAULT_DEMO_PGPORT}",
            "start",
        ],
        check=True,
    )
    createdb_env = os.environ | {"PGHOST": str(paths["socket"]), "PGPORT": str(DEFAULT_DEMO_PGPORT)}
    subprocess.run(
        [
            "createdb",
            "-h",
            str(paths["socket"]),
            "-p",
            str(DEFAULT_DEMO_PGPORT),
            DEFAULT_DEMO_PGDATABASE,
        ],
        env=createdb_env,
        check=False,
    )
    return {
        "status": "started",
        "log_path": str(paths["log"]),
        "dsn": (
            f"dbname={DEFAULT_DEMO_PGDATABASE} "
            f"user={DEFAULT_DEMO_PGUSER} "
            f"host={paths['socket']} "
            f"port={DEFAULT_DEMO_PGPORT}"
        ),
    }


def stop_demo_postgres(root: str | Path) -> dict[str, Any]:
    paths = postgres_paths(root)
    subprocess.run(
        ["/opt/homebrew/bin/pg_ctl", "-D", str(paths["data"]), "stop"],
        check=True,
    )
    return {"status": "stopped"}


def demo_postgres_dsn(root: str | Path) -> str:
    paths = postgres_paths(root)
    return (
        f"dbname={DEFAULT_DEMO_PGDATABASE} user={DEFAULT_DEMO_PGUSER} host={paths['socket']} port={DEFAULT_DEMO_PGPORT}"
    )
