"""Wrappers for PostgreSQL command-line executables.

Locates binaries from the bundled pginstall/ directory and provides
typed Python functions for initdb, pg_ctl, psql, etc.
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from pathlib import Path

logger = logging.getLogger("pgbox.commands")

# Bundled PostgreSQL binaries directory
PGINSTALL = Path(__file__).parent / "pginstall"
BIN_DIR = PGINSTALL / "bin"


def _bin(name: str) -> str:
    """Get the path to a bundled PostgreSQL binary."""
    path = BIN_DIR / name
    if not path.exists():
        raise FileNotFoundError(
            f"PostgreSQL binary '{name}' not found at {path}. "
            f"pgbox may not be installed correctly — "
            f"try reinstalling: pip install --force-reinstall pgbox"
        )
    return str(path)


def _run(
    args: list[str],
    *,
    check: bool = True,
    capture: bool = True,
    env: dict[str, str] | None = None,
    timeout: float = 120,
) -> subprocess.CompletedProcess:
    """Run a subprocess with proper env and error handling."""
    run_env = os.environ.copy()
    # Ensure our bundled libs are found
    lib_dir = str(PGINSTALL / "lib")
    if "DYLD_LIBRARY_PATH" in run_env:
        run_env["DYLD_LIBRARY_PATH"] = f"{lib_dir}:{run_env['DYLD_LIBRARY_PATH']}"
    else:
        run_env["DYLD_LIBRARY_PATH"] = lib_dir
    if "LD_LIBRARY_PATH" in run_env:
        run_env["LD_LIBRARY_PATH"] = f"{lib_dir}:{run_env['LD_LIBRARY_PATH']}"
    else:
        run_env["LD_LIBRARY_PATH"] = lib_dir

    if env:
        run_env.update(env)

    logger.debug("Running: %s", " ".join(args))

    return subprocess.run(
        args,
        check=check,
        capture_output=capture,
        text=True,
        env=run_env,
        timeout=timeout,
    )


def initdb(pgdata: str | Path) -> subprocess.CompletedProcess:
    """Initialize a PostgreSQL data directory."""
    pgdata = str(pgdata)
    return _run([
        _bin("initdb"),
        "-D", pgdata,
        "--no-locale",
        "--encoding=UTF8",
        "--auth=trust",
    ])


def pg_ctl_start(
    pgdata: str | Path,
    *,
    port: int | None = None,
    socket_dir: str | None = None,
    log_file: str | None = None,
) -> subprocess.CompletedProcess:
    """Start PostgreSQL server."""
    pgdata = str(pgdata)
    args = [_bin("pg_ctl"), "start", "-D", pgdata, "-w"]

    options = []
    if port:
        options.append(f"-p {port}")
    if socket_dir:
        options.append(f"-k {socket_dir}")
    if log_file:
        args.extend(["-l", log_file])

    if options:
        args.extend(["-o", " ".join(options)])

    return _run(args, timeout=30)


def pg_ctl_stop(pgdata: str | Path, *, mode: str = "fast") -> subprocess.CompletedProcess:
    """Stop PostgreSQL server."""
    return _run(
        [_bin("pg_ctl"), "stop", "-D", str(pgdata), "-m", mode, "-w"],
        check=False,
        timeout=30,
    )


def pg_ctl_status(pgdata: str | Path) -> subprocess.CompletedProcess:
    """Check PostgreSQL server status."""
    return _run(
        [_bin("pg_ctl"), "status", "-D", str(pgdata)],
        check=False,
    )


def psql(
    pgdata: str | Path,
    sql: str,
    *,
    database: str = "postgres",
    port: int | None = None,
    socket_dir: str | None = None,
) -> subprocess.CompletedProcess:
    """Execute SQL against the running server."""
    # Write SQL to temp file to avoid shell escaping issues
    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as f:
        f.write(sql)
        sql_file = f.name

    try:
        args = [_bin("psql"), "-d", database, "-f", sql_file, "--no-psqlrc", "-q"]
        if port:
            args.extend(["-p", str(port)])
        if socket_dir:
            args.extend(["-h", socket_dir])
        return _run(args, check=True)
    finally:
        os.unlink(sql_file)


def pg_isready(
    *,
    port: int | None = None,
    socket_dir: str | None = None,
) -> bool:
    """Check if PostgreSQL is accepting connections."""
    args = [_bin("pg_isready"), "-q"]
    if port:
        args.extend(["-p", str(port)])
    if socket_dir:
        args.extend(["-h", socket_dir])
    result = _run(args, check=False)
    return result.returncode == 0
