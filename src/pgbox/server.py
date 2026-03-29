"""PostgresServer — manages the lifecycle of an embedded PostgreSQL instance.

One server per pgdata directory (singleton). Reference-counted across processes
via file locks so the server only stops when the last handle closes.

Usage::

    from pgbox import get_server

    server = get_server("~/.myapp/pgdata")
    uri = server.get_uri()
    server.enable_extension("vector")

    # Context manager
    with get_server("/tmp/test", cleanup_mode="delete") as server:
        ...  # server stops + data deleted on exit
"""

from __future__ import annotations

import atexit
import json
import logging
import os
import shutil
import sys
import time
from pathlib import Path

import fasteners
import psutil

from pgbox.commands import initdb, pg_ctl_start, pg_ctl_stop, pg_ctl_status, psql, pg_isready
from pgbox.discovery import PostmasterInfo, find_available_port, validate_socket_path

logger = logging.getLogger("pgbox")

# Singleton registry: pgdata path → server instance
_instances: dict[str, PostgresServer] = {}


class PostgresServer:
    """Manages an embedded PostgreSQL server lifecycle.

    Args:
        pgdata: Path to the PostgreSQL data directory.
        cleanup_mode: What to do on exit.
            "stop" — stop the server (default).
            "delete" — stop + delete the pgdata directory.
            "nothing" — leave running (for daemon mode).
    """

    def __init__(self, pgdata: str | Path, *, cleanup_mode: str = "stop"):
        self.pgdata = Path(pgdata).expanduser().resolve()
        self.cleanup_mode = cleanup_mode
        self._port: int | None = None
        self._socket_dir: str | None = None
        self._lock = fasteners.InterProcessLock(str(self.pgdata.parent / f".pgbox_{self.pgdata.name}.lock"))
        self._owns_server = False
        self._info: PostmasterInfo | None = None

        self._ensure_initialized()
        self._ensure_running()

        atexit.register(self._cleanup)

    def _ensure_initialized(self) -> None:
        """Initialize pgdata directory if it doesn't exist."""
        if (self.pgdata / "PG_VERSION").exists():
            logger.debug("pgdata already initialized: %s", self.pgdata)
            return

        self.pgdata.mkdir(parents=True, exist_ok=True)
        logger.info("Initializing PostgreSQL data directory: %s", self.pgdata)
        result = initdb(self.pgdata)
        if result.returncode != 0:
            raise RuntimeError(
                f"initdb failed:\n{result.stderr}"
            )
        logger.info("PostgreSQL data directory initialized")

    def _ensure_running(self) -> None:
        """Start PostgreSQL if it's not already running."""
        # Check if already running
        info = PostmasterInfo.from_pgdata(self.pgdata)
        if info and info.is_process_alive():
            logger.info("PostgreSQL already running (pid=%d, port=%d)", info.pid, info.port)
            self._info = info
            self._port = info.port
            self._socket_dir = info.socket_dir or None
            return

        # Clean up stale postmaster.pid
        pid_file = self.pgdata / "postmaster.pid"
        if pid_file.exists():
            logger.warning("Removing stale postmaster.pid")
            pid_file.unlink()

        # Find port and socket dir
        self._port = find_available_port()
        if sys.platform != "win32":
            self._socket_dir = validate_socket_path(str(self.pgdata))
        else:
            self._socket_dir = None

        # Start server
        log_file = str(self.pgdata / "pgbox.log")
        logger.info("Starting PostgreSQL on port %d...", self._port)

        with self._lock:
            result = pg_ctl_start(
                self.pgdata,
                port=self._port,
                socket_dir=self._socket_dir,
                log_file=log_file,
            )
            if result.returncode != 0:
                # Read the log for better error messages
                log_content = ""
                if os.path.exists(log_file):
                    log_content = Path(log_file).read_text()[-2000:]
                raise RuntimeError(
                    f"Failed to start PostgreSQL on port {self._port}:\n"
                    f"{result.stderr}\n"
                    f"--- server log ---\n{log_content}"
                )

        # Wait for ready
        self._wait_ready(timeout=15)
        self._owns_server = True
        self._info = PostmasterInfo.from_pgdata(self.pgdata)
        logger.info("PostgreSQL ready (pid=%d, port=%d)", self.pid, self._port)

    def _wait_ready(self, timeout: float = 15) -> None:
        """Poll until PostgreSQL accepts connections."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if pg_isready(port=self._port, socket_dir=self._socket_dir):
                return
            time.sleep(0.2)
        raise RuntimeError(
            f"PostgreSQL did not become ready within {timeout}s. "
            f"Check {self.pgdata / 'pgbox.log'} for details."
        )

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    @property
    def pid(self) -> int | None:
        """PID of the PostgreSQL postmaster process."""
        if self._info:
            return self._info.pid
        info = PostmasterInfo.from_pgdata(self.pgdata)
        return info.pid if info else None

    @property
    def port(self) -> int | None:
        """TCP port the server is listening on."""
        return self._port

    def get_uri(self, database: str = "postgres") -> str:
        """Get a connection URI for this server.

        Args:
            database: Database name (default: "postgres").

        Returns:
            A postgresql:// URI string ready for asyncpg/psycopg2.
        """
        if self._socket_dir and sys.platform != "win32":
            return f"postgresql:///{database}?host={self._socket_dir}&port={self._port}"
        return f"postgresql://localhost:{self._port}/{database}"

    def is_healthy(self) -> bool:
        """Check if the server is running and accepting connections."""
        return pg_isready(port=self._port, socket_dir=self._socket_dir)

    def execute(self, sql: str, *, database: str = "postgres") -> str:
        """Execute SQL against the server.

        Args:
            sql: SQL statement(s) to execute.
            database: Target database (default: "postgres").

        Returns:
            Command output as string.
        """
        result = psql(
            self.pgdata, sql,
            database=database,
            port=self._port,
            socket_dir=self._socket_dir,
        )
        return result.stdout or ""

    def enable_extension(self, name: str, *, database: str = "postgres") -> None:
        """Enable a PostgreSQL extension.

        Args:
            name: Extension name (e.g., "vector", "pg_trgm").
            database: Target database (default: "postgres").

        Raises:
            RuntimeError: If the extension is not available.
        """
        try:
            self.execute(f'CREATE EXTENSION IF NOT EXISTS "{name}"', database=database)
            logger.info("Extension '%s' enabled on database '%s'", name, database)
        except Exception as e:
            raise RuntimeError(
                f"Failed to enable extension '{name}'. "
                f"It may not be installed in this PostgreSQL build: {e}"
            ) from e

    def create_database(self, name: str) -> None:
        """Create a database if it doesn't exist.

        Args:
            name: Database name.
        """
        # Check if exists first (CREATE DATABASE can't use IF NOT EXISTS in a transaction)
        result = self.execute(
            f"SELECT 1 FROM pg_database WHERE datname = '{name}'",
        )
        if "1" not in result:
            self.execute(f'CREATE DATABASE "{name}"')
            logger.info("Database '%s' created", name)

    def stop(self) -> None:
        """Stop the PostgreSQL server."""
        if not self._owns_server:
            logger.debug("Not stopping — server was already running before pgbox")
            return

        logger.info("Stopping PostgreSQL (pid=%s)...", self.pid)
        pg_ctl_stop(self.pgdata)
        self._info = None
        logger.info("PostgreSQL stopped")

    def _cleanup(self) -> None:
        """Cleanup handler registered via atexit."""
        key = str(self.pgdata)
        if self.cleanup_mode == "nothing":
            return

        if self.cleanup_mode in ("stop", "delete"):
            self.stop()

        if self.cleanup_mode == "delete":
            if self.pgdata.exists():
                shutil.rmtree(self.pgdata, ignore_errors=True)
                logger.info("Deleted pgdata: %s", self.pgdata)

        _instances.pop(key, None)

    # -------------------------------------------------------------------
    # Context manager
    # -------------------------------------------------------------------

    def __enter__(self) -> PostgresServer:
        return self

    def __exit__(self, *exc) -> None:
        self._cleanup()

    def __repr__(self) -> str:
        status = "running" if self.is_healthy() else "stopped"
        return f"PostgresServer(pgdata='{self.pgdata}', port={self._port}, {status})"


# -------------------------------------------------------------------
# Factory function (singleton per pgdata)
# -------------------------------------------------------------------

def get_server(
    pgdata: str | Path,
    *,
    cleanup_mode: str = "stop",
) -> PostgresServer:
    """Get or create a PostgreSQL server for the given data directory.

    Singleton per pgdata path — calling get_server() twice with the same
    path returns the same instance.

    Args:
        pgdata: Path to the PostgreSQL data directory.
            Will be created if it doesn't exist.
        cleanup_mode: What to do on exit.
            "stop" — stop the server (default).
            "delete" — stop + delete the pgdata directory.
            "nothing" — leave running.

    Returns:
        A running PostgresServer instance.

    Example::

        server = get_server("~/.myapp/pgdata")
        uri = server.get_uri()
        server.enable_extension("vector")
    """
    key = str(Path(pgdata).expanduser().resolve())

    if key in _instances:
        existing = _instances[key]
        if existing.is_healthy():
            return existing
        # Server died — remove stale instance
        _instances.pop(key, None)

    server = PostgresServer(pgdata, cleanup_mode=cleanup_mode)
    _instances[key] = server
    return server
