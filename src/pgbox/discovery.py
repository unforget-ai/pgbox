"""PostgreSQL server discovery — port finding, postmaster.pid parsing, URI generation."""

from __future__ import annotations

import socket
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class PostmasterInfo:
    """Parsed info from PostgreSQL's postmaster.pid file."""

    pid: int
    pgdata: str
    port: int
    socket_dir: str
    start_time: str

    @classmethod
    def from_pgdata(cls, pgdata: str | Path) -> PostmasterInfo | None:
        """Parse postmaster.pid if it exists."""
        pid_file = Path(pgdata) / "postmaster.pid"
        if not pid_file.exists():
            return None
        try:
            lines = pid_file.read_text().strip().split("\n")
            if len(lines) < 5:
                return None
            return cls(
                pid=int(lines[0]),
                pgdata=lines[1],
                port=int(lines[3]),
                socket_dir=lines[4] if len(lines) > 4 else "",
                start_time=lines[2] if len(lines) > 2 else "",
            )
        except (ValueError, IndexError):
            return None

    def get_uri(self, database: str = "postgres") -> str:
        """Build a connection URI for this server."""
        if self.socket_dir and sys.platform != "win32":
            # Unix socket connection — most reliable
            return f"postgresql:///{database}?host={self.socket_dir}&port={self.port}"
        return f"postgresql://localhost:{self.port}/{database}"

    def is_process_alive(self) -> bool:
        """Check if the postmaster process is still running."""
        import psutil
        try:
            proc = psutil.Process(self.pid)
            return proc.is_running() and proc.status() != psutil.STATUS_ZOMBIE
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return False


def find_available_port(start: int = 5500, end: int = 5600) -> int:
    """Find an available TCP port in the given range."""
    for port in range(start, end):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No available ports in range {start}-{end}")


def validate_socket_path(path: str) -> str:
    """Ensure the socket path doesn't exceed Unix socket length limits.

    PostgreSQL socket filenames are like: /path/.s.PGSQL.5432
    Unix socket paths are limited to ~104 chars on most systems.
    """
    max_len = 90  # conservative — leave room for .s.PGSQL.NNNNN
    if len(path) > max_len:
        # Use /tmp as fallback
        import tempfile
        return tempfile.mkdtemp(prefix="pgbox_")
    return path
