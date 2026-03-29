"""Tests for pgbox server lifecycle."""

import os
import tempfile
from pathlib import Path

import pytest

from pgbox import get_server, PostgresServer


@pytest.fixture
def tmp_pgdata(tmp_path):
    """Temporary pgdata directory."""
    return str(tmp_path / "pgdata")


class TestGetServer:
    """Test the get_server factory function."""

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "pgbox" / "pginstall" / "bin" / "postgres").exists(),
        reason="PostgreSQL binaries not built — run 'cd pgbuild && make' first",
    )
    def test_start_and_connect(self, tmp_pgdata):
        """Start a server, get a URI, verify it's connectable."""
        with get_server(tmp_pgdata, cleanup_mode="delete") as server:
            assert server.is_healthy()
            uri = server.get_uri()
            assert "postgresql" in uri
            assert server.port is not None
            assert server.pid is not None

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "pgbox" / "pginstall" / "bin" / "postgres").exists(),
        reason="PostgreSQL binaries not built",
    )
    def test_enable_extension(self, tmp_pgdata):
        """Enable pgvector extension."""
        with get_server(tmp_pgdata, cleanup_mode="delete") as server:
            server.enable_extension("vector")
            result = server.execute("SELECT extname FROM pg_extension WHERE extname = 'vector'")
            assert "vector" in result

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "pgbox" / "pginstall" / "bin" / "postgres").exists(),
        reason="PostgreSQL binaries not built",
    )
    def test_create_database(self, tmp_pgdata):
        """Create a database."""
        with get_server(tmp_pgdata, cleanup_mode="delete") as server:
            server.create_database("testdb")
            result = server.execute("SELECT datname FROM pg_database WHERE datname = 'testdb'")
            assert "testdb" in result

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "pgbox" / "pginstall" / "bin" / "postgres").exists(),
        reason="PostgreSQL binaries not built",
    )
    def test_singleton(self, tmp_pgdata):
        """Same pgdata returns same instance."""
        s1 = get_server(tmp_pgdata)
        s2 = get_server(tmp_pgdata)
        assert s1 is s2
        s1._cleanup()

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "pgbox" / "pginstall" / "bin" / "postgres").exists(),
        reason="PostgreSQL binaries not built",
    )
    def test_cleanup_delete(self, tmp_pgdata):
        """cleanup_mode='delete' removes pgdata."""
        server = get_server(tmp_pgdata, cleanup_mode="delete")
        assert Path(tmp_pgdata).exists()
        server._cleanup()
        assert not Path(tmp_pgdata).exists()

    @pytest.mark.skipif(
        not (Path(__file__).parent.parent / "src" / "pgbox" / "pginstall" / "bin" / "postgres").exists(),
        reason="PostgreSQL binaries not built",
    )
    def test_execute_sql(self, tmp_pgdata):
        """Execute SQL and get output."""
        with get_server(tmp_pgdata, cleanup_mode="delete") as server:
            server.execute("CREATE TABLE test (id serial PRIMARY KEY, name text)")
            server.execute("INSERT INTO test (name) VALUES ('hello')")
            result = server.execute("SELECT name FROM test")
            assert "hello" in result
