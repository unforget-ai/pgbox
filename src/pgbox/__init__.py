"""pgbox — Zero-config embedded PostgreSQL with pgvector for Python.

Usage::

    from pgbox import get_server

    server = get_server("~/.myapp/data")
    uri = server.get_uri()
    server.enable_extension("vector")

    # Use with any PostgreSQL client
    import asyncpg
    conn = await asyncpg.connect(uri)

    # Context manager for automatic cleanup
    with get_server("/tmp/test", cleanup_mode="delete") as server:
        uri = server.get_uri()
        # ... server stops + data deleted on exit
"""

from pgbox.server import PostgresServer, get_server

__version__ = "0.1.2"

__all__ = ["PostgresServer", "get_server", "__version__"]
