# pgbox

Zero-config embedded PostgreSQL with pgvector for Python.

```python
from pgbox import get_server

server = get_server("~/.myapp/data")
uri = server.get_uri()  # postgresql:///postgres?host=...&port=5501

server.enable_extension("vector")
server.execute("CREATE TABLE items (id serial, embedding vector(384))")
```

No Docker. No install steps. No configuration. Just `pip install pgbox` and you have a running PostgreSQL 17.4 with pgvector 0.8.0.

## Install

```bash
pip install pgbox
```

Pre-compiled PostgreSQL binaries are included in the wheel — nothing to download at runtime.

**Platforms:** macOS (Apple Silicon + Intel), Linux (x86_64)

**Python:** 3.11+

## Usage

### Basic

```python
from pgbox import get_server

# Start PostgreSQL (or connect to existing instance at this path)
server = get_server("/path/to/pgdata")

# Get connection URI for any PostgreSQL client
uri = server.get_uri()

# Use with asyncpg
import asyncpg
conn = await asyncpg.connect(uri)

# Use with psycopg2
import psycopg2
conn = psycopg2.connect(uri)

# Use with SQLAlchemy
from sqlalchemy import create_engine
engine = create_engine(uri)
```

### Enable extensions

```python
server.enable_extension("vector")    # pgvector for embeddings
server.enable_extension("pg_trgm")   # trigram similarity
```

### Execute SQL

```python
server.execute("CREATE TABLE users (id serial PRIMARY KEY, name text)")
server.execute("INSERT INTO users (name) VALUES ('Alice')")
result = server.execute("SELECT * FROM users")
```

### Create databases

```python
server.create_database("myapp")
uri = server.get_uri(database="myapp")
```

### Context manager (auto-cleanup)

```python
# Server stops when the block exits
with get_server("/tmp/test_data") as server:
    uri = server.get_uri()
    # ... use it ...

# Server stops + data directory deleted
with get_server("/tmp/test_data", cleanup_mode="delete") as server:
    uri = server.get_uri()
    # ... use it, everything cleaned up on exit ...
```

### Health check

```python
server.is_healthy()  # True if accepting connections
server.pid           # Postmaster process ID
server.port          # TCP port
```

## Cleanup modes

| Mode | Behavior |
|------|----------|
| `"stop"` (default) | Stop PostgreSQL on exit |
| `"delete"` | Stop + delete the data directory |
| `"nothing"` | Leave running (for daemon/service use) |

## How it works

pgbox bundles pre-compiled PostgreSQL 17.4 and pgvector 0.8.0 binaries directly in the Python wheel. When you call `get_server()`:

1. **Initialize** — runs `initdb` if the data directory doesn't exist
2. **Start** — runs `pg_ctl start` on an available port
3. **Wait** — polls `pg_isready` until the server accepts connections
4. **Return** — gives you a `PostgresServer` with connection URI

The server is a singleton per data directory — calling `get_server()` twice with the same path returns the same instance. Cross-process safety is handled via file locks.

## Versions

| Component | Version |
|-----------|---------|
| PostgreSQL | 17.4 |
| pgvector | 0.8.0 |

## License

Apache-2.0
