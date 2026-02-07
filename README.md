# singleserver

[![PyPI Version](https://badgen.net/pypi/v/singleserver)](https://pypi.org/project/singleserver/)
[![Python Versions](https://badgen.net/pypi/python/singleserver)](https://pypi.org/project/singleserver/)
[![License](https://badgen.net/badge/license/MIT/blue)](https://github.com/Technology-Company/singleserver/blob/main/LICENSE)
[![Last Commit](https://badgen.net/github/last-commit/Technology-Company/singleserver)](https://github.com/Technology-Company/singleserver/commits)

Manage singleton server processes across multiple workers using atomic socket binding.

## Problem

When running web applications with multiple workers (e.g., gunicorn), you often need auxiliary services (API servers, background processors, caches) that should only run as a **single instance** shared by all workers.

Current solutions like systemd unit files or separate containers require additional infrastructure and make local development different from production.

## Solution

`singleserver` uses the OS-level guarantee that **only one process can bind a socket at a time**. When multiple workers call `connect()`:

1. First worker acquires the lock and starts the server
2. Other workers detect the lock is held and connect as clients
3. If the owner dies, another worker can take over

```
Worker 1: connect() → bind(:18765) → SUCCESS → starts server, returns client
Worker 2: connect() → bind(:18765) → EADDRINUSE → waits for ready, returns client
Worker 3: connect() → bind(:18765) → EADDRINUSE → waits for ready, returns client
```

## Installation

```bash
pip install singleserver
```

## Quick Start

```python
from singleserver import SingleServer

# Define the server
api_server = SingleServer(
    name="api-server",
    command=["python", "-m", "my_api", "--port", "{port}"],
    port=8765,
)

# Connect (starts if needed, connects if already running)
with api_server.connect() as client:
    response = client.get("/data.json")
    print(response.json())
```

## Features

- **Atomic coordination**: Uses socket binding for distributed locking
- **Health monitoring**: Configurable health checks with automatic restarts
- **Output handling**: Redirect stdout/stderr to files
- **Graceful shutdown**: SIGTERM with timeout, then SIGKILL
- **No external dependencies**: Pure Python, no Redis/etcd/etc needed
- **Same code for dev and prod**: No systemd unit files required

## Configuration

```python
SingleServer(
    name="my-service",                    # Identifier for logging
    command=["python", "server.py", ...], # Command to run ({port} is replaced)

    # Server address
    port=8765,                           # TCP port
    # OR
    socket="/tmp/my-service.sock",       # Unix socket (faster, more secure)

    # Health checks
    health_check_url="/",                # URL to poll for readiness
    health_check_interval=5.0,           # Seconds between checks
    startup_timeout=30.0,                # Max seconds to wait for startup

    # Restart behavior
    restart_on_failure=True,             # Auto-restart if process dies
    max_restarts=3,                      # Give up after N restarts
    restart_delay=1.0,                   # Seconds between restart attempts

    # Process options
    env={"DATABASE_URL": "..."},         # Environment variables
    cwd="/app",                          # Working directory
    stdout="/var/log/my-service.log",    # Log file
    stderr="stdout",                     # Redirect stderr to stdout

    # Shutdown
    shutdown_timeout=10.0,               # Seconds for graceful stop
)
```

## Use Cases

### Internal API server

```python
from singleserver import SingleServer

api = SingleServer(
    name="internal-api",
    command=["python", "-m", "internal_api", "-p", "{port}"],
    port=8765,
)

def my_view(request):
    with api.connect() as client:
        data = client.get("/query").json()
    return JsonResponse(data)
```

### Background service

```python
service = SingleServer(
    name="background-service",
    command=["python", "service.py", "--port", "{port}"],
    port=8888,
    health_check_url="/health",
    restart_on_failure=True,
)
```

## How It Works

1. **Lock acquisition**: Uses a separate socket (port + 10000 by default) for coordination
2. **Process management**: Owner spawns subprocess in new process group
3. **Health monitoring**: Background thread polls health endpoint
4. **Restart logic**: Configurable restart attempts with delay
5. **Cleanup**: atexit handler and signal handlers for clean shutdown

## Development

```bash
# Install dev dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Type checking
uv run mypy singleserver

# Linting
uv run ruff check singleserver tests
```

## License

MIT
