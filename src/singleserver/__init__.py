"""
singleserver - Manage singleton server processes across multiple workers.

A Python library for coordinating a single instance of a server process
across multiple workers/processes using atomic socket binding.

Example:
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
"""

from .client import ServerClient, ServerNotReady
from .lock import LockFile, SocketLock
from .process import ProcessOwner, ProcessState
from .server import SingleServer

__version__ = "0.3.0"

__all__ = [
    # Main classes
    "SingleServer",
    # Supporting classes
    "ServerClient",
    "ProcessOwner",
    "ProcessState",
    # Lock utilities
    "SocketLock",
    "LockFile",
    # Exceptions
    "ServerNotReady",
]
