"""
Pytest fixtures for singleserver tests.
"""

import shutil
import socket
import subprocess
import sys
import tempfile
import time
from collections.abc import Generator, Iterator
from pathlib import Path

import pytest


def get_free_port() -> int:
    """Get a free port number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


@pytest.fixture
def free_port() -> int:
    """Get a free port for testing."""
    return get_free_port()


@pytest.fixture
def free_ports() -> Generator[int, None, None]:
    """Generator that yields free ports."""

    def port_generator() -> Iterator[int]:
        while True:
            yield get_free_port()

    return port_generator()


@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for testing."""
    path = Path(tempfile.mkdtemp(prefix="singleserver_test_"))
    yield path
    # Cleanup
    shutil.rmtree(path, ignore_errors=True)


@pytest.fixture
def socket_path(temp_dir: Path) -> Path:
    """Get a path for a Unix socket."""
    return temp_dir / "test.sock"


@pytest.fixture
def lock_file_path(temp_dir: Path) -> Path:
    """Get a path for a lock file."""
    return temp_dir / "test.lock"


@pytest.fixture
def test_server_script() -> Path:
    """Path to the test server helper script."""
    return Path(__file__).parent / "helpers.py"


@pytest.fixture
def test_server_command(test_server_script: Path, free_port: int) -> list[str]:
    """Command to run a test server."""
    return [sys.executable, str(test_server_script), str(free_port)]


@pytest.fixture
def running_test_server(
    test_server_script: Path, free_port: int
) -> Generator[tuple[subprocess.Popen, int], None, None]:
    """Start a test server and yield the process and port."""
    proc = subprocess.Popen(
        [sys.executable, str(test_server_script), str(free_port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for server to start
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("127.0.0.1", free_port))
                break
        except ConnectionRefusedError:
            time.sleep(0.1)
    else:
        proc.kill()
        pytest.fail("Test server failed to start")

    yield proc, free_port

    # Cleanup
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
