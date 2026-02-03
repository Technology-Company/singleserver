"""
Tests for SingleServer (integration tests).
"""

import multiprocessing
import os
import sys
import time
from pathlib import Path

import pytest

from singleserver import ManagedServer, ServerClient, SingleServer


# Module-level function for multiprocessing (can't pickle local functions)
def _coordination_worker(port: int, script: str, result_queue):
    """Worker function for multi-process coordination test."""
    server = SingleServer(
        name="test",
        command=[sys.executable, script, str(port)],
        port=port,
        startup_timeout=10.0,
    )
    try:
        client = server.connect()
        is_owner = server.is_owner

        # Verify we can make requests
        response = client.get("/pid")
        server_pid = int(response.text)

        result_queue.put(
            {
                "is_owner": is_owner,
                "worker_pid": os.getpid(),
                "server_pid": server_pid,
            }
        )

        # Hold connection for a bit
        time.sleep(1)
    except Exception as e:
        result_queue.put({"error": str(e)})
    finally:
        server.disconnect()


class TestSingleServerBasic:
    """Basic tests for SingleServer."""

    def test_validation_requires_port_or_socket(self):
        """Test that either port or socket is required."""
        with pytest.raises(ValueError, match="Must provide either"):
            SingleServer(name="test", command=["echo", "hello"])

    def test_create_server(self, free_port: int):
        """Test creating a SingleServer instance."""
        server = SingleServer(
            name="test",
            command=["echo", "hello"],
            port=free_port,
        )
        assert server.name == "test"
        assert server.port == free_port
        assert not server.is_owner
        assert not server.is_connected

    def test_command_placeholder_replacement(self, free_port: int):
        """Test that {port} placeholder is replaced."""
        server = SingleServer(
            name="test",
            command=["server", "-p", "{port}"],
            port=free_port,
        )
        assert server.command == ["server", "-p", str(free_port)]


class TestSingleServerConnect:
    """Tests for SingleServer.connect()."""

    def test_connect_starts_server(self, free_port: int, test_server_script: Path):
        """Test that connect() starts the server if not running."""
        server = SingleServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(free_port)],
            port=free_port,
            startup_timeout=10.0,
        )

        try:
            client = server.connect()
            assert server.is_owner
            assert server.is_connected
            assert isinstance(client, ServerClient)

            # Verify server is actually running
            response = client.get("/")
            assert response.status_code == 200
        finally:
            server.disconnect()

    def test_connect_context_manager(self, free_port: int, test_server_script: Path):
        """Test using SingleServer as a context manager."""
        server = SingleServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(free_port)],
            port=free_port,
            startup_timeout=10.0,
        )

        with server as client:
            assert server.is_owner
            response = client.get("/health")
            assert response.status_code == 200

        assert not server.is_owner
        assert not server.is_connected

    def test_connect_with_env_vars(self, free_port: int, temp_dir: Path, test_server_script: Path):
        """Test passing environment variables to the server."""
        # The test server script will have access to env vars
        # We verify by checking the server starts successfully with custom env
        server = SingleServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(free_port)],
            port=free_port,
            env={"MY_CUSTOM_VAR": "test_value"},
            startup_timeout=10.0,
        )

        try:
            client = server.connect()
            # Server started successfully with custom env
            response = client.get("/")
            assert response.status_code == 200
        finally:
            server.disconnect()

    def test_get_client_before_connect(self, free_port: int):
        """Test that get_client returns None before connect."""
        server = SingleServer(
            name="test",
            command=["echo"],
            port=free_port,
        )
        assert server.get_client() is None


class TestSingleServerCoordination:
    """Tests for multi-process coordination."""

    def test_only_one_becomes_owner(self, free_port: int, test_server_script: Path):
        """Test that only one process becomes the owner."""
        result_queue = multiprocessing.Queue()

        # Start multiple workers
        workers = [
            multiprocessing.Process(
                target=_coordination_worker,
                args=(free_port, str(test_server_script), result_queue),
            )
            for _ in range(3)
        ]

        for w in workers:
            w.start()

        for w in workers:
            w.join(timeout=15)

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # Check for errors
        errors = [r for r in results if "error" in r]
        assert len(errors) == 0, f"Workers had errors: {errors}"

        # Exactly one should be owner
        owners = [r for r in results if r.get("is_owner")]
        assert len(owners) == 1

        # All should connect to the same server
        server_pids = {r["server_pid"] for r in results if "server_pid" in r}
        assert len(server_pids) == 1

    def test_client_connects_to_existing_server(self, free_port: int, test_server_script: Path):
        """Test that a second process connects to the existing server."""
        # Start server in first process
        server1 = SingleServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(free_port)],
            port=free_port,
            startup_timeout=10.0,
        )
        client1 = server1.connect()
        assert server1.is_owner

        try:
            # Get the server PID
            response1 = client1.get("/pid")
            server_pid1 = int(response1.text)

            # Second "process" (same process, different SingleServer instance)
            server2 = SingleServer(
                name="test",
                command=[sys.executable, str(test_server_script), str(free_port)],
                port=free_port,
                startup_timeout=10.0,
            )
            client2 = server2.connect()

            # Second should not be owner
            assert not server2.is_owner

            # Should connect to same server
            response2 = client2.get("/pid")
            server_pid2 = int(response2.text)
            assert server_pid1 == server_pid2

            server2.disconnect()
        finally:
            server1.disconnect()


class TestSingleServerFailover:
    """Tests for failover when owner dies."""

    def test_new_owner_after_old_dies(self, free_ports, test_server_script: Path):
        """Test that a new owner takes over when the old one dies."""
        # Use two different ports to avoid port reuse timing issues
        port1 = next(free_ports)
        port2 = next(free_ports)

        # This test verifies the lock mechanism works - when one owner
        # releases the lock, another can acquire it

        server1 = SingleServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(port1)],
            port=port1,
            startup_timeout=10.0,
        )
        client1 = server1.connect()
        assert server1.is_owner

        # Get original server PID
        response = client1.get("/pid")
        original_pid = int(response.text)

        # Disconnect (simulates owner dying)
        server1.disconnect()

        # Wait for cleanup
        time.sleep(1.0)

        # A new SingleServer with the SAME lock port should be able to become owner
        # Using different server port to avoid TIME_WAIT issues
        server2 = SingleServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(port2)],
            port=port2,
            lock_port=port1 + 10000,  # Same lock as server1
            startup_timeout=10.0,
        )
        client2 = server2.connect()
        assert server2.is_owner

        # New server should have different PID
        response = client2.get("/pid")
        new_pid = int(response.text)
        assert new_pid != original_pid

        server2.disconnect()


class TestManagedServer:
    """Tests for ManagedServer (non-singleton variant)."""

    def test_basic_usage(self, free_port: int, test_server_script: Path):
        """Test basic ManagedServer usage."""
        server = ManagedServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(free_port)],
            port=free_port,
            startup_timeout=10.0,
        )

        with server as client:
            response = client.get("/")
            assert response.status_code == 200

    def test_start_twice_raises(self, free_port: int, test_server_script: Path):
        """Test that starting twice raises an error."""
        server = ManagedServer(
            name="test",
            command=[sys.executable, str(test_server_script), str(free_port)],
            port=free_port,
        )

        server.start()
        with pytest.raises(RuntimeError, match="already started"):
            server.start()

        server.stop()

    def test_command_placeholder(self, free_port: int):
        """Test command placeholder replacement."""
        server = ManagedServer(
            name="test",
            command=["server", "--port", "{port}"],
            port=free_port,
        )
        assert server.command == ["server", "--port", str(free_port)]


class TestSingleServerOutputRedirect:
    """Tests for output redirection."""

    def test_stdout_redirect(self, free_port: int, temp_dir: Path, test_server_script: Path):
        """Test stdout is redirected to file."""
        log_file = temp_dir / "server.log"

        # Create a simple script that prints and then serves
        script_file = temp_dir / "print_server.py"
        script_file.write_text(f'''
import sys
print("SERVER STARTED", flush=True)
sys.path.insert(0, "{test_server_script.parent}")
from helpers import run_test_server
run_test_server({free_port})
''')

        server = SingleServer(
            name="test",
            command=[sys.executable, str(script_file)],
            port=free_port,
            stdout=log_file,
            startup_timeout=10.0,
        )

        try:
            server.connect()
            time.sleep(0.5)  # Wait for output to be flushed

            # Check log file was created and has content
            assert log_file.exists()
            content = log_file.read_text()
            assert "SERVER STARTED" in content
        finally:
            server.disconnect()
