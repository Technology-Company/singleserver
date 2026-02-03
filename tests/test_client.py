"""
Tests for ServerClient.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from singleserver import ServerClient, ServerNotReady


class TestServerClientBasic:
    """Basic tests for ServerClient."""

    def test_validation_requires_port_or_socket(self):
        """Test that either port or socket_path is required."""
        with pytest.raises(ValueError, match="Must provide either"):
            ServerClient()

    def test_base_url_tcp(self):
        """Test base_url property for TCP."""
        client = ServerClient(host="localhost", port=8080)
        assert client.base_url == "http://localhost:8080"

    def test_base_url_unix(self, socket_path: Path):
        """Test base_url property for Unix socket."""
        client = ServerClient(socket_path=socket_path)
        assert f"http+unix://{socket_path}" in client.base_url


class TestServerClientReadiness:
    """Tests for server readiness detection."""

    def test_wait_ready_success(self, running_test_server):
        """Test waiting for a running server."""
        proc, port = running_test_server

        client = ServerClient(port=port, startup_timeout=5.0)
        result = client.wait_ready()

        assert result is True
        assert client.is_ready

    def test_wait_ready_timeout(self, free_port: int):
        """Test timeout when server is not running."""
        client = ServerClient(port=free_port, startup_timeout=1.0)

        with pytest.raises(ServerNotReady):
            client.wait_ready()

    def test_check_ready_returns_false(self, free_port: int):
        """Test check_ready returns False when server is not running."""
        client = ServerClient(port=free_port)
        assert client.check_ready() is False
        assert not client.is_ready

    def test_check_ready_returns_true(self, running_test_server):
        """Test check_ready returns True when server is running."""
        proc, port = running_test_server

        client = ServerClient(port=port)
        assert client.check_ready() is True
        assert client.is_ready

    def test_wait_ready_with_slow_startup(self, test_server_script: Path, free_port: int):
        """Test waiting for a server that takes time to start."""
        # Start server with 1 second delay
        proc = subprocess.Popen(
            [sys.executable, str(test_server_script), str(free_port), "1.0"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        try:
            client = ServerClient(port=free_port, startup_timeout=5.0)
            result = client.wait_ready()
            assert result is True
        finally:
            proc.terminate()
            proc.wait()


class TestServerClientHTTP:
    """Tests for HTTP methods."""

    def test_get_request(self, running_test_server):
        """Test making a GET request."""
        proc, port = running_test_server

        client = ServerClient(port=port)
        client.wait_ready()

        response = client.get("/")
        assert response.status_code == 200
        assert response.text == "OK"

    def test_get_health_endpoint(self, running_test_server):
        """Test getting the health endpoint."""
        proc, port = running_test_server

        client = ServerClient(port=port)
        client.wait_ready()

        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "pid" in data

    def test_get_404(self, running_test_server):
        """Test 404 response."""
        proc, port = running_test_server

        client = ServerClient(port=port)
        client.wait_ready()

        response = client.get("/nonexistent")
        assert response.status_code == 404

    def test_context_manager(self, running_test_server):
        """Test using ServerClient as a context manager."""
        proc, port = running_test_server

        with ServerClient(port=port) as client:
            client.wait_ready()
            response = client.get("/")
            assert response.status_code == 200

    def test_custom_health_check_url(self, running_test_server):
        """Test custom health check URL."""
        proc, port = running_test_server

        client = ServerClient(port=port, health_check_url="/health")
        result = client.wait_ready()
        assert result is True


class TestServerClientConnection:
    """Tests for connection handling."""

    def test_tcp_connection_check(self, running_test_server):
        """Test TCP connection check."""
        proc, port = running_test_server

        client = ServerClient(port=port)
        # Access the private method for testing
        assert client._check_tcp_connection() is True

    def test_tcp_connection_check_fails(self, free_port: int):
        """Test TCP connection check fails when nothing is listening."""
        client = ServerClient(port=free_port)
        assert client._check_tcp_connection() is False
