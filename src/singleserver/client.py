"""
Client for connecting to managed servers.
"""

from __future__ import annotations

import logging
import socket
import time
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False

try:
    import httpx  # noqa: F401

    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

logger = logging.getLogger(__name__)


class ServerNotReady(Exception):
    """Raised when the server is not ready within the timeout."""

    pass


class ServerClient:
    """
    Client for connecting to a managed server.

    Provides:
    - wait_ready() to block until the server is accepting connections
    - HTTP request methods (get, post, etc.) if requests library is available
    - Base URL construction for manual HTTP client usage
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int | None = None,
        socket_path: str | Path | None = None,
        health_check_url: str = "/",
        startup_timeout: float = 30.0,
    ):
        """
        Initialize the client.

        Args:
            host: Host to connect to (for TCP connections).
            port: TCP port to connect to.
            socket_path: Unix socket path to connect to.
            health_check_url: URL path to use for readiness checks.
            startup_timeout: Max seconds to wait for server to be ready.
        """
        if port is None and socket_path is None:
            raise ValueError("Must provide either port or socket_path")

        self.host = host
        self.port = port
        self.socket_path = Path(socket_path) if socket_path else None
        self.health_check_url = health_check_url
        self.startup_timeout = startup_timeout
        self._session: Any | None = None  # requests.Session or httpx.Client
        self._ready = False

    @property
    def base_url(self) -> str:
        """Base URL for making requests to the server."""
        if self.socket_path:
            # For Unix sockets, use http+unix:// scheme
            return f"http+unix://{self.socket_path}"
        return f"http://{self.host}:{self.port}"

    @property
    def is_ready(self) -> bool:
        """Return True if the server is confirmed ready."""
        return self._ready

    def _check_tcp_connection(self) -> bool:
        """Check if we can establish a TCP connection."""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            result = sock.connect_ex((self.host, self.port))
            sock.close()
            return result == 0
        except OSError:
            return False

    def _check_unix_connection(self) -> bool:
        """Check if we can establish a Unix socket connection."""
        if not self.socket_path or not self.socket_path.exists():
            return False
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(1.0)
            sock.connect(str(self.socket_path))
            sock.close()
            return True
        except OSError:
            return False

    def _check_http_health(self) -> bool:
        """Check if the HTTP health endpoint is responding."""
        if not REQUESTS_AVAILABLE:
            # Fall back to socket check
            if self.socket_path:
                return self._check_unix_connection()
            return self._check_tcp_connection()

        try:
            if self.socket_path:
                # Use requests-unixsocket for Unix sockets if available
                try:
                    import requests_unixsocket

                    session = requests_unixsocket.Session()
                    url = f"http+unix://{str(self.socket_path).replace('/', '%2F')}{self.health_check_url}"
                    response = session.get(url, timeout=2.0)
                    return response.ok
                except ImportError:
                    # Fall back to socket check
                    return self._check_unix_connection()
            else:
                response = requests.get(
                    f"http://{self.host}:{self.port}{self.health_check_url}",
                    timeout=2.0,
                )
                return response.ok
        except Exception:
            return False

    def wait_ready(self, timeout: float | None = None) -> bool:
        """
        Wait for the server to be ready.

        Args:
            timeout: Max seconds to wait. Uses startup_timeout if not specified.

        Returns:
            True if server is ready.

        Raises:
            ServerNotReady: If timeout is reached before server is ready.
        """
        if timeout is None:
            timeout = self.startup_timeout

        deadline = time.time() + timeout
        check_interval = 0.1  # Start with fast checks
        max_interval = 1.0

        while time.time() < deadline:
            if self._check_http_health():
                self._ready = True
                logger.debug("Server is ready")
                return True

            time.sleep(check_interval)
            # Exponential backoff up to max_interval
            check_interval = min(check_interval * 1.5, max_interval)

        raise ServerNotReady(f"Server did not become ready within {timeout} seconds")

    def check_ready(self) -> bool:
        """
        Check if the server is ready without waiting.

        Returns:
            True if server is ready, False otherwise.
        """
        ready = self._check_http_health()
        self._ready = ready
        return ready

    def _get_session(self) -> Any:
        """Get or create the HTTP session."""
        if self._session is not None:
            return self._session

        if REQUESTS_AVAILABLE:
            self._session = requests.Session()

            # Configure retry logic
            retry_strategy = Retry(
                total=3,
                backoff_factor=0.5,
                status_forcelist=[500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            self._session.mount("http://", adapter)
            self._session.mount("https://", adapter)
            return self._session

        raise ImportError(
            "requests library is required for HTTP methods. Install with: pip install requests"
        )

    def _make_url(self, path: str) -> str:
        """Construct full URL from path."""
        if self.socket_path:
            # Can't use requests for Unix sockets without requests-unixsocket
            raise NotImplementedError("HTTP methods with Unix sockets require requests-unixsocket")
        return urljoin(f"http://{self.host}:{self.port}/", path.lstrip("/"))

    def get(self, path: str, **kwargs) -> Any:
        """
        Make a GET request to the server.

        Args:
            path: URL path (will be joined with base URL).
            **kwargs: Additional arguments passed to requests.get().

        Returns:
            requests.Response object.
        """
        session = self._get_session()
        return session.get(self._make_url(path), **kwargs)

    def post(self, path: str, **kwargs) -> Any:
        """
        Make a POST request to the server.

        Args:
            path: URL path (will be joined with base URL).
            **kwargs: Additional arguments passed to requests.post().

        Returns:
            requests.Response object.
        """
        session = self._get_session()
        return session.post(self._make_url(path), **kwargs)

    def put(self, path: str, **kwargs) -> Any:
        """
        Make a PUT request to the server.

        Args:
            path: URL path (will be joined with base URL).
            **kwargs: Additional arguments passed to requests.put().

        Returns:
            requests.Response object.
        """
        session = self._get_session()
        return session.put(self._make_url(path), **kwargs)

    def delete(self, path: str, **kwargs) -> Any:
        """
        Make a DELETE request to the server.

        Args:
            path: URL path (will be joined with base URL).
            **kwargs: Additional arguments passed to requests.delete().

        Returns:
            requests.Response object.
        """
        session = self._get_session()
        return session.delete(self._make_url(path), **kwargs)

    def request(self, method: str, path: str, **kwargs) -> Any:
        """
        Make an arbitrary HTTP request to the server.

        Args:
            method: HTTP method (GET, POST, etc.).
            path: URL path (will be joined with base URL).
            **kwargs: Additional arguments passed to requests.request().

        Returns:
            requests.Response object.
        """
        session = self._get_session()
        return session.request(method, self._make_url(path), **kwargs)

    def close(self) -> None:
        """Close the client session."""
        if self._session is not None:
            self._session.close()
            self._session = None

    def __enter__(self) -> ServerClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
