"""
Main SingleServer class that ties together lock, process, and client.
"""

from __future__ import annotations

import atexit
import logging
import signal
import threading
from collections.abc import Callable
from pathlib import Path
from types import TracebackType
from typing import Any

from .client import ServerClient
from .lock import SocketLock
from .process import ProcessOwner

logger = logging.getLogger(__name__)


class SingleServer:
    """
    Manages a singleton server process across multiple workers.

    Uses atomic socket binding for coordination - only one process can become
    the "owner" and run the server, others become clients.

    Example:
        # Define the server
        datasette = SingleServer(
            name="datasette",
            command=["datasette", "serve", "data.db", "-p", "{port}"],
            port=8765,
        )

        # Connect (starts if needed, connects if already running)
        with datasette.connect() as client:
            response = client.get("/data/my_table.json")
    """

    def __init__(
        self,
        name: str,
        command: list[str],
        port: int | None = None,
        host: str = "127.0.0.1",
        socket: str | Path | None = None,
        lock_port: int | None = None,
        lock_socket: str | Path | None = None,
        health_check_url: str = "/",
        health_check_interval: float = 5.0,
        startup_timeout: float = 30.0,
        restart_on_failure: bool = True,
        max_restarts: int = 3,
        restart_delay: float = 1.0,
        env: dict[str, str] | None = None,
        cwd: str | Path | None = None,
        stdout: str | Path | None = None,
        stderr: str | Path | None = None,
        shutdown_timeout: float = 10.0,
        shutdown_signal: int = signal.SIGTERM,
        auto_reconnect: bool = True,
    ):
        """
        Initialize the SingleServer.

        Args:
            name: Identifier for logging/debugging.
            command: Command to run. Can include {port}, {host}, {socket} placeholders.
            port: TCP port the server will listen on.
            host: IP address to bind to (default: 127.0.0.1 for localhost only).
            socket: Unix socket path the server will listen on.
            lock_port: Separate port for the coordination lock (defaults to port - 1).
            lock_socket: Separate socket path for coordination lock.
            health_check_url: URL path to check for server readiness.
            health_check_interval: Seconds between health checks when running.
            startup_timeout: Max seconds to wait for server to become ready.
            restart_on_failure: Whether to restart if server dies unexpectedly.
            max_restarts: Maximum restart attempts before giving up.
            restart_delay: Seconds to wait between restart attempts.
            env: Additional environment variables for the server process.
            cwd: Working directory for the server process.
            stdout: Where to redirect stdout ("inherit", "null", or file path).
            stderr: Where to redirect stderr ("inherit", "null", "stdout", or file path).
            shutdown_timeout: Seconds to wait for graceful shutdown.
            shutdown_signal: Signal to send for shutdown.
            auto_reconnect: If True, client instances run a background watchdog
                that detects when the owner dies and re-acquires the lock to
                start a fresh server process.
        """
        if port is None and socket is None:
            raise ValueError("Must provide either port or socket")

        self.name = name
        self._command_template = command
        self.port = port
        self.host = host
        self.socket_path = Path(socket) if socket else None

        # Determine lock address (separate from server address)
        if lock_port is not None or lock_socket is not None:
            self._lock_port = lock_port
            self._lock_socket = Path(lock_socket) if lock_socket else None
        elif port is not None:
            # Use a separate lock port by default (ensure it stays within valid range)
            self._lock_port = port + 10000 if port + 10000 <= 65535 else port - 10000
            self._lock_socket = None
        else:
            # Use a separate lock socket
            self._lock_port = None
            self._lock_socket = Path(str(socket) + ".lock")

        self.health_check_url = health_check_url
        self.health_check_interval = health_check_interval
        self.startup_timeout = startup_timeout
        self.restart_on_failure = restart_on_failure
        self.max_restarts = max_restarts
        self.restart_delay = restart_delay
        self.env = env
        self.cwd = cwd
        self.stdout = stdout
        self.stderr = stderr
        self.shutdown_timeout = shutdown_timeout
        self.shutdown_signal = shutdown_signal

        self.auto_reconnect = auto_reconnect

        self._lock: SocketLock | None = None
        self._owner: ProcessOwner | None = None
        self._is_owner = False
        self._client: ServerClient | None = None
        self._cleanup_registered = False
        self._state_lock = threading.Lock()
        self._watchdog_thread: threading.Thread | None = None
        self._watchdog_stop = threading.Event()

    @property
    def command(self) -> list[str]:
        """Build the command with placeholders replaced."""
        result = []
        for part in self._command_template:
            if "{port}" in part:
                part = part.replace("{port}", str(self.port))
            if "{host}" in part:
                part = part.replace("{host}", self.host)
            if "{socket}" in part:
                part = part.replace("{socket}", str(self.socket_path))
            result.append(part)
        return result

    @property
    def is_owner(self) -> bool:
        """Return True if this process owns the server."""
        return self._is_owner

    @property
    def is_connected(self) -> bool:
        """Return True if connected to the server (as owner or client)."""
        return self._client is not None and self._client.is_ready

    def _create_health_check(self) -> Callable[[], bool]:
        """Create a health check function for the process owner."""

        def check() -> bool:
            try:
                client = ServerClient(
                    host=self.host,
                    port=self.port,
                    socket_path=self.socket_path,
                    health_check_url=self.health_check_url,
                    startup_timeout=2.0,
                )
                return bool(client.check_ready())
            except Exception:
                return False

        return check

    def _register_cleanup(self) -> None:
        """Register cleanup handlers."""
        if self._cleanup_registered:
            return
        self._cleanup_registered = True
        atexit.register(self._cleanup)

        # Also handle signals for clean shutdown
        def signal_handler(signum: int, frame: Any) -> None:
            self._cleanup()
            # Re-raise signal for default handling
            signal.signal(signum, signal.SIG_DFL)
            signal.raise_signal(signum)

        try:
            signal.signal(signal.SIGTERM, signal_handler)
            signal.signal(signal.SIGINT, signal_handler)
        except ValueError:
            # Can't set signal handlers from non-main thread
            pass

    def _start_watchdog(self) -> None:
        """Start the background watchdog thread for client-mode self-healing."""
        self._watchdog_stop.clear()
        self._watchdog_thread = threading.Thread(
            target=self._watchdog_loop,
            daemon=True,
            name=f"singleserver-watchdog-{self.name}",
        )
        self._watchdog_thread.start()
        logger.debug(f"Started watchdog for {self.name}")

    def _watchdog_loop(self) -> None:
        """Monitor the server and re-acquire ownership if it dies."""
        while not self._watchdog_stop.is_set():
            self._watchdog_stop.wait(self.health_check_interval)
            if self._watchdog_stop.is_set():
                break

            # Check if the server is still healthy
            if self._client and self._client.check_ready():
                continue

            logger.warning(f"Server {self.name} appears down, attempting to acquire lock")

            # Server is down — try to become the new owner
            lock = SocketLock(
                port=self._lock_port,
                socket_path=self._lock_socket,
            )

            if not lock.try_acquire():
                # Another worker got the lock — server should come back
                logger.debug(f"Lock for {self.name} held by another process, continuing to watch")
                continue

            # We got the lock — become the owner
            logger.info(f"Acquired lock for {self.name}, becoming owner (self-healing)")
            with self._state_lock:
                self._lock = lock
                self._is_owner = True

                self._owner = ProcessOwner(
                    command=self.command,
                    env=self.env,
                    cwd=self.cwd,
                    stdout=self.stdout,
                    stderr=self.stderr,
                    health_check=self._create_health_check(),
                    health_check_interval=self.health_check_interval,
                    startup_timeout=self.startup_timeout,
                    restart_on_failure=self.restart_on_failure,
                    max_restarts=self.max_restarts,
                    restart_delay=self.restart_delay,
                    shutdown_timeout=self.shutdown_timeout,
                    shutdown_signal=self.shutdown_signal,
                )
                self._owner.start()
                self._register_cleanup()

            # Wait for the new server to be ready
            try:
                if self._client:
                    self._client.wait_ready()
            except Exception as e:
                logger.error(f"New server failed to become ready: {e}")

            # ProcessOwner has its own monitor thread now, watchdog can exit
            break

    def _cleanup(self) -> None:
        """Clean up resources on exit."""
        logger.debug(f"Cleaning up SingleServer {self.name}")

        # Stop watchdog thread
        self._watchdog_stop.set()
        if self._watchdog_thread and self._watchdog_thread.is_alive():
            self._watchdog_thread.join(timeout=2)
        self._watchdog_thread = None

        if self._owner:
            try:
                self._owner.stop()
            except Exception as e:
                logger.warning(f"Error stopping process: {e}")
            self._owner = None

        if self._lock:
            try:
                self._lock.release()
            except Exception as e:
                logger.warning(f"Error releasing lock: {e}")
            self._lock = None

        if self._client:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

        self._is_owner = False

    def connect(self, wait_ready: bool = True, timeout: float | None = None) -> ServerClient:
        """
        Connect to the server, starting it if necessary.

        This is the main entry point. Multiple processes can call this concurrently:
        - First process to acquire the lock becomes the owner and starts the server
        - Other processes wait for the server to be ready and connect as clients

        Args:
            wait_ready: Whether to wait for the server to be ready.
            timeout: Max seconds to wait for ready (uses startup_timeout if not specified).

        Returns:
            ServerClient instance for making requests to the server.

        Raises:
            ServerNotReady: If timeout is reached before server is ready.
        """
        with self._state_lock:
            if self._client is not None:
                return self._client

            # Create the lock
            self._lock = SocketLock(
                port=self._lock_port,
                socket_path=self._lock_socket,
            )

            # Try to become the owner
            if self._lock.try_acquire():
                logger.info(f"Acquired lock for {self.name}, becoming owner")
                self._is_owner = True

                # Start the server process
                self._owner = ProcessOwner(
                    command=self.command,
                    env=self.env,
                    cwd=self.cwd,
                    stdout=self.stdout,
                    stderr=self.stderr,
                    health_check=self._create_health_check(),
                    health_check_interval=self.health_check_interval,
                    startup_timeout=self.startup_timeout,
                    restart_on_failure=self.restart_on_failure,
                    max_restarts=self.max_restarts,
                    restart_delay=self.restart_delay,
                    shutdown_timeout=self.shutdown_timeout,
                    shutdown_signal=self.shutdown_signal,
                )
                self._owner.start()
                self._register_cleanup()
            else:
                logger.info(f"Lock already held for {self.name}, connecting as client")
                # Release the lock object since we don't own it
                self._lock = None

            # Create client
            self._client = ServerClient(
                host=self.host,
                port=self.port,
                socket_path=self.socket_path,
                health_check_url=self.health_check_url,
                startup_timeout=timeout or self.startup_timeout,
            )

            # Start watchdog for non-owner clients
            if not self._is_owner and self.auto_reconnect:
                self._start_watchdog()

        # Wait for ready outside the lock
        if wait_ready:
            self._client.wait_ready(timeout=timeout)

        return self._client

    def disconnect(self) -> None:
        """
        Disconnect from the server.

        If this process is the owner, stops the server process.
        """
        self._cleanup()

    def stop(self) -> None:
        """Alias for disconnect()."""
        self.disconnect()

    def get_client(self) -> ServerClient | None:
        """
        Get the client without connecting.

        Returns:
            ServerClient if connected, None otherwise.
        """
        return self._client

    def __enter__(self) -> ServerClient:
        """Context manager entry - connects to the server."""
        return self.connect()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit - disconnects from the server."""
        self.disconnect()
