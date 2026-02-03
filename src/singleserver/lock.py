"""
Atomic lock using socket binding.

Socket binding is atomic at the OS level - two processes cannot bind the same
port/socket simultaneously. This provides a distributed lock mechanism without
needing external coordination services.
"""

from __future__ import annotations

import errno
import os
import socket
from pathlib import Path
from types import TracebackType


class SocketLock:
    """
    Atomic lock using socket binding.

    Uses the OS-level guarantee that only one process can bind a socket at a time.
    This works for both TCP ports and Unix domain sockets.

    Example:
        lock = SocketLock(port=8765)
        if lock.try_acquire():
            # We're the owner
            try:
                run_server()
            finally:
                lock.release()
        else:
            # Someone else owns it
            connect_to_existing_server()
    """

    def __init__(
        self,
        port: int | None = None,
        socket_path: str | Path | None = None,
        host: str = "127.0.0.1",
    ):
        """
        Initialize the lock.

        Args:
            port: TCP port to bind for locking. Mutually exclusive with socket_path.
            socket_path: Unix socket path for locking. Mutually exclusive with port.
            host: Host to bind to when using TCP port. Defaults to localhost.

        Raises:
            ValueError: If neither port nor socket_path is provided, or both are.
        """
        # Initialize all attributes first so __del__ doesn't fail if validation raises
        self._socket: socket.socket | None = None
        self._acquired = False
        self.port = port
        self.socket_path = Path(socket_path) if socket_path else None
        self.host = host

        if port is None and socket_path is None:
            raise ValueError("Must provide either port or socket_path")
        if port is not None and socket_path is not None:
            raise ValueError("Cannot provide both port and socket_path")

    @property
    def is_acquired(self) -> bool:
        """Return True if this lock instance has acquired the lock."""
        return self._acquired

    @property
    def address(self) -> tuple[str, int] | str:
        """Return the address this lock binds to."""
        if self.socket_path:
            return str(self.socket_path)
        assert self.port is not None  # Guaranteed by __init__ validation
        return (self.host, self.port)

    def try_acquire(self) -> bool:
        """
        Try to acquire the lock.

        Returns:
            True if the lock was acquired, False if already held by another process.

        Raises:
            OSError: For socket errors other than address-in-use.
            RuntimeError: If called when lock is already acquired.
        """
        if self._acquired:
            raise RuntimeError("Lock already acquired by this instance")

        try:
            if self.socket_path:
                # Clean up stale socket file if it exists
                # This handles the case where a previous process crashed without cleanup
                if self.socket_path.exists():
                    # Try to connect first to see if someone is actually listening
                    test_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    try:
                        test_sock.connect(str(self.socket_path))
                        # Connection succeeded, someone else has the lock
                        test_sock.close()
                        return False
                    except (ConnectionRefusedError, FileNotFoundError):
                        # No one is listening, safe to remove stale socket
                        try:
                            self.socket_path.unlink()
                        except FileNotFoundError:
                            pass
                    finally:
                        test_sock.close()

                self._socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                self._socket.bind(str(self.socket_path))
                # Start listening so others can detect the socket is in use
                self._socket.listen(1)
            else:
                self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                self._socket.bind((self.host, self.port))
                # Start listening so others can detect the socket is in use
                self._socket.listen(1)

            self._acquired = True
            return True

        except OSError as e:
            if e.errno in (errno.EADDRINUSE, errno.EADDRNOTAVAIL):
                if self._socket:
                    self._socket.close()
                    self._socket = None
                return False
            # Re-raise unexpected errors
            if self._socket:
                self._socket.close()
                self._socket = None
            raise

    def release(self) -> None:
        """
        Release the lock.

        Safe to call even if lock was not acquired.
        """
        if self._socket:
            try:
                self._socket.close()
            except OSError:
                pass
            self._socket = None

        if self.socket_path and self.socket_path.exists():
            try:
                self.socket_path.unlink()
            except (FileNotFoundError, OSError):
                pass

        self._acquired = False

    def __enter__(self) -> SocketLock:
        """Context manager entry - tries to acquire but doesn't raise if fails."""
        self.try_acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Context manager exit - releases the lock."""
        self.release()

    def __del__(self) -> None:
        """Destructor - ensure lock is released."""
        self.release()


class LockFile:
    """
    Alternative lock implementation using a lock file with PID.

    This is useful as a secondary coordination mechanism alongside SocketLock,
    particularly for detecting stale locks and storing metadata about the lock owner.
    """

    def __init__(self, path: str | Path):
        """
        Initialize the lock file.

        Args:
            path: Path to the lock file.
        """
        self.path = Path(path)
        self._acquired = False

    @property
    def is_acquired(self) -> bool:
        """Return True if this instance holds the lock."""
        return self._acquired

    def try_acquire(self) -> bool:
        """
        Try to acquire the lock by creating a lock file with our PID.

        Returns:
            True if acquired, False if another process holds it.
        """
        if self._acquired:
            raise RuntimeError("Lock already acquired by this instance")

        try:
            # Use O_EXCL for atomic file creation
            fd = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
                0o644,
            )
            try:
                os.write(fd, f"{os.getpid()}\n".encode())
            finally:
                os.close(fd)
            self._acquired = True
            return True

        except FileExistsError:
            # Lock file exists, check if the owning process is still alive
            try:
                with open(self.path) as f:
                    pid = int(f.read().strip())
                # Check if process is alive
                os.kill(pid, 0)
                # Process is alive, lock is held
                return False
            except (ValueError, ProcessLookupError, PermissionError):
                # Invalid PID or process is dead, try to clean up
                try:
                    self.path.unlink()
                    return self.try_acquire()
                except FileNotFoundError:
                    return self.try_acquire()
                except OSError:
                    return False

    def release(self) -> None:
        """Release the lock by removing the lock file."""
        if self._acquired:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
            self._acquired = False

    def get_owner_pid(self) -> int | None:
        """
        Get the PID of the process that holds the lock.

        Returns:
            PID if lock file exists and is valid, None otherwise.
        """
        try:
            with open(self.path) as f:
                return int(f.read().strip())
        except (FileNotFoundError, ValueError):
            return None

    def __enter__(self) -> LockFile:
        self.try_acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.release()

    def __del__(self) -> None:
        self.release()
