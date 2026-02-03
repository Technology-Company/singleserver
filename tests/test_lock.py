"""
Tests for SocketLock and LockFile.
"""

import multiprocessing
import os
import threading
import time
from pathlib import Path

import pytest

from singleserver import LockFile, SocketLock


# Module-level function for multiprocessing (can't pickle local functions)
def _acquire_lock_worker(port: int, result_queue):
    """Worker function for multiprocess lock test."""
    lock = SocketLock(port=port)
    acquired = lock.try_acquire()
    result_queue.put(acquired)
    time.sleep(0.5)  # Hold the lock
    if acquired:
        lock.release()


class TestSocketLockTCP:
    """Tests for SocketLock with TCP ports."""

    def test_acquire_and_release(self, free_port: int):
        """Test basic acquire and release."""
        lock = SocketLock(port=free_port)
        assert not lock.is_acquired

        result = lock.try_acquire()
        assert result is True
        assert lock.is_acquired

        lock.release()
        assert not lock.is_acquired

    def test_double_acquire_raises(self, free_port: int):
        """Test that acquiring twice raises RuntimeError."""
        lock = SocketLock(port=free_port)
        lock.try_acquire()

        with pytest.raises(RuntimeError, match="already acquired"):
            lock.try_acquire()

        lock.release()

    def test_release_is_idempotent(self, free_port: int):
        """Test that release can be called multiple times."""
        lock = SocketLock(port=free_port)
        lock.try_acquire()
        lock.release()
        lock.release()  # Should not raise

    def test_competing_locks_one_wins(self, free_port: int):
        """Test that only one lock can be acquired for the same port."""
        lock1 = SocketLock(port=free_port)
        lock2 = SocketLock(port=free_port)

        assert lock1.try_acquire() is True
        assert lock2.try_acquire() is False

        lock1.release()

        # Now lock2 should be able to acquire
        assert lock2.try_acquire() is True
        lock2.release()

    def test_context_manager(self, free_port: int):
        """Test using SocketLock as a context manager."""
        lock = SocketLock(port=free_port)

        with lock:
            assert lock.is_acquired

        assert not lock.is_acquired

    def test_address_property(self, free_port: int):
        """Test the address property."""
        lock = SocketLock(port=free_port, host="127.0.0.1")
        assert lock.address == ("127.0.0.1", free_port)

    def test_validation_requires_port_or_socket(self):
        """Test that either port or socket_path is required."""
        with pytest.raises(ValueError, match="Must provide either"):
            SocketLock()

    def test_validation_not_both_port_and_socket(self, free_port: int, socket_path: Path):
        """Test that both port and socket_path cannot be provided."""
        with pytest.raises(ValueError, match="Cannot provide both"):
            SocketLock(port=free_port, socket_path=socket_path)


class TestSocketLockUnix:
    """Tests for SocketLock with Unix sockets."""

    def test_acquire_and_release(self, socket_path: Path):
        """Test basic acquire and release with Unix socket."""
        lock = SocketLock(socket_path=socket_path)
        assert not lock.is_acquired

        result = lock.try_acquire()
        assert result is True
        assert lock.is_acquired
        assert socket_path.exists()

        lock.release()
        assert not lock.is_acquired
        assert not socket_path.exists()

    def test_competing_locks(self, socket_path: Path):
        """Test that only one lock can be acquired for the same socket."""
        lock1 = SocketLock(socket_path=socket_path)
        lock2 = SocketLock(socket_path=socket_path)

        assert lock1.try_acquire() is True
        assert lock2.try_acquire() is False

        lock1.release()
        assert lock2.try_acquire() is True
        lock2.release()

    def test_stale_socket_cleanup(self, socket_path: Path):
        """Test that stale socket files are cleaned up."""
        # Create an actual socket file without anyone listening
        import socket as sock_module

        stale_sock = sock_module.socket(sock_module.AF_UNIX, sock_module.SOCK_STREAM)
        stale_sock.bind(str(socket_path))
        stale_sock.close()  # Close without unlinking - simulates crash

        lock = SocketLock(socket_path=socket_path)
        # Should be able to acquire since no one is listening
        assert lock.try_acquire() is True
        lock.release()

    def test_address_property(self, socket_path: Path):
        """Test the address property for Unix socket."""
        lock = SocketLock(socket_path=socket_path)
        assert lock.address == str(socket_path)


class TestSocketLockConcurrency:
    """Tests for SocketLock under concurrent access."""

    def test_multithread_competition(self, free_port: int):
        """Test that lock works correctly with multiple threads."""
        results = []
        results_lock = threading.Lock()

        def try_lock(port: int):
            # Each thread creates its own lock instance
            lock = SocketLock(port=port)
            result = lock.try_acquire()
            with results_lock:
                results.append(result)
            time.sleep(0.3)  # Hold for a bit
            if result:
                lock.release()

        # Start multiple threads trying to acquire simultaneously
        threads = [threading.Thread(target=try_lock, args=(free_port,)) for _ in range(5)]

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Exactly one True (winner), rest False
        assert sum(results) == 1

    def test_multiprocess_competition(self, free_port: int):
        """Test that lock works correctly across processes."""
        result_queue = multiprocessing.Queue()

        # Start multiple processes
        processes = [
            multiprocessing.Process(target=_acquire_lock_worker, args=(free_port, result_queue))
            for _ in range(3)
        ]

        for p in processes:
            p.start()

        for p in processes:
            p.join(timeout=5)

        # Collect results
        results = []
        while not result_queue.empty():
            results.append(result_queue.get())

        # Exactly one should have acquired the lock
        assert sum(results) == 1


class TestLockFile:
    """Tests for LockFile."""

    def test_acquire_and_release(self, lock_file_path: Path):
        """Test basic acquire and release."""
        lock = LockFile(lock_file_path)
        assert not lock.is_acquired

        result = lock.try_acquire()
        assert result is True
        assert lock.is_acquired
        assert lock_file_path.exists()

        # Check that PID is written
        content = lock_file_path.read_text()
        assert content.strip() == str(os.getpid())

        lock.release()
        assert not lock.is_acquired
        assert not lock_file_path.exists()

    def test_competing_locks(self, lock_file_path: Path):
        """Test that only one lock can be acquired."""
        lock1 = LockFile(lock_file_path)
        lock2 = LockFile(lock_file_path)

        assert lock1.try_acquire() is True
        assert lock2.try_acquire() is False

        lock1.release()
        assert lock2.try_acquire() is True
        lock2.release()

    def test_stale_lock_cleanup(self, lock_file_path: Path):
        """Test that stale lock files (dead process) are cleaned up."""
        # Write a PID that doesn't exist
        lock_file_path.write_text("99999999\n")

        lock = LockFile(lock_file_path)
        # Should be able to acquire since the PID is dead
        # Note: This may fail if PID 99999999 happens to exist
        result = lock.try_acquire()
        assert result is True
        lock.release()

    def test_get_owner_pid(self, lock_file_path: Path):
        """Test get_owner_pid method."""
        lock = LockFile(lock_file_path)

        # No lock file
        assert lock.get_owner_pid() is None

        lock.try_acquire()
        assert lock.get_owner_pid() == os.getpid()

        lock.release()
        assert lock.get_owner_pid() is None

    def test_double_acquire_raises(self, lock_file_path: Path):
        """Test that acquiring twice raises RuntimeError."""
        lock = LockFile(lock_file_path)
        lock.try_acquire()

        with pytest.raises(RuntimeError, match="already acquired"):
            lock.try_acquire()

        lock.release()

    def test_context_manager(self, lock_file_path: Path):
        """Test using LockFile as a context manager."""
        lock = LockFile(lock_file_path)

        with lock:
            assert lock.is_acquired

        assert not lock.is_acquired
