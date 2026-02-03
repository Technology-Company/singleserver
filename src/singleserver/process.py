"""
Process management for subprocess lifecycle, health monitoring, and restarts.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import IO

logger = logging.getLogger(__name__)


class ProcessState(Enum):
    """State of a managed process."""

    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    UNHEALTHY = "unhealthy"
    STOPPING = "stopping"
    FAILED = "failed"


@dataclass
class ProcessOptions:
    """Configuration options for a managed process."""

    # Command and environment
    command: list[str] = field(default_factory=list)
    env: dict[str, str] | None = None
    cwd: str | Path | None = None

    # Output handling
    stdout: str | Path | IO | None = None  # "inherit", "null", path, or file object
    stderr: str | Path | IO | None = None  # "inherit", "null", "stdout", path, or file object

    # Health and lifecycle
    health_check: Callable[[], bool] | None = None
    health_check_interval: float = 5.0
    startup_timeout: float = 30.0

    # Restart behavior
    restart_on_failure: bool = True
    max_restarts: int = 3
    restart_delay: float = 1.0
    restart_window: float = 60.0  # Reset restart count after this many seconds of stability

    # Shutdown
    shutdown_timeout: float = 10.0
    shutdown_signal: int = signal.SIGTERM

    def __post_init__(self):
        if isinstance(self.cwd, str):
            self.cwd = Path(self.cwd)


class ProcessOwner:
    """
    Manages a subprocess when we're the lock owner.

    Handles:
    - Starting the subprocess with proper environment and output handling
    - Health monitoring in a background thread
    - Automatic restarts on failure (configurable)
    - Graceful shutdown with timeout
    """

    def __init__(
        self,
        command: list[str],
        env: dict[str, str] | None = None,
        cwd: str | Path | None = None,
        stdout: str | Path | IO | None = None,
        stderr: str | Path | IO | None = None,
        health_check: Callable[[], bool] | None = None,
        health_check_interval: float = 5.0,
        startup_timeout: float = 30.0,
        restart_on_failure: bool = True,
        max_restarts: int = 3,
        restart_delay: float = 1.0,
        restart_window: float = 60.0,
        shutdown_timeout: float = 10.0,
        shutdown_signal: int = signal.SIGTERM,
        on_state_change: Callable[[ProcessState], None] | None = None,
    ):
        """
        Initialize the process owner.

        Args:
            command: Command to run as a list of strings.
            env: Additional environment variables (merged with current env).
            cwd: Working directory for the process.
            stdout: Where to send stdout ("inherit", "null", path, or file object).
            stderr: Where to send stderr ("inherit", "null", "stdout", path, or file object).
            health_check: Callable that returns True if the process is healthy.
            health_check_interval: Seconds between health checks.
            startup_timeout: Max seconds to wait for process to become healthy.
            restart_on_failure: Whether to restart the process if it dies.
            max_restarts: Maximum restart attempts before giving up.
            restart_delay: Seconds to wait between restart attempts.
            restart_window: Seconds of stability after which restart count resets.
            shutdown_timeout: Seconds to wait for graceful shutdown before SIGKILL.
            shutdown_signal: Signal to send for graceful shutdown.
            on_state_change: Callback when process state changes.
        """
        self.options = ProcessOptions(
            command=command,
            env=env,
            cwd=cwd,
            stdout=stdout,
            stderr=stderr,
            health_check=health_check,
            health_check_interval=health_check_interval,
            startup_timeout=startup_timeout,
            restart_on_failure=restart_on_failure,
            max_restarts=max_restarts,
            restart_delay=restart_delay,
            restart_window=restart_window,
            shutdown_timeout=shutdown_timeout,
            shutdown_signal=shutdown_signal,
        )

        self._process: subprocess.Popen | None = None
        self._state = ProcessState.STOPPED
        self._state_lock = threading.Lock()
        self._monitor_thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._restart_count = 0
        self._last_restart_time: float | None = None
        self._last_healthy_time: float | None = None
        self._on_state_change = on_state_change
        self._stdout_file: IO | None = None
        self._stderr_file: IO | None = None

    @property
    def state(self) -> ProcessState:
        """Current state of the process."""
        with self._state_lock:
            return self._state

    @property
    def pid(self) -> int | None:
        """PID of the managed process, or None if not running."""
        if self._process:
            return self._process.pid
        return None

    @property
    def restart_count(self) -> int:
        """Number of times the process has been restarted."""
        return self._restart_count

    def _set_state(self, state: ProcessState) -> None:
        """Update state and notify callback."""
        with self._state_lock:
            old_state = self._state
            self._state = state

        if old_state != state:
            logger.debug(f"Process state changed: {old_state.value} -> {state.value}")
            if self._on_state_change:
                try:
                    self._on_state_change(state)
                except Exception as e:
                    logger.warning(f"Error in state change callback: {e}")

    def _build_env(self) -> dict[str, str]:
        """Build the environment for the subprocess."""
        env = os.environ.copy()
        if self.options.env:
            env.update(self.options.env)
        return env

    def _get_stdout(self) -> int | IO | None:
        """Get the stdout configuration for subprocess."""
        stdout = self.options.stdout

        if stdout is None or stdout == "inherit":
            return None
        elif stdout == "null":
            return subprocess.DEVNULL
        elif isinstance(stdout, (str, Path)):
            self._stdout_file = open(stdout, "a")
            return self._stdout_file
        else:
            return stdout

    def _get_stderr(self) -> int | IO | None:
        """Get the stderr configuration for subprocess."""
        stderr = self.options.stderr

        if stderr is None or stderr == "inherit":
            return None
        elif stderr == "null":
            return subprocess.DEVNULL
        elif stderr == "stdout":
            return subprocess.STDOUT
        elif isinstance(stderr, (str, Path)):
            self._stderr_file = open(stderr, "a")
            return self._stderr_file
        else:
            return stderr

    def _close_log_files(self) -> None:
        """Close any log files we opened."""
        if self._stdout_file:
            try:
                self._stdout_file.close()
            except Exception:
                pass
            self._stdout_file = None
        if self._stderr_file:
            try:
                self._stderr_file.close()
            except Exception:
                pass
            self._stderr_file = None

    def start(self) -> None:
        """
        Start the subprocess.

        Raises:
            RuntimeError: If the process is already running.
            subprocess.SubprocessError: If the process fails to start.
        """
        if self._state not in (ProcessState.STOPPED, ProcessState.FAILED):
            raise RuntimeError(f"Cannot start process in state {self._state.value}")

        self._stop_event.clear()
        self._set_state(ProcessState.STARTING)

        try:
            self._process = subprocess.Popen(
                self.options.command,
                env=self._build_env(),
                cwd=self.options.cwd,
                stdout=self._get_stdout(),
                stderr=self._get_stderr(),
                # Start in new process group for clean shutdown
                start_new_session=True,
            )

            logger.info(
                f"Started process with PID {self._process.pid}: {' '.join(self.options.command)}"
            )

            # Start the monitor thread
            self._monitor_thread = threading.Thread(
                target=self._monitor_loop,
                daemon=True,
                name=f"process-monitor-{self._process.pid}",
            )
            self._monitor_thread.start()

        except Exception:
            self._set_state(ProcessState.FAILED)
            self._close_log_files()
            raise

    def _wait_for_healthy(self) -> bool:
        """
        Wait for the process to become healthy.

        Returns:
            True if the process became healthy, False if timeout or failure.
        """
        if not self.options.health_check:
            # No health check configured, assume healthy if process is running
            time.sleep(0.1)  # Brief delay to let process initialize
            if self._process and self._process.poll() is None:
                self._set_state(ProcessState.RUNNING)
                self._last_healthy_time = time.time()
                return True
            return False

        deadline = time.time() + self.options.startup_timeout
        check_interval = min(0.5, self.options.health_check_interval)

        while time.time() < deadline:
            if self._stop_event.is_set():
                return False

            # Check if process is still running
            if self._process and self._process.poll() is not None:
                logger.warning(
                    f"Process exited during startup with code {self._process.returncode}"
                )
                return False

            try:
                if self.options.health_check():
                    self._set_state(ProcessState.RUNNING)
                    self._last_healthy_time = time.time()
                    logger.info("Process is healthy")
                    return True
            except Exception as e:
                logger.debug(f"Health check failed: {e}")

            time.sleep(check_interval)

        logger.warning("Process did not become healthy within timeout")
        return False

    def _monitor_loop(self) -> None:
        """Background thread that monitors process health and handles restarts."""
        # First, wait for the process to become healthy
        if not self._wait_for_healthy():
            if self._process and self._process.poll() is None:
                # Process is running but not healthy, stop it
                self._do_stop()
            self._handle_failure()
            return

        # Now monitor ongoing health
        while not self._stop_event.is_set():
            self._stop_event.wait(self.options.health_check_interval)
            if self._stop_event.is_set():
                break

            # Check if process is still running
            if self._process and self._process.poll() is not None:
                logger.warning(f"Process exited unexpectedly with code {self._process.returncode}")
                self._handle_failure()
                return

            # Run health check if configured
            if self.options.health_check:
                try:
                    if self.options.health_check():
                        self._last_healthy_time = time.time()
                        if self._state == ProcessState.UNHEALTHY:
                            self._set_state(ProcessState.RUNNING)
                    else:
                        if self._state == ProcessState.RUNNING:
                            self._set_state(ProcessState.UNHEALTHY)
                            logger.warning("Process became unhealthy")
                except Exception as e:
                    if self._state == ProcessState.RUNNING:
                        self._set_state(ProcessState.UNHEALTHY)
                        logger.warning(f"Health check error: {e}")

            # Reset restart count if process has been stable
            if self._last_healthy_time and self._last_restart_time:
                if time.time() - self._last_healthy_time < self.options.restart_window:
                    if self._restart_count > 0:
                        self._restart_count = 0
                        logger.info("Process stable, reset restart count")

    def _handle_failure(self) -> None:
        """Handle process failure, potentially restarting."""
        self._close_log_files()

        if not self.options.restart_on_failure:
            self._set_state(ProcessState.FAILED)
            logger.info("Process failed, restart disabled")
            return

        if self._restart_count >= self.options.max_restarts:
            self._set_state(ProcessState.FAILED)
            logger.error(f"Process failed after {self._restart_count} restart attempts")
            return

        if self._stop_event.is_set():
            self._set_state(ProcessState.STOPPED)
            return

        # Attempt restart
        self._restart_count += 1
        self._last_restart_time = time.time()
        logger.info(f"Attempting restart {self._restart_count}/{self.options.max_restarts}")

        time.sleep(self.options.restart_delay)

        if self._stop_event.is_set():
            self._set_state(ProcessState.STOPPED)
            return

        try:
            self._set_state(ProcessState.STARTING)
            self._process = subprocess.Popen(
                self.options.command,
                env=self._build_env(),
                cwd=self.options.cwd,
                stdout=self._get_stdout(),
                stderr=self._get_stderr(),
                start_new_session=True,
            )
            logger.info(f"Restarted process with PID {self._process.pid}")

            if not self._wait_for_healthy():
                self._handle_failure()  # Recursive, will increment counter

        except Exception as e:
            logger.error(f"Failed to restart process: {e}")
            self._handle_failure()

    def _do_stop(self) -> None:
        """Actually stop the process (internal, no state management)."""
        if not self._process:
            return

        # Try graceful shutdown first
        try:
            os.killpg(os.getpgid(self._process.pid), self.options.shutdown_signal)
        except (ProcessLookupError, PermissionError):
            # Process already gone
            return

        # Wait for graceful shutdown
        try:
            self._process.wait(timeout=self.options.shutdown_timeout)
            logger.info(f"Process stopped gracefully with code {self._process.returncode}")
            return
        except subprocess.TimeoutExpired:
            pass

        # Force kill
        logger.warning("Graceful shutdown timeout, sending SIGKILL")
        try:
            os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
            self._process.wait(timeout=5)
        except (ProcessLookupError, PermissionError, subprocess.TimeoutExpired):
            pass

    def stop(self) -> None:
        """
        Stop the subprocess.

        Attempts graceful shutdown first, then SIGKILL if timeout expires.
        """
        if self._state in (ProcessState.STOPPED, ProcessState.FAILED):
            return

        self._set_state(ProcessState.STOPPING)
        self._stop_event.set()

        # Wait for monitor thread to stop
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=1)

        self._do_stop()
        self._close_log_files()
        self._set_state(ProcessState.STOPPED)
        self._process = None

    def wait(self, timeout: float | None = None) -> int | None:
        """
        Wait for the process to exit.

        Args:
            timeout: Maximum seconds to wait, or None for indefinite.

        Returns:
            Process return code, or None if timeout.
        """
        if not self._process:
            return None

        try:
            return self._process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            return None

    def is_running(self) -> bool:
        """Check if the process is currently running."""
        return self._state in (ProcessState.STARTING, ProcessState.RUNNING, ProcessState.UNHEALTHY)

    def __enter__(self) -> ProcessOwner:
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop()
