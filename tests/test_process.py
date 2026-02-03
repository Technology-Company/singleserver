"""
Tests for ProcessOwner.
"""

import signal
import sys
import time
from pathlib import Path

import pytest

from singleserver import ProcessOwner, ProcessState


class TestProcessOwnerBasic:
    """Basic tests for ProcessOwner."""

    def test_start_and_stop_simple_command(self):
        """Test starting and stopping a simple command."""
        owner = ProcessOwner(
            command=[sys.executable, "-c", "import time; time.sleep(60)"],
            restart_on_failure=False,
        )

        owner.start()
        assert owner.is_running()
        assert owner.pid is not None

        owner.stop()
        assert not owner.is_running()
        assert owner.state == ProcessState.STOPPED

    def test_process_exits_normally(self):
        """Test handling a process that exits normally."""
        owner = ProcessOwner(
            command=[sys.executable, "-c", "print('hello')"],
            restart_on_failure=False,
        )

        owner.start()
        # Wait for process to complete
        time.sleep(0.5)

        # Process should have exited and state should be FAILED (exited unexpectedly)
        assert owner.state in (ProcessState.FAILED, ProcessState.STOPPED)

    def test_context_manager(self):
        """Test ProcessOwner as a context manager."""
        with ProcessOwner(
            command=[sys.executable, "-c", "import time; time.sleep(60)"],
            restart_on_failure=False,
        ) as owner:
            assert owner.is_running()

        assert not owner.is_running()

    def test_wait_for_process(self):
        """Test waiting for a process to complete."""
        owner = ProcessOwner(
            command=[sys.executable, "-c", "import time; time.sleep(0.1); print('done')"],
            restart_on_failure=False,
        )

        owner.start()
        retcode = owner.wait(timeout=5)
        assert retcode == 0

    def test_start_twice_raises(self):
        """Test that starting twice raises an error."""
        owner = ProcessOwner(
            command=[sys.executable, "-c", "import time; time.sleep(60)"],
            restart_on_failure=False,
        )

        owner.start()
        with pytest.raises(RuntimeError, match="Cannot start"):
            owner.start()

        owner.stop()


class TestProcessOwnerEnvironment:
    """Tests for environment and working directory."""

    def test_custom_environment(self, temp_dir: Path):
        """Test that custom environment variables are passed."""
        output_file = temp_dir / "env_output.txt"

        owner = ProcessOwner(
            command=[
                sys.executable,
                "-c",
                f"import os; open('{output_file}', 'w').write(os.environ.get('TEST_VAR', 'not_found'))",
            ],
            env={"TEST_VAR": "test_value"},
            restart_on_failure=False,
        )

        owner.start()
        owner.wait(timeout=5)

        content = output_file.read_text()
        assert content == "test_value"

    def test_custom_working_directory(self, temp_dir: Path):
        """Test that custom working directory is used."""
        output_file = temp_dir / "cwd_output.txt"
        # Resolve symlinks for comparison (macOS /var -> /private/var)
        resolved_temp_dir = temp_dir.resolve()

        owner = ProcessOwner(
            command=[
                sys.executable,
                "-c",
                f"import os; open('{output_file}', 'w').write(os.getcwd())",
            ],
            cwd=temp_dir,
            restart_on_failure=False,
        )

        owner.start()
        owner.wait(timeout=5)

        content = output_file.read_text()
        # Compare resolved paths to handle symlinks
        assert Path(content).resolve() == resolved_temp_dir


class TestProcessOwnerOutput:
    """Tests for stdout/stderr handling."""

    def test_stdout_to_file(self, temp_dir: Path):
        """Test redirecting stdout to a file."""
        log_file = temp_dir / "stdout.log"

        owner = ProcessOwner(
            command=[sys.executable, "-c", "print('hello stdout')"],
            stdout=log_file,
            restart_on_failure=False,
        )

        owner.start()
        owner.wait(timeout=5)
        time.sleep(0.1)  # Allow file to be written

        content = log_file.read_text()
        assert "hello stdout" in content

    def test_stdout_null(self):
        """Test discarding stdout."""
        owner = ProcessOwner(
            command=[sys.executable, "-c", "print('discarded')"],
            stdout="null",
            restart_on_failure=False,
        )

        owner.start()
        owner.wait(timeout=5)
        # Just ensure it doesn't crash
        assert True

    def test_stderr_to_stdout(self, temp_dir: Path):
        """Test redirecting stderr to stdout."""
        log_file = temp_dir / "combined.log"

        owner = ProcessOwner(
            command=[
                sys.executable,
                "-c",
                "import sys; print('stdout'); print('stderr', file=sys.stderr)",
            ],
            stdout=log_file,
            stderr="stdout",
            restart_on_failure=False,
        )

        owner.start()
        owner.wait(timeout=5)
        time.sleep(0.1)

        content = log_file.read_text()
        assert "stdout" in content
        assert "stderr" in content


class TestProcessOwnerHealthCheck:
    """Tests for health check functionality."""

    def test_with_health_check(self, free_port: int, test_server_script: Path):
        """Test process with health check."""
        health_check_called = []

        def health_check():
            health_check_called.append(True)
            try:
                import socket

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1)
                result = s.connect_ex(("127.0.0.1", free_port))
                s.close()
                return result == 0
            except Exception:
                return False

        owner = ProcessOwner(
            command=[sys.executable, str(test_server_script), str(free_port)],
            health_check=health_check,
            health_check_interval=0.5,
            startup_timeout=10.0,
            restart_on_failure=False,
        )

        owner.start()
        time.sleep(2)  # Wait for health checks

        assert owner.state == ProcessState.RUNNING
        assert len(health_check_called) > 0

        owner.stop()

    def test_startup_timeout(self):
        """Test that startup timeout is respected."""

        # Process that never becomes healthy
        def never_healthy():
            return False

        owner = ProcessOwner(
            command=[sys.executable, "-c", "import time; time.sleep(60)"],
            health_check=never_healthy,
            startup_timeout=1.0,
            restart_on_failure=False,
        )

        owner.start()
        time.sleep(2)  # Wait for timeout

        assert owner.state == ProcessState.FAILED
        owner.stop()


class TestProcessOwnerRestart:
    """Tests for restart behavior."""

    def test_restart_on_failure(self, temp_dir: Path):
        """Test that process restarts on failure."""
        counter_file = temp_dir / "restart_count.txt"
        counter_file.write_text("0")

        # Command that increments counter and then exits
        command = [
            sys.executable,
            "-c",
            f"""
import time
count = int(open('{counter_file}').read())
count += 1
open('{counter_file}', 'w').write(str(count))
# Exit immediately to trigger restart
if count < 3:
    exit(1)
else:
    time.sleep(60)  # Stay alive after 3rd start
""",
        ]

        owner = ProcessOwner(
            command=command,
            restart_on_failure=True,
            max_restarts=5,
            restart_delay=0.1,
        )

        owner.start()
        time.sleep(3)  # Wait for restarts

        count = int(counter_file.read_text())
        assert count >= 2  # At least one restart happened

        owner.stop()

    def test_max_restarts_respected(self, temp_dir: Path):
        """Test that max_restarts limit is respected."""
        counter_file = temp_dir / "max_restart_count.txt"
        counter_file.write_text("0")

        # Command that always fails
        command = [
            sys.executable,
            "-c",
            f"""
count = int(open('{counter_file}').read())
count += 1
open('{counter_file}', 'w').write(str(count))
exit(1)
""",
        ]

        owner = ProcessOwner(
            command=command,
            restart_on_failure=True,
            max_restarts=2,
            restart_delay=0.1,
        )

        owner.start()
        time.sleep(3)  # Wait for restarts to exhaust

        assert owner.state == ProcessState.FAILED
        count = int(counter_file.read_text())
        assert count <= 3  # Initial + 2 restarts

    def test_no_restart_when_disabled(self, temp_dir: Path):
        """Test that restart doesn't happen when disabled."""
        counter_file = temp_dir / "no_restart_count.txt"
        counter_file.write_text("0")

        command = [
            sys.executable,
            "-c",
            f"""
count = int(open('{counter_file}').read())
count += 1
open('{counter_file}', 'w').write(str(count))
exit(1)
""",
        ]

        owner = ProcessOwner(
            command=command,
            restart_on_failure=False,
        )

        owner.start()
        time.sleep(1)

        assert owner.state == ProcessState.FAILED
        count = int(counter_file.read_text())
        assert count == 1  # Only ran once


class TestProcessOwnerShutdown:
    """Tests for graceful shutdown."""

    def test_graceful_shutdown(self):
        """Test that SIGTERM is sent first."""
        # Process that handles SIGTERM gracefully
        command = [
            sys.executable,
            "-c",
            """
import signal
import time

def handler(sig, frame):
    exit(0)

signal.signal(signal.SIGTERM, handler)
time.sleep(60)
""",
        ]

        owner = ProcessOwner(
            command=command,
            shutdown_timeout=5.0,
            shutdown_signal=signal.SIGTERM,
            restart_on_failure=False,
        )

        owner.start()
        time.sleep(0.5)  # Let it start

        start = time.time()
        owner.stop()
        elapsed = time.time() - start

        # Should stop quickly with graceful shutdown
        assert elapsed < 2.0
        assert owner.state == ProcessState.STOPPED

    def test_force_kill_on_timeout(self):
        """Test that SIGKILL is sent after timeout."""
        # Process that ignores SIGTERM
        command = [
            sys.executable,
            "-c",
            """
import signal
import time

signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(60)
""",
        ]

        owner = ProcessOwner(
            command=command,
            shutdown_timeout=0.5,  # Short timeout
            shutdown_signal=signal.SIGTERM,
            restart_on_failure=False,
        )

        owner.start()
        time.sleep(0.5)

        start = time.time()
        owner.stop()
        elapsed = time.time() - start

        # Should be killed after timeout
        assert elapsed < 2.0
        assert owner.state == ProcessState.STOPPED


class TestProcessOwnerStateCallback:
    """Tests for state change callbacks."""

    def test_state_change_callback(self):
        """Test that state change callback is called."""
        states = []

        def on_state_change(state: ProcessState):
            states.append(state)

        owner = ProcessOwner(
            command=[sys.executable, "-c", "print('hello')"],
            restart_on_failure=False,
            on_state_change=on_state_change,
        )

        owner.start()
        time.sleep(1)
        owner.stop()

        assert ProcessState.STARTING in states
        # Should have transitioned through states
        assert len(states) >= 1
