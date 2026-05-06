"""Helpers for managing consumer subprocesses in demos and tests.

Consumers must run as real OS processes — not threads or coroutines — because
only separate processes form independent Kafka consumer group members. This is
what makes rebalances and crashes behave exactly as they would in production.
"""

import os
import signal
import subprocess
import sys
import time

from src.helpers import PROCESSED_LOG


def start_consumer(module: str) -> subprocess.Popen:
    """Spawn a consumer module as a subprocess and return its handle."""
    return subprocess.Popen(
        [sys.executable, "-m", module],
        preexec_fn=os.setsid,  # put in its own process group so kill reaches all children
        cwd=str(PROCESSED_LOG.parent.parent),
    )


def stop_consumer(proc: subprocess.Popen, timeout: int = 10) -> None:
    """Gracefully stop a consumer with SIGINT, falling back to SIGKILL on timeout."""
    os.killpg(os.getpgid(proc.pid), signal.SIGINT)
    try:
        proc.wait(timeout)
    except subprocess.TimeoutExpired:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait()


def kill_consumer(proc: subprocess.Popen) -> None:
    """Hard-kill a consumer with SIGKILL — simulates a crash with no cleanup."""
    os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    proc.wait()


def wait_until(condition, timeout: int = 30, interval: float = 0.2) -> bool:
    """Poll condition every `interval` seconds until it returns True or timeout expires."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False
