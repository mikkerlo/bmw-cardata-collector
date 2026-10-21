"""Single-instance guard."""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from bmw_collector.lock import AlreadyRunningError, StandaloneLeader


def test_second_acquire_in_the_same_process_is_refused(tmp_path: Path) -> None:
    first = StandaloneLeader(tmp_path / "a.lock")
    first.acquire_sync()
    try:
        # flock is per open file descriptor, so a second leader object on the
        # same path must still be refused.
        with pytest.raises(AlreadyRunningError, match="only one streaming connection"):
            StandaloneLeader(tmp_path / "a.lock").acquire_sync()
    finally:
        first.release_sync()


def test_releasing_allows_a_later_acquire(tmp_path: Path) -> None:
    path = tmp_path / "b.lock"
    with StandaloneLeader(path) as leader:
        assert leader.is_leader
    second = StandaloneLeader(path)
    second.acquire_sync()
    assert second.is_leader
    second.release_sync()
    assert not second.is_leader


def test_acquire_is_reentrant_for_the_holder(tmp_path: Path) -> None:
    leader = StandaloneLeader(tmp_path / "c.lock")
    leader.acquire_sync()
    leader.acquire_sync()
    leader.release_sync()


def test_lock_file_records_the_pid_and_is_owner_only(tmp_path: Path) -> None:
    path = tmp_path / "d.lock"
    import os

    with StandaloneLeader(path):
        assert path.read_text(encoding="utf-8").strip() == str(os.getpid())
        assert path.stat().st_mode & 0o077 == 0


def test_a_second_process_is_refused(tmp_path: Path) -> None:
    path = tmp_path / "e.lock"
    holder = StandaloneLeader(path)
    holder.acquire_sync()
    script = textwrap.dedent(
        f"""
        from bmw_collector.lock import AlreadyRunningError, StandaloneLeader
        try:
            StandaloneLeader({str(path)!r}).acquire_sync()
        except AlreadyRunningError:
            raise SystemExit(17)
        raise SystemExit(0)
        """
    )
    try:
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-c", script], capture_output=True, text=True, timeout=60, check=False
        )
    finally:
        holder.release_sync()
    assert result.returncode == 17, result.stderr


def test_lock_released_when_the_holding_process_dies(tmp_path: Path) -> None:
    path = tmp_path / "f.lock"
    # The kernel drops flock when the fd closes, including on SIGKILL, so a
    # crashed collector must not leave the lock stuck.
    script = textwrap.dedent(
        f"""
        import os, signal
        from bmw_collector.lock import StandaloneLeader
        StandaloneLeader({str(path)!r}).acquire_sync()
        os.kill(os.getpid(), signal.SIGKILL)
        """
    )
    subprocess.run(  # noqa: S603
        [sys.executable, "-c", script], capture_output=True, timeout=60, check=False
    )
    survivor = StandaloneLeader(path)
    survivor.acquire_sync()
    survivor.release_sync()
