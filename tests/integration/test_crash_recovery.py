"""Abrupt-termination behaviour.

The collector is killed with SIGKILL mid-flight — no handlers, no flush, the
closest thing to a power cut this suite can arrange — and the database is then
reopened to check that committed events are intact.
"""

from __future__ import annotations

import signal
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from bmw_collector.store import EventStore

WRITER_SCRIPT = textwrap.dedent(
    """
    import os, signal, sys
    from bmw_collector.store import EventStore, RawEvent

    path, count, synchronous = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    store = EventStore(path, synchronous=synchronous)
    store.open_sync()
    for index in range(count):
        store.append_events_sync(
            [RawEvent(source="mqtt", account_key="test", payload=str(index).encode())]
        )
        # Report progress so the parent knows exactly what was committed.
        print(index, flush=True)
    # Die without closing the database, flushing, or running any handler.
    os.kill(os.getpid(), signal.SIGKILL)
    """
)


def run_until_killed(path: Path, count: int, synchronous: str = "FULL") -> int:
    """Run the writer until SIGKILL; return how many commits it reported."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", WRITER_SCRIPT, str(path), str(count), synchronous],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr
    return len([line for line in result.stdout.splitlines() if line.strip()])


@pytest.mark.parametrize("synchronous", ["FULL", "NORMAL"])
def test_committed_events_survive_sigkill(tmp_path: Path, synchronous: str) -> None:
    path = tmp_path / "crash.sqlite3"
    committed = run_until_killed(path, 50, synchronous)
    assert committed == 50

    store = EventStore(path, synchronous=synchronous)
    store.open_sync()
    try:
        assert store.integrity_check_sync() == "ok"
        # Everything the process reported as committed must be readable back.
        assert store.count_events_sync() == committed
        assert [int(e.payload) for e in store.iter_events_sync()] == list(range(committed))
    finally:
        store.close_sync()


def test_wal_sidecar_is_recovered_on_reopen(tmp_path: Path) -> None:
    path = tmp_path / "crash.sqlite3"
    run_until_killed(path, 20)
    # The -wal file is left behind by the kill; opening must replay it.
    assert Path(f"{path}-wal").exists() or Path(path).exists()

    store = EventStore(path)
    store.open_sync()
    try:
        assert store.count_events_sync() == 20
        store.checkpoint_sync("TRUNCATE")
        assert store.count_events_sync() == 20
    finally:
        store.close_sync()


def test_a_crash_does_not_corrupt_earlier_state(tmp_path: Path) -> None:
    path = tmp_path / "crash.sqlite3"
    first = EventStore(path)
    first.open_sync()
    first.set_state_sync("rest_quota", {"day": "2026-10-18", "used": 11})
    first.close_sync()

    run_until_killed(path, 10)

    store = EventStore(path)
    store.open_sync()
    try:
        assert store.get_state_sync("rest_quota")["used"] == 11
        assert store.count_events_sync() == 10
        assert store.integrity_check_sync() == "ok"
    finally:
        store.close_sync()


def test_a_backup_taken_before_the_crash_is_unaffected(tmp_path: Path) -> None:
    path = tmp_path / "crash.sqlite3"
    seeded = EventStore(path)
    seeded.open_sync()
    from bmw_collector.store import RawEvent

    seeded.append_events_sync(
        [RawEvent(source="mqtt", account_key="test", payload=b"before-backup")]
    )
    backup = seeded.backup_sync(tmp_path / "snap.sqlite3")
    seeded.close_sync()

    run_until_killed(path, 5)

    restored = EventStore(backup)
    restored.open_sync()
    try:
        assert restored.integrity_check_sync() == "ok"
        assert restored.count_events_sync() == 1
    finally:
        restored.close_sync()


def test_the_instance_lock_is_free_after_a_crash(tmp_path: Path) -> None:
    from bmw_collector.lock import StandaloneLeader

    script = textwrap.dedent(
        f"""
        import os, signal
        from bmw_collector.lock import StandaloneLeader
        StandaloneLeader({str(tmp_path / "crash.lock")!r}).acquire_sync()
        os.kill(os.getpid(), signal.SIGKILL)
        """
    )
    subprocess.run([sys.executable, "-c", script], capture_output=True, timeout=60, check=False)  # noqa: S603
    # A crashed collector must not leave the next start locked out.
    survivor = StandaloneLeader(tmp_path / "crash.lock")
    survivor.acquire_sync()
    survivor.release_sync()


def test_graceful_sigterm_drains_the_queue(tmp_path: Path) -> None:
    """A clean stop must commit what was already accepted."""
    script = textwrap.dedent(
        """
        import asyncio, sys
        from bmw_collector.store import EventStore, RawEvent
        from bmw_collector.writer import EventWriter

        async def main(path):
            store = EventStore(path)
            await store.open()
            writer = EventWriter(store, queue_max=10000)
            writer.start()
            for index in range(500):
                writer.submit(
                    RawEvent(source="mqtt", account_key="test", payload=str(index).encode())
                )
            # Stop immediately, with the queue still full of work.
            await writer.stop()
            await store.close()
            print(writer.committed, flush=True)

        asyncio.run(main(sys.argv[1]))
        """
    )
    path = tmp_path / "drain.sqlite3"
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-c", script, str(path)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert int(result.stdout.strip()) == 500

    store = EventStore(path)
    store.open_sync()
    try:
        assert store.count_events_sync() == 500
    finally:
        store.close_sync()


def test_sigterm_during_a_run_is_handled_cleanly(tmp_path: Path) -> None:
    """The simulate path installs no handlers of its own, so use the runner."""
    config = tmp_path / "config.yaml"
    config.write_text(
        f"""
node:
  name: crash-test
storage:
  sqlite_path: {tmp_path / "run.sqlite3"}
bmw:
  client_id: test-client
  credentials_file: {tmp_path / "secrets" / "oauth.json"}
  vehicles:
    mode: allowlist
    vins:
      - WBA12345678901234
  mqtt:
    enabled: false
  rest:
    enabled: false
telemetry:
  listen_host: 127.0.0.1
  port: 19399
  log_level: INFO
""",
        encoding="utf-8",
    )
    process = subprocess.Popen(  # noqa: S603
        [sys.executable, "-m", "bmw_collector", "-c", str(config), "run", "--plain-logs"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        # Give it time to open storage and start serving.
        import time

        deadline = time.time() + 30
        while time.time() < deadline and not (tmp_path / "run.sqlite3").exists():
            time.sleep(0.2)
        process.send_signal(signal.SIGTERM)
        _stdout, stderr = process.communicate(timeout=60)
    finally:
        if process.poll() is None:  # pragma: no cover - only on a stuck child
            process.kill()
            process.communicate()
    assert "stopped" in stderr
    # No stored credential, so it never became ready; it must still exit clean.
    assert process.returncode == 0, stderr

    store = EventStore(tmp_path / "run.sqlite3")
    store.open_sync()
    try:
        assert store.integrity_check_sync() == "ok"
    finally:
        store.close_sync()


def test_database_permissions_survive_a_crash(tmp_path: Path) -> None:
    path = tmp_path / "crash.sqlite3"
    run_until_killed(path, 5)
    assert path.stat().st_mode & 0o077 == 0
