"""Single-instance guard.

This is an *accidental double-start* guard for one host only: it takes an
exclusive ``flock`` on a file next to the database. It is deliberately not a
distributed lock — two collectors on two machines will both acquire their own
local lock and then fight over the single MQTT connection BMW allows per GCID.
A real leader election is out of scope for the MVP; see
:class:`bmw_collector.lock.LeaderElector` for the seam.
"""

from __future__ import annotations

import contextlib
import fcntl
import logging
import os
from pathlib import Path
from types import TracebackType
from typing import Protocol, Self

_LOGGER = logging.getLogger(__name__)


class AlreadyRunningError(Exception):
    """Raised when another process on this host already holds the lock."""


class LeaderElector(Protocol):
    """Seam for a future distributed elector (etcd lease, Postgres advisory)."""

    async def acquire(self) -> None:
        """Block until this instance may collect."""

    async def release(self) -> None:
        """Give up the right to collect."""

    @property
    def is_leader(self) -> bool:
        """Whether this instance currently holds leadership."""


class StandaloneLeader:
    """Host-local exclusive lock implementing the :class:`LeaderElector` seam."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self._fd: int | None = None

    @property
    def is_leader(self) -> bool:
        """Whether the lock is currently held by this process."""
        return self._fd is not None

    def acquire_sync(self) -> None:
        """Take the lock, or raise :class:`AlreadyRunningError`."""
        if self._fd is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as err:
            os.close(fd)
            holder = ""
            with contextlib.suppress(OSError):  # unreadable lock file
                holder = self.path.read_text(encoding="utf-8").strip()
            raise AlreadyRunningError(
                f"another collector already holds {self.path}"
                + (f" (pid {holder})" if holder else "")
                + "; BMW permits only one streaming connection per account"
            ) from err
        os.truncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        os.fsync(fd)
        self._fd = fd
        _LOGGER.info("acquired instance lock", extra={"lock_path": str(self.path)})

    async def acquire(self) -> None:
        """Async form of :meth:`acquire_sync` (the lock is non-blocking)."""
        self.acquire_sync()

    def release_sync(self) -> None:
        """Release the lock if held."""
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)

    async def release(self) -> None:
        """Async form of :meth:`release_sync`."""
        self.release_sync()

    def __enter__(self) -> Self:
        self.acquire_sync()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release_sync()
