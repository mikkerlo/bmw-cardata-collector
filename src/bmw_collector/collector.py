"""Process composition: wires the components together and owns the lifecycle."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import time
from typing import Any

import aiohttp

from . import __version__, metrics
from .auth import AuthError, AuthManager, ReauthorizationRequiredError
from .bmw_source import CarDataRestClient
from .config import Config
from .lock import AlreadyRunningError, StandaloneLeader
from .mqtt_receiver import MqttReceiver
from .rest_scheduler import RestScheduler
from .store import EventStore, StorageError, StorageFullError
from .telemetry import HealthState, Phase, TelemetryServer
from .vehicle_registry import VehicleRegistry
from .writer import EventWriter

_LOGGER = logging.getLogger(__name__)

#: How often readiness, authorization and phase are re-evaluated.
SUPERVISE_INTERVAL_SECONDS = 15.0
#: How often durable storage is probed with a real write. Each probe is an
#: fsynced commit, so this is kept well above the supervise interval: the
#: point is to notice a full or read-only volume while the car is parked and
#: no real events are arriving, not to write constantly on idle hardware.
STORAGE_PROBE_INTERVAL_SECONDS = 300.0
#: How often to retry authorization while it is missing or broken.
AUTH_RETRY_SECONDS = 60.0
#: How often to checkpoint the WAL back into the main database file.
CHECKPOINT_INTERVAL_SECONDS = 900.0


class Collector:
    """The running collector: storage, auth, MQTT ingest, REST snapshots."""

    def __init__(self, config: Config) -> None:
        self.config = config
        self.health = HealthState(node_name=config.node.name, version=__version__)
        self.store = EventStore(
            config.storage.sqlite_path,
            synchronous=config.storage.synchronous,
            busy_timeout_ms=config.storage.busy_timeout_ms,
        )
        self.leader = StandaloneLeader(config.storage.resolved_lock_path())
        self.writer = EventWriter(self.store, queue_max=config.storage.queue_max)
        self.telemetry = TelemetryServer(
            self.health, host=config.telemetry.listen_host, port=config.telemetry.port
        )
        self._session: aiohttp.ClientSession | None = None
        self.auth: AuthManager | None = None
        self.rest_client: CarDataRestClient | None = None
        self.registry: VehicleRegistry | None = None
        self.receiver: MqttReceiver | None = None
        self.scheduler: RestScheduler | None = None
        self._stop = asyncio.Event()
        self._authorized = False
        # Last result of the periodic durable-storage write probe. Cached so
        # the readiness endpoint stays cheap: probing per scrape would mean a
        # database write per request.
        self._storage_writable = True

    # -- status providers ---------------------------------------------------
    def _register_providers(self) -> None:
        self.health.set_readiness_evaluator(self._readiness)
        self.health.register("storage", self._storage_status)
        self.health.register("writer", self.writer.status)
        self.health.register("auth", self._auth_status)
        if self.receiver is not None:
            self.health.register("mqtt", self.receiver.status)
        if self.scheduler is not None:
            self.health.register("rest", self.scheduler.status)
        if self.registry is not None:
            self.health.register("vehicles", self._vehicles_status)

    def _storage_status(self) -> dict[str, Any]:
        return {
            "path": str(self.config.storage.sqlite_path),
            "synchronous": self.config.storage.synchronous,
            "healthy": self.writer.storage_healthy,
            "last_error": self.writer.last_write_error,
        }

    def _auth_status(self) -> dict[str, Any]:
        if self.auth is None:
            return {"authorized": False, "reason": "not initialised"}
        status = self.auth.state.as_dict()
        status["credential_path"] = str(self.auth.credential_path)
        return status

    def _vehicles_status(self) -> dict[str, Any]:
        assert self.registry is not None
        # Counts only: a VIN identifies a specific private vehicle, and this
        # document is served by an unauthenticated endpoint.
        return {
            "configured_mode": self.config.bmw.vehicles.mode,
            "active_count": len(self.registry.active_vins()),
            "mapped_count": len(self.registry.known_mappings),
            "subscribable_count": len(self.registry.subscribable_vins()),
            "last_discovery_at": self.registry.last_discovery_at,
        }

    # -- startup ------------------------------------------------------------
    async def _open_storage(self) -> None:
        self.health.set_phase(Phase.OPENING_STORAGE)
        await self.store.open()
        version = await asyncio.to_thread(self.store.schema_version_sync)
        integrity = await asyncio.to_thread(self.store.integrity_check_sync)
        if integrity != "ok":
            # Refuse to append to a database that may be damaged.
            raise StorageError(f"SQLite integrity check failed: {integrity}")
        count = await self.store.count_events()
        _LOGGER.info(
            "storage ready",
            extra={"schema_version": version, "events": count, "integrity": integrity},
        )
        self._storage_writable = await self.store.writable()
        metrics.collector_storage_writable.set(1 if self._storage_writable else 0)

    async def _authorize(self) -> bool:
        """Try to establish a usable token set. Returns success."""
        assert self.auth is not None
        try:
            if not self.auth.load_persisted():
                self.health.set_phase(
                    Phase.AWAITING_AUTHORIZATION,
                    error=(
                        f"no stored credential at {self.auth.credential_path}; "
                        "run 'collector auth login' once to authorize this collector"
                    ),
                )
                metrics.bmw_auth_reauthorization_required.set(1)
                return False
            await self.auth.refresh()
        except ReauthorizationRequiredError as err:
            self.health.set_phase(Phase.AWAITING_AUTHORIZATION, error=str(err))
            metrics.bmw_auth_reauthorization_required.set(1)
            metrics.bmw_auth_errors_total.inc()
            _LOGGER.error("reauthorization required: %s", err)
            await self._incident("auth_reauthorization_required", {"error": err.error})
            return False
        except AuthError as err:
            self.health.set_phase(Phase.AWAITING_AUTHORIZATION, error=str(err))
            metrics.bmw_auth_errors_total.inc()
            _LOGGER.error("authorization failed: %s", err)
            return False
        except Exception as err:  # the collector must keep trying
            # An unexpected failure here must not take the process down: the
            # supervisor retries, and tearing the process down would also take
            # the telemetry endpoints with it, leaving nothing to diagnose
            # from. The restart policy is a worse tool than a retry loop for
            # something that may well be transient.
            self.health.set_phase(
                Phase.AWAITING_AUTHORIZATION, error=f"{type(err).__name__}: {err}"
            )
            metrics.bmw_auth_errors_total.inc()
            _LOGGER.exception("unexpected error while authorizing")
            await self._incident("auth_unexpected_error", {"error": str(err)})
            return False

        metrics.bmw_auth_reauthorization_required.set(0)
        metrics.bmw_auth_expires_at_seconds.set(self.auth.state.access_expires_at or 0)
        missing = self.auth.missing_scopes()
        if missing:
            _LOGGER.error(
                "the BMW client is missing required scope(s): %s. Subscribe this CarData "
                "client to both the API and the Stream product in the BMW portal, then run "
                "'collector auth login' again.",
                ", ".join(missing),
            )
            await self._incident("auth_missing_scopes", {"missing": list(missing)})
        _LOGGER.info("authorized", extra={"scopes": list(self.auth.state.scopes)})
        return True

    async def _build_components(self) -> None:
        assert self.auth is not None
        cfg = self.config.bmw
        self.registry = VehicleRegistry(cfg.vehicles, self.store, account_key=cfg.account_key)
        await self.registry.load_persisted()

        self.rest_client = CarDataRestClient(
            self.auth, session=self._session, timeout_seconds=cfg.rest.timeout_seconds
        )
        self.scheduler = RestScheduler(
            cfg.rest,
            self.rest_client,
            self.writer,
            self.store,
            self.registry,
            account_key=cfg.account_key,
        )
        await self.scheduler.load_state()

        if cfg.mqtt.enabled:
            self.receiver = MqttReceiver(
                cfg.mqtt,
                self.auth,
                self.writer,
                self.store,
                account_key=cfg.account_key,
                topics_provider=self.registry.subscribable_vins,
            )

    # -- run ----------------------------------------------------------------
    async def run(self) -> int:
        """Run until a termination signal arrives. Returns a process exit code."""
        try:
            self.leader.acquire_sync()
        except AlreadyRunningError as err:
            _LOGGER.error("refusing to start: %s", err)
            self.health.set_phase(Phase.FATAL, error=str(err))
            return 1

        self._install_signal_handlers()
        exit_code = 0
        try:
            # Serve telemetry first so a stalled startup is observable.
            await self.telemetry.start()
            await self._open_storage()
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.config.bmw.rest.timeout_seconds)
            )
            self.auth = AuthManager(
                self.config.bmw.client_id,
                self.config.bmw.credentials_file,
                session=self._session,
            )
            self.writer.start()
            self._authorized = await self._authorize()
            await self._build_components()
            self._register_providers()

            if self._authorized:
                self._start_collection()
            await self._supervise()
        except (StorageFullError, StorageError) as err:
            _LOGGER.error("fatal storage error: %s", err)
            self.health.set_phase(Phase.FATAL, error=str(err))
            self.health.set_ready(False)
            exit_code = 1
        except asyncio.CancelledError:
            pass
        except Exception as err:  # report it, then shut down cleanly
            _LOGGER.exception("fatal error")
            self.health.set_phase(Phase.FATAL, error=f"{type(err).__name__}: {err}")
            self.health.set_ready(False)
            exit_code = 1
        finally:
            await self._shutdown()
        return exit_code

    def _start_collection(self) -> None:
        if self.receiver is not None:
            self.receiver.start()
        if self.scheduler is not None:
            self.scheduler.start()
        self.health.set_phase(
            Phase.DISCOVERING_VEHICLES
            if self.registry is not None and not self.registry.known_mappings
            else Phase.RUNNING
        )

    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            with contextlib.suppress(NotImplementedError, ValueError):
                loop.add_signal_handler(sig, self._request_stop, sig)

    def _request_stop(self, sig: signal.Signals) -> None:
        _LOGGER.info("received %s, shutting down", sig.name)
        self._stop.set()

    async def _supervise(self) -> None:
        """Re-evaluate health, retry authorization, checkpoint the WAL."""
        last_checkpoint = time.monotonic()
        last_auth_attempt = 0.0
        last_storage_probe = time.monotonic()
        while not self._stop.is_set():
            if not self._authorized and time.monotonic() - last_auth_attempt > AUTH_RETRY_SECONDS:
                last_auth_attempt = time.monotonic()
                if await self._authorize():
                    self._authorized = True
                    self._start_collection()

            if self._authorized and self.auth is not None:
                metrics.bmw_auth_expires_at_seconds.set(self.auth.state.access_expires_at or 0)

            probe_due = time.monotonic() - last_storage_probe > STORAGE_PROBE_INTERVAL_SECONDS
            if probe_due:
                last_storage_probe = time.monotonic()
            await self._evaluate_readiness(probe_storage=probe_due)

            if time.monotonic() - last_checkpoint > CHECKPOINT_INTERVAL_SECONDS:
                last_checkpoint = time.monotonic()
                with contextlib.suppress(StorageError):
                    await self.store.checkpoint("PASSIVE")

            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=SUPERVISE_INTERVAL_SECONDS)

    def _readiness(self) -> tuple[bool, str | None]:
        """Whether this collector can archive what arrives, and why not.

        Deliberately cheap and synchronous: it runs on every readiness
        request. It does *not* require recent messages — a parked car sends
        nothing for days — but it does require durable storage, valid
        authorization, and, when streaming is enabled, a receiver that has
        connected at least once and is not blocked on a permission problem.
        """
        reasons: list[str] = []
        if not self._storage_writable:
            reasons.append("durable storage is not writable")
        if not self.writer.storage_healthy:
            reasons.append("the last storage write failed")
        if not self._authorized:
            reasons.append("not authorized with BMW")
            if self.auth is not None and self.auth.state.last_error:
                reasons.append(f"last auth error: {self.auth.state.last_error}")
        if self.receiver is not None:
            if self.receiver.auth_blocked:
                reasons.append("MQTT streaming is not authorized")
            elif not self.receiver.ever_connected:
                reasons.append("MQTT has not connected yet")

        if reasons:
            return False, "; ".join(reasons)
        return True, None

    async def _evaluate_readiness(self, *, probe_storage: bool = True) -> None:
        """Re-evaluate readiness and phase, optionally re-probing storage."""
        if probe_storage:
            self._storage_writable = await self.store.writable()
            metrics.collector_storage_writable.set(1 if self._storage_writable else 0)
        self.health.refresh()
        if not self.health.ready:
            if self.health.phase in {Phase.RUNNING, Phase.DISCOVERING_VEHICLES}:
                self.health.set_phase(Phase.DEGRADED)
            return
        degraded = self.receiver is not None and not self.receiver.connected
        self.health.set_phase(Phase.DEGRADED if degraded else Phase.RUNNING)

    async def _incident(self, kind: str, details: dict[str, Any]) -> None:
        with contextlib.suppress(Exception):
            await self.store.record_incident(kind, details)
            metrics.collector_incidents_total.labels(kind=kind).inc()

    # -- shutdown -----------------------------------------------------------
    async def _shutdown(self) -> None:
        if self.health.phase is not Phase.FATAL:
            self.health.set_phase(Phase.STOPPING)
        self.health.set_ready(False)
        _LOGGER.info("stopping")
        if self.receiver is not None:
            await self.receiver.stop()
        if self.scheduler is not None:
            await self.scheduler.stop()
        # Drain queued events before closing the database.
        await self.writer.stop()
        if self.rest_client is not None:
            await self.rest_client.close()
        if self.auth is not None:
            await self.auth.close()
        if self._session is not None and not self._session.closed:
            await self._session.close()
        with contextlib.suppress(StorageError):
            await self.store.checkpoint("TRUNCATE")
        await self.store.close()
        await self.telemetry.stop()
        self.leader.release_sync()
        _LOGGER.info(
            "stopped",
            extra={
                "committed": self.writer.committed,
                "dropped": self.writer.dropped,
                "write_failures": self.writer.write_failures,
            },
        )
