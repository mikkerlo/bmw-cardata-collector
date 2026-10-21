"""Local management endpoints: liveness, readiness and metrics.

These endpoints are unauthenticated, so they must not be exposed publicly.
The shipped compose file publishes the port on ``127.0.0.1`` only. ``/metrics``
carries no VIN or GCID labels, but request counts and timestamps still describe
a private vehicle's activity, which is reason enough to keep it local.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from enum import StrEnum
from typing import Any

from aiohttp import web
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from . import metrics

_LOGGER = logging.getLogger(__name__)


class Phase(StrEnum):
    """Lifecycle phases, surfaced so a stalled startup is diagnosable."""

    STARTING = "starting"
    OPENING_STORAGE = "opening_storage"
    AWAITING_AUTHORIZATION = "awaiting_authorization"
    DISCOVERING_VEHICLES = "discovering_vehicles"
    RUNNING = "running"
    DEGRADED = "degraded"
    STOPPING = "stopping"
    FATAL = "fatal"


class HealthState:
    """Mutable health snapshot shared between the runner and the endpoints."""

    def __init__(self, *, node_name: str, version: str) -> None:
        self.node_name = node_name
        self.version = version
        self.started_at = time.time()
        self.phase = Phase.STARTING
        self.ready = False
        self.ready_since: float | None = None
        self.last_error: str | None = None
        self.detail_providers: dict[str, Callable[[], dict[str, Any]]] = {}
        self._readiness_evaluator: Callable[[], tuple[bool, str | None]] | None = None
        metrics.collector_start_timestamp_seconds.set(self.started_at)
        metrics.collector_ready.set(0)
        metrics.collector_build_info.info({"version": version, "node": node_name})
        self.set_phase(Phase.STARTING)

    def set_phase(self, phase: Phase, *, error: str | None = None) -> None:
        """Record the current lifecycle phase."""
        if phase != self.phase:
            _LOGGER.info("phase %s -> %s", self.phase, phase, extra={"phase": str(phase)})
        self.phase = phase
        if error is not None:
            self.last_error = error
        metrics.collector_phase_info.info({"phase": str(phase)})

    def set_ready(self, ready: bool, *, error: str | None = None) -> None:
        """Mark the collector ready or not ready."""
        if ready and not self.ready:
            self.ready_since = time.time()
            metrics.collector_startup_duration_seconds.set(self.ready_since - self.started_at)
        self.ready = ready
        if error is not None:
            self.last_error = error
        metrics.collector_ready.set(1 if ready else 0)

    def register(self, name: str, provider: Callable[[], dict[str, Any]]) -> None:
        """Register a component status provider shown by ``/health/ready``."""
        self.detail_providers[name] = provider

    def set_readiness_evaluator(self, evaluator: Callable[[], tuple[bool, str | None]]) -> None:
        """Register a cheap, non-blocking readiness check.

        It is run on every ``/health/ready`` request so the answer describes
        the collector now, not whenever the supervisor last looked. An
        orchestrator acting on a stale "ready" is worse than a slow probe.
        """
        self._readiness_evaluator = evaluator

    def refresh(self) -> None:
        """Re-evaluate readiness, if an evaluator is registered."""
        if self._readiness_evaluator is None:
            return
        try:
            ready, error = self._readiness_evaluator()
        except Exception as err:  # never fail the probe on a bug here
            _LOGGER.exception("readiness evaluator failed")
            self.set_ready(False, error=f"readiness check failed: {type(err).__name__}")
            return
        self.set_ready(ready, error=error)

    def snapshot(self) -> dict[str, Any]:
        """Full, VIN-free status document."""
        components: dict[str, Any] = {}
        for name, provider in self.detail_providers.items():
            try:
                components[name] = provider()
            except Exception as err:  # noqa: BLE001 - never fail the probe
                components[name] = {"error": f"{type(err).__name__}: {err}"}
        return {
            "node": self.node_name,
            "version": self.version,
            "phase": str(self.phase),
            "ready": self.ready,
            "started_at": self.started_at,
            "uptime_seconds": round(time.time() - self.started_at, 3),
            "last_error": self.last_error,
            "components": components,
        }


def build_app(health: HealthState) -> web.Application:
    """Build the aiohttp application serving the management endpoints."""

    async def live(_request: web.Request) -> web.StreamResponse:
        # Liveness says only that this process and its event loop still run.
        # It deliberately does not depend on BMW sending anything: a parked car
        # is silent for days and that is not a fault.
        return web.json_response({"status": "alive", "phase": str(health.phase)})

    async def ready(_request: web.Request) -> web.StreamResponse:
        health.refresh()
        snapshot = health.snapshot()
        status = 200 if health.ready else 503
        return web.Response(
            status=status,
            body=json.dumps(snapshot, default=str).encode(),
            content_type="application/json",
        )

    async def metrics_endpoint(_request: web.Request) -> web.StreamResponse:
        return web.Response(
            body=generate_latest(metrics.REGISTRY), content_type=CONTENT_TYPE_LATEST.split(";")[0]
        )

    async def root(_request: web.Request) -> web.StreamResponse:
        return web.json_response(
            {
                "service": "bmw-cardata-collector",
                "version": health.version,
                "endpoints": ["/health/live", "/health/ready", "/metrics"],
            }
        )

    app = web.Application()
    app.add_routes(
        [
            web.get("/", root),
            web.get("/health/live", live),
            web.get("/health/ready", ready),
            web.get("/metrics", metrics_endpoint),
        ]
    )
    return app


class TelemetryServer:
    """Runs the management endpoints for the lifetime of the process."""

    def __init__(self, health: HealthState, *, host: str, port: int) -> None:
        self._health = health
        self._host = host
        self._port = port
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        """Bind and serve."""
        runner = web.AppRunner(build_app(self._health), access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        self._runner = runner
        _LOGGER.info(
            "management endpoints listening",
            extra={"host": self._host, "port": self._port},
        )

    async def stop(self) -> None:
        """Stop serving."""
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
