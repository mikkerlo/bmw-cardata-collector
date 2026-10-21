"""Configuration loading and validation.

Configuration is YAML. Environment references are expanded by this loader
(PyYAML does not do interpolation), supporting:

* ``${VAR}``            — required; startup fails if unset
* ``${VAR:-default}``   — optional with a default
* ``${file:/path}``     — read the (stripped) contents of a file, for Docker
                          or systemd secret mounts

Expansion applies to string scalars only, and a scalar that consists solely of
one reference keeps the referenced value's type after YAML re-parsing of
booleans and integers.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

_REF = re.compile(r"\$\{([^}]+)\}")


class ConfigError(Exception):
    """Raised when configuration is missing, malformed or inconsistent."""


def _resolve_ref(ref: str, *, env: dict[str, str]) -> str:
    if ref.startswith("file:"):
        path = Path(ref[len("file:") :].strip())
        try:
            return path.read_text(encoding="utf-8").strip()
        except OSError as err:
            raise ConfigError(f"cannot read secret file referenced by ${{{ref}}}: {err}") from err
    name, sep, default = ref.partition(":-")
    name = name.strip()
    if name in env:
        return env[name]
    if sep:
        return default
    raise ConfigError(
        f"environment variable {name!r} is referenced by the configuration but is not set"
    )


def expand(value: Any, *, env: dict[str, str] | None = None) -> Any:
    """Recursively expand environment references in a parsed YAML tree."""
    environ = dict(os.environ) if env is None else env
    if isinstance(value, dict):
        return {k: expand(v, env=environ) for k, v in value.items()}
    if isinstance(value, list):
        return [expand(v, env=environ) for v in value]
    if not isinstance(value, str):
        return value
    match = _REF.fullmatch(value.strip())
    if match:
        # Whole-scalar reference: re-parse so "true"/"8080" keep their type.
        resolved = _resolve_ref(match.group(1), env=environ)
        return yaml.safe_load(resolved) if resolved else resolved
    return _REF.sub(lambda m: _resolve_ref(m.group(1), env=environ), value)


VIN_RE = re.compile(r"^[A-HJ-NPR-Z0-9]{17}$")


@dataclass(frozen=True, slots=True)
class NodeConfig:
    """Identity of this collector instance, used in logs and metrics."""

    name: str = "collector-1"


@dataclass(frozen=True, slots=True)
class StorageConfig:
    """Durable storage settings."""

    sqlite_path: Path = Path("/data/collector.sqlite3")
    synchronous: str = "FULL"
    lock_path: Path | None = None
    queue_max: int = 10_000
    busy_timeout_ms: int = 10_000

    def resolved_lock_path(self) -> Path:
        """Lock file guarding against a second instance on the same host."""
        return self.lock_path or self.sqlite_path.with_suffix(".lock")


@dataclass(frozen=True, slots=True)
class VehiclesConfig:
    """Which VINs this collector is allowed to archive."""

    mode: str = "allowlist"
    vins: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class MqttConfig:
    """MQTT streaming receiver settings."""

    enabled: bool = True
    # ``per_vin`` subscribes to one topic per allowed VIN; ``wildcard`` uses
    # the documented "<gcid>/+" topic covering every mapped VIN.
    subscribe_mode: str = "per_vin"
    keepalive: int = 60
    reconnect_min_seconds: float = 5.0
    reconnect_max_seconds: float = 300.0
    # Backoff floor applied after an authorization/permission failure, so a
    # revoked client does not hammer BMW's broker.
    auth_failure_backoff_seconds: float = 900.0


@dataclass(frozen=True, slots=True)
class RestTaskConfig:
    """One periodic REST snapshot task."""

    type: str
    interval_hours: float
    container_id: str | None = None
    enabled: bool = True

    @property
    def key(self) -> str:
        """Stable identity used to persist the task's last-run timestamp."""
        return f"{self.type}:{self.container_id}" if self.container_id else self.type


KNOWN_REST_TASKS: frozenset[str] = frozenset(
    {"vehicle_mapping", "basic_data", "telematic_snapshot", "tyre_diagnosis"}
)


@dataclass(frozen=True, slots=True)
class RestConfig:
    """REST snapshot scheduler settings."""

    enabled: bool = True
    requests_per_day_budget: int = 35
    tasks: tuple[RestTaskConfig, ...] = ()
    timeout_seconds: float = 30.0
    max_retries: int = 3


@dataclass(frozen=True, slots=True)
class BmwConfig:
    """BMW account and client settings."""

    client_id: str
    credentials_file: Path = Path("/data/secrets/oauth.json")
    account_key: str = "default"
    vehicles: VehiclesConfig = field(default_factory=VehiclesConfig)
    mqtt: MqttConfig = field(default_factory=MqttConfig)
    rest: RestConfig = field(default_factory=RestConfig)


@dataclass(frozen=True, slots=True)
class TelemetryConfig:
    """Management endpoint settings."""

    listen_host: str = "0.0.0.0"  # noqa: S104 - container-internal; publish via Docker to 127.0.0.1
    port: int = 9090
    log_level: str = "INFO"


@dataclass(frozen=True, slots=True)
class Config:
    """Fully validated collector configuration."""

    bmw: BmwConfig
    node: NodeConfig = field(default_factory=NodeConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    telemetry: TelemetryConfig = field(default_factory=TelemetryConfig)


def _require_mapping(raw: Any, where: str) -> dict[str, Any]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"{where} must be a mapping, got {type(raw).__name__}")
    return raw


def _unknown_keys(raw: dict[str, Any], allowed: set[str], where: str) -> None:
    extra = set(raw) - allowed
    if extra:
        raise ConfigError(f"{where} has unknown key(s): {', '.join(sorted(extra))}")


def _parse_vehicles(raw: dict[str, Any]) -> VehiclesConfig:
    _unknown_keys(raw, {"mode", "vins"}, "bmw.vehicles")
    mode = str(raw.get("mode", "allowlist"))
    if mode not in {"allowlist", "all_primary"}:
        raise ConfigError("bmw.vehicles.mode must be 'allowlist' or 'all_primary'")
    vins_raw = raw.get("vins") or []
    if not isinstance(vins_raw, list):
        raise ConfigError("bmw.vehicles.vins must be a list")
    vins: list[str] = []
    for item in vins_raw:
        vin = str(item).strip().upper()
        if not VIN_RE.match(vin):
            raise ConfigError(
                f"bmw.vehicles.vins contains {item!r}, which is not a 17-character VIN"
            )
        if vin not in vins:
            vins.append(vin)
    return VehiclesConfig(mode=mode, vins=tuple(vins))


def _parse_mqtt(raw: dict[str, Any]) -> MqttConfig:
    allowed = {
        "enabled",
        "subscribe_mode",
        "keepalive",
        "reconnect_min_seconds",
        "reconnect_max_seconds",
        "auth_failure_backoff_seconds",
    }
    _unknown_keys(raw, allowed, "bmw.mqtt")
    mode = str(raw.get("subscribe_mode", "per_vin"))
    if mode not in {"per_vin", "wildcard"}:
        raise ConfigError("bmw.mqtt.subscribe_mode must be 'per_vin' or 'wildcard'")
    cfg = MqttConfig(
        enabled=bool(raw.get("enabled", True)),
        subscribe_mode=mode,
        keepalive=int(raw.get("keepalive", 60)),
        reconnect_min_seconds=float(raw.get("reconnect_min_seconds", 5.0)),
        reconnect_max_seconds=float(raw.get("reconnect_max_seconds", 300.0)),
        auth_failure_backoff_seconds=float(raw.get("auth_failure_backoff_seconds", 900.0)),
    )
    if cfg.reconnect_min_seconds <= 0 or cfg.reconnect_max_seconds < cfg.reconnect_min_seconds:
        raise ConfigError("bmw.mqtt reconnect window must satisfy 0 < min <= max")
    if cfg.keepalive < 10:
        raise ConfigError("bmw.mqtt.keepalive must be at least 10 seconds")
    return cfg


def _parse_rest(raw: dict[str, Any]) -> RestConfig:
    _unknown_keys(
        raw,
        {"enabled", "requests_per_day_budget", "tasks", "timeout_seconds", "max_retries"},
        "bmw.rest",
    )
    tasks_raw = raw.get("tasks") or []
    if not isinstance(tasks_raw, list):
        raise ConfigError("bmw.rest.tasks must be a list")
    tasks: list[RestTaskConfig] = []
    seen: set[str] = set()
    for index, item in enumerate(tasks_raw):
        item = _require_mapping(item, f"bmw.rest.tasks[{index}]")
        _unknown_keys(
            item, {"type", "interval_hours", "container_id", "enabled"}, f"bmw.rest.tasks[{index}]"
        )
        task_type = str(item.get("type", "")).strip()
        if task_type not in KNOWN_REST_TASKS:
            raise ConfigError(
                f"bmw.rest.tasks[{index}].type {task_type!r} is not one of: "
                + ", ".join(sorted(KNOWN_REST_TASKS))
            )
        try:
            interval = float(item["interval_hours"])
        except (KeyError, TypeError, ValueError) as err:
            raise ConfigError(f"bmw.rest.tasks[{index}] needs a numeric interval_hours") from err
        if interval <= 0:
            raise ConfigError(f"bmw.rest.tasks[{index}].interval_hours must be > 0")
        container = item.get("container_id")
        container_id = str(container) if container not in (None, "") else None
        if task_type == "telematic_snapshot" and not container_id:
            raise ConfigError(
                "bmw.rest.tasks: telematic_snapshot requires container_id "
                "(create a container in the BMW portal or with "
                "'collector containers create')"
            )
        task = RestTaskConfig(
            type=task_type,
            interval_hours=interval,
            container_id=container_id,
            enabled=bool(item.get("enabled", True)),
        )
        if task.key in seen:
            raise ConfigError(f"bmw.rest.tasks has a duplicate task {task.key!r}")
        seen.add(task.key)
        tasks.append(task)

    budget = int(raw.get("requests_per_day_budget", 35))
    if budget < 1:
        raise ConfigError("bmw.rest.requests_per_day_budget must be at least 1")
    cfg = RestConfig(
        enabled=bool(raw.get("enabled", True)),
        requests_per_day_budget=budget,
        tasks=tuple(tasks),
        timeout_seconds=float(raw.get("timeout_seconds", 30.0)),
        max_retries=int(raw.get("max_retries", 3)),
    )
    if cfg.enabled and not tasks:
        raise ConfigError("bmw.rest is enabled but no tasks are configured")
    return cfg


def _parse_storage(raw: dict[str, Any]) -> StorageConfig:
    _unknown_keys(
        raw, {"sqlite_path", "synchronous", "lock_path", "queue_max", "busy_timeout_ms"}, "storage"
    )
    synchronous = str(raw.get("synchronous", "FULL")).upper()
    if synchronous not in {"FULL", "NORMAL", "EXTRA"}:
        raise ConfigError("storage.synchronous must be FULL, EXTRA or NORMAL")
    queue_max = int(raw.get("queue_max", 10_000))
    if queue_max < 1:
        raise ConfigError("storage.queue_max must be at least 1")
    lock = raw.get("lock_path")
    return StorageConfig(
        sqlite_path=Path(str(raw.get("sqlite_path", "/data/collector.sqlite3"))),
        synchronous=synchronous,
        lock_path=Path(str(lock)) if lock else None,
        queue_max=queue_max,
        busy_timeout_ms=int(raw.get("busy_timeout_ms", 10_000)),
    )


def _parse_telemetry(raw: dict[str, Any]) -> TelemetryConfig:
    _unknown_keys(raw, {"listen_host", "port", "log_level"}, "telemetry")
    port = int(raw.get("port", 9090))
    if not 1 <= port <= 65535:
        raise ConfigError("telemetry.port must be between 1 and 65535")
    level = str(raw.get("log_level", "INFO")).upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ConfigError("telemetry.log_level must be a standard logging level name")
    return TelemetryConfig(
        listen_host=str(raw.get("listen_host", "0.0.0.0")),  # noqa: S104
        port=port,
        log_level=level,
    )


def parse_config(raw_tree: Any) -> Config:
    """Validate an already-expanded configuration tree."""
    root = _require_mapping(raw_tree, "configuration root")
    _unknown_keys(root, {"node", "storage", "bmw", "telemetry"}, "configuration root")

    node_raw = _require_mapping(root.get("node"), "node")
    _unknown_keys(node_raw, {"name"}, "node")

    bmw_raw = _require_mapping(root.get("bmw"), "bmw")
    _unknown_keys(
        bmw_raw,
        {"client_id", "credentials_file", "account_key", "vehicles", "mqtt", "rest"},
        "bmw",
    )
    client_id = str(bmw_raw.get("client_id", "")).strip()
    if not client_id:
        raise ConfigError(
            "bmw.client_id is required; create a CarData client in the BMW portal and pass "
            "it via the BMW_CLIENT_ID environment variable"
        )

    vehicles = _parse_vehicles(_require_mapping(bmw_raw.get("vehicles"), "bmw.vehicles"))
    mqtt = _parse_mqtt(_require_mapping(bmw_raw.get("mqtt"), "bmw.mqtt"))
    rest = _parse_rest(_require_mapping(bmw_raw.get("rest"), "bmw.rest"))

    # An empty allowlist must never silently subscribe to every car.
    if vehicles.mode == "allowlist" and not vehicles.vins and (mqtt.enabled or rest.enabled):
        raise ConfigError(
            "bmw.vehicles.mode is 'allowlist' but bmw.vehicles.vins is empty, so nothing "
            "would be collected. List the VINs to archive, or set mode 'all_primary' to "
            "archive every vehicle mapped to this account."
        )
    if mqtt.subscribe_mode == "wildcard" and vehicles.mode == "allowlist":
        raise ConfigError(
            "bmw.mqtt.subscribe_mode 'wildcard' receives every mapped VIN and therefore "
            "requires bmw.vehicles.mode 'all_primary'"
        )

    config = Config(
        node=NodeConfig(name=str(node_raw.get("name", "collector-1"))),
        storage=_parse_storage(_require_mapping(root.get("storage"), "storage")),
        bmw=BmwConfig(
            client_id=client_id,
            credentials_file=Path(str(bmw_raw.get("credentials_file", "/data/secrets/oauth.json"))),
            account_key=str(bmw_raw.get("account_key", "default")),
            vehicles=vehicles,
            mqtt=mqtt,
            rest=rest,
        ),
        telemetry=_parse_telemetry(_require_mapping(root.get("telemetry"), "telemetry")),
    )
    if config.bmw.rest.requests_per_day_budget > 50:
        # Not fatal: BMW documents 50/day/user, but the limit is theirs to change.
        import logging

        logging.getLogger(__name__).warning(
            "bmw.rest.requests_per_day_budget (%d) exceeds the documented BMW limit of 50 "
            "requests per day per user; requests will start failing with 403 CU-429",
            config.bmw.rest.requests_per_day_budget,
        )
    return config


def load_config(path: str | Path, *, env: dict[str, str] | None = None) -> Config:
    """Read, expand and validate the configuration file at ``path``."""
    file_path = Path(path)
    try:
        text = file_path.read_text(encoding="utf-8")
    except OSError as err:
        raise ConfigError(f"cannot read configuration file {file_path}: {err}") from err
    try:
        tree = yaml.safe_load(text)
    except yaml.YAMLError as err:
        raise ConfigError(f"configuration file {file_path} is not valid YAML: {err}") from err
    return parse_config(expand(tree, env=env))
