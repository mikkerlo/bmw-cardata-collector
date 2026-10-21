"""Command-line entry point.

Commands
--------
``run``                start the collector
``auth login``         interactive device-code authorization (one time)
``auth status``        authorization health, without revealing any token
``vehicles list``      vehicles known locally, and optionally refresh from BMW
``vehicles enable``    resume collection for a VIN
``vehicles disable``   stop collecting a VIN, keeping its history
``containers list``    list CarData containers (costs 1 REST request)
``containers create``  create a container for telematic snapshots
``snapshot``           run one REST task now (costs quota)
``export`` / ``import``  JSONL archive round-trip
``backup``             consistent database snapshot
``verify``             integrity check and a summary of what is stored
``incidents``          recent operational incidents
``simulate``           run the full write path with synthetic events, no BMW
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

from . import __version__
from .config import Config, ConfigError, RestTaskConfig, load_config
from .logging_setup import configure_logging

DEFAULT_CONFIG_PATH = os.environ.get("COLLECTOR_CONFIG", "/config/config.yaml")


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, default=str))


def _load(args: argparse.Namespace) -> Config:
    try:
        return load_config(args.config)
    except ConfigError as err:
        print(f"configuration error: {err}", file=sys.stderr)
        raise SystemExit(2) from err


# -- run ---------------------------------------------------------------------
def cmd_run(args: argparse.Namespace) -> int:
    """Start the collector."""
    config = _load(args)
    configure_logging(config.telemetry.log_level, plain=args.plain_logs)
    from .collector import Collector

    return asyncio.run(Collector(config).run())


# -- auth --------------------------------------------------------------------
async def _auth_login(config: Config, *, wait_seconds: float) -> int:
    from .auth import AuthError, AuthManager

    manager = AuthManager(config.bmw.client_id, config.bmw.credentials_file)
    try:
        grant = await manager.start_device_flow()
        print("\nAuthorize this collector with your BMW account:\n")
        print(f"  1. open {grant.verification_uri}")
        print(f"  2. sign in and enter the code:  {grant.user_code}")
        print(f"\nThe code expires in {grant.expires_in}s. Waiting...\n")
        sys.stdout.flush()
        try:
            tokens = await asyncio.wait_for(
                manager.poll_device_flow(grant), timeout=min(wait_seconds, grant.expires_in + 30)
            )
        except TimeoutError:
            print("timed out waiting for authorization", file=sys.stderr)
            return 1
        print("authorized.")
        print(f"refresh credential written to {manager.credential_path} (mode 0600)")
        missing = manager.missing_scopes()
        if missing:
            print(
                "\nWARNING: BMW did not grant: "
                + ", ".join(missing)
                + "\nSubscribe this CarData client to both the API and the Stream product in "
                "the BMW portal, then run this command again.",
                file=sys.stderr,
            )
        elif tokens.id_token is None:
            print(
                "\nWARNING: no id_token was returned, so MQTT streaming cannot authenticate. "
                "The 'openid' scope must be granted to this client.",
                file=sys.stderr,
            )
        return 0
    except AuthError as err:
        print(f"authorization failed: {err}", file=sys.stderr)
        return 1
    finally:
        await manager.close()


def cmd_auth_login(args: argparse.Namespace) -> int:
    """Run the interactive device-code flow."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    return asyncio.run(_auth_login(config, wait_seconds=args.timeout))


async def _auth_status(config: Config, *, refresh: bool) -> int:
    from .auth import AuthError, AuthManager

    manager = AuthManager(config.bmw.client_id, config.bmw.credentials_file)
    try:
        path = manager.credential_path
        status: dict[str, Any] = {
            "credential_path": str(path),
            "credential_present": path.is_file(),
            "credential_mode": oct(path.stat().st_mode & 0o777) if path.is_file() else None,
        }
        if not path.is_file():
            status["next_step"] = "run 'collector auth login'"
            _print_json(status)
            return 1
        manager.load_persisted()
        if refresh:
            try:
                tokens = await manager.refresh()
            except AuthError as err:
                status["refresh_ok"] = False
                status["error"] = str(err)
                _print_json(status)
                return 1
            status["refresh_ok"] = True
            status["access_token_expires_in_seconds"] = round(tokens.expires_at - time.time())
            status["id_token_present"] = tokens.id_token is not None
        status["state"] = manager.state.as_dict()
        _print_json(status)
        return 0
    finally:
        await manager.close()


def cmd_auth_status(args: argparse.Namespace) -> int:
    """Show authorization health without printing any token."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    return asyncio.run(_auth_status(config, refresh=args.refresh))


# -- vehicles ----------------------------------------------------------------
async def _vehicles_list(config: Config, *, refresh: bool) -> int:
    from .store import EventStore
    from .vehicle_registry import VehicleRegistry

    async with EventStore(
        config.storage.sqlite_path, synchronous=config.storage.synchronous
    ) as store:
        registry = VehicleRegistry(config.bmw.vehicles, store, account_key=config.bmw.account_key)
        await registry.load_persisted()
        if refresh:
            from .auth import AuthManager
            from .bmw_source import CarDataError, CarDataRestClient

            manager = AuthManager(config.bmw.client_id, config.bmw.credentials_file)
            client = CarDataRestClient(manager)
            try:
                print("fetching vehicle mappings (this costs 1 of BMW's 50 daily requests)...")
                response = await client.get_vehicle_mappings()
                await registry.update_from_response(response.body)
                await store.append_event(
                    registry.mapping_event(response.body, response.metadata(task="vehicle_mapping"))
                )
            except CarDataError as err:
                print(f"could not refresh mappings: {err}", file=sys.stderr)
                return 1
            finally:
                await client.close()
                await manager.close()
        _print_json(
            {
                "configured_mode": config.bmw.vehicles.mode,
                "configured_vins": list(config.bmw.vehicles.vins),
                "mapped": [
                    {
                        "vin": m.vin,
                        "mapping_type": m.mapping_type,
                        "mapped_since": m.mapped_since,
                        "primary": m.is_primary,
                    }
                    for m in registry.known_mappings
                ],
                "last_discovery_at": registry.last_discovery_at,
                "active": list(registry.active_vins()),
                "known_locally": await store.list_vehicles(),
                "note": (
                    "A mapped VIN is not necessarily collectable: streaming descriptors must "
                    "also be selected for it in the BMW CarData portal."
                ),
            }
        )
    return 0


def cmd_vehicles_list(args: argparse.Namespace) -> int:
    """List vehicles known locally, optionally refreshing from BMW."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    return asyncio.run(_vehicles_list(config, refresh=args.refresh))


async def _vehicles_set_enabled(config: Config, vin: str, enabled: bool) -> int:
    from .store import EventStore
    from .vehicle_registry import VehicleRegistry

    async with EventStore(
        config.storage.sqlite_path, synchronous=config.storage.synchronous
    ) as store:
        registry = VehicleRegistry(config.bmw.vehicles, store, account_key=config.bmw.account_key)
        await registry.load_persisted()
        await registry.set_enabled(vin, enabled)
        print(
            f"collection {'enabled' if enabled else 'disabled'} for the requested VIN; "
            "stored events are untouched"
        )
        if enabled and vin.strip().upper() not in config.bmw.vehicles.vins:
            print(
                "note: this VIN is not in bmw.vehicles.vins, so it will not be collected "
                "until it is added to the configuration",
                file=sys.stderr,
            )
        print("restart the collector for the change to take effect")
    return 0


def cmd_vehicles_enable(args: argparse.Namespace) -> int:
    """Resume collection for a VIN."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    return asyncio.run(_vehicles_set_enabled(config, args.vin, True))


def cmd_vehicles_disable(args: argparse.Namespace) -> int:
    """Stop collecting a VIN while keeping its history."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    return asyncio.run(_vehicles_set_enabled(config, args.vin, False))


# -- containers --------------------------------------------------------------
async def _containers(config: Config, args: argparse.Namespace) -> int:
    from .auth import AuthManager
    from .bmw_source import CarDataError, CarDataRestClient

    manager = AuthManager(config.bmw.client_id, config.bmw.credentials_file)
    client = CarDataRestClient(manager)
    try:
        if args.container_command == "list":
            print("listing containers (costs 1 of BMW's 50 daily requests)...")
            response = await client.list_containers()
        else:
            descriptors = [d.strip() for d in args.descriptors.split(",") if d.strip()]
            if not descriptors:
                print("at least one technical descriptor is required", file=sys.stderr)
                return 2
            print("creating container (costs 1 of BMW's 50 daily requests)...")
            response = await client.create_container(
                name=args.name, purpose=args.purpose, technical_descriptors=descriptors
            )
        print(response.body.decode("utf-8", errors="replace"))
        return 0
    except CarDataError as err:
        print(f"request failed: {err}", file=sys.stderr)
        return 1
    finally:
        await client.close()
        await manager.close()


def cmd_containers(args: argparse.Namespace) -> int:
    """List or create CarData containers."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    return asyncio.run(_containers(config, args))


# -- snapshot ----------------------------------------------------------------
async def _snapshot(config: Config, args: argparse.Namespace) -> int:
    from .auth import AuthManager
    from .bmw_source import CarDataError, CarDataRestClient
    from .rest_scheduler import RestScheduler
    from .store import EventStore
    from .vehicle_registry import VehicleRegistry
    from .writer import EventWriter

    task = RestTaskConfig(type=args.task, interval_hours=24.0, container_id=args.container_id)
    if task.type == "telematic_snapshot" and not task.container_id:
        print("--container-id is required for telematic_snapshot", file=sys.stderr)
        return 2

    async with EventStore(
        config.storage.sqlite_path, synchronous=config.storage.synchronous
    ) as store:
        manager = AuthManager(config.bmw.client_id, config.bmw.credentials_file)
        client = CarDataRestClient(manager)
        registry = VehicleRegistry(config.bmw.vehicles, store, account_key=config.bmw.account_key)
        await registry.load_persisted()
        writer = EventWriter(store, queue_max=config.storage.queue_max)
        writer.start()
        scheduler = RestScheduler(
            config.bmw.rest,
            client,
            writer,
            store,
            registry,
            account_key=config.bmw.account_key,
        )
        await scheduler.load_state()
        try:
            await scheduler.run_task_now(task)
        except CarDataError as err:
            print(f"snapshot failed: {err}", file=sys.stderr)
            return 1
        finally:
            await writer.stop()
            await client.close()
            await manager.close()
        _print_json({"task": task.key, "committed": writer.committed, **scheduler.status()})
    return 0


def cmd_snapshot(args: argparse.Namespace) -> int:
    """Run one REST task immediately."""
    config = _load(args)
    configure_logging(args.log_level, plain=True)
    return asyncio.run(_snapshot(config, args))


# -- archive tools -----------------------------------------------------------
def cmd_export(args: argparse.Namespace) -> int:
    """Export archived events as JSONL."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    from .export import export_to_path
    from .store import EventStore

    store = EventStore(config.storage.sqlite_path, synchronous=config.storage.synchronous)
    store.open_sync()
    try:
        count = export_to_path(
            store,
            args.output,
            vin=args.vin,
            source=args.source,
            since=args.since,
            until=args.until,
            after_id=args.after_id,
            limit=args.limit,
        )
    finally:
        store.close_sync()
    if str(args.output) != "-":
        print(f"exported {count} event(s) to {args.output}", file=sys.stderr)
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    """Import a JSONL export, idempotently by event UUID."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    from .export import import_from_path
    from .store import EventStore

    store = EventStore(config.storage.sqlite_path, synchronous=config.storage.synchronous)
    store.open_sync()
    try:
        inserted, skipped = import_from_path(store, args.input)
    finally:
        store.close_sync()
    _print_json({"inserted": inserted, "skipped_existing": skipped})
    return 0


def cmd_backup(args: argparse.Namespace) -> int:
    """Take a consistent snapshot of the database."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    from .store import EventStore

    store = EventStore(config.storage.sqlite_path, synchronous=config.storage.synchronous)
    store.open_sync()
    try:
        destination = store.backup_sync(args.output)
    finally:
        store.close_sync()
    print(f"backup written to {destination}")
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    """Check database integrity and summarise the archive."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    from .store import EventStore

    store = EventStore(config.storage.sqlite_path, synchronous=config.storage.synchronous)
    store.open_sync()
    try:
        result = store.integrity_check_sync()
        payload = {
            "path": str(config.storage.sqlite_path),
            "integrity_check": result,
            "schema_version": store.schema_version_sync(),
            "events": store.event_stats_sync(),
            "vehicles": len(store.list_vehicles_sync()),
        }
    finally:
        store.close_sync()
    _print_json(payload)
    return 0 if payload["integrity_check"] == "ok" else 1


def cmd_incidents(args: argparse.Namespace) -> int:
    """Show recent operational incidents."""
    config = _load(args)
    configure_logging("WARNING", plain=True)
    from .store import EventStore

    store = EventStore(config.storage.sqlite_path, synchronous=config.storage.synchronous)
    store.open_sync()
    try:
        _print_json(store.recent_incidents_sync(limit=args.limit))
    finally:
        store.close_sync()
    return 0


# -- simulate ----------------------------------------------------------------
async def _simulate(config: Config, *, count: int, interval: float, serve: bool) -> int:
    """Exercise the real write path with synthetic events and no BMW account."""
    from . import __version__ as version
    from .store import EventStore, RawEvent
    from .telemetry import HealthState, Phase, TelemetryServer
    from .writer import EventWriter

    health = HealthState(node_name=config.node.name, version=version)
    async with EventStore(
        config.storage.sqlite_path, synchronous=config.storage.synchronous
    ) as store:
        writer = EventWriter(store, queue_max=config.storage.queue_max)
        health.register("writer", writer.status)
        health.register("simulation", lambda: {"synthetic": True})
        writer.start()
        server = TelemetryServer(
            health, host=config.telemetry.listen_host, port=config.telemetry.port
        )
        if serve:
            await server.start()
        health.set_phase(Phase.RUNNING)
        health.set_ready(True)
        try:
            for index in range(count):
                await writer.enqueue(
                    RawEvent(
                        source="mqtt",
                        account_key=config.bmw.account_key,
                        payload=json.dumps(
                            {
                                "vin": "SIMULATEDVIN00000",
                                "entityId": f"sim-{index}",
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                                "data": {"simulated.counter": {"value": index}},
                            }
                        ).encode(),
                        vin="SIMULATEDVIN00000",
                        topic="simulated/SIMULATEDVIN00000",
                        mqtt_qos=0,
                        metadata={"simulated": True},
                    )
                )
                if interval:
                    await asyncio.sleep(interval)
            if serve:
                print(
                    f"simulation serving on {config.telemetry.listen_host}:"
                    f"{config.telemetry.port}; Ctrl-C to stop",
                    file=sys.stderr,
                )
                await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass
        finally:
            await writer.stop()
            if serve:
                await server.stop()
        _print_json({"simulated_events": count, **writer.status()})
    return 0


def cmd_simulate(args: argparse.Namespace) -> int:
    """Run the write path with synthetic events (no BMW credentials needed)."""
    config = _load(args)
    configure_logging(config.telemetry.log_level, plain=args.plain_logs)
    try:
        return asyncio.run(
            _simulate(config, count=args.count, interval=args.interval, serve=args.serve)
        )
    except KeyboardInterrupt:
        return 0


# -- parser ------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    """Construct the CLI parser."""
    # Global flags live in a parent parser so they work on either side of the
    # subcommand: both `collector -c f.yaml run` and `collector run -c f.yaml`.
    # SUPPRESS stops an absent subcommand copy from overwriting a value that was
    # given before the subcommand.
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument(
        "-c",
        "--config",
        type=Path,
        default=argparse.SUPPRESS,
        help=f"path to config.yaml (default: {DEFAULT_CONFIG_PATH})",
    )
    common.add_argument(
        "--plain-logs",
        action="store_true",
        default=argparse.SUPPRESS,
        help="human-readable logs instead of JSON",
    )

    parser = argparse.ArgumentParser(
        prog="collector",
        description="BMW CarData archival collector",
        parents=[common],
    )
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", parents=[common], help="start the collector")
    run.set_defaults(func=cmd_run)

    auth = sub.add_parser("auth", parents=[common], help="authorization").add_subparsers(
        dest="auth_command", required=True
    )
    login = auth.add_parser("login", parents=[common], help="interactive device-code authorization")
    login.add_argument("--timeout", type=float, default=600.0, help="seconds to wait for approval")
    login.set_defaults(func=cmd_auth_login)
    status = auth.add_parser(
        "status", parents=[common], help="authorization health (no tokens shown)"
    )
    status.add_argument(
        "--refresh", action="store_true", help="also exchange the refresh token to verify it"
    )
    status.set_defaults(func=cmd_auth_status)

    vehicles = sub.add_parser("vehicles", parents=[common], help="vehicle registry").add_subparsers(
        dest="vehicles_command", required=True
    )
    v_list = vehicles.add_parser("list", parents=[common], help="list known vehicles")
    v_list.add_argument(
        "--refresh", action="store_true", help="fetch mappings from BMW (costs 1 request)"
    )
    v_list.set_defaults(func=cmd_vehicles_list)
    v_enable = vehicles.add_parser("enable", parents=[common], help="resume collection for a VIN")
    v_enable.add_argument("vin")
    v_enable.set_defaults(func=cmd_vehicles_enable)
    v_disable = vehicles.add_parser(
        "disable", parents=[common], help="stop collecting a VIN, keeping history"
    )
    v_disable.add_argument("vin")
    v_disable.set_defaults(func=cmd_vehicles_disable)

    containers = sub.add_parser(
        "containers", parents=[common], help="CarData containers"
    ).add_subparsers(dest="container_command", required=True)
    c_list = containers.add_parser(
        "list", parents=[common], help="list containers (costs 1 request)"
    )
    c_list.set_defaults(func=cmd_containers)
    c_create = containers.add_parser(
        "create", parents=[common], help="create a container (costs 1 request)"
    )
    c_create.add_argument("--name", required=True)
    c_create.add_argument("--purpose", default="archival")
    c_create.add_argument(
        "--descriptors",
        required=True,
        help="comma-separated technical descriptors, e.g. "
        "vehicle.powertrain.electric.battery.stateOfCharge",
    )
    c_create.set_defaults(func=cmd_containers)

    snapshot = sub.add_parser(
        "snapshot", parents=[common], help="run one REST task now (costs quota)"
    )
    snapshot.add_argument(
        "task",
        choices=["vehicle_mapping", "basic_data", "telematic_snapshot", "tyre_diagnosis"],
    )
    snapshot.add_argument("--container-id", default=None)
    snapshot.add_argument("--log-level", default="INFO")
    snapshot.set_defaults(func=cmd_snapshot)

    export = sub.add_parser("export", parents=[common], help="export events as JSONL")
    export.add_argument("-o", "--output", default="-", help="output file, or - for stdout")
    export.add_argument("--vin")
    export.add_argument("--source", choices=["mqtt", "rest"])
    export.add_argument("--since", help="ISO-8601 lower bound on received_at_utc (inclusive)")
    export.add_argument("--until", help="ISO-8601 upper bound on received_at_utc (exclusive)")
    export.add_argument("--after-id", type=int, default=0, help="resume after this row id")
    export.add_argument("--limit", type=int)
    export.set_defaults(func=cmd_export)

    imp = sub.add_parser("import", parents=[common], help="import a JSONL export (idempotent)")
    imp.add_argument("-i", "--input", default="-", help="input file, or - for stdin")
    imp.set_defaults(func=cmd_import)

    backup = sub.add_parser("backup", parents=[common], help="consistent database snapshot")
    backup.add_argument("-o", "--output", required=True)
    backup.set_defaults(func=cmd_backup)

    verify = sub.add_parser("verify", parents=[common], help="integrity check and archive summary")
    verify.set_defaults(func=cmd_verify)

    incidents = sub.add_parser("incidents", parents=[common], help="recent operational incidents")
    incidents.add_argument("--limit", type=int, default=20)
    incidents.set_defaults(func=cmd_incidents)

    simulate = sub.add_parser(
        "simulate",
        parents=[common],
        help="exercise the write path with synthetic events (no BMW account)",
    )
    simulate.add_argument("--count", type=int, default=10)
    simulate.add_argument("--interval", type=float, default=0.0)
    simulate.add_argument(
        "--serve", action="store_true", help="keep serving health and metrics afterwards"
    )
    simulate.set_defaults(func=cmd_simulate)

    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    args = build_parser().parse_args(argv)
    # The global flags default to SUPPRESS so that a subcommand copy cannot
    # overwrite a value given before the subcommand; fill in the fallbacks here
    # rather than with set_defaults(), which would mutate the shared actions.
    if not hasattr(args, "config"):
        args.config = Path(DEFAULT_CONFIG_PATH)
    if not hasattr(args, "plain_logs"):
        args.plain_logs = False
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
