"""In-process CLI tests for the onboarding paths.

`tests/integration/test_cli.py` drives the CLI as a subprocess, which is the
real contract but cannot reach commands that talk to BMW. These exercise those
command functions directly with the BMW edges stubbed, because a broken
`containers create` or `snapshot` is discovered at the worst possible moment:
during first-time setup.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, ClassVar

import pytest

from bmw_collector.__main__ import build_parser, main
from bmw_collector.bmw_source import CarDataError, QuotaExhaustedError, RestResponse
from tests.conftest import VALID_VIN, write_config


class StubAuthManager:
    """Replaces AuthManager in CLI command functions."""

    instances: ClassVar[list[StubAuthManager]] = []

    def __init__(self, client_id: str, credentials_path: Any, **_kwargs: Any) -> None:
        self.client_id = client_id
        self.credential_path = Path(credentials_path)
        self.closed = False
        StubAuthManager.instances.append(self)

    async def access_token(self) -> str:
        """A bearer token."""
        return "stub-access-token"

    def load_persisted(self) -> bool:
        """Pretend a credential exists."""
        return True

    async def close(self) -> None:
        """Record that the CLI closed us."""
        self.closed = True


class StubRestClient:
    """Replaces CarDataRestClient in CLI command functions."""

    outcomes: ClassVar[list[Any]] = []
    calls: ClassVar[list[tuple[str, tuple[Any, ...], dict[str, Any]]]] = []

    def __init__(self, *_args: Any, **_kwargs: Any) -> None:
        self.closed = False

    def _next(self, _label: str, *args: Any, **kwargs: Any) -> RestResponse:
        StubRestClient.calls.append((_label, args, kwargs))
        if not StubRestClient.outcomes:
            return RestResponse(
                method="GET", url="https://api.invalid/x", status=200, body=b'{"ok":true}'
            )
        outcome = StubRestClient.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    async def list_containers(self) -> RestResponse:
        """Scripted container listing."""
        return self._next("list_containers")

    async def create_container(self, **kwargs: Any) -> RestResponse:
        """Scripted container creation."""
        return self._next("create_container", **kwargs)

    async def get_vehicle_mappings(self) -> RestResponse:
        """Scripted mappings call."""
        return self._next("get_vehicle_mappings")

    async def get_basic_data(self, vin: str) -> RestResponse:
        """Scripted basic-data call."""
        return self._next("get_basic_data", vin)

    async def get_telematic_data(self, vin: str, container_id: str) -> RestResponse:
        """Scripted telematic-data call."""
        return self._next("get_telematic_data", vin, container_id)

    async def get_tyre_diagnosis(self, vin: str) -> RestResponse:
        """Scripted tyre call."""
        return self._next("get_tyre_diagnosis", vin)

    async def close(self) -> None:
        """Mark closed."""
        self.closed = True


@pytest.fixture(autouse=True)
def stub_bmw(monkeypatch: pytest.MonkeyPatch) -> None:
    """Replace the BMW client and auth manager the CLI imports lazily."""
    StubAuthManager.instances.clear()
    StubRestClient.outcomes.clear()
    StubRestClient.calls.clear()
    monkeypatch.setattr("bmw_collector.auth.AuthManager", StubAuthManager)
    monkeypatch.setattr("bmw_collector.bmw_source.CarDataRestClient", StubRestClient)


@pytest.fixture
def config_path(tmp_path: Path) -> Path:
    """A valid configuration file."""
    return write_config(tmp_path)


def ok(body: bytes) -> RestResponse:
    """A successful captured response."""
    return RestResponse(method="GET", url="https://api.invalid/x", status=200, body=body)


# -- parser ------------------------------------------------------------------
def test_every_subcommand_is_reachable_and_documented() -> None:
    parser = build_parser()
    for argv in (
        ["run"],
        ["auth", "login"],
        ["auth", "status"],
        ["vehicles", "list"],
        ["vehicles", "enable", VALID_VIN],
        ["vehicles", "disable", VALID_VIN],
        ["containers", "list"],
        ["containers", "create", "--name", "n", "--descriptors", "a.b"],
        ["snapshot", "basic_data"],
        ["export"],
        ["import"],
        ["backup", "-o", "backup.sqlite3"],
        ["verify"],
        ["incidents"],
        ["simulate"],
    ):
        args = parser.parse_args(argv)
        assert callable(args.func), argv


def test_a_missing_subcommand_is_an_error() -> None:
    with pytest.raises(SystemExit):
        build_parser().parse_args([])


# -- containers --------------------------------------------------------------
def test_containers_list_prints_the_raw_response(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    StubRestClient.outcomes = [ok(b'{"containers":[{"containerId":"c1"}]}')]
    assert main(["-c", str(config_path), "containers", "list"]) == 0
    out = capsys.readouterr().out
    # Printed verbatim: the operator needs the container id BMW actually sent.
    assert '"containerId":"c1"' in out
    assert "costs 1" in out  # quota cost is stated up front
    assert StubRestClient.calls[0][0] == "list_containers"


def test_containers_create_passes_the_descriptors(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    StubRestClient.outcomes = [ok(b'{"containerId":"new-1"}')]
    exit_code = main(
        [
            "-c",
            str(config_path),
            "containers",
            "create",
            "--name",
            "archive",
            "--descriptors",
            "vehicle.drivetrain.odometer, vehicle.body.doors.lock ",
        ]
    )
    assert exit_code == 0
    _name, _args, kwargs = StubRestClient.calls[0]
    # Whitespace trimmed, order preserved.
    assert kwargs["technical_descriptors"] == [
        "vehicle.drivetrain.odometer",
        "vehicle.body.doors.lock",
    ]
    assert kwargs["name"] == "archive"
    assert "new-1" in capsys.readouterr().out


def test_containers_create_rejects_an_empty_descriptor_list(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = main(
        ["-c", str(config_path), "containers", "create", "--name", "x", "--descriptors", " , "]
    )
    assert exit_code == 2
    assert "at least one technical descriptor" in capsys.readouterr().err
    # No request issued, so no quota spent on a mistake.
    assert StubRestClient.calls == []


def test_containers_reports_a_rejected_request(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    StubRestClient.outcomes = [CarDataError("HTTP 403 (CU-002)", status=403)]
    assert main(["-c", str(config_path), "containers", "list"]) == 1
    assert "request failed" in capsys.readouterr().err


# -- snapshot ----------------------------------------------------------------
def test_snapshot_archives_the_response(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    StubRestClient.outcomes = [ok(b'{"model":"i4"}')]
    assert main(["-c", str(config_path), "snapshot", "basic_data"]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["committed"] == 1
    assert report["used_today"] == 1

    from bmw_collector.store import EventStore

    config_text = config_path.read_text()
    db = next(
        line.split(":", 1)[1].strip() for line in config_text.splitlines() if "sqlite_path" in line
    )
    store = EventStore(db)
    store.open_sync()
    try:
        event = next(iter(store.iter_events_sync()))
        assert event.payload == b'{"model":"i4"}'
        assert event.metadata["task"] == "basic_data"
        assert event.source == "rest"
    finally:
        store.close_sync()


def test_snapshot_requires_a_container_id_for_telematic_data(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["-c", str(config_path), "snapshot", "telematic_snapshot"]) == 2
    assert "--container-id is required" in capsys.readouterr().err
    assert StubRestClient.calls == []


def test_snapshot_passes_the_container_id(config_path: Path) -> None:
    StubRestClient.outcomes = [ok(b"{}")]
    exit_code = main(
        [
            "-c",
            str(config_path),
            "snapshot",
            "telematic_snapshot",
            "--container-id",
            "container-7",
        ]
    )
    assert exit_code == 0
    assert StubRestClient.calls[0][1] == (VALID_VIN, "container-7")


def test_snapshot_reports_quota_exhaustion(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    StubRestClient.outcomes = [QuotaExhaustedError("quota", status=403, error_id="CU-429")]
    assert main(["-c", str(config_path), "snapshot", "basic_data"]) == 1
    assert "snapshot failed" in capsys.readouterr().err


def test_snapshot_of_mappings_updates_the_registry(config_path: Path) -> None:
    payload = json.dumps([{"vin": VALID_VIN, "mappingType": "PRIMARY"}]).encode()
    StubRestClient.outcomes = [ok(payload)]
    assert main(["-c", str(config_path), "snapshot", "vehicle_mapping"]) == 0
    assert StubRestClient.calls[0][0] == "get_vehicle_mappings"


# -- vehicles list with refresh ---------------------------------------------
def test_vehicles_list_refresh_archives_the_mapping_response(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    payload = json.dumps(
        [{"vin": VALID_VIN, "mappingType": "PRIMARY", "mappedSince": "2026-01-01T00:00:00Z"}]
    ).encode()
    StubRestClient.outcomes = [ok(payload)]
    assert main(["-c", str(config_path), "vehicles", "list", "--refresh"]) == 0
    out = capsys.readouterr().out
    assert "costs 1" in out
    listed = json.loads(out[out.index("{") :])
    assert listed["mapped"][0]["vin"] == VALID_VIN
    assert listed["mapped"][0]["primary"] is True
    assert VALID_VIN in listed["active"]


def test_vehicles_list_refresh_reports_a_failure(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    StubRestClient.outcomes = [CarDataError("HTTP 401", status=401)]
    assert main(["-c", str(config_path), "vehicles", "list", "--refresh"]) == 1
    assert "could not refresh mappings" in capsys.readouterr().err


def test_enabling_a_vin_outside_the_allowlist_warns(
    config_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["-c", str(config_path), "vehicles", "enable", "WBA00000000000009"]) == 0
    captured = capsys.readouterr()
    # Enabling a VIN the config does not list would otherwise look like it worked.
    assert "not in bmw.vehicles.vins" in captured.err
    assert "restart the collector" in captured.out


# -- clean-up contracts ------------------------------------------------------
def test_commands_close_the_bmw_clients(config_path: Path) -> None:
    StubRestClient.outcomes = [ok(b"{}")]
    main(["-c", str(config_path), "containers", "list"])
    # A leaked aiohttp session produces warnings and holds sockets open.
    assert StubAuthManager.instances[0].closed is True
