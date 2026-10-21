"""End-to-end CLI flows, driven as a real subprocess.

These exercise the packaged console script the container runs, with no BMW
credentials anywhere.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

VIN = "WBA12345678901234"


def write_config(tmp_path: Path, *, port: int = 19401) -> Path:
    """A valid configuration pointing entirely inside tmp_path."""
    path = tmp_path / "config.yaml"
    path.write_text(
        f"""
node:
  name: cli-test
storage:
  sqlite_path: {tmp_path / "collector.sqlite3"}
  synchronous: FULL
  queue_max: 1000
bmw:
  client_id: ${{BMW_CLIENT_ID}}
  credentials_file: {tmp_path / "secrets" / "oauth.json"}
  account_key: cli
  vehicles:
    mode: allowlist
    vins:
      - {VIN}
  mqtt:
    enabled: true
  rest:
    enabled: true
    requests_per_day_budget: 35
    tasks:
      - type: vehicle_mapping
        interval_hours: 24
telemetry:
  listen_host: 127.0.0.1
  port: {port}
  log_level: INFO
""",
        encoding="utf-8",
    )
    return path


def run(config: Path, *args: str, expect: int = 0) -> subprocess.CompletedProcess[str]:
    """Invoke the CLI as a subprocess."""
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "bmw_collector", "-c", str(config), *args],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "BMW_CLIENT_ID": "00000000-0000-0000-0000-000000000000",
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    assert result.returncode == expect, f"stdout={result.stdout}\nstderr={result.stderr}"
    return result


def test_version_and_help_work_without_a_config() -> None:
    for args in (["--version"], ["--help"]):
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "bmw_collector", *args],
            capture_output=True,
            text=True,
            timeout=60,
            check=False,
        )
        assert result.returncode == 0, result.stderr


def test_a_missing_env_reference_fails_with_an_actionable_message(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "bmw_collector", "-c", str(config), "verify"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    assert result.returncode == 2
    assert "BMW_CLIENT_ID" in result.stderr


def test_simulate_then_verify_then_export_and_import(tmp_path: Path) -> None:
    config = write_config(tmp_path)

    simulated = run(config, "simulate", "--count", "40", "--plain-logs")
    assert json.loads(simulated.stdout)["committed"] == 40

    verified = json.loads(run(config, "verify").stdout)
    assert verified["integrity_check"] == "ok"
    assert verified["events"]["total"] == 40
    assert verified["schema_version"] == 2

    exported = tmp_path / "events.jsonl"
    run(config, "export", "-o", str(exported))
    assert len(exported.read_text().splitlines()) == 40

    # Re-import into the same database: idempotent by event UUID.
    reimported = json.loads(run(config, "import", "-i", str(exported)).stdout)
    assert reimported == {"inserted": 0, "skipped_existing": 40}
    assert json.loads(run(config, "verify").stdout)["events"]["total"] == 40


def test_export_to_stdout_is_pipeable(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    run(config, "simulate", "--count", "3")
    result = run(config, "export", "--limit", "2")
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) == 2
    assert all("payload_b64" in json.loads(line) for line in lines)


def test_import_into_a_second_database_replays_the_archive(tmp_path: Path) -> None:
    source_config = write_config(tmp_path)
    run(source_config, "simulate", "--count", "15")
    exported = tmp_path / "events.jsonl"
    run(source_config, "export", "-o", str(exported))

    replica_dir = tmp_path / "replica"
    replica_dir.mkdir()
    replica_config = write_config(replica_dir, port=19402)
    imported = json.loads(run(replica_config, "import", "-i", str(exported)).stdout)
    assert imported == {"inserted": 15, "skipped_existing": 0}
    assert json.loads(run(replica_config, "verify").stdout)["events"]["total"] == 15


def test_backup_produces_a_verifiable_database(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    run(config, "simulate", "--count", "10")
    backup = tmp_path / "backups" / "snap.sqlite3"
    run(config, "backup", "-o", str(backup))
    assert backup.exists()

    restored_dir = tmp_path / "restored"
    restored_dir.mkdir(exist_ok=True)
    restored_config = write_config(restored_dir, port=19403)
    # Restoring is a plain file copy: the snapshot is self-contained, with no
    # -wal sidecar to carry alongside it.
    (restored_dir / "collector.sqlite3").write_bytes(backup.read_bytes())
    verified = json.loads(run(restored_config, "verify").stdout)
    assert verified["integrity_check"] == "ok"
    assert verified["events"]["total"] == 10


def test_auth_status_without_a_credential_says_what_to_do(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    result = run(config, "auth", "status", expect=1)
    payload = json.loads(result.stdout)
    assert payload["credential_present"] is False
    assert "auth login" in payload["next_step"]


def test_auth_status_reports_a_stored_credential_without_revealing_it(
    tmp_path: Path,
) -> None:
    config = write_config(tmp_path)
    secrets = tmp_path / "secrets"
    secrets.mkdir(parents=True, exist_ok=True)
    (secrets / "oauth.json").write_text(
        json.dumps(
            {
                "version": 1,
                "refresh_token": "fabricated-refresh-token-value",
                "gcid": "aa111192-d570-4d3d-8835-656ee00edcd7",
                "scope": ["openid", "cardata:api:read"],
            }
        ),
        encoding="utf-8",
    )
    (secrets / "oauth.json").chmod(0o600)
    result = run(config, "auth", "status")
    assert "fabricated-refresh-token-value" not in result.stdout
    payload = json.loads(result.stdout)
    assert payload["credential_present"] is True
    assert payload["credential_mode"] == "0o600"
    assert payload["state"]["gcid_present"] is True


def test_vehicles_list_works_offline(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    payload = json.loads(run(config, "vehicles", "list").stdout)
    assert payload["configured_vins"] == [VIN]
    assert payload["mapped"] == []
    # Honest about what a listing does and does not prove.
    assert "streaming descriptors" in payload["note"]


def test_disable_then_enable_a_vehicle_keeps_history(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    run(config, "simulate", "--count", "5")

    run(config, "vehicles", "disable", VIN)
    assert json.loads(run(config, "verify").stdout)["events"]["total"] == 5
    listed = json.loads(run(config, "vehicles", "list").stdout)
    disabled = next(v for v in listed["known_locally"] if v["vin"] == VIN)
    assert disabled["collection_enabled"] is False
    assert VIN not in listed["active"]

    run(config, "vehicles", "enable", VIN)
    listed = json.loads(run(config, "vehicles", "list").stdout)
    assert VIN in listed["active"]
    assert json.loads(run(config, "verify").stdout)["events"]["total"] == 5


def test_incidents_are_listable(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    run(config, "simulate", "--count", "1")
    assert json.loads(run(config, "incidents").stdout) == []


def test_snapshot_requires_a_container_for_telematic_data(tmp_path: Path) -> None:
    config = write_config(tmp_path)
    result = run(config, "snapshot", "telematic_snapshot", expect=2)
    assert "--container-id is required" in result.stderr


def test_an_invalid_config_is_rejected_before_anything_runs(tmp_path: Path) -> None:
    config = tmp_path / "bad.yaml"
    # Valid YAML, but an empty allowlist would collect nothing at all.
    config.write_text(
        "bmw:\n"
        "  client_id: x\n"
        "  vehicles:\n"
        "    mode: allowlist\n"
        "    vins: []\n"
        "  rest:\n"
        "    enabled: true\n"
        "    tasks:\n"
        "      - type: vehicle_mapping\n"
        "        interval_hours: 24\n",
        encoding="utf-8",
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "bmw_collector", "-c", str(config), "verify"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    assert result.returncode == 2
    assert "nothing would be collected" in result.stderr
    assert not (tmp_path / "collector.sqlite3").exists()


@pytest.mark.parametrize("flag_position", ["before", "after"])
def test_the_config_flag_works_on_either_side_of_the_subcommand(
    tmp_path: Path, flag_position: str
) -> None:
    config = write_config(tmp_path)
    args = (
        ["-c", str(config), "verify"]
        if flag_position == "before"
        else ["verify", "-c", str(config)]
    )
    result = subprocess.run(  # noqa: S603
        [sys.executable, "-m", "bmw_collector", *args],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
        env={
            "PATH": "/usr/bin:/bin",
            "BMW_CLIENT_ID": "x",
            "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
        },
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["integrity_check"] == "ok"


def test_a_second_run_on_the_same_database_is_refused(tmp_path: Path) -> None:
    """The instance lock stops an accidental double start on this host."""
    from bmw_collector.lock import StandaloneLeader

    config = write_config(tmp_path, port=19404)
    holder = StandaloneLeader(tmp_path / "collector.lock")
    holder.acquire_sync()
    try:
        result = subprocess.run(  # noqa: S603
            [sys.executable, "-m", "bmw_collector", "-c", str(config), "run", "--plain-logs"],
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
            env={
                "PATH": "/usr/bin:/bin",
                "BMW_CLIENT_ID": "x",
                "PYTHONPATH": str(Path(__file__).resolve().parents[2] / "src"),
            },
        )
    finally:
        holder.release_sync()
    assert result.returncode == 1
    assert "one streaming connection" in result.stderr
