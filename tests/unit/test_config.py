"""Configuration expansion and validation."""

from __future__ import annotations

from pathlib import Path

import pytest

from bmw_collector.config import ConfigError, expand, load_config, parse_config
from tests.conftest import VALID_VIN, write_config


def test_expands_required_env_var() -> None:
    assert expand({"id": "${BMW_CLIENT_ID}"}, env={"BMW_CLIENT_ID": "abc"}) == {"id": "abc"}


def test_missing_required_env_var_is_fatal_and_names_the_variable() -> None:
    with pytest.raises(ConfigError, match="BMW_CLIENT_ID"):
        expand({"id": "${BMW_CLIENT_ID}"}, env={})


def test_default_value_is_used_when_env_var_is_absent() -> None:
    assert expand("${PORT:-9090}", env={}) == 9090


def test_whole_scalar_reference_keeps_its_type() -> None:
    # A bare "${X}" must not turn a boolean or an int into a string.
    assert expand("${FLAG}", env={"FLAG": "true"}) is True
    assert expand("${N}", env={"N": "42"}) == 42
    assert expand("prefix-${N}", env={"N": "42"}) == "prefix-42"


def test_file_reference_reads_and_strips_the_file(tmp_path: Path) -> None:
    secret = tmp_path / "client_id"
    secret.write_text("  from-file\n", encoding="utf-8")
    assert expand(f"${{file:{secret}}}", env={}) == "from-file"


def test_file_reference_failure_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="cannot read secret file"):
        expand(f"${{file:{tmp_path / 'absent'}}}", env={})


def test_valid_config_round_trips(tmp_path: Path) -> None:
    config = load_config(write_config(tmp_path))
    assert config.node.name == "test-node"
    assert config.bmw.vehicles.vins == (VALID_VIN,)
    assert config.storage.synchronous == "FULL"
    assert config.bmw.rest.tasks[0].type == "vehicle_mapping"


def test_empty_allowlist_refuses_to_start_rather_than_collecting_everything(
    tmp_path: Path,
) -> None:
    with pytest.raises(ConfigError, match="nothing would be collected"):
        load_config(write_config(tmp_path, vins=[]))


def test_empty_allowlist_is_allowed_when_nothing_is_enabled(tmp_path: Path) -> None:
    config = load_config(
        write_config(tmp_path, vins=[], mqtt_enabled=False, rest_enabled=False, tasks="")
    )
    assert config.bmw.vehicles.vins == ()


def test_wildcard_subscribe_requires_all_primary() -> None:
    # A wildcard topic receives every mapped VIN, so pairing it with an
    # allowlist would quietly archive cars the operator did not list.
    with pytest.raises(ConfigError, match="requires bmw.vehicles.mode 'all_primary'"):
        parse_config(
            {
                "bmw": {
                    "client_id": "x",
                    "vehicles": {"mode": "allowlist", "vins": [VALID_VIN]},
                    "mqtt": {"subscribe_mode": "wildcard"},
                    "rest": {"enabled": False},
                }
            }
        )


def test_wildcard_subscribe_is_accepted_with_all_primary() -> None:
    config = parse_config(
        {
            "bmw": {
                "client_id": "x",
                "vehicles": {"mode": "all_primary"},
                "mqtt": {"subscribe_mode": "wildcard"},
                "rest": {"enabled": False},
            }
        }
    )
    assert config.bmw.mqtt.subscribe_mode == "wildcard"


def test_malformed_vin_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="not a 17-character VIN"):
        load_config(write_config(tmp_path, vins=["TOOSHORT"]))


def test_vin_with_ambiguous_letters_is_rejected(tmp_path: Path) -> None:
    # I, O and Q are not valid VIN characters.
    with pytest.raises(ConfigError, match="not a 17-character VIN"):
        load_config(write_config(tmp_path, vins=["WBAIOQ45678901234"]))


def test_duplicate_vins_are_collapsed(tmp_path: Path) -> None:
    config = load_config(write_config(tmp_path, vins=[VALID_VIN, VALID_VIN.lower()]))
    assert config.bmw.vehicles.vins == (VALID_VIN,)


def test_unknown_key_is_rejected_rather_than_silently_ignored() -> None:
    with pytest.raises(ConfigError, match="unknown key"):
        parse_config(
            {
                "bmw": {
                    "client_id": "x",
                    "vehicles": {"vins": [VALID_VIN]},
                    "mqtt": {"enabled": False},
                    "rest": {"enabled": False},
                },
                "storage": {"typo_here": 1},
            }
        )


def test_telematic_snapshot_requires_a_container_id(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="telematic_snapshot requires container_id"):
        load_config(
            write_config(
                tmp_path,
                tasks="      - type: telematic_snapshot\n        interval_hours: 4\n",
            )
        )


def test_unknown_task_type_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="is not one of"):
        load_config(
            write_config(tmp_path, tasks="      - type: nonsense\n        interval_hours: 4\n")
        )


def test_duplicate_tasks_are_rejected(tmp_path: Path) -> None:
    tasks = (
        "      - type: basic_data\n        interval_hours: 4\n"
        "      - type: basic_data\n        interval_hours: 8\n"
    )
    with pytest.raises(ConfigError, match="duplicate task"):
        load_config(write_config(tmp_path, tasks=tasks))


def test_rest_enabled_without_tasks_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="no tasks are configured"):
        load_config(write_config(tmp_path, tasks=""))


def test_missing_client_id_explains_where_to_get_one() -> None:
    with pytest.raises(ConfigError, match="BMW portal"):
        parse_config({"bmw": {}})


def test_budget_above_the_documented_limit_warns_but_starts(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level("WARNING"):
        config = load_config(write_config(tmp_path, budget=80))
    assert config.bmw.rest.requests_per_day_budget == 80
    assert "documented BMW limit of 50" in caplog.text


def test_invalid_synchronous_mode_is_rejected() -> None:
    with pytest.raises(ConfigError, match="storage.synchronous"):
        parse_config(
            {
                "bmw": {"client_id": "x", "mqtt": {"enabled": False}, "rest": {"enabled": False}},
                "storage": {"synchronous": "SOMETIMES"},
            }
        )


def test_reconnect_window_must_be_ordered() -> None:
    with pytest.raises(ConfigError, match="0 < min <= max"):
        parse_config(
            {
                "bmw": {
                    "client_id": "x",
                    "vehicles": {"vins": [VALID_VIN]},
                    "mqtt": {"reconnect_min_seconds": 100, "reconnect_max_seconds": 10},
                    "rest": {"enabled": False},
                }
            }
        )


def test_lock_path_defaults_next_to_the_database() -> None:
    config = parse_config(
        {
            "bmw": {
                "client_id": "x",
                "vehicles": {"vins": [VALID_VIN]},
                "mqtt": {"enabled": False},
                "rest": {"enabled": False},
            },
            "storage": {"sqlite_path": "/data/db.sqlite3"},
        }
    )
    assert str(config.storage.resolved_lock_path()) == "/data/db.lock"
