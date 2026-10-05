"""RunManifest: fingerprint stability, archiving, code-version capture."""

from __future__ import annotations

import json
import re
from pathlib import Path

from pulsar_app import (
    RunConfig,
    RunManifest,
    collect_code_versions,
    config_fingerprint,
    execute_run,
    load_config,
)

from .conftest import EXAMPLE_CONFIG, registry_with_mocks

RUN_ID_PATTERN = re.compile(r"^run-(research|paper|live)-\d{8}T\d{6,14}Z-[0-9a-f]{8}$")


def test_fingerprint_is_stable_and_seed_sensitive(example_config: RunConfig) -> None:
    snapshot = example_config.model_dump(mode="json")
    first = config_fingerprint(snapshot, 0)
    assert first == config_fingerprint(snapshot, 0)
    assert first != config_fingerprint(snapshot, 1)


def test_execute_run_writes_a_roundtrippable_manifest(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_CONFIG)
    outcome = execute_run(config, registry_with_mocks(), runs_dir=tmp_path)

    manifest = outcome.manifest
    assert RUN_ID_PATTERN.fullmatch(manifest.run_id), manifest.run_id
    assert manifest.schema_version == "1"
    assert manifest.created_at.endswith("Z")
    assert manifest.canonical()  # canonical JSON form is producible

    written = json.loads(outcome.manifest_path.read_text(encoding="utf-8"))
    assert written["run_id"] == manifest.run_id
    assert written["config_snapshot"]["run"]["mode"] == "research"
    assert RunManifest.read(outcome.manifest_path) == manifest


def test_execute_run_leaves_no_temporary_files(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_CONFIG)
    outcome = execute_run(config, registry_with_mocks(), runs_dir=tmp_path)
    run_dir = outcome.manifest_path.parent
    assert sorted(p.name for p in run_dir.iterdir()) == ["manifest.json"]


def test_manifest_snapshot_keeps_env_references_unresolved(tmp_path: Path) -> None:
    config = load_config(EXAMPLE_CONFIG)
    tweaked = config.model_copy(
        update={
            "data": config.data.model_copy(
                update={
                    "source_params": {
                        "akshare": {"token": "${PULSAR_TEST_TOKEN}"},
                    }
                }
            )
        }
    )
    outcome = execute_run(
        tweaked, registry_with_mocks(), runs_dir=tmp_path, env={"PULSAR_TEST_TOKEN": "s3cr3t"}
    )
    blob = outcome.manifest_path.read_text(encoding="utf-8")
    assert "${PULSAR_TEST_TOKEN}" in blob
    assert "s3cr3t" not in blob


def test_collect_code_versions() -> None:
    versions = collect_code_versions()
    assert versions["python"]
    assert versions["pulsar-contracts"]
    assert versions["pulsar-app"]
