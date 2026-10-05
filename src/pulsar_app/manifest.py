"""RunManifest: the reproducibility archive of one run.

A run is *config + assembly + archive*: after every run, a manifest records
the exact configuration snapshot, the resolved plugin ids, the data
watermarks observed at assembly time, the code versions in play and the
seed — everything needed to reason about reproducibility later.

The manifest contains no secrets: the configuration snapshot keeps
credential environment-variable *references* (``"${ENV_NAME}"``) exactly as
written, never resolved values.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "SCHEMA_VERSION",
    "ResolvedPlugins",
    "RunManifest",
    "collect_code_versions",
    "config_fingerprint",
    "new_run_id",
]

#: Version of the manifest schema; bumped on breaking changes.
SCHEMA_VERSION: Literal["1"] = "1"


class ResolvedPlugins(BaseModel):
    """The plugin ids a run actually assembled, in primary/backup order."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    sources: tuple[str, ...] = Field(min_length=1)
    venue: str


class RunManifest(BaseModel):
    """Complete archive of one run (written next to the run artifacts)."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["1"] = SCHEMA_VERSION
    run_id: str
    created_at: str  # ISO-8601 UTC
    mode: str
    seed: int
    config_fingerprint: str
    config_snapshot: dict[str, Any]
    resolved_plugins: ResolvedPlugins
    data_watermarks: dict[str, dict[str, str]] = Field(default_factory=dict)
    code_versions: dict[str, str] = Field(default_factory=dict)

    def canonical(self) -> str:
        """Canonical JSON form (sorted keys, compact) — the identity used for diffing."""
        return json.dumps(
            self.model_dump(mode="json"), sort_keys=True, ensure_ascii=False, separators=(",", ":")
        )

    def write(self, runs_dir: str | Path) -> Path:
        """Write the manifest to ``<runs_dir>/<run_id>/manifest.json`` atomically."""
        run_dir = Path(runs_dir) / self.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        target = run_dir / "manifest.json"
        tmp = run_dir / ".manifest.json.tmp"
        tmp.write_text(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, ensure_ascii=False, indent=2)
            + "\n",
            encoding="utf-8",
        )
        os.replace(tmp, target)
        return target

    @classmethod
    def read(cls, path: str | Path) -> "RunManifest":
        """Load a manifest previously written by :meth:`write`."""
        return cls.model_validate_json(Path(path).read_text(encoding="utf-8"))


def collect_code_versions() -> dict[str, str]:
    """Collect the versions that define this run's behaviour.

    Records the pulsar packages present in the environment, the Python
    version and — when the caller exports ``PULSAR_GIT_COMMIT`` — the
    source commit of the entry repository. No network or git subprocess.
    """
    versions = {"python": sys.version.split()[0]}
    for distribution in ("pulsar-app", "pulsar-contracts"):
        try:
            versions[distribution] = version(distribution)
        except PackageNotFoundError:  # pragma: no cover - source checkout
            versions[distribution] = "unknown"
    commit = os.environ.get("PULSAR_GIT_COMMIT")
    if commit:
        versions["pulsar-app-git-commit"] = commit
    return versions


def config_fingerprint(snapshot: dict[str, Any], seed: int) -> str:
    """SHA-256 over the canonical configuration snapshot plus the seed.

    Two runs with the same fingerprint assembled the same plugin graph over
    the same configuration — the identity reproducibility is judged against.
    """
    payload = json.dumps(
        {"config": snapshot, "seed": seed}, sort_keys=True, ensure_ascii=False, separators=(",", ":")
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def new_run_id(mode: str, fingerprint: str) -> str:
    """Readable, collision-resistant run id: mode + UTC stamp + fingerprint head."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"run-{mode}-{stamp}-{fingerprint[:8]}"
