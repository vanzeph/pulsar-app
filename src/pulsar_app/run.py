"""One-run lifecycle skeleton: assemble, verify, archive.

``execute_run`` is the seam the core engine will later plug into. Today it
performs the assembly-only dry run mandated by the architecture baseline:
resolve plugins from the configuration, enforce the live gate, probe data
watermarks and write the RunManifest. No domain logic and no port behaviour
beyond the assembly probes live here.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, NamedTuple

from .assembly import assemble_run, collect_data_watermarks
from .config import RunConfig
from .manifest import ResolvedPlugins, RunManifest, collect_code_versions, config_fingerprint
from .registry import DEFAULT_REGISTRY, PluginRegistry

__all__ = [
    "RunOutcome",
    "execute_run",
]


class RunOutcome(NamedTuple):
    """Result of one executed dry run: the manifest and where it was written."""

    manifest: RunManifest
    manifest_path: Path


def execute_run(
    config: RunConfig,
    registry: PluginRegistry = DEFAULT_REGISTRY,
    *,
    runs_dir: str | Path,
    env: Mapping[str, str] | None = None,
) -> RunOutcome:
    """Assemble ``config``, archive a RunManifest under ``runs_dir`` and return it."""
    environment = os.environ if env is None else env
    assembled = assemble_run(config, registry, env=environment)

    snapshot = config.model_dump(mode="json")
    fingerprint = config_fingerprint(snapshot, assembled.seed)
    if fingerprint != assembled.config_fingerprint:  # pragma: no cover - invariant
        raise AssertionError("config fingerprint changed between assembly and archiving")

    manifest = RunManifest(
        run_id=assembled.run_id,
        created_at=datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        mode=assembled.mode.value,
        seed=assembled.seed,
        config_fingerprint=fingerprint,
        config_snapshot=snapshot,
        resolved_plugins=ResolvedPlugins(
            sources=tuple(config.data.sources), venue=config.exec.venue.value
        ),
        data_watermarks=collect_data_watermarks(assembled),
        code_versions=collect_code_versions(),
    )
    path = manifest.write(runs_dir)
    return RunOutcome(manifest, path)
