"""Runtime assembly: turn a validated RunConfig into live port instances.

The assembler is the single place that knows every plugin: it resolves the
configured data sources in primary/backup order, resolves the execution
venue, enforces the live-mode gate, verifies that factory results satisfy
the port protocols from ``pulsar-contracts`` and stamps the run identity.

It contains no domain logic and no port implementations — those live in
``pulsar-core`` / ``pulsar-data`` / ``pulsar-exec`` respectively.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping, Protocol, runtime_checkable

from pulsar_contracts import ExecutionPort, MarketDataPort

from .config import MODE_TO_VENUE, RunConfig, RunMode, resolve_env_refs
from .errors import ConfigError, LiveModeLockedError, PluginError, PortConformanceError
from .manifest import StoreObjectRecord, config_fingerprint, new_run_id
from .registry import DEFAULT_REGISTRY, PluginKind, PluginRegistry

__all__ = [
    "AssembledRun",
    "SupportsWatermark",
    "assemble_run",
    "collect_data_watermarks",
    "assemble_store_objects",
]


#: Values accepted as explicit confirmation of the live unlock variable.
_UNLOCK_TRUTHY = frozenset({"1", "true", "yes", "on"})

#: Parameter name the assembler injects into every market-data factory.
RESERVED_LAKE_DIR = "lake_dir"


@dataclass(frozen=True, slots=True)
class AssembledRun:
    """Everything one run needs after assembly, before any domain logic runs."""

    run_id: str
    mode: RunMode
    seed: int
    config: RunConfig
    config_fingerprint: str
    market_data: MarketDataPort
    backup_market_data: tuple[MarketDataPort, ...]
    execution: ExecutionPort
    store_objects: tuple[StoreObjectRecord, ...] = ()

    def all_market_data(self) -> tuple[MarketDataPort, ...]:
        """Data ports in configured primary/backup order."""
        return (self.market_data, *self.backup_market_data)


@runtime_checkable
class SupportsWatermark(Protocol):
    """Optional protocol data ports implement to report their lake watermarks.

    Returns dataset name -> as-of :class:`~datetime.date`. Watermarks are
    recorded in the RunManifest; ports without this protocol simply
    contribute none.
    """

    def watermark(self) -> dict[str, date]: ...


def assemble_run(
    config: RunConfig,
    registry: PluginRegistry = DEFAULT_REGISTRY,
    *,
    env: Mapping[str, str] | None = None,
) -> AssembledRun:
    """Assemble one run: resolve plugins, gate live mode, verify port conformance.

    ``env`` defaults to ``os.environ`` and is the only place credentials are
    ever read from — referenced by name from the configuration.
    """
    environment = os.environ if env is None else env

    mode = config.run.mode
    if mode is None:
        raise ConfigError("run.mode is not set; pass it in [run] or via the CLI subcommand")
    expected_venue = MODE_TO_VENUE[mode]
    if config.exec.venue != expected_venue:
        raise ConfigError(
            f"run mode {mode.value!r} pairs with venue {expected_venue.value!r}, "
            f"but [exec] venue is {config.exec.venue.value!r}"
        )

    _check_live_gate(config, mode, environment)

    ports = [
        _build_market_data(source_id, config, registry, environment)
        for source_id in config.data.sources
    ]
    execution = _build_execution(config, registry, environment)
    store_objects = assemble_store_objects(config)

    seed = config.run.seed
    fingerprint = config_fingerprint(config.model_dump(mode="json"), seed)
    return AssembledRun(
        run_id=new_run_id(mode.value, fingerprint),
        mode=mode,
        seed=seed,
        config=config,
        config_fingerprint=fingerprint,
        market_data=ports[0],
        backup_market_data=tuple(ports[1:]),
        execution=execution,
        store_objects=store_objects,
    )


def assemble_store_objects(config: RunConfig) -> tuple[StoreObjectRecord, ...]:
    """Materialize the configured store code and pin object references.

    The 自定义代码组装 seam: every name in ``[store].code`` is loaded
    from the store root (hash-verified, statically re-checked) and
    registered into the pulsar-core registries *before* the experiment
    layer resolves any names; ``[store].experiments`` entries are pinned
    by content hash so the RunManifest can reproduce the exact config
    bytes. A run without a ``[store]`` section assembles nothing extra.
    """
    section = config.store
    if not section.code and not section.experiments:
        return ()
    from .store.engine import Store
    from .store.loader import materialize_code

    store = Store(section.root)
    records: list[StoreObjectRecord] = []
    for name in section.code:
        materialized = materialize_code(store, name)
        head = store.resolve("code", name)
        records.append(
            StoreObjectRecord(
                role="code",
                namespace="code",
                name=name,
                content_hash=materialized.content_hash,
                seq=head.seq,
                note=head.note,
            )
        )
    for name in section.experiments:
        head = store.resolve("experiments", name)
        store.get("experiments", name, content_hash=head.content_hash)
        records.append(
            StoreObjectRecord(
                role="experiment",
                namespace="experiments",
                name=name,
                content_hash=head.content_hash,
                seq=head.seq,
                note=head.note,
            )
        )
    return tuple(records)


def _check_live_gate(config: RunConfig, mode: RunMode, env: Mapping[str, str]) -> None:
    """Enforce the live-mode gate: locked unless explicitly unlocked (baseline)."""
    if mode is not RunMode.LIVE:
        return
    params = config.miqmt_params()
    if params is None:
        raise ConfigError(
            "live mode requires the [exec.miniqmt] section with 'unlock_env' "
            "and 'max_order_value_cny'"
        )
    value = env.get(params.unlock_env)
    if value is None:
        raise LiveModeLockedError(
            f"live mode is locked: set environment variable {params.unlock_env!r} "
            f"to unlock (value one of 1/true/yes/on)"
        )
    if value.strip().lower() not in _UNLOCK_TRUTHY:
        raise LiveModeLockedError(
            f"live mode remains locked: {params.unlock_env!r} must be one of "
            f"1/true/yes/on (got {value!r})"
        )


def _build_market_data(
    source_id: str,
    config: RunConfig,
    registry: PluginRegistry,
    env: Mapping[str, str],
) -> MarketDataPort:
    spec = registry.get(source_id, PluginKind.MARKET_DATA)
    params = resolve_env_refs(config.data.source_params.get(source_id, {}), env)
    if RESERVED_LAKE_DIR in params:
        raise ConfigError(
            f"[data.{source_id}] must not set {RESERVED_LAKE_DIR!r}; it is reserved "
            f"and injected from [data] itself"
        )
    kwargs: dict[str, Any] = dict(params)
    kwargs[RESERVED_LAKE_DIR] = config.data.lake_dir
    port = _instantiate(spec.plugin_id, spec.factory, kwargs)
    if not isinstance(port, MarketDataPort):
        raise PortConformanceError(
            f"market-data plugin {source_id!r} produced an object that does not "
            f"satisfy the MarketDataPort protocol"
        )
    return port


def _build_execution(
    config: RunConfig,
    registry: PluginRegistry,
    env: Mapping[str, str],
) -> ExecutionPort:
    venue_id = config.exec.venue.value
    spec = registry.get(venue_id, PluginKind.EXECUTION)
    table = config.exec.venue_params.get(venue_id, {})
    params = resolve_env_refs(table, env)
    port = _instantiate(spec.plugin_id, spec.factory, dict(params))
    if not isinstance(port, ExecutionPort):
        raise PortConformanceError(
            f"execution plugin {venue_id!r} produced an object that does not "
            f"satisfy the ExecutionPort protocol"
        )
    return port


def _instantiate(plugin_id: str, factory: Any, kwargs: dict[str, Any]) -> Any:
    try:
        return factory(**kwargs)
    except TypeError as exc:
        raise PluginError(
            f"plugin {plugin_id!r} factory rejected its parameters: {exc}; "
            f"check the parameter tables in the configuration"
        ) from exc


def collect_data_watermarks(assembled: AssembledRun) -> dict[str, dict[str, str]]:
    """Collect watermarks from every assembled data port that reports them."""
    watermarks: dict[str, dict[str, str]] = {}
    for source_id, port in zip(assembled.config.data.sources, assembled.all_market_data(), strict=True):
        if isinstance(port, SupportsWatermark):
            watermarks[source_id] = _normalize_watermarks(source_id, port.watermark())
    return watermarks


def _normalize_watermarks(source_id: str, raw: Any) -> dict[str, str]:
    """Normalize a watermark report to ``{dataset: ISO date}`` strings."""

    def normalize(value: Any, where: str) -> str:
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        if isinstance(value, str):
            return value
        raise PluginError(
            f"watermark of data source {source_id!r} carries an unsupported value "
            f"at {where}: {type(value).__name__}"
        )

    if isinstance(raw, Mapping):
        return {str(name): normalize(value, str(name)) for name, value in raw.items()}
    if isinstance(raw, (datetime, date)):
        return {"default": normalize(raw, "default")}
    raise PluginError(
        f"watermark() of data source {source_id!r} must return a mapping of "
        f"dataset names to dates (got {type(raw).__name__})"
    )
