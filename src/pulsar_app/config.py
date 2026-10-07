"""Run configuration: TOML schema, parsing and validation.

This module implements the configuration layer of the Pulsar architecture
baseline ("plugin registration and runtime assembly"). One run is fully
described by a small TOML document::

    [run]
    mode = "research"                 # research | paper | live

    [data]
    sources = ["akshare", "baostock"] # primary/backup order
    lake_dir = "./data/lake"

    [exec]
    venue = "backtest"                # backtest | paper | miniqmt

    [exec.miqmt]                      # live venue only; locked by default
    unlock_env = "PULSAR_LIVE_CONFIRM"
    max_order_value_cny = 50000.0

Security baseline enforced here:

* credential-like parameters (``token``, ``password``, ``api_key``, ...)
  must reference an environment variable name, written ``"${ENV_NAME}"``;
  a literal value is rejected at parse time;
* no string anywhere in the configuration may embed userinfo credentials
  (``scheme://user:pass@host``);
* the live venue carries ``unlock_env`` (the *name* of the environment
  variable that unlocks live trading) plus an order-value cap — names and
  caps only, never secrets.
"""

from __future__ import annotations

import re
import tomllib
from enum import StrEnum
from pathlib import Path
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .errors import ConfigError

__all__ = [
    "RunMode",
    "Venue",
    "MODE_TO_VENUE",
    "MiqmtVenueParams",
    "RunSection",
    "DataSection",
    "ExecSection",
    "StoreSection",
    "RunConfig",
    "ENV_REF_TEMPLATE",
    "parse_toml_config",
    "load_config",
    "resolve_env_refs",
    "scan_for_plaintext_credentials",
    "validate_plugin_params",
]


class _Frozen(BaseModel):
    """Shared model style: immutable, unknown fields rejected."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class RunMode(StrEnum):
    """The three Pulsar run modes sharing one core (architecture baseline)."""

    RESEARCH = "research"
    PAPER = "paper"
    LIVE = "live"


class Venue(StrEnum):
    """Execution venue plugin ids known to the baseline."""

    BACKTEST = "backtest"
    PAPER = "paper"
    MINIQMT = "miniqmt"


#: Documented pairing of run mode and execution venue (architecture baseline).
MODE_TO_VENUE: dict[RunMode, Venue] = {
    RunMode.RESEARCH: Venue.BACKTEST,
    RunMode.PAPER: Venue.PAPER,
    RunMode.LIVE: Venue.MINIQMT,
}

#: Canonical form of an environment-variable reference inside plugin params.
ENV_REF_TEMPLATE = "${%s}"

_ENV_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ENV_REF_PATTERN = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")
_CREDENTIAL_KEY_PATTERN = re.compile(
    r"(?:password|passwd|pass|token|secret|api[_-]?key|access[_-]?key|credential)",
    re.IGNORECASE,
)
_EMBEDDED_CREDENTIAL_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://[^\s/@]+:[^\s/@]+@")


class MiqmtVenueParams(_Frozen):
    """Parameters of the live venue section ``[exec.miqmt]``.

    ``unlock_env`` names the environment variable that must be explicitly
    confirmed before live assembly; the caps bound single-order value and
    (optionally) daily turnover. None of these fields may carry a secret.
    """

    unlock_env: str
    max_order_value_cny: float = Field(gt=0)
    max_daily_turnover_cny: float | None = Field(default=None, gt=0)

    @field_validator("unlock_env")
    @classmethod
    def _unlock_env_is_a_name(cls, value: str) -> str:
        if not _ENV_NAME_PATTERN.fullmatch(value):
            raise ValueError(
                f"unlock_env must be an environment variable name "
                f"(letters, digits, underscore; got {value!r})"
            )
        return value


class RunSection(_Frozen):
    """The ``[run]`` section: mode, optional label and seed."""

    mode: RunMode | None = None
    name: str = ""
    seed: int = 0


class DataSection(_Frozen):
    """The ``[data]`` section: ordered source ids, lake dir, per-source params."""

    sources: tuple[str, ...] = Field(min_length=1)
    lake_dir: str = "./data/lake"
    source_params: dict[str, dict[str, Any]] = Field(default_factory=dict)

    @field_validator("sources")
    @classmethod
    def _sources_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("data sources must be unique (primary/backup order matters)")
        return value


class StoreSection(_Frozen):
    """The ``[store]`` section: unified-store objects this run assembles from.

    ``root`` is the storage root (four-layer workspace layout: git repos
    / store root / lake / runs). ``code`` names code objects materialized
    and registered into the pulsar-core registries before assembly;
    ``experiments`` names experiment objects whose content hashes are
    pinned into the RunManifest (reproduce-by-hash). Names refer to the
    *head* version at assembly time; the manifest records the resolved
    hashes.
    """

    root: str = "./store"
    code: tuple[str, ...] = Field(default_factory=tuple)
    experiments: tuple[str, ...] = Field(default_factory=tuple)

    @field_validator("code", "experiments")
    @classmethod
    def _store_names_unique(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(set(value)) != len(value):
            raise ValueError("store object names must be unique within their list")
        return value


class ExecSection(_Frozen):
    """The ``[exec]`` section: venue id plus optional per-venue parameter tables."""

    venue: Venue
    venue_params: dict[str, dict[str, Any]] = Field(default_factory=dict)


class RunConfig(_Frozen):
    """A fully validated run configuration (one run = config + assembly + manifest)."""

    run: RunSection = Field(default_factory=RunSection)
    data: DataSection
    exec: ExecSection
    store: StoreSection = Field(default_factory=StoreSection)

    def with_mode(self, mode: RunMode) -> "RunConfig":
        """Return a copy with ``run.mode`` set to ``mode``.

        The CLI subcommands call this to inject their mode; a configuration
        that already declares a *different* mode is rejected.
        """
        if self.run.mode is not None and self.run.mode is not mode:
            raise ConfigError(
                f"configuration declares run.mode={self.run.mode.value!r} "
                f"but the {mode.value!r} subcommand was invoked"
            )
        return self.model_copy(update={"run": self.run.model_copy(update={"mode": mode})})

    def miqmt_params(self) -> MiqmtVenueParams | None:
        """Return the parsed ``[exec.miqmt]`` table, if present."""
        table = self.exec.venue_params.get(Venue.MINIQMT.value)
        if table is None:
            return None
        try:
            return MiqmtVenueParams(**table)
        except ValidationError as exc:
            raise ConfigError(f"invalid [exec.{Venue.MINIQMT.value}] section: {_summary(exc)}") from exc


def _summary(exc: ValidationError) -> str:
    """One-line human summary of a pydantic validation error."""
    parts = []
    for error in exc.errors(include_url=False):
        location = ".".join(str(piece) for piece in error["loc"]) or "<root>"
        parts.append(f"{location}: {error['msg']}")
    return "; ".join(parts)


def scan_for_plaintext_credentials(tree: Mapping[str, Any], *, where: str) -> None:
    """Walk any value tree and reject plaintext credentials.

    * any credential-like key must hold an environment-variable reference
      ``"${ENV_NAME}"`` — literal values are rejected;
    * no string may embed userinfo credentials in a URL.

    Unlike :func:`validate_plugin_params` this imposes no type whitelist,
    so it can scan arbitrary documents (e.g. every TOML file of a
    repository) for the credential baseline alone.
    """
    for key, value in tree.items():
        path = f"{where}.{key}"
        if _CREDENTIAL_KEY_PATTERN.search(str(key)):
            if not isinstance(value, str) or not _ENV_REF_PATTERN.fullmatch(value):
                raise ConfigError(
                    f"{path}: credential-like parameter must reference an environment "
                    f'variable (write it as "${{ENV_NAME}}"); '
                    f"plaintext credentials are forbidden"
                )
        if isinstance(value, Mapping):
            scan_for_plaintext_credentials(value, where=path)
        elif isinstance(value, str):
            _check_string_value(path, value)
        elif isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                if isinstance(item, str):
                    _check_string_value(f"{path}[{index}]", item)


def validate_plugin_params(tree: Mapping[str, Any], *, where: str) -> None:
    """Validate one plugin parameter tree against the security baseline.

    * values are limited to scalars (str/int/float/bool), lists of scalars
      and nested tables — the shapes a plugin factory can consume safely;
    * the credential baseline of :func:`scan_for_plaintext_credentials`
      applies in full.
    """
    scan_for_plaintext_credentials(tree, where=where)
    for key, value in tree.items():
        path = f"{where}.{key}"
        if isinstance(value, Mapping):
            validate_plugin_params(value, where=path)
            continue
        if isinstance(value, (bool, int, float, str)):
            continue
        if isinstance(value, (list, tuple)):
            for index, item in enumerate(value):
                if not isinstance(item, (bool, int, float, str)):
                    raise ConfigError(
                        f"{path}[{index}]: unsupported parameter value type "
                        f"{type(item).__name__}; only scalars are allowed in lists"
                    )
            continue
        raise ConfigError(
            f"{path}: unsupported parameter value type {type(value).__name__}; "
            f"use scalars, lists of scalars or nested tables"
        )


def _check_string_value(path: str, value: Any) -> None:
    if isinstance(value, str) and _EMBEDDED_CREDENTIAL_PATTERN.match(value):
        raise ConfigError(
            f"{path}: URL carries embedded userinfo credentials; "
            f"pass them via an environment-variable reference instead"
        )


def resolve_env_refs(tree: Mapping[str, Any], env: Mapping[str, str]) -> dict[str, Any]:
    """Resolve every ``"${ENV_NAME}"`` reference against ``env``.

    Referenced-but-unset variables raise :class:`ConfigError` at assembly
    time — a run never starts with silently empty credentials. Resolution
    results go to plugin factories only; the configuration snapshot stored
    in the RunManifest keeps the unresolved references.
    """
    resolved: dict[str, Any] = {}
    for key, value in tree.items():
        if isinstance(value, Mapping):
            resolved[key] = resolve_env_refs(value, env)
        elif isinstance(value, (list, tuple)):
            resolved[key] = [_resolve_string(item, env) if isinstance(item, str) else item for item in value]
        elif isinstance(value, str):
            resolved[key] = _resolve_string(value, env)
        else:
            resolved[key] = value
    return resolved


def _resolve_string(value: str, env: Mapping[str, str]) -> str:
    match = _ENV_REF_PATTERN.fullmatch(value)
    if match is None:
        return value
    name = match.group(1)
    if name not in env:
        raise ConfigError(
            f"environment variable '{name}' referenced by the configuration is not set"
        )
    return env[name]


_ROOT_SECTIONS = ("run", "data", "exec", "store")
_DATA_SCALAR_KEYS = {"sources", "lake_dir"}
_STORE_SCALAR_KEYS = {"root"}

#: Venue-parameter section names allowed under ``[exec]``. ``miqmt`` is the
#: short form used by the architecture baseline's assembly example and is an
#: alias of the ``miniqmt`` venue id.
_VENUE_SECTION_TO_ID = {
    "backtest": Venue.BACKTEST.value,
    "paper": Venue.PAPER.value,
    "miniqmt": Venue.MINIQMT.value,
    "miqmt": Venue.MINIQMT.value,
}


def parse_toml_config(text: str, *, origin: str = "<string>") -> RunConfig:
    """Parse and validate a run configuration from TOML text.

    Raises :class:`ConfigError` with the ``origin`` prefix on any malformed
    input, unknown section/key, mode/venue mismatch or plaintext credential.
    """
    try:
        raw = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{origin}: invalid TOML: {exc}") from exc

    unknown_roots = sorted(set(raw) - set(_ROOT_SECTIONS))
    if unknown_roots:
        raise ConfigError(f"{origin}: unknown top-level section(s): {unknown_roots}")

    try:
        run_section = RunSection(**raw.get("run", {}))
    except ValidationError as exc:
        raise ConfigError(f"{origin}: invalid [run] section: {_summary(exc)}") from exc

    data_section = _parse_data_section(raw, origin)
    exec_section = _parse_exec_section(raw, origin)
    store_section = _parse_store_section(raw, origin)

    config = RunConfig(
        run=run_section, data=data_section, exec=exec_section, store=store_section
    )
    _check_mode_venue_pairing(config, origin)
    return config


def _parse_store_section(raw: Mapping[str, Any], origin: str) -> StoreSection:
    """Parse the optional ``[store]`` section (unified-store assembly)."""
    section = raw.get("store")
    if section is None:
        return StoreSection()
    if not isinstance(section, Mapping):
        raise ConfigError(f"{origin}: [store] must be a table of store settings")
    try:
        return StoreSection(**section)
    except ValidationError as exc:
        raise ConfigError(f"{origin}: invalid [store] section: {_summary(exc)}") from exc


def _parse_data_section(raw: Mapping[str, Any], origin: str) -> DataSection:
    section = raw.get("data")
    if not isinstance(section, Mapping):
        raise ConfigError(f"{origin}: missing required [data] section")
    sources_value = section.get("sources")
    if sources_value is None:
        raise ConfigError(f"{origin}: [data] requires 'sources' (ordered plugin id list)")
    if not isinstance(sources_value, list) or not all(
        isinstance(item, str) for item in sources_value
    ):
        raise ConfigError(f"{origin}: [data] sources must be a list of plugin id strings")
    lake_dir_value = section.get("lake_dir", "./data/lake")
    if not isinstance(lake_dir_value, str):
        raise ConfigError(f"{origin}: [data] lake_dir must be a string")
    source_params: dict[str, dict[str, Any]] = {}
    for key, value in section.items():
        if key in _DATA_SCALAR_KEYS:
            continue
        if not isinstance(value, Mapping):
            raise ConfigError(
                f"{origin}: [data] key {key!r} must be a table of plugin parameters "
                f"(only 'sources' and 'lake_dir' are scalars here)"
            )
        if "lake_dir" in value:
            raise ConfigError(
                f"{origin}: [data.{key}] must not set 'lake_dir'; "
                f"it is reserved and injected from [data] itself"
            )
        validate_plugin_params(value, where=f"{origin}: [data.{key}]")
        source_params[key] = dict(value)
    try:
        return DataSection(
            sources=tuple(sources_value),
            lake_dir=lake_dir_value,
            source_params=source_params,
        )
    except ValidationError as exc:
        raise ConfigError(f"{origin}: invalid [data] section: {_summary(exc)}") from exc


def _parse_exec_section(raw: Mapping[str, Any], origin: str) -> ExecSection:
    section = raw.get("exec")
    if not isinstance(section, Mapping):
        raise ConfigError(f"{origin}: missing required [exec] section")
    if "venue" not in section:
        raise ConfigError(f"{origin}: [exec] requires a 'venue' (backtest | paper | miniqmt)")
    venue_params: dict[str, dict[str, Any]] = {}
    for key, value in section.items():
        if key == "venue":
            continue
        if not isinstance(value, Mapping):
            raise ConfigError(
                f"{origin}: [exec] key {key!r} must be a table of venue parameters "
                f"('venue' is the only scalar here)"
            )
        venue_id = _VENUE_SECTION_TO_ID.get(key)
        if venue_id is None:
            raise ConfigError(
                f"{origin}: unknown venue table [exec.{key}]; "
                f"expected one of {sorted(_VENUE_SECTION_TO_ID)}"
            )
        if venue_id in venue_params:
            raise ConfigError(
                f"{origin}: [exec.{key}] duplicates venue parameters of "
                f"[exec.{venue_id}]; configure each venue once"
            )
        where = f"{origin}: [exec.{key}]"
        if venue_id == Venue.MINIQMT.value:
            try:
                MiqmtVenueParams(**value)
            except ValidationError as exc:
                raise ConfigError(f"{origin}: invalid [exec.{key}] section: {_summary(exc)}") from exc
        else:
            validate_plugin_params(value, where=where)
        venue_params[venue_id] = dict(value)
    try:
        return ExecSection(venue=section["venue"], venue_params=venue_params)
    except ValidationError as exc:
        raise ConfigError(f"{origin}: invalid [exec] section: {_summary(exc)}") from exc


def _check_mode_venue_pairing(config: RunConfig, origin: str) -> None:
    mode = config.run.mode
    if mode is None:
        return
    expected = MODE_TO_VENUE[mode]
    if config.exec.venue != expected:
        raise ConfigError(
            f"{origin}: run mode {mode.value!r} pairs with venue {expected.value!r}, "
            f"but [exec] venue is {config.exec.venue.value!r}"
        )


def load_config(path: str | Path) -> RunConfig:
    """Load and validate a run configuration from a TOML file."""
    file_path = Path(path)
    if not file_path.is_file():
        raise ConfigError(f"configuration file not found: {file_path}")
    try:
        text = file_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read configuration file {file_path}: {exc}") from exc
    return parse_toml_config(text, origin=str(file_path))
