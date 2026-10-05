"""pulsar-app: runtime assembly and configuration for the Pulsar system.

One run = configuration + assembly + archive. This package owns the TOML
run-configuration schema and validation, the plugin registry, the assembler
that resolves configured plugins into port instances, the RunManifest
archive and the research/paper/live CLI skeleton.

It deliberately contains no domain logic and no port implementations; the
port protocols and domain objects come from ``pulsar-contracts``, plugin
implementations live in their own repositories (``pulsar-data``,
``pulsar-exec``) and register themselves here.
"""

from importlib.metadata import PackageNotFoundError, version

from .assembly import AssembledRun, SupportsWatermark, assemble_run, collect_data_watermarks
from .config import (
    MODE_TO_VENUE,
    DataSection,
    ExecSection,
    MiqmtVenueParams,
    RunConfig,
    RunMode,
    RunSection,
    Venue,
    load_config,
    parse_toml_config,
)
from .errors import (
    ConfigError,
    DuplicatePluginError,
    LiveModeLockedError,
    PluginError,
    PortConformanceError,
    PulsarAppError,
    UnknownPluginError,
)
from .manifest import ResolvedPlugins, RunManifest, collect_code_versions, config_fingerprint
from .registry import DEFAULT_REGISTRY, PluginKind, PluginRegistry, PluginSpec
from .run import RunOutcome, execute_run

try:
    __version__ = version("pulsar-app")
except PackageNotFoundError:  # pragma: no cover - source checkout without install
    __version__ = "0.0.0.dev0"

__all__ = [
    "__version__",
    # config
    "MODE_TO_VENUE",
    "DataSection",
    "ExecSection",
    "MiqmtVenueParams",
    "RunConfig",
    "RunMode",
    "RunSection",
    "Venue",
    "load_config",
    "parse_toml_config",
    # registry
    "DEFAULT_REGISTRY",
    "PluginKind",
    "PluginRegistry",
    "PluginSpec",
    # assembly / run
    "AssembledRun",
    "SupportsWatermark",
    "assemble_run",
    "collect_data_watermarks",
    "RunOutcome",
    "execute_run",
    # manifest
    "ResolvedPlugins",
    "RunManifest",
    "collect_code_versions",
    "config_fingerprint",
    # errors
    "PulsarAppError",
    "ConfigError",
    "PluginError",
    "UnknownPluginError",
    "DuplicatePluginError",
    "PortConformanceError",
    "LiveModeLockedError",
]
