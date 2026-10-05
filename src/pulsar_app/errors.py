"""Error types raised deliberately by the pulsar-app runtime assembly layer.

Every failure mode of configuration loading, plugin resolution and run
assembly maps onto one of these exceptions so callers (the CLI included)
can turn them into stable exit codes instead of tracebacks.
"""

from __future__ import annotations

__all__ = [
    "PulsarAppError",
    "ConfigError",
    "PluginError",
    "UnknownPluginError",
    "DuplicatePluginError",
    "PortConformanceError",
    "LiveModeLockedError",
]


class PulsarAppError(Exception):
    """Base class for every error raised deliberately by pulsar-app."""


class ConfigError(PulsarAppError):
    """A run configuration is missing, malformed or violates a baseline rule."""


class PluginError(PulsarAppError):
    """A plugin could not be resolved or failed to instantiate."""


class UnknownPluginError(PluginError):
    """The configuration references a plugin id the registry does not know."""


class DuplicatePluginError(PluginError):
    """A plugin id was registered twice for the same port kind."""


class PortConformanceError(PluginError):
    """A factory result does not satisfy the port protocol it declared."""


class LiveModeLockedError(PulsarAppError):
    """Live mode stayed locked: the unlock environment variable was absent or not confirmed."""
