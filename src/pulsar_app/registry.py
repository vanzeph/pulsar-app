"""Plugin registry: adapters declare identifiers, the assembler resolves them.

Every adapter (market-data source or execution venue) is a plugin living in
its implementation repository (``pulsar-data`` / ``pulsar-exec``). It
registers itself under a stable lowercase identifier — the same identifier
a run configuration references::

    registry.register(PluginSpec(
        plugin_id="akshare",
        kind=PluginKind.MARKET_DATA,
        factory=lambda **params: AkshareAdapter(**params),
    ))

``pulsar-app`` itself ships no plugins: it is the assembly point, not an
implementation. Tests (and downstream repos) populate the registry; the
default global registry exists so the CLI can resolve whatever the process
has registered.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Callable

from .errors import DuplicatePluginError, PluginError, UnknownPluginError

__all__ = [
    "PluginKind",
    "PluginSpec",
    "PluginRegistry",
    "DEFAULT_REGISTRY",
]


class PluginKind(StrEnum):
    """Which port a plugin implements."""

    MARKET_DATA = "market_data"
    EXECUTION = "execution"


_PLUGIN_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")


@dataclass(frozen=True, slots=True)
class PluginSpec:
    """One registered plugin: identifier, port kind and factory.

    The factory is called by the assembler with the plugin's resolved
    parameters (credential environment-variable references already
    substituted) plus, for market-data plugins, the shared ``lake_dir``.
    """

    plugin_id: str
    kind: PluginKind
    factory: Callable[..., Any]
    description: str = ""

    def __post_init__(self) -> None:
        if not _PLUGIN_ID_PATTERN.fullmatch(self.plugin_id):
            raise PluginError(
                f"invalid plugin id {self.plugin_id!r}: must match [a-z][a-z0-9_]{{0,63}}"
            )
        if not callable(self.factory):
            raise PluginError(f"plugin {self.plugin_id!r}: factory must be callable")


class PluginRegistry:
    """In-process registry mapping ``(plugin_id, kind)`` to a :class:`PluginSpec`."""

    def __init__(self) -> None:
        self._specs: dict[tuple[str, PluginKind], PluginSpec] = {}

    def register(self, spec: PluginSpec) -> None:
        """Register ``spec``; a duplicate id for the same kind is rejected."""
        key = (spec.plugin_id, spec.kind)
        if key in self._specs:
            raise DuplicatePluginError(
                f"plugin id {spec.plugin_id!r} is already registered for kind "
                f"{spec.kind.value!r}"
            )
        self._specs[key] = spec

    def unregister(self, plugin_id: str, kind: PluginKind) -> None:
        """Remove a registration (mainly useful for test isolation)."""
        key = (plugin_id, kind)
        if key not in self._specs:
            raise UnknownPluginError(
                f"plugin id {plugin_id!r} is not registered for kind {kind.value!r}"
            )
        del self._specs[key]

    def get(self, plugin_id: str, kind: PluginKind) -> PluginSpec:
        """Look up a plugin; raises :class:`UnknownPluginError` when absent."""
        key = (plugin_id, kind)
        spec = self._specs.get(key)
        if spec is None:
            raise UnknownPluginError(
                f"unknown {kind.value} plugin id {plugin_id!r}; "
                f"registered ids: {self.list_ids(kind) or '<none>'}"
            )
        return spec

    def list_ids(self, kind: PluginKind | None = None) -> list[str]:
        """Sorted ids of registered plugins, optionally filtered by kind."""
        return sorted(
            plugin_id
            for plugin_id, plugin_kind in self._specs
            if kind is None or plugin_kind == kind
        )

    def clear(self) -> None:
        """Remove every registration (test helper)."""
        self._specs.clear()

    def __len__(self) -> int:
        return len(self._specs)

    def __contains__(self, key: tuple[str, PluginKind]) -> bool:
        return key in self._specs


#: Process-wide default registry used by the CLI. Empty by design — plugins
#: register themselves when their packages are imported.
DEFAULT_REGISTRY = PluginRegistry()
