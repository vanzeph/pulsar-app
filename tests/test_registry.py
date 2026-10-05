"""Plugin registry behaviour."""

from __future__ import annotations

import pytest

from pulsar_app import (
    DuplicatePluginError,
    PluginError,
    PluginKind,
    PluginRegistry,
    PluginSpec,
    UnknownPluginError,
)


def make_spec(plugin_id: str = "akshare", kind: PluginKind = PluginKind.MARKET_DATA) -> PluginSpec:
    return PluginSpec(plugin_id=plugin_id, kind=kind, factory=lambda **kwargs: object())


def test_register_and_get_roundtrip() -> None:
    registry = PluginRegistry()
    spec = make_spec()
    registry.register(spec)
    assert registry.get("akshare", PluginKind.MARKET_DATA) is spec
    assert registry.list_ids(PluginKind.MARKET_DATA) == ["akshare"]
    assert len(registry) == 1
    assert ("akshare", PluginKind.MARKET_DATA) in registry


def test_same_id_allowed_across_kinds() -> None:
    registry = PluginRegistry()
    registry.register(make_spec("dual", PluginKind.MARKET_DATA))
    registry.register(make_spec("dual", PluginKind.EXECUTION))
    assert set(registry.list_ids()) == {"dual"}


def test_duplicate_registration_rejected() -> None:
    registry = PluginRegistry()
    registry.register(make_spec())
    with pytest.raises(DuplicatePluginError):
        registry.register(make_spec())


def test_unknown_plugin_rejected_with_registered_ids_listed() -> None:
    registry = PluginRegistry()
    registry.register(make_spec())
    with pytest.raises(UnknownPluginError, match="akshare"):
        registry.get("tushare", PluginKind.MARKET_DATA)


def test_kind_mismatch_is_unknown() -> None:
    registry = PluginRegistry()
    registry.register(make_spec(kind=PluginKind.MARKET_DATA))
    with pytest.raises(UnknownPluginError):
        registry.get("akshare", PluginKind.EXECUTION)


def test_unregister_and_clear() -> None:
    registry = PluginRegistry()
    registry.register(make_spec())
    registry.unregister("akshare", PluginKind.MARKET_DATA)
    with pytest.raises(UnknownPluginError):
        registry.unregister("akshare", PluginKind.MARKET_DATA)
    registry.register(make_spec())
    registry.clear()
    assert len(registry) == 0


@pytest.mark.parametrize("bad_id", ["Akshare", "1abc", "with-dash", "", "a" * 65, "with space"])
def test_invalid_plugin_ids_rejected(bad_id: str) -> None:
    with pytest.raises(PluginError):
        make_spec(plugin_id=bad_id)


def test_non_callable_factory_rejected() -> None:
    with pytest.raises(PluginError, match="callable"):
        PluginSpec(plugin_id="akshare", kind=PluginKind.MARKET_DATA, factory="not-callable")  # type: ignore[arg-type]


def test_default_registry_is_empty() -> None:
    # pulsar-app ships no plugins; adapters register themselves on import.
    from pulsar_app import DEFAULT_REGISTRY

    assert len(DEFAULT_REGISTRY) == 0
