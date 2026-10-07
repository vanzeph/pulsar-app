"""Custom-code assembly: materialize stored code and register it in core.

This is the 自定义代码组装 half of the store contract (core-engine
design): pulsar-app reads a user code object from the unified store,
verifies it, executes it in a controlled namespace and lets it register
its factors / modelers into the pulsar-core registries — the same
registries experiments resolve names in at assembly time.

pulsar-core is imported *lazily* on purpose: pulsar-app's base install
(pydantic + pulsar-contracts) can host experiments/lake/runs objects and
path references without the domain stack; code objects and assembly need
``pulsar-core`` (the ``e2e`` extra / the release lock file installs it).

Two execution flavours share one helper:

* :func:`preview_registration` — the put-time *preview*. The module runs
  once against the real registries; the registry diff (which names it
  added, per kind) is captured and **rolled back** via
  ``Registry.discard`` (the public hook STORE1 added to pulsar-core), so
  validating a candidate never leaves process state behind. The captured
  names are persisted as the object's *declared names*; put-time
  conflict checks compare them against built-ins and against every other
  code object's declared names — no re-execution needed later.
* :func:`materialize_code` — the assembly-time *load*. Same execution,
  registrations kept, idempotent per content hash (the same bytes
  materialize exactly once per process; different bytes under the same
  store name is a conflict, not a silent replacement).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Mapping

from ..errors import StoreValidationError
from .catalog import DeclaredName
from .engine import Store
from .validation import check_code_source

__all__ = [
    "MaterializedCode",
    "CORE_INSTALL_HINT",
    "core_registries",
    "preview_registration",
    "materialize_code",
    "load_experiment_object",
    "ensure_not_materialized",
    "reset_materialization_state",
]

#: Readable pointer installed when pulsar-core is absent.
CORE_INSTALL_HINT = "custom code assembly requires pulsar-core (pip install pulsar-app[e2e])"

#: In-process memo of already-materialized code objects: store name ->
#: (content hash, declared names). Keeps repeat assemblies idempotent
#: without re-executing modules (their registrations are still live in
#: the registries), and powers :func:`reset_materialization_state`'s
#: clean teardown.
_materialized: dict[str, tuple[str, tuple[DeclaredName, ...]]] = {}


def core_registries() -> dict[str, Any]:
    """The pulsar-core registries custom code may register into.

    Ordered by the research pipeline so conflict messages read naturally.
    """
    try:
        from pulsar_core import (
            FACTOR_REGISTRY,
            MODEL_REGISTRY,
            PORTFOLIO_REGISTRY,
            PREPROCESS_REGISTRY,
            UNIVERSE_REGISTRY,
        )
    except ImportError as exc:  # pragma: no cover - depends on install
        raise StoreValidationError(f"{CORE_INSTALL_HINT} (import failed: {exc})") from exc
    return {
        "factor": FACTOR_REGISTRY,
        "model": MODEL_REGISTRY,
        "preprocess": PREPROCESS_REGISTRY,
        "portfolio": PORTFOLIO_REGISTRY,
        "universe": UNIVERSE_REGISTRY,
    }


def _require_core() -> Any:
    try:
        import pulsar_core  # noqa: F401 - presence check only
    except ImportError as exc:
        raise StoreValidationError(f"{CORE_INSTALL_HINT} (import failed: {exc})") from exc
    return pulsar_core


@dataclass(frozen=True, slots=True)
class MaterializedCode:
    """One code object loaded from the store, registered and ready."""

    name: str
    content_hash: str
    declared: tuple[DeclaredName, ...]
    module_namespace: Mapping[str, Any]


def _execute(source: str, *, module_name: str) -> dict[str, Any]:
    """Exec one module's source in a fresh controlled namespace."""
    namespace: dict[str, Any] = {"__name__": module_name, "__doc__": None}
    exec(compile(source, module_name, "exec"), namespace)  # noqa: S102 - the sanctioned materialization seam
    return namespace


def _registry_snapshot(registries: Mapping[str, Any]) -> dict[str, frozenset[str]]:
    return {kind: frozenset(reg.names()) for kind, reg in registries.items()}


def _rollback(
    registries: Mapping[str, Any], before: Mapping[str, frozenset[str]]
) -> None:
    for kind, registry in registries.items():
        for name in sorted(set(registry.names()) - before[kind]):
            registry.discard(name)


def preview_registration(source: str, *, name: str) -> tuple[DeclaredName, ...]:
    """Execute a candidate module once, capture what it registers, undo.

    Returns the declared names (sorted by kind, then name). Any failure —
    module-level exception, duplicate registration against a live
    registry, a module that registers nothing — becomes a readable
    :class:`StoreValidationError`; partial registrations are rolled back
    before the error leaves this function.
    """
    _require_core()
    registries = core_registries()
    before = _registry_snapshot(registries)
    origin = f"store://code/{name}"
    check_code_source(source, origin=origin)
    try:
        _execute(source, module_name=f"pulsar_store_preview_{name}")
    except Exception as exc:
        _rollback(registries, before)
        raise StoreValidationError(
            f"{origin}: module raised {type(exc).__name__} while executing: {exc}"
        ) from exc
    declared = [
        DeclaredName(kind, registered)
        for kind, registry in registries.items()
        for registered in sorted(set(registry.names()) - before[kind])
    ]
    _rollback(registries, before)
    if not declared:
        raise StoreValidationError(
            f"{origin}: module registered nothing; a code object must "
            f"register at least one factor / model / preprocess / "
            f"portfolio / universe name"
        )
    return tuple(declared)


def materialize_code(
    store: Store,
    name: str,
    *,
    content_hash: str | None = None,
) -> MaterializedCode:
    """Load one code object from ``store`` and register it in core.

    Reads the head version (or the pinned ``content_hash`` — the
    reproduce-by-hash path), re-verifies the bytes against the hash and
    the source against the static checks, then executes the module once
    per process. Registrations persist: this is the assembly seam.
    """
    _require_core()
    payload = store.get("code", name, content_hash=content_hash)
    source = payload.decode("utf-8")
    resolved_hash = content_hash or store.resolve("code", name).content_hash
    origin = f"store://code/{name}@{resolved_hash[:12]}"
    check_code_source(source, origin=origin)

    already = _materialized.get(name)
    if already is not None and already[0] == resolved_hash:
        declared = store.declared_names("code", name, content_hash=resolved_hash)
        return MaterializedCode(
            name=name,
            content_hash=resolved_hash,
            declared=tuple(declared),
            module_namespace={},
        )
    if already is not None:
        raise StoreValidationError(
            f"{origin}: store name {name!r} was already materialized with a "
            f"different content hash ({already[0][:12]}); refusing to swap "
            f"registered code mid-process — restart or use a new store name"
        )

    registries = core_registries()
    before = _registry_snapshot(registries)
    try:
        namespace = _execute(source, module_name=f"pulsar_store_code_{name}")
    except Exception as exc:
        _rollback(registries, before)  # a failed load leaves no registrations behind
        raise StoreValidationError(
            f"{origin}: module raised {type(exc).__name__} while executing: {exc}"
        ) from exc
    declared = tuple(
        DeclaredName(kind, registered)
        for kind, registry in registries.items()
        for registered in sorted(set(registry.names()) - before[kind])
    )
    if not declared:
        raise StoreValidationError(
            f"{origin}: module registered nothing; a code object must "
            f"register at least one factor / model / preprocess / "
            f"portfolio / universe name"
        )
    _materialized[name] = (resolved_hash, declared)
    return MaterializedCode(
        name=name,
        content_hash=resolved_hash,
        declared=declared,
        module_namespace=namespace,
    )


def load_experiment_object(
    store: Store, name: str, *, content_hash: str | None = None
) -> Any:
    """Load one experiments-namespace object as a core ``ExperimentConfig``.

    Full layered-config validation (factor names resolving against the
    registries included) happens here, not at put time — an experiment
    may reference custom factors stored as separate code objects, so the
    check only makes sense once those are materialized.
    """
    _require_core()
    import tomllib

    from pulsar_core import parse_experiment

    payload = store.get("experiments", name, content_hash=content_hash)
    resolved_hash = content_hash or store.resolve("experiments", name).content_hash
    try:
        tree = tomllib.loads(payload.decode("utf-8"))
    except tomllib.TOMLDecodeError as exc:  # pragma: no cover - put validated syntax
        raise StoreValidationError(
            f"store://experiments/{name}@{resolved_hash[:12]}: invalid TOML: {exc}"
        ) from exc
    return parse_experiment(
        tree, source_path=f"store://experiments/{name}@{resolved_hash}"
    )


def ensure_not_materialized(name: str) -> None:
    """Refuse writes to a code name that is live in this process.

    A materialized object's registrations sit in the pulsar-core
    registries; validating a *new* version of the same name would have to
    collide with them. Instead of silently swapping registered code, the
    put is refused with a readable message — Agent writes normally come
    from a separate process (the CLI), where nothing is materialized.
    """
    entry = _materialized.get(name)
    if entry is not None:
        raise StoreValidationError(
            f"code/{name} is materialized in this process with content hash "
            f"{entry[0][:12]}; a new version cannot be validated against its "
            f"live registrations — write from a fresh process (the CLI does) "
            f"or use a new store name"
        )


def reset_materialization_state(*, unregister: bool = True) -> None:
    """Forget the in-process materialization memo (test isolation).

    With ``unregister`` (the default) the registrations each materialized
    object made are first discarded from the pulsar-core registries, so
    the process state returns to pre-materialization — the same
    discipline the put-time preview applies to itself.
    """
    if unregister and _materialized:
        registries = core_registries()
        for _, declared in _materialized.values():
            for item in declared:
                registry = registries.get(item.registry_kind)
                if registry is not None and item.registered_name in registry:
                    registry.discard(item.registered_name)
    _materialized.clear()


#: Kept for readability at call sites that want the callable type.
MaterializeFn = Callable[..., MaterializedCode]


def lookup_code_hash(store: Store, name: str) -> str:
    """Head content hash of one code object (assembly bookkeeping)."""
    return store.resolve("code", name).content_hash
