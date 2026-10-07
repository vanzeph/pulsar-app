"""The unified workspace store (task STORE1): versioned objects under one
root, an Agent-writable interface (CLI + Python API) and the
custom-code assembly seam into pulsar-core's registries.

Four namespaces share one root (``experiments`` / ``code`` / ``lake`` /
``runs``); content-addressed objects (SHA-256) with append-only history
and rollback live in the first two, path references to the existing lake
and runs directories in the last two. Nothing here imports pulsar-core
at module level — the base pulsar-app install (pulsar-contracts only)
can host experiment objects and references; code preview / materialize /
experiment loading import it lazily.

Python surface (the CLI wraps exactly this)::

    from pulsar_app.store import Store

    store = Store("./store")
    version, created = store.put("code", "my_factors", source_text)
    store.put("experiments", "drill_momentum", toml_text)
    store.attach("lake", "default", "./data/lake")
    payload = store.get("code", "my_factors", content_hash=version.content_hash)

    from pulsar_app.store import materialize_code, load_experiment_object
    materialize_code(store, "my_factors")          # registers into pulsar-core
    experiment = load_experiment_object(store, "drill_momentum")
"""

from ..errors import (
    StoreBackendError,
    StoreError,
    StoreNotFoundError,
    StoreTamperedError,
    StoreValidationError,
)
from .backend import (
    CosStorageBackend,
    LocalDiskBackend,
    R2StorageBackend,
    StorageBackend,
)
from .catalog import (
    AttachmentRecord,
    DeclaredName,
    EntrySummary,
    StoreCatalog,
    VersionRecord,
)
from .engine import Namespace, Store, content_hash
from .validation import (
    DEFAULT_IMPORT_WHITELIST,
    DENIED_DYNAMIC_CALLS,
    EXPERIMENT_STATUSES,
    HARD_DENIED_IMPORTS,
    WHITELIST_ENV_VAR,
    check_code_source,
    check_experiment_document,
    resolve_import_whitelist,
)

__all__ = [
    # errors
    "StoreError",
    "StoreValidationError",
    "StoreNotFoundError",
    "StoreTamperedError",
    "StoreBackendError",
    # namespaces / engine
    "Namespace",
    "Store",
    "content_hash",
    # catalog records
    "StoreCatalog",
    "VersionRecord",
    "EntrySummary",
    "AttachmentRecord",
    "DeclaredName",
    # backends
    "StorageBackend",
    "LocalDiskBackend",
    "R2StorageBackend",
    "CosStorageBackend",
    # validation
    "WHITELIST_ENV_VAR",
    "DEFAULT_IMPORT_WHITELIST",
    "HARD_DENIED_IMPORTS",
    "DENIED_DYNAMIC_CALLS",
    "EXPERIMENT_STATUSES",
    "resolve_import_whitelist",
    "check_code_source",
    "check_experiment_document",
]


def __getattr__(name: str) -> object:
    """Lazily expose the pulsar-core-dependent loader surface.

    ``materialize_code`` / ``preview_registration`` /
    ``load_experiment_object`` need pulsar-core at call time (never at
    import time), so they resolve through this hook to keep
    ``import pulsar_app.store`` free of the domain stack.
    """
    if name in {
        "MaterializedCode",
        "CORE_INSTALL_HINT",
        "materialize_code",
        "preview_registration",
        "load_experiment_object",
        "core_registries",
    }:
        from . import loader

        return getattr(loader, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
