"""Write-time content validation: what may enter the store, checked first.

The store is an Agent-writable surface, so every write is validated
*before* anything is persisted (校验失败必须拒绝写入并给可读错误).
Three checks exist, one per namespace kind:

``code`` objects (Python modules that register factors / modelers):
    1. *syntax* — the source must compile;
    2. *import whitelist* — an AST scan over every import statement: the
       root module must be on the whitelist and must not be on the hard
       deny list (process / filesystem / network / interpreter surface:
       ``os``, ``subprocess``, ``socket``, ``urllib``, …). The whitelist
       defaults to :data:`DEFAULT_IMPORT_WHITELIST` (numpy, pandas, math,
       typing, … plus the public ``pulsar_core`` / ``pulsar_contracts``
       APIs) and is configured in exactly two places: this constant, or
       the ``PULSAR_STORE_IMPORT_WHITELIST`` environment variable
       (comma-separated, *replaces* the default — the hard deny list can
       never be overridden). Dynamic execution escapes
       (``__import__``/``eval``/``exec``/``compile`` calls) are rejected
       by the same scan;
    3. *registration-name conflicts* — handled by
       :mod:`pulsar_app.store.loader` (it needs the registry diff an
       execution produces; see that module).

``experiments`` objects (experiment TOML):
    TOML syntax, the credential baseline of :mod:`pulsar_app.config`
    (no plaintext credentials anywhere) and the C6 shape of the
    ``[experiment]`` section (non-empty ``id``, ``status`` one of
    candidate/active/retired). The full layered-config validation —
    factor names resolving against the registries included — runs at
    *assembly* time via pulsar-core, because an experiment legitimately
    references custom factors that are separate store objects.

``lake`` / ``runs`` namespaces carry no object content at all — they are
path references and never pass through this module.
"""

from __future__ import annotations

import ast
import os
import tomllib
from typing import Final, Mapping

from ..errors import StoreValidationError

__all__ = [
    "DEFAULT_IMPORT_WHITELIST",
    "HARD_DENIED_IMPORTS",
    "DENIED_DYNAMIC_CALLS",
    "WHITELIST_ENV_VAR",
    "EXPERIMENT_STATUSES",
    "resolve_import_whitelist",
    "check_code_source",
    "check_experiment_document",
]

#: Where the import whitelist can be overridden without code changes.
WHITELIST_ENV_VAR: Final = "PULSAR_STORE_IMPORT_WHITELIST"

#: The default import whitelist: compute + typing + the pulsar public API.
#: Configuration point for site policy: extend *this* set (or the env var)
#: — never the hard deny list.
DEFAULT_IMPORT_WHITELIST: Final[frozenset[str]] = frozenset(
    {
        "__future__",
        # stdlib compute / structure surface
        "math",
        "statistics",
        "cmath",
        "decimal",
        "fractions",
        "random",
        "itertools",
        "functools",
        "operator",
        "collections",
        "dataclasses",
        "enum",
        "typing",
        "datetime",
        "zoneinfo",
        # numeric stack
        "numpy",
        "pandas",
        "scipy",
        # pulsar public API surface
        "pulsar_contracts",
        "pulsar_core",
    }
)

#: Modules that can never be imported by stored code, whatever the
#: whitelist says: the process / filesystem / network / interpreter
#: surface (os.system, subprocess, sockets, urllib, ctypes, …).
HARD_DENIED_IMPORTS: Final[frozenset[str]] = frozenset(
    {
        "os",
        "sys",
        "subprocess",
        "socket",
        "ssl",
        "select",
        "signal",
        "urllib",
        "http",
        "requests",
        "ftplib",
        "smtplib",
        "telnetlib",
        "asyncio",
        "ctypes",
        "cffi",
        "importlib",
        "builtins",
        "__builtin__",
        "pickle",
        "shelve",
        "marshal",
        "shutil",
        "pathlib",
        "pathlib2",
        "io",
        "fileinput",
        "tempfile",
        "glob",
        "multiprocessing",
        "threading",
        "concurrent",
        "subprocess32",
        "pty",
        "posix",
        "nt",
        "winreg",
    }
)

#: Callables whose call sites are rejected outright — they bypass both
#: the compile check (``compile``/``eval``/``exec``) and the import scan
#: (``__import__``).
DENIED_DYNAMIC_CALLS: Final[frozenset[str]] = frozenset(
    {"__import__", "eval", "exec", "compile"}
)

#: C6 lifecycle states an experiment document's ``status`` may carry.
EXPERIMENT_STATUSES: Final[frozenset[str]] = frozenset(
    {"candidate", "active", "retired"}
)


def resolve_import_whitelist(
    override: Mapping[str, str] | None = None,
) -> frozenset[str]:
    """The effective whitelist: env/config override, else the default.

    ``override`` maps environment names to values (tests inject their
    own); the ``PULSAR_STORE_IMPORT_WHITELIST`` variable holds a
    comma-separated module list that *replaces* the default. Hard-denied
    modules are subtracted last so no override can ever widen the
    dangerous surface.
    """
    source = dict(os.environ) if override is None else dict(override)
    raw = source.get(WHITELIST_ENV_VAR, "")
    if raw.strip():
        modules = frozenset(
            piece.strip() for piece in raw.split(",") if piece.strip()
        )
    else:
        modules = DEFAULT_IMPORT_WHITELIST
    return modules - HARD_DENIED_IMPORTS


def check_code_source(source: str, *, origin: str) -> None:
    """Run the static checks over one Python code object's source.

    Syntax compilation first (a SyntaxError becomes a readable
    :class:`StoreValidationError` carrying the compiler's own line
    information), then the import-whitelist AST scan.
    """
    try:
        tree = ast.parse(source, filename=origin, mode="exec")
    except SyntaxError as exc:
        location = f"line {exc.lineno}" if exc.lineno is not None else "unknown line"
        raise StoreValidationError(
            f"{origin}: code object does not compile: {location}: {exc.msg}"
        ) from exc
    _check_imports(tree, origin=origin)
    _check_dynamic_calls(tree, origin=origin)


def _check_imports(tree: ast.Module, *, origin: str) -> None:
    whitelist = resolve_import_whitelist()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                _check_one_import(root, alias.name, whitelist, origin)
        elif isinstance(node, ast.ImportFrom):
            if node.level > 0:
                raise StoreValidationError(
                    f"{origin}: line {node.lineno}: relative imports are not "
                    f"allowed in stored code objects (module '{node.module or ''}' "
                    f"has no package context in the store)"
                )
            if node.module:
                root = node.module.split(".")[0]
                _check_one_import(root, node.module, whitelist, origin)


def _check_one_import(
    root: str, full: str, whitelist: frozenset[str], origin: str
) -> None:
    if root in HARD_DENIED_IMPORTS:
        raise StoreValidationError(
            f"{origin}: import {full!r} is denied: '{root}' is on the hard "
            f"deny list (process / filesystem / network / interpreter "
            f"surface); the whitelist cannot re-enable it"
        )
    if root not in whitelist:
        allowed = ", ".join(sorted(whitelist))
        raise StoreValidationError(
            f"{origin}: import {full!r} is not on the import whitelist; "
            f"allowed modules: {allowed} (configure via {WHITELIST_ENV_VAR} "
            f"or DEFAULT_IMPORT_WHITELIST in pulsar_app.store.validation)"
        )


def _check_dynamic_calls(tree: ast.Module, *, origin: str) -> None:
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
            if node.func.id in DENIED_DYNAMIC_CALLS:
                raise StoreValidationError(
                    f"{origin}: line {node.lineno}: calling "
                    f"{node.func.id}() is not allowed in stored code "
                    f"objects (dynamic execution bypasses the import "
                    f"whitelist)"
                )


def check_experiment_document(text: str, *, origin: str) -> None:
    """Validate one experiment TOML document's store-admissible shape.

    Checks TOML syntax, the credential baseline (via
    :func:`pulsar_app.config.scan_for_plaintext_credentials`) and the C6
    ``[experiment]`` shape (non-empty ``id``, ``status`` in the lifecycle
    set). Full layered-config validation — factors resolving against the
    registries included — is assembly's job (pulsar-core
    ``parse_experiment``), because experiments may reference custom code
    objects stored separately.
    """
    try:
        tree = tomllib.loads(text)
    except tomllib.TOMLDecodeError as exc:
        raise StoreValidationError(f"{origin}: invalid TOML: {exc}") from exc

    from ..config import scan_for_plaintext_credentials  # local: avoid cycle at import
    from ..errors import ConfigError

    try:
        scan_for_plaintext_credentials(tree, where=origin)
    except ConfigError as exc:
        raise StoreValidationError(str(exc)) from exc

    experiment = tree.get("experiment")
    if not isinstance(experiment, Mapping):
        raise StoreValidationError(
            f"{origin}: experiment objects require an [experiment] section"
        )
    experiment_id = experiment.get("id")
    if not isinstance(experiment_id, str) or not experiment_id.strip():
        raise StoreValidationError(
            f"{origin}: [experiment] id must be a non-empty string"
        )
    status = experiment.get("status")
    if not isinstance(status, str) or status not in EXPERIMENT_STATUSES:
        allowed = ", ".join(sorted(EXPERIMENT_STATUSES))
        raise StoreValidationError(
            f"{origin}: [experiment] status must be one of {allowed} "
            f"(C6 lifecycle); got {status!r}"
        )
