"""The unified store engine: versioned objects under four namespaces.

One store root holds the model configs, the custom code, references to
the data lake and references to run directories — same root, same
versioning discipline (总体架构设计, 统一工作区存储引擎):

* ``experiments`` / ``code`` are *content-addressed object* namespaces:
  every write stores the bytes under their SHA-256 (deduplicated across
  names and history), the catalog appends ``(namespace, name, seq) →
  hash`` versions, and reads verify the hash — a tampered object file
  can never masquerade as its former self.
* ``lake`` / ``runs`` are *path reference* namespaces: the catalog
  indexes the workspace's existing lake / runs directories by absolute
  path and never copies or migrates them (与既有湖目录兼容不迁移).

History is append-only: a rollback *appends* a version that points at an
older hash, so the audit trail shows both the mistake and the recovery.
Puts are idempotent — offering the current head's exact bytes again is a
no-op that returns the existing version.
"""

from __future__ import annotations

import hashlib
import re
from enum import StrEnum
from pathlib import Path
from typing import Iterable

from ..errors import (
    StoreError,
    StoreNotFoundError,
    StoreTamperedError,
    StoreValidationError,
)
from .backend import LocalDiskBackend, StorageBackend
from .catalog import (
    AttachmentRecord,
    DeclaredName,
    EntrySummary,
    StoreCatalog,
    VersionRecord,
)
from .validation import check_code_source, check_experiment_document

__all__ = [
    "Namespace",
    "Store",
    "content_hash",
]

#: Store names double as module-ish labels in the CLI and manifests.
_NAME_PATTERN = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")


class Namespace(StrEnum):
    """The four namespaces sharing one store root."""

    EXPERIMENTS = "experiments"
    CODE = "code"
    LAKE = "lake"
    RUNS = "runs"

    @property
    def is_object(self) -> bool:
        """Whether the namespace holds content-addressed objects."""
        return self in (Namespace.EXPERIMENTS, Namespace.CODE)

    @property
    def is_reference(self) -> bool:
        """Whether the namespace indexes external paths instead of objects."""
        return self in (Namespace.LAKE, Namespace.RUNS)


def content_hash(payload: bytes) -> str:
    """SHA-256 of the stored bytes — the object's identity."""
    return hashlib.sha256(payload).hexdigest()


#: Internal alias so method parameters named ``content_hash`` cannot
#: shadow the hash function inside this module.
_sha256_of = content_hash


def _coerce_namespace(value: str | Namespace) -> Namespace:
    try:
        return Namespace(value)
    except ValueError:
        known = ", ".join(item.value for item in Namespace)
        raise StoreError(
            f"unknown store namespace {value!r}; expected one of {known}"
        ) from None


def _check_name(name: str) -> None:
    if not _NAME_PATTERN.fullmatch(name):
        raise StoreValidationError(
            f"invalid store name {name!r}: must match [a-z][a-z0-9_.-]{{0,63}}"
        )


class Store:
    """One store root: backend bytes + catalog metadata + validation.

    Constructing a :class:`Store` creates the root directory and
    initializes the catalog schema (idempotent); the local disk backend
    is used unless ``backend`` injects another
    :class:`~pulsar_app.store.backend.StorageBackend` (interface
    placeholder for R2/COS exists; implementations do not).
    """

    def __init__(
        self, root: str | Path, *, backend: StorageBackend | None = None
    ) -> None:
        self.root = Path(root)
        self.backend: StorageBackend = backend or LocalDiskBackend(self.root)
        self.catalog = StoreCatalog(self.root)

    # -- writes ---------------------------------------------------------------

    def put(
        self,
        namespace: str | Namespace,
        name: str,
        content: bytes | str,
        *,
        note: str = "",
    ) -> tuple[VersionRecord, bool]:
        """Validate and store one new version of ``name``.

        Returns ``(version, created)`` — ``created`` is ``False`` when the
        content is byte-identical to the current head (idempotent put).
        ``code`` objects additionally pass the put-time preview that
        captures their declared registration names and rejects conflicts;
        ``lake`` / ``runs`` are reference namespaces (see :meth:`attach`).
        """
        ns = _coerce_namespace(namespace)
        _check_name(name)
        if ns.is_reference:
            raise StoreValidationError(
                f"{ns.value!r} is a path-reference namespace; register "
                f"directories with attach(), not put()"
            )
        payload = content.encode("utf-8") if isinstance(content, str) else content
        try:
            text = payload.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise StoreValidationError(
                f"{ns.value}/{name}: object content must be UTF-8 text: {exc}"
            ) from exc

        head = self.catalog.head(ns.value, name)
        digest = _sha256_of(payload)
        if head is not None and head.content_hash == digest:
            # Idempotent put: no new version, but still make sure the
            # object's bytes are intact (a re-put of known content heals
            # a tampered or lost object file).
            self.backend.put_object(digest, payload)
            return head, False

        declared: tuple[DeclaredName, ...] = ()
        if ns is Namespace.CODE:
            check_code_source(text, origin=f"{ns.value}/{name}")
            declared = self._put_code(name, text)
        else:
            check_experiment_document(text, origin=f"{ns.value}/{name}")
            declared = ()

        # put_object is content-guarded: intact objects are left untouched,
        # tampered or missing ones are (re)written atomically.
        self.backend.put_object(digest, payload)
        self.catalog.record_object(digest, len(payload))
        version = self.catalog.insert_version(
            ns.value, name, digest, note or self._default_note(ns)
        )
        if declared:
            self.catalog.record_declared_names(
                ns.value, name, version.seq, list(declared)
            )
        return version, True

    def _put_code(self, name: str, text: str) -> tuple[DeclaredName, ...]:
        """Preview a code candidate: static checks + conflict detection."""
        from .loader import (  # lazy: needs pulsar-core
            ensure_not_materialized,
            preview_registration,
        )

        origin = f"code/{name}"
        ensure_not_materialized(name)
        declared = preview_registration(text, name=name)
        conflicts = self._conflicts(name, declared)
        if conflicts:
            details = "; ".join(
                f"{item.registered_name} ({item.registry_kind}) already declared by "
                f"{owner}"
                for owner, item in conflicts
            )
            raise StoreValidationError(
                f"{origin}: registration-name conflict: {details}; stored code "
                f"objects must not collide — rename the registration or "
                f"roll the other object back"
            )
        return declared

    def _conflicts(
        self, name: str, declared: Iterable[DeclaredName]
    ) -> list[tuple[str, DeclaredName]]:
        """Declared names already claimed by *other* code objects."""
        claimed: dict[tuple[str, str], str] = {}
        for owner, item in self.catalog.all_declared_names(
            Namespace.CODE.value, exclude_name=name
        ):
            claimed.setdefault((item.registry_kind, item.registered_name), owner)
        return [
            (claimed[(item.registry_kind, item.registered_name)], item)
            for item in declared
            if (item.registry_kind, item.registered_name) in claimed
        ]

    @staticmethod
    def _default_note(namespace: Namespace) -> str:
        return f"{namespace.value} object"

    def attach(
        self,
        namespace: str | Namespace,
        name: str,
        path: str | Path,
        *,
        note: str = "",
    ) -> AttachmentRecord:
        """Index one existing directory in the ``lake`` / ``runs`` namespace.

        The path is recorded as an absolute reference in the catalog;
        nothing is copied, moved or migrated — the lake keeps its own
        layout and run directories keep their artifacts.
        """
        ns = _coerce_namespace(namespace)
        _check_name(name)
        if ns.is_object:
            raise StoreValidationError(
                f"{ns.value!r} is an object namespace; store content with "
                f"put(), not attach()"
            )
        target = Path(path)
        if not target.is_dir():
            raise StoreValidationError(
                f"{ns.value}/{name}: attach target is not an existing "
                f"directory: {target}"
            )
        return self.catalog.attach(
            ns.value, name, str(target.resolve()), note or f"{ns.value} path reference"
        )

    def rollback(
        self,
        namespace: str | Namespace,
        name: str,
        to: int | str,
        *,
        note: str = "",
    ) -> VersionRecord:
        """Append a version that restores an earlier content hash.

        ``to`` is a seq number (``int``) or a content hash (``str``).
        Rolling back to the current head is refused — that is not a
        change. The appended version's note records where it rolled back
        to, so history shows both the mistake and the recovery.
        """
        ns = _coerce_namespace(namespace)
        _check_name(name)
        if ns.is_reference:
            raise StoreValidationError(
                f"{ns.value!r} is a path-reference namespace; nothing to roll back"
            )
        if isinstance(to, bool):  # pragma: no cover - guard bool-is-int
            raise StoreValidationError("rollback target must be a seq or a hash")
        if isinstance(to, int):
            target = self.catalog.version_by_seq(ns.value, name, to)
        else:
            target = self.catalog.version_by_hash(ns.value, name, to)
        head = self.catalog.head(ns.value, name)
        if head is not None and head.seq == target.seq:
            raise StoreValidationError(
                f"{ns.value}/{name}: seq {target.seq} is already the head; "
                f"nothing to roll back"
            )
        rollback_note = (
            note or f"rollback to seq {target.seq} ({target.content_hash[:12]})"
        )
        version = self.catalog.insert_version(
            ns.value, name, target.content_hash, rollback_note
        )
        if ns is Namespace.CODE:
            # A code object's declared registration names are a function of
            # its bytes; carry them to the rollback point's new version so
            # the head always answers conflict queries without re-execution.
            carried = self.catalog.declared_names_at(ns.value, name, target.seq)
            self.catalog.record_declared_names(
                ns.value, name, version.seq, list(carried)
            )
        return version

    # -- reads ----------------------------------------------------------------

    def resolve(self, namespace: str | Namespace, name: str) -> VersionRecord:
        """The head version of ``name`` (raises when the name is unknown)."""
        ns = _coerce_namespace(namespace)
        _check_name(name)
        if ns.is_reference:
            raise StoreValidationError(
                f"{ns.value!r} is a path-reference namespace; resolve() applies "
                f"to object namespaces — use attachments()"
            )
        head = self.catalog.head(ns.value, name)
        if head is None:
            raise StoreNotFoundError(
                f"{ns.value}/{name} does not exist in the store"
            )
        return head

    def get(
        self,
        namespace: str | Namespace,
        name: str,
        *,
        content_hash: str | None = None,
        seq: int | None = None,
    ) -> bytes:
        """Read one object's bytes, verifying them against their hash.

        ``content_hash`` pins an exact version (the reproduce-by-hash
        path); ``seq`` picks a history entry; the default is the head.
        Hash verification happens on every read: a tampered object file
        raises instead of ever reaching validation or assembly.
        """
        ns = _coerce_namespace(namespace)
        if ns.is_reference:
            raise StoreValidationError(
                f"{ns.value!r} is a path-reference namespace; its entries are "
                f"paths, not objects"
            )
        if content_hash is not None:
            version = self.catalog.version_by_hash(ns.value, name, content_hash)
        elif seq is not None:
            version = self.catalog.version_by_seq(ns.value, name, seq)
        else:
            version = self.resolve(ns, name)
        payload = self.backend.get_object(version.content_hash)
        actual = _sha256_of(payload)
        if actual != version.content_hash:
            raise StoreTamperedError(
                f"{ns.value}/{name}@seq{version.seq}: object bytes no longer "
                f"match their content hash (expected "
                f"{version.content_hash[:12]}, read {actual[:12]}); refusing "
                f"to serve tampered content"
            )
        return payload

    def get_text(
        self,
        namespace: str | Namespace,
        name: str,
        *,
        content_hash: str | None = None,
        seq: int | None = None,
    ) -> str:
        """UTF-8 text form of :meth:`get`."""
        return self.get(namespace, name, content_hash=content_hash, seq=seq).decode(
            "utf-8"
        )

    def entries(
        self, namespace: str | Namespace | None = None
    ) -> tuple[EntrySummary | AttachmentRecord, ...]:
        """Object entries and (in reference namespaces) path attachments.

        Named ``entries`` (not ``list``) so the builtin ``list`` type stays
        usable in this module's annotations; the CLI subcommand keeps the
        user-facing name ``pulsar store list``.
        """
        if namespace is None:
            merged: list[EntrySummary | AttachmentRecord] = list(
                self.catalog.entries()
            )
            merged.extend(self.catalog.attachments())
            return tuple(merged)
        ns = _coerce_namespace(namespace)
        if ns.is_reference:
            return tuple(self.catalog.attachments(ns.value))
        return tuple(self.catalog.entries(ns.value))

    def history(self, namespace: str | Namespace, name: str) -> list[VersionRecord]:
        """Every version of ``name`` in seq order (rollbacks included)."""
        ns = _coerce_namespace(namespace)
        _check_name(name)
        if ns.is_reference:
            raise StoreValidationError(
                f"{ns.value!r} is a path-reference namespace; use attachments()"
            )
        return self.catalog.history(ns.value, name)

    def declared_names(
        self,
        namespace: str | Namespace,
        name: str,
        *,
        content_hash: str | None = None,
    ) -> tuple[DeclaredName, ...]:
        """Registration names one code object declared at put time."""
        ns = _coerce_namespace(namespace)
        if ns is not Namespace.CODE:
            return ()
        if content_hash is not None:
            version = self.catalog.version_by_hash(ns.value, name, content_hash)
        else:
            version = self.resolve(ns, name)
        return self.catalog.declared_names_at(ns.value, name, version.seq)

    def attachments(
        self, namespace: str | Namespace | None = None
    ) -> list[AttachmentRecord]:
        """Path references in the lake/runs namespaces."""
        ns = None if namespace is None else _coerce_namespace(namespace)
        return self.catalog.attachments(None if ns is None else ns.value)
