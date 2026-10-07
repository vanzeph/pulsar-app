"""The store catalog: SQLite metadata for names, versions and references.

Why SQLite and not DuckDB (the STORE1 decision, recorded here because the
task asked for the reason): the catalog is a *metadata* workload —
point lookups ("head of name X in namespace N"), short ordered scans
("history of X") and small inserts, one row per put/rollback/attach. That
is OLTP shape, and SQLite serves it from the standard library with
transactional guarantees and zero added dependencies (pulsar-app's base
install stays pydantic + pulsar-contracts). DuckDB is an analytical
engine — the right tool for scanning ``events.parquet`` archives, not for
a name→version index — and would add a heavy binary dependency to every
CI job for no query this catalog ever runs. If the catalog ever grows an
analytical face, it can read the same schema side by side.

Schema (version 1):

* ``objects``        — every content hash the backend holds (size, time)
* ``versions``       — append-only history: ``(namespace, name, seq)``
                       pointing at a hash; the head is the max seq. A
                       rollback *appends* a version referencing an old
                       hash (auditable: history shows the rollback, the
                       prior versions stay intact).
* ``declared_names`` — registration names a code object brings (from the
                       put-time preview), for conflict checks that must
                       not re-execute arbitrary modules.
* ``attachments``    — path references for the ``lake`` / ``runs``
                       namespaces: the catalog indexes existing
                       directories, it never copies or migrates them.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..errors import StoreNotFoundError

__all__ = [
    "CATALOG_FILENAME",
    "SCHEMA_VERSION",
    "VersionRecord",
    "EntrySummary",
    "AttachmentRecord",
    "DeclaredName",
    "StoreCatalog",
]

#: The catalog file inside the store root.
CATALOG_FILENAME = "catalog.db"

SCHEMA_VERSION = 1

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS objects (
    content_hash TEXT PRIMARY KEY,
    size_bytes INTEGER NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS versions (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    seq INTEGER NOT NULL,
    content_hash TEXT NOT NULL REFERENCES objects(content_hash),
    note TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL,
    PRIMARY KEY (namespace, name, seq)
);
CREATE INDEX IF NOT EXISTS idx_versions_object
    ON versions(namespace, name, content_hash);
CREATE TABLE IF NOT EXISTS declared_names (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    seq INTEGER NOT NULL,
    registry_kind TEXT NOT NULL,
    registered_name TEXT NOT NULL,
    PRIMARY KEY (namespace, name, seq, registry_kind, registered_name)
);
CREATE TABLE IF NOT EXISTS attachments (
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    path TEXT NOT NULL,
    note TEXT NOT NULL DEFAULT '',
    attached_at TEXT NOT NULL,
    PRIMARY KEY (namespace, name)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True, slots=True)
class VersionRecord:
    """One version of one name: the unit of history and rollback."""

    namespace: str
    name: str
    seq: int
    content_hash: str
    note: str
    created_at: str


@dataclass(frozen=True, slots=True)
class DeclaredName:
    """One registry name a code object declares (kind, name)."""

    registry_kind: str
    registered_name: str


@dataclass(frozen=True, slots=True)
class EntrySummary:
    """The head state of one name, as ``store list`` reports it."""

    namespace: str
    name: str
    head_seq: int
    head_hash: str
    versions: int
    created_at: str
    note: str
    declared_names: tuple[DeclaredName, ...] = ()


@dataclass(frozen=True, slots=True)
class AttachmentRecord:
    """A path reference in the lake/runs namespaces."""

    namespace: str
    name: str
    path: str
    note: str
    attached_at: str


def _row_to_version(row: sqlite3.Row) -> VersionRecord:
    return VersionRecord(
        namespace=str(row["namespace"]),
        name=str(row["name"]),
        seq=int(row["seq"]),
        content_hash=str(row["content_hash"]),
        note=str(row["note"]),
        created_at=str(row["created_at"]),
    )


class StoreCatalog:
    """SQLite-backed metadata index of one store root."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(self.root / CATALOG_FILENAME)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys = ON")
        with self._connection:
            self._connection.executescript(_SCHEMA)
            self._connection.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )

    def close(self) -> None:
        self._connection.close()

    # -- objects ------------------------------------------------------------

    def record_object(self, content_hash: str, size_bytes: int) -> None:
        with self._connection:
            self._connection.execute(
                "INSERT OR IGNORE INTO objects(content_hash, size_bytes, created_at)"
                " VALUES (?, ?, ?)",
                (content_hash, size_bytes, _now()),
            )

    def has_object(self, content_hash: str) -> bool:
        row = self._connection.execute(
            "SELECT 1 FROM objects WHERE content_hash = ?", (content_hash,)
        ).fetchone()
        return row is not None

    # -- versions -------------------------------------------------------------

    def insert_version(
        self, namespace: str, name: str, content_hash: str, note: str
    ) -> VersionRecord:
        with self._connection:
            row = self._connection.execute(
                "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM versions"
                " WHERE namespace = ? AND name = ?",
                (namespace, name),
            ).fetchone()
            seq = int(row["max_seq"]) + 1
            created = _now()
            self._connection.execute(
                "INSERT INTO versions(namespace, name, seq, content_hash, note, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (namespace, name, seq, content_hash, note, created),
            )
        return VersionRecord(namespace, name, seq, content_hash, note, created)

    def head(self, namespace: str, name: str) -> VersionRecord | None:
        row = self._connection.execute(
            "SELECT * FROM versions WHERE namespace = ? AND name = ?"
            " ORDER BY seq DESC LIMIT 1",
            (namespace, name),
        ).fetchone()
        return None if row is None else _row_to_version(row)

    def version_by_seq(self, namespace: str, name: str, seq: int) -> VersionRecord:
        row = self._connection.execute(
            "SELECT * FROM versions WHERE namespace = ? AND name = ? AND seq = ?",
            (namespace, name, seq),
        ).fetchone()
        if row is None:
            raise StoreNotFoundError(
                f"{namespace}/{name} has no version {seq}"
            )
        return _row_to_version(row)

    def version_by_hash(
        self, namespace: str, name: str, content_hash: str
    ) -> VersionRecord:
        row = self._connection.execute(
            "SELECT * FROM versions WHERE namespace = ? AND name = ?"
            " AND content_hash = ? ORDER BY seq DESC LIMIT 1",
            (namespace, name, content_hash),
        ).fetchone()
        if row is None:
            raise StoreNotFoundError(
                f"{namespace}/{name} has no version with content hash {content_hash}"
            )
        return _row_to_version(row)

    def history(self, namespace: str, name: str) -> list[VersionRecord]:
        rows = self._connection.execute(
            "SELECT * FROM versions WHERE namespace = ? AND name = ?"
            " ORDER BY seq ASC",
            (namespace, name),
        ).fetchall()
        return [_row_to_version(row) for row in rows]

    def entries(self, namespace: str | None = None) -> list[EntrySummary]:
        filter_clause = "" if namespace is None else "WHERE v.namespace = ?"
        params: tuple[Any, ...] = () if namespace is None else (namespace,)
        rows = self._connection.execute(
            "SELECT v.namespace, v.name, v.seq, v.content_hash, v.note, v.created_at,"
            " (SELECT COUNT(*) FROM versions v2"
            "  WHERE v2.namespace = v.namespace AND v2.name = v.name) AS versions"
            " FROM versions v"
            " JOIN (SELECT namespace, name, MAX(seq) AS max_seq FROM versions"
            "       GROUP BY namespace, name) head"
            "   ON head.namespace = v.namespace AND head.name = v.name"
            "  AND v.seq = head.max_seq"
            f" {filter_clause}"
            " ORDER BY v.namespace, v.name",
            params,
        ).fetchall()
        declared = self.declared_names_by_entry(namespace)
        return [
            EntrySummary(
                namespace=str(row["namespace"]),
                name=str(row["name"]),
                head_seq=int(row["seq"]),
                head_hash=str(row["content_hash"]),
                versions=int(row["versions"]),
                created_at=str(row["created_at"]),
                note=str(row["note"]),
                declared_names=declared.get((str(row["namespace"]), str(row["name"])), ()),
            )
            for row in rows
        ]

    # -- declared registration names -------------------------------------------

    def record_declared_names(
        self, namespace: str, name: str, seq: int, declared: list[DeclaredName]
    ) -> None:
        with self._connection:
            self._connection.execute(
                "DELETE FROM declared_names WHERE namespace = ? AND name = ? AND seq = ?",
                (namespace, name, seq),
            )
            self._connection.executemany(
                "INSERT INTO declared_names(namespace, name, seq, registry_kind,"
                " registered_name) VALUES (?, ?, ?, ?, ?)",
                [
                    (namespace, name, seq, item.registry_kind, item.registered_name)
                    for item in declared
                ],
            )

    def declared_names_by_entry(
        self, namespace: str | None
    ) -> dict[tuple[str, str], tuple[DeclaredName, ...]]:
        filter_clause = "" if namespace is None else "WHERE namespace = ?"
        params: tuple[Any, ...] = () if namespace is None else (namespace,)
        rows = self._connection.execute(
            "SELECT namespace, name, registry_kind, registered_name FROM declared_names"
            f" {filter_clause}",
            params,
        ).fetchall()
        collected: dict[tuple[str, str], list[DeclaredName]] = {}
        for row in rows:
            key = (str(row["namespace"]), str(row["name"]))
            collected.setdefault(key, []).append(
                DeclaredName(str(row["registry_kind"]), str(row["registered_name"]))
            )
        return {key: tuple(values) for key, values in collected.items()}

    def declared_names_of_head(
        self, namespace: str, name: str
    ) -> tuple[DeclaredName, ...]:
        return self.declared_names_by_entry(namespace).get((namespace, name), ())

    def declared_names_at(
        self, namespace: str, name: str, seq: int
    ) -> tuple[DeclaredName, ...]:
        rows = self._connection.execute(
            "SELECT registry_kind, registered_name FROM declared_names"
            " WHERE namespace = ? AND name = ? AND seq = ?"
            " ORDER BY registry_kind, registered_name",
            (namespace, name, seq),
        ).fetchall()
        return tuple(
            DeclaredName(str(row["registry_kind"]), str(row["registered_name"]))
            for row in rows
        )

    def all_declared_names(
        self, namespace: str, *, exclude_name: str | None = None
    ) -> list[tuple[str, DeclaredName]]:
        """Every declared registration name in a namespace, with its owner.

        ``exclude_name`` skips one entry (conflict checks compare a
        candidate against *other* code objects, never itself).
        """
        rows = self._connection.execute(
            "SELECT DISTINCT dn.name AS owner, dn.registry_kind, dn.registered_name"
            " FROM declared_names dn JOIN versions v"
            "   ON v.namespace = dn.namespace AND v.name = dn.name"
            "  AND v.seq = (SELECT MAX(seq) FROM versions vx"
            "               WHERE vx.namespace = dn.namespace AND vx.name = dn.name)"
            " WHERE dn.namespace = ?",
            (namespace,),
        ).fetchall()
        return [
            (str(row["owner"]), DeclaredName(str(row["registry_kind"]), str(row["registered_name"])))
            for row in rows
            if exclude_name is None or str(row["owner"]) != exclude_name
        ]

    # -- attachments (lake / runs path references) -------------------------------

    def attach(self, namespace: str, name: str, path: str, note: str) -> AttachmentRecord:
        attached = _now()
        with self._connection:
            self._connection.execute(
                "INSERT INTO attachments(namespace, name, path, note, attached_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(namespace, name) DO UPDATE SET"
                "   path = excluded.path, note = excluded.note,"
                "   attached_at = excluded.attached_at",
                (namespace, name, path, note, attached),
            )
        return AttachmentRecord(namespace, name, path, note, attached)

    def attachments(self, namespace: str | None = None) -> list[AttachmentRecord]:
        filter_clause = "" if namespace is None else "WHERE namespace = ?"
        params: tuple[Any, ...] = () if namespace is None else (namespace,)
        rows = self._connection.execute(
            "SELECT namespace, name, path, note, attached_at FROM attachments"
            f" {filter_clause} ORDER BY namespace, name",
            params,
        ).fetchall()
        return [
            AttachmentRecord(
                namespace=str(row["namespace"]),
                name=str(row["name"]),
                path=str(row["path"]),
                note=str(row["note"]),
                attached_at=str(row["attached_at"]),
            )
            for row in rows
        ]
