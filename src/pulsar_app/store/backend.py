"""Object backends of the unified store: where content-addressed bytes live.

The store engine keeps *metadata* (names, versions, history, declared
registration names) in the local catalog and the *bytes* of every object
in a backend. Two flavours exist by design (总体架构设计, 统一工作区存储
引擎):

``LocalDiskBackend``
    The first deliverable — objects under ``<root>/objects/<2-char
    fan-out>/<sha256>`` on the workspace's own disk. Content addressing
    makes writes idempotent (an existing hash is never rewritten) and
    makes every read verifiable against the catalog.

``R2StorageBackend`` / ``CosStorageBackend``
    Placeholders for the object-storage future (Cloudflare R2 / Tencent
    COS). The interface is fixed now so the engine never learns a second
    storage dialect later, but the implementations deliberately do
    nothing: constructing or using one raises, and the catalog stays on
    local disk either way (it indexes the workspace, it does not mirror
    it).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Protocol

from ..errors import StoreBackendError

__all__ = [
    "StorageBackend",
    "LocalDiskBackend",
    "R2StorageBackend",
    "CosStorageBackend",
]


class StorageBackend(Protocol):
    """Where object bytes live; addressed by content hash only."""

    def put_object(self, content_hash: str, payload: bytes) -> None: ...

    def get_object(self, content_hash: str) -> bytes: ...

    def has_object(self, content_hash: str) -> bool: ...


def _fan_out(content_hash: str) -> str:
    """Two-character fan-out prefix keeping directories small."""
    return content_hash[:2]


class LocalDiskBackend:
    """Objects as immutable files under ``<root>/objects``.

    Content addressing means identical bytes are one object: an intact
    object is never rewritten, and a write lands via atomic rename so a
    crashed write can never publish a truncated object under a hash. A
    file whose bytes no longer match its name (tampering, partial
    restore) is overwritten by the matching content — re-putting known
    content heals the store.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.objects_dir = self.root / "objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, content_hash: str) -> Path:
        return self.objects_dir / _fan_out(content_hash) / content_hash

    def put_object(self, content_hash: str, payload: bytes) -> None:
        target = self._path(content_hash)
        if target.is_file():
            try:
                if target.read_bytes() == payload:
                    return
            except OSError:  # pragma: no cover - unreadable file falls through
                pass
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(".tmp-" + os.urandom(6).hex())
        try:
            tmp.write_bytes(payload)
            os.replace(tmp, target)
        finally:
            if tmp.exists():  # pragma: no cover - only on a failed replace
                tmp.unlink()

    def get_object(self, content_hash: str) -> bytes:
        path = self._path(content_hash)
        if not path.is_file():
            raise StoreBackendError(
                f"object {content_hash} is missing from the local backend "
                f"({path}); the catalog references an object the backend "
                f"no longer has"
            )
        return path.read_bytes()

    def has_object(self, content_hash: str) -> bool:
        return self._path(content_hash).is_file()


class _ObjectStoragePlaceholder:
    """Shared behaviour of the not-yet-implemented remote backends."""

    _service = "object storage"

    def __init__(self, *, endpoint: str, bucket: str) -> None:
        self.endpoint = endpoint
        self.bucket = bucket

    def _refuse(self, operation: str) -> StoreBackendError:
        return StoreBackendError(
            f"{self._service} backend is an interface placeholder by design "
            f"(STORE1); {operation} is not implemented. Use the local disk "
            f"backend — the engine interface is the deliverable, remote "
            f"backends arrive with their own task."
        )

    def put_object(self, content_hash: str, payload: bytes) -> None:
        raise self._refuse("put_object")

    def get_object(self, content_hash: str) -> bytes:
        raise self._refuse("get_object")

    def has_object(self, content_hash: str) -> bool:
        raise self._refuse("has_object")


class R2StorageBackend(_ObjectStoragePlaceholder):
    """Cloudflare R2 backend — interface placeholder, not implemented."""

    _service = "cloudflare-r2"


class CosStorageBackend(_ObjectStoragePlaceholder):
    """Tencent COS backend — interface placeholder, not implemented."""

    _service = "tencent-cos"
