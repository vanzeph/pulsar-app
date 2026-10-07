"""Versioned-object engine tests: put → get → history → rollback, tamper
detection, idempotent puts and namespace discipline (experiments
namespace only — the code namespace needs pulsar-core and lives in the
gated assembly suite).
"""

from __future__ import annotations

import hashlib

import pytest

from pulsar_app.errors import (
    StoreNotFoundError,
    StoreTamperedError,
    StoreValidationError,
)
from pulsar_app.store import Namespace, Store, content_hash

# -- put / get ---------------------------------------------------------------


def test_put_get_head_round_trip(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    version, created = store.put("experiments", "drill", experiment_toml)
    assert created is True
    assert version.seq == 1
    assert version.content_hash == hashlib.sha256(experiment_toml.encode()).hexdigest()

    assert store.get_text("experiments", "drill") == experiment_toml
    assert store.resolve("experiments", "drill").seq == 1


def test_put_is_idempotent_for_identical_content(
    store_root: Path, experiment_toml: str
) -> None:
    store = Store(store_root)
    first, created_first = store.put("experiments", "drill", experiment_toml)
    second, created_second = store.put("experiments", "drill", experiment_toml)
    assert created_first and not created_second
    assert second.seq == first.seq
    assert store.history("experiments", "drill") == [first]


def test_object_bytes_are_deduplicated_across_names(
    store_root: Path, experiment_toml: str
) -> None:
    store = Store(store_root)
    store.put("experiments", "one", experiment_toml)
    store.put("experiments", "two", experiment_toml)
    object_files = list((store_root / "objects").rglob("*"))
    stored = [path for path in object_files if path.is_file() and len(path.stem) == 64]
    assert len(stored) == 1, "identical content must be exactly one object"


def test_put_rejects_non_utf8_payload(store_root: Path) -> None:
    store = Store(store_root)
    with pytest.raises(StoreValidationError, match="UTF-8"):
        store.put("experiments", "binary", b"\xff\xfe\x00")


def test_names_are_validated(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    for bad in ("UPPER", "with space", "1starts_digit", "", "a" * 65, "bad/slash"):
        with pytest.raises(StoreValidationError, match="invalid store name"):
            store.put("experiments", bad, experiment_toml)


def test_unknown_namespace_and_name_fail_loudly(store_root: Path) -> None:
    store = Store(store_root)
    with pytest.raises(Exception, match="unknown store namespace"):
        store.put("widgets", "x", "content")
    with pytest.raises(StoreNotFoundError):
        store.get("experiments", "missing")


# -- history / rollback --------------------------------------------------------


def test_history_and_rollback_by_seq(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    v1, _ = store.put("experiments", "drill", experiment_toml, note="initial")
    v2_text = experiment_toml.replace("top_n = 3", "top_n = 4")
    v2, _ = store.put("experiments", "drill", v2_text, note="wider book")

    history = store.history("experiments", "drill")
    assert [version.seq for version in history] == [1, 2]
    assert store.get_text("experiments", "drill") == v2_text

    v3 = store.rollback("experiments", "drill", v1.seq, note="revert wider book")
    assert v3.content_hash == v1.content_hash
    assert v3.seq == 3
    assert v3.note == "revert wider book"
    assert store.get_text("experiments", "drill") == experiment_toml

    history = store.history("experiments", "drill")
    assert [version.seq for version in history] == [1, 2, 3]
    assert history[2].note == "revert wider book"
    # default note records where the rollback pointed
    v4 = store.rollback("experiments", "drill", v2.seq)
    assert "rollback to seq 2" in v4.note
    assert store.get_text("experiments", "drill") == v2_text
    # the pinned older versions stay readable (reproduce-by-hash/seq)
    assert store.get("experiments", "drill", seq=v2.seq) == v2_text.encode()
    assert (
        store.get("experiments", "drill", content_hash=v2.content_hash)
        == v2_text.encode()
    )


def test_rollback_accepts_a_content_hash(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    v1, _ = store.put("experiments", "drill", experiment_toml)
    v2, _ = store.put("experiments", "drill", experiment_toml + "\n# tweak\n")
    rolled = store.rollback("experiments", "drill", v1.content_hash)
    assert rolled.content_hash == v1.content_hash
    assert store.resolve("experiments", "drill").seq == 3
    assert v2.seq == 2  # history intact


def test_rollback_to_head_is_refused(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    store.put("experiments", "drill", experiment_toml)
    with pytest.raises(StoreValidationError, match="already the head"):
        store.rollback("experiments", "drill", 1)


def test_rollback_to_unknown_target_fails(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    store.put("experiments", "drill", experiment_toml)
    with pytest.raises(StoreNotFoundError):
        store.rollback("experiments", "drill", 9)
    with pytest.raises(StoreNotFoundError):
        store.rollback("experiments", "drill", "f" * 64)


# -- tamper detection ------------------------------------------------------------


def test_tampered_object_is_detected_and_refused(
    store_root: Path, experiment_toml: str
) -> None:
    store = Store(store_root)
    version, _ = store.put("experiments", "drill", experiment_toml)
    object_path = store_root / "objects" / version.content_hash[:2] / version.content_hash
    object_path.write_bytes(b"# tampered replacement\n")

    with pytest.raises(StoreTamperedError, match="no longer match their content hash"):
        store.get("experiments", "drill")


def test_reput_of_known_content_heals_a_tampered_object(
    store_root: Path, experiment_toml: str
) -> None:
    store = Store(store_root)
    version, _ = store.put("experiments", "drill", experiment_toml)
    object_path = store_root / "objects" / version.content_hash[:2] / version.content_hash
    object_path.write_bytes(b"# tampered replacement\n")

    store.put("experiments", "drill", experiment_toml)  # same bytes: heals
    assert store.get_text("experiments", "drill") == experiment_toml


# -- namespace discipline ---------------------------------------------------------


def test_reference_namespaces_reject_put(store_root: Path) -> None:
    store = Store(store_root)
    with pytest.raises(StoreValidationError, match="path-reference namespace"):
        store.put("lake", "default", "content")


def test_object_namespaces_reject_attach(store_root: Path, experiment_toml: str) -> None:
    store = Store(store_root)
    store.put("experiments", "drill", experiment_toml)
    with pytest.raises(StoreValidationError, match="object namespace"):
        store.attach("experiments", "drill", store_root)


def test_reference_namespaces_have_no_history_or_objects(store_root: Path) -> None:
    store = Store(store_root)
    with pytest.raises(StoreValidationError, match="path-reference namespace"):
        store.history("lake", "default")
    with pytest.raises(StoreValidationError, match="path-reference namespace"):
        store.get("runs", "default")
    with pytest.raises(StoreValidationError, match="path-reference namespace"):
        store.resolve("lake", "x")


def test_content_hash_helper() -> None:
    assert content_hash(b"abc") == hashlib.sha256(b"abc").hexdigest()
    assert Namespace("code").is_object and not Namespace("code").is_reference
    assert Namespace("runs").is_reference and not Namespace("runs").is_object
