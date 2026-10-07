"""lake / runs namespaces: path references in the catalog, no migration.

The design constraint (STORE1): lake/runs 纳入 catalog 但**不迁移**既有
湖目录 — the catalog indexes existing directories by absolute path; the
directories themselves are never copied, moved or rewritten.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pulsar_app.errors import StoreValidationError
from pulsar_app.store import Store


def test_attach_records_absolute_path_reference(
    store_root: Path, lake_dir: Path, tmp_path: Path
) -> None:
    runs = tmp_path / "runs"
    runs.mkdir()
    store = Store(store_root)
    lake_record = store.attach("lake", "default", lake_dir, note="main lake")
    runs_record = store.attach("runs", "default", runs)

    assert lake_record.path == str(lake_dir.resolve())
    assert lake_record.note == "main lake"
    assert runs_record.path == str(runs.resolve())

    listing = {item.name: item for item in store.entries("lake")}
    assert listing["default"].path == str(lake_dir.resolve())
    all_entries = {item.name for item in store.entries()}
    assert {"default"} <= all_entries  # both namespaces surface in the flat list


def test_attach_is_idempotent_and_updates(store_root: Path, lake_dir: Path) -> None:
    store = Store(store_root)
    store.attach("lake", "default", lake_dir, note="first")
    updated = store.attach("lake", "default", lake_dir, note="second")
    assert updated.note == "second"
    assert len(store.attachments("lake")) == 1


def test_attach_requires_an_existing_directory(
    store_root: Path, tmp_path: Path
) -> None:
    store = Store(store_root)
    with pytest.raises(StoreValidationError, match="not an existing directory"):
        store.attach("lake", "default", tmp_path / "nope")
    # files are rejected too
    plain = tmp_path / "file.txt"
    plain.write_text("x")
    with pytest.raises(StoreValidationError, match="not an existing directory"):
        store.attach("runs", "default", plain)


def test_attach_never_touches_the_lake_directory(
    store_root: Path, lake_dir: Path
) -> None:
    before = {
        path.relative_to(lake_dir): path.stat().st_mtime_ns
        for path in lake_dir.rglob("*")
    }
    store = Store(store_root)
    store.attach("lake", "default", lake_dir)

    after = {
        path.relative_to(lake_dir): path.stat().st_mtime_ns
        for path in lake_dir.rglob("*")
    }
    assert before == after, "attach() must not migrate or rewrite lake contents"
    # and no lake bytes were copied into the store root
    assert list((store_root / "objects").rglob("*.parquet")) == []


def test_attachments_filter_by_namespace(store_root: Path, lake_dir: Path) -> None:
    runs = lake_dir.parent.parent / "runs"
    runs.mkdir(exist_ok=True)
    store = Store(store_root)
    store.attach("lake", "default", lake_dir)
    store.attach("runs", "default", runs)
    assert [record.namespace for record in store.attachments()] == ["lake", "runs"]
    assert [record.namespace for record in store.attachments("runs")] == ["runs"]
