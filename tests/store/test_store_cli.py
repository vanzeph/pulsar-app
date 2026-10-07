"""``pulsar store`` CLI tests (experiments / lake / runs; the code
namespace needs pulsar-core and is covered by the gated assembly suite).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from pulsar_app.cli import EXIT_CONFIG, EXIT_OK, EXIT_UNEXPECTED, main


@pytest.fixture
def store_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "store"
    monkeypatch.setenv("PULSAR_STORE_ROOT", str(root))
    return root


def _write(tmp_path: Path, name: str, text: str) -> Path:
    target = tmp_path / name
    target.write_text(text, encoding="utf-8")
    return target


def test_put_list_get_round_trip(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    document = _write(
        tmp_path,
        "demo.toml",
        '[experiment]\nid = "demo"\nstatus = "candidate"\n',
    )
    assert main(["store", "put", str(document), "--namespace", "experiments", "--name", "demo"]) == EXIT_OK
    out = capsys.readouterr().out
    assert "seq=1" in out and "created" in out

    assert main(["store", "list", "--namespace", "experiments"]) == EXIT_OK
    assert "experiments/demo" in capsys.readouterr().out

    assert main(["store", "get", "--namespace", "experiments", "--name", "demo"]) == EXIT_OK
    assert capsys.readouterr().out == '[experiment]\nid = "demo"\nstatus = "candidate"\n'


def test_get_writes_to_file_when_asked(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    document = _write(tmp_path, "demo.toml", '[experiment]\nid = "d"\nstatus = "candidate"\n')
    main(["store", "put", str(document), "--namespace", "experiments", "--name", "demo"])
    target = tmp_path / "out.toml"
    assert main(["store", "get", "--namespace", "experiments", "--name", "demo", "--out", str(target)]) == EXIT_OK
    assert target.read_text(encoding="utf-8") == document.read_text(encoding="utf-8")


def test_history_and_rollback_via_cli(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    v1 = _write(tmp_path, "v1.toml", '[experiment]\nid = "d"\nstatus = "candidate"\n')
    v2 = _write(tmp_path, "v2.toml", '[experiment]\nid = "d"\nstatus = "active"\n')
    main(["store", "put", str(v1), "--namespace", "experiments", "--name", "demo"])
    main(["store", "put", str(v2), "--namespace", "experiments", "--name", "demo"])

    assert main(["store", "history", "--namespace", "experiments", "--name", "demo"]) == EXIT_OK
    history = capsys.readouterr().out
    assert "seq=1" in history and "seq=2" in history

    assert main(["store", "rollback", "--namespace", "experiments", "--name", "demo", "--to", "1"]) == EXIT_OK
    assert "rolled back" in capsys.readouterr().out
    assert main(["store", "get", "--namespace", "experiments", "--name", "demo"]) == EXIT_OK
    assert 'status = "candidate"' in capsys.readouterr().out


def test_attach_and_list_references(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    lake = tmp_path / "lake"
    lake.mkdir()
    assert main(["store", "attach", str(lake), "--namespace", "lake", "--name", "default"]) == EXIT_OK
    assert main(["store", "list", "--namespace", "lake"]) == EXIT_OK
    listing = capsys.readouterr().out
    assert "lake/default" in listing and "[reference]" in listing


def test_validation_failure_is_a_config_exit(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    bad = _write(tmp_path, "bad.toml", "not = [a toml")
    assert main(["store", "put", str(bad), "--namespace", "experiments", "--name", "bad"]) == EXIT_CONFIG
    assert "invalid TOML" in capsys.readouterr().err


def test_missing_file_and_unknown_name_are_config_exits(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert main(["store", "put", str(tmp_path / "nope.toml"), "--namespace", "experiments", "--name", "x"]) == EXIT_CONFIG
    assert "not found" in capsys.readouterr().err
    assert main(["store", "get", "--namespace", "experiments", "--name", "ghost"]) == EXIT_CONFIG
    assert "does not exist" in capsys.readouterr().err


def test_tampered_object_is_an_unexpected_error(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import hashlib

    document = _write(tmp_path, "demo.toml", '[experiment]\nid = "d"\nstatus = "candidate"\n')
    main(["store", "put", str(document), "--namespace", "experiments", "--name", "demo"])
    digest = hashlib.sha256(document.read_bytes()).hexdigest()
    object_path = store_env / "objects" / digest[:2] / digest
    object_path.write_bytes(b"tampered")
    assert main(["store", "get", "--namespace", "experiments", "--name", "demo"]) == EXIT_UNEXPECTED
    assert "tampered" in capsys.readouterr().err.lower() or "hash" in capsys.readouterr().err.lower()


def test_store_flag_overrides_environment_root(
    tmp_path: Path, store_env: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    other = tmp_path / "other-store"
    document = _write(tmp_path, "demo.toml", '[experiment]\nid = "d"\nstatus = "candidate"\n')
    assert main(["store", "--store", str(other), "put", str(document), "--namespace", "experiments", "--name", "demo"]) == EXIT_OK
    assert (other / "catalog.db").is_file()
    assert not store_env.exists(), "explicit --store must win over the env root"


def test_lake_namespace_rejects_put_via_cli(
    tmp_path: Path, store_env: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    document = _write(tmp_path, "demo.toml", "x")
    from pulsar_app.cli import build_parser

    parser = build_parser()  # choices= guard makes this a usage error (argparse exit 2)
    with pytest.raises(SystemExit):
        parser.parse_args(["store", "put", str(document), "--namespace", "lake", "--name", "x"])
