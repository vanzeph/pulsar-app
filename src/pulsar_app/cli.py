"""pulsar CLI skeleton: the research / paper / live subcommands plus the
unified-store interface (``pulsar store ...``).

Each run subcommand loads a TOML run configuration, injects its run mode,
assembles the configured plugins and writes the RunManifest::

    pulsar research --config runs/dualma.toml [--runs-dir runs]

The store subcommands are the Agent write interface into the unified
workspace storage (task STORE1)::

    pulsar store put experiments/drill.toml --namespace experiments --name drill
    pulsar store put my_factors.py --namespace code --name my_factors
    pulsar store list [--namespace experiments]
    pulsar store history --namespace code --name my_factors
    pulsar store rollback --namespace experiments --name drill --to 1
    pulsar store get --namespace experiments --name drill [--hash <sha256>] [--out file]
    pulsar store attach data/lake --namespace lake --name default

Exit codes: 0 success, 1 unexpected pulsar-app error, 2 configuration or
store validation error, 3 live mode locked, 4 plugin resolution failure.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import __version__
from .config import RunMode, load_config
from .errors import (
    ConfigError,
    LiveModeLockedError,
    PluginError,
    PulsarAppError,
    StoreError,
    StoreNotFoundError,
    StoreValidationError,
)
from .run import execute_run
from .store.catalog import AttachmentRecord
from .store.engine import Store

__all__ = [
    "EXIT_OK",
    "EXIT_UNEXPECTED",
    "EXIT_CONFIG",
    "EXIT_LIVE_LOCKED",
    "EXIT_PLUGIN",
    "build_parser",
    "main",
]

EXIT_OK = 0
EXIT_UNEXPECTED = 1
EXIT_CONFIG = 2
EXIT_LIVE_LOCKED = 3
EXIT_PLUGIN = 4

_MODES = (RunMode.RESEARCH, RunMode.PAPER, RunMode.LIVE)

_STORE_ROOT_ENV = "PULSAR_STORE_ROOT"
_STORE_NAMESPACES = ("experiments", "code", "lake", "runs")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="pulsar",
        description="Pulsar runtime: assemble a run from TOML configuration and archive its manifest.",
    )
    parser.add_argument("--version", action="version", version=f"pulsar {__version__}")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for mode in _MODES:
        sub = subparsers.add_parser(
            mode.value,
            help=f"assemble and execute a {mode.value} run from a configuration file",
        )
        sub.add_argument(
            "--config",
            required=True,
            type=Path,
            help="path to the TOML run configuration",
        )
        sub.add_argument(
            "--runs-dir",
            type=Path,
            default=Path("runs"),
            help="directory receiving <run_id>/manifest.json (default: ./runs)",
        )

    store = subparsers.add_parser(
        "store",
        help="the unified workspace store: versioned objects and path references",
    )
    store.add_argument(
        "--store",
        type=Path,
        default=None,
        help=f"storage root (default: ${_STORE_ROOT_ENV} or ./store)",
    )
    store_sub = store.add_subparsers(dest="store_command", required=True)

    put = store_sub.add_parser("put", help="validate and store one object version")
    put.add_argument("file", type=Path, help="file whose bytes enter the store")
    put.add_argument(
        "--namespace", required=True, choices=_STORE_NAMESPACES[:2],
        help="object namespace (experiments | code)",
    )
    put.add_argument("--name", required=True, help="store name ([a-z][a-z0-9_.-]{0,63})")
    put.add_argument("--note", default="", help="free-form note recorded with the version")

    attach = store_sub.add_parser(
        "attach", help="index an existing directory in the lake/runs namespace"
    )
    attach.add_argument("directory", type=Path, help="existing directory to reference")
    attach.add_argument("--namespace", required=True, choices=_STORE_NAMESPACES[2:])
    attach.add_argument("--name", required=True)
    attach.add_argument("--note", default="")

    listing = store_sub.add_parser("list", help="list entries (all namespaces by default)")
    listing.add_argument("--namespace", choices=_STORE_NAMESPACES, default=None)

    history = store_sub.add_parser("history", help="show every version of one name")
    history.add_argument("--namespace", required=True, choices=_STORE_NAMESPACES[:2])
    history.add_argument("--name", required=True)

    rollback = store_sub.add_parser(
        "rollback", help="append a version restoring an earlier content hash"
    )
    rollback.add_argument("--namespace", required=True, choices=_STORE_NAMESPACES[:2])
    rollback.add_argument("--name", required=True)
    rollback.add_argument(
        "--to", required=True, help="target seq number or content hash"
    )
    rollback.add_argument("--note", default="")

    get = store_sub.add_parser(
        "get", help="read one object (head, or a pinned hash/seq), hash-verified"
    )
    get.add_argument("--namespace", required=True, choices=_STORE_NAMESPACES[:2])
    get.add_argument("--name", required=True)
    get.add_argument("--hash", default=None, help="pin an exact content hash")
    get.add_argument("--seq", type=int, default=None, help="pick a history seq")
    get.add_argument(
        "--out", type=Path, default=None, help="write bytes here instead of stdout"
    )

    return parser


def _resolve_store_root(override: Path | None) -> Path:
    if override is not None:
        return override
    from_env = os.environ.get(_STORE_ROOT_ENV)
    return Path(from_env) if from_env else Path("store")


def _run_store_command(args: argparse.Namespace) -> int:
    store = Store(_resolve_store_root(args.store))
    command = args.store_command

    if command == "put":
        if not args.file.is_file():
            raise StoreValidationError(f"input file not found: {args.file}")
        version, created = store.put(
            args.namespace,
            args.name,
            args.file.read_bytes(),
            note=args.note,
        )
        state = "created" if created else "unchanged (content matches head)"
        print(
            f"{args.namespace}/{args.name} seq={version.seq} "
            f"hash={version.content_hash[:12]} {state}"
        )
        for declared in store.declared_names(args.namespace, args.name):
            print(f"  declares {declared.registered_name} ({declared.registry_kind})")
        return EXIT_OK

    if command == "attach":
        record = store.attach(args.namespace, args.name, args.directory, note=args.note)
        print(f"{record.namespace}/{record.name} -> {record.path} ({record.note})")
        return EXIT_OK

    if command == "list":
        for entry in store.entries(args.namespace):
            if isinstance(entry, AttachmentRecord):
                print(f"{entry.namespace}/{entry.name} -> {entry.path} [reference]")
            else:
                suffix = (
                    " declared="
                    + ",".join(
                        f"{item.registered_name}({item.registry_kind})"
                        for item in entry.declared_names
                    )
                    if entry.declared_names
                    else ""
                )
                print(
                    f"{entry.namespace}/{entry.name} seq={entry.head_seq} "
                    f"hash={entry.head_hash[:12]} versions={entry.versions}{suffix}"
                )
        return EXIT_OK

    if command == "history":
        for version in store.history(args.namespace, args.name):
            print(
                f"seq={version.seq} hash={version.content_hash[:12]} "
                f"at={version.created_at} note={version.note}"
            )
        return EXIT_OK

    if command == "rollback":
        target = int(args.to) if args.to.isdigit() else args.to
        version = store.rollback(args.namespace, args.name, target, note=args.note)
        print(
            f"{args.namespace}/{args.name} rolled back: new seq={version.seq} "
            f"hash={version.content_hash[:12]} ({version.note})"
        )
        return EXIT_OK

    if command == "get":
        payload = store.get(
            args.namespace, args.name, content_hash=args.hash, seq=args.seq
        )
        if args.out is not None:
            args.out.write_bytes(payload)
            print(f"wrote {len(payload)} bytes to {args.out}")
        else:
            sys.stdout.buffer.write(payload)
            sys.stdout.buffer.flush()
        return EXIT_OK

    raise PulsarAppError(f"unknown store command {command!r}")  # pragma: no cover


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns the process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "store":
        try:
            return _run_store_command(args)
        except (StoreValidationError, StoreNotFoundError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_CONFIG
        except StoreError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_UNEXPECTED
        except PulsarAppError as exc:  # pragma: no cover - defensive catch-all
            print(f"error: {exc}", file=sys.stderr)
            return EXIT_UNEXPECTED

    try:
        config = load_config(args.config).with_mode(RunMode(args.command))
        outcome = execute_run(config, runs_dir=args.runs_dir)
    except LiveModeLockedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_LIVE_LOCKED
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    except StoreValidationError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
    except StoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_UNEXPECTED
    except PluginError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_PLUGIN
    except PulsarAppError as exc:  # pragma: no cover - defensive catch-all
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_UNEXPECTED

    print(
        f"run {outcome.manifest.run_id} ({outcome.manifest.mode}) complete; "
        f"manifest written to {outcome.manifest_path}"
    )
    return EXIT_OK


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
