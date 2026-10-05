"""pulsar CLI skeleton: the research / paper / live subcommands.

Each subcommand loads a TOML run configuration, injects its run mode,
assembles the configured plugins and writes the RunManifest::

    pulsar research --config runs/dualma.toml [--runs-dir runs]

Exit codes: 0 success, 1 unexpected pulsar-app error, 2 configuration
error, 3 live mode locked, 4 plugin resolution failure.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import __version__
from .config import RunMode, load_config
from .errors import ConfigError, LiveModeLockedError, PluginError, PulsarAppError
from .run import execute_run

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
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point; returns the process exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config).with_mode(RunMode(args.command))
        outcome = execute_run(config, runs_dir=args.runs_dir)
    except LiveModeLockedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_LIVE_LOCKED
    except ConfigError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_CONFIG
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
