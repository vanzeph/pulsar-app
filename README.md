# pulsar-app

Runtime assembly and configuration for the **Pulsar** A-share quantitative
trading system: one run = **configuration + assembly + archive**.

This repository is the assembly point of Pulsar's multi-repo topology. It owns:

- the **TOML run-configuration schema** and its validation,
- the **plugin registry** resolving data-source / execution-venue plugin ids,
- the **assembler** turning a configuration into port instances
  (`MarketDataPort` / `ExecutionPort` from
  [pulsar-contracts](https://github.com/vanzeph/pulsar-contracts)),
- the **RunManifest** archive written for every run (configuration snapshot,
  resolved plugin ids, data watermarks, code versions, seed),
- the **CLI skeleton** with `research` / `paper` / `live` subcommands.

It deliberately contains **no domain logic and no port implementations**;
adapters live in their own repositories (`pulsar-data`, `pulsar-exec`) and
register themselves here.

## Installation

```bash
pip install .
# development
pip install -e .[dev]
```

Python >= 3.11. Depends on `pydantic` and `pulsar-contracts` only.

> **New here?** [`docs/QUICKSTART.md`](docs/QUICKSTART.md) walks a fresh
> machine from a clean venv through data ingestion, a first experiment and
> the report/dashboard; [`tools/drill_local.sh`](tools/drill_local.sh) is
> that flow as one repeatable script, and
> [`docs/WINDOWS-READINESS.md`](docs/WINDOWS-READINESS.md) is the Windows
> portability review (deployment steps included).

## Run configuration

```toml
[run]
mode = "research"                 # research | paper | live

[data]
sources = ["akshare", "baostock"] # primary/backup order
lake_dir = "./data/lake"

[exec]
venue = "backtest"                # backtest | paper | miniqmt

[exec.miqmt]                      # live venue only; locked by default
unlock_env = "PULSAR_LIVE_CONFIRM"
max_order_value_cny = 50000.0
```

Validation highlights:

- `run.mode` pairs with the venue exactly as documented by the architecture
  baseline (`research`/`backtest`, `paper`/`paper`, `live`/`miniqmt`);
- data sources are ordered primary/backup and must be unique;
- `[data.<source>]` / `[exec.<venue>]` hold plugin parameters;
- `[exec.miqmt]` is the short section name used by the architecture
  baseline's assembly example; it configures the `miniqmt` venue (`[exec.miniqmt]`
  is accepted and equivalent — configuring both is rejected).

### Security baseline

- **Credentials are referenced by environment-variable name only.** Any
  credential-like parameter (`token`, `password`, `api_key`, ...) must be
  written `"${ENV_NAME}"`; a literal value is rejected at parse time.
- Strings embedding userinfo in a URL (`scheme://user:pass@host`) are
  rejected everywhere.
- `live` mode is locked by default: assembly proceeds only when the
  environment variable named by `[exec.miqmt].unlock_env` is explicitly
  confirmed (`1` / `true` / `yes` / `on`), and the venue carries order-value
  caps.
- The RunManifest stores configuration *references* (`${ENV_NAME}`), never
  resolved secret values.

## Plugin registry

Adapters declare a stable lowercase identifier; the assembler resolves it:

```python
from pulsar_app import PluginKind, PluginRegistry, PluginSpec

registry = PluginRegistry()
registry.register(PluginSpec(
    plugin_id="akshare",
    kind=PluginKind.MARKET_DATA,
    factory=lambda **params: AkshareAdapter(**params),   # lives in pulsar-data
))
```

Market-data factories are called with their resolved parameters plus the
shared `lake_dir`; execution factories receive their venue table
(`unlock_env` and caps included for `miniqmt`).

## Assembly and RunManifest

```python
from pulsar_app import load_config, execute_run

config = load_config("examples/runs/dualma.toml")
outcome = execute_run(config, registry, runs_dir="runs")
print(outcome.manifest_path)   # runs/<run_id>/manifest.json
```

`execute_run` assembles the ports (protocol conformance is verified against
the contracts), enforces the live gate, collects data watermarks from ports
that implement the optional `watermark()` protocol and writes the manifest
atomically. The manifest carries a `config_fingerprint` (SHA-256 over the
canonical configuration snapshot plus the seed) — the identity
reproducibility is judged against.

## CLI

```bash
pulsar research --config runs/dualma.toml [--runs-dir runs]
pulsar paper   --config runs/paper.toml
pulsar live    --config runs/live.toml      # requires the unlock env var
```

Exit codes: `0` success, `1` unexpected error, `2` configuration error,
`3` live mode locked, `4` plugin resolution failure.

The subcommand injects `run.mode`; a configuration declaring a different
mode is rejected. The CLI resolves plugins against the process-wide default
registry — import your adapter packages first to populate it.

## Repository layout

Single package `pulsar_app` (architecture baseline: every repository owns
exactly one package):

```
src/pulsar_app/
    config.py      # TOML schema, parsing, credential baseline
    registry.py    # plugin registry
    assembly.py    # assembler, live gate, watermarks
    manifest.py    # RunManifest, fingerprints, code versions
    run.py         # one-run lifecycle skeleton (dry run + archive)
    cli.py         # research / paper / live
examples/runs/     # example configurations
tests/             # pytest suite (mock ports live here, never in the package)
tests/e2e/         # free-source end-to-end acceptance chain (task A9)
tools/             # offline maintenance scripts
```

## End-to-end acceptance chain (free data sources)

`tests/e2e/` holds the cross-repo acceptance suite proving the delivered
stack assembles into one **offline** chain over the free sources
(AkShare primary, baostock backup):

```
fixtures (recorded once from the live free endpoints)
  -> akshare adapter -> ingestion pipeline -> temp lake
       (completeness: zero unexplained bar gaps vs the calendar)
  -> baostock backup path (cross-validation + router failover drill)
  -> experiment TOML -> C3 factor pipeline (training)
  -> research backtest over BacktestVenue (fills, fees, T+1)
  -> paper session over PaperBroker (snapshots synthesized from bars)
  -> run artifacts (run_manifest.json + events.parquet + metrics_report.json)
  -> reproducibility: same config twice -> bit-identical manifests,
     journals, fills and net-value curves
```

The suite asserts **engineering structure only** — no return / Sharpe /
drawdown threshold appears anywhere (that exclusion is deliberate and
documented in the test module header).

Install and run:

```bash
pip install -e ".[dev,e2e]"     # pins the delivered pulsar-core/data/exec heads
python -m pytest tests/e2e      # offline, seconds, never touches the network
```

Fixtures live in `tests/e2e/fixtures/free-sources/` (recorded raw frames:
25 CSI300-sample symbols over 2024 including one 2024 dividend+conversion
sample and one 2024 IPO, plus 6 dual-source baostock recordings). To
re-record them from the live endpoints — manual, network required, CI
never does this:

```bash
pip install -e ".[e2e-recorder]"
python tools/make_e2e_fixtures.py --start 2024-01-01 --end 2024-12-31
```

The script re-runs the offline self-checks after recording (zero gaps,
dividend/split coverage, akshare/baostock close agreement) so a fresh
recording either keeps the properties the suite asserts or fails loudly.

## License

MIT.
