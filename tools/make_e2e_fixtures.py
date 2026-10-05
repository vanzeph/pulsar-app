"""Record the free-source e2e fixtures (AkShare primary + baostock backup).

Run manually on a machine with network access to the upstream free
endpoints (Eastmoney/Sina via akshare, baostock); requires the source
SDKs::

    python -m venv .venv && .venv/bin/pip install -e .[e2e-recorder]
    .venv/bin/python tools/make_e2e_fixtures.py \
        --out tests/e2e/fixtures/free-sources \
        --start 2024-01-01 --end 2024-12-31

CI never runs this script: the e2e suite replays the committed fixtures
fully offline.  The script records *raw* frames exactly as the live
clients return them (same reliability plumbing as production: rate
limiting, retry/backoff, circuit breaker, egress guard):

* ``akshare/`` — layout consumed by
  ``pulsar_data.sources.akshare.client.FixtureAkShareClient``:
  calendar, universe snapshot, suspensions, per-symbol daily bars
  (raw + hfq from one endpoint so the adjustment-factor anchor stays
  consistent), dividend and rights-issue detail;
* ``baostock/`` — raw ``query_history_k_data_plus`` frames (adjustflag 3
  raw / 1 hfq) for a dual-source sample plus the trade calendar, replayed
  by the fixture client defined in ``tests/e2e/harness.py``.

After recording, the script re-runs the ingestion pipeline *offline*
against what it just wrote and fails loudly unless the fixture set keeps
the properties the e2e suite asserts: zero unexplained bar gaps over the
window, at least one 2024 cash-dividend sample and one 2024
dividend-plus-conversion (送转) sample, and akshare/baostock close-price
agreement within the cross-check tolerance.

Both sources are free and credential-less (no secrets are read, stored
or printed).
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from pulsar_data.backfill import BackfillRunner
from pulsar_data.crosscheck import CrossValidator
from pulsar_data.lake import DataLake
from pulsar_data.sources.akshare.client import LiveAkShareClient
from pulsar_data.sources.baostock.adapter import BaostockSourceAdapter
from pulsar_data.sources.baostock.client import LiveBaostockClient, to_baostock_code
from pulsar_data.sources.registry import get_adapter

# ---------------------------------------------------------------------------
# Fixed sample: a CSI300 cross-section (main SH/SZ, GEM, STAR; different
# sectors) plus the two deliberate special cases:
#   * SZ300896 — 2024-04-29 ex-date carrying BOTH a cash dividend and a
#     conversion (10转4派23.23), the dividend/split sample of the e2e suite;
#   * SZ301536 — listed 2024-03, exercising the pre-IPO "not listed"
#     completeness classification against real upstream data.
SAMPLE_SYMBOLS = [
    "SH600519",  # Kweichow Moutai (2024 cash dividends)
    "SH600036",  # China Merchants Bank
    "SH601318",  # China Ping An
    "SH600276",  # Hengrui Medicine
    "SH601899",  # Zijin Mining
    "SH603288",  # Haitian Flavouring
    "SH601127",  # Seres
    "SH600900",  # China Yangtze Power
    "SH601088",  # China Shenhua
    "SH601012",  # LONGi Green Energy
    "SZ000001",  # Ping An Bank
    "SZ000858",  # Wuliangye
    "SZ002415",  # Hikvision
    "SZ002594",  # BYD
    "SZ000651",  # Gree Electric
    "SZ003816",  # CGN Power
    "SZ000333",  # Midea Group
    "SZ300750",  # CATL (GEM)
    "SZ300059",  # East Money (GEM)
    "SZ300760",  # Mindray Medical (GEM)
    "SZ300896",  # Aimeike (GEM) — 2024-04-29 10转4派23.23 送转 sample
    "SH688981",  # SMIC (STAR)
    "SH688111",  # Kingsoft Office (STAR)
    "SH688012",  # AMEC (STAR)
    "SZ301536",  # 2024-03 IPO (GEM) — pre-IPO gap classification sample
]

# Dual-source (baostock backup) coverage sample: exchange/board spread
# plus the dividend/split and IPO special cases.
BAOSTOCK_SAMPLE = [
    "SH600519",
    "SZ000001",
    "SZ300750",
    "SH688111",
    "SZ300896",
    "SZ301536",
]


def _exists(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 0


def record_akshare(out: Path, start: date, end: date, pause: float) -> dict[str, object]:
    """Record the akshare-side raw frames (production client code path)."""
    (out / "bars").mkdir(parents=True, exist_ok=True)
    (out / "dividends").mkdir(exist_ok=True)
    (out / "rights").mkdir(exist_ok=True)
    client = LiveAkShareClient(min_interval=0.3, retries=2)

    if not _exists(out / "calendar.csv"):
        client.trade_dates().to_csv(out / "calendar.csv", index=False)
    if not _exists(out / "universe.csv"):
        client.universe().to_csv(out / "universe.csv", index=False)
    if not _exists(out / "suspensions.csv"):
        client.suspensions(start).to_csv(out / "suspensions.csv", index=False)
    print(
        "akshare calendar/universe/suspensions:",
        len(pd.read_csv(out / "calendar.csv")),
        len(pd.read_csv(out / "universe.csv")),
        len(pd.read_csv(out / "suspensions.csv")),
    )

    failures: list[str] = []
    for symbol in SAMPLE_SYMBOLS:
        code = symbol[2:]
        try:
            if not _exists(out / f"bars/{symbol}.raw.csv") or not _exists(
                out / f"bars/{symbol}.hfq.csv"
            ):
                raw, hfq = client.daily_bars_pair(code, start, end)
                raw.to_csv(out / f"bars/{symbol}.raw.csv", index=False)
                hfq.to_csv(out / f"bars/{symbol}.hfq.csv", index=False)
            if not _exists(out / f"dividends/{symbol}.csv"):
                client.dividend_detail(code).to_csv(
                    out / f"dividends/{symbol}.csv", index=False
                )
            if not _exists(out / f"rights/{symbol}.csv"):
                client.rights_detail(code).to_csv(
                    out / f"rights/{symbol}.csv", index=False
                )
            bars = pd.read_csv(out / f"bars/{symbol}.raw.csv")
            print(f"recorded {symbol}: {len(bars)} raw bar rows")
        except Exception as exc:  # noqa: BLE001 - recording helper reports and continues
            print(f"FAILED {symbol}: {exc}")
            failures.append(symbol)
        time.sleep(pause)
    return {"failed_symbols": failures}


def record_baostock(out: Path, start: date, end: date, pause: float) -> dict[str, object]:
    """Record the baostock-side raw frames (production client code path)."""
    (out / "bars").mkdir(parents=True, exist_ok=True)
    with LiveBaostockClient(min_interval=0.3, retries=2) as client:
        if not _exists(out / "calendar.csv"):
            client.trade_dates(start, end).to_csv(out / "calendar.csv", index=False)
        calendar = pd.read_csv(out / "calendar.csv")
        print(f"baostock calendar: {len(calendar)} rows")

        failures: list[str] = []
        for symbol in BAOSTOCK_SAMPLE:
            code = to_baostock_code(symbol)
            try:
                if not _exists(out / f"bars/{symbol}.raw.csv") or not _exists(
                    out / f"bars/{symbol}.hfq.csv"
                ):
                    raw, hfq = client.daily_bars_pair(code, start, end)
                    raw.to_csv(out / f"bars/{symbol}.raw.csv", index=False)
                    hfq.to_csv(out / f"bars/{symbol}.hfq.csv", index=False)
                bars = pd.read_csv(out / f"bars/{symbol}.raw.csv")
                print(f"recorded {symbol}: {len(bars)} raw bar rows")
            except Exception as exc:  # noqa: BLE001 - recording helper
                print(f"FAILED {symbol}: {exc}")
                failures.append(symbol)
            time.sleep(pause)
        return {"failed_symbols": failures}


# ---------------------------------------------------------------------------
# Offline self-checks: the recorded fixture must keep the properties the
# e2e suite asserts, otherwise the suite would fail on regeneration.
# ---------------------------------------------------------------------------
def _fixture_baostock_adapter(root: Path) -> BaostockSourceAdapter:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "tests" / "e2e"))
    try:
        from harness import FixtureBaostockClient  # noqa: PLC0415
    finally:
        sys.path.pop(0)
    return BaostockSourceAdapter(client=FixtureBaostockClient(root))


def self_check(out: Path, start: date, end: date) -> None:
    akshare_root = out / "akshare"
    baostock_root = out / "baostock"

    # 1. full-sample backfill through the real pipeline, zero unexplained gaps
    from pulsar_data.schema import Dataset  # noqa: PLC0415

    adapter = get_adapter("akshare", {"fixture_dir": str(akshare_root)})
    with tempfile.TemporaryDirectory(prefix="e2e-fixture-selfcheck-") as tmp:
        lake = DataLake(Path(tmp) / "lake")
        report = BackfillRunner(adapter, lake).run(SAMPLE_SYMBOLS, start, end)
        assert not report.failed_symbols, f"fixture symbols failed: {report.failed_symbols}"
        assert report.unexplained_gaps == 0, (
            f"fixture has unexplained gaps (swap the offending symbol for one "
            f"with full {start.year} coverage): "
            f"{ {s: c for s, c in report.completeness.items() if c.get('gap', 0)} }"
        )
        print(f"self-check backfill: {report.bars_rows} bar rows, 0 unexplained gaps")

        # 2. corporate-action samples: >=1 cash dividend and >=1 送转 in window
        ca = lake.read(Dataset.CORPORATE_ACTIONS)
        ca["ex_date"] = pd.to_datetime(ca["ex_date"])
        in_window = ca[ca["ex_date"].between(pd.Timestamp(start), pd.Timestamp(end))]
        cash = in_window[in_window["cash_dividend_per_share"] > 0]
        bonus = in_window[in_window["bonus_share_ratio"] > 0]
        assert not cash.empty, "no cash-dividend sample inside the fixture window"
        assert not bonus.empty, "no 送转 (bonus/conversion) sample inside the fixture window"
        print(
            f"self-check corporate actions: {len(cash)} cash rows, "
            f"{len(bonus)} bonus rows on {sorted(set(bonus['symbol']))}"
        )

    # 3. dual-source cross-validation on the baostock sample
    secondary = _fixture_baostock_adapter(baostock_root)
    cross = CrossValidator(adapter, secondary)
    report_cross = cross.check(BAOSTOCK_SAMPLE, start, end)
    close_events = [event for event in report_cross.events if event.field == "close"]
    assert not close_events, (
        f"akshare/baostock closes disagree beyond tolerance: {close_events[:3]}"
    )
    print(
        f"self-check cross-validation: {len(report_cross.events)} quality events "
        f"({report_cross.total_mismatches} total mismatches incl. volume) over "
        f"{len(BAOSTOCK_SAMPLE)} symbols"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("tests/e2e/fixtures/free-sources"))
    parser.add_argument("--start", type=date.fromisoformat, default=date(2024, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2024, 12, 31))
    parser.add_argument("--pause", type=float, default=0.4, help="seconds between symbols")
    parser.add_argument(
        "--skip-record",
        action="store_true",
        help="only run the offline self-checks against existing recordings"
    )
    args = parser.parse_args()

    out = args.out
    if not args.skip_record:
        ak = record_akshare(out / "akshare", args.start, args.end, args.pause)
        bs = record_baostock(out / "baostock", args.start, args.end, args.pause)
        manifest = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "window": [args.start.isoformat(), args.end.isoformat()],
            "akshare": {
                "symbols": SAMPLE_SYMBOLS,
                "via": "pulsar_data LiveAkShareClient (eastmoney primary, sina fallback)",
                **ak,
            },
            "baostock": {
                "symbols": BAOSTOCK_SAMPLE,
                "via": "pulsar_data LiveBaostockClient (anonymous login)",
                **bs,
            },
        }
        (out / "manifest.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8"
        )
    self_check(out, args.start, args.end)
    print("fixtures OK")


if __name__ == "__main__":
    main()
