"""Offline harness for the free-source e2e suite (tests/e2e).

Three pieces of *assembly-level* glue the e2e suite needs but no domain
repo should own (pulsar-app is the assembler; its main package is still
the A2 dry-run skeleton, so the wiring lives here, next to the tests):

* :class:`FixtureBaostockClient` — replays the raw baostock frames
  recorded by ``tools/make_e2e_fixtures.py`` through the same narrow
  ``BaostockClient`` protocol :class:`~pulsar_data.sources.baostock.adapter.
  BaostockSourceAdapter` consumes, so the backup source runs its real
  ``fetch_raw → normalize`` path fully offline;
* :func:`bus_driven_backtest_venue` — an ``ExecutionPort`` factory wiring
  one :class:`~pulsar_exec.venue.BacktestVenue` onto a
  :class:`~pulsar_core.bus.EventBus`: bar events feed ``on_bar``, day
  rollovers call ``on_session_end`` (before the new day's bars reach the
  strategy), session finish closes the final day. This is exactly the
  "replay loop drives the venue" driving model of the execution design;
* :func:`synthesize_snapshots` — projects fixture bars onto realtime
  :class:`~pulsar_contracts.market_data.Snapshot` five-level books for
  the Paper session (the task mandate: snapshots synthesized from
  fixture bars, never from the network).

Nothing here touches the network and nothing mutates domain behaviour.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time
from pathlib import Path
from typing import Iterable, Sequence
from zoneinfo import ZoneInfo

import pandas as pd
from pulsar_contracts import (
    Bar,
    Board,
    Exchange,
    Freq,
    Instrument,
    QuoteLevel,
    Snapshot,
)
from pulsar_core.bus import EventBus
from pulsar_core.events import Event, EventKind, SessionPhase
from pulsar_exec import BacktestVenue

__all__ = [
    "FIXTURE_WINDOW",
    "FixtureBaostockClient",
    "bus_driven_backtest_venue",
    "snapshot_from_bar",
    "synthesize_snapshots",
]

#: The fixed e2e data window (matches the recorded fixtures).
FIXTURE_WINDOW: tuple[date, date] = (date(2024, 1, 1), date(2024, 12, 31))

_TZ = ZoneInfo("Asia/Shanghai")


# ---------------------------------------------------------------------------
# baostock fixture replay client
# ---------------------------------------------------------------------------
class FixtureBaostockClient:
    """Replay recorded raw baostock frames (fully offline).

    Files carry exactly the columns ``query_history_k_data_plus`` /
    ``query_trade_dates`` return (strings); dates are re-windowed per
    request exactly like a live query would.
    """

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        if not self.root.is_dir():
            raise FileNotFoundError(f"baostock fixture directory not found: {self.root}")

    def _read(self, relative: str) -> pd.DataFrame:
        path = self.root / relative
        if not path.exists():
            return pd.DataFrame()
        try:
            return pd.read_csv(path, dtype={"code": str})
        except pd.errors.EmptyDataError:
            return pd.DataFrame()

    @staticmethod
    def _window(frame: pd.DataFrame, column: str, start: date, end: date) -> pd.DataFrame:
        if frame.empty or column not in frame.columns:
            return frame
        dates = pd.to_datetime(frame[column], errors="coerce")
        keep = dates.dt.date.between(start, end) & dates.notna()
        return frame.loc[keep]

    def daily_bars_pair(
        self, code: str, start: date, end: date
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return the recorded ``(raw, hfq)`` k-data frames for one code."""
        from pulsar_data.sources.baostock.client import from_baostock_code

        symbol = from_baostock_code(code)
        raw = self._window(self._read(f"bars/{symbol}.raw.csv"), "date", start, end)
        hfq = self._window(self._read(f"bars/{symbol}.hfq.csv"), "date", start, end)
        return raw, hfq

    def minute_bars(
        self, code: str, start: date, end: date, frequency: str = "5"
    ) -> pd.DataFrame:
        raise NotImplementedError("minute bars are not part of the e2e fixture set")

    def adjust_factor_events(
        self, code: str, start: date, end: date
    ) -> pd.DataFrame:
        return pd.DataFrame()

    def trade_dates(self, start: date, end: date) -> pd.DataFrame:
        frame = self._read("calendar.csv")
        return self._window(frame, "calendar_date", start, end)

    def close(self) -> None:  # no live session to close
        return None


# ---------------------------------------------------------------------------
# backtest venue on the kernel bus
# ---------------------------------------------------------------------------
#: Wall time the venue closes its trading day at (execution-side convention:
#: A-share continuous close; after the day's bars, before the next session).
_SESSION_CLOSE = time(15, 0)

_CLOSE_TIMER = "venue.session_close"


@dataclass
class _VenueDriver:
    """Feeds one BacktestVenue from the kernel loop, day-close aware.

    Bars are consumed in two phases of one dispatch (daily bars mean one
    bar per symbol per day, and intents are ``DAY``-scoped, so an order
    can never survive to its symbol's next bar — it must be matchable on
    the bar it was submitted on):

    * *open* (kind handler, registered before the strategy runtime):
      ``venue.on_bar(bar)`` advances the venue clock to the bar and
      matches whatever rested from earlier dispatches;
    * *close* (all-handlers hook, running after every kind handler —
      i.e. after the strategy submitted on this bar): ``venue.on_bar``
      again; the venue's per-bar volume-participation bookkeeping makes
      the second pass a continuation, not a double fill.

    Day closes are scheduled *inside* the day's dispatch window: on the
    first bar of a trading day the driver publishes a TIMER event at
    15:00 Asia/Shanghai; when the kernel dispatches it (still within the
    day's window, which ends at the next day's 00:00) the venue closes
    the session — expiring DAY orders and rolling T+1 with event
    timestamps the deterministic clock accepts.
    """

    venue: BacktestVenue
    bus: EventBus
    current_day: date | None = None
    current_bar: Bar | None = None
    bars_fed: int = 0
    sessions_closed: int = 0

    def on_market(self, event: Event) -> None:
        """Kind handler (registered before the strategy): open the bar."""
        bar = event.bar
        assert bar is not None  # MARKET envelope invariant
        day = bar.ts.date()
        if day != self.current_day:
            self.current_day = day
            self.bus.publish(
                Event.timer_event(
                    datetime.combine(day, _SESSION_CLOSE, tzinfo=_TZ),
                    _CLOSE_TIMER,
                    {"day": day.isoformat()},
                )
            )
        self.current_bar = bar
        self.venue.on_bar(bar)
        self.bars_fed += 1

    def on_late(self, event: Event) -> None:
        """All-handlers hook (after the strategy acted): close the bar."""
        bar = self.current_bar
        if event.kind is EventKind.MARKET and event.bar is not None and event.bar is bar:
            self.venue.on_bar(bar)

    def on_event(self, event: Event) -> None:
        if event.kind is EventKind.TIMER and event.timer is not None:
            if event.timer.name == _CLOSE_TIMER:
                day = date.fromisoformat(str(event.timer.data["day"]))
                self.venue.on_session_end(day)
                self.sessions_closed += 1
            return
        if event.kind is EventKind.SESSION and event.session_phase is SessionPhase.FINISHED:
            if self.current_day is not None:
                # safety net for runs whose final window was cut before
                # 15:00; a second close of the same day is a no-op
                self.venue.on_session_end(self.current_day)
                self.sessions_closed += 1


def bus_driven_backtest_venue(
    bus: EventBus,
    *,
    initial_cash: float = 1_000_000.0,
    instruments: Iterable[Instrument] = (),
    start: date,
) -> BacktestVenue:
    """Build a :class:`BacktestVenue` driven by ``bus`` (see module docs).

    The subscription happens *inside the factory* — i.e. before
    :func:`~pulsar_core.runner.run_experiment` constructs its
    :class:`~pulsar_core.runtime.StrategyRuntime` — and as a *kind*
    handler (``subscribe_all`` handlers would run after every kind
    handler, letting the strategy act on a bar before the venue consumed
    it, with the venue's clock — and therefore its event timestamps —
    one bar behind). Kind handlers run in registration order, so on
    every bar the venue first matches resting orders, then the strategy
    decides against a venue whose clock already sits on this bar.
    """
    venue = BacktestVenue(
        initial_cash=initial_cash,
        instruments=instruments,
        clock=datetime.combine(start, time(0, 0), tzinfo=_TZ),
    )
    driver = _VenueDriver(venue=venue, bus=bus)
    bus.subscribe(EventKind.MARKET, driver.on_market)
    bus.subscribe(EventKind.TIMER, driver.on_event)
    bus.subscribe(EventKind.SESSION, driver.on_event)
    bus.subscribe_all(driver.on_late)
    venue.driver = driver  # type: ignore[attr-defined]  # test introspection
    return venue


# ---------------------------------------------------------------------------
# snapshot synthesis from fixture bars
# ---------------------------------------------------------------------------
def snapshot_from_bar(
    bar: Bar,
    *,
    seq: int,
    at: datetime,
    spread_bps: float = 20.0,
    level_volume: float = 100_000.0,
) -> Snapshot:
    """Project one historical bar onto a five-level realtime book.

    The book is synthetic but *derived from real fixture prices*: the
    mid sits at ``close`` with a symmetric spread, so the PaperBroker's
    strict matching (fills only against strictly-penetrating levels)
    exercises its real price-boundary logic against fixture data.
    """
    step = round(bar.close * spread_bps / 1e4, 2)
    asks = tuple(
        QuoteLevel(price=round(bar.close + step * (index + 1), 2), volume=level_volume)
        for index in range(5)
    )
    bids = tuple(
        QuoteLevel(price=round(max(bar.close - step * (index + 1), 0.01), 2), volume=level_volume)
        for index in range(5)
    )
    return Snapshot(
        symbol=bar.symbol,
        ts=at,
        seq=seq,
        last_price=bar.close,
        volume=bar.volume,
        amount=bar.amount,
        bids=bids,
        asks=asks,
    )


def synthesize_snapshots(
    bars: Sequence[Bar],
    *,
    per_day_times: Sequence[time] = (time(9, 30), time(10, 30), time(14, 0), time(14, 55)),
    seq_start: int = 0,
) -> list[Snapshot]:
    """Build a deterministic snapshot stream from daily bars.

    Four books per trading day; ``seq`` is strictly increasing per symbol
    across the *whole* stream (the semantics ``PaperBroker.on_snapshot``
    expects: late/duplicate seq dropped, gaps tolerated) — synthesize
    each session's slice from one call, or carry ``seq_start`` forward
    between calls feeding the same broker session.
    """
    counters: dict[str, int] = {symbol: seq_start for symbol in {bar.symbol for bar in bars}}
    out: list[Snapshot] = []
    for bar in bars:
        for moment in per_day_times:
            counters[bar.symbol] = counters.get(bar.symbol, seq_start) + 1
            at = datetime.combine(bar.ts.date(), moment, tzinfo=_TZ)
            out.append(snapshot_from_bar(bar, seq=counters[bar.symbol], at=at))
    return out


def instruments_from_port(
    port, symbols: Sequence[str], as_of: date
) -> list[Instrument]:
    """Read ``Instrument`` metadata through the MarketDataPort (lake).

    Symbols the lake has no metadata for get the healthy main-board
    fallback (same convention the venue itself applies internally).
    """
    listed = {instrument.symbol: instrument for instrument in port.list_instruments(as_of)}
    out: list[Instrument] = []
    for symbol in symbols:
        instrument = listed.get(symbol)
        if instrument is None:
            instrument = Instrument(
                symbol=symbol,
                exchange=Exchange.SSE,
                board=Board.MAIN,
                list_date=date(1990, 12, 19),
            )
        out.append(instrument)
    return out


def bars_from_frame(frame: pd.DataFrame) -> list[Bar]:
    """Frame → validated ``Bar`` list (same conversion the core session uses)."""
    from pulsar_core.session import _bars_from_frame

    return list(_bars_from_frame(frame, Freq.DAILY))
