"""Mock port implementations used exclusively by the test suite.

These stubs implement the ``pulsar-contracts`` port protocols with the
least possible behaviour and are never shipped inside the package: plugin
implementations belong to ``pulsar-data`` / ``pulsar-exec``.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Callable

from pandas import DataFrame
from pulsar_contracts import (
    SHANGHAI_TZ,
    AdjustMode,
    CancelResult,
    ExecutionEvent,
    Freq,
    OrderId,
    OrderIntent,
    OrderState,
    OrderStatus,
    Position,
    Snapshot,
    Subscription,
)

#: Canonical bar columns promised by MarketDataPort.fetch_bars.
BAR_COLUMNS = (
    "symbol",
    "ts",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "adjust_factor",
    "quality",
)


class _NullSubscription:
    """Subscription handle that never streamed anything; unsubscribe is idempotent."""

    def unsubscribe(self) -> None:
        return None


class MockMarketDataPort:
    """Minimal MarketDataPort stub: empty answers, recorded kwargs, fixed watermark."""

    def __init__(self, *, lake_dir: str | None = None, **params: Any) -> None:
        self.lake_dir = lake_dir
        self.params = params
        self.calls: list[str] = []

    def list_instruments(self, as_of: date) -> list[Any]:
        self.calls.append("list_instruments")
        return []

    def fetch_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        freq: Freq,
        adjust: AdjustMode,
    ) -> DataFrame:
        self.calls.append("fetch_bars")
        return DataFrame(columns=list(BAR_COLUMNS))

    def fetch_corporate_actions(self, symbol: str) -> list[Any]:
        self.calls.append("fetch_corporate_actions")
        return []

    def calendar(self, start: date, end: date) -> list[date]:
        self.calls.append("calendar")
        return []

    def subscribe(
        self, symbols: list[str], on_snapshot: Callable[[Snapshot], None]
    ) -> Subscription:
        self.calls.append("subscribe")
        return _NullSubscription()

    def watermark(self) -> dict[str, date]:
        return {"daily_bars": date(2026, 9, 30)}


class MockExecutionPort:
    """Minimal ExecutionPort stub: accepted orders, recorded kwargs and callbacks."""

    def __init__(self, **params: Any) -> None:
        self.params = params
        self.submitted: list[OrderIntent] = []
        self.callbacks: list[Callable[[ExecutionEvent], None]] = []

    def submit(self, intent: OrderIntent) -> OrderId:
        self.submitted.append(intent)
        return OrderId(f"mock-{intent.idempotency_key.to_str()}")

    def cancel(self, order_id: OrderId) -> CancelResult:
        return CancelResult(order_id=order_id, accepted=True)

    def query(self, order_id: OrderId) -> OrderState:
        return OrderState(
            order_id=order_id,
            status=OrderStatus.CREATED,
            filled_quantity=0,
            updated_at=datetime.now(SHANGHAI_TZ),
        )

    def positions(self) -> list[Position]:
        return []

    def on_event(self, callback: Callable[[ExecutionEvent], None]) -> None:
        self.callbacks.append(callback)


class PlainMockMarketDataPort:
    """MarketDataPort stub that does *not* report watermarks."""

    def list_instruments(self, as_of: date) -> list[Any]:
        return []

    def fetch_bars(
        self,
        symbols: list[str],
        start: date,
        end: date,
        freq: Freq,
        adjust: AdjustMode,
    ) -> DataFrame:
        return DataFrame(columns=list(BAR_COLUMNS))

    def fetch_corporate_actions(self, symbol: str) -> list[Any]:
        return []

    def calendar(self, start: date, end: date) -> list[date]:
        return []

    def subscribe(
        self, symbols: list[str], on_snapshot: Callable[[Snapshot], None]
    ) -> Subscription:
        return _NullSubscription()


def mock_market_data_factory(**kwargs: Any) -> MockMarketDataPort:
    """Factory matching the assembler's calling convention for data plugins."""
    return MockMarketDataPort(**kwargs)


def mock_execution_factory(**kwargs: Any) -> MockExecutionPort:
    """Factory matching the assembler's calling convention for venue plugins."""
    return MockExecutionPort(**kwargs)
