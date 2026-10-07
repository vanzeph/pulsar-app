"""Example custom factor written for the unified store (task STORE1).

This file is the store's example *Agent write*: it is pushed into the
``code`` namespace with ``pulsar store put`` (or the Python API),
survives the put-time validation (syntax compile, import whitelist,
registration-name preview) and is materialized + registered into the
pulsar-core factor registry at assembly time — see
``docs/QUICKSTART.md`` §5 and ``tests/store/test_store_assembly.py``.

Note the import surface: only whitelist modules (``pulsar_core`` public
API included). ``os`` / ``subprocess`` / network modules are denied
outright — the put is rejected before anything is stored.
"""

from __future__ import annotations

from typing import Sequence

from pulsar_contracts import Bar
from pulsar_core.factors import FactorDefinition, register_factor


def compute_close_over_ma10(bars: Sequence[Bar]) -> float | None:
    """Close over its trailing 10-bar mean, minus 1 (higher = stronger)."""
    if len(bars) < 10:
        return None  # insufficient history is missing data, never a guess
    window = bars[-10:]
    mean = sum(bar.close for bar in window) / len(window)
    if mean <= 0:
        return None
    return window[-1].close / mean - 1.0


register_factor(
    FactorDefinition(
        name="close_over_ma10",
        label="close / MA10 - 1 (store example)",
        compute=compute_close_over_ma10,
        direction=1,
        min_bars=10,
    )
)
