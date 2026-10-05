"""Collection gate for the e2e suite.

The base pulsar-app CI job installs only ``.[dev]`` (pulsar-app plus
pulsar-contracts); the e2e chain additionally needs the pulsar-core /
pulsar-data / pulsar-exec stack (the ``e2e`` extra). When those are
absent, collection of this directory skips cleanly instead of erroring;
the dedicated e2e job installs the extras and runs the whole chain.
"""

from __future__ import annotations

import pytest

for module, why in (
    ("pulsar_core", "install pulsar-app[e2e] to run the free-source e2e chain"),
    ("pulsar_data", "install pulsar-app[e2e] to run the free-source e2e chain"),
    ("pulsar_exec", "install pulsar-app[e2e] to run the free-source e2e chain"),
):
    pytest.importorskip(module, reason=why)
