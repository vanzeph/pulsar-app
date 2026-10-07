"""Write-time validation: syntax compilation, the import whitelist and
the experiment-document checks. All static — pulsar-core is not needed.
"""

from __future__ import annotations

import pytest

from pulsar_app.errors import StoreValidationError
from pulsar_app.store import (
    DEFAULT_IMPORT_WHITELIST,
    HARD_DENIED_IMPORTS,
    check_code_source,
    check_experiment_document,
    resolve_import_whitelist,
)

# -- code: syntax ----------------------------------------------------------------


def test_syntax_errors_are_rejected_readably() -> None:
    with pytest.raises(StoreValidationError, match="does not compile.*line 1"):
        check_code_source("def broken(:\n    pass\n", origin="code/x")


def test_valid_python_passes_the_static_checks() -> None:
    check_code_source(
        "import math\nimport numpy as np\nfrom pulsar_core import register_factor\n"
        "value = math.sqrt(4.0)\n",
        origin="code/x",
    )


# -- code: import whitelist --------------------------------------------------------


@pytest.mark.parametrize(
    "source",
    [
        "import os\n",
        "import os.path\n",
        "from os import system\n",
        "import subprocess\n",
        "import socket\n",
        "import urllib.request\n",
        "import requests\n",
        "import sys\n",
        "import pickle\n",
        "import ctypes\n",
        "import shutil\n",
        "import pathlib\n",
        "import threading\n",
        "import importlib\n",
    ],
    ids=lambda source: source.strip().split()[1].split(".")[0],
)
def test_dangerous_imports_are_denied(source: str) -> None:
    with pytest.raises(StoreValidationError, match="hard deny list"):
        check_code_source(source, origin="code/x")


def test_non_whitelisted_imports_are_denied_with_the_allowed_list() -> None:
    with pytest.raises(StoreValidationError, match="not on the import whitelist"):
        check_code_source("import sklearn\n", origin="code/x")


def test_relative_imports_are_denied() -> None:
    with pytest.raises(StoreValidationError, match="relative imports"):
        check_code_source("from . import sibling\n", origin="code/x")


@pytest.mark.parametrize("call", ["__import__", "eval", "exec", "compile"])
def test_dynamic_execution_calls_are_denied(call: str) -> None:
    with pytest.raises(StoreValidationError, match=rf"{call}\(\) is not allowed"):
        check_code_source(f"result = {call}('1 + 1')\n", origin="code/x")


def test_whitelist_env_override_extends_and_hard_deny_wins() -> None:
    override = {"PULSAR_STORE_IMPORT_WHITELIST": "mymath,numpy"}
    resolved = resolve_import_whitelist(override)
    assert resolved == {"mymath", "numpy"}  # replaces the default
    assert "mymath" in resolve_import_whitelist(override)

    # a site cannot re-enable the dangerous surface via the env var
    hostile = {"PULSAR_STORE_IMPORT_WHITELIST": "os,subprocess,numpy"}
    assert resolve_import_whitelist(hostile) == {"numpy"}

    # empty/absent override keeps the documented default
    assert resolve_import_whitelist({}) == DEFAULT_IMPORT_WHITELIST
    assert not (DEFAULT_IMPORT_WHITELIST & HARD_DENIED_IMPORTS)


def test_default_whitelist_covers_the_documented_surface() -> None:
    for module in ("numpy", "pandas", "math", "typing", "pulsar_core", "pulsar_contracts"):
        assert module in DEFAULT_IMPORT_WHITELIST


# -- experiments ----------------------------------------------------------------


def _experiment_toml(status: str = "candidate", extra: str = "") -> str:
    return (
        '[experiment]\nid = "demo"\n'
        f'status = "{status}"\n'
        "[universe]\nsymbols = [\"SH600519\"]\n"
        "[factors]\nnames = [\"momentum_20\"]\n"
        "[model]\ntype = \"equal_weight\"\n"
        "[portfolio]\nmethod = \"top_n\"\ntop_n = 1\nrebalance = \"monthly\"\n"
        "[backtest]\nstart = 2024-01-01\nend = 2024-03-01\ncosts = \"a_share_default\"\n"
        f"{extra}"
    )


def test_valid_experiment_document_passes() -> None:
    check_experiment_document(_experiment_toml(), origin="experiments/demo")


def test_invalid_toml_is_rejected() -> None:
    with pytest.raises(StoreValidationError, match="invalid TOML"):
        check_experiment_document("[experiment\nid = ", origin="experiments/demo")


def test_missing_experiment_section_is_rejected() -> None:
    with pytest.raises(StoreValidationError, match=r"\[experiment\] section"):
        check_experiment_document("id = 'orphan'\n", origin="experiments/demo")


def test_missing_id_is_rejected() -> None:
    document = _experiment_toml().replace('id = "demo"\n', "")
    with pytest.raises(StoreValidationError, match="id must be a non-empty string"):
        check_experiment_document(document, origin="experiments/demo")


@pytest.mark.parametrize("status", ["shipped", "live", ""])
def test_non_lifecycle_status_is_rejected(status: str) -> None:
    document = _experiment_toml(status=status)
    with pytest.raises(StoreValidationError, match="status must be one of"):
        check_experiment_document(document, origin="experiments/demo")


@pytest.mark.parametrize("status", ["candidate", "active", "retired"])
def test_all_lifecycle_statuses_pass(status: str) -> None:
    check_experiment_document(_experiment_toml(status=status), origin="experiments/demo")


def test_plaintext_credentials_are_rejected_in_experiment_documents() -> None:
    document = _experiment_toml(
        extra='api_key = "sk-live-123"\n'
    )
    with pytest.raises(StoreValidationError, match="credential-like parameter"):
        check_experiment_document(document, origin="experiments/demo")
