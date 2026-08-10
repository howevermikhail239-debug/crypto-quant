import json
import logging
from pathlib import Path

import pytest

from crypto_quant.cli import main
from crypto_quant.logging import configure_logging, log_event
from crypto_quant.paths import initialize_data_root, resolve_data_root


def test_data_root_rejects_repository_path() -> None:
    with pytest.raises(ValueError):
        resolve_data_root("data", allow_inside_repository=False)


def test_data_root_initializes_runtime_tree(tmp_path: Path) -> None:
    created = initialize_data_root(tmp_path / "external")
    assert {path.name for path in created} == {
        "raw",
        "normalized",
        "quarantine",
        "spool",
        "logs",
        "manifests",
        "checkpoints",
        "gap_registry",
        "deletion_ledger",
        "schema_registry",
    }


def test_json_log_redacts_secret(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging(level="INFO", redact_keys={"api_key"}, secret_values=("message-secret",))
    log_event(
        logging.getLogger("test"),
        logging.INFO,
        "message-secret",
        component="test",
        event="redaction_test",
        api_key="context-secret",
        safe="ok",
    )
    event = json.loads(capsys.readouterr().err)
    assert event["timestamp_utc"].endswith("Z")
    assert event["component"] == "test"
    assert event["event"] == "redaction_test"
    assert event["message"] == "***REDACTED***"
    assert event["context"]["api_key"] == "***REDACTED***"
    assert event["context"]["safe"] == "ok"


def test_json_log_redacts_secret_under_unexpected_context_key(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging(level="INFO", redact_keys={"api_key"}, secret_values=("hidden-value",))
    log_event(
        logging.getLogger("test"),
        logging.INFO,
        "safe",
        component="test",
        event="redaction_test",
        unexpected="prefix-hidden-value-suffix",
    )
    event = json.loads(capsys.readouterr().err)
    assert "hidden-value" not in event["context"]["unexpected"]


def test_cli_config_check() -> None:
    assert main(["config-check"]) == 0
