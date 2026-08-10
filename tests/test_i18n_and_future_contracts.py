import json
from pathlib import Path

import pytest

from crypto_quant.i18n import Translator, load_catalog, placeholders
from crypto_quant.lifecycle import validate_revision_chain


def test_locale_catalogs_have_exact_keys_and_matching_interpolation() -> None:
    locale_dir = Path("locales")
    ru = load_catalog(locale_dir / "ru-RU.yaml")
    en = load_catalog(locale_dir / "en-US.yaml")
    assert set(ru) == set(en)
    assert {key: placeholders(value) for key, value in ru.items()} == {
        key: placeholders(value) for key, value in en.items()
    }


def test_translator_falls_back_and_validates_interpolation() -> None:
    catalogs = {
        "ru-RU": load_catalog(Path("locales/ru-RU.yaml")),
        "en-US": load_catalog(Path("locales/en-US.yaml")),
    }
    translator = Translator(catalogs, fallback_locale="en-US")
    assert translator.translate("signal.no_trade", locale="de-DE") == "No trade"
    assert translator.translate("signal.risk_label", locale="ru-RU", risk_class="Высокий")
    with pytest.raises(ValueError):
        translator.translate("signal.risk_label", locale="ru-RU")


def test_draft_future_signal_contract_reserves_separate_risk_and_exit_fields() -> None:
    schema = json.loads(Path("schemas/future_signal.schema.json").read_text(encoding="utf-8"))
    properties = schema["properties"]
    assert schema["$id"].endswith("/0.2.0-draft")
    assert schema["properties"]["schema_version"]["const"] == "0.2.0-draft"
    assert schema["title"].startswith("Future Canonical Signal and Trade Lifecycle")
    assert "model_confidence" in properties
    assert "trade_risk" in properties
    assert "trade_risk_score" in properties
    assert "expected_value_after_costs" in properties
    assert "invalidation" in properties
    assert "stop_loss" in properties
    assert {
        "revision_id",
        "parent_revision_id",
        "revision_number",
        "revision_timestamp",
    } <= set(properties)
    assert {"signal_created_at", "next_review_at", "scenario_expiry_at"} <= set(
        properties["time_context"]["properties"]
    )
    assert "TP3_REACHED" in properties["scenario_state"]["enum"]
    assert properties["stop_loss"]["properties"]["action"]["enum"] == [
        "KEEP_STOP", "TIGHTEN_STOP", "MOVE_TO_BREAK_EVEN", "TRAIL_STOP", "CANCEL_STOP_AND_EXIT"
    ]
    target = schema["$defs"]["target"]["properties"]
    assert {"target_id", "scenario_role", "status"} <= set(target)
    assert {
        "target_progress",
        "management_mode",
        "position_action",
        "localized_explanation_key",
        "revision_changes",
        "directional_probabilities",
    } <= set(properties)
    assert "polymarket_context" in properties
    assert properties["polymarket_context"]["description"].startswith("Reserved optional")
    assert len(schema["allOf"]) == 4


def test_phase_3f_order_and_phase_0_status_are_preserved() -> None:
    companion = Path("crypto_quant_phased_development_prompt.md").read_text(encoding="utf-8")
    phase_3e = companion.index("# 16. PHASE 3E")
    phase_3f = companion.index("## PHASE 3F")
    stop_point = companion.index("# 17. STOP POINT №3")
    assert phase_3e < phase_3f < stop_point

    design = Path("crypto_quant_revised_technical_design.md").read_text(encoding="utf-8")
    assert "PHASE 0 complete" in design
    assert "The next implementation task is PHASE 1C" in design


def _revision(*, number: int, state: str, stop: str, next_review: str | None) -> dict[str, object]:
    return {
        "signal_id": "btc-long-1",
        "revision_id": f"revision-{number}",
        "parent_revision_id": None if number == 1 else f"revision-{number - 1}",
        "revision_number": number,
        "revision_timestamp": f"2026-08-10T0{number}:00:00Z",
        "knowledge_time": f"2026-08-10T0{number}:00:00Z",
        "decision": "LONG",
        "scenario_state": state,
        "target_progress": "NONE",
        "stop_loss": {"price": stop, "action": "TIGHTEN_STOP", "method_version": "m1"},
        "time_context": {
            "signal_created_at": "2026-08-10T01:00:00Z",
            "scenario_expiry_at": "2026-08-11T01:00:00Z",
            "next_review_at": next_review,
        },
    }


def test_lifecycle_revision_validator_accepts_ordered_chain_and_rejects_widening() -> None:
    first = _revision(number=1, state="NEW", stop="100", next_review="2026-08-10T01:15:00Z")
    second = _revision(number=2, state="ACTIVE", stop="101", next_review="2026-08-10T02:15:00Z")
    first["target_progress"] = "NONE"
    second["target_progress"] = "TP1"
    validate_revision_chain([first, second])

    widened = _revision(number=2, state="ACTIVE", stop="99", next_review="2026-08-10T02:15:00Z")
    with pytest.raises(ValueError, match="widened"):
        validate_revision_chain([first, widened])


def test_lifecycle_revision_validator_rejects_terminal_review() -> None:
    initial = _revision(number=1, state="NEW", stop="100", next_review="2026-08-10T01:15:00Z")
    terminal = _revision(number=2, state="CLOSED", stop="101", next_review="2026-08-10T02:15:00Z")
    with pytest.raises(ValueError, match="terminal"):
        validate_revision_chain([initial, terminal])


def test_lifecycle_revision_validator_rejects_time_and_target_regressions() -> None:
    first = _revision(number=1, state="NEW", stop="100", next_review="2026-08-10T01:15:00Z")
    first["target_progress"] = "TP1"
    second = _revision(number=2, state="ACTIVE", stop="101", next_review="2026-08-10T02:15:00Z")
    second["target_progress"] = "NONE"
    with pytest.raises(ValueError, match="target_progress"):
        validate_revision_chain([first, second])

    first["target_progress"] = "NONE"
    second["knowledge_time"] = "2026-08-10T02:01:00Z"
    with pytest.raises(ValueError, match="knowledge_time"):
        validate_revision_chain([first, second])
