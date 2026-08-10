import json
from pathlib import Path

from crypto_quant.cli import _status_exit, health_checks
from crypto_quant.config import load_config
from crypto_quant.contracts import (
    Checkpoint,
    DataContract,
    DeletionRecord,
    GapRecord,
    ManifestEvent,
)
from crypto_quant.paths import initialize_data_root


def test_health_is_green_after_empty_runtime_tree(tmp_path: Path) -> None:
    config = load_config(Path("config/default.yaml"))
    storage = config.storage.model_copy(update={"data_root": str(tmp_path / "data")})
    config = config.model_copy(update={"storage": storage})
    initialize_data_root(storage.resolved_data_root())
    statuses = {item.name: item.status for item in health_checks(config)}
    assert statuses["data_root"] == "PASS"
    assert statuses["atomic_rename"] == "PASS"
    assert statuses["growth_projections"] == "UNKNOWN"
    assert _status_exit(health_checks(config)) == 0


def test_committed_schema_titles_and_required_fields_are_fresh() -> None:
    mapping = {
        "data_contract.schema.json": DataContract,
        "manifest_event.schema.json": ManifestEvent,
        "checkpoint.schema.json": Checkpoint,
        "gap_record.schema.json": GapRecord,
        "deletion_record.schema.json": DeletionRecord,
    }
    schema_dir = Path("schemas")
    for file_name, model in mapping.items():
        committed = json.loads((schema_dir / file_name).read_text(encoding="utf-8"))
        generated = model.model_json_schema()
        assert committed == generated
    contract_schema = json.loads(
        (schema_dir / "data_contract.schema.json").read_text(encoding="utf-8")
    )
    assert contract_schema["properties"]["fields"]["items"]["$ref"] == "#/$defs/ContractField"
