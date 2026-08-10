from pathlib import Path

import pytest

from crypto_quant.config import DiskThresholds, load_config


def test_default_config_loads() -> None:
    config = load_config(Path("config/default.yaml"))
    assert config.project.timezone == "UTC"
    assert config.scope.symbols == ("BTCUSDT", "ETHUSDT")
    assert config.retention.aggregate_1m_days is None
    assert config.localization.default_locale == "ru-RU"
    assert config.localization.fallback_locale == "en-US"
    assert config.future_interfaces.polymarket_nonblocking
    assert config.future_interfaces.polymarket_phase == "3F"
    assert config.future_interfaces.signal_contract_version == "0.2.0-draft"
    assert config.future_interfaces.trade_lifecycle_interface_version == "0.2.0-draft"


def test_disk_thresholds_must_descend() -> None:
    with pytest.raises(ValueError):
        DiskThresholds(warning=20, bootstrap_stop=50, critical_ingestion_stop=10)


def test_optional_secret_is_loaded_from_nested_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRYPTO_QUANT__SECRETS__BINANCE_API_KEY", "not-for-logs")
    config = load_config(Path("config/default.yaml"))
    assert config.secrets.binance_api_key is not None
    assert "not-for-logs" not in repr(config.secrets)
    assert config.secrets.values_for_redaction() == ("not-for-logs",)


def test_optional_secret_is_loaded_from_dotenv(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "CRYPTO_QUANT__SECRETS__BYBIT_API_KEY=dotenv-secret\n",
        encoding="utf-8",
    )
    config = load_config(Path("config/default.yaml"), env_file=env_file)
    assert config.secrets.bybit_api_key is not None
    assert config.secrets.bybit_api_key.get_secret_value() == "dotenv-secret"
