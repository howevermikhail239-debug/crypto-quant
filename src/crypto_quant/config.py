"""Typed YAML configuration with environment overrides and no required secrets."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .paths import repository_root, resolve_data_root


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DiskThresholds(StrictModel):
    warning: int = Field(ge=1)
    bootstrap_stop: int = Field(ge=1)
    critical_ingestion_stop: int = Field(ge=1)

    @model_validator(mode="after")
    def descending(self) -> DiskThresholds:
        if not self.warning > self.bootstrap_stop > self.critical_ingestion_stop:
            raise ValueError(
                "disk thresholds must be warning > bootstrap_stop > critical_ingestion_stop"
            )
        return self


class ProjectConfig(StrictModel):
    name: str
    availability_class: Literal["best_effort_local"]
    timezone: Literal["UTC"]


class ScopeConfig(StrictModel):
    exchanges: tuple[Literal["binance", "bybit"], ...]
    symbols: tuple[Literal["BTCUSDT", "ETHUSDT"], ...]
    spot: bool
    usdt_linear_perpetual: bool
    excluded_market_types: tuple[str, ...]
    datasets: tuple[DatasetScope, ...]


class DatasetScope(StrictModel):
    dataset: str
    applies_to: Literal["all_eight", "perpetual_only"]
    bootstrap: str
    retention: str
    manual_hold: bool = True
    availability: Literal["best_effort_local", "official_history_plus_local_realtime"]


class StorageConfig(StrictModel):
    data_root: str
    allow_data_root_inside_repository: bool = False
    disk_thresholds_gb: DiskThresholds

    def resolved_data_root(self) -> Path:
        return resolve_data_root(
            self.data_root,
            allow_inside_repository=self.allow_data_root_inside_repository,
        )


class RetentionConfig(StrictModel):
    source_faithful_days: int | None = Field(default=30, ge=1)
    normalized_individual_trades_days: int | None = Field(default=30, ge=1)
    aggregate_1m_days: int | None = None
    aggregate_1s_days: int | None = Field(default=90, ge=1)
    aggregate_5s_days: int | None = Field(default=90, ge=1)


class LoggingConfig(StrictModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    json_output: bool = Field(default=True, alias="json")
    redact_keys: tuple[str, ...] = (
        "api_key",
        "api_secret",
        "secret",
        "token",
        "password",
        "authorization",
        "signature",
    )


class VersionsConfig(StrictModel):
    schema_version: str
    data_contract_version: str
    storage_layout_version: str
    collector_version: str
    normalization_version: str
    feature_version: str


class FeeScheduleReservation(StrictModel):
    status: Literal["reserved_not_collected"]
    historical_collection_in_phase_1: bool = False


class LocalizationConfig(StrictModel):
    default_locale: Literal["ru-RU"] = "ru-RU"
    fallback_locale: Literal["en-US"] = "en-US"
    catalog_directory: str = "locales"


class FutureInterfaceReservation(StrictModel):
    status: Literal["reserved_not_implemented"] = "reserved_not_implemented"
    signal_contract_version: str = "0.2.0-draft"
    risk_exit_interface_version: str = "0.2.0-draft"
    trade_lifecycle_interface_version: str = "0.2.0-draft"
    polymarket_interface_version: str = "0.1.0-draft"
    polymarket_phase: Literal["3F"] = "3F"
    polymarket_nonblocking: bool = True


class SecretsConfig(StrictModel):
    binance_api_key: SecretStr | None = None
    binance_api_secret: SecretStr | None = None
    bybit_api_key: SecretStr | None = None
    bybit_api_secret: SecretStr | None = None

    def values_for_redaction(self) -> tuple[str, ...]:
        return tuple(
            value.get_secret_value()
            for value in self.model_dump().values()
            if isinstance(value, SecretStr) and value.get_secret_value()
        )


class AppConfig(StrictModel):
    project: ProjectConfig
    scope: ScopeConfig
    storage: StorageConfig
    retention: RetentionConfig
    logging: LoggingConfig
    versions: VersionsConfig
    fee_schedule: FeeScheduleReservation
    localization: LocalizationConfig = LocalizationConfig()
    future_interfaces: FutureInterfaceReservation = FutureInterfaceReservation()
    secrets: SecretsConfig = SecretsConfig()


class EnvironmentOverrides(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CRYPTO_QUANT__", env_nested_delimiter="__", extra="ignore"
    )
    logging: LoggingConfig | None = None
    secrets: SecretsConfig = SecretsConfig()


def load_config(path: Path, *, env_file: Path | None = None) -> AppConfig:
    with path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError("configuration root must be a YAML mapping")
    config = AppConfig.model_validate(raw)
    override = EnvironmentOverrides(_env_file=env_file or repository_root() / ".env")
    changes = {"secrets": override.secrets}
    if override.logging:
        changes["logging"] = override.logging
    return config.model_copy(update=changes)
