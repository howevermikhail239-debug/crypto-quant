"""Canonical instrument identity; mutable trading rules are intentionally excluded."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, field_validator

from .hashing import stable_id


class InstrumentIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    exchange: str
    venue_environment: str = "production"
    native_symbol: str
    market_type: str
    contract_type: str
    base_asset: str
    quote_asset: str
    settle_asset: str | None = None
    expiry: str | None = None
    quantity_unit: str
    notional_unit: str
    price_tick: str | None = None
    quantity_step: str | None = None
    contract_size: str | None = None

    @field_validator("exchange", "market_type", "contract_type", mode="before")
    @classmethod
    def normalize_lower(cls, value: str) -> str:
        return str(value).strip().lower()

    @field_validator("native_symbol", "base_asset", "quote_asset", "settle_asset", mode="before")
    @classmethod
    def normalize_upper(cls, value: str | None) -> str | None:
        return str(value).strip().upper() if value is not None else None

    @field_validator("venue_environment", mode="before")
    @classmethod
    def normalize_environment(cls, value: str) -> str:
        return str(value).strip().lower()

    @field_validator("market_type")
    @classmethod
    def validate_market_type(cls, value: str) -> str:
        if value not in {"spot", "derivative", "perpetual"}:
            raise ValueError("market_type must be spot, derivative, or perpetual")
        return value

    @field_validator("contract_type")
    @classmethod
    def validate_contract_type(cls, value: str) -> str:
        if value not in {"spot", "linear_perpetual", "inverse_perpetual", "dated_future"}:
            raise ValueError("unsupported contract_type")
        return value

    @property
    def instrument_id(self) -> str:
        identity = self.model_dump(
            include={
                "exchange",
                "venue_environment",
                "native_symbol",
                "market_type",
                "contract_type",
                "base_asset",
                "quote_asset",
                "settle_asset",
                "expiry",
            }
        )
        return stable_id("ins", identity)
