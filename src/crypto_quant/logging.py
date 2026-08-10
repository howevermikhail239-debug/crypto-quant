"""UTC JSON logging with secret-aware structured context."""

from __future__ import annotations

import json
import logging
from typing import Any

from .redaction import redact, redact_text
from .time import utc_now

REQUIRED_EVENT_FIELDS = (
    "component",
    "event",
    "run_id",
    "exchange",
    "instrument_id",
    "symbol",
    "error_type",
    "retry_state",
)


class JsonFormatter(logging.Formatter):
    def __init__(self, redact_keys: set[str], secret_values: tuple[str, ...]) -> None:
        super().__init__()
        self.redact_keys = redact_keys
        self.secret_values = secret_values

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp_utc": utc_now().isoformat().replace("+00:00", "Z"),
            "level": record.levelname,
            **{field: None for field in REQUIRED_EVENT_FIELDS},
            "message": redact_text(record.getMessage(), self.secret_values),
        }
        payload.update(getattr(record, "event_fields", {}))
        if hasattr(record, "context"):
            payload["context"] = redact(record.context, self.redact_keys, self.secret_values)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(
    *,
    level: str,
    redact_keys: set[str],
    secret_values: tuple[str, ...] = (),
    json_output: bool = True,
) -> None:
    handler = logging.StreamHandler()
    formatter = (
        JsonFormatter(redact_keys, secret_values)
        if json_output
        else logging.Formatter("%(message)s")
    )
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


def log_event(
    logger: logging.Logger,
    level: int,
    message: str,
    *,
    component: str,
    event: str,
    run_id: str | None = None,
    exchange: str | None = None,
    instrument_id: str | None = None,
    symbol: str | None = None,
    error_type: str | None = None,
    retry_state: str | None = None,
    **context: Any,
) -> None:
    logger.log(
        level,
        message,
        extra={
            "context": context,
            "event_fields": {
                "component": component,
                "event": event,
                "run_id": run_id,
                "exchange": exchange,
                "instrument_id": instrument_id,
                "symbol": symbol,
                "error_type": error_type,
                "retry_state": retry_state,
            },
        },
    )
