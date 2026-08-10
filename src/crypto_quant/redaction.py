"""Recursive redaction for structured logs and diagnostics."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import SecretStr


def redact(value: Any, keys: set[str], secret_values: tuple[str, ...] = ()) -> Any:
    normalized = {key.lower() for key in keys}
    if isinstance(value, Mapping):
        return {
            key: "***REDACTED***"
            if str(key).lower() in normalized
            else redact(item, keys, secret_values)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item, keys, secret_values) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item, keys, secret_values) for item in value)
    if isinstance(value, SecretStr):
        return "***REDACTED***"
    if isinstance(value, str):
        return redact_text(value, secret_values)
    return value


def redact_text(message: str, secret_values: tuple[str, ...]) -> str:
    """Mask known configured secret values even when interpolated into a message."""
    redacted = message
    for secret in secret_values:
        if secret:
            redacted = redacted.replace(secret, "***REDACTED***")
    return redacted
