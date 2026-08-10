"""Small YAML-backed localization boundary for future user-facing surfaces."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from string import Formatter
from typing import Any

import yaml


def _flatten(values: Mapping[str, Any], prefix: str = "") -> dict[str, str]:
    result: dict[str, str] = {}
    for key, value in values.items():
        full_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, Mapping):
            result.update(_flatten(value, full_key))
        elif isinstance(value, str):
            result[full_key] = value
        else:
            raise ValueError(f"locale key {full_key!r} must contain a string or mapping")
    return result


def load_catalog(path: Path) -> dict[str, str]:
    with path.open(encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, Mapping):
        raise ValueError(f"locale catalog {path} must be a YAML mapping")
    return _flatten(raw)


def placeholders(message: str) -> frozenset[str]:
    return frozenset(
        field_name
        for _, field_name, _, _ in Formatter().parse(message)
        if field_name is not None and field_name != ""
    )


class Translator:
    def __init__(self, catalogs: Mapping[str, Mapping[str, str]], *, fallback_locale: str) -> None:
        if fallback_locale not in catalogs:
            raise ValueError("fallback locale catalog is required")
        self._catalogs = catalogs
        self._fallback_locale = fallback_locale

    def translate(self, key: str, *, locale: str, **values: object) -> str:
        catalog = self._catalogs.get(locale, self._catalogs[self._fallback_locale])
        template = catalog.get(key) or self._catalogs[self._fallback_locale].get(key)
        if template is None:
            raise KeyError(f"unknown localization key: {key}")
        expected = placeholders(template)
        actual = frozenset(values)
        if expected != actual:
            raise ValueError(f"interpolation mismatch for {key}: expected {expected}, got {actual}")
        return template.format(**values)
