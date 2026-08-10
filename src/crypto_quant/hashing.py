"""Deterministic canonical JSON and SHA-256 helpers."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def stable_id(prefix: str, payload: Any) -> str:
    return f"{prefix}_{sha256_text(canonical_json(payload))[:20]}"
