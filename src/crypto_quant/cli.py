"""Small operational CLI for PHASE 0 checks only."""

from __future__ import annotations

import argparse
import json
import os
import platform
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

import yaml

from .config import AppConfig, load_config
from .logging import configure_logging
from .paths import (
    atomic_rename_probe,
    disk_free_bytes,
    disk_free_percent,
    initialize_data_root,
    repository_root,
    tree_size_bytes,
)
from .redaction import redact_text

DEFAULT_CONFIG_PATH = repository_root() / "config" / "default.yaml"


@dataclass(frozen=True)
class HealthCheck:
    name: str
    status: str
    detail: str


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="crypto-quant")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for name in ("config-check", "health", "paths-init"):
        subparsers.add_parser(name)
    return parser


def _status_exit(checks: list[HealthCheck]) -> int:
    statuses = {check.status for check in checks}
    if "FAIL" in statuses:
        return 2
    if "WARN" in statuses:
        return 1
    return 0


def health_checks(config: AppConfig) -> list[HealthCheck]:
    root = config.storage.resolved_data_root()
    checks = [
        HealthCheck(
            "python_3_12",
            "PASS" if platform.python_version_tuple()[:2] == ("3", "12") else "FAIL",
            platform.python_version(),
        ),
        HealthCheck(
            "timezone_policy",
            "PASS" if config.project.timezone == "UTC" else "FAIL",
            config.project.timezone,
        ),
        HealthCheck("external_data_root", "PASS", str(root)),
    ]
    if not root.exists():
        checks.extend(
            [
                HealthCheck("data_root", "WARN", "does not exist; run paths-init"),
                HealthCheck(
                    "data_root_writable",
                    "UNKNOWN",
                    "root absent; no creation during health",
                ),
                HealthCheck("atomic_rename", "UNKNOWN", "root absent; probe not run"),
            ]
        )
    elif not root.is_dir():
        checks.append(HealthCheck("data_root", "FAIL", "exists but is not a directory"))
    else:
        checks.append(HealthCheck("data_root", "PASS", "exists"))
        writable = os.access(root, os.W_OK)
        checks.append(
            HealthCheck(
                "data_root_writable",
                "PASS" if writable else "FAIL",
                str(writable),
            )
        )
        try:
            checks.append(
                HealthCheck(
                    "atomic_rename",
                    "PASS" if atomic_rename_probe(root) else "FAIL",
                    "probe",
                )
            )
        except OSError as error:
            checks.append(HealthCheck("atomic_rename", "FAIL", type(error).__name__))
    anchor = root if root.exists() else root.parent
    if anchor.exists():
        free_bytes = disk_free_bytes(anchor)
        free_gb = round(free_bytes / 1024**3, 2)
        free_percent = disk_free_percent(anchor)
        threshold = config.storage.disk_thresholds_gb
        status = "PASS"
        if free_gb < threshold.critical_ingestion_stop:
            status = "FAIL"
        elif free_gb < threshold.warning:
            status = "WARN"
        checks.append(HealthCheck("disk_space", status, f"{free_gb} GiB; {free_percent}% free"))
    else:
        checks.append(HealthCheck("disk_space", "UNKNOWN", "data-root parent absent"))
    checks.append(HealthCheck("dataset_tree_size", "PASS", str(tree_size_bytes(root))))
    checks.append(HealthCheck("growth_projections", "UNKNOWN", "no measured history in PHASE 0"))
    versions = config.versions
    checks.append(
        HealthCheck(
            "version_tuple",
            "PASS",
            ".".join(
                (
                    versions.schema_version,
                    versions.data_contract_version,
                    versions.storage_layout_version,
                    versions.collector_version,
                    versions.normalization_version,
                    versions.feature_version,
                )
            ),
        )
    )
    probe_secret = "phase0-secret-redaction-probe"
    hidden = redact_text(probe_secret, (probe_secret,))
    checks.append(
        HealthCheck("secret_redaction", "PASS" if probe_secret not in hidden else "FAIL", hidden)
    )
    return checks


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        config = load_config(args.config)
        configure_logging(
            level=config.logging.level,
            redact_keys=set(config.logging.redact_keys),
            secret_values=config.secrets.values_for_redaction(),
            json_output=config.logging.json_output,
        )
        root = config.storage.resolved_data_root()
        if args.command == "config-check":
            print(
                json.dumps({"status": "PASS", "config": str(args.config), "data_root": str(root)})
            )
            return 0
        if args.command == "paths-init":
            created = initialize_data_root(root)
            print(json.dumps({"status": "PASS", "created": [str(path) for path in created]}))
            return 0
        checks = health_checks(config)
        from .ingestion.health import compute_collector_health
        collector_health = compute_collector_health(
            exchange="binance",
            market_type="spot",
            symbol="BTCUSDT",
            root=root,
        )
        print(json.dumps({
            "checks": [asdict(check) for check in checks],
            "collector_health": collector_health.to_dict(),
        }))
        return _status_exit(checks)
    except (OSError, ValueError, yaml.YAMLError) as error:
        print(json.dumps({"status": "FAIL", "error": str(error)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
