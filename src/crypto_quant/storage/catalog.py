"""Manifest-aware DuckDB views over active immutable Parquet generations.

Parquet remains authoritative.  The DuckDB file contains views and small literal
catalog metadata only; it never imports market observations into mutable tables.
"""

from __future__ import annotations

import hashlib
import json
import os
import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

DATASET_VIEWS: dict[str, str] = {
    "ohlcv": "market_ohlcv",
    "individual_trade": "market_individual_trade",
    "exchange_aggregate_trade": "market_exchange_aggregate_trade",
    "derived_trade_bucket": "market_derived_trade_bucket",
    "funding_rate": "market_funding_rate",
    "open_interest": "market_open_interest",
    "liquidations": "market_liquidations",
}

_CUMULATIVE_DATASETS = {"funding_rate", "open_interest", "liquidations"}
_PUBLISH_ACTIONS = {"", "NORMALIZED", "INGESTED", "COMPACTED"}
_REMOVE_ACTIONS = {"SUPERSEDED", "DELETED"}


@dataclass(frozen=True)
class ActiveArtifact:
    dataset_class: str
    path: Path
    relative_path: str
    parquet_sha256: str | None
    source_dataset_id: str | None
    exchange: str | None
    market_type: str | None
    contract_type: str | None
    instrument_id: str | None
    symbol: str | None
    schema_version: str | None
    collector_version: str | None
    normalization_version: str | None
    manifest_path: str
    manifest_record_number: int


@dataclass(frozen=True)
class CatalogBuildResult:
    catalog_path: Path
    active_artifacts: tuple[ActiveArtifact, ...]
    view_row_counts: dict[str, int]
    metadata_snapshot_count: int


def _normalise_ref(value: str) -> str:
    return value.replace("\\", "/")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _resolve_under_root(root: Path, reference: str) -> Path:
    candidate = (root / _normalise_ref(reference)).resolve()
    resolved_root = root.resolve()
    try:
        candidate.relative_to(resolved_root)
    except ValueError as error:
        raise ValueError(f"Manifest artifact escapes data root: {reference}") from error
    return candidate


def _dataset_class(row: dict[str, Any], manifest_name: str) -> str | None:
    explicit = row.get("dataset_class")
    if explicit:
        value = str(explicit)
        return {
            "funding": "funding_rate",
            "liquidation": "liquidations",
            "trade_bucket": "derived_trade_bucket",
            "aggregate_trade": "exchange_aggregate_trade",
        }.get(value, value)
    source = str(row.get("source_dataset_id", "")).lower()
    object_ref = str(row.get("object_id", "")).lower().replace("\\", "/")
    joined = f"{manifest_name.lower()} {source} {object_ref}"
    if "derived_trade_bucket" in joined or "trade_bucket" in joined:
        return "derived_trade_bucket"
    if "individual_trade" in joined:
        return "individual_trade"
    if "aggregate_trade" in joined or "aggtrade" in joined:
        return "exchange_aggregate_trade"
    if "open_interest" in joined:
        return "open_interest"
    if "funding" in joined:
        return "funding_rate"
    if "liquidation" in joined:
        return "liquidations"
    if "ohlcv" in joined or "klines" in joined:
        return "ohlcv"
    return None


def _artifact_refs(row: dict[str, Any]) -> list[tuple[str, str | None]]:
    refs: list[str] = []
    object_id = row.get("object_id")
    if object_id and str(object_id).lower().endswith(".parquet"):
        refs.append(str(object_id))
    refs.extend(str(value) for value in row.get("created_parquets", []))
    hashes = row.get("parquet_sha256")
    if isinstance(hashes, list):
        expected = [str(value) for value in hashes]
    elif hashes:
        expected = [str(hashes)]
    else:
        expected = []
    return [(ref, expected[index] if index < len(expected) else None) for index, ref in enumerate(refs)]


def _generation_key(row: dict[str, Any], dataset_class: str, manifest_name: str) -> tuple[str, ...]:
    return (
        manifest_name,
        dataset_class,
        str(row.get("source_dataset_id", "")),
        str(row.get("exchange", "")),
        str(row.get("market_type", "")),
        str(row.get("symbol") or row.get("instrument_id") or ""),
        str(row.get("period", "")),
    )


def _removed_references(row: dict[str, Any]) -> set[str]:
    values: list[str] = []
    for key in (
        "superseded_object_id",
        "deleted_object_id",
        "object_id",
    ):
        if row.get(key):
            values.append(str(row[key]))
    for key in ("superseded_object_ids", "deleted_object_ids"):
        values.extend(str(value) for value in row.get(key, []))
    return {_normalise_ref(value) for value in values if value.lower().endswith(".parquet")}


def _read_manifest_rows(root: Path) -> list[tuple[Path, int, dict[str, Any], str]]:
    manifest_root = root / "control" / "manifests"
    if not manifest_root.exists():
        return []
    result: list[tuple[Path, int, dict[str, Any], str]] = []
    for path in sorted(manifest_root.rglob("*.jsonl")):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if not line.strip():
                continue
            row = json.loads(line)
            dataset_class = _dataset_class(row, path.name)
            if dataset_class in DATASET_VIEWS:
                result.append((path, number, row, dataset_class))
    return result


def resolve_active_artifacts(root: Path) -> tuple[ActiveArtifact, ...]:
    """Resolve active Parquet objects using each accepted manifest's generation semantics."""
    rows = _read_manifest_rows(root)
    quarantined_names: set[str] = set()
    removed_refs: set[str] = set()
    latest_cumulative: dict[tuple[str, ...], tuple[Path, int, dict[str, Any], str]] = {}
    additive: list[tuple[Path, int, dict[str, Any], str]] = []

    for manifest, number, row, dataset_class in rows:
        action = str(row.get("action", "")).upper()
        if action == "QUARANTINED":
            quarantined_names.update(
                Path(_normalise_ref(value)).name for value in row.get("quarantined_parquets", [])
            )
            continue
        if action in _REMOVE_ACTIONS:
            removed_refs.update(_removed_references(row))
            continue
        if action not in _PUBLISH_ACTIONS:
            continue
        record = (manifest, number, row, dataset_class)
        if dataset_class in _CUMULATIVE_DATASETS:
            latest_cumulative[_generation_key(row, dataset_class, manifest.name)] = record
        else:
            additive.append(record)

    selected = additive + list(latest_cumulative.values())
    selected_by_reference: dict[
        str, tuple[Path, int, dict[str, Any], str, str | None]
    ] = {}
    for manifest, number, row, dataset_class in selected:
        for reference, expected_hash in _artifact_refs(row):
            normalized = _normalise_ref(reference)
            if normalized in removed_refs or Path(normalized).name in quarantined_names:
                continue
            # Append-only manifests may publish a corrected generation at the same
            # logical path. The latest record is authoritative; superseded hashes
            # must not be validated as if they were still active.
            selected_by_reference[normalized] = (
                manifest,
                number,
                row,
                dataset_class,
                expected_hash,
            )
    artifacts: dict[str, ActiveArtifact] = {}
    for normalized, record in selected_by_reference.items():
        manifest, number, row, dataset_class, expected_hash = record
        artifact_path = _resolve_under_root(root, normalized)
        if not artifact_path.is_file():
            raise FileNotFoundError(f"Active manifest artifact is missing: {normalized}")
        if expected_hash and _sha256_file(artifact_path) != expected_hash:
            raise ValueError(f"Active manifest checksum mismatch: {normalized}")
        artifacts[normalized] = ActiveArtifact(
            dataset_class=dataset_class,
            path=artifact_path,
            relative_path=normalized,
            parquet_sha256=expected_hash,
            source_dataset_id=row.get("source_dataset_id"),
            exchange=row.get("exchange"),
            market_type=row.get("market_type"),
            contract_type=row.get("contract_type"),
            instrument_id=row.get("instrument_id"),
            symbol=row.get("symbol"),
            schema_version=row.get("schema_version"),
            collector_version=row.get("collector_version"),
            normalization_version=row.get("normalization_version"),
            manifest_path=str(manifest.relative_to(root)).replace("\\", "/"),
            manifest_record_number=number,
        )
    return tuple(sorted(artifacts.values(), key=lambda artifact: artifact.relative_path))


def _sql_string(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _path_list(paths: Iterable[Path]) -> str:
    return "[" + ", ".join(_sql_string(path.as_posix()) for path in paths) + "]"


def _create_artifact_view(connection: duckdb.DuckDBPyConnection, artifacts: list[ActiveArtifact]) -> None:
    columns = (
        "dataset_class",
        "file_path",
        "relative_path",
        "parquet_sha256",
        "source_dataset_id",
        "exchange",
        "market_type",
        "contract_type",
        "instrument_id",
        "symbol",
        "schema_version",
        "collector_version",
        "normalization_version",
        "manifest_path",
        "manifest_record_number",
    )
    if not artifacts:
        select = ", ".join(
            f"CAST(NULL AS {'BIGINT' if column == 'manifest_record_number' else 'VARCHAR'}) AS {_sql_identifier(column)}"
            for column in columns
        )
        connection.execute(
            f"CREATE VIEW catalog_active_artifacts AS SELECT {select} WHERE FALSE"
        )
        return
    values: list[str] = []
    for artifact in artifacts:
        row = (
            artifact.dataset_class,
            artifact.path.as_posix(),
            artifact.relative_path,
            artifact.parquet_sha256,
            artifact.source_dataset_id,
            artifact.exchange,
            artifact.market_type,
            artifact.contract_type,
            artifact.instrument_id,
            artifact.symbol,
            artifact.schema_version,
            artifact.collector_version,
            artifact.normalization_version,
            artifact.manifest_path,
        )
        values.append(
            "(" + ", ".join(_sql_string(value) for value in row) + f", {artifact.manifest_record_number})"
        )
    connection.execute(
        "CREATE VIEW catalog_active_artifacts AS SELECT * FROM (VALUES "
        + ", ".join(values)
        + ") AS active("
        + ", ".join(_sql_identifier(column) for column in columns)
        + ")"
    )


def _metadata_rows(root: Path) -> list[tuple[str, str, int, str | None, str | None, str | None]]:
    metadata_root = root / "control" / "instrument_metadata"
    rows: list[tuple[str, str, int, str | None, str | None, str | None]] = []
    if not metadata_root.exists():
        return rows
    for path in sorted(metadata_root.glob("*.json")):
        raw = path.read_bytes()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        rows.append(
            (
                path.as_posix(),
                hashlib.sha256(raw).hexdigest(),
                len(raw),
                payload.get("retrieved_at"),
                payload.get("dataset_id"),
                payload.get("native_symbol") or payload.get("symbol"),
            )
        )
    return rows


def _create_metadata_view(
    connection: duckdb.DuckDBPyConnection,
    rows: list[tuple[str, str, int, str | None, str | None, str | None]],
) -> None:
    if not rows:
        connection.execute(
            """CREATE VIEW instrument_metadata_snapshots AS
            SELECT CAST(NULL AS VARCHAR) AS file_path,
                   CAST(NULL AS VARCHAR) AS file_sha256,
                   CAST(NULL AS BIGINT) AS file_bytes,
                   CAST(NULL AS VARCHAR) AS retrieved_at,
                   CAST(NULL AS VARCHAR) AS source_dataset_id,
                   CAST(NULL AS VARCHAR) AS symbol
            WHERE FALSE"""
        )
        return
    values = []
    for file_path, digest, size, retrieved_at, dataset_id, symbol in rows:
        values.append(
            "("
            + ", ".join(
                (
                    _sql_string(file_path),
                    _sql_string(digest),
                    str(size),
                    _sql_string(retrieved_at),
                    _sql_string(dataset_id),
                    _sql_string(symbol),
                )
            )
            + ")"
        )
    connection.execute(
        "CREATE VIEW instrument_metadata_snapshots AS SELECT * FROM (VALUES "
        + ", ".join(values)
        + ") AS metadata(file_path, file_sha256, file_bytes, retrieved_at, source_dataset_id, symbol)"
    )


def build_catalog(root: Path, catalog_path: Path | None = None) -> CatalogBuildResult:
    """Atomically build a persistent view-only DuckDB catalog for active artifacts."""
    artifacts = resolve_active_artifacts(root)
    destination = catalog_path or root / "control" / "catalog" / "phase1e1.duckdb"
    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.partial")
    grouped: dict[str, list[ActiveArtifact]] = defaultdict(list)
    for artifact in artifacts:
        grouped[artifact.dataset_class].append(artifact)
    metadata = _metadata_rows(root)
    row_counts: dict[str, int] = {}

    try:
        connection = duckdb.connect(str(partial))
        try:
            _create_artifact_view(connection, list(artifacts))
            _create_metadata_view(connection, metadata)
            for dataset_class, view_name in DATASET_VIEWS.items():
                selected = grouped.get(dataset_class, [])
                if selected:
                    connection.execute(
                        f"CREATE VIEW {_sql_identifier(view_name)} AS "
                        f"SELECT * FROM read_parquet({_path_list(item.path for item in selected)}, "
                        "union_by_name = true, filename = true)"
                    )
                else:
                    connection.execute(
                        f"CREATE VIEW {_sql_identifier(view_name)} AS "
                        "SELECT CAST(NULL AS VARCHAR) AS filename WHERE FALSE"
                    )
                row_counts[dataset_class] = int(
                    connection.execute(
                        f"SELECT count(*) FROM {_sql_identifier(view_name)}"
                    ).fetchone()[0]
                )
            market_tables = connection.execute(
                "SELECT count(*) FROM duckdb_tables() WHERE internal = false"
            ).fetchone()[0]
            if market_tables:
                raise AssertionError("Catalog must not contain mutable user tables")
            connection.execute("CHECKPOINT")
        finally:
            connection.close()
        os.replace(partial, destination)
    except BaseException:
        partial.unlink(missing_ok=True)
        raise
    return CatalogBuildResult(
        catalog_path=destination,
        active_artifacts=artifacts,
        view_row_counts=row_counts,
        metadata_snapshot_count=len(metadata),
    )
