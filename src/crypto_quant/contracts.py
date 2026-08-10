"""PHASE 0 control-plane contracts, independent of exchange adapters."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator, model_validator

from .time import require_utc


class StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DataContract(StrictContract):
    contract_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]+$")
    source_dataset_id: str
    exchange: str
    market_type: str
    source_kind: str
    official_documentation_url: HttpUrl
    verified_at: datetime
    schema_version: str
    fields: tuple[ContractField, ...] = ()

    _utc = field_validator("verified_at")(require_utc)


class ContractField(StrictContract):
    source_field: str
    semantic_meaning: str
    source_unit: str | None = None
    timestamp_meaning: str | None = None
    nullable: bool
    canonical_field: str
    transformation: str
    normalized_unit: str | None = None
    validation_rules: tuple[str, ...] = ()
    known_limitations: tuple[str, ...] = ()


class ManifestAction(StrEnum):
    INGESTED = "INGESTED"
    VALIDATED = "VALIDATED"
    NORMALIZED = "NORMALIZED"
    COMPACTED = "COMPACTED"
    SUPERSEDED = "SUPERSEDED"
    QUARANTINED = "QUARANTINED"
    DELETION_PLANNED = "DELETION_PLANNED"
    DELETED = "DELETED"
    DELETE_FAILED = "DELETE_FAILED"


class ManifestEvent(StrictContract):
    event_id: str
    action: ManifestAction
    object_id: str
    occurred_at: datetime
    checksum_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    reason: str | None = None
    schema_version: str
    collector_version: str
    normalization_version: str

    _utc = field_validator("occurred_at", "coverage_start", "coverage_end")(require_utc)

    @model_validator(mode="after")
    def validate_coverage(self) -> ManifestEvent:
        if self.coverage_start and self.coverage_end and self.coverage_end < self.coverage_start:
            raise ValueError("coverage_end must be >= coverage_start")
        return self


class Checkpoint(StrictContract):
    checkpoint_id: str
    source_dataset_id: str
    instrument_id: str
    committed_at: datetime
    cursor: str | None = None
    last_event_time: datetime | None = None
    last_knowledge_time: datetime | None = None

    _utc = field_validator("committed_at", "last_event_time", "last_knowledge_time")(require_utc)

    @model_validator(mode="after")
    def validate_watermarks(self) -> Checkpoint:
        if self.last_event_time and self.last_event_time > self.committed_at:
            raise ValueError("last_event_time cannot be after durable committed_at")
        if self.last_knowledge_time and self.last_knowledge_time > self.committed_at:
            raise ValueError("last_knowledge_time cannot be after durable committed_at")
        if self.last_event_time and self.last_knowledge_time:
            if self.last_knowledge_time < self.last_event_time:
                raise ValueError("last_knowledge_time cannot precede last_event_time")
        return self


class GapKind(StrEnum):
    EXCHANGE_GAP = "exchange_gap"
    LOCAL_COLLECTOR_GAP = "local_collector_gap"
    UNKNOWN_GAP = "unknown_gap"


class GapRecord(StrictContract):
    gap_id: str
    source_dataset_id: str
    instrument_id: str
    kind: GapKind
    started_at: datetime
    ended_at: datetime
    detected_at: datetime
    status: str
    reason: str | None = None

    _utc = field_validator("started_at", "ended_at", "detected_at")(require_utc)

    @model_validator(mode="after")
    def validate_gap_times(self) -> GapRecord:
        if self.ended_at <= self.started_at:
            raise ValueError("gap ended_at must be after started_at")
        if self.detected_at < self.started_at:
            raise ValueError("gap detected_at cannot precede started_at")
        return self


class DeletionRecord(StrictContract):
    deletion_id: str
    object_id: str
    planned_at: datetime
    deleted_at: datetime | None = None
    reason: str
    coverage_start: datetime | None = None
    coverage_end: datetime | None = None
    checksum_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    _utc = field_validator("planned_at", "deleted_at", "coverage_start", "coverage_end")(
        require_utc
    )

    @model_validator(mode="after")
    def validate_deletion(self) -> DeletionRecord:
        if self.deleted_at and not self.checksum_sha256:
            raise ValueError("deleted record requires checksum_sha256")
        if self.deleted_at and self.deleted_at < self.planned_at:
            raise ValueError("deleted_at cannot precede planned_at")
        if self.coverage_start and self.coverage_end and self.coverage_end < self.coverage_start:
            raise ValueError("coverage_end must be >= coverage_start")
        return self


class KnowledgeRecord(StrictContract):
    event_time: datetime
    knowledge_time: datetime
    knowledge_time_basis: str

    _utc = field_validator("event_time", "knowledge_time")(require_utc)

    def eligible_at(self, decision_time: datetime) -> bool:
        return self.knowledge_time <= require_utc(decision_time)
