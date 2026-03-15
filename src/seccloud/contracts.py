from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


def _serialize(value: Any) -> Any:
    if hasattr(value, "to_dict"):
        return value.to_dict()
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize(item) for key, item in value.items()}
    return value


@dataclass(slots=True)
class Principal:
    id: str
    entity_id: str
    entity_key: str
    kind: str
    provider: str
    email: str
    display_name: str
    department: str
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Resource:
    id: str
    entity_id: str
    entity_key: str
    kind: str
    provider: str
    name: str
    sensitivity: str
    attributes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class Action:
    source: str
    verb: str
    category: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class EvidencePointer:
    source: str
    object_key: str
    raw_event_id: str
    observed_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class RawIntakeBatch:
    batch_id: str
    intake_kind: str
    source: str
    received_at: str
    records: list[dict[str, Any]]
    integration_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    schema_version: str = "raw-intake-batch-v1"

    def to_dict(self) -> dict[str, Any]:
        return {
            "batch_id": self.batch_id,
            "schema_version": self.schema_version,
            "intake_kind": self.intake_kind,
            "source": self.source,
            "integration_id": self.integration_id,
            "received_at": self.received_at,
            "record_count": len(self.records),
            "records": self.records,
            "metadata": self.metadata,
        }


@dataclass(slots=True)
class Event:
    event_id: str
    event_key: str
    integration_id: str | None
    source: str
    source_event_id: str
    principal: Principal
    resource: Resource
    action: Action
    observed_at: str
    environment: dict[str, Any]
    attributes: dict[str, Any]
    evidence: EvidencePointer

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "event_key": self.event_key,
            "integration_id": self.integration_id,
            "source": self.source,
            "source_event_id": self.source_event_id,
            "principal": self.principal.to_dict(),
            "resource": self.resource.to_dict(),
            "action": self.action.to_dict(),
            "observed_at": self.observed_at,
            "environment": self.environment,
            "attributes": self.attributes,
            "evidence": self.evidence.to_dict(),
        }


@dataclass(slots=True)
class Detection:
    detection_id: str
    scenario: str
    title: str
    score: float
    confidence: float
    severity: str
    reasons: list[str]
    feature_attributions: dict[str, float]
    event_ids: list[str]
    related_entity_ids: list[str]
    evidence: list[EvidencePointer]
    model_version: str
    status: str = "open"

    def to_dict(self) -> dict[str, Any]:
        return {
            "detection_id": self.detection_id,
            "scenario": self.scenario,
            "title": self.title,
            "score": self.score,
            "confidence": self.confidence,
            "severity": self.severity,
            "reasons": self.reasons,
            "feature_attributions": self.feature_attributions,
            "event_ids": self.event_ids,
            "related_entity_ids": self.related_entity_ids,
            "evidence": [pointer.to_dict() for pointer in self.evidence],
            "model_version": self.model_version,
            "status": self.status,
        }


@dataclass(slots=True)
class Case:
    case_id: str
    detection_ids: list[str]
    timeline_event_ids: list[str]
    evidence_snapshots: list[dict[str, Any]]
    status: str
    disposition: str | None
    analyst_notes: list[str]
    feedback_labels: list[str]
    created_at: str
    updated_at: str

    def to_dict(self) -> dict[str, Any]:
        return _serialize(asdict(self))


@dataclass(slots=True)
class DerivedState:
    principal_profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    peer_groups: dict[str, dict[str, Any]] = field(default_factory=dict)
    access_histories: dict[str, dict[str, Any]] = field(default_factory=dict)
    aggregates: dict[str, Any] = field(default_factory=dict)
    embeddings: dict[str, list[float]] = field(default_factory=dict)
    case_artifacts: dict[str, dict[str, Any]] = field(default_factory=dict)
    feedback_labels: dict[str, list[str]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _serialize(asdict(self))
