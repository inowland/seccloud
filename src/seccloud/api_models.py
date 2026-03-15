from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class ApiSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Health(ApiSchema):
    status: str


class EvidencePointer(ApiSchema):
    source: str
    object_key: str
    raw_event_id: str
    observed_at: str


class Principal(ApiSchema):
    id: str
    kind: str
    provider: str
    email: str
    display_name: str
    department: str
    attributes: dict[str, Any]


class Resource(ApiSchema):
    id: str
    kind: str
    provider: str
    name: str
    sensitivity: str
    attributes: dict[str, Any]


class Action(ApiSchema):
    source: str
    verb: str
    category: str


class Event(ApiSchema):
    event_id: str
    source: str
    source_event_id: str
    principal: Principal
    resource: Resource
    action: Action
    observed_at: str
    environment: dict[str, Any]
    attributes: dict[str, Any]
    evidence: EvidencePointer


class Detection(ApiSchema):
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
    status: str


class Pagination(ApiSchema):
    limit: int
    offset: int
    total: int
    has_more: bool


class DetectionPeerComparison(ApiSchema):
    principal_id: str
    peer_group: str
    resource_id: str
    principal_total_events: int
    principal_prior_resource_access_count: int
    peer_group_resource_access_count: int
    peer_group_principal_count: int
    detection_reasons: list[str]


class EvidenceBundleItem(ApiSchema):
    pointer: EvidencePointer
    raw_payload: dict[str, Any] | None
    retention_expired: bool


class DetectionEvidenceBundle(ApiSchema):
    evidence_items: list[EvidenceBundleItem]


class DetectionDetail(ApiSchema):
    detection: Detection
    peer_comparison: DetectionPeerComparison
    evidence_bundle: DetectionEvidenceBundle
    events: list[Event]


class EventList(ApiSchema):
    items: list[Event]
    page: Pagination


class DetectionList(ApiSchema):
    items: list[Detection]
    page: Pagination


class StreamState(ApiSchema):
    cursor: int
    total_source_events: int
    complete: bool
    normalized_event_count: int | None = None
    detection_count: int | None = None


class OpsMetadata(ApiSchema):
    workspace: str | None = None
    event_counts_by_source: dict[str, int]
    dead_letter_count: int
    dead_letter_counts_by_source: dict[str, int] | None = None
    contains_raw_payloads: bool | None = None


class Overview(ApiSchema):
    stream_state: StreamState
    ops_metadata: OpsMetadata


class SourceCapabilityDetails(ApiSchema):
    display_name: str
    required_event_types: list[str]
    required_fields: list[str]
    seen_event_types: list[str]
    missing_required_event_types: list[str]
    required_field_coverage: dict[str, bool]
    missing_required_fields: list[str]
    raw_event_count: int
    normalized_event_count: int
    dead_letter_count: int
    dead_letter_reason_counts: dict[str, int]


class SourceCapabilityMatrix(ApiSchema):
    source_pack: list[str]
    sources: dict[str, SourceCapabilityDetails]


class StreamReset(ApiSchema):
    status: str
    total_source_events: int
    cursor: int


class StreamAdvance(ApiSchema):
    status: str
    cursor: int
    total_source_events: int
    complete: bool
    batch_size: int
    ingest: dict[str, Any]
    detect: dict[str, Any]
    ops_metadata: OpsMetadata
