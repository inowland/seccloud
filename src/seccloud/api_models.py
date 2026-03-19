from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class ApiSchema(BaseModel):
    model_config = ConfigDict(extra="ignore")


class Health(ApiSchema):
    status: str


class IntakeRequest(ApiSchema):
    source: str
    intake_kind: str = "push_gateway"
    integration_id: str | None = None
    received_at: str | None = None
    records: list[dict[str, Any]] = Field(min_length=1)
    metadata: dict[str, Any] | None = None


class IntakeAccepted(ApiSchema):
    batch_id: str
    tenant_id: str
    source: str
    integration_id: str
    record_count: int
    manifest_key: str
    object_key: str
    idempotency_key: str
    duplicate: bool
    queue_path: str


class EvidencePointer(ApiSchema):
    source: str
    object_key: str
    raw_event_id: str
    observed_at: str


class Principal(ApiSchema):
    id: str
    entity_id: str
    entity_key: str
    kind: str
    provider: str
    email: str
    display_name: str
    department: str
    attributes: dict[str, Any]


class Resource(ApiSchema):
    id: str
    entity_id: str
    entity_key: str
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
    event_key: str
    integration_id: str | None = None
    source: str
    source_event_id: str
    principal: Principal
    resource: Resource
    action: Action
    observed_at: str
    environment: dict[str, Any]
    attributes: dict[str, Any]
    evidence: EvidencePointer


class DetectionModelRationale(ApiSchema):
    scoring_mode: str
    policy_scope: str
    detection_threshold: float
    high_severity_threshold: float
    model_score: float
    score_margin: float
    calibration_source: str | None = None
    calibration_reason: str | None = None


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
    model_rationale: DetectionModelRationale | None = None
    status: str


class Pagination(ApiSchema):
    limit: int
    offset: int
    returned: int
    total: int | None = None
    has_more: bool


class EventQueryPage(ApiSchema):
    limit: int
    returned: int
    has_more: bool
    cursor: str | None = None
    next_cursor: str | None = None


class DetectionPeerComparison(ApiSchema):
    principal_id: str
    peer_group: str
    resource_id: str
    principal_role: str
    principal_location: str
    principal_privilege_level: str
    principal_total_events: int
    principal_prior_event_count: int
    principal_prior_action_count: int
    principal_prior_resource_access_count: int
    peer_group_resource_access_count: int
    peer_group_principal_count: int
    peer_group_resource_principal_count: int
    department_peer_count: int
    manager_peer_count: int
    group_peer_count: int
    geo_seen_before: bool
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


class EntityOverview(ApiSchema):
    latest_observed_at: str | None = None
    first_observed_at: str | None = None
    timeline_event_count: int
    total_event_count: int
    distinct_source_count: int
    distinct_sources: list[str]
    distinct_resource_count: int
    high_sensitivity_event_count: int


class EntityDetail(ApiSchema):
    principal: Principal
    overview: EntityOverview
    peer_comparison: DetectionPeerComparison
    timeline: list[Event]


class EventList(ApiSchema):
    items: list[Event]
    page: EventQueryPage
    freshness: EventQueryFreshness


class EventQueryFreshness(ApiSchema):
    query_backend: str
    canonical_store: str
    canonical_format: str
    status: str = "current"
    detail: str | None = None
    watermark_at: str | None = None


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
    recent_window_anchor_at: str | None = None
    raw_last_seen_at: str | None = None
    normalized_last_seen_at: str | None = None
    raw_24h_count: int
    normalized_24h_count: int
    dead_letter_7d_count: int
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
    accepted_batches: int
    accepted_records: int
    pending_batch_count: int


class WorkerState(ApiSchema):
    normalization_runs: int
    feature_runs: int
    detection_runs: int
    source_stats_runs: int
    service_runs: int
    last_submitted_batch_id: str | None = None
    last_processed_batch_id: str | None = None
    last_normalization_at: str | None = None
    last_feature_at: str | None = None
    last_detection_at: str | None = None
    last_source_stats_at: str | None = None
    last_service_at: str | None = None
    last_service_status: str | None = None
    pending_batch_count: int
    processed_batch_count: int


class FeatureTableStatus(ApiSchema):
    action_row_count: int
    history_row_count: int
    collaboration_row_count: int
    static_row_count: int
    peer_group_row_count: int


class FeatureVocabStatus(ApiSchema):
    principal_count: int
    resource_count: int


class ScoringInputStatus(ApiSchema):
    ready: bool
    mode: str
    reason: str
    materialized_table_count: int
    materialized_tables: list[str]


class ModelSourceActivationGateStatus(ApiSchema):
    eligible: bool
    reason: str
    status: str | None = None
    final_loss: float | None = None
    evaluation_scope: str | None = None
    sampled_top1_accuracy: float | None = None
    pairwise_win_rate: float | None = None
    mean_margin: float | None = None
    pair_count: int | None = None


class ModelCoverageGateStatus(ApiSchema):
    eligible: bool
    reason: str
    covered_sources: list[str] = Field(default_factory=list)
    covered_identity_sources: list[str] = Field(default_factory=list)
    covered_resource_sources: list[str] = Field(default_factory=list)
    required_source_count: int = 0
    require_identity_coverage: bool = True
    require_resource_coverage: bool = True
    identity_sources: list[str] = Field(default_factory=list)
    resource_sources: list[str] = Field(default_factory=list)


class ModelPromotionPolicyStatus(ApiSchema):
    required_source_count: int = 0
    require_identity_coverage: bool = True
    require_resource_coverage: bool = True
    identity_sources: list[str] = Field(default_factory=list)
    resource_sources: list[str] = Field(default_factory=list)


class ModelActivationGateStatus(ApiSchema):
    eligible: bool
    reason: str
    status: str | None = None
    training_pair_count: int
    final_loss: float | None = None
    evaluation_scope: str | None = None
    sampled_top1_accuracy: float | None = None
    pairwise_win_rate: float | None = None
    mean_margin: float | None = None
    source_gates: dict[str, ModelSourceActivationGateStatus] = Field(default_factory=dict)
    evaluated_source_count: int = 0
    failing_sources: list[str] = Field(default_factory=list)
    coverage_gate: ModelCoverageGateStatus | None = None


class ModelSourceScorePolicyStatus(ApiSchema):
    detection_threshold: float
    high_severity_threshold: float
    calibration_source: str | None = None
    calibration_reason: str | None = None
    positive_distance_p95: float | None = None
    negative_distance_p50: float | None = None
    negative_distance_p90: float | None = None
    evaluation_pair_count: int | None = None


class ModelScorePolicyStatus(ApiSchema):
    detection_threshold: float
    high_severity_threshold: float
    calibration_source: str | None = None
    calibration_reason: str | None = None
    positive_distance_p95: float | None = None
    negative_distance_p50: float | None = None
    negative_distance_p90: float | None = None
    evaluation_pair_count: int | None = None
    source_policies: dict[str, ModelSourceScorePolicyStatus] = Field(default_factory=dict)


class ModelActivationRecord(ApiSchema):
    action: str
    model_id: str | None = None
    metadata_path: str | None = None
    requested_mode: str
    activation_source: str | None = None
    activated_at: str | None = None


class ModelRuntimeStatus(ApiSchema):
    available: bool
    requested_mode: str
    effective_mode: str
    reason: str
    model_id: str | None = None
    model_version: str | None = None
    model_family: str | None = None
    exported_at: str | None = None
    metadata_path: str | None = None
    activated_at: str | None = None
    activation_source: str | None = None
    action_source_count: int
    principal_vocab_count: int
    resource_vocab_count: int
    activation_gate: ModelActivationGateStatus
    recent_activation_history: list[ModelActivationRecord]
    installed_model_count: int
    installed_model_ids: list[str]
    score_policy: ModelScorePolicyStatus | None = None
    promotion_policy: ModelPromotionPolicyStatus


class EventIndexStatus(ApiSchema):
    available: bool
    event_count: int
    principal_key_count: int
    resource_key_count: int
    department_count: int
    input_signature: str | None = None


class CanonicalEventStoreStatus(ApiSchema):
    canonical_store: str
    canonical_format: str
    query_backend: str
    detail_hydration: str
    cursor_query_support: bool
    text_query_support: bool
    time_range_support: bool


class QuickwitStatus(ApiSchema):
    root: str
    url: str
    index_id: str
    config_path: str
    log_path: str
    paths: dict[str, str]
    initialized: bool
    pid: int | None = None
    running: bool
    ready: bool
    rss_bytes: int | None = None
    log_size_bytes: int
    last_start_attempted_at: str | None = None
    last_start_completed_at: str | None = None
    last_start_duration_ms: int | None = None
    last_start_status: str | None = None
    last_start_error: str | None = None
    watermark_at: str | None = None
    indexed_event_count: int
    last_sync_completed_at: str | None = None
    last_sync_duration_ms: int | None = None
    last_sync_status: str | None = None
    last_sync_error: str | None = None


class DetectionContextStatus(ApiSchema):
    available: bool
    event_count: int
    input_signature: str | None = None
    context_version: int


class IdentityProfilesStatus(ApiSchema):
    available: bool
    source: str | None = None
    principal_count: int
    team_count: int


class WorkerControlStatus(ApiSchema):
    source_stats_refresh_requested: bool
    source_stats_refresh_requested_at: str | None = None


class IntakeStatus(ApiSchema):
    pending_batch_count: int
    processed_batch_count: int
    submitted_batch_count: int
    accepted_idempotency_key_count: int


class PostgresStatus(ApiSchema):
    root: str
    dsn: str | None = None
    paths: dict[str, str]
    initialized: bool


class RuntimeStatus(ApiSchema):
    workspace: str
    tenant_id: str
    stream_state: StreamState
    worker_state: WorkerState
    feature_tables: FeatureTableStatus
    feature_vocab: FeatureVocabStatus
    scoring_input: ScoringInputStatus
    model_runtime: ModelRuntimeStatus
    event_index: EventIndexStatus
    canonical_event_store: CanonicalEventStoreStatus
    quickwit: QuickwitStatus
    detection_context: DetectionContextStatus
    identity_profiles: IdentityProfilesStatus
    worker_control: WorkerControlStatus
    intake: IntakeStatus
    postgres: PostgresStatus
