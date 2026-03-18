import type { components, paths } from "./api.generated";
import { DetailSectionCard } from "./panels";

type JsonResponse<Path extends keyof paths, Method extends keyof paths[Path]> =
  NonNullable<paths[Path][Method]> extends {
    responses: { 200: { content: { "application/json": infer Response } } };
  }
    ? Response
    : never;

type BadgeTone = "critical" | "attention" | "neutral" | "positive";
type EventDetail = JsonResponse<"/api/events/{event_id}", "get">;
type DetectionDetail = JsonResponse<"/api/detections/{detection_id}", "get">;
type RuntimeStatus = JsonResponse<"/api/runtime-status", "get">;
type SourceCapabilityDetails = components["schemas"]["SourceCapabilityDetails"];

interface ActionCard {
  title: string;
  body: string;
  tone: BadgeTone;
}

export interface IntegrationEntry {
  source: string;
  details: SourceCapabilityDetails;
}

interface FormattingHelpers {
  formatNumber: (value: number | undefined) => string;
  formatObservedAt: (value: string) => string;
  formatSourceName: (source: string) => string;
}

interface DetectionDetailContentProps extends FormattingHelpers {
  acknowledging: boolean;
  badgeToneForSeverity: (severity: string) => BadgeTone;
  detail: DetectionDetail;
  onAcknowledge: () => void;
  openEventPage: (eventId: string) => void;
  stackSummaryCards?: boolean;
}

interface EventDetailContentProps {
  event: EventDetail;
  formatObservedAt: (value: string) => string;
  formatSourceName: (source: string) => string;
}

interface IntegrationDetailContentProps {
  entry: IntegrationEntry;
  formatNumber: (value: number | undefined) => string;
  formatObservedAt: (value: string) => string;
  integrationActionItems: (details: SourceCapabilityDetails) => ActionCard[];
  integrationCoverageCount: (details: SourceCapabilityDetails) => number;
  integrationStatus: (details: SourceCapabilityDetails) => {
    tone: BadgeTone;
    label: string;
    note: string;
  };
  runtimeStatus: RuntimeStatus | null;
}

export function DetectionDetailContent({
  acknowledging,
  badgeToneForSeverity,
  detail,
  formatNumber,
  formatObservedAt,
  formatSourceName,
  onAcknowledge,
  openEventPage,
  stackSummaryCards = false,
}: DetectionDetailContentProps) {
  return (
    <div className="detail-pane">
      <div className="detail-hero">
        <div className="detail-hero__content">
          <div>
            <span
              className={`badge badge--${badgeToneForSeverity(
                detail.detection.severity,
              )}`}
            >
              {detail.detection.severity}
            </span>
            <h3>{detail.detection.title}</h3>
            <p className="detail-subtle">{detail.detection.scenario}</p>
            <span
              className="badge badge--neutral"
              style={{ fontSize: "0.7rem", marginTop: "0.25rem" }}
            >
              {detail.detection.model_version}
            </span>
          </div>
        </div>
        <div className="detail-hero__actions">
          <button
            disabled={acknowledging || detail.detection.status !== "open"}
            onClick={onAcknowledge}
            type="button"
          >
            {detail.detection.status === "open"
              ? acknowledging
                ? "Acknowledging..."
                : "Acknowledge"
              : "Acknowledged"}
          </button>
        </div>
      </div>

      <DetailSectionCard title="Detection summary">
        <dl className="detail-field-grid">
          <div className="detail-field">
            <dt>Severity</dt>
            <dd>{detail.detection.severity}</dd>
          </div>
          <div className="detail-field">
            <dt>Status</dt>
            <dd>{detail.detection.status}</dd>
          </div>
          <div className="detail-field">
            <dt>Score</dt>
            <dd>{detail.detection.score.toFixed(2)}</dd>
          </div>
          <div className="detail-field">
            <dt>Confidence</dt>
            <dd>{detail.detection.confidence.toFixed(2)}</dd>
          </div>
          <div className="detail-field">
            <dt>Evidence items</dt>
            <dd>
              {formatNumber(detail.evidence_bundle.evidence_items.length)}
            </dd>
          </div>
          <div className="detail-field">
            <dt>Related events</dt>
            <dd>{formatNumber(detail.events.length)}</dd>
          </div>
        </dl>
      </DetailSectionCard>

      <div
        className={
          stackSummaryCards
            ? "detail-section-grid detail-section-grid--stacked"
            : "detail-section-grid"
        }
      >
        <DetailSectionCard title="Reasons">
          <ul className="flat-list">
            {detail.detection.reasons.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
          </ul>
        </DetailSectionCard>

        {detail.detection.model_rationale && (
          <DetailSectionCard title="Model rationale">
            <ul className="flat-list">
              <li>
                Scoring mode: {detail.detection.model_rationale.scoring_mode}
              </li>
              <li>
                Applied threshold:{" "}
                {detail.detection.model_rationale.detection_threshold.toFixed(
                  2,
                )}
                {" • "}score{" "}
                {detail.detection.model_rationale.model_score.toFixed(2)}
                {" • "}margin{" "}
                {detail.detection.model_rationale.score_margin.toFixed(2)}
              </li>
              <li>
                Policy scope: {detail.detection.model_rationale.policy_scope}
              </li>
              <li>
                Calibration:{" "}
                {detail.detection.model_rationale.calibration_source ??
                  "unknown"}
                {" • "}
                {(
                  detail.detection.model_rationale.calibration_reason ??
                  "unknown"
                ).replace(/_/g, " ")}
              </li>
            </ul>
          </DetailSectionCard>
        )}

        {Object.keys(detail.detection.feature_attributions).length > 0 && (
          <DetailSectionCard title="Feature attributions">
            <div className="attribution-bars">
              {Object.entries(detail.detection.feature_attributions)
                .sort(([, a], [, b]) => b - a)
                .map(([feature, value]) => {
                  const maxVal = Math.max(
                    ...Object.values(detail.detection.feature_attributions),
                  );
                  const pct = maxVal > 0 ? (value / maxVal) * 100 : 0;
                  return (
                    <div className="attribution-row" key={feature}>
                      <span className="attribution-label">
                        {feature.replace(/_/g, " ")}
                      </span>
                      <div className="attribution-track">
                        <div
                          className="attribution-fill"
                          style={{ width: `${pct}%` }}
                        />
                      </div>
                      <span className="attribution-value">
                        {value.toFixed(3)}
                      </span>
                    </div>
                  );
                })}
            </div>
          </DetailSectionCard>
        )}

        <DetailSectionCard title="Peer context">
          <ul className="flat-list">
            <li>
              Role and privilege: {detail.peer_comparison.principal_role} •{" "}
              {detail.peer_comparison.principal_privilege_level}
            </li>
            <li>
              Typical location: {detail.peer_comparison.principal_location}
            </li>
            <li>
              Principal events:{" "}
              {formatNumber(detail.peer_comparison.principal_total_events)}
            </li>
            <li>
              Baseline before event:{" "}
              {formatNumber(detail.peer_comparison.principal_prior_event_count)}{" "}
              events {" • "}action seen{" "}
              {formatNumber(
                detail.peer_comparison.principal_prior_action_count,
              )}{" "}
              times
            </li>
            <li>
              Prior accesses to resource:{" "}
              {formatNumber(
                detail.peer_comparison.principal_prior_resource_access_count,
              )}
            </li>
            <li>
              Peer-group accesses:{" "}
              {formatNumber(
                detail.peer_comparison.peer_group_resource_access_count,
              )}
            </li>
            <li>
              Distinct peers on resource:{" "}
              {formatNumber(
                detail.peer_comparison.peer_group_resource_principal_count,
              )}
              /{formatNumber(detail.peer_comparison.peer_group_principal_count)}
            </li>
            <li>
              Peer sets: dept{" "}
              {formatNumber(detail.peer_comparison.department_peer_count)}
              {" • "}mgr{" "}
              {formatNumber(detail.peer_comparison.manager_peer_count)}
              {" • "}group{" "}
              {formatNumber(detail.peer_comparison.group_peer_count)}
            </li>
            <li>
              Geography seen before:{" "}
              {detail.peer_comparison.geo_seen_before ? "yes" : "no"}
            </li>
          </ul>
        </DetailSectionCard>
      </div>

      <DetailSectionCard title="Related events">
        <div className="list-stack list-stack--tight">
          {detail.events.slice(0, 6).map((event) => (
            <button
              className="mini-card mini-card--button"
              key={event.event_id}
              onClick={() => openEventPage(event.event_id)}
            >
              <div>
                {event.principal.display_name} {event.action.verb}{" "}
                {event.resource.name}
              </div>
              <div className="detail-subtle">
                {formatSourceName(event.source)} •{" "}
                {formatObservedAt(event.observed_at)}
              </div>
            </button>
          ))}
        </div>
      </DetailSectionCard>

      <DetailSectionCard title="Evidence objects">
        <div className="list-stack list-stack--tight">
          {detail.evidence_bundle.evidence_items.map((item) => (
            <div className="mini-card" key={item.pointer.object_key}>
              <div>{item.pointer.object_key}</div>
              <div className="detail-subtle">
                {item.retention_expired
                  ? "Retention expired"
                  : "Raw evidence available"}
              </div>
            </div>
          ))}
        </div>
      </DetailSectionCard>
    </div>
  );
}

export function EventDetailContent({
  event,
  formatObservedAt,
  formatSourceName,
}: EventDetailContentProps) {
  return (
    <div className="detail-pane">
      <div className="detail-hero">
        <div className="detail-hero__content">
          <span className="badge badge--neutral">
            {formatSourceName(event.source)}
          </span>
          <h3>
            {event.principal.display_name} {event.action.verb}{" "}
            {event.resource.name}
          </h3>
          <p className="detail-subtle">{formatObservedAt(event.observed_at)}</p>
        </div>
      </div>

      <DetailSectionCard title="Event summary">
        <dl className="detail-field-grid">
          <div className="detail-field">
            <dt>Actor</dt>
            <dd>{event.principal.display_name}</dd>
          </div>
          <div className="detail-field">
            <dt>Observed</dt>
            <dd>{formatObservedAt(event.observed_at)}</dd>
          </div>
          <div className="detail-field">
            <dt>Source</dt>
            <dd>{formatSourceName(event.source)}</dd>
          </div>
          <div className="detail-field">
            <dt>Action</dt>
            <dd>{event.action.verb}</dd>
          </div>
          <div className="detail-field">
            <dt>Action type</dt>
            <dd>{event.action.category}</dd>
          </div>
          <div className="detail-field">
            <dt>Resource</dt>
            <dd>{event.resource.name}</dd>
          </div>
          <div className="detail-field">
            <dt>Sensitivity</dt>
            <dd>{event.resource.sensitivity}</dd>
          </div>
        </dl>
      </DetailSectionCard>

      <DetailSectionCard
        title="Source record"
        subtitle="Trace this normalized event back to the provider event and stored raw payload."
      >
        <div className="list-stack list-stack--tight">
          <div className="mini-card">
            <div>Provider event ID</div>
            <div className="detail-subtle">{event.source_event_id}</div>
          </div>
          <div className="mini-card">
            <div>Stored raw record key</div>
            <div className="detail-subtle">{event.evidence.object_key}</div>
          </div>
        </div>
      </DetailSectionCard>

      <DetailSectionCard title="Attributes">
        <pre className="json-preview">
          {JSON.stringify(event.attributes, null, 2)}
        </pre>
      </DetailSectionCard>
    </div>
  );
}

export function IntegrationDetailContent({
  entry,
  formatNumber,
  formatObservedAt,
  integrationActionItems,
  integrationCoverageCount,
  integrationStatus,
  runtimeStatus,
}: IntegrationDetailContentProps) {
  const { source, details } = entry;
  const status = integrationStatus(details);
  const coverageCount = integrationCoverageCount(details);
  const actionItems = integrationActionItems(details);
  const deadLetterReasons = Object.entries(details.dead_letter_reason_counts)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 6);
  const projectedSourceCount =
    runtimeStatus?.projection.overview?.ops_metadata.event_counts_by_source[
      source
    ] ?? 0;
  const projectionStream = runtimeStatus?.projection.overview?.stream_state;

  return (
    <div className="detail-pane">
      <div className="detail-hero">
        <div className="detail-hero__content">
          <span className="badge badge--neutral">{source}</span>
          <h3>{details.display_name}</h3>
          <p className="detail-subtle">{status.note}</p>
        </div>
        <span className={`badge badge--${status.tone}`}>{status.label}</span>
      </div>

      <DetailSectionCard title="Integration summary">
        <dl className="detail-field-grid">
          <div className="detail-field">
            <dt>Status</dt>
            <dd>{status.label}</dd>
          </div>
          <div className="detail-field">
            <dt>Window anchor</dt>
            <dd>
              {details.recent_window_anchor_at
                ? formatObservedAt(details.recent_window_anchor_at)
                : "Waiting for source activity."}
            </dd>
          </div>
          <div className="detail-field">
            <dt>24h landed</dt>
            <dd>{formatNumber(details.raw_24h_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>24h normalized</dt>
            <dd>{formatNumber(details.normalized_24h_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>7d dead letters</dt>
            <dd>{formatNumber(details.dead_letter_7d_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>Field coverage</dt>
            <dd>
              {formatNumber(coverageCount)}/
              {formatNumber(details.required_fields.length)}
            </dd>
          </div>
        </dl>
      </DetailSectionCard>

      <div className="token-row">
        {details.missing_required_event_types.map((eventType) => (
          <span className="token token--critical" key={eventType}>
            Missing type: {eventType}
          </span>
        ))}
        {details.missing_required_fields.map((field) => (
          <span className="token token--critical" key={field}>
            Missing field: {field}
          </span>
        ))}
        {details.dead_letter_7d_count === 0 &&
        details.missing_required_event_types.length === 0 &&
        details.missing_required_fields.length === 0 ? (
          <span className="token token--positive">
            No current contract issues
          </span>
        ) : null}
      </div>

      <DetailSectionCard
        title="Next actions"
        subtitle="Use these operational cues before you resync or trust the feed."
      >
        <div className="action-list">
          {actionItems.map((action) => (
            <article
              className={`action-card action-card--${action.tone}`}
              key={action.title}
            >
              <strong>{action.title}</strong>
              <p>{action.body}</p>
            </article>
          ))}
        </div>
      </DetailSectionCard>

      <div className="detail-section-grid">
        <DetailSectionCard
          title="Configured contract"
          subtitle="What this integration is expected to emit."
        >
          <div>
            <span className="detail-label">Required event types</span>
            <div className="token-row">
              {details.required_event_types.map((eventType) => (
                <span className="token" key={eventType}>
                  {eventType}
                </span>
              ))}
            </div>
          </div>

          <div>
            <span className="detail-label">Required fields</span>
            <div className="token-row">
              {details.required_fields.map((field) => (
                <span
                  className={`token token--${
                    details.required_field_coverage[field]
                      ? "positive"
                      : "critical"
                  }`}
                  key={field}
                >
                  {field}
                </span>
              ))}
            </div>
          </div>
        </DetailSectionCard>

        <DetailSectionCard
          title="Observed state"
          subtitle="What the runtime has actually seen from this source."
        >
          <div>
            <span className="detail-label">Recent activity</span>
            <div className="token-row">
              <span className="token token--muted">
                Last raw:{" "}
                {details.raw_last_seen_at
                  ? formatObservedAt(details.raw_last_seen_at)
                  : "waiting"}
              </span>
              <span className="token token--muted">
                Last normalized:{" "}
                {details.normalized_last_seen_at
                  ? formatObservedAt(details.normalized_last_seen_at)
                  : "waiting"}
              </span>
            </div>
          </div>

          <div>
            <span className="detail-label">Seen event types</span>
            <div className="token-row">
              {details.seen_event_types.length === 0 ? (
                <span className="token token--muted">
                  No source activity seen yet
                </span>
              ) : (
                details.seen_event_types.map((eventType) => (
                  <span className="token token--muted" key={eventType}>
                    {eventType}
                  </span>
                ))
              )}
            </div>
          </div>

          <div>
            <span className="detail-label">Dead letter reasons</span>
            {deadLetterReasons.length === 0 ? (
              <div className="panel-note">
                No dead letters recorded for this integration.
              </div>
            ) : (
              <div className="list-stack list-stack--tight">
                {deadLetterReasons.map(([reason, count]) => (
                  <div className="mini-card" key={reason}>
                    <div className="record-card__header">
                      <div>
                        <h3>{reason}</h3>
                      </div>
                      <span className="badge badge--critical">
                        {formatNumber(count)}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        </DetailSectionCard>
      </div>

      <DetailSectionCard
        title="Runtime substrate"
        subtitle="What the local pipeline has actually materialized underneath the demo."
      >
        {runtimeStatus === null ? (
          <div className="panel-note">
            Runtime substrate status is still loading.
          </div>
        ) : (
          <div className="list-stack list-stack--tight">
            <div className="mini-card">
              <div className="record-card__header">
                <div>
                  <h3>Event index</h3>
                  <p className="detail-subtle">
                    Indexed normalized events available to local investigation
                    and rebuild paths.
                  </p>
                </div>
                <span
                  className={`badge badge--${
                    runtimeStatus.event_index.available
                      ? "positive"
                      : "attention"
                  }`}
                >
                  {runtimeStatus.event_index.available ? "Ready" : "Building"}
                </span>
              </div>
              <ul className="flat-list">
                <li>
                  Indexed events:{" "}
                  {formatNumber(runtimeStatus.event_index.event_count)}
                </li>
                <li>
                  Detection context:{" "}
                  {runtimeStatus.detection_context.available
                    ? "ready"
                    : "pending"}
                  {" • "}
                  {formatNumber(
                    runtimeStatus.detection_context.event_count,
                  )}{" "}
                  events
                </li>
                <li>
                  Principal keys:{" "}
                  {formatNumber(runtimeStatus.event_index.principal_key_count)}
                  {" • "}Resource keys:{" "}
                  {formatNumber(runtimeStatus.event_index.resource_key_count)}
                </li>
                <li>
                  Departments:{" "}
                  {formatNumber(runtimeStatus.event_index.department_count)}
                </li>
                <li>
                  This source in projection:{" "}
                  {formatNumber(projectedSourceCount)}
                </li>
              </ul>
            </div>

            <div className="mini-card">
              <div className="record-card__header">
                <div>
                  <h3>Feature lake</h3>
                  <p className="detail-subtle">
                    Durable feature tables used by scoring and investigation.
                  </p>
                </div>
                <span className="badge badge--neutral">
                  {formatNumber(
                    runtimeStatus.feature_tables.action_row_count +
                      runtimeStatus.feature_tables.history_row_count +
                      runtimeStatus.feature_tables.collaboration_row_count +
                      runtimeStatus.feature_tables.static_row_count +
                      runtimeStatus.feature_tables.peer_group_row_count,
                  )}{" "}
                  rows
                </span>
              </div>
              <ul className="flat-list">
                <li>
                  Scoring input: {runtimeStatus.scoring_input.mode}
                  {" • "}
                  {runtimeStatus.scoring_input.ready ? "ready" : "fallback"}
                </li>
                <li>{runtimeStatus.scoring_input.reason}</li>
                <li>
                  Action/history:{" "}
                  {formatNumber(runtimeStatus.feature_tables.action_row_count)}
                  {" / "}
                  {formatNumber(runtimeStatus.feature_tables.history_row_count)}
                </li>
                <li>
                  Collaboration/static/peer:{" "}
                  {formatNumber(
                    runtimeStatus.feature_tables.collaboration_row_count,
                  )}
                  {" / "}
                  {formatNumber(runtimeStatus.feature_tables.static_row_count)}
                  {" / "}
                  {formatNumber(
                    runtimeStatus.feature_tables.peer_group_row_count,
                  )}
                </li>
                <li>
                  Vocab principals/resources:{" "}
                  {formatNumber(runtimeStatus.feature_vocab.principal_count)}
                  {" / "}
                  {formatNumber(runtimeStatus.feature_vocab.resource_count)}
                </li>
              </ul>
            </div>

            <div className="mini-card">
              <div className="record-card__header">
                <div>
                  <h3>Identity context</h3>
                  <p className="detail-subtle">
                    Durable org/profile input backing peer groups and static
                    features.
                  </p>
                </div>
                <span
                  className={`badge badge--${
                    runtimeStatus.identity_profiles.available
                      ? "positive"
                      : "attention"
                  }`}
                >
                  {runtimeStatus.identity_profiles.source ?? "unknown"}
                </span>
              </div>
              <ul className="flat-list">
                <li>
                  Principals:{" "}
                  {formatNumber(
                    runtimeStatus.identity_profiles.principal_count,
                  )}
                </li>
                <li>
                  Teams:{" "}
                  {formatNumber(runtimeStatus.identity_profiles.team_count)}
                </li>
                <li>
                  Worker feature runs:{" "}
                  {formatNumber(runtimeStatus.worker_state.feature_runs)}
                </li>
              </ul>
            </div>

            <div className="mini-card">
              <div className="record-card__header">
                <div>
                  <h3>Model runtime</h3>
                  <p className="detail-subtle">
                    Active model policy, eval gate, and recent promotions.
                  </p>
                </div>
                <span
                  className={`badge badge--${
                    runtimeStatus.model_runtime.effective_mode === "onnx_native"
                      ? "positive"
                      : "attention"
                  }`}
                >
                  {runtimeStatus.model_runtime.effective_mode}
                </span>
              </div>
              <ul className="flat-list">
                <li>
                  Requested/effective:{" "}
                  {runtimeStatus.model_runtime.requested_mode}
                  {" / "}
                  {runtimeStatus.model_runtime.effective_mode}
                </li>
                <li>
                  Version: {runtimeStatus.model_runtime.model_version ?? "none"}
                </li>
                <li>Reason: {runtimeStatus.model_runtime.reason}</li>
                <li>
                  Eval gate:{" "}
                  {runtimeStatus.model_runtime.activation_gate.eligible
                    ? "eligible"
                    : "blocked"}
                  {" • "}
                  {runtimeStatus.model_runtime.activation_gate.reason}
                </li>
                {runtimeStatus.model_runtime.activation_gate
                  .evaluation_scope ? (
                  <li>
                    Eval scope/top1/win-rate:{" "}
                    {
                      runtimeStatus.model_runtime.activation_gate
                        .evaluation_scope
                    }
                    {" • "}
                    {runtimeStatus.model_runtime.activation_gate.sampled_top1_accuracy?.toFixed(
                      2,
                    ) ?? "n/a"}
                    {" • "}
                    {runtimeStatus.model_runtime.activation_gate.pairwise_win_rate?.toFixed(
                      2,
                    ) ?? "n/a"}
                  </li>
                ) : null}
                {runtimeStatus.model_runtime.activation_gate.mean_margin !=
                null ? (
                  <li>
                    Mean margin:{" "}
                    {runtimeStatus.model_runtime.activation_gate.mean_margin.toFixed(
                      3,
                    )}
                  </li>
                ) : null}
                {runtimeStatus.model_runtime.activation_gate
                  .evaluated_source_count > 0 ? (
                  <li>
                    Source gates:{" "}
                    {Object.entries(
                      runtimeStatus.model_runtime.activation_gate
                        .source_gates ?? {},
                    )
                      .map(
                        ([source, gate]) =>
                          `${source}:${gate.eligible ? "pass" : "block"}`,
                      )
                      .join(" • ")}
                  </li>
                ) : null}
                {(
                  runtimeStatus.model_runtime.activation_gate.failing_sources ??
                  []
                ).length > 0 ? (
                  <li>
                    Blocking sources:{" "}
                    {(
                      runtimeStatus.model_runtime.activation_gate
                        .failing_sources ?? []
                    ).join(", ")}
                  </li>
                ) : null}
                {runtimeStatus.model_runtime.activation_gate.coverage_gate ? (
                  <li>
                    Coverage gate:{" "}
                    {runtimeStatus.model_runtime.activation_gate.coverage_gate
                      .eligible
                      ? "eligible"
                      : "blocked"}
                    {" • "}
                    {
                      runtimeStatus.model_runtime.activation_gate.coverage_gate
                        .reason
                    }
                    {" • "}sources{" "}
                    {(
                      runtimeStatus.model_runtime.activation_gate.coverage_gate
                        .covered_sources ?? []
                    ).join(", ") || "none"}
                  </li>
                ) : null}
                <li>
                  Installed models:{" "}
                  {formatNumber(
                    runtimeStatus.model_runtime.installed_model_count,
                  )}
                </li>
                <li>
                  Recent promotions:{" "}
                  {formatNumber(
                    runtimeStatus.model_runtime.recent_activation_history
                      .length,
                  )}
                </li>
                {runtimeStatus.model_runtime.recent_activation_history
                  .slice()
                  .reverse()
                  .slice(0, 2)
                  .map((entry, index) => (
                    <li key={`${entry.activated_at ?? "none"}-${index}`}>
                      {entry.action}: {entry.model_id ?? "heuristic"}
                      {" • "}
                      {entry.activation_source ?? "unknown"}
                    </li>
                  ))}
              </ul>
            </div>

            <div className="mini-card">
              <div className="record-card__header">
                <div>
                  <h3>Projection</h3>
                  <p className="detail-subtle">
                    Postgres-backed serving state for the UI and API.
                  </p>
                </div>
                <span
                  className={`badge badge--${
                    runtimeStatus.projection.available ? "positive" : "critical"
                  }`}
                >
                  {runtimeStatus.projection.available ? "Connected" : "Offline"}
                </span>
              </div>
              <ul className="flat-list">
                <li>
                  Projected normalized events:{" "}
                  {formatNumber(projectionStream?.normalized_event_count ?? 0)}
                </li>
                <li>
                  Projected detections:{" "}
                  {formatNumber(projectionStream?.detection_count ?? 0)}
                </li>
                <li>
                  Pending intake batches:{" "}
                  {formatNumber(runtimeStatus.worker_state.pending_batch_count)}
                </li>
              </ul>
              {runtimeStatus.projection.error ? (
                <div className="panel-note">
                  {runtimeStatus.projection.error}
                </div>
              ) : null}
            </div>
          </div>
        )}
      </DetailSectionCard>
    </div>
  );
}
