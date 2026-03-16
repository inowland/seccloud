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
              Principal events:{" "}
              {formatNumber(detail.peer_comparison.principal_total_events)}
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
}: IntegrationDetailContentProps) {
  const { source, details } = entry;
  const status = integrationStatus(details);
  const coverageCount = integrationCoverageCount(details);
  const actionItems = integrationActionItems(details);
  const deadLetterReasons = Object.entries(details.dead_letter_reason_counts)
    .sort((left, right) => right[1] - left[1])
    .slice(0, 6);

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
    </div>
  );
}
