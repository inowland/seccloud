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
  badgeToneForSeverity: (severity: string) => BadgeTone;
  detail: DetectionDetail;
  openEventPage: (eventId: string) => void;
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
  badgeToneForSeverity,
  detail,
  formatNumber,
  formatObservedAt,
  formatSourceName,
  openEventPage,
}: DetectionDetailContentProps) {
  return (
    <div className="detail-pane">
      <div className="detail-hero">
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
        </div>
      </div>

      <div className="mini-metrics">
        <div className="mini-metric">
          <span>Score</span>
          <strong>{detail.detection.score.toFixed(2)}</strong>
        </div>
        <div className="mini-metric">
          <span>Confidence</span>
          <strong>{detail.detection.confidence.toFixed(2)}</strong>
        </div>
        <div className="mini-metric">
          <span>Evidence</span>
          <strong>
            {formatNumber(detail.evidence_bundle.evidence_items.length)}
          </strong>
        </div>
      </div>

      <div className="detail-section-grid">
        <DetailSectionCard title="Reasons">
          <ul className="flat-list">
            {detail.detection.reasons.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
          </ul>
        </DetailSectionCard>

        <DetailSectionCard title="Peer context">
          <div className="panel-note">
            Principal events:{" "}
            {formatNumber(detail.peer_comparison.principal_total_events)}. Prior
            accesses to resource:{" "}
            {formatNumber(
              detail.peer_comparison.principal_prior_resource_access_count,
            )}
            . Peer-group accesses:{" "}
            {formatNumber(
              detail.peer_comparison.peer_group_resource_access_count,
            )}
            .
          </div>
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
        <span className="badge badge--neutral">
          {formatSourceName(event.source)}
        </span>
        <h3>{event.principal.display_name}</h3>
        <p className="detail-subtle">
          {formatObservedAt(event.observed_at)} • {event.action.verb}
        </p>
      </div>

      <div className="mini-metrics">
        <div className="mini-metric">
          <span>Resource</span>
          <strong>{event.resource.name}</strong>
        </div>
        <div className="mini-metric">
          <span>Sensitivity</span>
          <strong>{event.resource.sensitivity}</strong>
        </div>
        <div className="mini-metric">
          <span>Evidence</span>
          <strong>{event.evidence.object_key}</strong>
        </div>
      </div>

      <DetailSectionCard title="Overview">
        <div className="panel-note">
          {event.action.verb} {event.resource.name} on{" "}
          {formatSourceName(event.source)}.
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
        <div>
          <h3>{details.display_name}</h3>
          <p className="detail-subtle">{source}</p>
        </div>
        <span className={`badge badge--${status.tone}`}>{status.label}</span>
      </div>

      <div className="panel-note">{status.note}</div>
      <div className="panel-note">
        Window anchor:{" "}
        {details.recent_window_anchor_at
          ? formatObservedAt(details.recent_window_anchor_at)
          : "Waiting for source activity."}
      </div>

      <div className="mini-metrics">
        <div className="mini-metric">
          <span>24h landed</span>
          <strong>{formatNumber(details.raw_24h_count)}</strong>
        </div>
        <div className="mini-metric">
          <span>24h normalized</span>
          <strong>{formatNumber(details.normalized_24h_count)}</strong>
        </div>
        <div className="mini-metric">
          <span>7d dead letters</span>
          <strong>{formatNumber(details.dead_letter_7d_count)}</strong>
        </div>
        <div className="mini-metric">
          <span>Field coverage</span>
          <strong>
            {formatNumber(coverageCount)}/
            {formatNumber(details.required_fields.length)}
          </strong>
        </div>
      </div>

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
        {details.dead_letter_count === 0 &&
        details.missing_required_event_types.length === 0 &&
        details.missing_required_fields.length === 0 ? (
          <span className="token token--positive">
            Contract currently satisfied
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
