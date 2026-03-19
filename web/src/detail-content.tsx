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
type EntityDetail = JsonResponse<"/api/entities/{principal_key}", "get">;
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
  openEntityPage: (principalKey: string) => void;
  openEventPage: (eventId: string) => void;
  stackSummaryCards?: boolean;
}

interface EntityDetailContentProps extends FormattingHelpers {
  detail: EntityDetail;
  openEventPage: (eventId: string) => void;
}

interface EventDetailContentProps extends Pick<
  FormattingHelpers,
  "formatObservedAt" | "formatSourceName"
> {
  event: EventDetail;
  openEntityPage: (principalKey: string) => void;
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

interface PipelineStatusContentProps extends Pick<
  FormattingHelpers,
  "formatNumber" | "formatObservedAt"
> {
  runtimeStatus: RuntimeStatus | null;
}

function topAttributions(detail: DetectionDetail): Array<[string, number]> {
  return Object.entries(detail.detection.feature_attributions)
    .sort(([, left], [, right]) => right - left)
    .slice(0, 3);
}

function formatBytes(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) {
    return "n/a";
  }
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${(value / 1024).toFixed(1)} KB`;
  }
  if (value < 1024 * 1024 * 1024) {
    return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  }
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function pipelineStatusTone(
  ready: boolean,
  warning = false,
  critical = false,
): BadgeTone {
  if (critical) {
    return "critical";
  }
  if (ready && !warning) {
    return "positive";
  }
  if (warning) {
    return "attention";
  }
  return "neutral";
}

function detectionHighlights(
  detail: DetectionDetail,
  formatNumber: (value: number | undefined) => string,
  formatSourceName: (source: string) => string,
): string[] {
  const anchorEvent = detail.events[0];
  const topFeatures = topAttributions(detail);
  const highlights = [
    `${detail.detection.title} fired with score ${detail.detection.score.toFixed(2)} and confidence ${detail.detection.confidence.toFixed(2)}.`,
  ];

  if (anchorEvent) {
    highlights.push(
      `${anchorEvent.principal.display_name} ${anchorEvent.action.verb} ${anchorEvent.resource.name} in ${formatSourceName(anchorEvent.source)}.`,
    );
  }
  if (detail.peer_comparison.principal_prior_resource_access_count === 0) {
    highlights.push("The anchor resource appears new for this principal.");
  }
  if (!detail.peer_comparison.geo_seen_before) {
    highlights.push(
      "The activity occurred from a geography not seen previously.",
    );
  }
  if (detail.peer_comparison.peer_group_principal_count > 0) {
    highlights.push(
      `${formatNumber(detail.peer_comparison.peer_group_resource_principal_count)} of ${formatNumber(detail.peer_comparison.peer_group_principal_count)} peers touched the same resource.`,
    );
  }
  if (topFeatures.length > 0) {
    highlights.push(
      `Top model drivers were ${topFeatures
        .map(([feature]) => feature.replace(/_/g, " "))
        .join(", ")}.`,
    );
  }

  return highlights;
}

function detectionNextActions(
  detail: DetectionDetail,
  formatSourceName: (source: string) => string,
): ActionCard[] {
  const anchorEvent = detail.events[0];
  const actions: ActionCard[] = [];

  actions.push({
    title: "Pivot into the principal",
    body: "Review the principal timeline first to confirm whether this alert fits a larger behavior shift or a one-off event.",
    tone: "attention",
  });

  if (anchorEvent) {
    actions.push({
      title: "Validate the anchor event",
      body: `Start with the ${formatSourceName(anchorEvent.source)} event and confirm the actor, action, resource, and sensitivity match the detection story.`,
      tone: "neutral",
    });
  }

  if (detail.peer_comparison.principal_prior_resource_access_count === 0) {
    actions.push({
      title: "Check for first-time access",
      body: "This looks like a first-seen resource for the principal. Confirm whether the access was expected for their role or current task.",
      tone: "critical",
    });
  }

  if (!detail.peer_comparison.geo_seen_before) {
    actions.push({
      title: "Verify geography change",
      body: "The location context appears new. Confirm whether this was travel, VPN behavior, or a stronger compromise indicator.",
      tone: "critical",
    });
  }

  if (
    detail.evidence_bundle.evidence_items.some(
      (item) => !item.retention_expired,
    )
  ) {
    actions.push({
      title: "Inspect raw evidence",
      body: "Raw evidence is still available. Use it to verify that the normalized event and feature attribution match the source record.",
      tone: "positive",
    });
  }

  return actions.slice(0, 4);
}

function rawPayloadPreview(
  payload: Record<string, unknown> | null,
): string | null {
  if (payload === null) {
    return null;
  }
  const pretty = JSON.stringify(payload, null, 2);
  if (pretty.length <= 900) {
    return pretty;
  }
  return `${pretty.slice(0, 900)}\n...`;
}

export function DetectionDetailContent({
  acknowledging,
  badgeToneForSeverity,
  detail,
  formatNumber,
  formatObservedAt,
  formatSourceName,
  onAcknowledge,
  openEntityPage,
  openEventPage,
  stackSummaryCards = false,
}: DetectionDetailContentProps) {
  const principalId =
    detail.events[0]?.principal.id ?? detail.detection.related_entity_ids[0];
  const anchorEvent = detail.events[0];
  const highlights = detectionHighlights(
    detail,
    formatNumber,
    formatSourceName,
  );
  const nextActions = detectionNextActions(detail, formatSourceName);
  const topFeatures = topAttributions(detail);
  const distinctSources = Array.from(
    new Set(
      detail.events
        .map((event) => event.source)
        .filter((source): source is string => Boolean(source)),
    ),
  );
  const anchorEvidenceItem =
    anchorEvent === undefined
      ? null
      : (detail.evidence_bundle.evidence_items.find(
          (item) =>
            item.pointer.object_key === anchorEvent.evidence.object_key ||
            item.pointer.raw_event_id === anchorEvent.evidence.raw_event_id,
        ) ?? null);
  const anchorPayloadPreview = rawPayloadPreview(
    anchorEvidenceItem?.raw_payload ?? null,
  );

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
          {principalId ? (
            <button onClick={() => openEntityPage(principalId)} type="button">
              Investigate Principal
            </button>
          ) : null}
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

      <DetailSectionCard
        title="Investigation summary"
        subtitle="What changed and why this alert matters."
      >
        <ul className="flat-list">
          {highlights.map((highlight) => (
            <li key={highlight}>{highlight}</li>
          ))}
        </ul>
      </DetailSectionCard>

      <DetailSectionCard
        title="Next actions"
        subtitle="Recommended pivots before deciding whether to dismiss or escalate."
      >
        <div className="action-list">
          {nextActions.map((action) => (
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

      <div
        className={
          stackSummaryCards
            ? "detail-section-grid detail-section-grid--stacked"
            : "detail-section-grid"
        }
      >
        <DetailSectionCard
          title="What changed"
          subtitle="Behavior shifts visible in the current alert context."
        >
          <ul className="flat-list">
            <li>
              Prior accesses to resource:{" "}
              {formatNumber(
                detail.peer_comparison.principal_prior_resource_access_count,
              )}
            </li>
            <li>
              Baseline before event:{" "}
              {formatNumber(detail.peer_comparison.principal_prior_event_count)}{" "}
              events
            </li>
            <li>
              Prior action count:{" "}
              {formatNumber(
                detail.peer_comparison.principal_prior_action_count,
              )}
            </li>
            <li>
              Geography seen before:{" "}
              {detail.peer_comparison.geo_seen_before ? "yes" : "no"}
            </li>
            <li>
              Distinct peers on resource:{" "}
              {formatNumber(
                detail.peer_comparison.peer_group_resource_principal_count,
              )}
              /{formatNumber(detail.peer_comparison.peer_group_principal_count)}
            </li>
          </ul>
        </DetailSectionCard>

        <DetailSectionCard
          title="Why It Fired"
          subtitle="Policy reasons and model signal for this detection."
        >
          <ul className="flat-list">
            {detail.detection.reasons.map((reason) => (
              <li key={reason}>{reason}</li>
            ))}
            {detail.detection.model_rationale ? (
              <li>
                Threshold{" "}
                {detail.detection.model_rationale.detection_threshold.toFixed(
                  2,
                )}
                {" • "}score{" "}
                {detail.detection.model_rationale.model_score.toFixed(2)}
                {" • "}margin{" "}
                {detail.detection.model_rationale.score_margin.toFixed(2)}
              </li>
            ) : null}
            {topFeatures.length > 0 ? (
              <li>
                Top feature signals:{" "}
                {topFeatures
                  .map(([feature]) => feature.replace(/_/g, " "))
                  .join(", ")}
              </li>
            ) : null}
          </ul>
        </DetailSectionCard>

        <DetailSectionCard
          title="Evidence chain"
          subtitle="How the alert connects to normalized events and raw proof."
        >
          <ul className="flat-list">
            <li>
              Related normalized events: {formatNumber(detail.events.length)}
            </li>
            <li>
              Raw evidence objects:{" "}
              {formatNumber(detail.evidence_bundle.evidence_items.length)}
            </li>
            <li>
              Raw evidence available now:{" "}
              {
                detail.evidence_bundle.evidence_items.filter(
                  (item) => !item.retention_expired,
                ).length
              }
            </li>
            <li>
              Sources involved:{" "}
              {distinctSources
                .map((source) => formatSourceName(source))
                .join(", ")}
            </li>
            {anchorEvent ? (
              <li>
                Anchor event: {anchorEvent.action.verb}{" "}
                {anchorEvent.resource.name}
              </li>
            ) : null}
          </ul>
        </DetailSectionCard>

        <DetailSectionCard
          title="Anchor event"
          subtitle="The event that best represents the detection narrative."
        >
          {anchorEvent ? (
            <div className="list-stack list-stack--tight">
              <div className="mini-card">
                <div className="mini-card__header">
                  <strong>
                    {anchorEvent.principal.display_name}{" "}
                    {anchorEvent.action.verb} {anchorEvent.resource.name}
                  </strong>
                  <span className="badge badge--neutral">
                    {formatSourceName(anchorEvent.source)}
                  </span>
                </div>
                <div className="detail-subtle">
                  {formatObservedAt(anchorEvent.observed_at)} •{" "}
                  {anchorEvent.action.category} •{" "}
                  {anchorEvent.resource.sensitivity}
                </div>
                <div className="token-row">
                  <span className="token token--muted">
                    event {anchorEvent.event_id}
                  </span>
                  <span className="token token--muted">
                    raw {anchorEvent.evidence.raw_event_id}
                  </span>
                </div>
              </div>
              {anchorPayloadPreview ? (
                <pre className="json-preview">{anchorPayloadPreview}</pre>
              ) : (
                <div className="panel-note">
                  Raw anchor payload is not currently available in the evidence
                  bundle.
                </div>
              )}
            </div>
          ) : (
            <div className="panel-note">
              No anchor event is available for this detection.
            </div>
          )}
        </DetailSectionCard>

        <DetailSectionCard title="Model rationale">
          {detail.detection.model_rationale ? (
            <ul className="flat-list">
              <li>
                Scoring mode: {detail.detection.model_rationale.scoring_mode}
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
          ) : (
            <div className="panel-note">
              No explicit model rationale was returned for this detection.
            </div>
          )}
        </DetailSectionCard>

        <DetailSectionCard title="Feature attributions">
          {Object.keys(detail.detection.feature_attributions).length > 0 ? (
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
          ) : (
            <div className="panel-note">
              No feature attribution data was returned for this detection.
            </div>
          )}
        </DetailSectionCard>

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
              Peer-group accesses:{" "}
              {formatNumber(
                detail.peer_comparison.peer_group_resource_access_count,
              )}
            </li>
            <li>
              Peer sets: dept{" "}
              {formatNumber(detail.peer_comparison.department_peer_count)}
              {" • "}mgr{" "}
              {formatNumber(detail.peer_comparison.manager_peer_count)}
              {" • "}group{" "}
              {formatNumber(detail.peer_comparison.group_peer_count)}
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
              <div className="mini-card__header">
                <strong>
                  {event.principal.display_name} {event.action.verb}{" "}
                  {event.resource.name}
                </strong>
                {anchorEvent && event.event_id === anchorEvent.event_id ? (
                  <span className="badge badge--attention">Anchor</span>
                ) : null}
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
              <div className="mini-card__header">
                <strong>{item.pointer.object_key}</strong>
                {anchorEvidenceItem &&
                item.pointer.object_key ===
                  anchorEvidenceItem.pointer.object_key ? (
                  <span className="badge badge--attention">Anchor</span>
                ) : null}
              </div>
              <div className="detail-subtle">
                {item.retention_expired
                  ? "Retention expired"
                  : "Raw evidence available"}
              </div>
              {item.raw_payload ? (
                <pre className="json-preview">
                  {rawPayloadPreview(item.raw_payload)}
                </pre>
              ) : null}
            </div>
          ))}
        </div>
      </DetailSectionCard>
    </div>
  );
}

function entityHighlights(
  detail: EntityDetail,
  formatSourceName: (source: string) => string,
  formatNumber: (value: number | undefined) => string,
): string[] {
  const latestEvent = detail.timeline[0];
  const highlights = [
    `${detail.principal.display_name} shows ${formatNumber(detail.overview.timeline_event_count)} recent events across ${formatNumber(detail.overview.distinct_source_count)} source${detail.overview.distinct_source_count === 1 ? "" : "s"}.`,
  ];

  if (latestEvent) {
    highlights.push(
      `Most recent activity was ${latestEvent.action.verb} on ${latestEvent.resource.name} in ${formatSourceName(latestEvent.source)}.`,
    );
  }
  if (detail.peer_comparison.principal_prior_resource_access_count === 0) {
    highlights.push(
      "The current resource appears to be new for this principal.",
    );
  }
  if (!detail.peer_comparison.geo_seen_before) {
    highlights.push(
      "The current geography appears new relative to prior behavior.",
    );
  }
  if (detail.peer_comparison.peer_group_principal_count > 0) {
    highlights.push(
      `${formatNumber(detail.peer_comparison.peer_group_resource_principal_count)} of ${formatNumber(detail.peer_comparison.peer_group_principal_count)} peers touched the anchor resource.`,
    );
  }
  return highlights;
}

export function EntityDetailContent({
  detail,
  formatNumber,
  formatObservedAt,
  formatSourceName,
  openEventPage,
}: EntityDetailContentProps) {
  const latestEvent = detail.timeline[0];
  const highlights = entityHighlights(detail, formatSourceName, formatNumber);

  return (
    <div className="detail-pane">
      <div className="detail-hero">
        <div className="detail-hero__content">
          <div>
            <span className="badge badge--neutral">
              {detail.principal.department}
            </span>
            <h3>{detail.principal.display_name}</h3>
            <p className="detail-subtle">{detail.principal.email}</p>
          </div>
        </div>
      </div>

      <DetailSectionCard title="Principal summary">
        <dl className="detail-field-grid">
          <div className="detail-field">
            <dt>Department</dt>
            <dd>{detail.principal.department}</dd>
          </div>
          <div className="detail-field">
            <dt>Role / privilege</dt>
            <dd>
              {detail.peer_comparison.principal_role} /{" "}
              {detail.peer_comparison.principal_privilege_level}
            </dd>
          </div>
          <div className="detail-field">
            <dt>Recent events shown</dt>
            <dd>{formatNumber(detail.overview.timeline_event_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>Total events</dt>
            <dd>{formatNumber(detail.overview.total_event_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>Sources</dt>
            <dd>{formatNumber(detail.overview.distinct_source_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>Resources touched</dt>
            <dd>{formatNumber(detail.overview.distinct_resource_count)}</dd>
          </div>
          <div className="detail-field">
            <dt>High-sensitivity events</dt>
            <dd>
              {formatNumber(detail.overview.high_sensitivity_event_count)}
            </dd>
          </div>
          <div className="detail-field">
            <dt>Latest activity</dt>
            <dd>
              {detail.overview.latest_observed_at
                ? formatObservedAt(detail.overview.latest_observed_at)
                : "Unknown"}
            </dd>
          </div>
        </dl>
      </DetailSectionCard>

      <div className="detail-section-grid">
        <DetailSectionCard
          title="Investigation summary"
          subtitle="What currently stands out for this principal."
        >
          <ul className="flat-list">
            {highlights.map((highlight) => (
              <li key={highlight}>{highlight}</li>
            ))}
          </ul>
        </DetailSectionCard>

        <DetailSectionCard title="Peer context">
          <ul className="flat-list">
            <li>
              Typical location: {detail.peer_comparison.principal_location}
            </li>
            <li>
              Prior action count:{" "}
              {formatNumber(
                detail.peer_comparison.principal_prior_action_count,
              )}
            </li>
            <li>
              Prior resource accesses:{" "}
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
              Geography seen before:{" "}
              {detail.peer_comparison.geo_seen_before ? "yes" : "no"}
            </li>
          </ul>
        </DetailSectionCard>
      </div>

      <DetailSectionCard title="Source coverage">
        <div className="token-row">
          {detail.overview.distinct_sources.map((source) => (
            <span className="token token--muted" key={source}>
              {formatSourceName(source)}
            </span>
          ))}
        </div>
      </DetailSectionCard>

      <DetailSectionCard
        title="Principal timeline"
        subtitle="Recent cross-source activity for this principal."
      >
        <div className="list-stack list-stack--tight">
          {detail.timeline.map((event) => (
            <button
              className="mini-card mini-card--button"
              key={event.event_id}
              onClick={() => openEventPage(event.event_id)}
            >
              <div className="mini-card__header">
                <strong>
                  {event.action.verb} {event.resource.name}
                </strong>
                <span className="badge badge--neutral">
                  {formatSourceName(event.source)}
                </span>
              </div>
              <div className="detail-subtle">
                {formatObservedAt(event.observed_at)} • {event.action.category}{" "}
                • {event.resource.sensitivity}
              </div>
            </button>
          ))}
        </div>
      </DetailSectionCard>

      {latestEvent ? (
        <DetailSectionCard title="Latest anchor event">
          <div className="mini-card">
            <div className="mini-card__header">
              <strong>
                {latestEvent.principal.display_name} {latestEvent.action.verb}{" "}
                {latestEvent.resource.name}
              </strong>
              <span className="badge badge--neutral">
                {formatSourceName(latestEvent.source)}
              </span>
            </div>
            <div className="detail-subtle">
              {formatObservedAt(latestEvent.observed_at)} •{" "}
              {latestEvent.resource.sensitivity}
            </div>
          </div>
        </DetailSectionCard>
      ) : null}
    </div>
  );
}

export function EventDetailContent({
  event,
  formatObservedAt,
  formatSourceName,
  openEntityPage,
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
        <div className="detail-hero__actions">
          <button
            onClick={() => openEntityPage(event.principal.id)}
            type="button"
          >
            Investigate Principal
          </button>
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
          <span className="badge badge--critical" key={eventType}>
            Missing type: {eventType}
          </span>
        ))}
        {details.missing_required_fields.map((field) => (
          <span className="badge badge--critical" key={field}>
            Missing field: {field}
          </span>
        ))}
        {details.dead_letter_7d_count === 0 &&
        details.missing_required_event_types.length === 0 &&
        details.missing_required_fields.length === 0 ? (
          <span className="badge badge--positive">
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
                <span className="badge badge--neutral" key={eventType}>
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
                  className={`badge badge--${
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
              <span className="badge badge--neutral">
                Last raw:{" "}
                {details.raw_last_seen_at
                  ? formatObservedAt(details.raw_last_seen_at)
                  : "waiting"}
              </span>
              <span className="badge badge--neutral">
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
                <span className="badge badge--neutral">
                  No source activity seen yet
                </span>
              ) : (
                details.seen_event_types.map((eventType) => (
                  <span className="badge badge--neutral" key={eventType}>
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
        subtitle="What the local runtime has actually materialized."
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
                  Normalized events for this source:{" "}
                  {formatNumber(details.normalized_event_count)}
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
          </div>
        )}
      </DetailSectionCard>
    </div>
  );
}

export function PipelineStatusContent({
  formatNumber,
  formatObservedAt,
  runtimeStatus,
}: PipelineStatusContentProps) {
  if (runtimeStatus === null) {
    return <div className="panel-note">Pipeline state is still loading.</div>;
  }

  const streamState = runtimeStatus.stream_state;
  const quickwitHealthy =
    runtimeStatus.quickwit.running &&
    runtimeStatus.quickwit.ready &&
    runtimeStatus.quickwit.last_sync_status !== "failed";
  const scoringHealthy = runtimeStatus.scoring_input.ready;
  const eventPlaneHealthy =
    runtimeStatus.event_index.available &&
    runtimeStatus.detection_context.available;
  const modelActive =
    runtimeStatus.model_runtime.effective_mode === "onnx_native";
  const runtimeReady =
    runtimeStatus.quickwit.initialized &&
    runtimeStatus.quickwit.running &&
    runtimeStatus.quickwit.ready;
  const intakeBacklogCount = runtimeStatus.intake.pending_batch_count;
  const processingBacklogCount = Math.max(
    0,
    (streamState.normalized_event_count ?? 0) -
      runtimeStatus.event_index.event_count,
  );
  const streamBufferedCount = Math.max(
    0,
    streamState.total_source_events - streamState.cursor,
  );
  const streamPrimed = streamState.total_source_events > 0;
  const streamLikelyAdvancing =
    intakeBacklogCount > 0 || processingBacklogCount > 0;
  const streamStatusLabel = streamLikelyAdvancing
    ? "Advancing"
    : streamBufferedCount > 0
      ? "Buffered"
      : streamPrimed && !streamState.complete
        ? "Paused"
        : streamState.complete
          ? "Complete"
          : "Idle";
  const searchBacklogCount = Math.max(
    0,
    runtimeStatus.event_index.event_count -
      runtimeStatus.quickwit.indexed_event_count,
  );
  const anyBacklog =
    intakeBacklogCount > 0 ||
    processingBacklogCount > 0 ||
    searchBacklogCount > 0;
  const caughtUp =
    intakeBacklogCount === 0 &&
    processingBacklogCount === 0 &&
    searchBacklogCount === 0;
  const freshnessTone = pipelineStatusTone(caughtUp, anyBacklog);
  const modelTone = pipelineStatusTone(
    modelActive && scoringHealthy,
    !modelActive,
  );
  const searchTone = pipelineStatusTone(
    quickwitHealthy && searchBacklogCount === 0,
    quickwitHealthy && searchBacklogCount > 0,
    !quickwitHealthy,
  );
  const runtimeStatusDetail = !runtimeStatus.quickwit.initialized
    ? "Quickwit has not been initialized."
    : !runtimeStatus.quickwit.running
      ? "Quickwit is not running."
      : !runtimeStatus.quickwit.ready
        ? "Quickwit is still starting."
        : "All required local services are up.";
  const searchStatusDetail = !quickwitHealthy
    ? runtimeStatus.quickwit.last_sync_status === "failed"
      ? "Quickwit sync failed."
      : !runtimeStatus.quickwit.running
        ? "Quickwit is not running."
        : !runtimeStatus.quickwit.ready
          ? "Quickwit is still starting."
          : "Quickwit is not healthy yet."
    : searchBacklogCount > 0
      ? `${formatNumber(searchBacklogCount)} searchable event${searchBacklogCount === 1 ? "" : "s"} are still missing from Quickwit.`
      : "Quickwit is current with the event index.";
  const freshnessSummary = caughtUp
    ? "All visible stages are caught up."
    : [
        intakeBacklogCount > 0
          ? `${formatNumber(intakeBacklogCount)} intake batch${intakeBacklogCount === 1 ? "" : "es"} queued`
          : null,
        processingBacklogCount > 0
          ? `${formatNumber(processingBacklogCount)} source event${processingBacklogCount === 1 ? "" : "s"} waiting on processing`
          : null,
        searchBacklogCount > 0
          ? `${formatNumber(searchBacklogCount)} searchable event${searchBacklogCount === 1 ? "" : "s"} still missing from Quickwit`
          : null,
        streamLikelyAdvancing ? "ingest stream still advancing" : null,
        !streamLikelyAdvancing && streamBufferedCount > 0
          ? `${formatNumber(streamBufferedCount)} source event${streamBufferedCount === 1 ? "" : "s"} buffered for future intake`
          : null,
      ]
        .filter((value): value is string => Boolean(value))
        .join(" • ");
  const formatMaybeObservedAt = (value: string | null | undefined) =>
    value ? formatObservedAt(value) : "never";

  return (
    <div className="detail-pane">
      <DetailSectionCard title="Pipeline">
        <div className="record-card__header">
          <div>
            <strong>
              {caughtUp
                ? "Everything is up to date."
                : "The pipeline is still catching up."}
            </strong>
            <p className="detail-subtle">{freshnessSummary}</p>
          </div>
          <span className={`badge badge--${freshnessTone}`}>
            {caughtUp ? "Ready" : "Catching up"}
          </span>
        </div>
        <div className="record-card__summary">
          <div className="record-card__summary-item">
            <span>Intake queue</span>
            <strong>{formatNumber(intakeBacklogCount)}</strong>
          </div>
          <div className="record-card__summary-item">
            <span>Processing lag</span>
            <strong>{formatNumber(processingBacklogCount)}</strong>
          </div>
          <div className="record-card__summary-item">
            <span>Search lag</span>
            <strong>{formatNumber(searchBacklogCount)}</strong>
          </div>
          <div className="record-card__summary-item">
            <span>Stream state</span>
            <strong>{streamStatusLabel}</strong>
          </div>
        </div>
        <ul className="flat-list pipeline-checklist">
          <li>
            <div className="pipeline-checklist__body">
              <span>Runtime bootstrapped and services running</span>
              <span className="detail-subtle">{runtimeStatusDetail}</span>
            </div>
            <span
              className={`badge badge--${pipelineStatusTone(
                runtimeReady,
                false,
                !runtimeReady,
              )}`}
            >
              {runtimeReady ? "Ready" : "Not ready"}
            </span>
          </li>
          <li>
            <div className="pipeline-checklist__body">
              <span>Processing is caught up to the current stream</span>
              <span className="detail-subtle">
                {processingBacklogCount === 0 && intakeBacklogCount === 0
                  ? "No intake or processing backlog is visible."
                  : `${formatNumber(intakeBacklogCount)} queued batch${intakeBacklogCount === 1 ? "" : "es"} and ${formatNumber(processingBacklogCount)} source event${processingBacklogCount === 1 ? "" : "s"} still waiting on processing.`}
              </span>
            </div>
            <span
              className={`badge badge--${pipelineStatusTone(
                processingBacklogCount === 0 && intakeBacklogCount === 0,
                processingBacklogCount > 0 || intakeBacklogCount > 0,
              )}`}
            >
              {processingBacklogCount === 0 && intakeBacklogCount === 0
                ? "Ready"
                : "Catching up"}
            </span>
          </li>
          <li>
            <div className="pipeline-checklist__body">
              <span>Investigation surfaces are online</span>
              <span className="detail-subtle">
                {eventPlaneHealthy
                  ? "Event index and detection context are available."
                  : "One or more investigation layers are still unavailable."}
              </span>
            </div>
            <span
              className={`badge badge--${pipelineStatusTone(
                eventPlaneHealthy,
                false,
                !eventPlaneHealthy,
              )}`}
            >
              {eventPlaneHealthy ? "Ready" : "Not ready"}
            </span>
          </li>
          <li>
            <div className="pipeline-checklist__body">
              <span>Event search is current and healthy</span>
              <span className="detail-subtle">{searchStatusDetail}</span>
            </div>
            <span className={`badge badge--${searchTone}`}>
              {quickwitHealthy && searchBacklogCount === 0
                ? "Ready"
                : runtimeStatus.quickwit.last_sync_status === "failed"
                  ? "Error"
                  : "Catching up"}
            </span>
          </li>
        </ul>
      </DetailSectionCard>

      <DetailSectionCard title="Advanced details">
        <div className="pipeline-advanced-list">
          <details className="pipeline-advanced">
            <summary className="pipeline-advanced__summary">
              Intake and processing
              <span className={`badge badge--${freshnessTone}`}>
                {caughtUp ? "Current" : "Backlog"}
              </span>
            </summary>
            <ul className="flat-list">
              <li>
                Pending/processed batches:{" "}
                {formatNumber(runtimeStatus.intake.pending_batch_count)}
                {" / "}
                {formatNumber(runtimeStatus.intake.processed_batch_count)}
              </li>
              <li>
                Submitted/idempotency keys:{" "}
                {formatNumber(runtimeStatus.intake.submitted_batch_count)}
                {" / "}
                {formatNumber(
                  runtimeStatus.intake.accepted_idempotency_key_count,
                )}
              </li>
              <li>
                Stream cursor: {formatNumber(streamState.cursor)} of{" "}
                {formatNumber(streamState.total_source_events)}
              </li>
              <li>
                Normalized/indexed events:{" "}
                {formatNumber(streamState.normalized_event_count ?? 0)}
                {" / "}
                {formatNumber(runtimeStatus.event_index.event_count)}
              </li>
              <li>
                Last submitted/processed batch:{" "}
                {runtimeStatus.worker_state.last_submitted_batch_id ?? "none"}
                {" / "}
                {runtimeStatus.worker_state.last_processed_batch_id ?? "none"}
              </li>
            </ul>
          </details>

          <details className="pipeline-advanced">
            <summary className="pipeline-advanced__summary">
              Event plane and Quickwit
              <span className={`badge badge--${searchTone}`}>
                {quickwitHealthy && searchBacklogCount === 0
                  ? "Healthy"
                  : runtimeStatus.quickwit.last_sync_status === "failed"
                    ? "Error"
                    : "Lagging"}
              </span>
            </summary>
            <ul className="flat-list">
              <li>
                Event index/detection context:{" "}
                {runtimeStatus.event_index.available ? "ready" : "pending"}
                {" / "}
                {runtimeStatus.detection_context.available
                  ? "ready"
                  : "pending"}
              </li>
              <li>
                Manifest/Quickwit indexed events:{" "}
                {formatNumber(runtimeStatus.event_index.event_count)}
                {" / "}
                {formatNumber(runtimeStatus.quickwit.indexed_event_count)}
              </li>
              <li>
                Principal/resource/department keys:{" "}
                {formatNumber(runtimeStatus.event_index.principal_key_count)}
                {" / "}
                {formatNumber(runtimeStatus.event_index.resource_key_count)}
                {" / "}
                {formatNumber(runtimeStatus.event_index.department_count)}
              </li>
              <li>
                Query backend/detail hydration:{" "}
                {runtimeStatus.canonical_event_store.query_backend}
                {" / "}
                {runtimeStatus.canonical_event_store.detail_hydration}
              </li>
              <li>
                Quickwit start/sync:{" "}
                {runtimeStatus.quickwit.last_start_status ?? "n/a"}
                {" / "}
                {runtimeStatus.quickwit.last_sync_status ?? "n/a"}
              </li>
              <li>
                Quickwit startup/sync duration:{" "}
                {runtimeStatus.quickwit.last_start_duration_ms ?? 0} ms
                {" / "}
                {runtimeStatus.quickwit.last_sync_duration_ms ?? 0} ms
              </li>
              <li>
                Quickwit RSS/log size:{" "}
                {formatBytes(runtimeStatus.quickwit.rss_bytes)}
                {" / "}
                {formatBytes(runtimeStatus.quickwit.log_size_bytes)}
              </li>
              <li>
                Watermark:{" "}
                {runtimeStatus.quickwit.watermark_at
                  ? formatObservedAt(runtimeStatus.quickwit.watermark_at)
                  : "none"}
              </li>
            </ul>
            {runtimeStatus.quickwit.last_start_error ? (
              <div className="panel-note">
                Startup error: {runtimeStatus.quickwit.last_start_error}
              </div>
            ) : null}
            {runtimeStatus.quickwit.last_sync_error ? (
              <div className="panel-note">
                Sync error: {runtimeStatus.quickwit.last_sync_error}
              </div>
            ) : null}
          </details>

          <details className="pipeline-advanced">
            <summary className="pipeline-advanced__summary">
              Feature lake and model
              <span className={`badge badge--${modelTone}`}>
                {modelActive ? "Model active" : "Fallback"}
              </span>
            </summary>
            <ul className="flat-list">
              <li>
                Action/history rows:{" "}
                {formatNumber(runtimeStatus.feature_tables.action_row_count)}
                {" / "}
                {formatNumber(runtimeStatus.feature_tables.history_row_count)}
              </li>
              <li>
                Collaboration/static/peer rows:{" "}
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
              <li>
                Materialized tables:{" "}
                {runtimeStatus.scoring_input.materialized_tables.join(", ") ||
                  "none"}
              </li>
              <li>
                Model gate:{" "}
                {runtimeStatus.model_runtime.activation_gate.eligible
                  ? "eligible"
                  : "blocked"}
                {" • "}
                {runtimeStatus.model_runtime.activation_gate.reason}
              </li>
              <li>Scoring reason: {runtimeStatus.scoring_input.reason}</li>
            </ul>
          </details>

          <details className="pipeline-advanced">
            <summary className="pipeline-advanced__summary">
              Operator state
              <span
                className={`badge badge--${pipelineStatusTone(
                  eventPlaneHealthy && runtimeReady,
                  !eventPlaneHealthy || !runtimeReady,
                )}`}
              >
                {eventPlaneHealthy && runtimeReady ? "Healthy" : "Degraded"}
              </span>
            </summary>
            <ul className="flat-list">
              <li>
                Stream normalized/detections:{" "}
                {formatNumber(streamState.normalized_event_count ?? 0)}
                {" / "}
                {formatNumber(streamState.detection_count ?? 0)}
              </li>
              <li>
                Normalization/feature/detection runs:{" "}
                {formatNumber(runtimeStatus.worker_state.normalization_runs)}
                {" / "}
                {formatNumber(runtimeStatus.worker_state.feature_runs)}
                {" / "}
                {formatNumber(runtimeStatus.worker_state.detection_runs)}
              </li>
              <li>
                Last normalization/feature/detection:{" "}
                {runtimeStatus.worker_state.last_normalization_at
                  ? formatObservedAt(
                      runtimeStatus.worker_state.last_normalization_at,
                    )
                  : "never"}
                {" / "}
                {runtimeStatus.worker_state.last_feature_at
                  ? formatObservedAt(runtimeStatus.worker_state.last_feature_at)
                  : "never"}
                {" / "}
                {runtimeStatus.worker_state.last_detection_at
                  ? formatObservedAt(
                      runtimeStatus.worker_state.last_detection_at,
                    )
                  : "never"}
              </li>
              <li>
                Last source stats/service:{" "}
                {formatMaybeObservedAt(
                  runtimeStatus.worker_state.last_source_stats_at,
                )}
                {" / "}
                {formatMaybeObservedAt(
                  runtimeStatus.worker_state.last_service_at,
                )}
              </li>
              <li>
                Identity profiles:{" "}
                {runtimeStatus.identity_profiles.available
                  ? "ready"
                  : "pending"}
                {" • "}
                {runtimeStatus.identity_profiles.source ?? "unknown"}
              </li>
              <li>
                Postgres:{" "}
                {runtimeStatus.postgres.initialized ? "initialized" : "missing"}
                {" • "}
                {runtimeStatus.postgres.dsn ? "dsn configured" : "dsn missing"}
              </li>
              <li>
                Source-stats refresh requested:{" "}
                {runtimeStatus.worker_control.source_stats_refresh_requested
                  ? `yes${
                      runtimeStatus.worker_control
                        .source_stats_refresh_requested_at
                        ? ` @ ${formatObservedAt(
                            runtimeStatus.worker_control
                              .source_stats_refresh_requested_at,
                          )}`
                        : ""
                    }`
                  : "no"}
              </li>
              <li>
                Postgres root/log: {runtimeStatus.postgres.root}
                {" / "}
                {runtimeStatus.postgres.paths.log ?? "n/a"}
              </li>
              <li>
                Quickwit config/log: {runtimeStatus.quickwit.config_path}
                {" / "}
                {runtimeStatus.quickwit.log_path}
              </li>
            </ul>
          </details>
        </div>
      </DetailSectionCard>
    </div>
  );
}
