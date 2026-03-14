import { startTransition, useEffect, useState } from "react";

const pollIntervalMs = 3000;
const pages = ["queue", "investigations", "integrations"];

async function fetchJson(path, options) {
  const response = await fetch(path, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function Section({ title, subtitle, children, tone = "default" }) {
  return (
    <section className={`panel panel--${tone}`}>
      <div className="panel__header">
        <div>
          <h2>{title}</h2>
          {subtitle ? <p>{subtitle}</p> : null}
        </div>
      </div>
      {children}
    </section>
  );
}

function EmptyState({ title, body }) {
  return (
    <div className="empty-state">
      <strong>{title}</strong>
      <p>{body}</p>
    </div>
  );
}

function pageMeta(page, counts) {
  if (page === "queue") {
    return {
      title: "Queue",
      subtitle: "Engine outputs waiting for analyst review and escalation.",
      count: counts.highSeverityDetections,
      countLabel: "high severity",
    };
  }
  if (page === "investigations") {
    return {
      title: "Investigations",
      subtitle:
        "Open analyst workflows with evidence, timeline, and disposition.",
      count: counts.openCases,
      countLabel: "open",
    };
  }
  return {
    title: "Input Integrations",
    subtitle:
      "Source readiness, onboarding compatibility, and projection health.",
    count: counts.integrations,
    countLabel: "connected",
  };
}

function DetailPane({
  selectedItem,
  detailBusy,
  busy,
  selectCase,
  selectDetection,
  selectEvent,
  createCaseForDetection,
  updateCaseDisposition,
}) {
  return (
    <Section
      title="Selected Detail"
      subtitle="Click a record to inspect evidence, related context, and the next analyst action."
      tone="detail"
    >
      {detailBusy ? (
        <EmptyState
          title="Loading detail…"
          body="Fetching the selected record."
        />
      ) : !selectedItem ? (
        <EmptyState
          title="Nothing selected yet."
          body="Choose a detection, investigation, or event from the active page."
        />
      ) : selectedItem.type === "detection" ? (
        <div className="detail-stack">
          <div>
            <span
              className={`badge badge--${selectedItem.data.detection.severity}`}
            >
              {selectedItem.data.detection.severity}
            </span>
            <h3>{selectedItem.data.detection.title}</h3>
            <p className="detail-subtle">
              {selectedItem.data.detection.scenario}
            </p>
          </div>
          <div className="detail-actions">
            {selectedItem.data.linked_case_id ? (
              <button
                disabled={busy}
                onClick={() => selectCase(selectedItem.data.linked_case_id)}
              >
                Open Investigation
              </button>
            ) : (
              <button
                disabled={busy}
                onClick={() => createCaseForDetection(selectedItem.id)}
              >
                Create Investigation
              </button>
            )}
          </div>
          <div>
            <strong>Why this fired</strong>
            <ul className="flat-list">
              {selectedItem.data.detection.reasons.map((reason) => (
                <li key={reason}>{reason}</li>
              ))}
            </ul>
          </div>
          <div>
            <strong>Peer context</strong>
            <div className="panel-note">
              Principal events:{" "}
              {selectedItem.data.peer_comparison.principal_total_events}, prior
              access count:{" "}
              {
                selectedItem.data.peer_comparison
                  .principal_prior_resource_access_count
              }
              , peer-group access count:{" "}
              {
                selectedItem.data.peer_comparison
                  .peer_group_resource_access_count
              }
            </div>
          </div>
          <div>
            <strong>Evidence</strong>
            <div className="list-stack list-stack--tight">
              {selectedItem.data.evidence_bundle.evidence_items.map((item) => (
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
          </div>
        </div>
      ) : selectedItem.type === "case" ? (
        <div className="detail-stack">
          <div>
            <span className="badge badge--neutral">
              {selectedItem.data.case.status}
            </span>
            <h3>{selectedItem.data.summary.case_title}</h3>
            <p className="detail-subtle">
              {selectedItem.data.case.disposition
                ? `Disposition: ${selectedItem.data.case.disposition}`
                : "No disposition yet"}
            </p>
          </div>
          <div className="detail-actions">
            <button
              disabled={busy}
              onClick={() =>
                updateCaseDisposition(selectedItem.id, "escalated")
              }
            >
              Mark Escalated
            </button>
            <button
              disabled={busy}
              onClick={() => updateCaseDisposition(selectedItem.id, "closed")}
            >
              Close Investigation
            </button>
          </div>
          <div>
            <strong>Linked detections</strong>
            <div className="list-stack list-stack--tight">
              {selectedItem.data.detections.map((detection) => (
                <button
                  className="mini-card mini-card--button"
                  key={detection.detection_id}
                  onClick={() => selectDetection(detection.detection_id)}
                >
                  <div>{detection.title}</div>
                  <div className="detail-subtle">{detection.scenario}</div>
                </button>
              ))}
            </div>
          </div>
          <div>
            <strong>Timeline</strong>
            <div className="list-stack list-stack--tight">
              {selectedItem.data.timeline_events.slice(-10).map((event) => (
                <button
                  className="mini-card mini-card--button"
                  key={event.event_id}
                  onClick={() => selectEvent(event.event_id)}
                >
                  <div>
                    {event.principal.display_name} {event.action.verb}{" "}
                    {event.resource.name}
                  </div>
                  <div className="detail-subtle">{event.observed_at}</div>
                </button>
              ))}
            </div>
          </div>
        </div>
      ) : (
        <div className="detail-stack">
          <div>
            <h3>{selectedItem.data.principal.display_name}</h3>
            <p className="detail-subtle">
              {selectedItem.data.source} / {selectedItem.data.observed_at}
            </p>
          </div>
          <div className="panel-note">
            {selectedItem.data.action.verb} {selectedItem.data.resource.name} (
            {selectedItem.data.resource.sensitivity})
          </div>
          <pre className="json-preview">
            {JSON.stringify(selectedItem.data.attributes, null, 2)}
          </pre>
        </div>
      )}
    </Section>
  );
}

function DemoOverlay({ busy, streamState, performAction, overview }) {
  return (
    <div className="demo-overlay">
      <div className="demo-overlay__header">
        <span className="eyebrow">Demo Controls</span>
        <strong>
          {streamState.cursor ?? 0}/{streamState.total_source_events ?? 0}
        </strong>
      </div>
      <div className="demo-overlay__meta">
        <span>{overview?.event_count ?? 0} events projected</span>
        <span>{overview?.detection_count ?? 0} detections</span>
        <span>{overview?.case_count ?? 0} investigations</span>
      </div>
      <div className="demo-overlay__actions">
        <button
          disabled={busy}
          onClick={() => performAction("/api/demo/reset")}
        >
          Reset
        </button>
        <button
          disabled={busy}
          onClick={() => performAction("/api/demo/advance?batch_size=10")}
        >
          +10
        </button>
        <button
          disabled={busy}
          onClick={() => performAction("/api/demo/advance?batch_size=25")}
        >
          +25
        </button>
        <button
          disabled={busy}
          onClick={() => performAction("/api/demo/advance?batch_size=50")}
        >
          +50
        </button>
      </div>
    </div>
  );
}

export function App() {
  const [page, setPage] = useState("queue");
  const [overview, setOverview] = useState(null);
  const [events, setEvents] = useState([]);
  const [detections, setDetections] = useState([]);
  const [cases, setCases] = useState([]);
  const [sourceCapability, setSourceCapability] = useState(null);
  const [evaluationSummary, setEvaluationSummary] = useState("");
  const [selectedItem, setSelectedItem] = useState(null);
  const [busy, setBusy] = useState(false);
  const [detailBusy, setDetailBusy] = useState(false);
  const [error, setError] = useState("");

  async function loadDashboard() {
    try {
      const [
        overviewData,
        eventData,
        detectionData,
        caseData,
        capabilityData,
        evaluationData,
      ] = await Promise.all([
        fetchJson("/api/overview"),
        fetchJson("/api/events?limit=24"),
        fetchJson("/api/detections?limit=24"),
        fetchJson("/api/cases?limit=24"),
        fetchJson("/api/source-capability"),
        fetchJson("/api/evaluation-summary"),
      ]);
      startTransition(() => {
        setOverview(overviewData);
        setEvents(eventData);
        setDetections(detectionData);
        setCases(caseData);
        setSourceCapability(capabilityData);
        setEvaluationSummary(evaluationData.markdown || "");
      });
      setError("");
    } catch (loadError) {
      setError(loadError.message);
    }
  }

  useEffect(() => {
    loadDashboard();
    const timer = window.setInterval(loadDashboard, pollIntervalMs);
    return () => window.clearInterval(timer);
  }, []);

  useEffect(() => {
    const hash = window.location.hash.replace("#", "");
    if (pages.includes(hash)) {
      setPage(hash);
    }
    const onHashChange = () => {
      const nextHash = window.location.hash.replace("#", "");
      if (pages.includes(nextHash)) {
        setPage(nextHash);
      }
    };
    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  function navigate(nextPage) {
    window.location.hash = nextPage;
    setPage(nextPage);
  }

  async function performAction(path) {
    setBusy(true);
    try {
      await fetchJson(path, { method: "POST" });
      await loadDashboard();
      if (selectedItem?.type === "case") {
        await selectCase(selectedItem.id);
      } else if (selectedItem?.type === "detection") {
        await selectDetection(selectedItem.id);
      }
    } catch (actionError) {
      setError(actionError.message);
    } finally {
      setBusy(false);
    }
  }

  async function selectDetection(detectionId) {
    setDetailBusy(true);
    try {
      const detail = await fetchJson(`/api/detections/${detectionId}`);
      setSelectedItem({ type: "detection", id: detectionId, data: detail });
      setError("");
    } catch (detailError) {
      setError(detailError.message);
    } finally {
      setDetailBusy(false);
    }
  }

  async function selectCase(caseId) {
    setDetailBusy(true);
    try {
      const detail = await fetchJson(`/api/cases/${caseId}`);
      setSelectedItem({ type: "case", id: caseId, data: detail });
      setError("");
    } catch (detailError) {
      setError(detailError.message);
    } finally {
      setDetailBusy(false);
    }
  }

  async function selectEvent(eventId) {
    setDetailBusy(true);
    try {
      const detail = await fetchJson(`/api/events/${eventId}`);
      setSelectedItem({ type: "event", id: eventId, data: detail });
      setError("");
    } catch (detailError) {
      setError(detailError.message);
    } finally {
      setDetailBusy(false);
    }
  }

  async function createCaseForDetection(detectionId) {
    setBusy(true);
    try {
      const created = await fetchJson(`/api/detections/${detectionId}/case`, {
        method: "POST",
      });
      await loadDashboard();
      navigate("investigations");
      await selectCase(created.case_id);
    } catch (actionError) {
      setError(actionError.message);
    } finally {
      setBusy(false);
    }
  }

  async function updateCaseDisposition(caseId, disposition) {
    setBusy(true);
    try {
      await fetchJson(`/api/cases/${caseId}/disposition`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ disposition }),
      });
      await loadDashboard();
      await selectCase(caseId);
    } catch (actionError) {
      setError(actionError.message);
    } finally {
      setBusy(false);
    }
  }

  const streamState = overview?.stream_state || {};
  const openCases = cases.filter((item) => item.status === "open");
  const closedCases = cases.filter((item) => item.disposition === "closed");
  const highSeverityDetections = detections.filter(
    (item) => item.severity === "high",
  );
  const sourceRows = Object.entries(sourceCapability?.sources || {});
  const counts = {
    openCases: openCases.length,
    highSeverityDetections: highSeverityDetections.length,
    integrations: sourceRows.length,
  };
  const currentPage = pageMeta(page, counts);

  return (
    <div className="app-shell">
      <header className="app-topbar">
        <button
          className="app-brand app-brand--button"
          onClick={() => navigate("queue")}
        >
          <span className="app-brand__mark">SC</span>
          <div>
            <div className="app-brand__name">Seccloud</div>
            <div className="app-brand__subtle">Internal analyst demo</div>
          </div>
        </button>
        <nav className="app-nav" aria-label="Primary">
          <button
            className={
              page === "queue"
                ? "app-nav__item app-nav__item--active"
                : "app-nav__item"
            }
            onClick={() => navigate("queue")}
          >
            Queue
            <span className="app-nav__badge">
              {highSeverityDetections.length}
            </span>
          </button>
          <button
            className={
              page === "investigations"
                ? "app-nav__item app-nav__item--active"
                : "app-nav__item"
            }
            onClick={() => navigate("investigations")}
          >
            Investigations
            <span className="app-nav__badge">{openCases.length}</span>
          </button>
          <button
            className={
              page === "integrations"
                ? "app-nav__item app-nav__item--active"
                : "app-nav__item"
            }
            onClick={() => navigate("integrations")}
          >
            Input Integrations
            <span className="app-nav__badge">{sourceRows.length}</span>
          </button>
        </nav>
      </header>

      {error ? <div className="error-banner">{error}</div> : null}

      <section className="page-header">
        <div>
          <h1>{currentPage.title}</h1>
          <p>{currentPage.subtitle}</p>
        </div>
        <div className="page-header__badge">
          <strong>{currentPage.count}</strong>
          <span>{currentPage.countLabel}</span>
        </div>
      </section>

      {page === "queue" ? (
        <div className="workspace-grid">
          <div className="workspace-grid__main">
            <Section
              title="Detection Queue"
              subtitle="Scored outputs waiting for analyst review and case creation."
              tone="priority"
            >
              <div className="list-stack">
                {detections.length === 0 ? (
                  <EmptyState
                    title="No detections yet."
                    body="Advance the demo stream until risky behavior reaches the queue."
                  />
                ) : (
                  detections.map((detection) => (
                    <button
                      className="record-card record-card--button"
                      key={detection.detection_id}
                      onClick={() => selectDetection(detection.detection_id)}
                    >
                      <div className="record-card__header">
                        <h3>{detection.title}</h3>
                        <span className={`badge badge--${detection.severity}`}>
                          {detection.severity}
                        </span>
                      </div>
                      <div className="record-card__meta">
                        <span>{detection.scenario}</span>
                        <span>score {detection.score}</span>
                      </div>
                      <p>{detection.reasons.slice(0, 2).join(", ")}</p>
                    </button>
                  ))
                )}
              </div>
            </Section>

            <Section
              title="Recent Event Feed"
              subtitle="Latest normalized events entering the analyst surface."
            >
              <div className="event-feed">
                {events.map((event) => (
                  <button
                    className="event-row event-row--button"
                    key={event.event_id}
                    onClick={() => selectEvent(event.event_id)}
                  >
                    <div className="event-row__time">{event.observed_at}</div>
                    <div className="event-row__body">
                      <strong>{event.principal.display_name}</strong>{" "}
                      {event.action.verb} <strong>{event.resource.name}</strong>
                      <div className="event-row__meta">
                        {event.source} / {event.resource.sensitivity}
                      </div>
                    </div>
                  </button>
                ))}
              </div>
            </Section>
          </div>
          <aside className="workspace-grid__detail">
            <DetailPane
              selectedItem={selectedItem}
              detailBusy={detailBusy}
              busy={busy}
              selectCase={selectCase}
              selectDetection={selectDetection}
              selectEvent={selectEvent}
              createCaseForDetection={createCaseForDetection}
              updateCaseDisposition={updateCaseDisposition}
            />
          </aside>
        </div>
      ) : null}

      {page === "investigations" ? (
        <div className="workspace-grid">
          <div className="workspace-grid__main">
            <Section
              title="Open Investigations"
              subtitle="Active analyst workflows with linked detections, evidence, and timeline."
              tone="priority"
            >
              <div className="list-stack">
                {openCases.length === 0 ? (
                  <EmptyState
                    title="No open investigations yet."
                    body="Create a case from a detection in the queue."
                  />
                ) : (
                  openCases.map((item) => (
                    <button
                      className="record-card record-card--button"
                      key={item.case_id}
                      onClick={() => selectCase(item.case_id)}
                    >
                      <div className="record-card__header">
                        <h3>{item.summary?.case_title || item.case_id}</h3>
                        <span className="badge badge--neutral">
                          {item.status}
                        </span>
                      </div>
                      <div className="record-card__meta">
                        <span>{item.detection_ids.length} detections</span>
                        <span>
                          {item.timeline_event_ids.length} timeline events
                        </span>
                      </div>
                      <p>
                        {item.summary?.primary_detection?.title ||
                          "No primary detection summary yet."}
                      </p>
                    </button>
                  ))
                )}
              </div>
            </Section>

            <Section
              title="Recently Closed"
              subtitle="Completed investigations that have already been dispositioned."
            >
              <div className="list-stack">
                {closedCases.length === 0 ? (
                  <EmptyState
                    title="No closed investigations yet."
                    body="Close an investigation from the detail pane to see it here."
                  />
                ) : (
                  closedCases.slice(0, 8).map((item) => (
                    <button
                      className="record-card record-card--button"
                      key={item.case_id}
                      onClick={() => selectCase(item.case_id)}
                    >
                      <div className="record-card__header">
                        <h3>{item.summary?.case_title || item.case_id}</h3>
                        <span className="badge badge--neutral">
                          {item.disposition || "closed"}
                        </span>
                      </div>
                      <div className="record-card__meta">
                        <span>{item.detection_ids.length} detections</span>
                        <span>
                          {item.timeline_event_ids.length} timeline events
                        </span>
                      </div>
                    </button>
                  ))
                )}
              </div>
            </Section>
          </div>
          <aside className="workspace-grid__detail">
            <DetailPane
              selectedItem={selectedItem}
              detailBusy={detailBusy}
              busy={busy}
              selectCase={selectCase}
              selectDetection={selectDetection}
              selectEvent={selectEvent}
              createCaseForDetection={createCaseForDetection}
              updateCaseDisposition={updateCaseDisposition}
            />
          </aside>
        </div>
      ) : null}

      {page === "integrations" ? (
        <div className="integrations-grid">
          <Section
            title="Connected Inputs"
            subtitle="Runtime-derived source readiness and ingest health across the active source pack."
          >
            <div className="source-table">
              {sourceRows.map(([source, details]) => (
                <div className="source-row" key={source}>
                  <div>
                    <strong>{details.display_name}</strong>
                    <div className="detail-subtle">{source}</div>
                  </div>
                  <div>{details.normalized_event_count} normalized</div>
                  <div>{details.dead_letter_count} dead letters</div>
                  <div>
                    {details.missing_required_event_types?.length || 0} missing
                    types
                  </div>
                </div>
              ))}
            </div>
          </Section>

          <Section
            title="Current Evaluation Snapshot"
            subtitle="Seeded-scenario status from the projected backend state."
          >
            <pre className="markdown-preview">{evaluationSummary}</pre>
          </Section>

          <Section
            title="Projection Health"
            subtitle="Secondary platform state, kept on a separate page so it does not dominate the analyst workflow."
          >
            <div className="platform-grid">
              <div>
                <strong>Projected events</strong>
                <div className="panel-note">{overview?.event_count ?? 0}</div>
              </div>
              <div>
                <strong>Detections</strong>
                <div className="panel-note">
                  {overview?.detection_count ?? 0}
                </div>
              </div>
              <div>
                <strong>Investigations</strong>
                <div className="panel-note">{overview?.case_count ?? 0}</div>
              </div>
              <div>
                <strong>Events by source</strong>
                <div className="panel-note">
                  {JSON.stringify(
                    overview?.ops_metadata?.event_counts_by_source || {},
                  )}
                </div>
              </div>
              <div>
                <strong>Dead letters</strong>
                <div className="panel-note">
                  {overview?.ops_metadata?.dead_letter_count ?? 0}
                </div>
              </div>
              <div>
                <strong>Demo stream</strong>
                <div className="panel-note">
                  {streamState.cursor ?? 0}/
                  {streamState.total_source_events ?? 0}
                </div>
              </div>
            </div>
          </Section>
        </div>
      ) : null}

      <DemoOverlay
        busy={busy}
        streamState={streamState}
        performAction={performAction}
        overview={overview}
      />
    </div>
  );
}
