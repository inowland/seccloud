import {
  startTransition,
  useDeferredValue,
  useEffect,
  useEffectEvent,
  useRef,
  useState,
  type CSSProperties,
  type PointerEvent as ReactPointerEvent,
  type ReactNode,
} from "react";
import type { components, paths } from "./api.generated";
import {
  DetectionDetailContent,
  EventDetailContent,
  IntegrationDetailContent,
  type IntegrationEntry,
} from "./detail-content";
import {
  EmptyState,
  type EmptyStateProps,
  RecordPage,
  Section,
  type SectionTone,
} from "./panels";
import {
  pageForRoute,
  parseRouteFromHash,
  type Page,
  type Route,
  routeToHash,
  routeToHref,
} from "./routes";

const pollIntervalMs = 3000;
const queuePageSize = 10;
const eventsPageSize = 14;
const splitPaneStorageKey = "seccloud.split-pane-width";
const navPaneStorageKey = "seccloud.nav-pane-width";
const navPaneCollapsedStorageKey = "seccloud.nav-pane-collapsed";
const eventListColumnsStorageKey = "seccloud.events.columns";
const eventListDensityStorageKey = "seccloud.events.density";
const eventListSortStorageKey = "seccloud.events.sort";
const detectionListDensityStorageKey = "seccloud.detections.density";
const integrationListDensityStorageKey = "seccloud.integrations.density";
const splitPaneDefaultWidth = 820;
const splitPaneMinWidth = 320;
const splitPaneDetailMinWidth = 360;
const splitPaneDividerWidth = 14;
const navPaneDefaultWidth = 248;
const navPaneMinWidth = 220;
const navPaneMaxWidth = 360;
const navPaneCollapsedWidth = 88;
const navPaneDividerWidth = 14;
type BadgeTone = "critical" | "attention" | "neutral" | "positive";
type JsonResponse<Path extends keyof paths, Method extends keyof paths[Path]> =
  NonNullable<paths[Path][Method]> extends {
    responses: { 200: { content: { "application/json": infer Response } } };
  }
    ? Response
    : never;

type Overview = JsonResponse<"/api/overview", "get">;
type DetectionList = JsonResponse<"/api/detections", "get">;
type EventList = JsonResponse<"/api/events", "get">;
type EventRecord = components["schemas"]["Event"];
type EventDetail = JsonResponse<"/api/events/{event_id}", "get">;
type DetectionDetail = JsonResponse<"/api/detections/{detection_id}", "get">;
type Detection = components["schemas"]["Detection"];
type SourceCapabilityMatrix = JsonResponse<"/api/source-capability", "get">;
type SourceCapabilityDetails = components["schemas"]["SourceCapabilityDetails"];
type Pagination = components["schemas"]["Pagination"];
type StreamState = components["schemas"]["StreamState"];
type WorkerState = JsonResponse<"/api/workers/state", "get">;

interface AppData {
  overview: Overview;
  detections: DetectionList;
  events: EventList;
  sourceCapability: SourceCapabilityMatrix;
  streamState: StreamState;
  workerState: WorkerState;
}

interface Counts {
  events: number | null;
  detections: number;
  integrations: number;
}

interface PageMeta {
  title: string;
  count?: number | null;
  countLabel?: string;
}

interface PagerProps {
  page: Pagination;
  label: string;
  onPrevious: () => void;
  onNext: () => void;
}

interface DetailPaneProps {
  acknowledgingDetectionId: string | null;
  selectedItem: SelectedItem | null;
  detailBusy: boolean;
  onAcknowledgeDetection: (detectionId: string) => void;
  openEventPage: (eventId: string) => void;
}

interface StreamOverlayProps {
  busy: boolean;
  streamState: StreamState;
  workerState: WorkerState;
  performAction: (path: string) => Promise<void>;
}

interface IntegrationDetailPaneProps {
  entry: IntegrationEntry | null;
  formatObservedAt: (value: string) => string;
  titleHref?: string;
}

interface SplitPaneProps {
  left: ReactNode;
  right: ReactNode;
  width: number;
  onResize: (width: number) => void;
}

interface AppFrameProps {
  sidebar: ReactNode;
  main: ReactNode;
  width: number;
  collapsed: boolean;
  onResize: (width: number) => void;
}

interface MasterListPaneProps {
  searchValue: string;
  onSearchChange: (value: string) => void;
  searchPlaceholder: string;
  resultCount: number;
  hasItems: boolean;
  hasResults: boolean;
  emptyState: EmptyStateProps;
  noResultsState: EmptyStateProps;
  children: ReactNode;
  footer?: ReactNode;
  tone?: SectionTone;
  toolbarActions?: ReactNode;
}

interface ActionCard {
  title: string;
  body: string;
  tone: BadgeTone;
}

type EventColumnKey =
  | "observedAt"
  | "actor"
  | "actorEmail"
  | "action"
  | "resource"
  | "source"
  | "sensitivity"
  | "actionType"
  | "eventId"
  | "sourceEventId";

interface EventColumnOption {
  key: EventColumnKey;
  label: string;
}

interface EventColumnPreset {
  id: string;
  label: string;
  columns: EventColumnKey[];
}

type EventDensity = "comfortable" | "compact";
type EventSortDirection = "asc" | "desc";

interface EventSortState {
  column: EventColumnKey;
  direction: EventSortDirection;
}

type SelectedItem =
  | { type: "detection"; id: string; data: DetectionDetail }
  | { type: "event"; id: string; data: EventDetail };

const dateFormatter = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});
const numberFormatter = new Intl.NumberFormat("en-US");

const emptyStreamState: StreamState = {
  cursor: 0,
  total_source_events: 0,
  complete: false,
};
const emptyWorkerState: WorkerState = {
  normalization_runs: 0,
  detection_runs: 0,
  source_stats_runs: 0,
  projection_runs: 0,
  service_runs: 0,
  last_source_stats_at: null,
  pending_batch_count: 0,
  processed_batch_count: 0,
};
const emptyPagination: Pagination = {
  limit: 0,
  offset: 0,
  returned: 0,
  total: null,
  has_more: false,
};
const eventColumnOptions: EventColumnOption[] = [
  { key: "observedAt", label: "Observed" },
  { key: "actor", label: "Actor" },
  { key: "actorEmail", label: "Actor email" },
  { key: "action", label: "Action" },
  { key: "resource", label: "Resource" },
  { key: "source", label: "Source" },
  { key: "sensitivity", label: "Sensitivity" },
  { key: "actionType", label: "Action type" },
  { key: "eventId", label: "Event ID" },
  { key: "sourceEventId", label: "Source event ID" },
];
const defaultEventColumns: EventColumnKey[] = [
  "observedAt",
  "actor",
  "action",
  "resource",
  "source",
  "sensitivity",
  "actionType",
];
const triageEventColumns: EventColumnKey[] = [
  "observedAt",
  "actor",
  "action",
  "resource",
  "sensitivity",
  "actionType",
];
const provenanceEventColumns: EventColumnKey[] = [
  "observedAt",
  "actor",
  "source",
  "eventId",
  "sourceEventId",
];
const eventColumnPresets: EventColumnPreset[] = [
  { id: "default", label: "Default", columns: defaultEventColumns },
  { id: "triage", label: "Triage", columns: triageEventColumns },
  { id: "provenance", label: "Provenance", columns: provenanceEventColumns },
];
const defaultEventSort: EventSortState = {
  column: "observedAt",
  direction: "desc",
};

async function fetchJson<T>(path: string, options?: RequestInit): Promise<T> {
  const response = await fetch(path, options);
  if (!response.ok) {
    let detail = `Request failed: ${response.status}`;
    try {
      const payload = (await response.json()) as { detail?: string };
      if (payload.detail) {
        detail = payload.detail;
      }
    } catch {
      // Ignore non-JSON error responses and keep the status-based message.
    }
    throw new Error(detail);
  }
  return (await response.json()) as T;
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unexpected error";
}

function buildListUrl(path: string, limit: number, offset: number): string {
  return `${path}?limit=${limit}&offset=${offset}`;
}

async function fetchDashboardData(
  queueOffset: number,
  eventsOffset: number,
): Promise<AppData> {
  const [
    overview,
    detections,
    events,
    sourceCapability,
    streamState,
    workerState,
  ] = await Promise.all([
    fetchJson<Overview>("/api/overview"),
    fetchJson<DetectionList>(
      buildListUrl("/api/detections", queuePageSize, queueOffset),
    ),
    fetchJson<EventList>(
      buildListUrl("/api/events", eventsPageSize, eventsOffset),
    ),
    fetchJson<SourceCapabilityMatrix>("/api/source-capability"),
    fetchJson<StreamState>("/api/stream/state"),
    fetchJson<WorkerState>("/api/workers/state"),
  ]);

  return {
    overview,
    detections,
    events,
    sourceCapability,
    streamState,
    workerState,
  };
}

function applyDashboardData(
  data: AppData,
  actions: {
    setOverview: (value: Overview) => void;
    setDetections: (value: DetectionList) => void;
    setEvents: (value: EventList) => void;
    setSourceCapability: (value: SourceCapabilityMatrix) => void;
    setStreamState: (value: StreamState) => void;
    setWorkerState: (value: WorkerState) => void;
    setError: (value: string) => void;
  },
) {
  startTransition(() => {
    actions.setOverview(data.overview);
    actions.setDetections(data.detections);
    actions.setEvents(data.events);
    actions.setSourceCapability(data.sourceCapability);
    actions.setStreamState(data.streamState);
    actions.setWorkerState(data.workerState);
  });
  actions.setError("");
}

function formatNumber(value: number | null | undefined): string {
  return numberFormatter.format(value ?? 0);
}

function formatObservedAt(value: string): string {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return dateFormatter.format(parsed);
}

function formatSourceName(source: string): string {
  if (source === "gworkspace") {
    return "Google Workspace";
  }
  if (source === "okta") {
    return "Okta";
  }
  if (source === "github") {
    return "GitHub";
  }
  if (source === "snowflake") {
    return "Snowflake";
  }
  return source;
}

function integrationIssueCount(details: SourceCapabilityDetails): number {
  return (
    details.missing_required_event_types.length +
    details.missing_required_fields.length +
    details.dead_letter_7d_count
  );
}

function integrationCoverageCount(details: SourceCapabilityDetails): number {
  return Object.values(details.required_field_coverage).filter(Boolean).length;
}

function integrationStatus(details: SourceCapabilityDetails): {
  tone: BadgeTone;
  label: string;
  note: string;
} {
  const issueCount = integrationIssueCount(details);
  if (issueCount === 0 && details.normalized_last_seen_at) {
    return {
      tone: "positive",
      label: "Healthy",
      note: "Contract met and normalized data is still arriving.",
    };
  }
  if (!details.raw_last_seen_at && !details.normalized_last_seen_at) {
    return {
      tone: "attention",
      label: "Not seeded",
      note: "No source activity has landed for this integration yet.",
    };
  }
  if (details.raw_last_seen_at && !details.normalized_last_seen_at) {
    return {
      tone: "critical",
      label: "Blocked",
      note: "Raw data is arriving but nothing is making it into normalized analytics.",
    };
  }
  return {
    tone: "critical",
    label: "Attention",
    note: "Observed data diverges from the current contract.",
  };
}

function integrationActionItems(
  details: SourceCapabilityDetails,
): ActionCard[] {
  const actions: ActionCard[] = [];

  if (!details.raw_last_seen_at && !details.normalized_last_seen_at) {
    actions.push({
      title: "Seed or connect the source",
      body: "Nothing has landed yet, so validate the connector and send a small test batch before judging the contract.",
      tone: "attention",
    });
  }

  if (details.raw_last_seen_at && !details.normalized_last_seen_at) {
    actions.push({
      title: "Restore normalization flow",
      body: "Raw source activity is landing but nothing is reaching the normalized dataset. Check mapping, validation failures, and worker output before trusting this integration.",
      tone: "critical",
    });
  }

  if (details.missing_required_event_types.length > 0) {
    actions.push({
      title: "Backfill missing event types",
      body: `${formatNumber(details.missing_required_event_types.length)} required event type(s) have not been observed. Fix the upstream feed or mapper, then rerun a resync/backfill for this source.`,
      tone: "critical",
    });
  }

  if (details.missing_required_fields.length > 0) {
    actions.push({
      title: "Repair field mapping",
      body: `${formatNumber(details.missing_required_fields.length)} required field(s) are absent in normalized output. Resolve the mapping gap before trusting detections from this integration.`,
      tone: "critical",
    });
  }

  if (details.dead_letter_7d_count > 0) {
    actions.push({
      title: "Clear dead letters before resync",
      body: `${formatNumber(details.dead_letter_7d_count)} dead-letter event(s) landed in the latest 7-day slice. Inspect the reason counts below, correct the parser or contract, then rerun ingestion for the affected window.`,
      tone: "critical",
    });
  }

  if (actions.length === 0) {
    actions.push({
      title: "Monitor only",
      body: "This integration is currently healthy. Keep it in the watch list and only resync if the contract drifts or dead letters appear.",
      tone: "positive",
    });
  }

  return actions;
}

function badgeToneForSeverity(severity: string): BadgeTone {
  if (severity === "high") {
    return "critical";
  }
  if (severity === "medium") {
    return "attention";
  }
  return "neutral";
}

function pageMeta(page: Page, counts: Counts): PageMeta {
  if (page === "detections") {
    return {
      title: "Detections",
      count: counts.detections,
      countLabel: "detections",
    };
  }
  if (page === "events") {
    return {
      title: "Events",
    };
  }
  if (page === "integrations") {
    return {
      title: "Integrations",
      count: counts.integrations,
      countLabel: "need attention",
    };
  }
  return {
    title: "Detections",
    count: counts.detections,
    countLabel: "detections",
  };
}

function sortIntegrationEntries(
  sources: Record<string, SourceCapabilityDetails>,
): IntegrationEntry[] {
  return Object.entries(sources)
    .map(([source, details]) => ({ source, details }))
    .sort((left, right) => {
      const leftIssues = integrationIssueCount(left.details);
      const rightIssues = integrationIssueCount(right.details);
      if (leftIssues !== rightIssues) {
        return rightIssues - leftIssues;
      }
      return left.details.display_name.localeCompare(
        right.details.display_name,
      );
    });
}

function readStoredSplitPaneWidth(): number {
  if (typeof window === "undefined") {
    return splitPaneDefaultWidth;
  }
  const stored = Number(window.localStorage.getItem(splitPaneStorageKey));
  if (!Number.isFinite(stored)) {
    return splitPaneDefaultWidth;
  }
  return Math.max(splitPaneMinWidth, stored);
}

function clampSplitPaneWidth(width: number, containerWidth: number): number {
  const maxWidth = Math.max(
    splitPaneMinWidth,
    containerWidth - splitPaneDetailMinWidth - splitPaneDividerWidth,
  );
  return Math.min(Math.max(width, splitPaneMinWidth), maxWidth);
}

function readStoredNavPaneWidth(): number {
  if (typeof window === "undefined") {
    return navPaneDefaultWidth;
  }
  const stored = Number(window.localStorage.getItem(navPaneStorageKey));
  if (!Number.isFinite(stored)) {
    return navPaneDefaultWidth;
  }
  return Math.min(Math.max(stored, navPaneMinWidth), navPaneMaxWidth);
}

function readStoredNavPaneCollapsed(): boolean {
  if (typeof window === "undefined") {
    return false;
  }
  return window.localStorage.getItem(navPaneCollapsedStorageKey) === "true";
}

function clampNavPaneWidth(width: number): number {
  return Math.min(Math.max(width, navPaneMinWidth), navPaneMaxWidth);
}

function normalizeEventColumns(value: unknown): EventColumnKey[] {
  const allowed = new Set<EventColumnKey>(
    eventColumnOptions.map((option) => option.key),
  );
  if (!Array.isArray(value)) {
    return defaultEventColumns;
  }

  const normalized: EventColumnKey[] = [];
  for (const item of value) {
    if (
      typeof item === "string" &&
      allowed.has(item as EventColumnKey) &&
      !normalized.includes(item as EventColumnKey)
    ) {
      normalized.push(item as EventColumnKey);
    }
  }

  return normalized.length > 0 ? normalized : defaultEventColumns;
}

function readStoredEventColumns(): EventColumnKey[] {
  if (typeof window === "undefined") {
    return defaultEventColumns;
  }
  const stored = window.localStorage.getItem(eventListColumnsStorageKey);
  if (!stored) {
    return defaultEventColumns;
  }
  try {
    return normalizeEventColumns(JSON.parse(stored));
  } catch {
    return defaultEventColumns;
  }
}

function eventColumnsEqual(
  left: EventColumnKey[],
  right: EventColumnKey[],
): boolean {
  if (left.length !== right.length) {
    return false;
  }
  return left.every((column, index) => column === right[index]);
}

function eventColumnLabel(column: EventColumnKey): string {
  return (
    eventColumnOptions.find((option) => option.key === column)?.label ?? column
  );
}

function formatTokenLabel(value: string): string {
  return value
    .replace(/[_-]+/g, " ")
    .replace(/\b\w/g, (match) => match.toUpperCase());
}

function sensitivityTone(value: string): BadgeTone {
  const normalized = value.trim().toLowerCase();
  if (normalized === "high" || normalized === "critical") {
    return "critical";
  }
  if (normalized === "medium" || normalized === "internal") {
    return "attention";
  }
  if (normalized === "low" || normalized === "public") {
    return "positive";
  }
  return "neutral";
}

function defaultSortDirection(column: EventColumnKey): EventSortDirection {
  if (column === "observedAt" || column === "sensitivity") {
    return "desc";
  }
  return "asc";
}

function normalizeEventDensity(value: unknown): EventDensity {
  return value === "compact" ? "compact" : "comfortable";
}

function readStoredDensity(storageKey: string): EventDensity {
  if (typeof window === "undefined") {
    return "comfortable";
  }
  return normalizeEventDensity(window.localStorage.getItem(storageKey));
}

function readStoredEventDensity(): EventDensity {
  return readStoredDensity(eventListDensityStorageKey);
}

function normalizeEventSort(value: unknown): EventSortState {
  if (!value || typeof value !== "object") {
    return defaultEventSort;
  }

  const candidate = value as {
    column?: unknown;
    direction?: unknown;
  };
  const column =
    typeof candidate.column === "string" &&
    eventColumnOptions.some((option) => option.key === candidate.column)
      ? (candidate.column as EventColumnKey)
      : defaultEventSort.column;
  const direction =
    candidate.direction === "asc" || candidate.direction === "desc"
      ? candidate.direction
      : defaultSortDirection(column);

  return { column, direction };
}

function readStoredEventSort(): EventSortState {
  if (typeof window === "undefined") {
    return defaultEventSort;
  }
  const stored = window.localStorage.getItem(eventListSortStorageKey);
  if (!stored) {
    return defaultEventSort;
  }
  try {
    return normalizeEventSort(JSON.parse(stored));
  } catch {
    return defaultEventSort;
  }
}

function compareText(
  left: string | null | undefined,
  right: string | null | undefined,
): number {
  return (left ?? "").localeCompare(right ?? "", undefined, {
    sensitivity: "base",
  });
}

function compareSensitivity(left: string, right: string): number {
  const rank = (value: string) => {
    const normalized = value.trim().toLowerCase();
    if (normalized === "critical") {
      return 4;
    }
    if (normalized === "high") {
      return 3;
    }
    if (normalized === "medium" || normalized === "internal") {
      return 2;
    }
    if (normalized === "low" || normalized === "public") {
      return 1;
    }
    return 0;
  };

  return rank(left) - rank(right);
}

function compareEvents(
  left: EventRecord,
  right: EventRecord,
  sort: EventSortState,
): number {
  let comparison = 0;
  if (sort.column === "observedAt") {
    comparison =
      new Date(left.observed_at).getTime() -
      new Date(right.observed_at).getTime();
  } else if (sort.column === "actor") {
    comparison = compareText(
      left.principal.display_name,
      right.principal.display_name,
    );
  } else if (sort.column === "actorEmail") {
    comparison = compareText(left.principal.email, right.principal.email);
  } else if (sort.column === "action") {
    comparison = compareText(left.action.verb, right.action.verb);
  } else if (sort.column === "resource") {
    comparison = compareText(left.resource.name, right.resource.name);
  } else if (sort.column === "source") {
    comparison = compareText(left.source, right.source);
  } else if (sort.column === "sensitivity") {
    comparison = compareSensitivity(
      left.resource.sensitivity,
      right.resource.sensitivity,
    );
  } else if (sort.column === "actionType") {
    comparison = compareText(left.action.category, right.action.category);
  } else if (sort.column === "eventId") {
    comparison = compareText(left.event_id, right.event_id);
  } else {
    comparison = compareText(left.source_event_id, right.source_event_id);
  }

  if (comparison === 0) {
    comparison = compareText(left.event_id, right.event_id);
  }

  return sort.direction === "asc" ? comparison : comparison * -1;
}

function SplitPane({ left, right, width, onResize }: SplitPaneProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const dragStateRef = useRef<{
    containerLeft: number;
    containerWidth: number;
  } | null>(null);
  const [dragging, setDragging] = useState(false);
  const style = {
    "--split-pane-width": `${width}px`,
    "--split-pane-min-width": `${splitPaneMinWidth}px`,
    "--split-pane-detail-min-width": `${splitPaneDetailMinWidth}px`,
    "--split-pane-divider-width": `${splitPaneDividerWidth}px`,
  } as CSSProperties;

  useEffect(() => {
    if (!dragging) {
      return;
    }

    function handlePointerMove(event: PointerEvent) {
      const dragState = dragStateRef.current;
      if (!dragState) {
        return;
      }
      onResize(
        clampSplitPaneWidth(
          event.clientX - dragState.containerLeft,
          dragState.containerWidth,
        ),
      );
    }

    function stopDragging() {
      dragStateRef.current = null;
      setDragging(false);
    }

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", stopDragging);
    window.addEventListener("pointercancel", stopDragging);

    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", stopDragging);
      window.removeEventListener("pointercancel", stopDragging);
    };
  }, [dragging, onResize]);

  function beginDragging(event: ReactPointerEvent<HTMLButtonElement>) {
    const rect = rootRef.current?.getBoundingClientRect();
    if (!rect) {
      return;
    }
    dragStateRef.current = {
      containerLeft: rect.left,
      containerWidth: rect.width,
    };
    setDragging(true);
    event.preventDefault();
  }

  function nudgeWidth(delta: number) {
    const rect = rootRef.current?.getBoundingClientRect();
    if (!rect) {
      return;
    }
    onResize(clampSplitPaneWidth(width + delta, rect.width));
  }

  return (
    <div
      className={dragging ? "split-pane split-pane--dragging" : "split-pane"}
      ref={rootRef}
      style={style}
    >
      <div className="split-pane__main">{left}</div>
      <button
        aria-label="Resize panes"
        className="split-pane__divider"
        onKeyDown={(event) => {
          if (event.key === "ArrowLeft") {
            event.preventDefault();
            nudgeWidth(-24);
          } else if (event.key === "ArrowRight") {
            event.preventDefault();
            nudgeWidth(24);
          }
        }}
        onPointerDown={beginDragging}
        type="button"
      >
        <span className="split-pane__grabber" />
      </button>
      <aside className="split-pane__detail">{right}</aside>
    </div>
  );
}

function AppFrame({
  sidebar,
  main,
  width,
  collapsed,
  onResize,
}: AppFrameProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [dragging, setDragging] = useState(false);

  useEffect(() => {
    if (!dragging || collapsed) {
      return;
    }

    function handlePointerMove(event: PointerEvent) {
      const rect = rootRef.current?.getBoundingClientRect();
      if (!rect) {
        return;
      }
      onResize(clampNavPaneWidth(event.clientX - rect.left));
    }

    function stopDragging() {
      setDragging(false);
    }

    window.addEventListener("pointermove", handlePointerMove);
    window.addEventListener("pointerup", stopDragging);
    window.addEventListener("pointercancel", stopDragging);

    return () => {
      window.removeEventListener("pointermove", handlePointerMove);
      window.removeEventListener("pointerup", stopDragging);
      window.removeEventListener("pointercancel", stopDragging);
    };
  }, [collapsed, dragging, onResize]);

  const style = {
    "--app-nav-width": `${collapsed ? navPaneCollapsedWidth : width}px`,
    "--app-nav-collapsed-width": `${navPaneCollapsedWidth}px`,
    "--app-nav-divider-width": `${navPaneDividerWidth}px`,
  } as CSSProperties;

  return (
    <div
      className={
        collapsed
          ? "app-frame app-frame--collapsed"
          : dragging
            ? "app-frame app-frame--dragging"
            : "app-frame"
      }
      ref={rootRef}
      style={style}
    >
      <aside className="app-frame__sidebar">{sidebar}</aside>
      <button
        aria-label="Resize navigation"
        className="app-frame__divider"
        disabled={collapsed}
        onPointerDown={(event) => {
          if (collapsed) {
            return;
          }
          setDragging(true);
          event.preventDefault();
        }}
        type="button"
      >
        <span className="split-pane__grabber" />
      </button>
      <main className="app-frame__main">{main}</main>
    </div>
  );
}

function MasterListPane({
  searchValue,
  onSearchChange,
  searchPlaceholder,
  resultCount,
  hasItems,
  hasResults,
  emptyState,
  noResultsState,
  children,
  footer,
  tone = "default",
  toolbarActions,
}: MasterListPaneProps) {
  return (
    <Section footer={footer} tone={tone}>
      <div className="master-list-pane">
        <div className="master-list-toolbar">
          <input
            aria-label={searchPlaceholder}
            className="search-input"
            onChange={(event) => onSearchChange(event.target.value)}
            placeholder={searchPlaceholder}
            type="search"
            value={searchValue}
          />
          <div className="master-list-toolbar__actions">
            {toolbarActions}
            <span className="panel-note">
              {formatNumber(resultCount)} shown
            </span>
          </div>
        </div>

        <div className="scroll-region">
          {!hasItems ? (
            <EmptyState title={emptyState.title} body={emptyState.body} />
          ) : !hasResults ? (
            <EmptyState
              title={noResultsState.title}
              body={noResultsState.body}
            />
          ) : (
            children
          )}
        </div>
      </div>
    </Section>
  );
}

interface EventColumnsMenuProps {
  visibleColumns: EventColumnKey[];
  onChange: (columns: EventColumnKey[]) => void;
}

function EventColumnsMenu({ visibleColumns, onChange }: EventColumnsMenuProps) {
  function toggleColumn(column: EventColumnKey) {
    const current = new Set(visibleColumns);
    if (current.has(column)) {
      if (visibleColumns.length === 1) {
        return;
      }
      current.delete(column);
    } else {
      current.add(column);
    }

    onChange(
      eventColumnOptions
        .map((option) => option.key)
        .filter((key) => current.has(key)),
    );
  }

  return (
    <details className="toolbar-menu">
      <summary className="toolbar-menu__summary toolbar-button">
        Columns
        <span className="toolbar-menu__summary-count">
          {formatNumber(visibleColumns.length)}
        </span>
      </summary>
      <div className="toolbar-menu__panel">
        <div className="toolbar-menu__section">
          <span className="toolbar-menu__label">Presets</span>
          <div className="toolbar-menu__presets">
            {eventColumnPresets.map((preset) => (
              <button
                className={
                  eventColumnsEqual(visibleColumns, preset.columns)
                    ? "toolbar-chip toolbar-chip--selected"
                    : "toolbar-chip"
                }
                key={preset.id}
                onClick={() => onChange(preset.columns)}
                type="button"
              >
                {preset.label}
              </button>
            ))}
          </div>
        </div>

        <div className="toolbar-menu__section">
          <span className="toolbar-menu__label">Visible columns</span>
          <div className="toolbar-menu__options">
            {eventColumnOptions.map((option) => {
              const checked = visibleColumns.includes(option.key);
              return (
                <label className="toolbar-menu__option" key={option.key}>
                  <input
                    checked={checked}
                    disabled={checked && visibleColumns.length === 1}
                    onChange={() => toggleColumn(option.key)}
                    type="checkbox"
                  />
                  <span>{option.label}</span>
                </label>
              );
            })}
          </div>
        </div>
      </div>
    </details>
  );
}

interface EventDensityToggleProps {
  density: EventDensity;
  onChange: (density: EventDensity) => void;
}

function EventDensityToggle({ density, onChange }: EventDensityToggleProps) {
  return (
    <button
      aria-label="Toggle density"
      className="toolbar-button toolbar-button--density"
      onClick={() =>
        onChange(density === "comfortable" ? "compact" : "comfortable")
      }
      title={`Switch to ${
        density === "comfortable" ? "compact" : "comfortable"
      } density`}
      type="button"
    >
      Density
      <span className="toolbar-menu__summary-count">
        {density === "comfortable" ? "Comfortable" : "Compact"}
      </span>
    </button>
  );
}

interface EventListTableProps {
  events: EventRecord[];
  visibleColumns: EventColumnKey[];
  selectedEventId: string | null;
  density: EventDensity;
  sort: EventSortState;
  formatObservedAt: (value: string) => string;
  formatSourceName: (source: string) => string;
  onSortChange: (column: EventColumnKey) => void;
  openEventPage: (eventId: string) => void;
  selectEvent: (eventId: string) => void;
}

function renderEventTableCell(
  event: EventRecord,
  column: EventColumnKey,
  formatObservedAtValue: (value: string) => string,
  formatSourceNameValue: (source: string) => string,
): ReactNode {
  const actorLabel = event.principal.display_name || "Unknown actor";
  const actorEmail = event.principal.email || "No email";
  const sourceLabel = formatSourceNameValue(event.source);
  const sensitivityLabel = formatTokenLabel(event.resource.sensitivity);
  const actionTypeLabel = formatTokenLabel(event.action.category);

  if (column === "observedAt") {
    return (
      <div>
        <div className="event-table__primary">
          {formatObservedAtValue(event.observed_at)}
        </div>
      </div>
    );
  }
  if (column === "actor") {
    return (
      <div>
        <div
          className="event-table__primary event-table__truncate"
          title={actorLabel}
        >
          {actorLabel}
        </div>
        {event.principal.email ? (
          <div
            className="event-table__secondary event-table__truncate"
            title={actorEmail}
          >
            {actorEmail}
          </div>
        ) : null}
      </div>
    );
  }
  if (column === "actorEmail") {
    return (
      <span
        className="event-table__secondary event-table__truncate"
        title={actorEmail}
      >
        {actorEmail}
      </span>
    );
  }
  if (column === "action") {
    return (
      <span
        className="event-table__primary event-table__truncate"
        title={event.action.verb}
      >
        {event.action.verb}
      </span>
    );
  }
  if (column === "resource") {
    return (
      <span
        className="event-table__primary event-table__truncate"
        title={event.resource.name}
      >
        {event.resource.name}
      </span>
    );
  }
  if (column === "source") {
    return (
      <span
        className="event-table__pill event-table__pill--neutral"
        title={sourceLabel}
      >
        {sourceLabel}
      </span>
    );
  }
  if (column === "sensitivity") {
    return (
      <span
        className={`event-table__pill event-table__pill--${sensitivityTone(
          event.resource.sensitivity,
        )}`}
        title={sensitivityLabel}
      >
        {sensitivityLabel}
      </span>
    );
  }
  if (column === "actionType") {
    return (
      <span
        className="event-table__pill event-table__pill--neutral"
        title={actionTypeLabel}
      >
        {actionTypeLabel}
      </span>
    );
  }
  if (column === "eventId") {
    return (
      <span
        className="event-table__code event-table__truncate"
        title={event.event_id}
      >
        {event.event_id}
      </span>
    );
  }
  return (
    <span
      className="event-table__code event-table__truncate"
      title={event.source_event_id}
    >
      {event.source_event_id}
    </span>
  );
}

function EventListTable({
  events,
  visibleColumns,
  selectedEventId,
  density,
  sort,
  formatObservedAt,
  formatSourceName,
  onSortChange,
  openEventPage,
  selectEvent,
}: EventListTableProps) {
  return (
    <div
      className={
        density === "compact"
          ? "event-table-shell event-table-shell--compact event-list--table"
          : "event-table-shell event-list--table"
      }
    >
      <table className="event-table">
        <thead>
          <tr>
            {visibleColumns.map((column) => (
              <th key={column} scope="col">
                <button
                  className={
                    sort.column === column
                      ? "event-table__sort event-table__sort--active"
                      : "event-table__sort"
                  }
                  onClick={() => onSortChange(column)}
                  type="button"
                >
                  <span>{eventColumnLabel(column)}</span>
                  <span
                    aria-hidden="true"
                    className="event-table__sort-indicator"
                  >
                    {sort.column === column
                      ? sort.direction === "asc"
                        ? "↑"
                        : "↓"
                      : "↕"}
                  </span>
                </button>
              </th>
            ))}
            <th className="event-table__actions-header" scope="col">
              Open
            </th>
          </tr>
        </thead>
        <tbody>
          {events.map((event) => {
            const selected = selectedEventId === event.event_id;
            return (
              <tr
                aria-selected={selected}
                className={
                  selected
                    ? "event-table__row event-table__row--selected"
                    : "event-table__row"
                }
                key={event.event_id}
                onClick={() => selectEvent(event.event_id)}
                onKeyDown={(keyboardEvent) => {
                  if (
                    keyboardEvent.key === "Enter" ||
                    keyboardEvent.key === " "
                  ) {
                    keyboardEvent.preventDefault();
                    selectEvent(event.event_id);
                  }
                }}
                tabIndex={0}
              >
                {visibleColumns.map((column) => (
                  <td key={column}>
                    {renderEventTableCell(
                      event,
                      column,
                      formatObservedAt,
                      formatSourceName,
                    )}
                  </td>
                ))}
                <td className="event-table__actions-cell">
                  <a
                    aria-label={`Open event ${event.event_id} in full page`}
                    className="list-row-action list-row-action--icon"
                    href={routeToHref({
                      kind: "event",
                      eventId: event.event_id,
                    })}
                    onClick={(clickEvent) => {
                      clickEvent.preventDefault();
                      clickEvent.stopPropagation();
                      openEventPage(event.event_id);
                    }}
                    title="Open full page"
                  >
                    <svg
                      aria-hidden="true"
                      className="list-row-action__icon"
                      viewBox="0 0 16 16"
                    >
                      <path
                        d="M6 3h7v7"
                        fill="none"
                        stroke="currentColor"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth="1.5"
                      />
                      <path
                        d="M13 3L7 9"
                        fill="none"
                        stroke="currentColor"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth="1.5"
                      />
                      <path
                        d="M11 8v4H3V4h4"
                        fill="none"
                        stroke="currentColor"
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth="1.5"
                      />
                    </svg>
                    <span className="visually-hidden">Open full page</span>
                  </a>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

interface EventListCardsProps {
  events: EventRecord[];
  selectedEventId: string | null;
  formatObservedAt: (value: string) => string;
  formatSourceName: (source: string) => string;
  openEventPage: (eventId: string) => void;
  selectEvent: (eventId: string) => void;
}

function EventListCards({
  events,
  selectedEventId,
  formatObservedAt,
  formatSourceName,
  openEventPage,
  selectEvent,
}: EventListCardsProps) {
  return (
    <div className="event-feed event-list--cards">
      {events.map((event) => (
        <article
          className={
            selectedEventId === event.event_id
              ? "event-row event-row--interactive event-row--selected"
              : "event-row event-row--interactive"
          }
          key={event.event_id}
        >
          <button
            className="event-row__surface"
            onClick={() => selectEvent(event.event_id)}
            type="button"
          >
            <div className="event-row__time">
              {formatObservedAt(event.observed_at)}
            </div>
            <div className="event-row__body">
              <div className="event-row__title">
                <strong>{event.principal.display_name}</strong>{" "}
                {event.action.verb} <strong>{event.resource.name}</strong>
              </div>
              <div className="event-row__meta">
                <span>{formatSourceName(event.source)}</span>
                <span>{event.resource.sensitivity}</span>
                <span>{event.action.category}</span>
              </div>
            </div>
          </button>
          <div className="list-row-actions">
            <a
              aria-label={`Open event ${event.event_id} in full page`}
              className="list-row-action list-row-action--icon"
              href={routeToHref({
                kind: "event",
                eventId: event.event_id,
              })}
              onClick={(clickEvent) => {
                clickEvent.preventDefault();
                openEventPage(event.event_id);
              }}
              title="Open full page"
            >
              <svg
                aria-hidden="true"
                className="list-row-action__icon"
                viewBox="0 0 16 16"
              >
                <path
                  d="M6 3h7v7"
                  fill="none"
                  stroke="currentColor"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="1.5"
                />
                <path
                  d="M13 3L7 9"
                  fill="none"
                  stroke="currentColor"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="1.5"
                />
                <path
                  d="M11 8v4H3V4h4"
                  fill="none"
                  stroke="currentColor"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth="1.5"
                />
              </svg>
              <span className="visually-hidden">Open full page</span>
            </a>
          </div>
        </article>
      ))}
    </div>
  );
}

function Pager({ page, label, onPrevious, onNext }: PagerProps) {
  if (page.returned === 0) {
    return null;
  }
  const start = page.offset + 1;
  const end = page.offset + page.returned;

  return (
    <div className="pager">
      <div>
        <strong>{label}</strong>
        <span>
          {formatNumber(start)}-{formatNumber(end)}
          {page.total !== null && page.total !== undefined
            ? ` of ${formatNumber(page.total)}`
            : " shown"}
        </span>
      </div>
      <div className="pager__actions">
        <button
          aria-label={`Previous ${label.toLowerCase()}`}
          className="pager__nav-button"
          disabled={page.offset === 0}
          onClick={onPrevious}
          title={`Previous ${label.toLowerCase()}`}
        >
          ‹
        </button>
        <button
          aria-label={`Next ${label.toLowerCase()}`}
          className="pager__nav-button"
          disabled={!page.has_more}
          onClick={onNext}
          title={`Next ${label.toLowerCase()}`}
        >
          ›
        </button>
      </div>
    </div>
  );
}

function DetailPane({
  acknowledgingDetectionId,
  selectedItem,
  detailBusy,
  onAcknowledgeDetection,
  openEventPage,
}: DetailPaneProps) {
  const titleHref = selectedItem
    ? selectedItem.type === "detection"
      ? routeToHref({
          kind: "detection",
          detectionId: selectedItem.id,
        })
      : routeToHref({
          kind: "event",
          eventId: selectedItem.id,
        })
    : undefined;

  return (
    <Section title="Details" titleHref={titleHref} tone="detail">
      {detailBusy ? (
        <EmptyState
          title="Loading detail…"
          body="Fetching the selected record."
        />
      ) : !selectedItem ? (
        <EmptyState title="Nothing selected yet." body="Select a record." />
      ) : selectedItem.type === "detection" ? (
        <DetectionDetailContent
          acknowledging={acknowledgingDetectionId === selectedItem.id}
          badgeToneForSeverity={badgeToneForSeverity}
          detail={selectedItem.data}
          formatNumber={formatNumber}
          formatObservedAt={formatObservedAt}
          formatSourceName={formatSourceName}
          onAcknowledge={() => onAcknowledgeDetection(selectedItem.id)}
          openEventPage={openEventPage}
          stackSummaryCards
        />
      ) : (
        <EventDetailContent
          event={selectedItem.data}
          formatObservedAt={formatObservedAt}
          formatSourceName={formatSourceName}
        />
      )}
    </Section>
  );
}

function IntegrationDetailPane({
  entry,
  formatObservedAt,
  titleHref,
}: IntegrationDetailPaneProps) {
  if (!entry) {
    return (
      <Section title="Details" tone="detail">
        <EmptyState
          title="No integration selected"
          body="Pick a source from the list to inspect its contract, state, and next actions."
        />
      </Section>
    );
  }

  return (
    <Section title="Details" titleHref={titleHref} tone="detail">
      <IntegrationDetailContent
        entry={entry}
        formatNumber={formatNumber}
        formatObservedAt={formatObservedAt}
        integrationActionItems={integrationActionItems}
        integrationCoverageCount={integrationCoverageCount}
        integrationStatus={integrationStatus}
      />
    </Section>
  );
}

function StreamOverlay({
  busy,
  streamState,
  workerState,
  performAction,
}: StreamOverlayProps) {
  const workerStatus =
    workerState.pending_batch_count > 0
      ? `${formatNumber(workerState.pending_batch_count)} pending`
      : (workerState.last_service_status ?? "idle");

  return (
    <div className="stream-overlay">
      <div className="stream-overlay__header">
        <div>
          <span className="eyebrow">Runtime Controls</span>
          <strong>
            {streamState.cursor ?? 0}/{streamState.total_source_events ?? 0}
          </strong>
        </div>
        <span className="stream-overlay__status">
          {streamState.complete ? "Current" : "In progress"}
        </span>
      </div>
      <div className="stream-overlay__meta">
        <span>{formatNumber(streamState.detection_count)} detections</span>
        <span>Worker {workerStatus}</span>
      </div>
      <div className="stream-overlay__actions">
        <button
          disabled={busy}
          onClick={() => performAction("/api/stream/reset")}
        >
          Restart stream
        </button>
        <button
          disabled={busy}
          onClick={() => performAction("/api/stream/advance?batch_size=10")}
        >
          Advance 10
        </button>
        <button
          disabled={busy}
          onClick={() => performAction("/api/stream/advance?batch_size=25")}
        >
          Advance 25
        </button>
        <button
          disabled={busy}
          onClick={() => performAction("/api/stream/advance?batch_size=50")}
        >
          Advance 50
        </button>
      </div>
    </div>
  );
}

function pageShortLabel(page: Page): string {
  if (page === "detections") {
    return "DE";
  }
  if (page === "events") {
    return "EV";
  }
  return "IN";
}

export function App() {
  const initialRoute =
    typeof window === "undefined"
      ? ({ kind: "page", page: "detections" } satisfies Route)
      : parseRouteFromHash(window.location.hash);
  const [route, setRoute] = useState<Route>(initialRoute);
  const [queueOffset, setQueueOffset] = useState(0);
  const [eventsOffset, setEventsOffset] = useState(0);
  const [overview, setOverview] = useState<Overview | null>(null);
  const [streamState, setStreamState] = useState<StreamState>(emptyStreamState);
  const [workerState, setWorkerState] = useState<WorkerState>(emptyWorkerState);
  const [detections, setDetections] = useState<DetectionList>({
    items: [],
    page: emptyPagination,
  });
  const [events, setEvents] = useState<EventList>({
    items: [],
    page: emptyPagination,
  });
  const [sourceCapability, setSourceCapability] =
    useState<SourceCapabilityMatrix | null>(null);
  const [selectedItem, setSelectedItem] = useState<SelectedItem | null>(null);
  const [selectedIntegrationSource, setSelectedIntegrationSource] = useState<
    string | null
  >(null);
  const [detectionSearch, setDetectionSearch] = useState("");
  const [eventSearch, setEventSearch] = useState("");
  const [integrationSearch, setIntegrationSearch] = useState("");
  const [eventVisibleColumns, setEventVisibleColumns] = useState<
    EventColumnKey[]
  >(readStoredEventColumns);
  const [eventDensity, setEventDensity] = useState<EventDensity>(
    readStoredEventDensity,
  );
  const [eventSort, setEventSort] =
    useState<EventSortState>(readStoredEventSort);
  const [detectionDensity, setDetectionDensity] = useState<EventDensity>(() =>
    readStoredDensity(detectionListDensityStorageKey),
  );
  const [integrationDensity, setIntegrationDensity] = useState<EventDensity>(
    () => readStoredDensity(integrationListDensityStorageKey),
  );
  const [navPaneWidth, setNavPaneWidth] = useState(readStoredNavPaneWidth);
  const [navPaneCollapsed, setNavPaneCollapsed] = useState(
    readStoredNavPaneCollapsed,
  );
  const [splitPaneWidth, setSplitPaneWidth] = useState(
    readStoredSplitPaneWidth,
  );
  const [busy, setBusy] = useState(false);
  const [detailBusy, setDetailBusy] = useState(
    initialRoute.kind === "detection" || initialRoute.kind === "event",
  );
  const [acknowledgingDetectionId, setAcknowledgingDetectionId] = useState<
    string | null
  >(null);
  const [error, setError] = useState("");
  const page = pageForRoute(route);

  async function refreshDashboard() {
    try {
      const data = await fetchDashboardData(queueOffset, eventsOffset);
      applyDashboardData(data, {
        setOverview,
        setDetections,
        setEvents,
        setSourceCapability,
        setStreamState,
        setWorkerState,
        setError,
      });
    } catch (loadError) {
      setError(getErrorMessage(loadError));
    }
  }

  useEffect(() => {
    async function pollDashboard() {
      try {
        const data = await fetchDashboardData(queueOffset, eventsOffset);
        applyDashboardData(data, {
          setOverview,
          setDetections,
          setEvents,
          setSourceCapability,
          setStreamState,
          setWorkerState,
          setError,
        });
      } catch (loadError) {
        setError(getErrorMessage(loadError));
      }
    }

    void pollDashboard();
    const timer = window.setInterval(() => {
      void pollDashboard();
    }, pollIntervalMs);
    return () => window.clearInterval(timer);
  }, [eventsOffset, queueOffset]);

  useEffect(() => {
    const onHashChange = () => {
      setRoute(parseRouteFromHash(window.location.hash));
    };

    window.addEventListener("hashchange", onHashChange);
    return () => window.removeEventListener("hashchange", onHashChange);
  }, []);

  useEffect(() => {
    setSelectedItem(null);
  }, [page]);

  const handleDetectionRoute = useEffectEvent((detectionId: string) => {
    void selectDetection(detectionId);
  });
  const handleEventRoute = useEffectEvent((eventId: string) => {
    void selectEvent(eventId);
  });

  useEffect(() => {
    if (route.kind === "detection") {
      handleDetectionRoute(route.detectionId);
      return;
    }
    if (route.kind !== "event") {
      return;
    }
    handleEventRoute(route.eventId);
  }, [route]);

  useEffect(() => {
    window.localStorage.setItem(splitPaneStorageKey, String(splitPaneWidth));
  }, [splitPaneWidth]);

  useEffect(() => {
    window.localStorage.setItem(navPaneStorageKey, String(navPaneWidth));
  }, [navPaneWidth]);

  useEffect(() => {
    window.localStorage.setItem(
      navPaneCollapsedStorageKey,
      String(navPaneCollapsed),
    );
  }, [navPaneCollapsed]);

  useEffect(() => {
    window.localStorage.setItem(
      eventListColumnsStorageKey,
      JSON.stringify(eventVisibleColumns),
    );
  }, [eventVisibleColumns]);

  useEffect(() => {
    window.localStorage.setItem(eventListDensityStorageKey, eventDensity);
  }, [eventDensity]);

  useEffect(() => {
    window.localStorage.setItem(
      eventListSortStorageKey,
      JSON.stringify(eventSort),
    );
  }, [eventSort]);

  useEffect(() => {
    window.localStorage.setItem(
      detectionListDensityStorageKey,
      detectionDensity,
    );
  }, [detectionDensity]);

  useEffect(() => {
    window.localStorage.setItem(
      integrationListDensityStorageKey,
      integrationDensity,
    );
  }, [integrationDensity]);

  const deferredDetectionSearch = useDeferredValue(detectionSearch.trim());
  const filteredDetections = detections.items.filter((detection) => {
    if (deferredDetectionSearch.length === 0) {
      return true;
    }
    const query = deferredDetectionSearch.toLowerCase();
    return (
      detection.title.toLowerCase().includes(query) ||
      detection.scenario.toLowerCase().includes(query) ||
      detection.reasons.some((reason) => reason.toLowerCase().includes(query))
    );
  });

  const deferredEventSearch = useDeferredValue(eventSearch.trim());
  const filteredEvents = events.items.filter((event) => {
    if (deferredEventSearch.length === 0) {
      return true;
    }
    const query = deferredEventSearch.toLowerCase();
    return (
      event.principal.display_name.toLowerCase().includes(query) ||
      event.resource.name.toLowerCase().includes(query) ||
      event.action.verb.toLowerCase().includes(query) ||
      event.action.category.toLowerCase().includes(query) ||
      formatSourceName(event.source).toLowerCase().includes(query)
    );
  });
  const sortedFilteredEvents = [...filteredEvents].sort((left, right) =>
    compareEvents(left, right, eventSort),
  );

  const integrationEntries = sortIntegrationEntries(
    sourceCapability?.sources ?? {},
  );
  const deferredIntegrationSearch = useDeferredValue(integrationSearch.trim());
  const filteredIntegrationEntries = integrationEntries.filter((entry) => {
    if (deferredIntegrationSearch.length === 0) {
      return true;
    }
    const query = deferredIntegrationSearch.toLowerCase();
    return (
      entry.source.toLowerCase().includes(query) ||
      entry.details.display_name.toLowerCase().includes(query) ||
      formatSourceName(entry.source).toLowerCase().includes(query)
    );
  });
  const filteredIntegrationSignature = filteredIntegrationEntries
    .map((entry) => entry.source)
    .join("|");
  const firstFilteredIntegrationSource =
    filteredIntegrationEntries[0]?.source ?? null;

  useEffect(() => {
    if (page !== "integrations") {
      return;
    }

    if (filteredIntegrationEntries.length === 0) {
      if (selectedIntegrationSource !== null) {
        setSelectedIntegrationSource(null);
      }
      return;
    }

    const hasCurrentSelection = filteredIntegrationEntries.some(
      (entry) => entry.source === selectedIntegrationSource,
    );
    if (!hasCurrentSelection) {
      setSelectedIntegrationSource(firstFilteredIntegrationSource);
    }
  }, [
    filteredIntegrationEntries,
    filteredIntegrationSignature,
    firstFilteredIntegrationSource,
    page,
    selectedIntegrationSource,
  ]);

  function navigateToRoute(nextRoute: Route) {
    window.location.hash = routeToHash(nextRoute);
    setRoute(nextRoute);
  }

  function navigate(nextPage: Page) {
    navigateToRoute({ kind: "page", page: nextPage });
  }

  function openEventPage(eventId: string) {
    setDetailBusy(true);
    navigateToRoute({ kind: "event", eventId });
  }

  function openDetectionPage(detectionId: string) {
    setDetailBusy(true);
    navigateToRoute({ kind: "detection", detectionId });
  }

  function openIntegrationPage(source: string) {
    navigateToRoute({ kind: "integration", source });
  }

  async function performAction(path: string) {
    setBusy(true);
    try {
      await fetchJson(path, { method: "POST" });
      await refreshDashboard();
      if (selectedItem?.type === "detection") {
        await selectDetection(selectedItem.id);
      } else if (selectedItem?.type === "event") {
        await selectEvent(selectedItem.id);
      }
    } catch (actionError) {
      setError(getErrorMessage(actionError));
    } finally {
      setBusy(false);
    }
  }

  async function selectDetection(detectionId: string) {
    setDetailBusy(true);
    try {
      const detail = await fetchJson<DetectionDetail>(
        `/api/detections/${detectionId}`,
      );
      setSelectedItem({ type: "detection", id: detectionId, data: detail });
      setError("");
    } catch (detailError) {
      setError(getErrorMessage(detailError));
    } finally {
      setDetailBusy(false);
    }
  }

  async function selectEvent(eventId: string) {
    setDetailBusy(true);
    try {
      const detail = await fetchJson<EventDetail>(`/api/events/${eventId}`);
      setSelectedItem({ type: "event", id: eventId, data: detail });
      setError("");
    } catch (detailError) {
      setError(getErrorMessage(detailError));
    } finally {
      setDetailBusy(false);
    }
  }

  async function acknowledgeDetection(detectionId: string) {
    setAcknowledgingDetectionId(detectionId);
    try {
      await fetchJson<Detection>(`/api/detections/${detectionId}/acknowledge`, {
        method: "POST",
      });
      await refreshDashboard();
      if (route.kind === "detection" && route.detectionId === detectionId) {
        await selectDetection(detectionId);
      } else {
        setSelectedItem(null);
      }
      setError("");
    } catch (acknowledgeError) {
      setError(getErrorMessage(acknowledgeError));
    } finally {
      setAcknowledgingDetectionId(null);
    }
  }

  const selectedIntegrationEntry =
    filteredIntegrationEntries.find(
      (entry) => entry.source === selectedIntegrationSource,
    ) ?? null;
  const routeIntegrationEntry =
    route.kind === "integration"
      ? (integrationEntries.find((entry) => entry.source === route.source) ??
        null)
      : null;
  const counts = {
    events: null,
    detections: overview?.stream_state.detection_count ?? 0,
    integrations: integrationEntries.filter(
      (entry) => integrationStatus(entry.details).tone !== "positive",
    ).length,
  };
  const currentPageTitle =
    route.kind === "detection"
      ? `Detection - ${route.detectionId}`
      : route.kind === "event"
        ? `Event - ${route.eventId}`
        : route.kind === "integration"
          ? `Integration - ${route.source}`
          : pageMeta(page, counts).title;
  const navItems = [
    {
      page: "detections" as const,
      label: "Detections",
      count: counts.detections,
    },
    {
      page: "events" as const,
      label: "Events",
      count: counts.events,
    },
    {
      page: "integrations" as const,
      label: "Integrations",
      count: counts.integrations,
    },
  ];
  const selectedDetection =
    route.kind === "detection" &&
    selectedItem?.type === "detection" &&
    selectedItem.id === route.detectionId
      ? selectedItem.data
      : null;
  const selectedEvent =
    route.kind === "event" &&
    selectedItem?.type === "event" &&
    selectedItem.id === route.eventId
      ? selectedItem.data
      : null;

  return (
    <div className="app-shell">
      <AppFrame
        collapsed={navPaneCollapsed}
        main={
          <>
            <header className="app-topbar">
              <button
                className="app-brand app-brand--button"
                onClick={() => navigate("detections")}
              >
                <span className="app-brand__mark">SC</span>
                <div>
                  <div className="app-brand__name">{currentPageTitle}</div>
                </div>
              </button>
            </header>

            {error ? <div className="error-banner">{error}</div> : null}

            {route.kind === "detection" ? (
              <RecordPage>
                {detailBusy ? (
                  <EmptyState
                    title="Loading detail…"
                    body="Fetching the requested detection."
                  />
                ) : selectedDetection ? (
                  <DetectionDetailContent
                    acknowledging={
                      acknowledgingDetectionId ===
                      selectedDetection.detection.detection_id
                    }
                    badgeToneForSeverity={badgeToneForSeverity}
                    detail={selectedDetection}
                    formatNumber={formatNumber}
                    formatObservedAt={formatObservedAt}
                    formatSourceName={formatSourceName}
                    onAcknowledge={() =>
                      void acknowledgeDetection(
                        selectedDetection.detection.detection_id,
                      )
                    }
                    openEventPage={openEventPage}
                  />
                ) : (
                  <EmptyState
                    title="Detection unavailable"
                    body={`No detection detail is available for ${route.detectionId}.`}
                  />
                )}
              </RecordPage>
            ) : null}

            {route.kind === "event" ? (
              <RecordPage>
                {detailBusy ? (
                  <EmptyState
                    title="Loading detail…"
                    body="Fetching the requested event."
                  />
                ) : selectedEvent ? (
                  <EventDetailContent
                    event={selectedEvent}
                    formatObservedAt={formatObservedAt}
                    formatSourceName={formatSourceName}
                  />
                ) : (
                  <EmptyState
                    title="Event unavailable"
                    body={`No event detail is available for ${route.eventId}.`}
                  />
                )}
              </RecordPage>
            ) : null}

            {route.kind === "integration" ? (
              <RecordPage>
                {sourceCapability === null ? (
                  <EmptyState
                    title="Loading detail…"
                    body="Fetching the requested integration."
                  />
                ) : routeIntegrationEntry ? (
                  <IntegrationDetailContent
                    entry={routeIntegrationEntry}
                    formatNumber={formatNumber}
                    formatObservedAt={formatObservedAt}
                    integrationActionItems={integrationActionItems}
                    integrationCoverageCount={integrationCoverageCount}
                    integrationStatus={integrationStatus}
                  />
                ) : (
                  <EmptyState
                    title="Integration unavailable"
                    body={`No integration detail is available for ${route.source}.`}
                  />
                )}
              </RecordPage>
            ) : null}

            {route.kind === "page" && page === "detections" ? (
              <SplitPane
                left={
                  <MasterListPane
                    emptyState={{
                      title: "No detections",
                      body: "No detections available.",
                    }}
                    footer={
                      <Pager
                        label="Page"
                        page={detections.page}
                        onPrevious={() =>
                          setQueueOffset((current) =>
                            Math.max(0, current - queuePageSize),
                          )
                        }
                        onNext={() =>
                          setQueueOffset((current) => current + queuePageSize)
                        }
                      />
                    }
                    hasItems={detections.items.length > 0}
                    hasResults={filteredDetections.length > 0}
                    noResultsState={{
                      title: "No matches",
                      body: "Try a different detection search.",
                    }}
                    onSearchChange={setDetectionSearch}
                    resultCount={filteredDetections.length}
                    searchPlaceholder="Search detections"
                    searchValue={detectionSearch}
                    toolbarActions={
                      <EventDensityToggle
                        density={detectionDensity}
                        onChange={setDetectionDensity}
                      />
                    }
                    tone="priority"
                  >
                    <div className="list-stack">
                      {filteredDetections.map((detection) => (
                        <article
                          className={
                            selectedItem?.type === "detection" &&
                            selectedItem.id === detection.detection_id
                              ? detectionDensity === "compact"
                                ? "record-card record-card--interactive record-card--selected record-card--compact"
                                : "record-card record-card--interactive record-card--selected"
                              : detectionDensity === "compact"
                                ? "record-card record-card--interactive record-card--compact"
                                : "record-card record-card--interactive"
                          }
                          key={detection.detection_id}
                        >
                          <button
                            className="record-card__surface"
                            onClick={() =>
                              selectDetection(detection.detection_id)
                            }
                            type="button"
                          >
                            <div className="record-card__header">
                              <div>
                                <h3>{detection.title}</h3>
                                <p>{detection.scenario}</p>
                              </div>
                              <span
                                className={`badge badge--${badgeToneForSeverity(
                                  detection.severity,
                                )}`}
                              >
                                {detection.severity}
                              </span>
                            </div>
                            <div className="record-card__summary">
                              <div className="record-card__summary-item">
                                <span>Score</span>
                                <strong>{detection.score.toFixed(2)}</strong>
                              </div>
                              <div className="record-card__summary-item">
                                <span>Confidence</span>
                                <strong>
                                  {detection.confidence.toFixed(2)}
                                </strong>
                              </div>
                              <div className="record-card__summary-item">
                                <span>Evidence</span>
                                <strong>
                                  {formatNumber(detection.evidence.length)}
                                </strong>
                              </div>
                              <div className="record-card__summary-item">
                                <span>Status</span>
                                <strong>{detection.status}</strong>
                              </div>
                            </div>
                            <div className="token-row">
                              <span className="token token--muted">
                                {detection.scenario}
                              </span>
                              {detection.reasons.slice(0, 3).map((reason) => (
                                <span className="token" key={reason}>
                                  {reason}
                                </span>
                              ))}
                            </div>
                          </button>
                          <div className="list-row-actions">
                            <a
                              aria-label={`Open detection ${detection.detection_id} in full page`}
                              className="list-row-action list-row-action--icon"
                              href={routeToHref({
                                kind: "detection",
                                detectionId: detection.detection_id,
                              })}
                              onClick={(event) => {
                                event.preventDefault();
                                openDetectionPage(detection.detection_id);
                              }}
                              title="Open full page"
                            >
                              <svg
                                aria-hidden="true"
                                className="list-row-action__icon"
                                viewBox="0 0 16 16"
                              >
                                <path
                                  d="M6 3h7v7"
                                  fill="none"
                                  stroke="currentColor"
                                  strokeLinecap="round"
                                  strokeLinejoin="round"
                                  strokeWidth="1.5"
                                />
                                <path
                                  d="M13 3L7 9"
                                  fill="none"
                                  stroke="currentColor"
                                  strokeLinecap="round"
                                  strokeLinejoin="round"
                                  strokeWidth="1.5"
                                />
                                <path
                                  d="M11 8v4H3V4h4"
                                  fill="none"
                                  stroke="currentColor"
                                  strokeLinecap="round"
                                  strokeLinejoin="round"
                                  strokeWidth="1.5"
                                />
                              </svg>
                              <span className="visually-hidden">
                                Open full page
                              </span>
                            </a>
                          </div>
                        </article>
                      ))}
                    </div>
                  </MasterListPane>
                }
                onResize={setSplitPaneWidth}
                right={
                  <DetailPane
                    acknowledgingDetectionId={acknowledgingDetectionId}
                    detailBusy={detailBusy}
                    onAcknowledgeDetection={(detectionId) => {
                      void acknowledgeDetection(detectionId);
                    }}
                    openEventPage={openEventPage}
                    selectedItem={selectedItem}
                  />
                }
                width={splitPaneWidth}
              />
            ) : null}

            {route.kind === "page" && page === "events" ? (
              <SplitPane
                left={
                  <MasterListPane
                    emptyState={{
                      title: "No events",
                      body: "No events available.",
                    }}
                    footer={
                      <Pager
                        label="Page"
                        page={events.page}
                        onPrevious={() =>
                          setEventsOffset((current) =>
                            Math.max(0, current - eventsPageSize),
                          )
                        }
                        onNext={() =>
                          setEventsOffset((current) => current + eventsPageSize)
                        }
                      />
                    }
                    hasItems={events.items.length > 0}
                    hasResults={filteredEvents.length > 0}
                    noResultsState={{
                      title: "No matches",
                      body: "Try a different event search.",
                    }}
                    onSearchChange={setEventSearch}
                    resultCount={filteredEvents.length}
                    searchPlaceholder="Search events"
                    searchValue={eventSearch}
                    toolbarActions={
                      <>
                        <EventDensityToggle
                          density={eventDensity}
                          onChange={setEventDensity}
                        />
                        <EventColumnsMenu
                          onChange={(columns) => {
                            setEventVisibleColumns(columns);
                            if (!columns.includes(eventSort.column)) {
                              setEventSort(defaultEventSort);
                            }
                          }}
                          visibleColumns={eventVisibleColumns}
                        />
                      </>
                    }
                  >
                    <EventListTable
                      density={eventDensity}
                      events={sortedFilteredEvents}
                      formatObservedAt={formatObservedAt}
                      formatSourceName={formatSourceName}
                      onSortChange={(column) => {
                        setEventSort((current) => {
                          if (current.column === column) {
                            return {
                              column,
                              direction:
                                current.direction === "asc" ? "desc" : "asc",
                            };
                          }
                          return {
                            column,
                            direction: defaultSortDirection(column),
                          };
                        });
                      }}
                      openEventPage={openEventPage}
                      selectEvent={(eventId) => {
                        void selectEvent(eventId);
                      }}
                      selectedEventId={
                        selectedItem?.type === "event" ? selectedItem.id : null
                      }
                      sort={eventSort}
                      visibleColumns={eventVisibleColumns}
                    />
                    <EventListCards
                      events={sortedFilteredEvents}
                      formatObservedAt={formatObservedAt}
                      formatSourceName={formatSourceName}
                      openEventPage={openEventPage}
                      selectEvent={(eventId) => {
                        void selectEvent(eventId);
                      }}
                      selectedEventId={
                        selectedItem?.type === "event" ? selectedItem.id : null
                      }
                    />
                  </MasterListPane>
                }
                onResize={setSplitPaneWidth}
                right={
                  <DetailPane
                    acknowledgingDetectionId={acknowledgingDetectionId}
                    detailBusy={detailBusy}
                    onAcknowledgeDetection={(detectionId) => {
                      void acknowledgeDetection(detectionId);
                    }}
                    openEventPage={openEventPage}
                    selectedItem={selectedItem}
                  />
                }
                width={splitPaneWidth}
              />
            ) : null}

            {route.kind === "page" && page === "integrations" ? (
              <SplitPane
                left={
                  <MasterListPane
                    emptyState={{
                      title: "No integrations",
                      body: "No source capability data is available yet.",
                    }}
                    hasItems={integrationEntries.length > 0}
                    hasResults={filteredIntegrationEntries.length > 0}
                    noResultsState={{
                      title: "No matches",
                      body: "Try a different integration search.",
                    }}
                    onSearchChange={setIntegrationSearch}
                    resultCount={filteredIntegrationEntries.length}
                    searchPlaceholder="Search integrations"
                    searchValue={integrationSearch}
                    toolbarActions={
                      <EventDensityToggle
                        density={integrationDensity}
                        onChange={setIntegrationDensity}
                      />
                    }
                  >
                    <div className="integration-master-list">
                      {filteredIntegrationEntries.map((entry) => {
                        const status = integrationStatus(entry.details);
                        const coverageCount = integrationCoverageCount(
                          entry.details,
                        );
                        return (
                          <article
                            className={
                              entry.source === selectedIntegrationSource
                                ? integrationDensity === "compact"
                                  ? "integration-list-item integration-list-item--interactive integration-list-item--selected integration-list-item--compact"
                                  : "integration-list-item integration-list-item--interactive integration-list-item--selected"
                                : integrationDensity === "compact"
                                  ? "integration-list-item integration-list-item--interactive integration-list-item--compact"
                                  : "integration-list-item integration-list-item--interactive"
                            }
                            key={entry.source}
                          >
                            <button
                              className="integration-list-item__surface"
                              onClick={() =>
                                setSelectedIntegrationSource(entry.source)
                              }
                              type="button"
                            >
                              <div className="integration-list-item__top">
                                <div>
                                  <h3>{entry.details.display_name}</h3>
                                  <p>{entry.source}</p>
                                </div>
                                <span className={`badge badge--${status.tone}`}>
                                  {status.label}
                                </span>
                              </div>
                              <div className="token-row">
                                <span className="token token--muted">
                                  {entry.source}
                                </span>
                                {entry.details.missing_required_event_types
                                  .length > 0 ? (
                                  <span className="token token--critical">
                                    {formatNumber(
                                      entry.details.missing_required_event_types
                                        .length,
                                    )}{" "}
                                    missing types
                                  </span>
                                ) : null}
                                {entry.details.missing_required_fields.length >
                                0 ? (
                                  <span className="token token--critical">
                                    {formatNumber(
                                      entry.details.missing_required_fields
                                        .length,
                                    )}{" "}
                                    missing fields
                                  </span>
                                ) : null}
                                {entry.details.dead_letter_7d_count > 0 ? (
                                  <span className="token token--critical">
                                    {formatNumber(
                                      entry.details.dead_letter_7d_count,
                                    )}{" "}
                                    dead letters
                                  </span>
                                ) : null}
                                {entry.details.dead_letter_7d_count === 0 &&
                                entry.details.missing_required_event_types
                                  .length === 0 &&
                                entry.details.missing_required_fields.length ===
                                  0 ? (
                                  <span className="token token--positive">
                                    No current contract issues
                                  </span>
                                ) : null}
                              </div>
                              <p className="integration-list-item__note">
                                {status.note}
                              </p>
                              <div className="record-card__summary">
                                <div className="record-card__summary-item">
                                  <span>24h landed</span>
                                  <strong>
                                    {formatNumber(entry.details.raw_24h_count)}
                                  </strong>
                                </div>
                                <div className="record-card__summary-item">
                                  <span>24h normalized</span>
                                  <strong>
                                    {formatNumber(
                                      entry.details.normalized_24h_count,
                                    )}
                                  </strong>
                                </div>
                                <div className="record-card__summary-item">
                                  <span>7d dead letters</span>
                                  <strong>
                                    {formatNumber(
                                      entry.details.dead_letter_7d_count,
                                    )}
                                  </strong>
                                </div>
                                <div className="record-card__summary-item">
                                  <span>Field coverage</span>
                                  <strong>
                                    {formatNumber(coverageCount)}/
                                    {formatNumber(
                                      entry.details.required_fields.length,
                                    )}
                                  </strong>
                                </div>
                              </div>
                            </button>
                            <div className="list-row-actions">
                              <a
                                aria-label={`Open integration ${entry.source} in full page`}
                                className="list-row-action list-row-action--icon"
                                href={routeToHref({
                                  kind: "integration",
                                  source: entry.source,
                                })}
                                onClick={(event) => {
                                  event.preventDefault();
                                  openIntegrationPage(entry.source);
                                }}
                                title="Open full page"
                              >
                                <svg
                                  aria-hidden="true"
                                  className="list-row-action__icon"
                                  viewBox="0 0 16 16"
                                >
                                  <path
                                    d="M6 3h7v7"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    strokeWidth="1.5"
                                  />
                                  <path
                                    d="M13 3L7 9"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    strokeWidth="1.5"
                                  />
                                  <path
                                    d="M11 8v4H3V4h4"
                                    fill="none"
                                    stroke="currentColor"
                                    strokeLinecap="round"
                                    strokeLinejoin="round"
                                    strokeWidth="1.5"
                                  />
                                </svg>
                                <span className="visually-hidden">
                                  Open full page
                                </span>
                              </a>
                            </div>
                          </article>
                        );
                      })}
                    </div>
                  </MasterListPane>
                }
                onResize={setSplitPaneWidth}
                right={
                  <IntegrationDetailPane
                    entry={selectedIntegrationEntry}
                    formatObservedAt={formatObservedAt}
                    titleHref={
                      selectedIntegrationEntry
                        ? routeToHref({
                            kind: "integration",
                            source: selectedIntegrationEntry.source,
                          })
                        : undefined
                    }
                  />
                }
                width={splitPaneWidth}
              />
            ) : null}
          </>
        }
        onResize={setNavPaneWidth}
        sidebar={
          <div
            className={
              navPaneCollapsed
                ? "app-sidebar app-sidebar--collapsed"
                : "app-sidebar"
            }
          >
            <div className="app-sidebar__header">
              <button
                className="app-sidebar__brand"
                onClick={() => navigate("detections")}
                title="Seccloud"
                type="button"
              >
                <span className="app-brand__mark">SC</span>
                {!navPaneCollapsed ? (
                  <div className="app-sidebar__brand-text">
                    <strong>Seccloud</strong>
                    <span>Console</span>
                  </div>
                ) : null}
              </button>
              <button
                aria-label={
                  navPaneCollapsed ? "Expand navigation" : "Collapse navigation"
                }
                className="app-sidebar__collapse"
                onClick={() => setNavPaneCollapsed((current) => !current)}
                title={
                  navPaneCollapsed ? "Expand navigation" : "Collapse navigation"
                }
                type="button"
              >
                <svg
                  aria-hidden="true"
                  className="app-sidebar__collapse-icon"
                  viewBox="0 0 16 16"
                >
                  <path
                    d={navPaneCollapsed ? "M6 3l5 5-5 5" : "M10 3L5 8l5 5"}
                    fill="none"
                    stroke="currentColor"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth="1.5"
                  />
                </svg>
                <span className="visually-hidden">
                  {navPaneCollapsed
                    ? "Expand navigation"
                    : "Collapse navigation"}
                </span>
              </button>
            </div>

            <div className="app-sidebar__section">
              {!navPaneCollapsed ? (
                <span className="app-sidebar__eyebrow">Views</span>
              ) : null}
              <nav className="app-sidebar__nav" aria-label="Primary">
                {navItems.map((item) => (
                  <button
                    className={
                      page === item.page
                        ? "app-sidebar__nav-item app-sidebar__nav-item--active"
                        : "app-sidebar__nav-item"
                    }
                    key={item.page}
                    onClick={() => navigate(item.page)}
                    title={item.label}
                    type="button"
                  >
                    <span className="app-sidebar__nav-mark">
                      {pageShortLabel(item.page)}
                    </span>
                    {!navPaneCollapsed ? (
                      <>
                        <span className="app-sidebar__nav-label">
                          {item.label}
                        </span>
                        {item.count !== null && item.count !== undefined ? (
                          <span className="app-sidebar__nav-count">
                            {formatNumber(item.count)}
                          </span>
                        ) : null}
                      </>
                    ) : null}
                  </button>
                ))}
              </nav>
            </div>
          </div>
        }
        width={navPaneWidth}
      />

      <StreamOverlay
        busy={busy}
        streamState={streamState}
        workerState={workerState}
        performAction={performAction}
      />
    </div>
  );
}
