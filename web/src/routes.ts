const pages = ["detections", "events", "integrations", "pipeline"] as const;

export type Page = (typeof pages)[number];
export type Route =
  | { kind: "page"; page: Page }
  | { kind: "detection"; detectionId: string }
  | { kind: "entity"; principalKey: string }
  | { kind: "event"; eventId: string }
  | { kind: "integration"; source: string };

function isPage(value: string): value is Page {
  return pages.includes(value as Page);
}

export function parseRouteFromHash(hash: string): Route {
  const value = hash.replace("#", "");
  if (isPage(value)) {
    return { kind: "page", page: value };
  }

  const detectionMatch = value.match(/^detection\/(.+)$/);
  if (detectionMatch) {
    return {
      kind: "detection",
      detectionId: decodeURIComponent(detectionMatch[1]),
    };
  }

  const entityMatch = value.match(/^entity\/(.+)$/);
  if (entityMatch) {
    return {
      kind: "entity",
      principalKey: decodeURIComponent(entityMatch[1]),
    };
  }

  const eventMatch = value.match(/^event\/(.+)$/);
  if (eventMatch) {
    return { kind: "event", eventId: decodeURIComponent(eventMatch[1]) };
  }

  const integrationMatch = value.match(/^integration\/(.+)$/);
  if (integrationMatch) {
    return {
      kind: "integration",
      source: decodeURIComponent(integrationMatch[1]),
    };
  }

  return { kind: "page", page: "detections" };
}

export function routeToHash(route: Route): string {
  if (route.kind === "page") {
    return route.page;
  }
  if (route.kind === "detection") {
    return `detection/${encodeURIComponent(route.detectionId)}`;
  }
  if (route.kind === "entity") {
    return `entity/${encodeURIComponent(route.principalKey)}`;
  }
  if (route.kind === "integration") {
    return `integration/${encodeURIComponent(route.source)}`;
  }
  return `event/${encodeURIComponent(route.eventId)}`;
}

export function routeToHref(route: Route): string {
  return `#${routeToHash(route)}`;
}

export function pageForRoute(route: Route): Page {
  if (route.kind === "page") {
    return route.page;
  }
  if (route.kind === "detection") {
    return "detections";
  }
  if (route.kind === "entity") {
    return "detections";
  }
  if (route.kind === "integration") {
    return "integrations";
  }
  return "events";
}
