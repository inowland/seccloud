# WS04 Push Ingestion Runtime Notes

This note documents the implementation assumptions for the current push-ingestion path. It does not change the frozen contracts in `project/spec/`.

## Authentication

- The gateway requires `Authorization: Bearer <token>`.
- Tokens are configured through `SECCLOUD_PUSH_AUTH_TOKENS` as a JSON object keyed by token value.
- Each token must bind at least:
  - `tenant_id`
  - `source`
- Tokens may also bind `integration_id`. When present, the request must either omit `integration_id` or match the configured value.

Example:

```json
{
  "push-token": {
    "tenant_id": "default-tenant",
    "source": "okta",
    "integration_id": "okta-primary"
  }
}
```

## Request Shape

- The endpoint remains `/api/intake/raw-events`.
- The request body is JSON and may be sent with `Content-Encoding: gzip`.
- The body must contain:
  - `source`
  - `records`
- Each record must include at least:
  - `source_event_id`
  - `observed_at`
- A nested record-level `source`, when present, must match the batch `source`.

## Idempotency

- Clients may send `Idempotency-Key`.
- If omitted, the gateway derives a stable request-level key from the tenant-bound request payload.
- Replaying the same logical request returns the original batch metadata with `duplicate=true`.
- Reusing an `Idempotency-Key` with different payload bytes is rejected.

## Landing Behavior

- Accepted requests land one immutable `jsonl.gz` raw batch under `lake/raw/...`.
- Each landed batch also writes one immutable raw manifest under `lake/manifests/...`.
- The API enqueues only manifest-backed intake work and does not run normalization inline.
