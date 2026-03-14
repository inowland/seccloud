# Source Capability Matrix

## Fixed PoC Source Pack
- Okta
- Google Workspace
- GitHub
- Snowflake

## Capability Status
### Okta (`okta`)
- Raw events: `11`
- Normalized events: `10`
- Dead letters: `1`
- Required event types: `['login']`
- Seen event types: `['login', 'mfa_challenge']`
- Missing required event types: `[]`
- Missing required fields: `[]`
- Dead-letter reasons: `{'unsupported_event_type': 1}`

### Google Workspace (`gworkspace`)
- Raw events: `10`
- Normalized events: `9`
- Dead letters: `1`
- Required event types: `['view', 'share_external']`
- Seen event types: `['share_external', 'view']`
- Missing required event types: `[]`
- Missing required fields: `[]`
- Dead-letter reasons: `{'missing_required_fields:resource_kind': 1}`

### GitHub (`github`)
- Raw events: `21`
- Normalized events: `20`
- Dead letters: `0`
- Required event types: `['view', 'archive_download']`
- Seen event types: `['archive_download', 'clone', 'view']`
- Missing required event types: `[]`
- Missing required fields: `[]`
- Dead-letter reasons: `{}`

### Snowflake (`snowflake`)
- Raw events: `13`
- Normalized events: `13`
- Dead letters: `0`
- Required event types: `['query', 'export']`
- Seen event types: `['export', 'query']`
- Missing required event types: `[]`
- Missing required fields: `[]`
- Dead-letter reasons: `{}`

## Interpretation
- This artifact shows what the current PoC expects from each source and whether the generated runtime satisfied those contracts.
- Dead letters indicate source events that were observed but deliberately excluded from normalized analytics because the current product contract could not safely consume them.
