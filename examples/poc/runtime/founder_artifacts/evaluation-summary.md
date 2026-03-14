# Evaluation Summary

## Headline
- Overall evaluation status: `pass`
- Total detections: `5`
- Unexpected detections: `0`
- Missed expected events: `0`

## Scenario Coverage
- `benign_finance_close_access`: status=`pass`, expected_events=`1`, matching_detections=`0`
- `benign_incident_response_access`: status=`pass`, expected_events=`1`, matching_detections=`0`
- `benign_role_change_control`: status=`pass`, expected_events=`2`, matching_detections=`0`
- `benign_travel_with_access`: status=`pass`, expected_events=`1`, matching_detections=`0`
- `compromised_privileged_identity`: status=`pass`, expected_events=`2`, matching_detections=`2`
- `source_noise_control`: status=`pass`, expected_events=`5`, matching_detections=`0`
- `unusual_data_access`: status=`pass`, expected_events=`1`, matching_detections=`1`
- `unusual_external_sharing`: status=`pass`, expected_events=`1`, matching_detections=`1`
- `unusual_repo_export`: status=`pass`, expected_events=`1`, matching_detections=`1`

## Source Metrics
- `okta`: raw_events=`11`, detections=`1`, unexpected_detections=`0`, dead_letters=`1`
- `github`: raw_events=`21`, detections=`1`, unexpected_detections=`0`, dead_letters=`0`
- `snowflake`: raw_events=`13`, detections=`2`, unexpected_detections=`0`, dead_letters=`0`
- `gworkspace`: raw_events=`10`, detections=`1`, unexpected_detections=`0`, dead_letters=`1`

## Ingest Diagnostics
- Dead letters: `2`
- Dead-letter reasons: `{'missing_required_fields:resource_kind': 1, 'unsupported_event_type': 1}`

## Current Decision Signals
- The PoC currently shows zero unexpected detections on baseline, benign-control, and initial benign-drift events.
- The fixed source pack produces evidence-backed detections across identity, code, collaboration, and data sources.
- The ingest path now distinguishes valid telemetry from malformed or unsupported events instead of pretending every source delivery is trustworthy.
- The next product question is whether the same contracts survive a much broader benign-drift set and more realistic source variability.

## Active Detections
- Unusual sensitive dataset access (`unusual_data_access`, score=0.9, severity=high)
- Privileged identity used from an unfamiliar geography (`compromised_privileged_identity`, score=0.7, severity=medium)
- Unusual external sharing of a sensitive document (`unusual_external_sharing`, score=0.99, severity=high)
- Unusual sensitive dataset access (`unusual_data_access`, score=0.95, severity=high)
- Unusual repository clone or export (`unusual_repo_export`, score=0.75, severity=medium)
