# Evidence-Backed Scenario Outputs

## Scenario Evaluation
```json
{
  "detected_scenarios": [
    "compromised_privileged_identity",
    "unusual_data_access",
    "unusual_external_sharing",
    "unusual_repo_export"
  ],
  "detection_count_by_event_scenario": {
    "compromised_privileged_identity": 2,
    "unusual_data_access": 1,
    "unusual_external_sharing": 1,
    "unusual_repo_export": 1
  },
  "detection_count_by_scenario": {
    "compromised_privileged_identity": 1,
    "unusual_data_access": 2,
    "unusual_external_sharing": 1,
    "unusual_repo_export": 1
  },
  "expected_scenarios": {
    "benign_finance_close_access": false,
    "benign_incident_response_access": false,
    "benign_role_change_control": false,
    "benign_travel_with_access": false,
    "compromised_privileged_identity": true,
    "source_noise_control": false,
    "unusual_data_access": true,
    "unusual_external_sharing": true,
    "unusual_repo_export": true
  },
  "ingest_diagnostics": {
    "dead_letter_count": 2,
    "dead_letter_reason_counts": {
      "missing_required_fields:resource_kind": 1,
      "unsupported_event_type": 1
    }
  },
  "missed_expected_events": [],
  "raw_event_count_by_scenario": {
    "baseline": 40,
    "benign_finance_close_access": 1,
    "benign_incident_response_access": 1,
    "benign_role_change_control": 2,
    "benign_travel_with_access": 1,
    "compromised_privileged_identity": 2,
    "source_noise_control": 5,
    "unusual_data_access": 1,
    "unusual_external_sharing": 1,
    "unusual_repo_export": 1
  },
  "raw_event_count_by_source": {
    "github": 21,
    "gworkspace": 10,
    "okta": 11,
    "snowflake": 13
  },
  "results": {
    "benign_finance_close_access": {
      "detected": false,
      "expected_detection": false,
      "expected_event_count": 1,
      "matching_detection_count": 0,
      "status": "pass"
    },
    "benign_incident_response_access": {
      "detected": false,
      "expected_detection": false,
      "expected_event_count": 1,
      "matching_detection_count": 0,
      "status": "pass"
    },
    "benign_role_change_control": {
      "detected": false,
      "expected_detection": false,
      "expected_event_count": 2,
      "matching_detection_count": 0,
      "status": "pass"
    },
    "benign_travel_with_access": {
      "detected": false,
      "expected_detection": false,
      "expected_event_count": 1,
      "matching_detection_count": 0,
      "status": "pass"
    },
    "compromised_privileged_identity": {
      "detected": true,
      "expected_detection": true,
      "expected_event_count": 2,
      "matching_detection_count": 2,
      "status": "pass"
    },
    "source_noise_control": {
      "detected": false,
      "expected_detection": false,
      "expected_event_count": 5,
      "matching_detection_count": 0,
      "status": "pass"
    },
    "unusual_data_access": {
      "detected": true,
      "expected_detection": true,
      "expected_event_count": 1,
      "matching_detection_count": 1,
      "status": "pass"
    },
    "unusual_external_sharing": {
      "detected": true,
      "expected_detection": true,
      "expected_event_count": 1,
      "matching_detection_count": 1,
      "status": "pass"
    },
    "unusual_repo_export": {
      "detected": true,
      "expected_detection": true,
      "expected_event_count": 1,
      "matching_detection_count": 1,
      "status": "pass"
    }
  },
  "source_metrics": {
    "github": {
      "dead_letter_count": 0,
      "detection_count": 1,
      "raw_event_count": 21,
      "unexpected_detection_count": 0
    },
    "gworkspace": {
      "dead_letter_count": 1,
      "detection_count": 1,
      "raw_event_count": 10,
      "unexpected_detection_count": 0
    },
    "okta": {
      "dead_letter_count": 1,
      "detection_count": 1,
      "raw_event_count": 11,
      "unexpected_detection_count": 0
    },
    "snowflake": {
      "dead_letter_count": 0,
      "detection_count": 2,
      "raw_event_count": 13,
      "unexpected_detection_count": 0
    }
  },
  "summary": {
    "expected_detected_scenario_count": 4,
    "missed_expected_event_count": 0,
    "passes": true,
    "total_detection_count": 5,
    "unexpected_detection_count": 0
  },
  "unexpected_detections": []
}
```

## Detections
```json
[
  {
    "confidence": 0.99,
    "detection_id": "det-47d5c4dbcf78",
    "event_ids": [
      "evt-22beec3a405e4ee1"
    ],
    "evidence": [
      {
        "object_key": "snowflake/2026/01/09/snowflake-0047.json",
        "observed_at": "2026-01-09T16:00:00Z",
        "raw_event_id": "snowflake-0047",
        "source": "snowflake"
      }
    ],
    "feature_attributions": {
      "high_risk_action": 0.2,
      "large_query": 0.15,
      "new_resource_for_principal": 0.25,
      "peer_group_rarity": 0.15,
      "sensitive_resource": 0.15
    },
    "model_version": "rules-heuristics-fusion-v1",
    "reasons": [
      "principal_has_not_accessed_this_resource_before",
      "resource_is_rare_for_peer_group",
      "resource_is_sensitive",
      "action_has_exfiltration_characteristics",
      "large_sensitive_query"
    ],
    "related_entity_ids": [
      "carol@example.com",
      "snowflake:dataset/customer_pii"
    ],
    "scenario": "unusual_data_access",
    "score": 0.9,
    "severity": "high",
    "status": "open",
    "title": "Unusual sensitive dataset access"
  },
  {
    "confidence": 0.9,
    "detection_id": "det-c79f607ba412",
    "event_ids": [
      "evt-d630d9ddb377fed6"
    ],
    "evidence": [
      {
        "object_key": "okta/2026/01/09/okta-0041.json",
        "observed_at": "2026-01-09T14:00:00Z",
        "raw_event_id": "okta-0041",
        "source": "okta"
      }
    ],
    "feature_attributions": {
      "new_geo": 0.2,
      "privileged_activity": 0.2,
      "privileged_geo_combo": 0.15,
      "sensitive_resource": 0.15
    },
    "model_version": "rules-heuristics-fusion-v1",
    "reasons": [
      "resource_is_sensitive",
      "privileged_identity_activity",
      "login_from_unfamiliar_geography",
      "privileged_login_combines_unfamiliar_geo_and_sensitive_access"
    ],
    "related_entity_ids": [
      "alice@example.com",
      "okta:admin-console"
    ],
    "scenario": "compromised_privileged_identity",
    "score": 0.7,
    "severity": "medium",
    "status": "open",
    "title": "Privileged identity used from an unfamiliar geography"
  },
  {
    "confidence": 0.99,
    "detection_id": "det-d700092717aa",
    "event_ids": [
      "evt-35e4079f1092d5c0"
    ],
    "evidence": [
      {
        "object_key": "gworkspace/2026/01/09/gworkspace-0048.json",
        "observed_at": "2026-01-09T17:00:00Z",
        "raw_event_id": "gworkspace-0048",
        "source": "gworkspace"
      }
    ],
    "feature_attributions": {
      "external_share": 0.2,
      "high_risk_action": 0.2,
      "new_action_for_principal": 0.25,
      "new_resource_for_principal": 0.25,
      "peer_group_rarity": 0.15,
      "sensitive_resource": 0.15
    },
    "model_version": "rules-heuristics-fusion-v1",
    "reasons": [
      "principal_has_not_performed_this_action_before",
      "principal_has_not_accessed_this_resource_before",
      "resource_is_rare_for_peer_group",
      "resource_is_sensitive",
      "action_has_exfiltration_characteristics",
      "resource_shared_externally"
    ],
    "related_entity_ids": [
      "dave@example.com",
      "gworkspace:doc/board-notes"
    ],
    "scenario": "unusual_external_sharing",
    "score": 0.99,
    "severity": "high",
    "status": "open",
    "title": "Unusual external sharing of a sensitive document"
  },
  {
    "confidence": 0.99,
    "detection_id": "det-d9947ba2ccff",
    "event_ids": [
      "evt-033aa44a588e14a5"
    ],
    "evidence": [
      {
        "object_key": "snowflake/2026/01/09/snowflake-0043.json",
        "observed_at": "2026-01-09T14:20:00Z",
        "raw_event_id": "snowflake-0043",
        "source": "snowflake"
      }
    ],
    "feature_attributions": {
      "large_query": 0.15,
      "new_action_for_principal": 0.25,
      "new_resource_for_principal": 0.25,
      "peer_group_rarity": 0.15,
      "sensitive_resource": 0.15
    },
    "model_version": "rules-heuristics-fusion-v1",
    "reasons": [
      "principal_has_not_performed_this_action_before",
      "principal_has_not_accessed_this_resource_before",
      "resource_is_rare_for_peer_group",
      "resource_is_sensitive",
      "large_sensitive_query"
    ],
    "related_entity_ids": [
      "alice@example.com",
      "snowflake:dataset/payroll"
    ],
    "scenario": "unusual_data_access",
    "score": 0.95,
    "severity": "high",
    "status": "open",
    "title": "Unusual sensitive dataset access"
  },
  {
    "confidence": 0.93,
    "detection_id": "det-f0d58be48a56",
    "event_ids": [
      "evt-65a4dffa255effde"
    ],
    "evidence": [
      {
        "object_key": "github/2026/01/09/github-0045.json",
        "observed_at": "2026-01-09T15:00:00Z",
        "raw_event_id": "github-0045",
        "source": "github"
      }
    ],
    "feature_attributions": {
      "high_risk_action": 0.2,
      "new_resource_for_principal": 0.25,
      "peer_group_rarity": 0.15,
      "sensitive_resource": 0.15
    },
    "model_version": "rules-heuristics-fusion-v1",
    "reasons": [
      "principal_has_not_accessed_this_resource_before",
      "resource_is_rare_for_peer_group",
      "resource_is_sensitive",
      "action_has_exfiltration_characteristics"
    ],
    "related_entity_ids": [
      "bob@example.com",
      "github:repo/security-agent"
    ],
    "scenario": "unusual_repo_export",
    "score": 0.75,
    "severity": "medium",
    "status": "open",
    "title": "Unusual repository clone or export"
  }
]
```
