# Model Artifact Schema V1

`model artifact schema v1` defines the durable handoff between Python training
and the Rust scoring runtime.

## Workspace Layout

Active model selection lives in:

`manifests/model_artifact.json`

```json
{
  "manifest_version": 1,
  "active_model_id": "contrastive-demo-v1",
  "active_metadata_path": "models/contrastive-demo-v1/metadata.json",
  "activated_at": "2026-03-17T00:00:00Z",
  "activation_source": "local_install"
}
```

The model bundle itself lives under:

`models/{model-id}/`

with at least:

- `metadata.json`
- `eval-report.json`
- one or more `action_tower_{source}.onnx`
- `context_tower.onnx`
- `principal-vocab.json`
- `resource-vocab.json`
- `categorical-vocabs.json`

## Metadata Contract

`models/{model-id}/metadata.json`

```json
{
  "artifact_version": 1,
  "tenant_id": "tenant-1",
  "model_id": "contrastive-demo-v1",
  "model_version": "contrastive-demo-v1",
  "model_family": "contrastive-facade",
  "scoring_mode": "onnx",
  "exported_at": "2026-03-17T00:00:00Z",
  "feature_schema_version": "feature.v1",
  "required_feature_tables": ["action", "history", "static", "peer_group"],
  "padding": {
    "max_tokens": 64,
    "max_windows": 64,
    "max_res_per_window": 16,
    "max_peers": 64
  },
  "action_towers": {
    "okta": {
      "path": "action_tower_okta.onnx",
      "sha256": "sha256:...",
      "input_names": ["indices", "weights", "mask"],
      "output_name": "embedding"
    }
  },
  "context_tower": {
    "path": "context_tower.onnx",
    "sha256": "sha256:...",
    "input_names": [
      "hist_window_indices",
      "hist_window_mask",
      "hist_num_windows",
      "peer_indices",
      "peer_weights",
      "peer_mask",
      "role",
      "location",
      "duration",
      "privilege"
    ],
    "output_name": "embedding"
  },
  "model_config": {
    "...": "exported PyTorch model config"
  },
  "eval_report_path": "eval-report.json",
  "score_policy": {
    "detection_threshold": 0.41,
    "high_severity_threshold": 0.57,
    "calibration_source": "heldout",
    "calibration_reason": "heldout_midpoint_between_positive_p95_and_negative_p50",
    "positive_distance_p95": 0.33,
    "negative_distance_p50": 0.49,
    "negative_distance_p90": 0.61,
    "source_policies": {
      "github": {
        "detection_threshold": 0.38,
        "high_severity_threshold": 0.53,
        "calibration_source": "heldout:github",
        "calibration_reason": "heldout_midpoint_between_positive_p95_and_negative_p50",
        "positive_distance_p95": 0.31,
        "negative_distance_p50": 0.45,
        "negative_distance_p90": 0.58,
        "evaluation_pair_count": 24
      }
    }
  },
  "input_vocabs": {
    "principal_entity_keys_path": "principal-vocab.json",
    "resource_entity_keys_path": "resource-vocab.json",
    "categorical_vocabs_path": "categorical-vocabs.json",
    "principal_vocab_count": 120,
    "resource_vocab_count": 480,
    "categorical_vocab_counts": {
      "role": 6,
      "location": 5,
      "duration_bucket": 5,
      "privilege_level": 3
    }
  }
}
```

## Runtime Semantics

- If `manifests/model_artifact.json` is absent, the scoring runtime is in
  `heuristic` mode.
- If an active metadata path is present and all referenced ONNX files exist, the
  runtime reports `requested_mode=onnx`.
- A model bundle is not considered runtime-ready unless the referenced vocab
  artifacts also exist, because Rust inference needs the exact same entity and
  categorical index mappings the model was trained against.
- The current local implementation uses an `onnx_native` execution mode: Rust
  owns feature loading, tensorization, ONNX Runtime execution, and detection
  writing.
- A narrow Python ONNX scoring bridge still exists only in test builds, so unit
  coverage can exercise model-backed detector behavior without checking real
  ONNX graphs into the repo.
- The durable artifact contract is anchored on the native Rust ONNX runtime.
- `score_policy` defines how raw model distance maps into detection decisions.
  The current local implementation calibrates this from held-out sampled retrieval
  distances during export, and the Rust detector uses it instead of a hard-coded
  anomaly threshold.
- `score_policy.source_policies` is optional and allows the detector to apply a
  different threshold for a specific source when held-out data supports it.
- `eval-report.json` may also carry `activation_gate.source_gates`, which allows
  model promotion to be blocked when a source with sufficient held-out coverage
  fails its own retrieval-quality gate even if the aggregate gate passes.
- `eval-report.json` may also carry `activation_gate.coverage_gate`, which blocks
  auto-promotion when the held-out evaluation is too narrow.
- The current local runtime evaluates that coverage gate against a workspace
  promotion policy stored in `manifests/model_promotion_policy.json`. The
  default policy requires at least two covered sources, including one identity
  source (`okta`) and one resource/data source (`github`, `gworkspace`, or
  `snowflake`), but operators can relax or tighten that policy per workspace.
- The detection payload should preserve the applied threshold, score margin, and
  calibration scope so investigator views can explain why a model-backed alert
  fired.

That split is intentional: the model bundle and scoring contract stay stable
whether inference runs through the native Rust path or the optional debug bridge.
