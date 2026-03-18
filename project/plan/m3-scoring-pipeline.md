# M3: Model-Backed Detection Runtime

## Goal

Close the ML loop for the local/shared-workspace runtime: train models on durable
feature data, export them to ONNX, and run anomaly scoring in the Rust data plane.

The baseline for M3 is now different from the original draft:

- local normalization, feature materialization, detection-context refresh,
  detections, and projection are already Rust-first
- durable feature tables, event index, and detection context already exist in the
  local/shared-workspace runtime
- Postgres plus local manifests are the current serving/coordination substrate,
  with cloud deployment concerns deferred to later milestones

So M3 is not "introduce a Rust worker pipeline." It is "replace the remaining
heuristic scoring path with model-backed scoring while preserving the durable
runtime substrate that already exists," with production-account deployment left to
later milestones.

## Success Criteria

1. **End-to-end local loop**: raw events → features → model training → scoring →
   detections, fully automated for a single tenant workspace.
2. **Durable training/export path**: train and evaluate from the feature lake,
   export ONNX bundles plus metadata/eval artifacts, and install them into the
   runtime without ad hoc glue.
3. **Native Rust scoring**: the detector runs ONNX Runtime directly in Rust, with
   heuristic scoring retained only as an explicit cold-start fallback.
4. **Model lifecycle**: installed model artifacts support activate/deactivate/
   rollback, gating, score policy export, and runtime status reporting.
5. **Operator trust surfaces**: the runtime exposes enough status, comparison, and
   rationale data to make model-backed alerts explainable in the demo.

## Architecture

### Training Pipeline (Python first)

```
Feature lake Parquet ──► Training Job (local workspace first)
                           │
                           ├── Load features from lake/object storage (PyArrow)
                           ├── Contrastive learning loop (PyTorch)
                           ├── Evaluation against held-out data
                           ├── Export model to ONNX
                           └── Write model artifact + metadata to object storage
```

- Local/dev mode should be able to train and evaluate from the same durable feature
  lake the demo already uses; cloud GPU execution is a later scale-up path, not an
  M3 exit criterion
- Training jobs triggered by: schedule (weekly retrain), manual request, or
  sufficient new data threshold
- Model artifacts stored in the workspace/object-store layout:
  ```
  {object-store}/{tenant}/v1/models/{model-id}/model.onnx
  {object-store}/{tenant}/v1/models/{model-id}/metadata.json
  {object-store}/{tenant}/v1/models/{model-id}/eval-report.json
  ```

### Scoring Workers (Rust)

Scoring is a transform chain built on the M1 `seccloud-pipeline` core crate:

```
Feature vectors / detection context ──► Rust Scoring Worker
                              │
                              ReadFeatureParquet + detection context
                              │
                              Score (OnnxScoring or HeuristicScoring)
                              │
                              ClusterRefinement
                              │
                              WriteDetections (lake/object storage)
                              │
                              ProjectHotDetections (Postgres)
```

- ONNX Runtime (ort crate) for model inference in Rust
- Model hot-loading from the active artifact manifest; cloud rollout and hot-reload
  mechanics are successor work, not blockers for M3 completion
- Batch scoring: process feature vectors in batches for throughput; exact sizing
  should be driven by the existing feature lake layout and projection latency budget
- Heuristic/ML swap via composition: the `Score` slot in the chain accepts any
  `Transform` implementation. During cold-start (no trained model), the pipeline
  uses `HeuristicScoring` (port of PoC scoring.py). Once a model is available, it
  swaps to `OnnxScoring`. No branching logic in the worker — just a different
  transform plugged into the same chain

### Current M3 Starting Point

Before M3 starts, the repo already has:

- Rust normalization, feature materialization, detection-context materialization,
  detection worker, projector worker, and worker-service orchestration
- durable feature tables consumed by Python scoring when complete
- a durable event index and detection-context manifest used by investigation and
  runtime-status surfaces

The first executable M3 slice should therefore be:

1. freeze `model artifact contract v1`
2. train from the existing durable feature lake
3. load model artifacts in the Rust worker runtime
4. swap detection scoring from heuristic to ONNX-backed scoring behind the same
   worker-service path
5. keep the current heuristic scorer only as an explicit cold-start fallback until
   model lifecycle and rollout are proven

The `model artifact contract v1` is now defined in
[model-artifact-schema-v1.md](/Users/inowland/Development/seccloud/project/plan/model-artifact-schema-v1.md).
The current implementation already supports:

- ONNX bundle export with `metadata.json` and `eval-report.json`
- exported principal/resource/categorical vocab artifacts required for inference
- active-model selection via `manifests/model_artifact.json`
- Rust runtime loading of the active artifact
- explicit runtime reporting of `requested_mode` vs `effective_mode`
- local workspace training/export via `seccloud train-model-artifact`
- native ONNX-backed event scoring inside the Rust detector
- detection lake output for model-backed detections
- active-model lifecycle controls including activate/deactivate/rollback
- held-out activation gates plus durable score-policy export
- source-aware activation gates that can block promotion when a covered source
  regresses even if the aggregate gate passes
- workspace-configurable coverage gating for auto-promotion: the default local
  policy requires at least two covered sources including one identity source and
  one resource/data source, but operators can override that per workspace when
  rollout needs to be stricter or more permissive

What remains inside M3 is no longer the mechanics of ONNX execution. The remaining
work is:

1. tighten activation policy quality further, especially around what the default
   workspace promotion policy should require before a promotion is considered
   trustworthy
2. keep using `seccloud compare-detection-modes` to compare model-backed
   detections against heuristics on shared scenarios so investigator usefulness
   is measured, not just runtime correctness
3. keep improving operator/investigator visibility into why a promotion was
   allowed or blocked and what the model score means in practice

### Non-Goals For M3

- design-partner deployment
- customer-account BYOC provisioning
- EKS GPU training as a milestone gate
- multi-cloud deployment

### Model Lifecycle

1. **Base model**: trained on synthetic + public data during M0, used for cold-start
2. **Fine-tuned model**: trained on customer-specific feature data after onboarding
3. **Retrained model**: periodic retraining to adapt to organizational drift
4. **A/B testing**: run new model alongside current model, compare detection quality
   before promotion

### Detection Output

```
s3://{bucket}/{tenant}/v1/detections/{YYYY}/{MM}/{DD}/{HH}/{partition-id}.parquet
```

Detection schema:

- `detection_id`: unique identifier
- `timestamp`: when the anomalous action occurred
- `principal_key`: who performed the action
- `action_ids`: references to normalized events (evidence)
- `score`: anomaly score (0-1)
- `confidence`: model confidence
- `severity`: derived severity (info/low/medium/high/critical)
- `feature_attributions`: which features contributed most to the score
- `scenario_type`: model's characterization (exfiltration, privilege_escalation, etc.)
- `model_version`: which model produced this detection

## Key Decisions

- **ONNX as the model interchange format**: decouples training (Python) from inference
  (Rust). Well-supported in both ecosystems.
- **Heuristic fallback via transform composition**: during cold-start, the scoring
  pipeline plugs in `HeuristicScoring` where `OnnxScoring` would go. Both implement
  the same `Transform` trait from the M1 core crate. No if/else in the worker — the
  pipeline is assembled at startup based on model availability.
- **Batch scoring, not streaming**: scoring operates on micro-batches of feature
  vectors, not individual events. This aligns with the current lake/object-store
  architecture and gives better GPU/CPU utilization.

## Dependencies

- M0: validated model architecture and training pipeline
- M2: durable feature tables, feature schema, event index, and detection context

## Deliverables

1. Local workspace training pipeline over the durable feature lake.
2. Rust scoring worker with native ONNX Runtime integration.
3. Model artifact contract plus runtime loading path in Rust.
4. Model lifecycle management (versioning, promotion, rollback, gating).
5. Detection schema v1 specification and model rationale payloads.
6. Runtime/operator surfaces that make model-backed scoring debuggable.
