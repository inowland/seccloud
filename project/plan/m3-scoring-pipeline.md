# M3: Training Pipeline & Real-Time Scoring

## Goal

Close the ML loop: train models on customer feature data, export to ONNX, and run
real-time anomaly scoring in the Rust data plane. Deploy to the first design partner
with real data.

## Success Criteria

1. **End-to-end loop**: raw events → features → model training → scoring → detections,
   fully automated for a single tenant.
2. **Training pipeline**: runs on EKS GPU nodes, completes full training in < 8 hours
   for a representative enterprise.
3. **Inference latency**: < 10ms p99 per event in the Rust scoring worker (ONNX Runtime).
4. **Model lifecycle**: versioned model artifacts in S3, automated rollout to scoring
   workers.
5. **First design partner**: at least one enterprise customer running end-to-end on
   real data.

## Architecture

### Training Pipeline (Python on EKS)

```
S3 feature Parquet ──► Training Job (EKS GPU pod)
                           │
                           ├── Load features from S3 (PyArrow)
                           ├── Contrastive learning loop (PyTorch)
                           ├── Evaluation against held-out data
                           ├── Export model to ONNX
                           └── Write model artifact + metadata to S3
```

- Kubetorch-style execution: data scientists write standard PyTorch, job scheduler
  handles pod provisioning and GPU allocation
- Training jobs triggered by: schedule (weekly retrain), manual request, or
  sufficient new data threshold
- Model artifacts stored in S3:
  ```
  s3://{bucket}/{tenant}/v1/models/{model-id}/model.onnx
  s3://{bucket}/{tenant}/v1/models/{model-id}/metadata.json
  s3://{bucket}/{tenant}/v1/models/{model-id}/eval-report.json
  ```

### Scoring Workers (Rust)

Scoring is a transform chain built on the M1 `seccloud-pipeline` core crate:

```
Feature vectors (S3) ──► Rust Scoring Worker
                              │
                              ReadFeatureParquet
                              │
                              Score (OnnxScoring or HeuristicScoring)
                              │
                              ClusterRefinement
                              │
                              WriteDetections (S3)
                              │
                              ProjectHotDetections (DynamoDB/Postgres)
```

- ONNX Runtime (ort crate) for model inference in Rust
- Model hot-reloading: poll S3 for new model versions, swap the `OnnxScoring`
  transform instance behind an `Arc<RwLock<>>` — the pipeline shape doesn't change
- Batch scoring: process feature vectors in batches of 256-1024 for throughput
- Heuristic/ML swap via composition: the `Score` slot in the chain accepts any
  `Transform` implementation. During cold-start (no trained model), the pipeline
  uses `HeuristicScoring` (port of PoC scoring.py). Once a model is available, it
  swaps to `OnnxScoring`. No branching logic in the worker — just a different
  transform plugged into the same chain

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
  vectors, not individual events. This aligns with the S3-backed architecture and
  gives better GPU/CPU utilization.

## Dependencies

- M0: validated model architecture and training pipeline
- M2: feature vectors in S3, feature vector schema

## Deliverables

1. Python training pipeline deployable on EKS GPU nodes.
2. Rust scoring worker with ONNX Runtime integration.
3. Heuristic fallback scorer (port of PoC logic).
4. Model lifecycle management (versioning, promotion, rollback).
5. Detection schema v1 specification.
6. First design partner deployment with real data flowing end-to-end.
