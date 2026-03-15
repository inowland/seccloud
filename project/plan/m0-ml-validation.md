# M0: ML Validation

## Goal

Prove that a Facade-style contrastive learning approach produces reliable insider-threat
detection on realistic enterprise data before investing in production infrastructure.

This is the gate milestone. If we cannot achieve acceptable detection quality with a
tractable training and fine-tuning workflow, we revisit the ML approach before
proceeding to M1+.

## Success Criteria

1. **False positive rate < 0.1%** at action level on held-out benign data across at
   least three simulated enterprise profiles.
2. **Detection rate > 80%** on planted insider-threat scenarios (data exfiltration,
   privilege escalation, anomalous access patterns, compromised credentials).
3. **Cold-start viability**: acceptable detection quality (FPR < 0.5%) within 2 weeks
   of simulated history, converging to full quality by 4 weeks.
4. **Model stability**: no significant quality degradation over 6 months of simulated
   drift without retraining.
5. **Training tractability**: full model training completes in < 8 hours on a single
   GPU node for a representative enterprise (50K principals, 100M events/month).

## Architecture: Adapting Facade

Facade (Google, arxiv 2412.06700) is the reference architecture. Their system detects
insider threats using unsupervised contrastive learning on benign data at Google scale
(10^5 principals, 10^10 actions/year). We adapt their approach to our setting.

### What Facade Does

**Two-tower scoring model:**
- **Context tower** (E_C): encodes who the principal is — role, peer group, history
  pattern, social network position. Produces a d-dimensional L2-normalized embedding.
- **Action towers** (E_A): one tower per resource type. Encodes what was accessed —
  the resource identity, represented as the weighted set of all prior accessors
  (frequency-weighted, not recency-weighted). Produces a d-dimensional L2-normalized
  embedding in the same space.

**Scoring function:**
```
f(a, c) = d_cos(E_A(a), E_C(c))
```
An action is anomalous when its embedding is far from the context embedding in cosine
distance — i.e., "this resource is not the kind of thing someone in this role/position
typically accesses."

**Contrastive learning (benign data only):**
- Natural pairs: (action_i, context_i) are real observations — labeled as "normal"
  (negative in contrastive terms).
- Synthetic positives: (action_i, context_j) where principal_i != principal_j — a
  random person accessing this resource would be suspicious. n_p = 10 synthetic
  positives per natural pair during training.
- The model learns to distinguish "this person accessing this resource" from "a random
  person accessing this resource."

**Key theorem:** This objective learns the pointwise mutual information
P(a,c) / (P(a) * P(c)), meaning the model scores actions as anomalous only when
they're unexpected *given context*, not just rare in general.

**Action representation:** Each resource is represented by the weighted set of all
principals who have previously accessed it (weights proportional to access frequency,
sum to 1). This means the action embedding encodes "what kind of people access this
resource" without needing resource metadata.

**Context representation:**
- History: stored in non-overlapping 2-hour windows. Each window captures one action
  per distinct resource accessed.
- Social network: meeting peers (weighted by shared meetings, inversely by meeting
  size), code-review peers (weighted by frequency/recency), manager hierarchy peers,
  cost center peers.
- Static features: job family type, discretized employment duration.
- Context features are refreshed daily (they evolve slower than actions).

**Multi-scale detection via clustering:**
```
g(A, c) = sum_{X in G} max_{a in X} f(a, c)
```
Hierarchical agglomerative clustering on action embeddings (cosine similarity).
Redundancy parameter delta groups similar actions. This prevents inflated scores from
repeated access to the same resource while boosting scores when a principal accesses
multiple *different* unusual resources.

**Loss function (pairwise ranking):**
```
L(B) = (1/N * sum_i (1/P * sum_j l(h + (y_j+ - y_i-) / s))^omega)^(1/omega)
```
Where omega emphasizes hard negatives, s is a soft margin, h is a hard margin. The
pointwise loss l is Huber-like (quadratic near zero, linear far from zero).

**Scale:** Trained on ~3 TB of compressed data, ~10^10 simplified actions, ~10^8
distinct resources. Training takes "a few hours" on a single H100. The entire system
is ~10,000 lines of code.

**Results:** FPR < 0.01% overall, < 0.0003% per action. Stable for 1 year without
retraining. Ranks attackers in top 0.01% of 10^5+ principals.

### How We Adapt It

**What stays the same:**
- Two-tower architecture (context + action)
- Contrastive learning on benign data only
- Cosine distance scoring
- Action representation as weighted accessor sets
- Clustering-based multi-scale detection

**What changes:**

| Facade (Google) | Our Adaptation | Rationale |
|---|---|---|
| 3 resource types (docs, SQL, HTTP) | 4 source types (Okta, Google Workspace, GitHub, Snowflake) | Different source mix; add sources incrementally |
| Meeting data for social graph | Inferred collaboration from shared resource access | We won't have calendar data initially |
| Code review peers | GitHub co-contributor graph | Same signal, different source |
| Manager/cost-center from HRIS | Okta group membership + org attributes | Available in identity provider data |
| 10^5+ principals | 10K-500K principals | Fortune 500 range; need to validate at lower end |
| 10^10 actions/year | 10^7-10^9 actions/year | Lower volume per customer; feature quality matters more |
| TensorFlow + Vizier | PyTorch + simple hyperparameter search | Team preference; less infra dependency |
| Google internal infrastructure | S3 + local Parquet files | Works locally and in BYOC |

**Our source-to-tower mapping:**

| Source | Action Tower | Resource Representation |
|---|---|---|
| Okta | Identity events tower | App/resource ID as weighted accessor set |
| Google Workspace | Document access tower | Document ID as weighted accessor set |
| GitHub | Code repository tower | Repo ID as weighted accessor set |
| Snowflake | Data access tower | Table/schema ID as weighted accessor set |

**Context features we can compute from our sources:**

| Feature | Source | Facade Equivalent |
|---|---|---|
| Peer group (department) | Okta user attributes | Cost center peers |
| Manager hierarchy | Okta org structure | Manager/grand-manager peers |
| Collaboration graph | Shared resource access patterns | Meeting peers (proxy) |
| Code review peers | GitHub PR co-authors/reviewers | Code review peers |
| Job function | Okta role/title attributes | Job family type |
| Employment duration | Okta account creation date | Employment duration |
| Geographic pattern | Okta login geo, IP addresses | (not explicitly in Facade) |

---

## Workstreams

### WS1: Synthetic Data Generator at Scale

**Goal:** Generate realistic multi-source enterprise activity at a scale sufficient
to train and evaluate contrastive learning models.

The current PoC synthetic generator produces ~55 events across 5 principals. We need
10^6-10^8 events across 10^3-10^5 principals with realistic distributions.

**What to build:**

1. **Organization generator**: create a synthetic enterprise with configurable size.
   - Org chart: departments, teams, reporting chains (tree structure with fan-out 5-8)
   - Roles: engineer, analyst, PM, manager, admin, executive (Zipf distribution)
   - Locations: 3-10 offices with realistic geographic distribution
   - Groups: Okta groups reflecting team structure + cross-functional groups

2. **Resource universe generator**: create the resources principals will access.
   - Documents: 10x-100x the number of principals, with access patterns reflecting
     team structure (80% within-team, 15% within-department, 5% cross-department)
   - Repositories: 1x-5x teams, with contributor patterns reflecting team ownership
   - Database tables: 10x-50x teams, with access reflecting role-based permissions
   - Applications: 10-50 enterprise apps, with access reflecting role

3. **Activity generator**: produce event streams with realistic distributions.
   - Per-principal daily activity: log-normal distribution (median 20 events, long tail)
   - Time-of-day: business hours bell curve + per-principal timezone offset
   - Day-of-week: weekday-heavy with some weekend activity for on-call/global roles
   - Access patterns: preferential attachment (principals return to familiar resources)
   - Resource sharing: team-level clustering with cross-team edges

4. **Scenario injector**: plant known-bad patterns at controlled rates.
   - Slow exfiltration: gradual increase in data access volume over weeks
   - Credential compromise: sudden geo shift + access to unfamiliar resources
   - Privilege escalation: access to admin resources after role doesn't change
   - Departing employee: broad access sweep before leaving
   - Account takeover: access pattern shift (different hours, different resources)
   - Insider collaboration: two accounts accessing unusual resources in coordination

5. **Drift simulator**: evolve the organization over time.
   - Role changes: 2-5% of principals change roles per quarter
   - New hires / departures: 1-3% per month
   - New resources: continuous creation of new documents, repos, tables
   - Seasonal patterns: quarter-end activity spikes, holiday dips

**Output format:** Parquet files matching the PoC normalized event schema, with ground
truth labels in a separate sidecar file (not visible to the model during training).

**Validation:** Compare synthetic distributions against published enterprise activity
research (CERT dataset statistics, MITRE ATT&CK frequency data).

### WS2: Feature Engineering Pipeline

**Goal:** Transform normalized events into the representations Facade's model expects:
weighted accessor sets for actions, context feature vectors for principals.

**Action featurization (per Facade):**

For each event, the action is the resource being accessed. The resource is represented
as the weighted set of all principals who have previously accessed it:

```python
# For resource r:
accessor_weights[r] = {
    principal_key: access_count / total_accesses
    for principal_key, access_count in access_history[r].items()
}
# This is a sparse vector over the principal vocabulary
```

This means the action embedding encodes "what kind of person typically accesses this
resource." When a security admin accesses a database table normally accessed only by
data analysts, the action embedding (analyst-heavy) will be far from the context
embedding (security-admin-like).

**Implementation:** Use a token-based representation where each principal is a token
with a frequency weight. The action tower maps these weighted token sets to fixed-size
embeddings.

**Context featurization:**

Per principal, compute and cache (refresh daily or on change):

1. **History profile**: For each non-overlapping 2-hour window in the trailing N days,
   record the set of distinct resources accessed (one entry per resource). This
   captures the principal's behavioral baseline.

2. **Peer group features** (from Okta data):
   - Department peers: all principals in same department
   - Manager peers: principals sharing same manager or grand-manager (half weight for
     grand-manager)
   - Group peers: Okta group co-membership (weighted by group size inverse)

3. **Collaboration features** (inferred):
   - Shared resource access: principals who frequently access the same resources
     (weighted by co-access frequency, inversely by resource popularity)
   - GitHub co-contribution: principals who contribute to the same repositories

4. **Static features**:
   - Role/job function (categorical, from Okta attributes)
   - Employment duration (discretized: <3mo, 3-12mo, 1-3yr, 3-5yr, 5yr+)
   - Location (categorical, from Okta or login geo)
   - Privilege level (regular, elevated, admin — from Okta group membership)

**Storage:** Feature vectors stored as Parquet files in S3 (or local filesystem for
M0). Principal profiles stored as a rolling state table (DynamoDB in production,
SQLite or in-memory dict for M0).

**Key design choice:** Following Facade, we intentionally exclude timestamps and
access modality from the action representation. This prevents the model from learning
temporal shortcuts (e.g., "accessing documents at 3am is always suspicious") and forces
it to learn the semantic relationship between principal context and resource access
patterns. Temporal features enter only through the context tower (history profile).

### WS3: Model Implementation

**Goal:** Implement the two-tower contrastive model in PyTorch, train on synthetic
data, and evaluate detection quality.

**Architecture:**

```
Action Tower (per source type):
  Input: weighted token set (sparse, over principal vocabulary)
    → Token embedding layer (vocab_size × d_token)
    → Weighted sum pooling (weighted by access frequencies)
    → Linear → ReLU → Linear → ReLU → Linear
    → L2 normalization
    → Output: d-dimensional unit vector

Context Tower:
  Input: concatenation of context features
    → History encoder:
        Input: sequence of 2-hour window descriptors (each a weighted token set)
        → Per-window: token embedding → weighted sum pooling
        → Temporal aggregation (mean or attention over windows)
    → Peer encoder:
        Input: peer group weighted sets (department, manager, collaboration)
        → Per-group: token embedding → weighted sum pooling
    → Static features:
        Input: categorical features (role, location, duration, privilege)
        → Embedding per categorical → concatenate
    → Concatenate [history_repr, peer_repr, static_repr]
    → Linear → ReLU → Linear → ReLU → Linear
    → L2 normalization
    → Output: d-dimensional unit vector
```

**Hyperparameters (starting points, to be tuned):**
- Embedding dimension d: 64 or 128
- Token embedding dimension d_token: 32 or 64
- Tower hidden dimensions: [256, 128, d] or [512, 256, d]
- History window: 2 hours, trailing 30 days (360 windows)
- Synthetic positive ratio n_p: 10 (training), 1 (validation)
- Margin parameters h, s: tune via validation AUC
- Hard negative emphasis omega: tune via validation AUC
- Batch size: 1024-4096
- Learning rate: 1e-4 to 1e-3 (Adam)
- Training epochs: until validation loss plateaus (expect 10-50)

**Contrastive learning implementation:**

```python
class FacadeModel(nn.Module):
    def __init__(self, config):
        self.context_tower = ContextTower(config)
        self.action_towers = nn.ModuleDict({
            source: ActionTower(config) for source in config.sources
        })

    def forward(self, action, context, source_type):
        e_a = self.action_towers[source_type](action)
        e_c = self.context_tower(context)
        return cosine_distance(e_a, e_c)

# Training: for each batch of real (action, context) pairs:
#   1. Compute f(a_i, c_i) for natural pairs (these should be low distance)
#   2. For each natural pair, sample n_p random contexts c_j where principal_j != principal_i
#   3. Compute f(a_i, c_j) for synthetic pairs (these should be high distance)
#   4. Compute pairwise ranking loss
```

**Loss implementation:**

```python
def pairwise_ranking_loss(natural_scores, synthetic_scores, h, s, omega):
    """
    natural_scores: [batch_size] — distances for real (action, context) pairs
    synthetic_scores: [batch_size, n_p] — distances for synthetic pairs
    """
    # For each natural pair, compare against its synthetic positives
    # l(h + (y_j+ - y_i-) / s) where y_j+ are synthetic, y_i- are natural
    margins = h + (synthetic_scores - natural_scores.unsqueeze(1)) / s
    pointwise = huber_like_loss(margins)  # quadratic near 0, linear far
    # Power-mean with omega to emphasize hard cases
    per_natural = (pointwise.mean(dim=1)) ** omega
    return per_natural.mean() ** (1.0 / omega)
```

**Token vocabulary management:**

Principal keys are the vocabulary for action representations. For a 50K-principal
enterprise, this is manageable as a direct embedding table. For larger enterprises,
use feature hashing to a fixed vocabulary size (e.g., 2^17 = 131K buckets).

### WS4: Clustering and Multi-Scale Detection

**Goal:** Implement the clustering layer that aggregates per-action scores into
per-principal detection signals.

**Approach (per Facade):**

Given a set of actions A performed by a principal in a time window, and the principal's
context c:

1. Compute action embeddings E_A(a) for each a in A.
2. Cluster actions using hierarchical agglomerative clustering with cosine similarity.
   Merge threshold: delta (hyperparameter).
3. For each cluster X, take the max anomaly score: max_{a in X} f(a, c).
4. Sum across clusters: g(A, c) = sum_{X in G} max_{a in X} f(a, c).

This means:
- Accessing the same unusual resource 100 times scores the same as accessing it once
  (idempotence via clustering).
- Accessing 5 *different* unusual resources scores ~5x one unusual access (monotonicity
  via summation across clusters).

**Implementation:**
```python
from scipy.cluster.hierarchy import fcluster, linkage

def multi_scale_score(action_embeddings, action_scores, delta):
    """
    action_embeddings: [n_actions, d] — E_A outputs for this principal's actions
    action_scores: [n_actions] — f(a, c) scores
    delta: float — clustering threshold
    """
    if len(action_embeddings) == 1:
        return action_scores[0]

    # Hierarchical agglomerative clustering
    Z = linkage(action_embeddings, method='average', metric='cosine')
    clusters = fcluster(Z, t=delta, criterion='distance')

    # Max score per cluster, sum across clusters
    total = 0.0
    for cluster_id in set(clusters):
        mask = clusters == cluster_id
        total += action_scores[mask].max()
    return total
```

**Detection thresholding:**

After computing g(A, c) for each principal in each time window:
- Rank principals by score
- Detection threshold: top K principals per day (configurable, default K=10 per
  Facade's recommendation for Google-scale)
- Alternatively: fixed score threshold calibrated against validation FPR

### WS5: Evaluation Framework

**Goal:** Build a rigorous evaluation harness that measures detection quality across
multiple dimensions and prevents us from fooling ourselves.

**Evaluation dimensions:**

1. **Detection quality on planted scenarios:**
   - For each scenario type (exfiltration, credential compromise, privilege escalation,
     departing employee, account takeover, insider collaboration):
     - TPR at FPR = 0.1%, 0.01%, 0.001%
     - ROC AUC
     - Precision-recall AUC
   - Aggregate across all scenarios

2. **False positive analysis:**
   - FPR at action level (what fraction of benign actions score above threshold)
   - FPR at principal level (what fraction of benign principals appear in daily top-K)
   - Characterize false positives: are they truly benign, or ambiguous edge cases?

3. **Cold-start curve:**
   - Train on D days of history, evaluate detection quality
   - D = 1, 3, 7, 14, 21, 30 days
   - Measure: at what D does quality reach 80% of full-history quality?

4. **Temporal robustness (drift):**
   - Train on months 1-3, evaluate on months 4, 5, 6, 7, 8, 9 (no retraining)
   - Measure: at what month does quality degrade below acceptable threshold?

5. **Enterprise size sensitivity:**
   - Evaluate on synthetic enterprises of 1K, 5K, 10K, 50K, 100K principals
   - Measure: is there a minimum size below which the approach fails?

6. **Ablation studies:**
   - Remove each feature family and measure quality delta:
     - History only (no peers, no static)
     - Peers only (no history, no static)
     - No social/collaboration features
     - No geographic features
     - Single source type only vs all sources combined
   - This tells us what matters and what we can ship without

7. **Comparison to PoC heuristics:**
   - Run the current scoring.py heuristics on the same evaluation data
   - ML approach must strictly dominate heuristics on TPR at matched FPR
   - If it doesn't, we need to understand why

**Data splitting (per Facade):**

Temporal split: last events in training precede first events in test.
Spatial split: randomly hold out 50% of principals from training.

This is critical — it tests both temporal generalization (future events) and spatial
generalization (unseen principals). The model must generalize to principals it has
never trained on, using only their context features.

**Metrics implementation:**

```python
class EvaluationHarness:
    def evaluate(self, model, test_data, scenarios):
        results = {}
        for scenario in scenarios:
            scenario_events = test_data.filter(scenario=scenario.name)
            benign_events = test_data.filter(scenario=None)

            scores_attack = model.score_batch(scenario_events)
            scores_benign = model.score_batch(benign_events)

            results[scenario.name] = {
                'roc_auc': roc_auc_score(...),
                'tpr_at_fpr_01': tpr_at_fpr(scores_attack, scores_benign, 0.001),
                'tpr_at_fpr_001': tpr_at_fpr(scores_attack, scores_benign, 0.0001),
                'pr_auc': average_precision_score(...),
            }

        return results
```

### WS6: ONNX Export and Inference Validation

**Goal:** Export the trained model to ONNX and validate that inference produces
identical results, establishing the interface for future Rust integration.

**Steps:**
1. Export both towers to ONNX using torch.onnx.export
2. Validate numerical equivalence (< 1e-5 max absolute difference)
3. Benchmark inference latency: target < 1ms per action on CPU
4. Document the ONNX input/output schemas as the contract between Python training
   and future Rust inference

**ONNX considerations:**
- The action tower takes variable-size weighted token sets — either pad to max length
  or use a fixed-size hash-based representation
- The context tower takes the full context feature vector — document the exact layout
- Both towers output d-dimensional L2-normalized vectors

---

## Implementation Sequence

```
Week 1-2:   WS1 — Synthetic data generator (org, resources, activity, scenarios)
Week 2-3:   WS2 — Feature engineering pipeline (accessor sets, context features)
Week 3-5:   WS3 — Model implementation (two-tower, contrastive loss, training loop)
Week 4-5:   WS4 — Clustering and multi-scale detection
Week 5-7:   WS5 — Evaluation framework and experiments
Week 6-7:   WS6 — ONNX export and inference validation
Week 7-8:   Analysis, ablation studies, write-up, decision point
```

Overlap is intentional — WS2 can begin before WS1 is complete (use small synthetic
data first), WS4-5 can begin as soon as WS3 produces initial models.

**Decision point (end of week 8):**
- If success criteria are met → proceed to M1 with confidence
- If FPR is too high → investigate: is it the model, the features, or the data?
- If cold-start is too slow → investigate transfer learning, heuristic bridging
- If training is too slow → investigate model size, data sampling, mixed precision

---

## Relation to Current PoC

The existing Python PoC provides:

**Reuse directly:**
- Normalized event schema and contracts (contracts.py)
- Entity key generation for stable identity (ids.py)
- Source-specific field mappings (vendor_exports.py)
- Dead-letter handling patterns
- The 4 source integrations: Okta, Google Workspace, GitHub, Snowflake

**Extend significantly:**
- Synthetic data generator (synthetic.py): from 55 events / 5 principals to 10^6+
  events / 10^3+ principals
- Feature engineering: from 6-dimensional heuristic vector to Facade-style accessor
  sets and context features

**Baseline to beat:**
- Current heuristic scoring (scoring.py): 6 detectors with hard-coded weights and
  thresholds. The ML approach must strictly dominate this on evaluation metrics.

**Keep running in parallel:**
- The PoC continues as the demo and design-partner vehicle while M0 validates the
  ML approach. Heuristic scoring ships first; ML scoring replaces it when validated.

## Open Questions

1. What embedding dimensionality d works best at our scale (smaller enterprises than
   Google)? Facade doesn't disclose theirs.
2. How well does the inferred collaboration graph (shared resource access) substitute
   for explicit meeting data?
3. Is there a minimum enterprise size below which contrastive learning on per-customer
   data doesn't have enough signal? If so, what's the transfer learning strategy?
4. Should we use a transformer-based encoder or simpler MLP + weighted pooling (as
   Facade appears to use)?
5. Can we pre-train a useful base model on the CERT Insider Threat Dataset or similar
   public data, or are the distributions too different?
6. How do we handle the principal vocabulary for the action tower when onboarding a
   new customer (all tokens are new)?

## Risk Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Synthetic data doesn't reflect real enterprise distributions | Model works on synthetic, fails on real | Validate against CERT dataset; calibrate synthetic distributions against published research; get design partner data ASAP |
| Contrastive learning produces too many false positives | Product is unusable for SOC teams | Clustering refinement; hierarchical scoring (action → session → principal); tune threshold on validation set |
| Cold-start period is too long | New customers see no value for weeks | Heuristic-first scoring (current PoC) bridges the gap; transfer learning from base model; pre-computed resource popularity priors |
| Training is too slow for per-customer fine-tuning | Operational burden, slow onboarding | Profile and optimize data loading; smaller fine-tuning model; freeze base model, only train adapter layers |
| Facade approach doesn't work at smaller enterprise scale | Fundamental approach failure | This is what M0 validates; if it fails at 10K principals, investigate hybrid approaches (contrastive + supervised on synthetic attacks) |
