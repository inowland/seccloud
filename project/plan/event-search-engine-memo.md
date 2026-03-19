# Event Engine Decision Memo

Date: March 18, 2026

## Decision

Use **Quickwit** as the primary event engine for the next 1-2 milestones.

Keep **LanceDB** as the strongest alternative to watch.

Treat **custom Tantivy + DataFusion** as the long-term exit path if product fit, operational burden, or governance pressure makes owning the stack preferable.

## Locked Constraints

- Canonical long-lived event data lives only in object storage.
- Serving systems may store only metadata, pointers, indices, and caches.
- Near-term event UX is `search + timeline`, not a full analyst workbench.
- Vector/hybrid retrieval importance is medium, not immediate.
- We prefer not to choose a core event architecture whose likely long-term scale story depends on a paid vendor path.

## Scored Comparison

Scores are relative and specific to Seccloud's current constraints.

| Criterion                             | Quickwit   | LanceDB     | Custom Tantivy + DataFusion     |
| ------------------------------------- | ---------- | ----------- | ------------------------------- |
| Metadata-only secondary copy fit      | High       | Medium-High | High                            |
| Near-term search/timeline product fit | High       | Medium-High | Medium                          |
| Small-scale operational burden        | Medium-Low | Medium      | Low now, potentially High later |
| Long-term control/governance          | Low-Medium | Medium      | High                            |
| Future vector/hybrid path             | Low-Medium | High        | Medium-High                     |
| Delivery speed                        | High       | Medium      | Low                             |

## Why Quickwit Wins Now

- Quickwit is explicitly built for cloud-native observability search over immutable event-style data: [Quickwit docs](https://quickwit.io/docs) and [Quickwit GitHub](https://github.com/quickwit-oss/quickwit).
- That maps well to our current event-plane job:
  - metadata+pointer index over canonical lake truth
  - structured filters
  - reverse-chronological browse
  - principal/resource pivots
- It is the fastest path to a credible event surface without building the engine ourselves.

## Why Quickwit Is On A Short Leash

Quickwit is the recommendation, but not on trust alone. Small-scale operational burden is a first-class criterion.

For Seccloud, small-scale operational burden means:

- local startup complexity
- index creation and sync friction
- debugging visibility
- failure handling quality
- memory and CPU footprint for small tenants
- how much bespoke operator glue we have to maintain

Quickwit already shows some risk here:

- it is a heavier system than we would like for a small local stack
- we are already carrying explicit runtime management, health checks, sync commands, and smoke tooling around it
- OSS/governance confidence is weaker than ideal after the Datadog acquisition; this is an inference, but a reasonable one

As of March 18, 2026, GitHub shows roughly **10.5k stars** for Quickwit and the latest listed release as **v0.8.2 on June 17, 2024**: [Quickwit GitHub](https://github.com/quickwit-oss/quickwit).

## Why LanceDB Is The Strongest Alternative

- LanceDB is a much more credible alternative than Meilisearch under our cost rule because the underlying Lance format is explicitly object-store and lakehouse oriented: [Lance format](https://docs.lancedb.com/lance).
- It also has the most attractive future retrieval story in one system:
  - full-text search
  - vector search
  - hybrid search
    Sources:
    [FTS](https://docs.lancedb.com/search/full-text-search),
    [hybrid search](https://docs.lancedb.com/search/hybrid-search),
    [vector search](https://docs.lancedb.com/search/vector-search)

Why it does not win today:

- current event UX is still `search + timeline`, not hybrid semantic retrieval
- we care more about immediate event-surface delivery than unified future retrieval elegance
- confidence in the long-term self-controlled scale path is weaker than we want
- LanceDB's strongest scale story appears to lean toward Cloud or Enterprise rather than a fully proven OSS path we would own end to end; see [LanceDB Cloud](https://docs.lancedb.com/cloud), [Enterprise features](https://docs.lancedb.com/enterprise/features), and [Enterprise architecture](https://docs.lancedb.com/enterprise/architecture)

So LanceDB is the serious watch candidate, not the current default.

## Why Custom Does Not Win Today

The custom path is:

- **Tantivy** for search and indexing
- **DataFusion** for lake-native execution
- ANN/vector only when product needs justify it

This remains the strongest long-term control story:

- full ownership of query behavior and ranking
- no external engine governance risk
- clean alignment with the Rust-heavy stack

It still does not win today because:

- it front-loads engine work before the product surface is fully proven
- it delays analyst-facing progress
- it forces us to own indexing, query semantics, pagination, filtering, and reliability too early

## Re-evaluation Triggers

We should re-open the engine choice if any of these happen:

- small-scale Quickwit operations remain painful after current runtime hardening
- Quickwit startup, sync, and debug behavior require too much custom glue
- Quickwit resource footprint is too high for small deployments
- Quickwit OSS/governance confidence worsens materially
- vector or hybrid retrieval becomes near-term product-critical
- LanceDB's OSS or self-hosted scale story becomes materially stronger and no longer appears to imply a likely paid path

## Recommendation In One Line

Use **Quickwit now**, monitor small-scale operational pain aggressively, keep **LanceDB** under active watch, and preserve **custom Tantivy + DataFusion** as the deliberate long-term exit path.
