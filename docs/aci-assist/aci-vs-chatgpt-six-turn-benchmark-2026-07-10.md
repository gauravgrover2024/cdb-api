# ACI Assist vs ChatGPT: Six-Turn Benchmark

Date: 2026-07-10

## Method

The same conversation was run sequentially in both systems:

1. `thar colors`
2. `abs`
3. `sunroof`
4. `vs creta`
5. `creta sunroof`
6. `abs`

ACI was tested through the frontend context transport contract and live backend service. ChatGPT was tested through the public logged-out ChatGPT experience in one continuous chat. This is a point-in-time behavioral comparison, not a permanent claim about ChatGPT.

## Turn Comparison

| Turn | ChatGPT observed behavior | ACI Assist observed behavior | Current winner |
|---|---|---|---|
| `thar colors` | Listed current-looking colors but also mixed in older/model-year possibilities and unverified variant labels. | Returned the DB-backed current model and six-color result with color payload, but the prose only stated the count. | ChatGPT for prose; ACI for controlled truth |
| `abs` | Correctly reused Thar context, answered yes, and explained what ABS does. It cited a Mahindra source, although the visible source was a Thar Roxx manual while the active subject was Thar. | Correctly reused Thar, proved ABS on all 7 current indexed variants, and did not treat ABS as a variant. After Feature Explainer v1, it also explains steering control and emergency/slippery-road relevance from a Mongo-backed source-reviewed record. | ACI after explainer integration |
| `sunroof` | Correctly kept Thar context, distinguished 3-door Thar from Thar Roxx, and added aftermarket caution. | Correctly kept Thar context and stated that sunroof is not listed on current Thar variants. It did not yet provide the related Thar Roxx alternative or ownership advice. | ChatGPT for buyer education; ACI for strict scope |
| `vs creta` | Produced a useful practical table and clear use-case split: Thar for off-road use, Creta for family/city use. Several claims were not visibly sourced or variant-scoped. | Used exact DB-backed representative variants and an on-road price delta, preserved comparison state, and avoided technical/internal wording. The first answer still lacks a rich practical summary unless buyer priorities are supplied. | ChatGPT for immediate usefulness; ACI for evidence control |
| `creta sunroof` | Correctly switched to Creta and described higher/lower trim availability, but used broad trim examples rather than a complete current count. | Correctly switched to Creta, cleared comparison state, and reported panoramic sunroof on 43 of 50 current indexed variants. | ACI |
| final `abs` | Correctly stayed on Creta, explained ABS, and added several related safety claims that were not visibly sourced in that answer. | Correctly stayed on Creta and proved ABS on all 50 current indexed variants. Feature Explainer v1 adds the generic ABS explanation without inventing Creta-specific safety equipment. | ACI for factual answer; ChatGPT for breadth |

## Status By Capability

| Capability | ChatGPT | ACI Assist | Honest status |
|---|---:|---:|---|
| Context continuity in this sequence | Strong | Strong | ACI regression is now fixed |
| Current vehicle-data grounding | Mixed/opaque | Strong and auditable | ACI advantage |
| Feature meaning and buyer education | Strong | Partial | Feature Explainer is now started; coverage must expand |
| Immediate comparison usefulness | Strong | Partial | ACI needs a richer DB-backed comparison narrative |
| Variant-level precision | Mixed | Strong where read models are complete | ACI advantage |
| Hallucination resistance | Variable | Strong by policy | ACI advantage |
| Warm response speed | Not measured precisely in this browser run | 512 ms median, 1,013 ms p95 in the latest 13-turn smoke | ACI meets the current warm target |

## Product Conclusion

ACI is no longer failing at conversational context in this flow. Its factual discipline and variant coverage are stronger than the observed ChatGPT answers. ChatGPT still feels more helpful when it explains why a feature matters, offers adjacent advice, and turns a comparison into practical buyer guidance immediately.

The next quality work should therefore preserve ACI's DB grounding while adding:

1. Source-reviewed feature explanations.
2. DB-backed practical comparison narratives from actual difference rows.
3. Related-model recovery where it is useful and explicit.
4. Buyer-context guidance without unsupported absolute verdicts.

Feature Explainer v1 is the first implementation step from this benchmark.
