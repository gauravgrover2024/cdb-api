# Feature Explainer Launch Readiness

Date: 2026-07-10

## Status

The Feature Explainer runtime and canonical coverage are ready for the current new-car feature scope. Editorial depth is strong for the 39 feature-specific rules and structurally complete, but less detailed, for the remaining taxonomy-driven records.

- Atlas collection: `aci_feature_explainers_v1`
- Canonical catalog: `vehicle_feature_catalog_v2`
- Coverage: 397/397 published features across 14 groups
- Coverage audit: zero missing, orphaned, duplicate or invalid records
- Runtime: cache-backed and model-free after startup
- Warm direct-explanation benchmark: 0.18 ms median and 8.99 ms p95 across 30 mixed feature queries; the first bridge call was 556.49 ms
- Vehicle availability: always read from the feature matrix, never from explainer prose

## Content Quality

- All 397 records are now rebuilt by the Codex-authored deterministic editorial taxonomy. Gemini is not used by the build or runtime.
- Every Atlas record declares `contentOrigin: codex_curated_taxonomy`, either `qualityStatus: codex_feature_reviewed` or `codex_taxonomy_validated`, `editorial.modelGenerated: true`, `editorial.generationProvider: openai_codex`, `editorial.runtimeModelGenerated: false` and `editorial.geminiUsed: false`. This keeps authoring provenance honest while confirming there is no Gemini or runtime generation dependency.
- Legacy generated source references are not reused. ABS carries explicitly curated Bosch and NHTSA references; other records receive deterministic authoritative group context or canonical-catalog provenance until feature-specific source review is completed.
- 39 high-frequency or high-risk concepts use feature-specific editorial rules; the remaining records use measurement-, safety-, powertrain- or equipment-group rules driven by the canonical catalog.
- The legacy Gemini generator and the obsolete two-record seed builder were removed so they cannot overwrite production explainers later.
- ADAS scope guards prevent ACC/stop-go, high-beam/matrix-light and lane-keep/lane-centring capability inflation.
- Safety guards cover warning-vs-intervention language, airbag coverage, curtain-airbag rollover claims and crash-rating protocol context.

## Buyer Experience

- `ABS` without vehicle context returns a standalone explanation.
- `ABS` after selecting Thar remains a DB-backed availability answer and adds decision-useful education.
- `what is ADAS?` resolves the ADAS package rather than a random ADAS sub-feature.
- `is a sunroof worth it for family city use?` explains cabin light, heat, added complexity, safe use and exact-variant checks.
- `ABS vs EBD` compares both roles without treating similar names as interchangeable.
- Variant feature comparisons attach buyer-impact summaries to real gained/lost difference rows.

## Launch Gates

- `npm run -s aci:feature-explainer:coverage`
- `npm run -s aci:feature-explainer:smoke`
- `npm run -s aci:no-hardcoded-facts:audit`
- `npm run -s aci:safety:fast`

## Remaining Work

These items do not block the explainer runtime, but remain outside a claim that the whole advisory backend is complete:

- Expand feature-specific primary-source review beyond ABS.
- Finish the dedicated gained/lost-feature narrative operation.
- Continue replacing broad taxonomy explanations with feature-specific editorial rules where buyer traffic or audits show a material quality gain.
