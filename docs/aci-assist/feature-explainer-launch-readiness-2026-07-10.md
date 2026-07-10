# Feature Explainer Launch Readiness

Date: 2026-07-10

## Status

The Feature Explainer knowledge and chat runtime are ready for the current canonical new-car feature scope.

- Atlas collection: `aci_feature_explainers_v1`
- Canonical catalog: `vehicle_feature_catalog_v2`
- Coverage: 397/397 published features across 14 groups
- Coverage audit: zero missing, orphaned, duplicate or invalid records
- Runtime: cache-backed and model-free after startup
- Warm direct-explanation benchmark: 0.18 ms median and 8.99 ms p95 across 30 mixed feature queries; the first bridge call was 556.49 ms
- Vehicle availability: always read from the feature matrix, never from explainer prose

## Content Quality

- 395 records were generated offline with structured fields, then passed through a separate adversarial review and deterministic safety audit.
- Generated quality scores range from 0.89 to 0.98, with a 0.942 mean.
- ABS and sunroof are manually curated, source-backed high-frequency records.
- ADAS scope guards prevent ACC/stop-go, high-beam/matrix-light and lane-keep/lane-centring capability inflation.
- Safety guards cover warning-vs-intervention language, airbag coverage, curtain-airbag rollover claims and crash-rating protocol context.

## Buyer Experience

- `ABS` without vehicle context returns a standalone explanation.
- `ABS` after selecting Thar remains a DB-backed availability answer and adds decision-useful education.
- `what is ADAS?` resolves the ADAS package rather than a random ADAS sub-feature.
- `is a sunroof worth it for family city use?` explains value, heat, headroom, maintenance and test-drive checks.
- `ABS vs EBD` compares both roles without treating similar names as interchangeable.
- Variant feature comparisons attach buyer-impact summaries to real gained/lost difference rows.

## Launch Gates

- `npm run -s aci:feature-explainer:coverage`
- `npm run -s aci:feature-explainer:smoke`
- `npm run -s aci:no-hardcoded-facts:audit`
- `npm run -s aci:safety:fast`

## Remaining Work

These items do not block the explainer runtime, but remain outside a claim that the whole advisory backend is complete:

- Expand primary-source review beyond ABS and sunroof.
- Calibrate feature importance for final recommendation scoring.
- Finish the dedicated gained/lost-feature narrative operation.
- Keep final recommendation activation disabled until its separate policy gates pass.
