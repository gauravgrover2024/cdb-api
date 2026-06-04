# ACI Decision Module Phase 0 Closure Report v1

Date: 2026-06-04  
Repo: cdb-api  
Scope: ACI Assist / CDrive backend decision governance foundation

## Closure status

Phase 0 is closed as a governance foundation, not as a fully wired final recommendation system.

This means the repository now has the core policy, contracts, provenance, degraded-mode, market-judgement isolation and real-output fixture gates needed before deeper decision-module runtime work.

## What Phase 0 locked

1. Decision Module Production Plan v1.
2. Decision policy and eligibility contract skeleton.
3. Shared decision output contract helpers.
4. Provenance and freshness helper.
5. Degraded-mode helper and eval.
6. Module-specific policy profiles.
7. Policy eval corpus.
8. Market-judgement isolation audit.
9. Score insight real-output policy fixture eval.
10. Similar-cars real-output policy fixture eval.
11. Similar-cars filtering audit.
12. Similar-cars relation-mode regression eval.
13. Similar-cars runtime relation-mode fix.

## What Phase 0 does not claim

Phase 0 does not mean:

1. Final recommendation engine is production-ready.
2. All runtime decision outputs are fully policy-wrapped.
3. Evidence/provenance fields are enforced across every runtime tool.
4. Score insight wording has been cleaned.
5. Cross-model diagnostic score insight is built.
6. Recommendation composer is finalized.
7. Sponsored/revenue systems are wired.
8. All future scale indexes are locked.

## Current production guardrail principle

Score insight, similar cars, comparison and upgrade-ladder style modules remain diagnostic-only.

Only the dedicated future recommendation module can eventually produce a final recommendation, and only after buyer context, evidence thresholds, provenance/freshness and policy eligibility checks pass.

## Similar-cars correction locked during Phase 0

The similar-cars graph may contain multiple relation types:

- direct_rival
- platform_twin
- nearby_alternative
- adjacent_crossover
- cheaper_step_down
- premium_step_up
- powertrain_shift

Default "similar cars" must show clean close alternatives only. It must not silently mix cheaper step-downs, premium step-ups or EV/powertrain-shift rows as default close alternatives.

Explicit cheaper / premium / EV requests are handled by their own relation modes.

## Index and performance note

Current index inspection shows the main price path is healthy:

- aci_vehicle_price_rows uses citySlug + exShowroomPrice index efficiently.
- Similar graph collection is small enough that current scans are acceptable.
- Score-profile eval sampling currently examines roughly 2012 docs in broad sampling paths; this is acceptable for current eval tooling but should be revisited before scale or before adding heavier score-profile runtime paths.

Do not add speculative indexes blindly. Add indexes only after locking actual query shapes and verifying explain plans.

## Phase 0 required gate bundle

Before Phase 0 closure commit, these must pass:

1. aci:decision:policy:smoke
2. aci:decision:policy:eval
3. aci:decision:provenance:eval
4. aci:decision:degraded-mode:eval
5. aci:decision:module-policy:eval
6. aci:decision:market-judgement:audit
7. aci:decision:score-output-fixture:eval
8. aci:decision:similar-output-fixture:eval
9. aci:decision:similar-filter:audit
10. aci:decision:similar-relation-mode:eval
11. smokeSimilarModelGraphV1
12. aci:safety:fast

## Next phase

Phase 1 may start only after this closure report and gate bundle are committed.

Phase 1 should focus on current score insight cleanup:

1. Refine diagnostic language audit.
2. Remove overconfident score wording.
3. Keep score insight diagnostic-only.
4. Do not change score formulas blindly.
5. Do not add hardcoded market judgement.
6. Do not wire final recommendation behavior.
