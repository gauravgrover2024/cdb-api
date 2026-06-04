# ACI Decision Module Production Plan v1

Status: Locked plan / Phase 0
Owner: Product + Backend + QA
Date locked: 2026-06-04
Current safe baseline: 07cdf1d Add guarded similar cars graph read model

---

## 1. Purpose

ACI Assist must become a production-grade buyer decision-intelligence system, not a chatbot that sounds confident because a route succeeded.

This plan defines the production policy, contracts, provenance, degraded modes, eval gates, and implementation sequence required before ACI Assist is allowed to give final car-buying recommendations.

The immediate next task is not cross-model scoring and not score wording cleanup. The immediate next task is Phase 0: decision policy, output contracts, and eval framework.

---

## 2. Locked non-negotiables

1. Every buyer-facing decision claim must be traceable to data.
2. Missing data must be disclosed.
3. Facts, diagnostic scores, and opinions must remain separate.
4. Final buy-this recommendations are blocked unless Decision Policy allows them.
5. Empty result must not pass as success.
6. A route selecting the right tool is not sufficient; the result must contain useful rows/evidence or a clear degraded-mode state.
7. No hardcoded car, variant, rival, persona, segment, resale, service-network, or market-judgment shortcuts.
8. No large decision logic blobs inside vehicleScoreInsight.tool.js.
9. Tools return structured facts/signals; central composer/language layer handles final wording and leading questions later.
10. Sponsored/revenue systems must never influence organic recommendations, rankings, comparisons, price verdicts, safest/best-value advice, or buyer decision logic.
11. City-specific price/availability stays separate from global variant/model intelligence.
12. DB/read-model grounded logic only.

---

## 3. Current module state after planning correction

- Similar cars graph/read model: about 82/100
- Variant/model score insight: about 96/100
- Decision policy/contracts: 0/100
- Decision eval framework: 0/100
- Cross-model diagnostic insight: 0/100 after revert
- Final recommendation engine: about 20-25/100
- Full decision module: about 55/100

The full module score is intentionally lowered because production controls are missing.

---

## 4. Decision Policy / Eligibility contract

Every decision-related output must pass through a central policy layer before final recommendation wording is allowed.

Minimum policy output fields:

- decisionPolicyVersion
- canUseForFinalRecommendation
- allowedAnswerType
- blockedReasons
- missingMandatoryInputs
- evidenceStatus
- confidence
- degradedMode
- claimType

Allowed answer types:

- fact_only: only verified facts may be stated.
- diagnostic_only: scores/trade-offs can be shown but no final buy verdict.
- clarification_required: ask for required missing buyer context.
- recovery_required: exact result failed; offer alternatives/recovery.
- final_recommendation_allowed: a final recommendation/verdict is allowed.
- blocked: do not answer as decision advice.

Claim types:

- fact: direct DB/source-backed fact.
- diagnostic: derived score, comparison, trade-off, or read-model inference.
- opinion: contextual buyer-facing recommendation/opinion allowed only after policy approval.

---

## 5. Decision output schema

All decision tools/services must eventually normalize to a shared decision output envelope with:

- schemaVersion
- module
- intent
- comparisonScope
- claimType
- decisionPolicy
- evidence
- provenance
- degradedMode
- rows
- diagnostics
- recoveryOptions
- trace

Evidence must include:

- evidenceStatus
- confidence
- sourceTransparency
- missingData
- usableEvidenceCount
- requiredEvidenceCount

Provenance must include:

- buildVersion
- builtAt
- sourceClass
- stalenessDays
- needsRebuild

Trace must include:

- toolRoute
- collectionsUsed
- matchedRows
- candidateCount
- warnings

No decision route should return success if the normalized output has zero useful rows/evidence and no degraded mode.

---

## 6. Evidence, freshness, and provenance fields

Decision outputs must expose provenance whenever the answer depends on built read models or derived scores.

Required provenance fields:

- buildVersion: version of builder/config/schema used to produce the artifact.
- builtAt: build timestamp.
- sourceClass: type of evidence used.
- stalenessDays: days since build/source timestamp.
- needsRebuild: true when artifact is stale or source version changed.

Source classes:

- direct_db_fact
- internal_score_profile
- similar_graph_inference
- estimated_value
- inherited_model_level_evidence
- manual_curated_evidence
- mixed

Freshness rule:

A stale artifact must not silently drive a confident recommendation. It can still produce diagnostic output if disclosed and policy allows.

---

## 7. Degraded-mode taxonomy

All decision modules must return one of these degraded states when full output is not available:

- resolver_succeeded_no_candidates
- candidates_found_no_score_profiles
- score_profiles_found_no_context_score
- evidence_confidence_too_low
- stale_artifact_needs_rebuild
- conflicting_evidence_blocked
- buyer_context_incomplete
- unsupported_city
- final_recommendation_blocked
- diagnostic_only_available
- empty_result_recovery_required

Empty result cannot be classified as success.

---

## 8. Final recommendation eligibility rules

Final recommendation means any answer that says or implies:

- Buy this.
- This is the best choice for you.
- Choose X over Y.
- Final verdict: X.
- I recommend X.

These are blocked unless Decision Policy returns:

- canUseForFinalRecommendation: true
- allowedAnswerType: final_recommendation_allowed

Minimum mandatory buyer inputs:

1. city
2. budget or priceCeiling
3. bodyPreference or primaryUseCase
4. familySize or occupancy/use pattern
5. fuelPreference or monthly running estimate
6. transmissionPreference
7. safetyPriority
8. featurePriority
9. shortlistedModels or discoveryScope

Minimum evidence thresholds:

1. Candidate set must be non-empty.
2. Relevant score/decision profiles must exist.
3. Price/city overlay must be available for supported city when price/value is part of the advice.
4. Missing data must not affect the decisive claim.
5. Source/provenance must be available.
6. Artifact must not require rebuild.
7. Sponsored/revenue inputs must be absent from organic ranking and verdict logic.

If requirements are not met, the answer may be fact_only, diagnostic_only, clarification_required, or recovery_required.

---

## 9. Market-judgment isolation rules

Market judgment includes claims about resale value, service network, reliability, brand perception, premium feel, enthusiast appeal, family suitability, chauffeur suitability, urban premium, best for Indian families, rival positioning, market leader, avoid this, and must buy.

Rules:

1. No market judgment may be hardcoded inside route logic, parser logic, score insight tools, or answer templates.
2. Market judgment must come from curated/source-backed profiles or explicit verified signals.
3. Generic body-type/persona shortcuts are disallowed.
4. Brand/model names must not be used as hidden scoring shortcuts.
5. Eval tests must scan for unauthorized hardcoded judgement phrases.
6. Final composer may phrase approved claims, but cannot invent them.

---

## 10A. Validation-discovered production risks to cover

The 2026-06-04 pre-commit validation passed mechanically, but exposed product-quality risks that must be covered before runtime decision expansion.

### Answer-specificity risk

Score insight answers must vary by requested dimension. A value question, strength/weakness question, family-use question, city-use question and upgrade question must not receive the same generic paragraph.

Required future eval:
- Same vehicle, different decision question.
- Assert answer focus changes according to intent.
- Assert final recommendation remains blocked unless policy allows it.

### Watchout boilerplate risk

Generic caveats must not flood buyer-facing answers. Caveats should be tiered.

Required future contract:
- `watchouts.primary`: buyer-impacting, variant-specific.
- `watchouts.secondary`: relevant but not urgent.
- `watchouts.technical`: debug/source caveats, hidden unless expanded.

Required future eval:
- Buyer-facing answer should show only the top non-generic watchouts by default.

### Prewarm SLA risk

Prewarm warnings must become hard gates in freeze/full validation before launch.

Required future gates:
- Fast mode: warn only.
- Freeze mode: fail above agreed threshold.
- Full/launch mode: fail above public-launch threshold.

### Mileage evidence gap risk

Missing mileage/running-cost evidence can bias value, running-cost and regret diagnostics.

Required future gate:
- Report exact `mileageNotScored` count and affected variant keys.
- Block final recommendation when missing mileage materially affects the decisive claim.

### Feature alias diagnostic risk

`featureAliasDiagnostic` must not remain only a count.

Required future gate:
- Report affected variant/profile keys.
- Report whether feature score was degraded.
- Fail if affected variants include high-volume models above threshold.

### Transmission subtype city-score risk

AMT, CVT, torque converter, DCT/DSG and manual must not be treated as equally city-friendly without evidence.

Required future contract:
- Transmission subtype evidence.
- City-use confidence.
- Low-speed smoothness caveat when evidence is incomplete.

### Safety score confidence risk

Internal feature-matrix safety signals must not visually look equivalent to verified official crash applicability.

Required future contract:
- Verified official crash evidence and inherited/internal evidence must have different confidence and score caps.
- Identical top safety scores across very different evidence classes should trigger an audit warning.

### Sponsored separation test risk

Sponsored/revenue separation is a locked principle but not production-ready until a test proves organic rankings/recommendations ignore paid inventory.

Required future eval:
- Inject sponsored metadata into fixture/context.
- Verify organic recommendation order and verdict logic are unchanged.
- Verify sponsored inventory can appear only as labeled paid placement outside neutral recommendation brain.


## 10. Eval suites and pass/fail gates

Smoke tests are not enough. Phase 0 requires eval harness definitions before new decision logic ships.

Required eval suites:

- Named rival comparisons
- Similar cars with constraints
- Variant ladder / upgrade
- Recommendation-block cases
- Missing-data honesty
- Unsupported city
- Premium/performance edge cases
- No hardcoded brand judgement
- Final recommendation eligibility
- Empty result handling

Phase 0 is not complete until:

1. Decision policy contract exists.
2. Decision output schema is documented.
3. Degraded-mode taxonomy is documented.
4. Final recommendation eligibility contract is documented.
5. Eval suite list and expected blocking cases are documented.
6. Progress registry reflects Phase 0.
7. No runtime scoring or recommendation code has been changed.

---

## 11. Implementation phases

Phase 0 - Decision policy + contracts + eval framework:
- This production plan.
- Progress tracker/registry update.
- Later: aciDecisionPolicy contract skeleton.
- Later: decision eval corpus skeleton.
- No scoring behavior changes yet.

Phase 1 - Current score insight cleanup:
- Keep vehicleScoreInsight.tool.js lean.
- Move reusable policy/contract logic into dedicated files.
- Preserve existing safe score insight behavior.
- Ensure every score output is diagnostic, not final recommendation.

Phase 2 - Dedicated cross-model candidate resolver service:
- DB/read-model grounded resolver.
- No hardcoded rival pairs.
- Explicit no-candidate degraded mode.
- Constraint-aware candidate set.

Phase 3 - Cross-model diagnostic score insight:
- Diagnostic-only cross-model score insights.
- No final recommendation wording.
- Evidence/provenance attached.
- Empty result blocked.

Phase 4 - Evidence/freshness/degraded-mode hardening:
- buildVersion, builtAt, sourceClass, stalenessDays, needsRebuild.
- Degraded-mode propagation through tools and composer.
- Result usefulness checks.

Phase 5 - Final recommendation eligibility engine:
- Buyer context completeness checks.
- Evidence threshold checks.
- Policy-approved final verdicts only.
- Clarification/recovery when blocked.

Phase 6 - Final answer composer / centralized language layer:
- Centralized buyer-facing wording.
- Separation of tool facts/signals from final phrasing.
- No leading questions scattered inside tools.
- Policy-aware verdict language.

---

## 12. Files not to touch in Phase 0

Do not modify these in Phase 0 unless explicitly reviewing only:

- src/services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js
- src/services/aiAgent/tools/newCars/vehicleSimilar.tool.js
- src/services/aiAgent/tools/newCars/vehicleRecommendation.tool.js
- src/services/aciCore/integration/aciCoreLiveBridge.service.js
- src/services/aciCore/integration/aciCoreToLegacyPlan.adapter.js
- score/read-model builders
- DB refresh scripts

Phase 0 is docs/progress/contract planning only.

---

## 13. Definition of done for Phase 0

Phase 0 is done when:

1. ACI_DECISION_MODULE_PRODUCTION_PLAN_V1.md is committed.
2. Progress tracker mentions Decision Module Production Policy and Contracts.
3. Live progress registry has a dedicated module for Decision Policy, Contracts & Evals.
4. Validation passes:
   - node -c src/services/aciProgress/aciProgress.registry.cjs
   - node -c src/services/aciProgress/aciProgress.service.cjs
   - npm run aci:safety:fast
5. Diff confirms no runtime decision/scoring/recommendation files changed.
