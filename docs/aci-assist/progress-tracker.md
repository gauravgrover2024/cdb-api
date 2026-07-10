# ACI Assist Progress Tracker

<!-- ACI_LOCKED_SCOPE_2026_05_28_START -->
## Locked Scope Update — 2026-05-28

### Product Direction Locked Today

ACI Assist is not being built as a normal chatbot or listing portal. The locked direction is:

**Buyer decision-intelligence layer for new-car purchase journeys.**

The product should help users feel unusually clear, confident, and assisted before buying a car. It must answer factual questions, but the deeper goal is to reduce buyer confusion, variant regret, price uncertainty, and weak lead quality.

---

## Newly Locked Intelligence / UX Scope

### 1. Chat Bar Autosuggest

Status: **Locked — Not started**

Autosuggest should be added after the core live route and answer quality stabilize.

Allowed autosuggest types:

- DB-backed brand suggestions
- DB-backed model suggestions
- DB-backed variant suggestions
- DB-backed feature suggestions
- Contextual next-action suggestions
- Later: personalized / buyer-memory suggestions

Explicitly excluded:

- Generic question-template suggestions

Hard rules:

- No Gemini/LLM call on every keystroke
- No hardcoded factual car/model/variant/feature data
- Suggestions must come from DB/read models/catalogs
- Must be prewarmed/cached
- Must be extremely fast
- Must avoid stale-context pollution
- Must stay visually clean and not feel like a form-heavy dropdown

Suggested implementation stage:

1. Build backend suggestion index/read model
2. Add fast suggestion API
3. Add frontend chat bar dropdown
4. Add click tracking later for learning engine

Potential backend artifacts:

- `aci_chat_suggestion_index`
- `/api/aci-assist/suggestions?q=...`
- Suggestion types: `brand`, `model`, `variant`, `feature`, `context_action`

---

### 2. Visible Context Chip Near Chat Bar

Status: **Locked — Not started**

Purpose:

- Show current selected car/variant/city/session context near the chat bar
- Allow user to clear/change context
- Prevent stale context confusion

Example:

`Context: Hyundai Verna SX IVT · Delhi`

This should be backed by structured context state, not just frontend display state.

---

### 3. Smart Disambiguation / Clarification Cards

Status: **Locked — Not started**

Purpose:

- When a query is ambiguous, ACI should not silently guess.
- It should show a clean clarification card.

Example:

User: `city price`

ACI should ask whether the user means:

- Honda City price
- Price in a specific city

This must be driven by backend confidence/ambiguity signals.

---

### 4. What Changed / What Do I Lose or Gain Engine

Status: **Locked — Partially related work started through variant comparison enrichment**

Purpose:

- Help users understand upgrade/downgrade trade-offs.
- Prevent variant regret.
- Explain feature loss/gain, price delta, and buyer impact.

Example:

`What do I lose if I buy Verna SX instead of SX(O)?`

Expected output:

- Price saved
- Features lost
- Features gained
- Comfort/safety impact
- Regret-risk note
- Neutral recommendation

This should build on the structured variant comparison enrichment already started.

---

### 5. Session-Level Buyer Intent Memory

Status: **Locked — Not started**

Purpose:

ACI should remember buyer preferences within the active session, such as:

- Budget
- City
- Fuel preference
- Transmission preference
- Use case: family, city driving, highway, business, chauffeur-driven, etc.
- Safety priority
- Comfort priority
- Shortlisted vehicles
- Compared vehicles
- Lead readiness

This should not initially be creepy long-term memory. Start with session memory only.

---

### 6. Missing-Data Honesty / Source-Confidence Layer

Status: **Locked — Not started**

Purpose:

ACI should clearly distinguish:

- Verified data
- Partial data
- Missing data
- Low-confidence matches
- Fallback answers
- Gemini-polished language vs DB-backed facts

This is critical for trust.

Bad:

`No data found.`

Better:

`I could verify price and variant data, but feature coverage is incomplete for some variants.`

---

### 7. No-Result Recovery

Status: **Locked — Not started**

Purpose:

ACI should not dead-end when exact data is unavailable.

Instead of only saying no data found, it should suggest:

- Nearby variants
- Similar models
- Available alternatives
- Broader searches
- Clear next actions

Example:

`I could not verify this exact variant, but I found nearby Verna automatic variants. Do you want those?`

---

### 8. Recently Compared / Shortlist Tray

Status: **Locked — Not started**

Purpose:

ACI should quietly maintain cars the user has compared or shown interest in during the session.

This enables questions like:

`Which one should I finally buy?`

Expected tracked state:

- Recently viewed cars
- Recently compared cars
- Shortlisted cars
- Removed cars
- User priorities attached to shortlist

---

### 9. Internal Confidence / Debug Trace Logging

Status: **Locked — Partially exists through current eval/smoke/progress direction, needs formalization**

Every answer should internally log:

- User query
- Meaning frame
- Intent
- Matched entities
- Selected tool/route
- DB collections used
- Matched row counts
- Confidence
- Missing data flags
- Fallback used or not
- Gemini used or not
- Latency
- Canvas type
- Recovery options
- Lead readiness signals

This should power:

- Progress tracker
- Debug dashboard
- Eval reports
- Failed-query mining
- ACI Learning Engine

---

### 10. Fair Deal / Negotiation Assistant

Status: **Locked — Not started**

Purpose:

Help buyers judge dealer quotes and on-road price fairness.

Example:

`Dealer quoted me 16.8 lakh for Creta SX. Is this fair?`

Expected checks:

- Ex-showroom
- RTO
- Insurance estimate
- Accessories
- Handling charges
- Offers/discounts
- Suspicious add-ons
- Fair payable range
- Negotiation advice

This should only be built after on-road price breakup integrity is stable.

---

### 11. Lead Seriousness Scoring

Status: **Locked — Not started**

Purpose:

Before pushing users into CRM/quotation, ACI should score lead seriousness.

Signals may include:

- Variant-specific price query
- On-road price query
- EMI query
- Dealer quote shared
- City shared
- Shortlisted cars
- Repeated comparison
- Quotation intent
- Offer/availability query
- Finance/insurance query

This should improve CRM quality and avoid junk leads.

---

## ACI Learning Engine Direction

Status: **Locked — Not started**

ACI Learning Engine should be a controlled learning layer, not unsafe self-modifying AI.

It should log:

- Queries
- Meaning frames
- Tool routes
- Answers/canvases
- Matched rows
- Errors
- Clicks
- Follow-ups
- Quotations
- Conversions

Use this dataset for:

- Failed-query mining
- Eval generation
- Recommendation ranking
- Lead seriousness scoring
- Personalization
- Future ML models

Rules:

- Factual answers remain DB/tool-grounded
- No model is allowed to invent prices/features/offers/specs/availability
- Improvements must be offline-trained or versioned
- All learning-driven changes must be eval-tested and rollback-safe

---

## ACI Answer Composer Direction

Status: **Locked — Started**

ACI Answer Composer should remain a separate layer after deterministic DB/tool execution and before frontend rendering.

Purpose:

- Improve buyer-facing language
- Add verdicts
- Add caveats
- Add next actions
- Keep brain/tool execution stable
- Allow future Gemini polishing without destabilizing routing/data correctness

---

## Correct Build Order From Here

### Immediate Priority — Core Correctness

Status: **In progress**

Finish/stabilize:

- ACI Core live route
- Context isolation
- Answer correctness
- Comparison enrichment
- On-road breakup integrity
- Answer Composer v1
- 100-question customer corpus
- Progress tracker updates
- Frontend debug-renderer decision separately

---

### Next — Foundational Intelligence Layer

Status: **Planned**

Build backend contracts for:

- Trace logging
- Confidence/missing-data flags
- Clarification/disambiguation
- No-result recovery
- Context state
- Recovery options
- Structured next actions

Frontend should render these only after backend contract exists.

---

### Then — UX Intelligence Layer

Status: **Planned**

Build:

- Context chip
- Autosuggest backend
- Autosuggest frontend
- Session buyer memory
- Recently compared / shortlist tray
- Contextual next actions

---

### Then — Decision / Conversion Layer

Status: **Planned**

Build:

- What-do-I-lose/gain engine
- Fair deal / negotiation assistant
- Lead seriousness scoring
- Quotation/CRM handoff
- Later WhatsApp handoff through same core

---

## Product Principle Locked

Do not build these as isolated UI cards.

Build the backend intelligence contract first:

- `contextState`
- `confidence`
- `clarification`
- `missingData`
- `recoveryOptions`
- `decisionInsights`
- `shortlist`
- `leadScore`
- `suggestedNextActions`
- `trace`

Then let frontend render premium UI from structured data.

This protects ACI Assist from becoming a pile of features and keeps it as a serious buyer decision-intelligence product.
<!-- ACI_LOCKED_SCOPE_2026_05_28_END -->


---

<!-- ACI_PREMORTEM_GEMMA_LOCK_2026_06_08_START -->

## ACI Assist Pre-Mortem Guardrails & Local Gemma Layer — Locked

### Status
Planned / locked into roadmap. Not production implemented yet.

### Decision taken
The ACI Assist pre-mortem is now a governing product and engineering guardrail. It must be used before further broad module work, especially decision intelligence, SEO scale, and monetization systems.

The local Gemma / TranslateGemma layer is also locked as a planned understanding-language layer, not as a factual car-truth source.

### Pre-mortem guardrail scope
The guardrail system must prevent:

- wrong or stale DB facts
- slow exact-price and core flows
- generic chatbot behavior
- hardcoded vehicle, variant, persona, market, or scoring logic
- hidden unsupported-city fallback
- LLM/Gemma/Gemini becoming factual source of truth
- recommendation pollution by ads or dealer monetization
- stale progress tracker states
- weak SEO utility pages
- weak mobile buyer journey
- weak lead conversion timing

### Required pre-mortem implementation items

- no-hardcoded vehicle fact audit
- unsupported-city honesty audit
- price / feature / color / spec trace metadata
- stale/missing data visibility
- buyer journey evals
- progress registry fallback prevention
- full audit and safety gate before production-ready status

### Local Gemma / TranslateGemma scope

Gemma may be used for:

- Hindi/Hinglish semantic understanding
- translation / rewriting if TranslateGemma performs better
- typo/context interpretation
- intent and meaning-frame extraction
- clarification phrasing support

Gemma must not own:

- price
- feature availability
- variant ranking
- sponsored logic
- safety judgment
- offer validity
- dealer routing
- city support
- final factual car claims

All extracted models, variants, cities, features, and buyer intents must be validated against DB/read-models/tools before execution.

### Correct sequence from here

1. Finish current no-data/spec cleanup and keep full bridge/public audits green.
2. Implement pre-mortem guardrail audits and trace requirements.
3. Run core buyer journey evals.
4. Build a small local Gemma/TranslateGemma POC behind an env flag.
5. Only then continue broader decision intelligence modules.

### Linked documents

- `docs/aci-assist/ACI_ASSIST_PREMORTEM.md`
- `docs/aci-assist/ACI_DECISION_MODULE_PRODUCTION_PLAN_V1.md`
- `docs/aci-assist/EVAL_AND_QA_PLAN.md`
- `docs/aci-assist/SECURITY_PRIVACY_AND_GUARDRAILS_PLAN.md`

<!-- ACI_PREMORTEM_GEMMA_LOCK_2026_06_08_END -->

<!-- ACI_LAUNCH_DISTRIBUTION_REVENUE_2026_05_30_START -->

## Launch Distribution, Social Presence & Revenue Systems — Locked

### Status
Planned / locked into roadmap. This must not be missed during backend/frontend freeze planning.

### Launch distribution and trust setup
- Instagram page setup.
- Instagram first launch / awareness post for ACI Assist.
- Facebook page setup.
- Google Business Profile setup.
- Relevant local/business/listing profiles where useful.
- WhatsApp Business presence.
- WhatsApp green tick / official business account eligibility tracking.
- Consistent brand/business identity across website, social pages, Google profile, WhatsApp, and listings.
- Launch-time content/distribution checklist before public release.

### Revenue models to support later
- Consent-based dealer/showroom lead monetization.
- Lead routing by pincode, city, brand, model, variant, and customer intent.
- Lead seriousness / quality scoring before showroom handoff.
- CRM/dealer partner routing readiness.
- Lead delivery logs and audit trail.
- Billing / monetization records for sold leads.
- Sponsored brand banner campaigns.
- Time-bound launch/campaign banner inventory, for example a brand buying banner space for a few days during a new-car launch.
- Sponsored placement targeting, caps, approval workflow, disclosure, and tracking.
- Impression, click, lead, and conversion tracking.
- Strict separation from ACI Assist’s neutral recommendation brain.

### Product rule
Sponsored placements and revenue systems must never manipulate organic answers, recommendations, rankings, comparisons, price verdicts, safest/best-value advice, or buyer decision logic. Paid inventory must be clearly labeled, auditable, capped, and separated in backend contracts and UI.

<!-- ACI_LAUNCH_DISTRIBUTION_REVENUE_2026_05_30_END -->

---

## Backend Decision Intelligence Read Models — Locked Direction

**Status:** Planned / architecture locked, implementation not started.

### Decision taken
ACI Assist will not use hardcoded persona/scoring shortcuts. Recommendation, safety, quickest, value, similar cars, and variant-upgrade intelligence must be data-driven and precomputed before live chat.

### Required read models
- `aci_vehicle_variant_decision_profile`
- `aci_vehicle_variant_city_price_profile`
- `aci_vehicle_model_decision_profile`
- `aci_vehicle_model_city_price_profile`
- `aci_vehicle_editorial_profile`
- `aci_vehicle_variant_upgrade_ladder`
- `aci_vehicle_similar_cars_graph`
- `aci_vehicle_score_config`

### Locked principles
- Variant-wise scoring is mandatory for safety, features, value, performance/quickest, mileage/running cost, practicality, family fit, city/highway use, comfort/premium feel, and regret risk.
- Model-level rankings must store the exact variant that caused the model to rank high.
- City-specific duplication should be avoided. Features, safety, persona, scores, upgrade logic, and similar-car intelligence should be global; only price and availability should be city overlays.
- Variant upgrade logic must compare the next meaningful same-fuel/same-transmission equipment step.
- Dual-tone, cosmetic-only, and special-colour variants must be skipped in upgrade comparisons unless explicitly requested.
- Persona fields such as idealFor, skipIf, strengths, compromises, bestUseCases, buyerTypeTags, and regretWarnings must be curated/source-backed for every model. These must not be invented live or hardcoded through generic body-type rules.

### Next work
1. Gather all required fields from current DB collections.
2. Audit missing data for safety, quickest/performance, mileage, dimensions, feature evidence, and market perception.
3. Design final schemas.
4. Build offline read-model builders.
5. Replace slow live recommendation path with fast profile reads.
6. Re-run 40-query customer corpus.

---

<!-- ACI_DECISION_MODULE_PRODUCTION_PLAN_V1_2026_06_04_START -->

## ACI Decision Module Production Plan v1 - Locked

**Status:** Phase 0 locked / implementation not started.

### Decision taken

Do not continue to cross-model scoring or score wording cleanup yet. First build and lock the Decision Module Production Policy and Contracts layer.

External architecture review correctly exposed missing production controls: decision policy, strict output schema, provenance/freshness, degraded-mode taxonomy, eval harnesses, market-judgment isolation tests, and final-recommendation blocking.

### Correct order from here

1. Decision policy + contracts + eval framework.
2. Current score insight cleanup.
3. Dedicated cross-model candidate resolver service.
4. Cross-model diagnostic score insight.
5. Evidence/freshness/degraded-mode hardening.
6. Final recommendation eligibility engine.
7. Final answer composer / centralized language layer.

### Production rules

- Every claim must be traceable to data.
- Missing data must be disclosed.
- Facts, diagnostic scores and opinions must stay separate.
- No final buy-this verdict unless Decision Policy allows it.
- No hardcoded car, variant, rival, persona or market-judgment logic.
- No ad/sponsored/revenue influence on recommendation brain.
- Empty result must not pass as success.
- Route success is not enough; output must contain useful rows/evidence or a clear degraded-mode state.
- Tools return structured facts/signals; central composer/language layer handles wording later.

### Minimum inputs before final recommendation

Final recommendation must require city, budget/price ceiling, body/use-case preference, family size or occupancy use, fuel preference or monthly running, transmission preference, safety priority, feature priority, and shortlisted models or discovery scope.

### Phase 0 deliverable

Primary plan doc:

docs/aci-assist/ACI_DECISION_MODULE_PRODUCTION_PLAN_V1.md

<!-- ACI_DECISION_MODULE_PRODUCTION_PLAN_V1_2026_06_04_END -->

---

<!-- ACI_NO_HARDCODED_FACTS_GUARDRAIL_2026_06_08_START -->

## Guardrail Update — No-Hardcoded Vehicle Facts Audit Active

### Status
Done / safety-gated.

### What changed
The no-hardcoded vehicle facts audit has been implemented and wired into `aci:safety:fast`.

### Locked protection
The safety gate now blocks high-risk runtime patterns such as:

- model-specific factual routing branches
- hardcoded vehicle/variant candidate objects used to force buyer answers
- hardcoded typo aliases that convert ambiguous text into factual vehicle entities
- hardcoded feature-explorer model whitelists

### Recent cleanup completed
Removed committed runtime hardcodes including:

- Creta King special feature routing
- Verna SX special feature routing
- `scorpion -> Scorpio N` hard alias
- Scorpio N score-routing hardcoded candidate branch

### Latest verified baseline
- `aci:no-hardcoded-facts:audit`: passing
- `aci:safety:fast`: passing
- full buyer deep audit baseline remains accepted with 9 honest no-data cases

### Remaining guardrail work
Next pre-mortem guardrails:

1. trace metadata coverage for price / features / colors / specs / comparisons
2. unsupported-city honesty audit hardening
3. progress registry stale/fallback prevention
4. core buyer journey evals
5. then local Gemma/TranslateGemma POC

<!-- ACI_NO_HARDCODED_FACTS_GUARDRAIL_2026_06_08_END -->

---

<!-- ACI_FACTUAL_TRACE_METADATA_GUARDRAIL_2026_06_08_START -->

## Guardrail Update — Factual Trace Metadata Audit Active

### Status
Done / safety-gated.

### What changed
A factual trace metadata audit has been implemented for the public ACI Assist buyer-answer route.

### Coverage verified
The audit verifies trace/source coverage for:

- price/pricelist answers
- unsupported-city price answers
- feature lookup answers
- feature explorer/context feature answers
- color answers
- spec/attribute answers
- comparison answers

### Contract now enforced by audit
Factual outputs must expose:

- `sourceTransparency`
- `runtimeResultsMeta`
- source/read-model collection signals
- record or match count signals

### Comparison metadata normalization added
Comparison responses now include read-model provenance for:

- `aci_vehicle_price_rows`
- `vehicle_variant_feature_matrix_v2`

### Latest verified baseline
- `aci:factual-trace:audit`: 7/7 passing
- targeted comparison audit: 12/12 passing
- full buyer deep audit: 185/185 passing
- `aci:safety:fast`: passing
- accepted no-data baseline remains 9 honest no-data cases

### Safety gate status
Wired into `aci:safety:fast` in strict bridge mode. The audit does not depend on a manually running localhost server.

<!-- ACI_FACTUAL_TRACE_METADATA_GUARDRAIL_2026_06_08_END -->
---

<!-- ACI_PROGRESS_STATUS_VALUES_FIX_2026_06_08_START -->

## Progress Tracker Status Correction — Pre-Mortem Guardrails

The progress registry has been corrected so completed pre-mortem guardrail items use UI-recognized `ready` status instead of non-standard `done` status.

### Marked ready
- Pre-mortem plan document
- No-hardcoded vehicle fact audit
- Factual trace metadata audit
- Price / feature / color / spec trace metadata

### Module status
The overall pre-mortem guardrail module remains `partial`, because unsupported-city honesty, no-data/data-quality review, core buyer journey evals, and progress fallback/staleness prevention are still pending/planned.

<!-- ACI_PROGRESS_STATUS_VALUES_FIX_2026_06_08_END -->
---

<!-- ACI_UNSUPPORTED_CITY_SAFETY_GUARDRAIL_2026_06_08_START -->

## Guardrail Update — Unsupported-City Honesty Safety-Gated

### Status
Done / safety-gated.

### What changed
Unsupported-city pricing honesty is now protected by a dedicated bridge-mode audit.

### Coverage verified
The audit checks that unsupported cities do not silently fall back to New Delhi pricing.

Unsupported city coverage includes:
- Mumbai / Bombay
- Bangalore / Bengaluru
- Jaipur
- Pune
- Chennai
- Hyderabad
- Kolkata
- Ahmedabad
- Chandigarh
- Faridabad
- Ghaziabad

Supported city coverage includes:
- New Delhi / Delhi
- Noida
- Gurgaon / Gurugram

### Context protection added
Follow-up price questions now respect unsupported city context, including cases like:
- `same in Mumbai`
- `price in Mumbai`
- `price there` with Mumbai in context

### Latest verified baseline
- `aci:unsupported-city:audit`: 23/23 passing
- targeted unsupported-city deep audit: 9/9 passing
- full buyer deep audit: 185/185 passing
- `aci:safety:fast`: passing with unsupported-city audit wired

<!-- ACI_UNSUPPORTED_CITY_SAFETY_GUARDRAIL_2026_06_08_END -->
---

<!-- ACI_NO_DATA_BASELINE_GUARDRAIL_2026_06_08_START -->

## Guardrail Update — No-Data Baseline Audit

### Status
Done / baseline locked.

### What changed
A strict no-data baseline audit has been added.

### Current accepted no-data baseline
The full 185 buyer-answer audit currently has exactly 10 accepted no-data answers:

- 4 expected unsupported-city pricing cases
- 1 valid negative color result
- 1 known score-data gap
- 4 expected pending module cases

### Guardrail behavior
The audit fails if:
- a new unexpected no-data answer appears
- a known expected no-data case disappears without review
- feature/spec/comparison read-model gaps reappear
- deep audit summary is not clean
- a no-data answer changes wording away from the expected honest/unavailable framing

### Latest verified baseline
- `aci:no-data:baseline:audit`: 10/10 expected no-data cases passing
- full buyer deep audit: 185/185 passing
- `aci:safety:fast`: passing

### Wiring decision
This should be wired into a freeze/full gate, not `aci:safety:fast`, because it depends on a full 185 audit log.

<!-- ACI_NO_DATA_BASELINE_GUARDRAIL_2026_06_08_END -->
---

<!-- ACI_NO_DATA_FREEZE_GATE_WIRING_2026_06_08_START -->

## Guardrail Update — No-Data Baseline Wired Into Freeze Gate

### Status
Wired into freeze/full gate and passing.

### What changed
The no-data baseline freeze gate now runs a fresh full 185 buyer-answer deep audit, verifies JSON markers, and then validates the strict 10-case no-data baseline against that exact fresh log.

### Verified
- `aci:no-data:freeze-gate`: passing
- `ACI No-Data Baseline Audit v1`: passing
- Expected no-data count: 10
- Actual no-data count: 10
- Failed no-data baseline IDs: none

### Important note
Overall `aci:safety:freeze` is still not clean because of pre-existing failures outside the no-data guardrail:
- `contract-internal-private`
- `punch-sunroof-and-adas`
- `punch-seven-feature-bundle`
- `punch-variant-sunroof-and-adas`
- `backendFreezeTrust`

Next cleanup should start with `contract-internal-private`.

<!-- ACI_NO_DATA_FREEZE_GATE_WIRING_2026_06_08_END -->
---

<!-- ACI_BACKEND_FREEZE_GREEN_2026_06_08_START -->

## Guardrail Update — Backend Freeze Safety Gate Green

### Status
Done for backend-only scope.

### Verified backend gates
- `aci:safety:fast`: passing
- `aci:safety:freeze`: passing
- Contract foundation: 7/7 passing
- Vehicle entity index audit: 7/7 passing
- Multi-feature query audit: 2/2 passing
- Variant multi-feature query audit: 1/1 passing
- Feature comparison query audit: 1/1 passing
- Context switch audit: passing
- Backend freeze trust audit: 7/7 passing
- No-data baseline freeze gate: passing
- Full buyer deep audit inside no-data freeze gate: 185/185 passing
- No-data baseline: expected 10, actual 10, failed IDs none

### Fixes included before this green state
- Internal CDrive/private query `Loan closure 7077` now routes to `internal_passthrough` instead of public clarification.
- Multi-feature questions now route before live bridge when they are true multi-feature checks.
- Turbo/turbocharged budget discovery now preserves `turbo charger` as a must-have feature and uses `feature_match_builder_canvas` instead of silently returning broad budget recommendations.
- Unsupported-city price flows remain honest and do not silently fall back to Delhi/New Delhi.

### Scope note
Frontend/UI evaluation is intentionally excluded because frontend is not ready. Current work remains backend-only until frontend work is explicitly resumed.

### Next backend step
Build core buyer journey backend evals. These should test backend response correctness, routing, context, trace, and honest degraded states only — no frontend rendering or UI assessment.

<!-- ACI_BACKEND_FREEZE_GREEN_2026_06_08_END -->
---

<!-- ACI_BACKEND_BUYER_JOURNEY_GREEN_2026_06_08_START -->

## Guardrail Update — Core Buyer Journey Backend Audit Green

### Status
Done for backend-only scope.

### Verified
- `aci:buyer-journey:backend:audit`: passing
- Total journeys: 6
- Total steps: 17
- Failed journeys: 0
- Failed steps: 0
- `aci:safety:fast`: passing
- `aci:safety:freeze`: passing
- No-data baseline freeze gate: passing
- Full buyer deep audit inside no-data freeze gate: 185/185 passing

### Regression now covered
- `creta price in mumbai` returns honest `unsupported_city_canvas` and preserves `Hyundai Creta` in context.
- Follow-up `delhi price` uses preserved vehicle context and returns New Delhi price rows.
- Supported → unsupported → supported city flow remains covered.
- Unsupported-first → supported city recovery is now covered.

### Scope note
Frontend/UI evaluation remains excluded. This is backend-only.

### Next backend quality issue
Add answer-label hygiene audit. Current logs show non-blocking but visible label issues such as duplicated comparison titles and duplicated make/model labels. These should be caught by backend audits before frontend resumes.

<!-- ACI_BACKEND_BUYER_JOURNEY_GREEN_2026_06_08_END -->
---

<!-- ACI_ANSWER_LABEL_HYGIENE_READY_2026_06_08_START -->

## Guardrail Update — Answer Label Hygiene Audit Ready

### Status
Done for backend-only scope.

### Verified
- `aci:answer-label-hygiene:audit`: passing
- Total label hygiene cases: 5
- Failed cases: 0
- `aci:buyer-journey:backend:audit`: passing
- `aci:safety:fast`: passing
- `aci:safety:freeze`: passing

### Regression now covered
- Comparison follow-ups no longer duplicate the same comparison pair.
- `Creta vs Seltos → price difference` now remains `Kia Seltos vs Hyundai Creta` with 2 comparison rows.
- `Creta vs Seltos → which one?` now remains `Kia Seltos vs Hyundai Creta` with 2 comparison rows.
- Unsupported-first pricing label remains clean: `Hyundai Creta pricing unavailable in Mumbai`.
- Spec label hygiene remains covered for `Tata Punch mileage`.

### Scope note
Frontend/UI evaluation remains excluded. This is backend-only.

### Next backend guardrail
Progress registry fallback/staleness prevention.

<!-- ACI_ANSWER_LABEL_HYGIENE_READY_2026_06_08_END -->
---

<!-- ACI_PROGRESS_REGISTRY_GUARD_READY_2026_06_08_START -->

## Guardrail Update — Progress Registry Guard Ready

### Status
Done for backend-only scope.

### Verified
- `aci:progress-registry:guard`: passing
- Guard runtime: ~15–16ms
- `aci:safety:fast`: now runs progress registry guard before the main safety runner
- `aci:safety:fast`: passing
- Progress snapshot source: `live_registry`
- `fallbackUsed`: false
- `meta.registryIntegrity.ok`: true
- Registry module count: 30
- Duplicate module IDs: 0
- Missing required modules: 0
- Invalid filtered modules: 0

### Regression now covered
- Progress API cannot silently hide an invalid registry shape.
- Empty module arrays, duplicate module IDs, missing required modules, invalid statuses, missing item keys, and fallback/stale-source states now fail audit.
- Progress snapshot exposes `source`, `registrySource`, `fallbackUsed`, `generatedAt`, and `meta.registryIntegrity`.

### Scope note
Frontend/UI evaluation remains excluded. This is backend-only.

<!-- ACI_PROGRESS_REGISTRY_GUARD_READY_2026_06_08_END -->
---

<!-- ACI_EXACT_PRICE_PERFORMANCE_READY_2026_06_08_START -->

## Guardrail Update — Exact Supported-City Price Performance Audit Ready

### Status
Done for backend-only scope.

### Verified
- `aci:exact-price-performance:audit`: passing
- Total exact price cases: 8
- Passed cases: 8
- Failed cases: 0
- Average duration: ~924ms
- Max duration: ~964ms
- `aci:safety:fast`: passing

### Regression now covered
- `Creta SX on-road price Delhi`
- `Creta SX price New Delhi`
- `Creta SX on road Gurgaon`
- `Creta SX price Noida`
- `Hyundai Creta SX on-road price Delhi`
- `Seltos HTE on-road price Delhi`
- `Kia Seltos HTE price Gurgaon`
- `Tata Punch Adventure S price Delhi`

### Required fast-path guarantees
- `contextIsolation`: `supported_exact_price_fast_path`
- `tool`: `vehicle_pricelist`
- `planMode`: `single_tool`
- `usedGemini`: false
- Exact variant queries return exactly one DB-backed price row.
- Supported pricing cities stay limited to New Delhi, Noida, and Gurgaon.

### Scope note
Frontend/UI evaluation remains excluded. This is backend-only.

<!-- ACI_EXACT_PRICE_PERFORMANCE_READY_2026_06_08_END -->

---

## 2026-06-09 — Plain Comparison Fast Path Stabilized

Status: **Completed and pushed**

Commits:
- `8277084` — Optimize plain vehicle comparison read path
- `099dd8c` — Add explicit comparison fast path in live bridge

### What was fixed

Plain two-car comparison queries now bypass the heavy live bridge candidate/planner route when the query is a clean explicit comparison.

Examples verified:
- `Creta vs Seltos`
- `Grand Vitara vs Hyryder`
- `Hyryder vs Grand Vitara price difference`
- `compare Creta and Seltos`

The live bridge now routes these through:

- `contextIsolation: explicit_comparison_fast_path`
- `comparisonResolutionMode: direct_comparison_read_model`
- `tool: vehicle_compare`
- `canvasType: comparison_canvas`

### Performance impact

Before:
- Cold/noisy `Creta vs Seltos` path could take around 10–13s.
- Plain comparisons were paying heavy candidate/context/planner cost.
- Some runs still fell through slower fallback-style comparison shaping.

After:
- Explicit plain comparison smoke showed approximately 0.8–3.8s.
- `compare Creta and Seltos` verified at under 1s in post-commit smoke.
- `Grand Vitara vs Hyryder` and `Hyryder vs Grand Vitara price difference` now use the same direct read-model path.

### Correctness / non-hijack checks

Verified that the explicit comparison fast path does **not** hijack:

- Feature comparison:
  - `Creta vs Seltos sunroof`
  - remains `feature_comparison_canvas`
  - remains `standalone_model_feature_comparison_fast_path`

- Unsupported city pricing:
  - `Seltos price Mumbai`
  - remains `unsupported_city_canvas`
  - remains `unsupported_city_fast_path`

- Cross-model score diagnostic:
  - `Creta vs Seltos diagnostic score comparison`
  - remains `vehicle_score_insight`
  - remains `cross_model_score_diagnostic_fast_path`

### Verified gates

Before commit:
- Provenance envelope audit: `7/7 passed`
- No-data freeze gate: `185/185 passed`
- Safety fast gate: passed
- No-hardcoded vehicle facts audit: passed
- Unsupported city honesty audit: `23/23 passed`
- Progress registry guard: live registry, fallback not used

### Data-backed price delta verification

The direct comparison path was verified against `aci_vehicle_price_rows`:

- Kia Seltos HTE New Delhi on-road: ₹13,33,591
- Hyundai Creta E New Delhi on-road: ₹12,79,355
- Delta: ₹54,236
- Cheaper: Hyundai Creta E

- Toyota Hyryder E New Delhi on-road: ₹13,34,691
- Maruti Grand Vitara Sigma New Delhi on-road: ₹12,47,209
- Delta: ₹87,482
- Cheaper: Maruti Grand Vitara Sigma

### Remaining backend latency items

This does not close all comparison-adjacent latency. Remaining separate tasks:

1. Feature comparison latency
   - Example: `Creta vs Seltos sunroof`
   - Post-commit smoke: around 5.4s

2. Cross-model score diagnostic latency
   - Example: `Creta vs Seltos diagnostic score comparison`
   - Post-commit smoke: around 7.1s

3. Broader post-commit backend freeze
   - Run a heavier backend freeze only after the next small consolidation pass, not during every micro-patch.

### Guardrail

Do not broaden `explicit_comparison_fast_path` without re-running:
- feature-comparison non-hijack check
- unsupported-city non-hijack check
- score-diagnostic non-hijack check
- no-data freeze gate

---

## 2026-07-10 - Context Roller-Coaster, ABS and Conditional Decisions

Status: **Backend context scope ready; overall ACI Assist backend not launch-complete**

### Completed in this pass

- ABS is classified as a feature in the deterministic direct-fact and feature routes.
- Feature names and acronyms are removed from legacy variant slots before DB queries.
- Model-level feature switches clear inherited variant and comparison state.
- The frontend chat transport preserves `contextLedger`, `buyerContext` and `buyerGuidanceContext` instead of dropping them between turns.
- Active comparisons support feature follow-ups such as `which has abs` and `which has sunroof`.
- Contextual `vs creta` and relative `last vs thar roxx` comparisons use the DB-backed direct comparison path.
- Comparison answers no longer expose `indexed feature/spec differences` wording.
- Feature and comparison catalogs are prewarmed for warm-turn latency.
- Conditional decision guidance is active for comparison-scoped final-choice questions with complete buyer context. It gives a practical lean and mismatch caveats while keeping absolute final-recommendation flags disabled.
- Three unused tracked legacy `.bak` files were removed after import/reference checks.

### Verified

- Frontend-contract roller-coaster: 13/13 passed; latest median 512 ms and p95/max 1,013 ms after warm-up.
- Context Phase1Co2 gate: passed.
- Context stress audit: passed.
- Decision Phase0 gate: all 41 tasks passed with the conditional guidance smoke included.
- Safety fast gate: passed with 72 pass mentions and 0 failures.
- Conditional comparison decision smoke: passed; final flags remain false.
- Frontend production build: passed.
- Repository-wide backend JavaScript syntax scan: all 506 `.js`, `.cjs` and `.mjs` files passed.

### Honest readiness

`context_manager_v1` can be marked `ready` for the current backend and frontend transport contract. A deployed multi-instance/session soak test is still required before claiming infrastructure-level persistence.

The Decision Policy module is `mostly_ready`. Conditional guidance is active, but an absolute `buy this car` verdict remains blocked until matched fuel/transmission candidates and trustworthy service-network, ownership-cost, resale and current-market evidence are available.

The controlled ACI Learning Engine remains `planned`. It was not silently activated because production learning requires consent, retention policy, versioned offline training/evaluation and rollback controls. Autosuggest, visible context controls, shortlist persistence, lead/CRM workflows, offers freshness, feature explainer, fair-deal assistance and broader language coverage also remain incomplete.

The backend should not be labelled globally launch-ready yet. The reported six-turn ABS/context failure is fixed and regression-gated, but the remaining modules above are real product blockers rather than code-cleanup leftovers.
