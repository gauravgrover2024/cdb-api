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
