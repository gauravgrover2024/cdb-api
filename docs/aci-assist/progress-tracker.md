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
