# ACI Assist Pre-Mortem Plan

## Core Assumption

Assume ACI Assist launches, receives initial user interest, but fails to retain users or convert serious leads.

ACI Assist will not fail because other portals have more pages. It will fail if users stop trusting it, answers are slow or wrong, or the product feels like another generic chatbot.

## Non-Negotiable Product Promise

Ask anything before buying a car. Get a clear, correct, practical answer.

ACI Assist must not become:
- a generic chatbot
- a listing portal
- an ad-influenced recommender
- a hallucinated car advice engine

## Top Failure Modes

### 1. Wrong prices, variants, features, or unsupported city answers

Prevention:
- DB-backed facts only
- no hidden city fallback
- strict supported-city contract
- no answer without source-backed data
- missing data must be stated clearly
- LLM can interpret, but must not invent car facts
- every factual answer should have internal trace metadata

Required systems:
- price data freshness audit
- variant resolution audit
- unsupported city audit
- no-hardcoded vehicle fact audit
- price/feature/color/spec answer trace
- stale/missing data audit

Launch gate:
- exact price evals
- ambiguous variant evals
- unsupported-city evals
- variant typo evals
- follow-up context evals

### 2. Generic chatbot behavior

Prevention:
- variant-wise decision profiles
- model decision profiles
- variant upgrade ladder
- buyer context memory
- regret-prevention engine
- what-do-I-gain/lose engine
- transparent opinion/verdict layer
- final recommendation mode

ACI must answer:
- which exact variant makes sense
- what the buyer gains by upgrading
- what the buyer loses by choosing cheaper
- what the buyer may regret later
- whether to buy, wait, negotiate, or avoid

### 3. Slow core flows

Prevention:
- exact price fast path
- unsupported city fast path
- deterministic routing before heavy planning
- prewarmed model/variant/city indexes
- timeout-controlled planner fallback
- per-step latency trace

Targets:
- unsupported city price: under 100ms
- exact supported-city price: under 1.5s
- model price list: under 2s
- feature lookup: under 1.5s
- color lookup: under 1.5s
- known-car comparison: under 3s
- budget discovery: under 3s
- verdict/opinion answer: under 4s

### 4. Scattered product instead of one buying journey

Core loop:
Ask → Understand → Show facts → Explain trade-offs → Recommend next action → Capture lead/quotation

Primary journeys:
1. Known car price journey
2. Variant price list journey
3. Compare journey
4. Budget discovery journey
5. Confused buyer recommendation journey
6. Quote/lead capture journey

Every module must support this loop.

### 5. Recommendation pollution by revenue systems

Rule:
Neutral Decision Brain ≠ Sponsored Campaign System ≠ Dealer Lead Routing

Sponsored systems must never alter:
- verdict
- comparison outcome
- variant recommendation
- ranking logic
- regret warning
- safety/value judgment

ACI can sell attention and leads. ACI must not sell the answer.

### 6. Data incompleteness or staleness

Every data area needs visible status:
- prices: fresh / stale / missing
- features: complete / partial / missing
- colors: complete / partial / missing
- offers: active / expired / unknown
- safety: verified / unavailable
- mileage: certified / owner-reported / unavailable
- variant decision profile: curated / pending
- similar cars graph: generated / reviewed / stale

Required systems:
- data coverage dashboard
- stale-data alerts
- source confidence per collection
- no-data answer templates
- admin review queue

### 7. Weak SEO utility

SEO pages must be useful, not thin pages.

Required page types:
- model price in city
- variants
- colors
- compare pages
- best cars by budget
- cars with feature
- EMI calculator pages

Each page needs:
- structured data
- DB-backed facts
- honest unsupported-city handling
- internal links
- FAQ
- ACI Assist CTA
- quote CTA
- freshness/status note

### 8. UI looks premium but feels slow/confusing

UI principles:
- chat-first
- embedded cards
- mobile-first
- premium white/glass
- ACI blue CTAs
- no dark/loud theme
- no permanent split-screen
- no dummy data
- clear context chip
- clear full-detail open action
- quote/compare/EMI/view variants CTAs only when useful

### 9. LLM becomes source of truth

Architecture rule:
LLM = parser / language / clarification / explanation
DB tools = facts
Decision engine = scoring/trade-offs
Composer = final buyer answer

LLM/Gemma/Gemini must not own:
- price
- feature availability
- variant ranking
- sponsored logic
- safety judgment
- offer validity
- dealer routing
- city support

Every important answer must be reproducible from structured facts.

### 10. Progress tracker becomes decorative

Progress tracker must show:
- module
- status
- production readiness
- data dependency
- test coverage
- known blockers
- next action
- owner/source file
- last verified date

Status levels:
- not_started
- planned
- in_progress
- coded
- smoke_passed
- eval_passed
- production_ready
- blocked
- deprecated

A feature is not done because code exists. It is done only when data, backend contract, frontend state, evals, and fallback behavior are verified.

### 11. Weak business conversion

Lead capture must happen after value delivery.

Best lead moments:
- after exact price answer
- after comparison verdict
- after variant recommendation
- after EMI affordability answer
- after final recommendation
- after offer/discount discussion
- after user asks for quote/deal/where to buy

Required lead fields:
- name
- phone
- city
- pincode
- model
- variant
- budget
- buying timeline
- exchange intent
- finance intent
- seriousness score
- source query
- recommendation trace

### 12. Competitors copy the surface

Moat is not AI chat.

Moat is:
car data + decision logic + buyer journey + conversion system + trust

Defensibility should come from:
- structured car intelligence
- variant-wise trade-off engine
- regret-prevention
- buyer memory
- city-specific pricing integrity
- dealership/lead routing
- usage analytics
- programmatic SEO utility
- trust and neutrality

## Risk Register

| Risk | Severity | Probability | Prevention |
|---|---:|---:|---|
| Wrong price/variant answer | Critical | High | DB-only facts, trace, audits |
| Slow exact queries | Critical | High | Fast paths, prewarm, planner bypass |
| Generic chatbot feel | Critical | Medium | Decision engine, regret logic |
| Hardcoded logic creeps in | Critical | High | no-hardcode audit, review gate |
| City fallback mistakes | Critical | Medium | strict supported-city contract |
| Data staleness | High | High | freshness dashboard |
| Recommendation bias from ads | Critical | Medium | hard separation + audit |
| Weak SEO pages | High | Medium | useful templates + structured data |
| Poor mobile UX | High | Medium | mobile-first QA |
| Lead conversion weak | High | Medium | contextual CTA + CRM flow |
| Scope creep | High | High | launch journey gates |
| LLM hallucination | Critical | Medium | deterministic tools + composer |
| Progress tracker stale | Medium | High | registry discipline |

## Immediate Priority Order

### Phase 1: Trust and correctness foundation

1. Exact supported-city price fast path.
2. Unsupported city honest response.
3. Variant resolver accuracy.
4. No-hardcoded-facts audit.
5. Price/feature/color/spec trace logs.
6. Eval set for embarrassing/wrong answers.
7. No-data/data-quality review.

No broad intelligence work before this is stable.

### Phase 2: Core buyer journeys

1. Known car price journey.
2. Variant price list journey.
3. Compare journey.
4. Budget discovery journey.
5. Final recommendation journey.
6. Quote/lead capture journey.

### Phase 3: Decision intelligence

1. Variant decision profile.
2. Model decision profile.
3. Variant upgrade ladder.
4. Similar cars graph.
5. Regret-prevention engine.
6. Buyer persona/use-case matching.
7. Final recommendation mode.

Decision intelligence must be data-backed, not static hardcoded scoring.

### Phase 4: SEO and distribution

1. DB-backed crawlable pages.
2. Sitemap index.
3. Robots.
4. Structured data.
5. Internal linking.
6. Search Console/Bing submission.
7. Instagram/Facebook/Google Business setup.
8. Launch content.

### Phase 5: Monetization

Only after trust layer is safe:
1. Quotation leads.
2. Dealer routing.
3. Lead scoring.
4. Audit logs.
5. Billing records.
6. Sponsored banners.
7. Campaign caps.
8. Strict disclosure.
9. No influence on neutral recommendation brain.

## Public Launch Definition

ACI Assist is public-launch ready only when:

1. Exact price answers are fast and correct.
2. Unsupported cities are handled honestly.
3. Comparison and recommendation flows are genuinely helpful.
4. No hardcoded vehicle facts exist.
5. Major data gaps are known and visible.
6. Mobile chat/card UX is polished.
7. Quote lead flow works.
8. Progress tracker is accurate.
9. SEO pages are useful, not thin.
10. Ads/revenue systems are separated from the neutral recommendation brain.
