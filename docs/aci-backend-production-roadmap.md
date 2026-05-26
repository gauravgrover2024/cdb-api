# ACI Assist Backend Production Roadmap

## Mission

Build ACI Assist backend as a production-grade, DB-backed, deterministic, scalable new-car intelligence system for India.

This backend must not become a pile of patches. Every file must have a clear responsibility, tests, source transparency, and no hardcoded automotive facts.

## Non-negotiable Rules

1. No hardcoded automotive facts.
   - No hardcoded models.
   - No hardcoded variants.
   - No hardcoded prices.
   - No hardcoded feature availability.
   - No hardcoded colors as truth.
   - No hardcoded offers.
   - No hardcoded mileage/spec values.
   - No hardcoded body type/transmission/fuel truth.

2. Language synonyms are allowed only for intent parsing.
   Example:
   - "kitna deti hai" may map to mileage intent.
   - "best price" may map to quotation intent.
   But factual answers must come from DB/tools only.

3. Every factual customer answer must be backed by deterministic tools/read models.

4. Every response must carry stable sourceTransparency/dataSource where relevant.

5. Every backend change must pass:
   npm run aci:safety

6. Work on main, but create backup branches before risky refactors.

7. Commit one logical change at a time.

8. No test-drive flows until explicitly reintroduced.

9. No mock/demo/fake vehicle fallback data.

10. Any deprecated old path must be marked and removed only after import checks prove it is unused.

---

## Phase 0 — Control the Process

### Goal
Stop scattered coding. Track every active backend file.

### Deliverables
- docs/aci-backend-production-roadmap.md
- docs/aci-backend-file-audit-tracker.md
- npm run aci:safety must stay green

### Done When
- Roadmap committed.
- Tracker committed.
- Current backend state is green.

---

## Phase 1 — Core ACI Agent Architecture

### 1.1 aiAgent.service.js

Status: Mostly clean as orchestrator after recent cleanup.

Expected responsibility:
- normalize input
- call early fast-path
- call planner
- call executor
- repair context
- normalize final contract
- return response

Must not contain:
- direct DB queries
- direct vehicle tool imports
- hardcoded model/feature/color/price truth
- large routing logic
- early gate internals

Done criteria:
- no direct DB/tool usage
- no early-gate internals
- no hardcoded automotive facts
- under control as orchestration layer
- npm run aci:safety green

### 1.2 aiAgent.earlyFeatureGate.js

Status: Extracted but not production-grade internally.

Problems to solve:
- too many responsibilities in one file
- detection, plan building, tool running, response polishing, and context patching are mixed
- feature/category regexes need classification
- feature truth must remain tool/DB-backed
- language aliases must not become factual truth

Target split:
- aiAgent.earlyFeatureGate.js
- aiAgent.earlyFeatureGate.detector.js
- aiAgent.earlyFeatureGate.planBuilder.js
- aiAgent.earlyFeatureGate.runner.js
- aiAgent.earlyFeatureGate.responsePolisher.js
- aiAgent.earlyFeatureGate.contextPatch.js

Done criteria:
- no model truth hardcoding
- no factual feature truth hardcoding
- aliases only used for intent/tool routing
- tool output remains source of truth
- npm run aci:safety green

### 1.3 aiAgent.modelContextResolver.js

Purpose:
- DB-backed explicit model resolution
- read-model hydration
- no static model fallback

Done criteria:
- no hardcoded car model list
- resolver returns make/model/fullModel reliably
- context switch audit passes
- resolver tests added

### 1.4 aiAgent.contextPriority.js

Purpose:
- decide when explicit message beats old context
- repair final contextPatch
- prevent stale model/variant carryover

Done criteria:
- explicit car switch wins
- compare-with follow-ups do not corrupt selected car
- current variant carries only with pronoun/current variant language
- no automotive factual truth

---

## Phase 2 — Planner and Semantic Routing

### 2.1 aiAgent.plannerRedFixes.js

Blunt assessment:
This file is likely a patch graveyard and must be cleaned carefully.

Risks:
- too many special cases
- duplicate planner logic
- feature/fuel/body/city assumptions
- tests may pass because of patches, not because architecture is clean

Target:
Rename/reframe later as deterministic planner overrides.

Possible split:
- plannerOverrides.intentGuards.js
- plannerOverrides.entityGuards.js
- plannerOverrides.multiIntent.js
- plannerOverrides.lead.js
- plannerOverrides.recommendation.js
- plannerOverrides.explainer.js

Allowed:
- language pattern to intent
- security guard
- multi-intent detection
- lead intent detection

Not allowed:
- factual vehicle data
- hardcoded feature availability
- arbitrary rankings
- fake fallback outputs

Done criteria:
- every override has a reason
- no factual truth embedded
- tests cover major overrides
- npm run aci:safety green

### 2.2 aiAgent.semanticCompiler.js

Purpose:
Convert message + context + resolved entities into semantic intent structure.

Must not:
- answer customer facts
- hardcode availability
- duplicate planner red fixes

Done criteria:
- clean semantic schema
- confidence scores
- ambiguity metadata
- no customer-facing factual truth
- tests for semantic cases

### 2.3 aiAgent.planner.js / intentParser / intentRouter

Purpose:
Stable plan selection and routing.

Done criteria:
- no stale old system path
- no hidden fallback to wrong canvas
- clear planner output contract
- no hardcoded factual truth

---

## Phase 3 — Entity and Data Resolver Layer

### 3.1 aiAgent.vehicleModelResolver.js

Purpose:
DB-backed vehicle model resolution.

Required:
- model-only matching
- make+model matching
- fuzzy matching
- alias matching from DB/index, not static car list
- generic word protection
- ambiguity metadata

Tests:
- Verna
- Hyundai Verna
- Creta
- Seltos
- Honda City
- City as car vs city as location
- Thar
- i20
- XUV variants if present in DB
- Innova Hycross vs Crysta
- Creta vs Creta Electric

Done criteria:
- no static car list
- resolver suite green
- context switch audit green

### 3.2 aiAgent.vehicleEntityIndex.js

Purpose:
Fast entity index for model/variant/context resolution.

Required:
- cache strategy
- warmup
- indexed read-model queries
- no COLLSCAN in hot path
- stale context protection

Done criteria:
- profiler confirms fast warmup
- no critical COLLSCAN
- deterministic scoring

### 3.3 featureResolverV2 / featurePayloadBuilder

Purpose:
Feature truth and frontend-ready payloads.

Done criteria:
- feature catalog is source of truth
- variant-feature matrix is source of truth
- no fake feature data
- category metadata DB-backed where possible

---

## Phase 4 — Executor and Response Contract

### 4.1 aiAgent.executor.js

Purpose:
Execute planner tools deterministically.

Required:
- clean adapter map
- no duplicate tool calls
- partial failure support
- per-tool sourceTransparency
- no silent old collection fallback

Done criteria:
- executor tests for all tools
- source metadata complete
- npm run aci:safety green

### 4.2 aiAgent.responseTools.js

Purpose:
Build factual response payloads.

Required:
- stable contextPatch
- stable canvas/inline payloads
- no fake rows
- no hardcoded facts
- full source transparency

Done criteria:
- all major canvases tested
- frontend-safe payload shape

### 4.3 aiAgent.contractNormalizer.js

Purpose:
Final stable frontend contract.

Required:
- consistent displayMode
- canvasType/inlineType normalization
- contextPatch normalization
- sourceTransparency normalization
- actions/leadingQuestions normalization

Done criteria:
- official contract tests expanded
- frontend can trust stable keys

### 4.4 aiAgent.responseSanitizer.js

Purpose:
Safe final response cleanup.

Required:
- no internal leakage
- no raw errors
- no unsafe prompt injection echo
- no customer PII leakage

---

## Phase 5 — Deterministic New-Car Tools

### 5.1 Pricelist

Files:
- tools/newCars/vehiclePricelist.tool.js
- tools/vehiclePricelist.tool.js
- tools/shared/pricing.js

Required:
- read-model first
- one price source of truth
- city handling centralized
- no old collection fallback unless documented
- filters for variant/fuel/transmission/budget

Features:
- price list
- variant filters
- on-road breakup
- EMI handoff
- quotation handoff

### 5.2 Features

Files:
- tools/newCars/vehicleFeatures.tool.js
- featureResolverV2
- featurePayloadBuilder

Features:
- does variant have X
- which variants have X
- compare features between variants
- category feature explorer
- missing features
- best variant for feature set

Rules:
- feature catalog and matrix are truth
- synonyms only map user language to lookup

### 5.3 Colors

Files:
- tools/newCars/vehicleColors.tool.js

Required:
- remove hardcoded brand universe
- DB-backed model disambiguation
- no fake color fallback
- honest variant-wise availability if missing

Features:
- model-level colors
- selected color context
- image frame metadata
- color studio payload
- honest “stock unavailable” answer

### 5.4 Recommendations

Required:
- DB-backed filters
- explainable ranking
- no arbitrary claims

Features:
- budget recommendation
- body type recommendation
- feature-set recommendation
- family/city/highway use case
- safety-focused recommendation
- ownership-cost recommendation later

---

## Phase 6 — Lead, Quotation, CRM

Required:
- lead seriousness scoring
- lead profile
- dedupe
- source tracking
- CRM-ready payload
- human handoff
- audit trail
- consent/opt-in

Lead profile fields:
- name
- phone
- city
- model
- variant
- color
- budget
- finance
- exchange
- insurance
- timeline
- source
- conversation summary

Done criteria:
- every quote lead creates structured data
- no duplicate lead flooding
- privacy-safe logging

---

## Phase 7 — Security and Privacy

Required:
- rate limiting
- payload validation
- prompt-injection guard
- PII redaction in logs
- role-based internal access
- public/internal data separation
- error sanitization
- audit logs
- data retention rules

Critical rule:
Public ACI Assist must never expose internal CDrive records.

---

## Phase 8 — Performance and Scale

Required:
- p50/p95/p99 tracking
- DB query profiling
- no COLLSCAN in hot paths
- read-model indexes
- runtime prewarm
- cache strategy
- duplicate call reduction
- streaming status response

Targets to define:
- simple price query
- color query
- feature answer
- comparison
- multi-intent query
- cold start

---

## Phase 9 — Evaluation System

Required test suites:
- foundation smoke
- context switch
- model resolver
- variant resolver
- price
- feature answer
- feature explorer
- comparison
- EMI
- color
- recommendation
- lead/quotation
- security/prompt injection
- internal isolation
- real user regression

Scale:
- 150 questions
- 500 questions
- 1500 questions

Do not jump to 1500 before architecture is clean.

---

## Phase 10 — WhatsApp

Not before backend contract and lead lifecycle are stable.

Features:
- inbound chat
- phone number lead capture
- quick car answers
- quotation lead
- finance reminders
- human handoff
- deep links to web canvas
- template follow-ups only for high-value reminders

---

## Phase 11 — Mastra + Gemini

Not before deterministic tools and tests are stable.

Purpose:
- better reasoning
- lower token usage planning
- richer conversation
- not a replacement for clean backend truth

