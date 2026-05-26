# ACI Backend File Audit Tracker

Statuses:
- NOT_STARTED
- AUDITED
- NEEDS_REFACTOR
- IN_PROGRESS
- GREEN
- DONE
- DEPRECATED_REVIEW
- DEPRECATED_REMOVE

Done means:
- clear responsibility
- no forbidden hardcoding
- no fake fallback
- no hidden old path
- no unnecessary DB access
- source transparency correct
- context behavior tested where relevant
- npm run aci:safety green

| Phase | File / Module | Current Status | Target Status | Notes |
|---|---|---:|---:|---|
| 1 | src/services/aiAgent/aiAgent.service.js | GREEN | DONE | Final safety needed after last cleanup |
| 1 | src/services/aiAgent/aiAgent.earlyFeatureGate.js | NEEDS_REFACTOR | DONE | Extracted, but internally too large |
| 1 | src/services/aiAgent/aiAgent.modelContextResolver.js | AUDITED | DONE | DB-backed; needs focused tests |
| 1 | src/services/aiAgent/aiAgent.contextPriority.js | AUDITED | DONE | Needs final context edge audit |
| 2 | src/services/aiAgent/aiAgent.plannerRedFixes.js | NOT_STARTED | DONE | High-risk patch graveyard |
| 2 | src/services/aiAgent/aiAgent.semanticCompiler.js | NOT_STARTED | DONE | Must not duplicate planner fixes |
| 2 | src/services/aiAgent/aiAgent.planner.js | NOT_STARTED | DONE | Planner contract audit needed |
| 2 | src/services/aiAgent/aiAgent.intentParser.js | NOT_STARTED | DONE | Language-only mapping allowed |
| 2 | src/services/aiAgent/aiAgent.intentRouter.js | NOT_STARTED | DONE | Routing audit needed |
| 3 | src/services/aiAgent/aiAgent.vehicleModelResolver.js | AUDITED | DONE | Static fallback removed; add resolver suite |
| 3 | src/services/aiAgent/aiAgent.vehicleEntityIndex.js | NOT_STARTED | DONE | Index/cache/perf audit needed |
| 3 | src/services/aiAgent/aiAgent.featureResolverV2.js | NOT_STARTED | DONE | Feature truth audit |
| 3 | src/services/aiAgent/aiAgent.featurePayloadBuilder.js | NOT_STARTED | DONE | Frontend payload audit |
| 4 | src/services/aiAgent/aiAgent.executor.js | NOT_STARTED | DONE | Large file; adapter/source audit |
| 4 | src/services/aiAgent/aiAgent.responseTools.js | NOT_STARTED | DONE | Large file; no fake rows |
| 4 | src/services/aiAgent/aiAgent.contractNormalizer.js | NOT_STARTED | DONE | Final contract stability |
| 4 | src/services/aiAgent/aiAgent.responseSanitizer.js | NOT_STARTED | DONE | Security/privacy output audit |
| 5 | src/services/aiAgent/tools/newCars/vehiclePricelist.tool.js | NOT_STARTED | DONE | Read-model/source audit |
| 5 | src/services/aiAgent/tools/newCars/vehicleFeatures.tool.js | NOT_STARTED | DONE | Feature catalog/matrix truth |
| 5 | src/services/aiAgent/tools/newCars/vehicleColors.tool.js | NOT_STARTED | DONE | Hardcoded brand/conflict rules found |
| 5 | src/services/aiAgent/tools/vehiclePricelist.tool.js | NOT_STARTED | DONE | Duplicate/new-vs-old path audit |
| 5 | src/services/aiAgent/tools/shared/* | NOT_STARTED | DONE | Shared matching/pricing/db audit |
| 6 | Lead/quotation modules | NOT_STARTED | DONE | CRM-ready lifecycle missing |
| 7 | Routes/controllers for ACI Assist | NOT_STARTED | DONE | Public/internal isolation |
| 8 | Performance scripts/profilers | AUDITED | DONE | Expand after module cleanup |
| 9 | Test scripts | AUDITED | DONE | Expand from 22 checks to larger suites |
| 10 | src/services/aiAgent/_deprecated_v1/* | DEPRECATED_REVIEW | DEPRECATED_REMOVE | Remove only after import proof |

## Current Active Rule Violations / Risks

| Risk | Status | Notes |
|---|---:|---|
| Hardcoded model truth in service | FIXED | Replaced with DB-backed resolver |
| test-drive leftovers | FIXED | Removed from service |
| service.js mixed responsibilities | MOSTLY_FIXED | Down to orchestration layer |
| earlyFeatureGate internal complexity | OPEN | Next cleanup target |
| plannerRedFixes size/patch risk | OPEN | High-risk |
| semanticCompiler duplication risk | OPEN | High-risk |
| colors tool hardcoded brand/conflict rules | OPEN | Needs DB-backed disambiguation |
| test coverage too small | OPEN | Need staged test expansion |

## Immediate Next Work Queue

1. Ensure current service cleanup is green and committed.
2. Audit aiAgent.earlyFeatureGate.js.
3. Split earlyFeatureGate into detector / planBuilder / runner / responsePolisher / contextPatch.
4. Run npm run aci:safety.
5. Update this tracker.
6. Move to modelContextResolver / vehicleModelResolver tests.
