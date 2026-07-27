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
| 3 | src/services/aiAgent/aiAgent.featureResolverV2.js | GREEN | DONE | Feature/variant collision guard and roller-coaster smoke pass; broader taxonomy audit remains |
| 3 | src/services/aiAgent/aiAgent.featurePayloadBuilder.js | NOT_STARTED | DONE | Frontend payload audit |
| 4 | src/services/aiAgent/aiAgent.executor.js | NOT_STARTED | DONE | Large file; adapter/source audit |
| 4 | src/services/aiAgent/aiAgent.responseTools.js | NOT_STARTED | DONE | Large file; no fake rows |
| 4 | src/services/aiAgent/aiAgent.contractNormalizer.js | AUDITED | DONE | Context/decision answer preservation verified; full contract inventory remains |
| 4 | src/services/aiAgent/aiAgent.responseSanitizer.js | NOT_STARTED | DONE | Security/privacy output audit |
| 5 | src/services/aiAgent/tools/newCars/vehiclePricelist.tool.js | NOT_STARTED | DONE | Read-model/source audit |
| 5 | src/services/aiAgent/tools/newCars/vehicleFeatures.tool.js | GREEN | DONE | ABS feature truth and feature/variant collision regression pass |
| 5 | src/services/aciCore/features/aciFeatureExplainer.service.js | GREEN | DONE | Mongo-backed canonical resolver, source provenance and prewarm smoke pass; content coverage remains a separate backlog |
| 5 | src/services/aiAgent/tools/newCars/vehicleColors.tool.js | NOT_STARTED | DONE | Hardcoded brand/conflict rules found |
| 5 | src/services/aiAgent/tools/vehiclePricelist.tool.js | NOT_STARTED | DONE | Duplicate/new-vs-old path audit |
| 5 | src/services/aiAgent/tools/shared/* | NOT_STARTED | DONE | Shared matching/pricing/db audit |
| 6 | Lead/quotation modules | NOT_STARTED | DONE | CRM-ready lifecycle missing |
| 7 | Routes/controllers for ACI Assist | AUDITED | DONE | Public chat route is thin; frontend transport contract now preserves durable context state |
| 8 | Performance scripts/profilers | AUDITED | DONE | Expand after module cleanup |
| 9 | Test scripts | GREEN | DONE | Added 13-turn roller-coaster and conditional-decision regression; safety/context/decision gates pass |
| 10 | src/services/aiAgent/_legacy_backup_before_v2/*.bak | DONE | DEPRECATED_REMOVE | Three tracked backups removed after zero-import/reference proof |

## 2026-07-10 Repository-Wide Verification

- All 506 backend JavaScript files under `src` passed `node --check`.
- `aci:safety:fast` passed with 72 pass mentions and zero failures.
- `aci:context-manager:phase1co2` passed.
- `aci:context-manager:stress` passed.
- `aci:decision:gate:phase0` passed with all 41 tasks, including conditional comparison guidance.
- Progress registry guard passed with 30 modules, no duplicates, no missing required modules and no fallback registry.
- Feature Explainer smoke passed against `aci_feature_explainers_v1`; the no-hardcoded audit now scans 134 runtime files with zero banned findings.

This is a repository-wide static and gate verification, not a claim that every file has received a line-by-line semantic audit. The phase table remains the honest semantic-audit backlog.

## Current Active Rule Violations / Risks

| Risk | Status | Notes |
|---|---:|---|
| Hardcoded model truth in service | FIXED | Replaced with DB-backed resolver |
| test-drive leftovers | FIXED | Removed from service |
| service.js mixed responsibilities | MOSTLY_FIXED | Down to orchestration layer |
| earlyFeatureGate internal complexity | MOSTLY_FIXED | Split into orchestrator/detector/languageMap/planBuilder/runner/contextPatch/polisher |
| plannerRedFixes size/patch risk | OPEN | High-risk |
| semanticCompiler duplication risk | OPEN | High-risk |
| colors tool hardcoded brand/conflict rules | OPEN | Needs DB-backed disambiguation |
| test coverage too small | IMPROVED | Context, ABS, decision, safety and frontend-contract regressions added; full multilingual/adversarial coverage remains |

## Immediate Next Work Queue

1. Ensure current service cleanup is green and committed.
2. Audit aiAgent.earlyFeatureGate.js.
3. Split earlyFeatureGate into detector / planBuilder / runner / responsePolisher / contextPatch.
4. Run npm run aci:safety.
5. Update this tracker.
6. Move to modelContextResolver / vehicleModelResolver tests.
