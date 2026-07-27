#!/usr/bin/env node
"use strict";

const fs = require("fs");
const read = (file) => fs.readFileSync(file, "utf8");
const files = {
  finalEligibility: "src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs",
  finalReadiness: "src/services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs",
  finalRecommendation: "src/services/aciCore/recommendations/aciFinalRecommendation.service.js",
  evidenceReadiness: "src/services/aciCore/candidates/aciCandidateEvidenceReadiness.service.js",
  activeMarket: "src/services/aciCore/candidates/aciCandidateActiveMarketEligibility.service.js",
  marketConfidence: "src/services/aciCore/candidates/aciCandidateMarketConfidence.service.js",
  modulePolicy: "src/services/aciCore/decisionPolicy/aciDecisionModulePolicyProfiles.service.cjs",
  liveBridge: "src/services/aciCore/integration/aciCoreLiveBridge.service.js",
  finalSmoke: "src/scripts/aci-decision/smokeFinalRecommendationRuntimeV2.cjs",
};
const content = Object.fromEntries(Object.entries(files).map(([name, file]) => [name, read(file)]));
const checks = [];
const add = (id, ok, owner, blocks) => checks.push({ id, ok: Boolean(ok), owner, blocks });

add(
  "activation-requires-dedicated-final-object",
  content.finalEligibility.includes("finalRecommendation.status === 'final_ready'") &&
    content.finalEligibility.includes("finalRecommendation.finalRecommendationEnabled === true") &&
    content.finalEligibility.includes("moduleName === DECISION_MODULES.RECOMMENDATION"),
  "aciFinalRecommendationEligibility.service.cjs",
  "activation without a ready recommendation-module result",
);
add(
  "buyer-context-incomplete-remains-blocked",
  content.finalEligibility.includes("BUYER_CONTEXT_INCOMPLETE") &&
    content.finalRecommendation.includes('blockedReasons: ["buyer_context_incomplete"]'),
  "eligibility + recommendation composer",
  "final verdict with missing mandatory buyer inputs",
);
add(
  "missing-evidence-remains-blocked",
  content.finalEligibility.includes("EVIDENCE_THRESHOLD_NOT_MET") &&
    content.finalRecommendation.includes("insufficient_exact_variant_evidence") &&
    content.finalRecommendation.includes("no_fresh_source_grounded_candidates"),
  "eligibility + recommendation composer",
  "final verdict below exact evidence threshold",
);
add(
  "freshness-and-source-gates-remain-blocking",
  content.finalRecommendation.includes("maximumEvidenceAgeDays") &&
    content.finalRecommendation.includes("activeVehicleCount") &&
    content.finalRecommendation.includes("sourceSignalCount"),
  "aciFinalRecommendation.service.js",
  "stale, discontinued or untraceable candidates",
);
add(
  "on-road-and-feature-hard-filters-remain-blocking",
  content.finalRecommendation.includes("featureRequirementSatisfied") &&
    content.finalRecommendation.includes("price > budget") &&
    content.finalRecommendation.includes('priceBasis === "on_road"'),
  "aciFinalRecommendation.service.js",
  "winner outside budget or without every must-have feature",
);
add(
  "diagnostic-candidate-contracts-do-not-self-activate",
  /finalRecommendationEnabled:\s*false/.test(content.evidenceReadiness) &&
    /finalRecommendationEnabled:\s*false/.test(content.activeMarket) &&
    /finalRecommendationEnabled:\s*false/.test(content.marketConfidence),
  "candidate evidence contracts",
  "diagnostic candidate evidence becoming a final verdict by itself",
);
add(
  "non-recommendation-modules-remain-ineligible",
  content.modulePolicy.includes("canEverUseForFinalRecommendation: false") &&
    content.modulePolicy.includes("MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE"),
  "aciDecisionModulePolicyProfiles.service.cjs",
  "score, comparison, similar or upgrade modules producing final verdicts",
);
add(
  "live-bridge-retains-blocked-ux",
  content.liveBridge.includes("applyFinalRecommendationBlockedAnswer") &&
    content.liveBridge.includes("finalBlockedUx") &&
    content.liveBridge.includes("!finalRecommendationReady"),
  "aciCoreLiveBridge.service.js",
  "unsafe buyer-facing final language when activation gates fail",
);
add(
  "positive-and-negative-runtime-controls-exist",
  content.finalSmoke.includes("finalRecommendationEnabled, true") &&
    content.finalSmoke.includes("incomplete.finalRecommendationEnabled") &&
    content.finalSmoke.includes("unsupported.finalRecommendationEnabled"),
  "smokeFinalRecommendationRuntimeV2.cjs",
  "activation without runtime positive and negative controls",
);
add(
  "readiness-reports-conditional-activation",
  content.finalReadiness.includes("canActivateFinalRecommendation") &&
    content.finalReadiness.includes("buyerContextComplete") &&
    content.finalReadiness.includes("evidenceThresholdMet") &&
    content.finalReadiness.includes("finalRecommendationReady"),
  "aciFinalRecommendationReadiness.service.cjs",
  "unconditional global activation",
);

const failed = checks.filter((item) => !item.ok);
console.log(JSON.stringify({
  suite: "ACI Final Activation Blocker Matrix Audit v2",
  ok: failed.length === 0,
  finalRecommendationActivationAllowed: "recommendation_module_only_when_all_gates_pass",
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((item) => item.id),
  blockerMatrix: checks,
}, null, 2));

if (failed.length) process.exit(1);
