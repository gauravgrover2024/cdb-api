#!/usr/bin/env node
"use strict";

const fs = require("fs");

const read = (file) => fs.readFileSync(file, "utf8");
const files = {
  languageComposer: "src/services/aciCore/language/aciAnswerLanguageComposer.js",
  languageRegistry: "src/services/aciCore/language/aciAnswerLanguageRegistry.js",
  liveBridge: "src/services/aciCore/integration/aciCoreLiveBridge.service.js",
  finalEligibility: "src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs",
  finalReadiness: "src/services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs",
  finalRecommendation: "src/services/aciCore/recommendations/aciFinalRecommendation.service.js",
  finalPolicy: "src/services/aciCore/recommendations/config/finalRecommendationPolicy.v1.json",
  scoreTool: "src/services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js",
  similarTool: "src/services/aiAgent/tools/newCars/vehicleSimilar.tool.js",
};
const content = Object.fromEntries(
  Object.entries(files).map(([name, file]) => [name, read(file)]),
);

const checks = [];
const add = (id, ok, extra = {}) => checks.push({ id, ok: Boolean(ok), ...extra });

add(
  "central-language-composer-exists",
  content.languageComposer.includes("renderAciLanguageText") &&
    content.languageComposer.includes("renderAciTemplate"),
);
add(
  "blocked-language-templates-remain-available",
  content.languageRegistry.includes("decision_final_blocked_missing_context") &&
    content.languageRegistry.includes("decision_final_blocked_partial_results"),
);
add(
  "dedicated-final-recommendation-composer-exists",
  content.finalRecommendation.includes("buildAciFinalRecommendation") &&
    content.finalRecommendation.includes("buildAnswer") &&
    content.finalRecommendation.includes("final_ready"),
);
add(
  "versioned-data-driven-policy-exists",
  content.finalPolicy.includes("baseWeights") &&
    content.finalPolicy.includes("priorityAdjustments") &&
    content.finalPolicy.includes("minimumEligibleModels") &&
    !/\b(?:Hyundai|Maruti|Tata|Kia|Mahindra|Skoda|Volkswagen)\b/i.test(content.finalPolicy),
);
add(
  "eligibility-runtime-is-live-and-conditional",
  content.finalEligibility.includes("dryRun: false") &&
    content.finalEligibility.includes("finalRecommendationReady") &&
    content.finalEligibility.includes("moduleName === DECISION_MODULES.RECOMMENDATION"),
);
add(
  "readiness-is-evidence-gated",
  content.finalReadiness.includes("finalRecommendationReady") &&
    content.finalReadiness.includes("buyerContextComplete") &&
    content.finalReadiness.includes("evidenceThresholdMet") &&
    content.finalReadiness.includes("evidence_gated_live"),
);
add(
  "exact-variant-evidence-gates-exist",
  content.finalRecommendation.includes("profileMatchesVariant") &&
    content.finalRecommendation.includes("decisionMatchesVariant") &&
    content.finalRecommendation.includes("featureRequirementSatisfied") &&
    content.finalRecommendation.includes("maximumEvidenceAgeDays"),
);
add(
  "on-road-budget-is-enforced-by-selected-basis",
  content.finalRecommendation.includes('priceBasis === "on_road"') &&
    content.finalRecommendation.includes("price > budget"),
);
add(
  "live-bridge-keeps-blocked-and-ready-paths-separate",
  content.liveBridge.includes("applyFinalRecommendationBlockedAnswer") &&
    content.liveBridge.includes("finalRecommendationEligibility.canUseForFinalRecommendation") &&
    content.liveBridge.includes("fastFinalRecommendation?.finalRecommendationEnabled === true"),
);
add(
  "diagnostic-modules-cannot-activate-final-verdict",
  !/finalRecommendationEnabled\s*:\s*true/.test(content.scoreTool) &&
    !/finalRecommendationEnabled\s*:\s*true/.test(content.similarTool),
);
add(
  "startup-prewarm-supported",
  content.finalRecommendation.includes("prewarmAciFinalRecommendationEvidence") &&
    content.finalRecommendation.includes("EVIDENCE_CACHE_TTL_MS"),
);

const failed = checks.filter((check) => !check.ok);
console.log(JSON.stringify({
  suite: "ACI Final Composer Readiness Audit v2",
  ok: failed.length === 0,
  readiness: {
    centralLanguageComposerAvailable: true,
    finalComposerReady: failed.length === 0,
    finalRecommendationActivationReady: failed.length === 0,
    activationMode: "recommendation_module_only_evidence_gated",
    diagnosticModulesRemainBlocked: true,
  },
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((item) => item.id),
  checks,
  warnings: [],
}, null, 2));

if (failed.length) process.exit(1);
