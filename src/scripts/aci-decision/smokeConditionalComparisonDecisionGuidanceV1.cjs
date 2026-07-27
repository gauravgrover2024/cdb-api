#!/usr/bin/env node
"use strict";

require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

const FULL_BUYER_CONTEXT = Object.freeze({
  city: "new-delhi",
  budgetOrPriceCeiling: 1800000,
  bodyPreferenceOrPrimaryUseCase: "family city use",
  familySizeOrOccupancyUse: "family of 4",
  monthlyRunning: "1200 km monthly",
  transmissionPreference: "automatic",
  safetyPriority: "high",
  featurePriority: ["6 airbags", "rear camera"],
  shortlistedModelsOrDiscoveryScope: ["Mahindra Thar", "Hyundai Creta"],
});

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const getConditionalGuidance = (response = {}) =>
  response.conditionalDecisionGuidance ||
  response.data?.conditionalDecisionGuidance ||
  response.meta?.conditionalDecisionGuidance ||
  response.finalBlockedUx?.conditionalDecisionGuidance ||
  null;

const getEligibility = (response = {}) =>
  response.finalRecommendationEligibility ||
  response.data?.finalRecommendationEligibility ||
  response.meta?.finalRecommendationEligibility ||
  null;

const buildDecisionContext = (comparisonResponse = {}) => {
  const patch = comparisonResponse.contextPatch || {};
  const previousState = patch.contextState || patch.aciContextState || {};
  const contextState = {
    ...previousState,
    buyerContext: FULL_BUYER_CONTEXT,
    activeComparison: patch.activeComparison || previousState.activeComparison || {},
    selectedComparisonSet:
      patch.selectedComparisonSet || previousState.selectedComparisonSet || {},
  };

  return {
    contextState,
    aciContextState: contextState,
    buyerContext: FULL_BUYER_CONTEXT,
    buyerGuidanceContext: patch.buyerGuidanceContext || {},
    contextLedger: previousState.contextLedger || patch.contextLedger || {},
    selectedVehicle: patch.selectedVehicle || previousState.selectedVehicle || {},
    activeComparison: contextState.activeComparison,
    selectedComparisonSet: contextState.selectedComparisonSet,
    anchorCity: "new-delhi",
  };
};

async function main() {
  const uri = mongoUri();
  assert(uri, "Mongo URI is required for the conditional decision smoke.");

  await mongoose.connect(uri);

  const [{ chatWithAgent }, { prewarmAciAssistRuntime }, { prewarmAciCoreRuntime }] =
    await Promise.all([
      import("../../services/aiAgent/aiAgent.service.js"),
      import("../../services/aiAgent/aiAgent.runtimePrewarm.js"),
      import("../../services/aciCore/aciCore.prewarm.js"),
    ]);

  await Promise.all([
    prewarmAciAssistRuntime({ force: true }),
    prewarmAciCoreRuntime({ force: true, mode: "light", background: false }),
  ]);

  try {
    const comparisonResponse = await chatWithAgent({
      message: "thar vs creta",
      context: { anchorCity: "new-delhi" },
    });
    const context = buildDecisionContext(comparisonResponse);
    const response = await chatWithAgent({
      message: "Based on my priorities, which one should I buy?",
      context,
    });

    const guidance = getConditionalGuidance(response);
    const eligibility = getEligibility(response);
    const answer = String(response.answer || "");
    const fuelStatus = eligibility?.buyerDecisionInput?.inputStatus?.fuelPreferenceOrMonthlyRunning || {};

    assert(guidance, "conditional decision guidance should be attached");
    assert.strictEqual(guidance.activated, true);
    assert.strictEqual(guidance.mode, "conditional_decision_guidance");
    assert.strictEqual(guidance.canUseForFinalRecommendation, false);
    assert.strictEqual(guidance.finalRecommendationEnabled, false);
    assert(
      /lean toward|genuinely close/i.test(answer),
      `buyer-facing answer should make a practical decision lean: ${answer}`,
    );
    assert(!/indexed feature|diagnostic shortlist|module score/i.test(answer), "internal decision language must not leak");
    assert.strictEqual(eligibility?.requestedFinalRecommendation, true);
    assert.strictEqual(eligibility?.canUseForFinalRecommendation, false);
    assert.strictEqual(eligibility?.finalRecommendationEnabled, false);
    assert(!/diesel/i.test(String(fuelStatus.value || "")), "compared variant fuel must not become buyer fuel preference");
    assert(!/diesel/i.test(String(fuelStatus.source || "")), "compared variant fuel must not become buyer fuel source");

    console.log(JSON.stringify({
      suite: "ACI conditional comparison decision guidance smoke v1",
      ok: true,
      answer,
      guidance,
      fuelPreferenceOrMonthlyRunning: fuelStatus,
      finalRecommendationEnabled: eligibility.finalRecommendationEnabled,
      canUseForFinalRecommendation: eligibility.canUseForFinalRecommendation,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI conditional comparison decision guidance smoke v1",
    ok: false,
    error: error.message,
  }, null, 2));
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
