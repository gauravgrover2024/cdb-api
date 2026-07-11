#!/usr/bin/env node
"use strict";

require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

const mongoUri = () =>
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.MONGODB_URL ||
  process.env.MONGO_URL ||
  process.env.DATABASE_URL ||
  "";

const INTERNAL_LANGUAGE = /\b(diagnostic shortlist|candidateMarket|blockedReasons|allowedAnswerType|finalRecommendationEnabled|canUseForFinalRecommendation|evidence-eligible|decision data)\b/i;

async function main() {
  const uri = mongoUri();
  assert(uri, "Mongo URI is required");
  await mongoose.connect(uri);

  try {
    const [{ chatWithAgent }, { prewarmAciAssistRuntime }] = await Promise.all([
      import("../../services/aiAgent/aiAgent.service.js"),
      import("../../services/aiAgent/aiAgent.runtimePrewarm.js"),
    ]);
    await prewarmAciAssistRuntime({ force: true });

    const completeMessage = "Recommend me the best automatic petrol SUV under 20 lakh on-road in Delhi for a family of four, about 1200 km per month. Safety is my top priority and I want six airbags and a sunroof.";
    const startedAt = Date.now();
    const complete = await chatWithAgent({ message: completeMessage, context: {} });
    const durationMs = Date.now() - startedAt;
    const finalRecommendation = complete.finalRecommendation || complete.data?.finalRecommendation;
    const winner = finalRecommendation?.winner;

    assert.strictEqual(complete.intent, "vehicle_recommendation");
    assert.strictEqual(complete.tool, "vehicle_recommend");
    assert.strictEqual(complete.finalRecommendationEnabled, true);
    assert.strictEqual(complete.canUseForFinalRecommendation, true);
    assert.strictEqual(finalRecommendation?.status, "final_ready");
    assert.strictEqual(finalRecommendation?.evidence?.priceBasis, "on_road");
    assert.strictEqual(finalRecommendation?.evidence?.budget, 2000000);
    assert(finalRecommendation?.evidence?.inputModelCount >= 10, "final ranker should evaluate the full candidate set");
    assert(finalRecommendation?.evidence?.evaluatedVariantCount >= 2, "at least two exact variants should clear evidence gates");
    assert(winner?.onRoadPrice > 0 && winner.onRoadPrice <= 2000000, "winner must stay inside the on-road cap");
    assert(/\bmy pick\b/i.test(complete.answer || ""));
    assert(/six airbags|6 airbags/i.test(complete.answer || ""));
    assert(/sunroof/i.test(complete.answer || ""));
    assert(/crash-test applicability|crash evidence/i.test(complete.answer || ""));
    assert(!INTERNAL_LANGUAGE.test(complete.answer || ""), "buyer answer leaked internal decision language");

    for (const row of finalRecommendation.rows || []) {
      assert.strictEqual(String(row.fuelType).toLowerCase(), "petrol");
      assert(/automatic|amt|cvt|dct|ivt|\bat\b/i.test(String(row.transmission)));
      assert(row.onRoadPrice > 0 && row.onRoadPrice <= 2000000, `${row.fullModel} exceeds the on-road cap`);
    }

    const featureDoc = await mongoose.connection.db.collection("vehicle_variant_feature_matrix_v2").findOne({
      modelKey: winner.modelKey,
      $or: [
        { variantKey: winner.variantKey },
        { variant: new RegExp(`^${String(winner.variant).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i") },
      ],
      "featuresByKey.six_airbags.available": true,
      "featuresByKey.sunroof.available": true,
    });
    assert(featureDoc, "winner must have exact matrix proof for every must-have feature");

    const incomplete = await chatWithAgent({
      message: "Recommend the best automatic SUV for me. Safety matters.",
      context: {},
    });
    assert.notStrictEqual(incomplete.finalRecommendationEnabled, true);
    assert.notStrictEqual(incomplete.canUseForFinalRecommendation, true);
    assert(!/\byou should buy\b/i.test(incomplete.answer || ""));
    assert(((incomplete.answer || "").match(/\?/g) || []).length <= 1, "incomplete context should ask at most one question");

    const comparison = await chatWithAgent({
      message: "ABS vs EBD: which one does what?",
      context: {},
    });
    assert.strictEqual(comparison.intent, "vehicle_feature_comparison_explanation");
    assert.notStrictEqual(comparison.finalRecommendationEnabled, true);

    const manual = await chatWithAgent({
      message: "Recommend me the best manual petrol SUV under 20 lakh on-road in Delhi for a family of four, about 1000 km per month. Safety is a high priority and six airbags are a must.",
      context: {},
    });
    const manualFinal = manual.finalRecommendation || manual.data?.finalRecommendation;
    assert.strictEqual(manualFinal?.status, "final_ready");
    for (const row of manualFinal.rows || []) {
      assert(/manual|\bmt\b/i.test(String(row.transmission)), `${row.fullModel} is not an exact manual match`);
      assert(!/automatic|amt|cvt|dct|ivt|dsg/i.test(String(row.transmission)), `${row.fullModel} leaked an automatic variant`);
    }

    const unsupported = await chatWithAgent({
      message: "Recommend the best automatic petrol SUV under 20 lakh on-road in Mumbai for a family of four. Safety is high and I need six airbags and a sunroof.",
      context: {},
    });
    assert.notStrictEqual(unsupported.finalRecommendationEnabled, true);
    assert(/not supported|currently support|pricing is currently available|live on-road pricing/i.test(unsupported.answer || ""));

    console.log(JSON.stringify({
      suite: "ACI final recommendation runtime v2",
      ok: true,
      total: 5,
      passed: 5,
      failed: 0,
      durationMs,
      winner,
      runnerUp: finalRecommendation.runnerUp,
      evidence: finalRecommendation.evidence,
      answer: complete.answer,
      incompleteAnswer: incomplete.answer,
      unsupportedAnswer: unsupported.answer,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({ suite: "ACI final recommendation runtime v2", ok: false, error: error.message }, null, 2));
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
