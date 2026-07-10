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

async function main() {
  const uri = mongoUri();
  assert(uri, "Mongo URI is required for the feature explainer smoke");
  await mongoose.connect(uri);

  try {
    const [explainerModule, agentModule, prewarmModule] = await Promise.all([
      import("../../services/aciCore/features/aciFeatureExplainer.service.js"),
      import("../../services/aiAgent/aiAgent.service.js"),
      import("../../services/aiAgent/aiAgent.runtimePrewarm.js"),
    ]);

    await prewarmModule.prewarmAciAssistRuntime({ force: true });
    const prewarm = await explainerModule.prewarmAciFeatureExplainers();
    assert.strictEqual(prewarm.ok, true, "feature explainer cache should prewarm");

    const explainer = await explainerModule.resolveAciFeatureExplainer({
      canonicalKey: "anti_lock_braking_system_abs",
      featureName: "ABS",
    });
    assert(explainer, "ABS explainer should resolve from MongoDB");
    assert.strictEqual(explainer.sourceCollection, "aci_feature_explainers_v1");
    assert(/steerable|steering/i.test(explainer.buyerSummary));
    assert(explainer.sourceRefs.some((source) => /bosch-mobility\.com/i.test(source.url)));

    const startedAt = Date.now();
    const response = await agentModule.chatWithAgent({
      message: "thar abs",
      context: { anchorCity: "new-delhi" },
    });
    const durationMs = Date.now() - startedAt;
    const attached = response.featureExplanation || response.data?.featureExplanation;

    assert(attached, "chat response should attach the DB-backed feature explanation");
    assert.strictEqual(attached.canonicalKey, "anti_lock_braking_system_abs");
    assert(/steerable|steering/i.test(response.answer || ""));
    assert(/emergency stops/i.test(response.answer || ""));
    assert(!/older .* variant/i.test(response.answer || ""));

    console.log(JSON.stringify({
      suite: "ACI Feature Explainer smoke v1",
      ok: true,
      total: 1,
      passed: 1,
      failed: 0,
      failedIds: [],
      durationMs,
      answer: response.answer,
      explainer: attached,
      prewarm,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Feature Explainer smoke v1",
    ok: false,
    total: 1,
    passed: 0,
    failed: 1,
    failedIds: ["abs-feature-explainer"],
    error: error.message,
  }, null, 2));
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
