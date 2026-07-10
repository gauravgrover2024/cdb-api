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
    const [catalogCount, publishedExplainerCount] = await Promise.all([
      mongoose.connection.db.collection("vehicle_feature_catalog_v2").countDocuments({}),
      mongoose.connection.db.collection("aci_feature_explainers_v1").countDocuments({
        status: "published",
      }),
    ]);
    assert.strictEqual(
      publishedExplainerCount,
      catalogCount,
      "every canonical vehicle feature should have one published explainer",
    );

    const explainer = await explainerModule.resolveAciFeatureExplainer({
      canonicalKey: "anti_lock_braking_system_abs",
      featureName: "ABS",
    });
    assert(explainer, "ABS explainer should resolve from MongoDB");
    assert.strictEqual(explainer.sourceCollection, "aci_feature_explainers_v1");
    assert(/steerable|steering/i.test(explainer.buyerSummary));
    assert(explainer.sourceRefs.some((source) => /bosch-mobility\.com/i.test(source.url)));

    const resolvedFromText = await explainerModule.resolveAciFeatureExplainerFromText(
      "what is ABS and why does it matter?",
    );
    assert.strictEqual(
      resolvedFromText?.canonicalKey,
      "anti_lock_braking_system_abs",
      "standalone text resolution should identify ABS without a vehicle",
    );

    const standalone = await agentModule.chatWithAgent({
      message: "what is ABS and why does it matter?",
      context: {
        contextState: {
          buyerContext: { safetyPriority: "high" },
        },
      },
    });
    assert.strictEqual(standalone.intent, "vehicle_feature_explanation");
    assert.strictEqual(standalone.tool, "feature_explainer");
    assert.strictEqual(
      standalone.featureExplanation?.canonicalKey || standalone.data?.featureExplanation?.canonicalKey,
      "anti_lock_braking_system_abs",
    );
    assert(/wheels? from locking|wheel-speed sensors/i.test(standalone.answer || ""));
    assert(/cannot create extra tyre grip/i.test(standalone.answer || ""));

    const bareFeature = await agentModule.chatWithAgent({
      message: "ABS",
      context: {},
    });
    assert.strictEqual(bareFeature.intent, "vehicle_feature_explanation");
    assert.strictEqual(
      bareFeature.featureExplanation?.canonicalKey || bareFeature.data?.featureExplanation?.canonicalKey,
      "anti_lock_braking_system_abs",
    );

    const acronymFeature = await agentModule.chatWithAgent({
      message: "what is ISOFIX?",
      context: {},
    });
    assert.strictEqual(acronymFeature.intent, "vehicle_feature_explanation");
    assert.strictEqual(
      acronymFeature.featureExplanation?.canonicalKey || acronymFeature.data?.featureExplanation?.canonicalKey,
      "isofix_child_seat_mounts",
      "display-name acronyms should resolve without a hardcoded alias list",
    );

    const genericMetric = await agentModule.chatWithAgent({
      message: "what is ground clearance?",
      context: {},
    });
    assert.strictEqual(genericMetric.intent, "vehicle_feature_explanation");
    assert(
      /ground_clearance/.test(
        genericMetric.featureExplanation?.canonicalKey ||
        genericMetric.data?.featureExplanation?.canonicalKey ||
        "",
      ),
      "generic metric wording should resolve a qualified catalog label",
    );

    const sunroof = await agentModule.chatWithAgent({
      message: "is a sunroof worth it for family city use?",
      context: {
        contextState: {
          buyerContext: {
            city: "Delhi",
            familySizeOrOccupancyUse: "family of four",
          },
        },
      },
    });
    assert.strictEqual(sunroof.intent, "vehicle_feature_explanation");
    assert.strictEqual(
      sunroof.featureExplanation?.canonicalKey || sunroof.data?.featureExplanation?.canonicalKey,
      "sunroof",
    );
    assert(/sunroof/i.test(sunroof.answer || ""));
    assert(/city|family|priority|prioritise|purchase/i.test(sunroof.answer || ""));

    const featureComparison = await agentModule.chatWithAgent({
      message: "ABS vs adaptive cruise control for highway family use",
      context: {
        contextState: {
          buyerContext: {
            familySizeOrOccupancyUse: "family of four",
            bodyPreferenceOrPrimaryUseCase: "highway touring",
          },
        },
      },
    });
    const comparedKeys = (featureComparison.featureExplanations || [])
      .map((item) => item.canonicalKey);
    assert.strictEqual(featureComparison.intent, "vehicle_feature_comparison_explanation");
    assert(comparedKeys.includes("anti_lock_braking_system_abs"));
    assert(comparedKeys.includes("adaptive_cruise_control"));
    assert(!comparedKeys.includes("cruise_control"), "nested alias must not replace ABS");

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
      total: 9,
      passed: 9,
      failed: 0,
      failedIds: [],
      durationMs,
      coverage: { catalogCount, publishedExplainerCount },
      standaloneAnswer: standalone.answer,
      sunroofAnswer: sunroof.answer,
      featureComparisonAnswer: featureComparison.answer,
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
      total: 9,
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
