#!/usr/bin/env node
require("dotenv").config();

const assert = require("assert");
const mongoose = require("mongoose");

function hasUnsafeRecommendationLanguage(value = "") {
  return /\b(must buy|buy this|clear winner|recommended buy)\b/i.test(String(value || ""));
}

async function main() {
  const mod = await import("../../services/aciCore/integration/aciCoreLiveBridge.service.js");
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  const runAciCoreLiveBridge = mod.runAciCoreLiveBridge || mod.default;
  if (typeof runAciCoreLiveBridge !== "function") {
    throw new Error("runAciCoreLiveBridge export not found");
  }
  if (typeof connectDB !== "function") {
    throw new Error("connectDB export not found");
  }

  await connectDB();

  const scorpioNPriceRowCount = await mongoose.connection.db
    .collection("aci_vehicle_price_rows")
    .countDocuments({
      model: /scorpio\s*n/i,
      citySlug: "new-delhi",
    });

  const cases = [
    {
      id: "model-value-baleno-no-score-insight-label",
      message: "is baleno good value",
      context: {
        selectedVehicle: {
          make: "Maruti",
          model: "Baleno",
          fullModel: "Maruti Baleno",
          city: "new-delhi",
          citySlug: "new-delhi",
        },
      },
      assertResult(body) {
        const blob = JSON.stringify(body || {});
        const answer = String(body.answer || "");
        const operation =
          body.operation ||
          body.data?.operation ||
          body.meta?.scoreInsightOperation ||
          body.aciCoreBridge?.operation ||
          "";
        assert(/baleno/i.test(blob), "response must mention Baleno");
        assert(!/Score insight/i.test(answer), "buyer answer must not use generic 'Score insight' label");
        assert(!/I found score insight data for/i.test(answer), "generic score fallback leaked");
        assert.strictEqual(body.aciCoreBridge?.tool || body.meta?.aciCoreBridge?.tool, "vehicle_score_insight");
        assert(
          ["same_family_value_insights", "model_score_insights"].includes(operation),
          `expected model/family score operation, got ${operation}`,
        );
        assert(/diagnostic/i.test(answer), "answer must remain diagnostic-only");
        assert(!hasUnsafeRecommendationLanguage(blob), "unsafe recommendation wording leaked");
      },
    },
    {
      id: "variant-value-baleno-alpha-keeps-variant",
      message: "is baleno alpha good value",
      context: {
        selectedVehicle: {
          make: "Maruti",
          model: "Baleno",
          fullModel: "Maruti Baleno",
          variant: "Alpha",
          city: "new-delhi",
          citySlug: "new-delhi",
        },
      },
      assertResult(body) {
        const blob = JSON.stringify(body || {});
        const answer = String(body.answer || "");
        const operation =
          body.operation ||
          body.data?.operation ||
          body.meta?.scoreInsightOperation ||
          body.aciCoreBridge?.operation ||
          "";
        assert(/baleno/i.test(blob), "response must mention Baleno");
        assert(/alpha/i.test(blob), "response must mention Alpha");
        assert(!/Score insight/i.test(answer), "buyer answer must not use generic 'Score insight' label");
        assert.strictEqual(body.aciCoreBridge?.tool || body.meta?.aciCoreBridge?.tool, "vehicle_score_insight");
        assert.strictEqual(operation, "variant_score_insight", `expected variant_score_insight, got ${operation}`);
        assert(/diagnostic/i.test(answer), "answer must remain diagnostic-only");
        assert(!hasUnsafeRecommendationLanguage(blob), "unsafe recommendation wording leaked");
      },
    },
    {
      id: "direct-price-overrides-stale-comparison-context",
      message: "scorpio n price",
      context: {
        selectedVehicle: {
          make: "Mahindra",
          model: "Scorpio N",
          fullModel: "Mahindra Scorpio N",
          city: "new-delhi",
          citySlug: "new-delhi",
        },
        activeComparison: {
          vehicles: [
            { make: "Mahindra", model: "Scorpio N", fullModel: "Mahindra Scorpio N" },
            { make: "Mahindra", model: "Scorpio", fullModel: "Mahindra Scorpio" },
          ],
        },
      },
      assertResult(body) {
        const blob = JSON.stringify(body || {});
        const answer = String(body.answer || "");
        const tool = body.aciCoreBridge?.tool || body.meta?.aciCoreBridge?.tool || body.tool;
        const rows = body.rows || body.data?.rows || body.variants || body.data?.variants || [];
        assert.strictEqual(tool, "vehicle_pricelist", `expected vehicle_pricelist, got ${tool}`);
        assert.notStrictEqual(body.canvasType, "comparison_canvas", "price query must not return comparison canvas");
        assert.strictEqual(body.canvasType, "pricelist_canvas", `expected pricelist_canvas, got ${body.canvasType}`);
        assert(!/\bcompared\b/i.test(answer), "price query must not answer as comparison");
        assert(!/could not find confirmed price rows/i.test(answer), "price rows exist, but answer says rows were not found");
        assert(scorpioNPriceRowCount > 0, "expected Scorpio N DB price rows in new-delhi");
        assert(Array.isArray(rows) && rows.length > 0, "response must include price rows");
        assert(/scorpio\s*n/i.test(blob), "response must mention Scorpio N");
        assert(/₹|price/i.test(blob), "response must include price labels or rupee pricing");
      },
    },
  ];

  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();
    try {
      const output = await runAciCoreLiveBridge({
        message: testCase.message,
        context: testCase.context,
        user: null,
        session: {},
        meta: { source: "auditAciBuyerAnswerQualityV1" },
      });

      testCase.assertResult(output);

      results.push({
        id: testCase.id,
        pass: true,
        durationMs: Date.now() - startedAt,
        summary: {
          intent: output.intent,
          tool: output.aciCoreBridge?.tool || output.meta?.aciCoreBridge?.tool || output.tool,
          operation:
            output.operation ||
            output.data?.operation ||
            output.meta?.scoreInsightOperation ||
            output.aciCoreBridge?.operation ||
            "",
          canvasType: output.canvasType,
          answerPreview: String(output.answer || "").slice(0, 300),
          rowsCount: Array.isArray(output.rows || output.data?.rows)
            ? (output.rows || output.data?.rows).length
            : 0,
        },
      });
    } catch (error) {
      results.push({
        id: testCase.id,
        pass: false,
        durationMs: Date.now() - startedAt,
        failures: [error.message || String(error)],
      });
    }
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI Buyer Answer Quality Audit v1",
    ok: failed.length === 0,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
  }, null, 2));

  if (failed.length) process.exit(1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
}).finally(async () => {
  if (mongoose.connection?.readyState) {
    await mongoose.disconnect();
  }
});
