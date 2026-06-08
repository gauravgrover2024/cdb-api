#!/usr/bin/env node

require("dotenv/config");

const assert = require("assert");

const CASE_TIMEOUT_MS = Number(process.env.ACI_EXACT_PRICE_CASE_TIMEOUT_MS || 3500);
const MAX_DURATION_MS = Number(process.env.ACI_EXACT_PRICE_MAX_DURATION_MS || 3000);
const AVG_DURATION_MS = Number(process.env.ACI_EXACT_PRICE_AVG_DURATION_MS || 1800);

const cases = [
  {
    id: "creta-sx-on-road-delhi",
    message: "Creta SX on-road price Delhi",
    expectedTitleIncludes: ["Hyundai Creta", "SX"],
    expectedCanvasType: "price_breakup_canvas",
    expectedCity: "new-delhi",
  },
  {
    id: "creta-sx-price-new-delhi",
    message: "Creta SX price New Delhi",
    expectedTitleIncludes: ["Hyundai Creta", "SX"],
    expectedCity: "new-delhi",
  },
  {
    id: "creta-sx-on-road-gurgaon",
    message: "Creta SX on road Gurgaon",
    expectedTitleIncludes: ["Hyundai Creta", "SX"],
    expectedCanvasType: "price_breakup_canvas",
    expectedCity: "gurgaon",
  },
  {
    id: "creta-sx-price-noida",
    message: "Creta SX price Noida",
    expectedTitleIncludes: ["Hyundai Creta", "SX"],
    expectedCity: "noida",
  },
  {
    id: "hyundai-creta-sx-on-road-delhi",
    message: "Hyundai Creta SX on-road price Delhi",
    expectedTitleIncludes: ["Hyundai Creta", "SX"],
    expectedCanvasType: "price_breakup_canvas",
    expectedCity: "new-delhi",
  },
  {
    id: "seltos-hte-on-road-delhi",
    message: "Seltos HTE on-road price Delhi",
    expectedTitleIncludes: ["Kia Seltos", "HTE"],
    expectedCanvasType: "price_breakup_canvas",
    expectedCity: "new-delhi",
  },
  {
    id: "kia-seltos-hte-price-gurgaon",
    message: "Kia Seltos HTE price Gurgaon",
    expectedTitleIncludes: ["Kia Seltos", "HTE"],
    expectedCity: "gurgaon",
  },
  {
    id: "tata-punch-adventure-s-price-delhi",
    message: "Tata Punch Adventure S price Delhi",
    expectedTitleIncludes: ["Tata Punch", "Adventure S"],
    expectedCity: "new-delhi",
  },
];

const asArray = (value) => Array.isArray(value) ? value : value ? [value] : [];
const text = (value = "") => String(value || "").trim();

const withTimeout = async (promise, ms, label) =>
  Promise.race([
    promise,
    new Promise((_, reject) =>
      setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms),
    ),
  ]);

const getRows = (response = {}) =>
  asArray(response.rows || response.items || response.data?.rows || response.data?.items);

const getBridge = (response = {}) =>
  response.aciCoreBridge || response.meta?.aciCoreBridge || {};

const validateCase = ({ testCase, response, durationMs }) => {
  const failures = [];
  const rows = getRows(response);
  const bridge = getBridge(response);
  const selectedVehicle = response.contextPatch?.selectedVehicle || response.data?.selectedVehicle || {};

  const check = (fn) => {
    try {
      fn();
    } catch (error) {
      failures.push(error?.message || String(error));
    }
  };

  check(() => assert.strictEqual(response.intent, "vehicle_pricelist", "intent must be vehicle_pricelist"));
  check(() => assert(rows.length === 1, `expected exactly 1 price row, got ${rows.length}`));
  check(() => assert.notStrictEqual(response.canvasType, "unsupported_city_canvas", "must not return unsupported city canvas"));
  check(() => assert.strictEqual(bridge.contextIsolation, "supported_exact_price_fast_path", `expected supported_exact_price_fast_path, got ${bridge.contextIsolation || ""}`));
  check(() => assert.strictEqual(bridge.tool, "vehicle_pricelist", `expected vehicle_pricelist tool, got ${bridge.tool || ""}`));
  check(() => assert.strictEqual(bridge.planMode, "single_tool", `expected single_tool planMode, got ${bridge.planMode || ""}`));
  check(() => assert.strictEqual(bridge.usedGemini, false, "exact price fast path must not use Gemini"));
  check(() => assert(durationMs <= MAX_DURATION_MS, `duration ${durationMs}ms exceeds ${MAX_DURATION_MS}ms`));

  if (testCase.expectedCanvasType) {
    check(() => assert.strictEqual(response.canvasType, testCase.expectedCanvasType, `expected canvasType ${testCase.expectedCanvasType}, got ${response.canvasType || ""}`));
  } else {
    check(() => assert(["price_breakup_canvas", "pricelist_canvas"].includes(response.canvasType), `unexpected canvasType ${response.canvasType || ""}`));
  }

  const blob = JSON.stringify({
    title: response.title,
    answer: response.answer,
    selectedVehicle,
    rows,
  });

  for (const expected of testCase.expectedTitleIncludes || []) {
    check(() => assert(new RegExp(expected, "i").test(blob), `expected output to include ${expected}`));
  }

  if (testCase.expectedCity) {
    check(() => assert.strictEqual(selectedVehicle.citySlug || rows[0]?.citySlug || rows[0]?.city, testCase.expectedCity, `expected city ${testCase.expectedCity}, got ${selectedVehicle.citySlug || rows[0]?.citySlug || rows[0]?.city || ""}`));
  }

  return failures;
};

async function main() {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") throw new Error("connectDB export not found");
  await connectDB();

  const bridgeMod = await import("../../services/aciCore/integration/aciCoreLiveBridge.service.js");
  const runAciCoreLiveBridge = bridgeMod.runAciCoreLiveBridge || bridgeMod.default;
  if (typeof runAciCoreLiveBridge !== "function") throw new Error("runAciCoreLiveBridge export not found");

  const startedAt = Date.now();
  const results = [];

  for (const testCase of cases) {
    const caseStartedAt = Date.now();

    let response = null;
    let failures = [];

    try {
      response = await withTimeout(
        runAciCoreLiveBridge({
          message: testCase.message,
          context: {},
          user: null,
          session: {},
          meta: { source: "auditAciExactPricePerformanceV1", caseId: testCase.id },
        }),
        CASE_TIMEOUT_MS,
        testCase.id,
      );

      const durationMs = Date.now() - caseStartedAt;
      failures = validateCase({ testCase, response, durationMs });

      results.push({
        id: testCase.id,
        message: testCase.message,
        pass: failures.length === 0,
        durationMs,
        failures,
        summary: {
          intent: response.intent || "",
          canvasType: response.canvasType || "",
          title: response.title || "",
          rowCount: getRows(response).length,
          selectedVehicle: response.contextPatch?.selectedVehicle || response.data?.selectedVehicle || {},
          aciCoreBridge: getBridge(response),
          answerPreview: text(response.answer).slice(0, 220),
        },
      });
    } catch (error) {
      results.push({
        id: testCase.id,
        message: testCase.message,
        pass: false,
        durationMs: Date.now() - caseStartedAt,
        failures: [error?.message || String(error)],
        summary: {},
      });
    }
  }

  const durations = results.map((item) => item.durationMs);
  const failed = results.filter((item) => !item.pass);
  const avgDurationMs = Math.round(durations.reduce((sum, value) => sum + value, 0) / durations.length);
  const maxDurationMs = Math.max(...durations);

  const performanceFailures = [];
  if (avgDurationMs > AVG_DURATION_MS) {
    performanceFailures.push(`avgDurationMs ${avgDurationMs} exceeds ${AVG_DURATION_MS}`);
  }
  if (maxDurationMs > MAX_DURATION_MS) {
    performanceFailures.push(`maxDurationMs ${maxDurationMs} exceeds ${MAX_DURATION_MS}`);
  }

  const output = {
    suite: "ACI Exact Supported-City Price Performance Audit v1",
    ok: failed.length === 0 && performanceFailures.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length + performanceFailures.length,
    failedIds: [
      ...failed.map((item) => item.id),
      ...(performanceFailures.length ? ["exact_price_performance_budget"] : []),
    ],
    avgDurationMs,
    maxDurationMs,
    thresholds: {
      caseTimeoutMs: CASE_TIMEOUT_MS,
      maxDurationMs: MAX_DURATION_MS,
      avgDurationMs: AVG_DURATION_MS,
    },
    performanceFailures,
    results,
    durationMs: Date.now() - startedAt,
  };

  console.log(JSON.stringify(output, null, 2));

  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) await mongoose.disconnect();

  process.exit(output.ok ? 0 : 1);
}

main().catch(async (error) => {
  console.error(error?.stack || error?.message || String(error));
  const mongoose = require("mongoose");
  if (mongoose.connection?.readyState) await mongoose.disconnect();
  process.exit(1);
});
