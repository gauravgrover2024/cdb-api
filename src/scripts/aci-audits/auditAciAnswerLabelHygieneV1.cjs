#!/usr/bin/env node

require("dotenv/config");

const assert = require("assert");

const text = (value = "") => String(value || "").trim();
const lower = (value = "") => text(value).toLowerCase();
const asArray = (value) => (Array.isArray(value) ? value : value ? [value] : []);

const getRows = (response = {}) =>
  asArray(
    response.rows ||
      response.items ||
      response.data?.rows ||
      response.data?.items ||
      response.data?.results ||
      response.results,
  );

const getBlob = (response = {}) =>
  [
    response.intent,
    response.canvasType,
    response.title,
    response.answer,
    response.data?.title,
    response.data?.answer,
    JSON.stringify(response.contextPatch || {}),
  ].join(" ");

const duplicatePatterns = [
  {
    id: "duplicate_tata_prefix",
    pattern: /\bTata\s+Tata\b/i,
    description: "Duplicate Tata make prefix.",
  },
  {
    id: "duplicate_hyundai_prefix",
    pattern: /\bHyundai\s+Hyundai\b/i,
    description: "Duplicate Hyundai make prefix.",
  },
  {
    id: "duplicate_kia_prefix",
    pattern: /\bKia\s+Kia\b/i,
    description: "Duplicate Kia make prefix.",
  },
  {
    id: "duplicated_comparison_pair_creta_seltos",
    pattern: /\b(?:Hyundai\s+Creta\s+vs\s+Kia\s+Seltos|Kia\s+Seltos\s+vs\s+Hyundai\s+Creta)\s+vs\s+(?:Hyundai\s+Creta\s+vs\s+Kia\s+Seltos|Kia\s+Seltos\s+vs\s+Hyundai\s+Creta)\b/i,
    description: "Comparison title repeats the same model pair.",
  },
  {
    id: "double_vs_chain_same_pair",
    pattern: /\b(vs)\b[\s\S]*\b(vs)\b[\s\S]*\b(vs)\b/i,
    description: "Comparison label has more than one pair join for a two-car comparison.",
  },
];

const cases = [
  {
    id: "comparison-initial-title",
    messages: ["Creta vs Seltos"],
    expectedCanvasType: "comparison_canvas",
  },
  {
    id: "comparison-price-difference-title",
    messages: ["Creta vs Seltos", "price difference"],
    expectedCanvasType: "comparison_canvas",
  },
  {
    id: "comparison-which-one-title",
    messages: ["Creta vs Seltos", "which one?"],
    expectedCanvasType: "comparison_canvas",
  },
  {
    id: "tata-punch-mileage-label",
    messages: ["Tata Punch mileage"],
    expectedIntent: "vehicle_spec_attribute_answer",
  },
  {
    id: "unsupported-first-price-label",
    messages: ["creta price in mumbai", "delhi price"],
    expectedIntent: "vehicle_pricelist",
  },
];

const hasVehicle = (vehicle = {}) =>
  Boolean(text(vehicle.make || vehicle.brand) || text(vehicle.model) || text(vehicle.fullModel || vehicle.displayName));

const mergeContext = (context = {}, response = {}) => {
  const patch = response.contextPatch && typeof response.contextPatch === "object" ? response.contextPatch : {};
  const nextVehicle =
    patch.selectedVehicle ||
    response.data?.selectedVehicle ||
    response.selectedVehicle ||
    {};

  return {
    ...context,
    ...patch,
    selectedVehicle: hasVehicle(nextVehicle) ? nextVehicle : context.selectedVehicle || {},
    ...(patch.selectedComparisonSet ? { selectedComparisonSet: patch.selectedComparisonSet } : {}),
    ...(patch.activeComparison ? { activeComparison: patch.activeComparison } : {}),
  };
};

const assertLabelHygiene = ({ testCase, response }) => {
  const blob = getBlob(response);

  for (const pattern of duplicatePatterns) {
    if (pattern.id === "double_vs_chain_same_pair" && response.canvasType !== "comparison_canvas") {
      continue;
    }

    assert(
      !pattern.pattern.test(blob),
      `${testCase.id}: ${pattern.description} Matched ${pattern.id}. Title=${JSON.stringify(response.title)} Answer=${JSON.stringify(String(response.answer || "").slice(0, 220))}`,
    );
  }

  if (response.canvasType === "comparison_canvas") {
    const title = text(response.title || response.data?.title || "");
    const vsCount = (title.match(/\bvs\b/gi) || []).length;
    assert(vsCount <= 1, `${testCase.id}: comparison title has ${vsCount} vs joins: ${title}`);

    const rows = getRows(response);
    assert(rows.length <= 2, `${testCase.id}: two-car comparison returned ${rows.length} rows`);
  }

  const title = text(response.title || response.data?.title || "");
  assert(!/\s{2,}/.test(title), `${testCase.id}: title has repeated spaces: ${JSON.stringify(title)}`);
};

async function main() {
  const dbMod = await import("../../config/db.js");
  const connectDB = dbMod.default || dbMod.connectDB || dbMod;
  if (typeof connectDB !== "function") throw new Error("connectDB export not found");
  await connectDB();

  const serviceMod = await import("../../services/aiAgent/aiAgent.service.js");
  const chatWithAgent = serviceMod.chatWithAgent || serviceMod.default;
  if (typeof chatWithAgent !== "function") throw new Error("chatWithAgent export not found");

  const results = [];
  const startedAt = Date.now();

  for (const testCase of cases) {
    let context = {};
    let response = null;
    const failures = [];
    const stepSummaries = [];

    for (const message of testCase.messages) {
      response = await chatWithAgent({
        message,
        context,
        user: null,
        session: {},
        meta: {
          source: "auditAciAnswerLabelHygieneV1",
          caseId: testCase.id,
        },
      });

      stepSummaries.push({
        message,
        intent: response.intent || "",
        canvasType: response.canvasType || "",
        title: response.title || response.data?.title || "",
        answerPreview: String(response.answer || response.data?.answer || "").slice(0, 220),
        rowCount: getRows(response).length,
      });

      context = mergeContext(context, response);
    }

    try {
      if (testCase.expectedCanvasType) {
        assert.strictEqual(
          response.canvasType,
          testCase.expectedCanvasType,
          `${testCase.id}: expected canvasType ${testCase.expectedCanvasType}, got ${response.canvasType}`,
        );
      }

      if (testCase.expectedIntent) {
        assert.strictEqual(
          response.intent,
          testCase.expectedIntent,
          `${testCase.id}: expected intent ${testCase.expectedIntent}, got ${response.intent}`,
        );
      }

      assertLabelHygiene({ testCase, response });
    } catch (error) {
      failures.push(error?.message || String(error));
    }

    results.push({
      id: testCase.id,
      pass: failures.length === 0,
      failures,
      steps: stepSummaries,
    });
  }

  const failed = results.filter((item) => !item.pass);

  const output = {
    suite: "ACI Answer Label Hygiene Audit v1",
    ok: failed.length === 0,
    backendOnly: true,
    frontendEvaluated: false,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    durationMs: Date.now() - startedAt,
    results,
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
