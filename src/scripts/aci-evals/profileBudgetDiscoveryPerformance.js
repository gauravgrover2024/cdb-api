import "dotenv/config";
import mongoose from "mongoose";
import { performance } from "node:perf_hooks";

import connectDB from "../../config/db.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";
import { runtimeBudgetVehicleDiscovery } from "../../services/aiAgent/aiAgent.executor.js";

const BUDGET_MAX = 2000000;

const cases = [
  {
    id: "cars-under-20l",
    message: "cars under 20 lakhs",
    toolPlan: { filters: { budgetMax: BUDGET_MAX }, entities: {} },
  },
  {
    id: "automatic-cars-under-20l",
    message: "automatic cars under 20 lakhs",
    toolPlan: { filters: { budgetMax: BUDGET_MAX, transmission: "automatic" }, entities: {} },
  },
  {
    id: "suvs-under-20l",
    message: "SUVs under 20 lakhs",
    toolPlan: { filters: { budgetMax: BUDGET_MAX, bodyType: "SUV" }, entities: {} },
  },
  {
    id: "best-automatic-suvs-under-20l",
    message: "best automatic SUVs under 20 lakhs",
    toolPlan: { filters: { budgetMax: BUDGET_MAX, bodyType: "SUV", transmission: "automatic" }, entities: {} },
  },
];

const now = () => performance.now();

async function time(label, fn) {
  const start = now();
  try {
    const result = await fn();
    return { label, ok: true, durationMs: Math.round(now() - start), result };
  } catch (error) {
    return {
      label,
      ok: false,
      durationMs: Math.round(now() - start),
      error: error?.stack || error?.message || String(error),
    };
  }
}

function summarizeBudgetRuntime(result = {}) {
  const rows = result.rows || [];
  const groups = result.modelGroups || [];
  const budget = result.budgetDiscovery || {};

  return {
    rowCount: rows.length,
    modelGroupCount: groups.length,
    matched: result.matched,
    count: result.count,
    totalQualifyingModels: budget.totalQualifyingModels,
    totalUniqueQualifyingVariants: budget.totalUniqueQualifyingVariants,
    totalQualifyingPriceRows: budget.totalQualifyingPriceRows,
    returnedPreviewGroups: budget.returnedPreviewGroups,
    diversifiedPreview: budget.diversifiedPreview,
    previewBodyTypeGroups: budget.previewBodyTypeGroups,
    source: result.source,
    dataSource: result.dataSource,
  };
}

function summarizeChat(response = {}) {
  const rows = response.data?.rows || response.rows || [];
  const groups = response.data?.modelGroups || response.modelGroups || [];
  const budget = response.data?.budgetDiscovery || response.budgetDiscovery || {};
  const bridge = response.aciCoreBridge || response.meta?.aciCoreBridge || {};

  return {
    intent: response.intent,
    canvasType: response.canvasType,
    answer: response.answer,
    rowCount: rows.length,
    modelGroupCount: groups.length,
    matched: response.matched,
    totalQualifyingModels: budget.totalQualifyingModels,
    totalUniqueQualifyingVariants: budget.totalUniqueQualifyingVariants,
    totalQualifyingPriceRows: budget.totalQualifyingPriceRows,
    returnedPreviewGroups: budget.returnedPreviewGroups,
    previewBodyTypeGroups: budget.previewBodyTypeGroups,
    bridgeDurationMs: bridge.durationMs,
    bridgePrimaryTask: bridge.primaryTask,
    bridgeTool: bridge.tool,
    modulesChecked: response.sourceTransparency?.modulesChecked || [],
  };
}

async function explainPriceRows(db) {
  const collection = db.collection("aci_vehicle_price_rows");

  const indexes = await collection.indexes();

  const explain = await collection
    .find(
      {
        exShowroomPrice: { $gt: 0, $lte: BUDGET_MAX },
      },
      {
        projection: {
          make: 1,
          makeKey: 1,
          model: 1,
          modelKey: 1,
          fullModel: 1,
          variant: 1,
          variantKey: 1,
          city: 1,
          citySlug: 1,
          fuel: 1,
          fuelType: 1,
          fuelKey: 1,
          transmission: 1,
          transmissionKey: 1,
          gearbox: 1,
          gearboxKey: 1,
          bodyType: 1,
          bodyTypeKey: 1,
          segment: 1,
          exShowroomPrice: 1,
          exShowroomPriceLabel: 1,
        },
      },
    )
    .sort({ exShowroomPrice: 1, make: 1, model: 1, variant: 1 })
    .limit(6000)
    .explain("executionStats");

  return {
    indexes: indexes.map((idx) => ({ name: idx.name, key: idx.key })),
    winningPlan: explain.queryPlanner?.winningPlan,
    executionStats: {
      executionTimeMillis: explain.executionStats?.executionTimeMillis,
      totalKeysExamined: explain.executionStats?.totalKeysExamined,
      totalDocsExamined: explain.executionStats?.totalDocsExamined,
      nReturned: explain.executionStats?.nReturned,
    },
  };
}

await connectDB();

const db = mongoose.connection.db;

console.log("\n===== Mongo explain: aci_vehicle_price_rows budget query =====");
console.log(JSON.stringify(await explainPriceRows(db), null, 2));

console.log("\n===== Warm-up one call =====");
await runtimeBudgetVehicleDiscovery({ toolPlan: cases[0].toolPlan, context: {} });

for (const testCase of cases) {
  console.log(`\n==============================`);
  console.log(`CASE: ${testCase.id}`);

  const direct = await time("direct runtimeBudgetVehicleDiscovery", () =>
    runtimeBudgetVehicleDiscovery({
      toolPlan: testCase.toolPlan,
      context: {},
    }),
  );

  console.log(JSON.stringify({
    phase: direct.label,
    ok: direct.ok,
    durationMs: direct.durationMs,
    error: direct.error,
    summary: direct.ok ? summarizeBudgetRuntime(direct.result) : null,
  }, null, 2));

  const chat = await time("chatWithAgent live bridge", () =>
    chatWithAgent({
      message: testCase.message,
      context: {},
    }),
  );

  console.log(JSON.stringify({
    phase: chat.label,
    ok: chat.ok,
    durationMs: chat.durationMs,
    error: chat.error,
    summary: chat.ok ? summarizeChat(chat.result) : null,
  }, null, 2));
}

await mongoose.disconnect();
