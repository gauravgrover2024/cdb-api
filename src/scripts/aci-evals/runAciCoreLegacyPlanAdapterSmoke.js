import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";

import {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
} from "../../services/aciCore/candidates/aciDbCandidateRetriever.js";

import {
  parseHybridMeaningFrame,
} from "../../services/aciCore/understanding/hybridMeaningFrame.parser.js";

import {
  runAciUnderstandingEngine,
} from "../../services/aciCore/understanding/aciUnderstandingEngine.js";

import {
  buildLegacyPlanFromAciMeaningFrame,
} from "../../services/aciCore/integration/aciCoreToLegacyPlan.adapter.js";

import {
  validatePlannerPlan,
} from "../../services/aiAgent/aiAgent.planSchema.js";

process.env.ACI_MEANING_PARSER_ENABLED = "false";

const cases = [
  {
    id: "broad-hyundai-sunroof-budget",
    message: "Hyundai cars with sunroof under 20 lakh",
    expected: {
      primaryTask: "vehicle_discovery",
      tool: "vehicle_recommend",
      mode: "single_tool",
      conversationMode: "recommendation",
      make: "Hyundai",
      feature: "sunroof",
      budgetMax: 2000000,
    },
  },
  {
    id: "variant-comparison",
    message: "Verna HX8 iVT vs City ZX CVT",
    expected: {
      primaryTask: "vehicle_comparison",
      tool: "vehicle_compare",
      mode: "single_tool",
      conversationMode: "comparison",
      models: ["Hyundai Verna", "Honda City"],
      variants: ["Hyundai Verna HX8 iVT", "Honda City ZX CVT"],
    },
  },
  {
    id: "extreme-multi-intent",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
    expected: {
      primaryTask: "vehicle_comparison",
      tool: "vehicle_feature_comparison",
      mode: "single_tool",
      conversationMode: "comparison",
      models: ["Tata Punch", "Tata Nexon"],
      features: ["sunroof", "adas_package", "anti_lock_braking_system_abs"],
      fuelType: "cng",
    },
  },
  {
    id: "onroad-price",
    message: "Creta SX on-road price Delhi",
    expected: {
      primaryTask: "on_road_estimate",
      tool: "vehicle_pricelist",
      mode: "single_tool",
      conversationMode: "direct_answer",
      model: "Hyundai Creta",
      variant: "Hyundai Creta SX",
      priceBasis: "on_road",
    },
  },
];

const lower = (value) => String(value || "").toLowerCase();

const includesText = (values = [], expected = "") =>
  values.map(lower).join(" | ").includes(lower(expected));

const includesAll = (values = [], expected = []) =>
  expected.every((item) => includesText(values, item));

const asArray = (value) => Array.isArray(value) ? value.filter(Boolean) : [];

const firstTool = (plan = {}) => asArray(plan.tools)[0] || {};

const checkCase = ({ output, expected }) => {
  const failures = [];
  const frame = output.meaningFrame || {};
  const plan = output.plan || {};
  const tool = firstTool(plan);
  const entities = tool.entities || {};
  const filters = tool.filters || {};
  const validation = output.validation || {};

  if (!validation.valid) {
    failures.push(`plan validation failed: ${(validation.errors || []).join("; ")}`);
  }

  if (expected.primaryTask && frame.primaryTask !== expected.primaryTask) {
    failures.push(`primaryTask expected ${expected.primaryTask}, got ${frame.primaryTask}`);
  }

  if (expected.tool && tool.tool !== expected.tool) {
    failures.push(`tool expected ${expected.tool}, got ${tool.tool}`);
  }

  if (expected.mode && plan.mode !== expected.mode) {
    failures.push(`mode expected ${expected.mode}, got ${plan.mode}`);
  }

  if (expected.conversationMode && plan.conversationMode !== expected.conversationMode) {
    failures.push(`conversationMode expected ${expected.conversationMode}, got ${plan.conversationMode}`);
  }

  if (expected.make && lower(entities.make || filters.make) !== lower(expected.make)) {
    failures.push(`make expected ${expected.make}, got ${entities.make || filters.make}`);
  }

  if (expected.model && !includesText([entities.model, filters.model], expected.model)) {
    failures.push(`model expected ${expected.model}, got ${entities.model || filters.model}`);
  }

  if (expected.variant && !includesText([entities.variant, filters.variant], expected.variant)) {
    failures.push(`variant expected ${expected.variant}, got ${entities.variant || filters.variant}`);
  }

  if (expected.models && !includesAll([...(entities.models || []), ...(filters.models || [])], expected.models)) {
    failures.push(`models expected ${JSON.stringify(expected.models)}, got ${JSON.stringify(entities.models || filters.models || [])}`);
  }

  if (expected.variants && !includesAll([...(entities.variants || []), ...(filters.variants || [])], expected.variants)) {
    failures.push(`variants expected ${JSON.stringify(expected.variants)}, got ${JSON.stringify(entities.variants || filters.variants || [])}`);
  }

  if (expected.feature && !includesText([entities.feature, ...(entities.features || []), ...(filters.mustHaveFeatures || [])], expected.feature)) {
    failures.push(`feature expected ${expected.feature}, got ${JSON.stringify({ entities, filters })}`);
  }

  if (expected.features && !includesAll([...(entities.features || []), ...(filters.mustHaveFeatures || []), ...(filters.compareFeatures || [])], expected.features)) {
    failures.push(`features expected ${JSON.stringify(expected.features)}, got ${JSON.stringify({ entities, filters })}`);
  }

  if (expected.fuelType && lower(entities.fuelType || filters.fuelType) !== lower(expected.fuelType)) {
    failures.push(`fuelType expected ${expected.fuelType}, got ${entities.fuelType || filters.fuelType}`);
  }

  if (expected.budgetMax && Number(filters.budgetMax || 0) !== expected.budgetMax) {
    failures.push(`budgetMax expected ${expected.budgetMax}, got ${filters.budgetMax}`);
  }

  if (expected.priceBasis && filters.priceBasis !== expected.priceBasis) {
    failures.push(`priceBasis expected ${expected.priceBasis}, got ${filters.priceBasis}`);
  }

  return failures;
};

async function main() {
  await connectDB();
  clearAciCandidateRetrieverCaches();

  const results = [];
  const failures = [];

  for (const item of cases) {
    const startedAt = Date.now();

    const understanding = await runAciUnderstandingEngine({
      message: item.message,
      candidateRetriever: retrieveAciDbCandidates,
      parser: parseHybridMeaningFrame,
    });

    const plan = buildLegacyPlanFromAciMeaningFrame({
      meaningFrame: understanding.meaningFrame,
      message: item.message,
      context: {},
    });

    const validation = validatePlannerPlan(plan, { message: item.message });

    const output = {
      id: item.id,
      message: item.message,
      pass: false,
      durationMs: Date.now() - startedAt,
      meaningFrame: understanding.meaningFrame,
      plan: validation.plan || plan,
      validation: {
        valid: validation.valid,
        errors: validation.errors || [],
        warnings: validation.warnings || [],
      },
    };

    const caseFailures = checkCase({ output, expected: item.expected });

    output.pass = caseFailures.length === 0;
    output.failures = caseFailures;
    output.summary = {
      primaryTask: output.meaningFrame.primaryTask,
      tool: firstTool(output.plan).tool,
      mode: output.plan.mode,
      conversationMode: output.plan.conversationMode,
      entities: firstTool(output.plan).entities,
      filters: firstTool(output.plan).filters,
      output: firstTool(output.plan).output,
      plannerValidation: output.validation,
    };

    results.push(output.summary);

    if (caseFailures.length) {
      failures.push({
        id: item.id,
        message: item.message,
        failures: caseFailures,
        summary: output.summary,
      });
    }
  }

  console.log(JSON.stringify({
    suite: "ACI Core legacy planner-plan adapter smoke",
    ok: failures.length === 0,
    total: cases.length,
    passed: cases.length - failures.length,
    failed: failures.length,
    failedIds: failures.map((item) => item.id),
    failures,
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failures.length) process.exit(1);
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Core legacy planner-plan adapter smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
