import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";

import {
  retrieveAciDbCandidates,
  clearAciCandidateRetrieverCaches,
} from "../../services/aciCore/candidates/aciDbCandidateRetriever.js";

import {
  prewarmAciCoreRuntime,
} from "../../services/aciCore/aciCore.prewarm.js";

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
  executeAciPlannerPlan,
} from "../../services/aiAgent/aiAgent.executor.js";

import {
  normalizeAciFinalResponse,
} from "../../services/aiAgent/aiAgent.contractNormalizer.js";

process.env.ACI_MEANING_PARSER_ENABLED = "false";

const cases = [
  {
    id: "broad-hyundai-sunroof-budget",
    message: "Hyundai cars with sunroof under 20 lakh",
    expectedTool: "vehicle_recommend",
    expectedIntentIncludes: ["recommend", "vehicle", "feature"],
    minMatched: 1,
    forbiddenAnswerPatterns: [
      "could not find variants with \\.",
      "with \\.$",
      "undefined",
      "null",
    ],
  },
  {
    id: "variant-comparison",
    message: "Verna HX8 iVT vs City ZX CVT",
    expectedTool: "vehicle_compare",
    expectedIntentIncludes: ["comparison"],
  },
  {
    id: "extreme-multi-intent",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
    expectedTool: "vehicle_feature_comparison",
    expectedIntentIncludes: ["feature", "comparison"],
    minMatched: 1,
    forbiddenAnswerPatterns: [
      "could not find",
      "undefined",
      "null",
    ],
  },
  {
    id: "onroad-price",
    message: "Creta SX on-road price Delhi",
    expectedTool: "vehicle_pricelist",
    expectedIntentIncludes: ["price"],
    minMatched: 1,
    forbiddenAnswerPatterns: [
      "could not find",
      "undefined",
      "null",
    ],
  },
];

const asArray = (value) => Array.isArray(value) ? value.filter(Boolean) : [];

const lower = (value) => String(value || "").toLowerCase();

const includesAny = (value = "", terms = []) =>
  terms.some((term) => lower(value).includes(lower(term)));

const getFirstTool = (plan = {}) => asArray(plan.tools)[0] || {};

const getRuntimeMeta = (response = {}) =>
  asArray(response.executor?.runtimeResultsMeta || response.runtimeResultsMeta);

const checkResponse = ({
  caseId = "",
  response = {},
  plan = {},
  expectedTool = "",
  expectedIntentIncludes = [],
  expectedCanvasType = "",
  minMatched = null,
  maxMatched = null,
  expectedPrimaryVariant = "",
  forbidModelLevelPricelist = false,
  forbiddenAnswerPatterns = [],
} = {}) => {
  const failures = [];
  const tool = getFirstTool(plan);
  const runtimeMeta = getRuntimeMeta(response);
  const answerText = `${response.title || ""} ${response.answer || ""}`;

  if (tool.tool !== expectedTool) {
    failures.push(`plan tool expected ${expectedTool}, got ${tool.tool}`);
  }

  if (response.planner?.validation?.valid === false) {
    failures.push(`executor planner validation failed: ${(response.planner?.validation?.errors || []).join("; ")}`);
  }

  if (!response.intent) {
    failures.push("response.intent missing");
  }

  if (expectedIntentIncludes.length && response.intent && !includesAny(response.intent, expectedIntentIncludes)) {
    failures.push(`response.intent expected to include one of ${expectedIntentIncludes.join(", ")}, got ${response.intent}`);
  }

  if (!response.displayMode) {
    failures.push("response.displayMode missing");
  }

  if (!response.answer && !response.title) {
    failures.push("response missing answer/title");
  }

  if (expectedCanvasType && response.canvasType !== expectedCanvasType) {
    failures.push(`canvasType expected ${expectedCanvasType}, got ${response.canvasType}`);
  }

  if (!runtimeMeta.length) {
    failures.push("runtimeResultsMeta missing");
  }

  const totalMatched = runtimeMeta.reduce((sum, item) => sum + Number(item.matched || 0), 0);

  if (caseId === "onroad-price") {
    if (totalMatched > 3) {
      failures.push(`specific on-road variant query returned too many rows: ${totalMatched}`);
    }

    if (response.canvasType === "pricelist_canvas" && totalMatched > 3) {
      failures.push(`specific on-road variant query degraded to model-level pricelist with ${totalMatched} rows`);
    }

    const answerTextLower = answerText.toLowerCase();
    if (
      answerTextLower.includes("price list") &&
      !answerTextLower.includes("on-road") &&
      !answerTextLower.includes("on road")
    ) {
      failures.push("specific on-road query returned generic price-list language");
    }
  }
  if (Number.isFinite(minMatched) && totalMatched < minMatched) {
    failures.push(`runtime matched expected >=${minMatched}, got ${totalMatched}`);
  }

  if (Number.isFinite(maxMatched) && totalMatched > maxMatched) {
    failures.push(`runtime matched expected <=${maxMatched}, got ${totalMatched}`);
  }

  if (expectedPrimaryVariant) {
    const primaryVariant =
      tool.entities?.primaryVariant ||
      tool.entities?.variant ||
      tool.filters?.variant ||
      response.contextPatch?.anchorVariant ||
      response.contextPatch?.selectedVehicle?.variant ||
      "";

    if (!lower(primaryVariant).includes(lower(expectedPrimaryVariant))) {
      failures.push(`primary variant expected ${expectedPrimaryVariant}, got ${primaryVariant}`);
    }
  }

  if (forbidModelLevelPricelist && response.canvasType === "pricelist_canvas" && totalMatched > 3) {
    failures.push(`specific variant/on-road query returned model-level pricelist with ${totalMatched} rows`);
  }

  const missingTool = runtimeMeta.find((item) => item.missingTool || item.dataSource === "missing_v2_tool");
  if (missingTool) {
    failures.push(`missing runtime tool: ${missingTool.tool || expectedTool}`);
  }

  const runtimeError = runtimeMeta.find((item) => item.dataSource === "runtime_error" || item.error);
  if (runtimeError) {
    failures.push(`runtime error in ${runtimeError.tool || expectedTool}: ${runtimeError.error || "unknown"}`);
  }

  for (const pattern of forbiddenAnswerPatterns) {
    const regex = new RegExp(pattern, "i");
    if (regex.test(answerText)) {
      failures.push(`forbidden answer pattern matched: ${pattern}`);
    }
  }

  if (totalMatched > 0 && /could not find|couldn't find|no matching|not find/i.test(answerText)) {
    failures.push(`response says no results even though runtime matched ${totalMatched}`);
  }

  return failures;
};

async function runCase(item) {
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

  const executed = await executeAciPlannerPlan({
    plan,
    userMessage: item.message,
    context: {},
    runtimeHints: {
      aciCore: {
        meaningFrame: understanding.meaningFrame,
        parserTrace: understanding.parserTrace,
      },
    },
  });

  const normalized = await normalizeAciFinalResponse(executed, {
    message: item.message,
    context: {},
  });

  const failures = checkResponse({
    caseId: item.id,
    response: normalized,
    plan,
    expectedTool: item.expectedTool,
    expectedIntentIncludes: item.expectedIntentIncludes,
    expectedCanvasType: item.expectedCanvasType,
    minMatched: item.minMatched,
    maxMatched: item.maxMatched,
    expectedPrimaryVariant: item.expectedPrimaryVariant,
    forbidModelLevelPricelist: Boolean(item.forbidModelLevelPricelist),
    forbiddenAnswerPatterns: item.forbiddenAnswerPatterns || [],
  });

  return {
    id: item.id,
    message: item.message,
    pass: failures.length === 0,
    durationMs: Date.now() - startedAt,
    failures,
    summary: {
      primaryTask: understanding.meaningFrame?.primaryTask,
      selectedParser: understanding.meaningFrame?.parserTrace?.router?.selectedParser ||
        understanding.parserTrace?.router?.selectedParser ||
        "",
      usedGemini: Boolean(
        understanding.meaningFrame?.parserTrace?.router?.usedGemini ||
        understanding.parserTrace?.router?.usedGemini,
      ),
      tool: getFirstTool(plan).tool,
      intent: normalized.intent,
      displayMode: normalized.displayMode,
      canvasType: normalized.canvasType,
      inlineType: normalized.inlineType,
      title: normalized.title,
      answer: normalized.answer,
      matched: normalized.executor?.runtimeResultsMeta?.[0]?.matched,
      dataSource: normalized.executor?.runtimeResultsMeta?.[0]?.dataSource,
      modulesChecked: normalized.executor?.runtimeResultsMeta?.[0]?.modulesChecked,
      runtimeResultsMeta: getRuntimeMeta(normalized),
    },
  };
}

async function main() {
  await connectDB();

  clearAciCandidateRetrieverCaches();
  await prewarmAciCoreRuntime({ force: true });

  const results = [];
  const failures = [];

  for (const item of cases) {
    const result = await runCase(item);
    results.push(result);

    if (!result.pass) {
      failures.push({
        id: result.id,
        message: result.message,
        failures: result.failures,
        summary: result.summary,
      });
    }
  }

  console.log(JSON.stringify({
    suite: "ACI Core legacy executor smoke",
    ok: failures.length === 0,
    total: cases.length,
    passed: cases.length - failures.length,
    failed: failures.length,
    failedIds: failures.map((item) => item.id),
    failures,
    results: results.map((item) => ({
      id: item.id,
      message: item.message,
      pass: item.pass,
      durationMs: item.durationMs,
      failures: item.failures,
      summary: item.summary,
    })),
  }, null, 2));

  await mongoose.disconnect();

  if (failures.length) process.exit(1);
}

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Core legacy executor smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
