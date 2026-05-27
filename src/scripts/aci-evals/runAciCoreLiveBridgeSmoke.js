import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import { prewarmAciCoreRuntime } from "../../services/aciCore/aciCore.prewarm.js";
import { runAciCoreLiveBridge } from "../../services/aciCore/integration/aciCoreLiveBridge.service.js";

const CASES = [
  {
    id: "broad-feature-discovery",
    message: "Hyundai cars with sunroof under 20 lakh",
    expectedIntentIncludes: ["feature"],
    expectedCanvasType: "feature_match_builder_canvas",
    minMatched: 1,
  },
  {
    id: "variant-comparison",
    message: "Verna HX8 iVT vs City ZX CVT",
    expectedIntentIncludes: ["comparison"],
    expectedCanvasType: "comparison_canvas",
    minMatched: 1,
  },
  {
    id: "feature-comparison",
    message: "Punch and Nexon CNG sunroof ABS ADAS",
    expectedIntentIncludes: ["feature", "comparison"],
    expectedCanvasType: "feature_comparison_canvas",
    minMatched: 1,
  },
  {
    id: "exact-onroad-price",
    message: "Creta SX on-road price Delhi",
    expectedIntentIncludes: ["price"],
    minMatched: 1,
  },
];

const text = (value = "") => String(value || "").toLowerCase();

const getMatched = (response = {}) =>
  Number(response.matched || 0) ||
  Number(response.count || 0) ||
  Number(response.meta?.recordCount || 0) ||
  Number(response.sourceTransparency?.recordCount || 0) ||
  Number(response.executor?.runtimeResultsMeta?.[0]?.matched || 0) ||
  Number(response.runtimeResultsMeta?.[0]?.matched || 0) ||
  Number(response.rows?.length || 0) ||
  Number(response.items?.length || 0) ||
  Number(response.data?.rows?.length || 0) ||
  Number(response.data?.items?.length || 0);

const runCase = async (item) => {
  const startedAt = Date.now();
  const response = await runAciCoreLiveBridge({
    message: item.message,
    context: {},
  });

  const failures = [];
  const intent = text(response.intent);
  const canvasType = response.canvasType || response.widget?.canvasType || "";
  const matched = getMatched(response);
  const answer = String(response.answer || "");

  for (const part of item.expectedIntentIncludes || []) {
    if (!intent.includes(part)) {
      failures.push(`intent expected to include ${part}, got ${response.intent}`);
    }
  }

  if (item.expectedCanvasType && canvasType !== item.expectedCanvasType) {
    failures.push(`canvasType expected ${item.expectedCanvasType}, got ${canvasType}`);
  }

  if (matched < item.minMatched) {
    failures.push(`matched expected >=${item.minMatched}, got ${matched}`);
  }

  if (matched > 0 && /could not find|couldn't find|no matching|not find/i.test(answer)) {
    failures.push(`answer says no results even though matched ${matched}`);
  }

  return {
    id: item.id,
    message: item.message,
    pass: failures.length === 0,
    durationMs: Date.now() - startedAt,
    failures,
    summary: {
      intent: response.intent,
      displayMode: response.displayMode,
      canvasType,
      inlineType: response.inlineType,
      title: response.title,
      answer,
      matched,
      aciCoreBridge: response.aciCoreBridge || response.meta?.aciCoreBridge || null,
      modulesChecked:
        response.modulesChecked ||
        response.sourceTransparency?.modulesChecked ||
        response.runtimeResultsMeta?.[0]?.modulesChecked ||
        [],
    },
  };
};

const main = async () => {
  await connectDB();
  await prewarmAciCoreRuntime({ force: true });

  const results = [];
  for (const item of CASES) {
    results.push(await runCase(item));
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI Core live bridge smoke",
    ok: failed.length === 0,
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    failures: failed.map((item) => ({
      id: item.id,
      message: item.message,
      failures: item.failures,
      summary: item.summary,
    })),
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failed.length) process.exit(1);
};

main().catch(async (error) => {
  console.error(JSON.stringify({
    suite: "ACI Core live bridge smoke",
    ok: false,
    error: error.message,
    stack: error.stack,
  }, null, 2));

  try {
    await mongoose.disconnect();
  } catch {}

  process.exit(1);
});
