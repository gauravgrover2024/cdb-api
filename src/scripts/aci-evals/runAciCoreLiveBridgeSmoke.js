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
  const rows = response.data?.rows || response.rows || response.items || [];
  const modelGroups = response.data?.modelGroups || response.modelGroups || [];

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

  if (item.id === "broad-feature-discovery") {
    if (!rows.length && !modelGroups.length) {
      failures.push("broad feature discovery returned no rows/modelGroups");
    }

    if (/^I found \d+ variants? with\b/i.test(answer.trim())) {
      failures.push("broad feature discovery returned variant-only answer copy");
    }

    if (!/\bmodels?\b/i.test(answer)) {
      failures.push("broad feature discovery answer should mention grouped models");
    }

    const sampleGroup = modelGroups[0] || rows[0] || {};
    if (
      !sampleGroup.model ||
      !sampleGroup.startsFromVariant ||
      !sampleGroup.bestUnderBudgetVariant ||
      !Number(sampleGroup.qualifyingVariantCount || 0)
    ) {
      failures.push("broad feature discovery rows/modelGroups missing model, startsFromVariant, bestUnderBudgetVariant, or qualifyingVariantCount");
    }

    const groups = modelGroups.length ? modelGroups : rows;
    const modelKeys = new Set();
    for (const group of groups) {
      if (group.modelKey) {
        if (modelKeys.has(group.modelKey)) {
          failures.push(`duplicate modelKey in broad feature discovery: ${group.modelKey}`);
        }
        modelKeys.add(group.modelKey);
      }

      const variants = Array.isArray(group.qualifyingVariants) ? group.qualifyingVariants : [];
      if (Number(group.qualifyingVariantCount || 0) !== variants.length) {
        failures.push(`qualifyingVariantCount mismatch for ${group.model || group.modelKey}`);
      }

      const invalidVariant = variants.find(
        (variant) =>
          Number(variant.foundMatrixRows || 0) <= 0 ||
          variant.featureAvailability?.available !== true,
      );
      if (invalidVariant) {
        failures.push(`broad feature discovery contains unverified variant: ${group.model || group.modelKey} ${invalidVariant.variant || invalidVariant.variantName || ""}`.trim());
      }
    }
  }

  if (item.id === "feature-comparison") {
    const featureKeys = rows.map((row) => String(row.featureKey || row.key || row.feature || row.displayName || "").toLowerCase());
    const joined = featureKeys.join(" ");

    if (rows.length !== 3) {
      failures.push(`feature comparison expected exactly 3 feature rows, got ${rows.length}`);
    }

    if (!/sunroof/.test(joined)) failures.push("feature comparison missing sunroof row");
    if (!/abs|anti[_ -]?lock/.test(joined)) failures.push("feature comparison missing ABS row");
    if (!/adas/.test(joined)) failures.push("feature comparison missing ADAS row");
    if (/cng.*mileage|mileage.*cng/.test(joined)) {
      failures.push("feature comparison treated CNG mileage as a compared feature");
    }

    const fuelFilter = String(response.fuelFilter || response.data?.fuelFilter || response.meta?.fuelFilter || "").toLowerCase();
    if (!fuelFilter.includes("cng")) {
      failures.push("feature comparison missing CNG fuel filter metadata");
    }
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
      rowCount: rows.length,
      modelGroupCount: modelGroups.length,
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
