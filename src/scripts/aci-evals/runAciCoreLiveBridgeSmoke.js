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
  {
    id: "budget-cars-under-20l",
    message: "cars under 20 lakhs",
    expectedIntentIncludes: ["recommendation"],
    expectedCanvasType: "recommendation_results_canvas",
    minMatched: 1,
    budgetMax: 2000000,
  },
  {
    id: "budget-best-cars-under-20l",
    message: "best cars under 20 lakhs",
    expectedIntentIncludes: ["recommendation"],
    expectedCanvasType: "recommendation_results_canvas",
    minMatched: 1,
    budgetMax: 2000000,
  },
  {
    id: "budget-suvs-under-20l",
    message: "SUVs under 20 lakhs",
    expectedIntentIncludes: ["recommendation"],
    expectedCanvasType: "recommendation_results_canvas",
    minMatched: 1,
    budgetMax: 2000000,
    bodyType: "suv",
  },
  {
    id: "budget-automatic-cars-under-20l",
    message: "automatic cars under 20 lakhs",
    expectedIntentIncludes: ["recommendation"],
    expectedCanvasType: "recommendation_results_canvas",
    minMatched: 1,
    budgetMax: 2000000,
    transmission: "automatic",
  },
  {
    id: "budget-best-automatic-suvs-under-20l",
    message: "best automatic SUVs under 20 lakhs",
    expectedIntentIncludes: ["recommendation"],
    expectedCanvasType: "recommendation_results_canvas",
    minMatched: 1,
    budgetMax: 2000000,
    bodyType: "suv",
    transmission: "automatic",
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

const getRows = (response = {}) =>
  response.data?.rows || response.rows || response.items || response.data?.items || [];

const getModelGroups = (response = {}) =>
  response.data?.modelGroups || response.modelGroups || response.widget?.modelGroups || [];

const getPreviewModelGroups = (response = {}) =>
  response.data?.previewModelGroups ||
  response.previewModelGroups ||
  response.data?.rows ||
  response.rows ||
  response.items ||
  [];

const getBridgeIsolation = (response = {}) =>
  response.aciCoreBridge?.contextIsolation ||
  response.meta?.aciCoreBridge?.contextIsolation ||
  "";

const mergeContextPatch = (context = {}, response = {}) => ({
  ...(context || {}),
  ...(response.contextPatch || {}),
});

const getModelNames = (response = {}) => {
  const models = response.data?.models || response.models || [];
  return models
    .map((model) => {
      if (typeof model === "string") return model;
      return model.fullModel || model.displayName || [model.make || model.brand, model.model].filter(Boolean).join(" ");
    })
    .filter(Boolean);
};

const numberFromValue = (value = 0) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const textValue = String(value || "").replace(/,/g, "").trim().toLowerCase();
  const number = Number((textValue.match(/\d+(?:\.\d+)?/) || [])[0] || 0);
  if (!Number.isFinite(number) || number <= 0) return 0;
  if (/\bcr|crore/.test(textValue)) return Math.round(number * 10000000);
  if (/\bl|lakh|lakhs|lac|lacs/.test(textValue) && number <= 300) {
    return Math.round(number * 100000);
  }
  return Math.round(number);
};

const getResponseModules = (response = {}) =>
  response.modulesChecked ||
  response.sourceTransparency?.modulesChecked ||
  response.runtimeResultsMeta?.[0]?.modulesChecked ||
  [];

const activeComparisonFrom = (response = {}) =>
  response.contextPatch?.activeComparison ||
  response.data?.activeComparison ||
  response.activeComparison ||
  null;

const runSequentialContextIsolationCases = async () => {
  const results = [];

  {
    const failures = [];
    let context = {};
    const setupResponse = await runAciCoreLiveBridge({
      message: "Creta SX on-road price Delhi",
      context,
    });
    context = mergeContextPatch(context, setupResponse);

    const response = await runAciCoreLiveBridge({
      message: "Hyundai cars with sunroof under 20 lakh",
      context,
    });

    const modelGroups = getModelGroups(response);
    const rows = getRows(response);
    const groups = modelGroups.length ? modelGroups : rows;
    const modelKeys = new Set(groups.map((group) => group.modelKey || group.model || group.displayName).filter(Boolean));
    const vehicle = response.data?.vehicle || response.vehicle || response.widget?.vehicle || {};
    const selectedVehicle = response.contextPatch?.selectedVehicle || {};
    const selectedVehicleText = text([
      selectedVehicle.model,
      selectedVehicle.variant,
      selectedVehicle.fullModel,
      selectedVehicle.displayName,
      vehicle.model,
      vehicle.variant,
      vehicle.fullModel,
      vehicle.displayName,
    ].filter(Boolean).join(" "));

    if (response.canvasType !== "feature_match_builder_canvas") {
      failures.push(`expected feature_match_builder_canvas, got ${response.canvasType}`);
    }

    if (getBridgeIsolation(response) !== "broad_discovery_without_model") {
      failures.push(`expected broad_discovery_without_model isolation, got ${getBridgeIsolation(response)}`);
    }

    if (modelKeys.size <= 1) {
      failures.push(`expected broad discovery across multiple model groups, got ${modelKeys.size}`);
    }

    if (/\bcreta\b|\bsx\b/.test(selectedVehicleText)) {
      failures.push("broad discovery contextPatch/vehicle retained the previous selected model or variant");
    }

    results.push({
      id: "sequential-broad-discovery-context-isolation",
      message: "Creta SX on-road price Delhi -> Hyundai cars with sunroof under 20 lakh",
      pass: failures.length === 0,
      failures,
      summary: {
        setupIntent: setupResponse.intent,
        intent: response.intent,
        canvasType: response.canvasType,
        matched: getMatched(response),
        modelGroupCount: modelGroups.length,
        rowCount: rows.length,
        modelKeys: [...modelKeys].slice(0, 10),
        selectedVehicle: response.contextPatch?.selectedVehicle || null,
        aciCoreBridge: response.aciCoreBridge || response.meta?.aciCoreBridge || null,
      },
    });
  }

  {
    const failures = [];
    let context = {};
    const setupResponse = await runAciCoreLiveBridge({
      message: "Mahindra Thar price list",
      context,
    });
    context = mergeContextPatch(context, setupResponse);

    const response = await runAciCoreLiveBridge({
      message: "Best SUV under 20 lakhs with Sunroof",
      context,
    });
    const rows = getRows(response);
    const selectedVehicle = response.contextPatch?.selectedVehicle;
    const answer = text(response.answer);

    if (response.intent !== "vehicle_recommendation") {
      failures.push(`expected vehicle_recommendation intent, got ${response.intent}`);
    }

    if (response.canvasType !== "feature_match_builder_canvas") {
      failures.push(`expected feature_match_builder_canvas, got ${response.canvasType}`);
    }

    if (getBridgeIsolation(response) !== "broad_discovery_without_model") {
      failures.push(`expected broad_discovery_without_model isolation, got ${getBridgeIsolation(response)}`);
    }

    if (rows.length < 2) {
      failures.push(`expected a multi-model sunroof shortlist, got ${rows.length} rows`);
    }

    if (selectedVehicle !== null || response.contextPatch?.clearSelectedVehicle !== true) {
      failures.push("broad recommendation did not clear the previous Thar selection");
    }

    if (!/suvs with sunroof under/.test(answer) || /not available|not listed/.test(answer)) {
      failures.push(`unexpected shortlist answer: ${response.answer}`);
    }

    results.push({
      id: "sequential-selected-car-to-generic-feature-shortlist",
      message: "Mahindra Thar price list -> Best SUV under 20 lakhs with Sunroof",
      pass: failures.length === 0,
      failures,
      summary: {
        setupIntent: setupResponse.intent,
        intent: response.intent,
        canvasType: response.canvasType,
        rowCount: rows.length,
        selectedVehicle,
        clearSelectedVehicle: response.contextPatch?.clearSelectedVehicle,
        answer: response.answer,
        aciCoreBridge: response.aciCoreBridge || response.meta?.aciCoreBridge || null,
      },
    });
  }

  {
    const failures = [];
    let context = {};
    const setupResponse = await runAciCoreLiveBridge({
      message: "Punch and Nexon CNG sunroof ABS ADAS",
      context,
    });
    context = mergeContextPatch(context, setupResponse);

    const response = await runAciCoreLiveBridge({
      message: "Verna HX8 iVT vs City ZX CVT",
      context,
    });

    const modelNames = getModelNames(response);
    const rows = getRows(response);
    const visibleComparisonText = text([
      response.title,
      response.answer,
      ...modelNames,
      ...rows.map((row) => row.displayName || row.fullModel || row.model || row.vehicle?.model),
    ].filter(Boolean).join(" "));

    if (!text(response.intent).includes("comparison")) {
      failures.push(`expected comparison intent, got ${response.intent}`);
    }

    if (response.canvasType !== "comparison_canvas") {
      failures.push(`expected comparison_canvas, got ${response.canvasType}`);
    }

    if (getBridgeIsolation(response) !== "explicit_comparison_targets") {
      failures.push(`expected explicit_comparison_targets isolation, got ${getBridgeIsolation(response)}`);
    }

    if (modelNames.length !== 2) {
      failures.push(`expected exactly two compared models, got ${modelNames.length}`);
    }

    if (!/\bverna\b/.test(visibleComparisonText) || !/\bcity\b/.test(visibleComparisonText)) {
      failures.push("explicit comparison did not keep the current two requested models");
    }

    if (/\bpunch\b|\bnexon\b/.test(visibleComparisonText)) {
      failures.push("explicit comparison retained previous comparison models");
    }

    results.push({
      id: "sequential-comparison-context-isolation",
      message: "Punch and Nexon CNG sunroof ABS ADAS -> Verna HX8 iVT vs City ZX CVT",
      pass: failures.length === 0,
      failures,
      summary: {
        setupIntent: setupResponse.intent,
        intent: response.intent,
        canvasType: response.canvasType,
        matched: getMatched(response),
        modelNames,
        rowCount: rows.length,
        aciCoreBridge: response.aciCoreBridge || response.meta?.aciCoreBridge || null,
      },
    });
  }

  return results;
};

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
  const rows = getRows(response);
  const modelGroups = getModelGroups(response);

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

    if (/\bqualifying variants?\b/i.test(answer)) {
      failures.push("broad feature discovery answer should be model-focused and not expose variant-count wording");
    }

    const featureDiscovery =
      response.data?.featureDiscovery ||
      response.featureDiscovery ||
      response.meta?.featureDiscovery ||
      {};
    const totalFeatureModels = Number(
      featureDiscovery.totalQualifyingModels ||
        response.data?.totalQualifyingModels ||
        0,
    );
    const totalFeatureUniqueVariants = Number(
      featureDiscovery.totalUniqueQualifyingVariants ||
        featureDiscovery.totalQualifyingVariants ||
        response.data?.totalUniqueQualifyingVariants ||
        0,
    );

    if (!totalFeatureModels || !totalFeatureUniqueVariants) {
      failures.push("broad feature discovery missing total model/unique variant metadata");
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

  if (item.budgetMax) {
    const budgetDiscovery =
      response.data?.budgetDiscovery ||
      response.budgetDiscovery ||
      response.meta?.budgetDiscovery ||
      {};
    const previewGroups = getPreviewModelGroups(response);
    const totalQualifyingModels = Number(
      budgetDiscovery.totalQualifyingModels ||
        response.data?.totalQualifyingModels ||
        response.totalQualifyingModels ||
        0,
    );
    const totalQualifyingVariants = Number(
      budgetDiscovery.totalUniqueQualifyingVariants ||
        budgetDiscovery.totalQualifyingVariants ||
        response.data?.totalQualifyingVariants ||
        response.totalQualifyingVariants ||
        0,
    );
    const totalQualifyingPriceRows = Number(
      budgetDiscovery.totalQualifyingPriceRows ||
        response.data?.totalQualifyingPriceRows ||
        response.totalQualifyingPriceRows ||
        0,
    );
    const returnedPreviewGroups = Number(
      budgetDiscovery.returnedPreviewGroups ||
        response.data?.returnedPreviewGroups ||
        response.returnedPreviewGroups ||
        previewGroups.length ||
        0,
    );

    if (!intent.includes("recommendation")) {
      failures.push(`budget discovery expected recommendation intent, got ${response.intent}`);
    }

    if (canvasType !== "recommendation_results_canvas") {
      failures.push(`budget discovery expected recommendation_results_canvas, got ${canvasType}`);
    }

    if (!previewGroups.length) {
      failures.push("budget discovery returned no preview groups");
    }

    if (activeComparisonFrom(response)) {
      failures.push("budget discovery should not write activeComparison");
    }

    const composer = response.answerComposer || response.meta?.answerComposer || null;
    if (!composer?.applied || composer.intent !== "vehicle_recommendation") {
      failures.push("budget discovery did not apply vehicle_recommendation answer composer");
    }

    if (!/\bmodels?\b/i.test(answer) || !/\bunder\b/i.test(answer) || !/showing the top/i.test(answer)) {
      if (!/showing \d+ good starting points/i.test(answer)) {
        failures.push("budget discovery answer should be model-first, budget-aware, and preview-aware");
      }
    }

    if (/I found 24 models/i.test(answer)) {
      failures.push("budget discovery answer is using preview count as total model count");
    }

    if (/\bqualifying variants?\b/i.test(answer)) {
      failures.push("budget discovery buyer-facing answer should not mention qualifying variant counts");
    }

    const modules = getResponseModules(response);
    if (!modules.includes("aci_vehicle_price_rows")) {
      failures.push(`budget discovery should use aci_vehicle_price_rows, got ${modules.join(", ")}`);
    }

    if (item.id === "budget-cars-under-20l") {
      if (!(totalQualifyingModels > returnedPreviewGroups)) {
        failures.push(`expected totalQualifyingModels > returnedPreviewGroups, got ${totalQualifyingModels} <= ${returnedPreviewGroups}`);
      }
      if (!(totalQualifyingVariants > totalQualifyingModels)) {
        failures.push(`expected totalQualifyingVariants > totalQualifyingModels, got ${totalQualifyingVariants} <= ${totalQualifyingModels}`);
      }
      if (!(totalQualifyingVariants > 0)) {
        failures.push(`expected totalUniqueQualifyingVariants > 0, got ${totalQualifyingVariants}`);
      }
      if (!(totalQualifyingPriceRows >= totalQualifyingVariants)) {
        failures.push(`expected price-row count >= unique variant count for city-scoped cache, got ${totalQualifyingPriceRows} < ${totalQualifyingVariants}`);
      }
      if (budgetDiscovery.cityScoped === false) {
        failures.push("budget discovery should indicate city-scoped/default-city behavior when available");
      }
    }

    if (returnedPreviewGroups > 8 || previewGroups.length > 8) {
      failures.push(`budget discovery preview should be <=8 groups, got returned=${returnedPreviewGroups}, rows=${previewGroups.length}`);
    }

    if (response.modelGroupCount > 8 || response.data?.modelGroupCount > 8) {
      failures.push(`budget discovery modelGroupCount should represent preview count <=8, got top=${response.modelGroupCount}, data=${response.data?.modelGroupCount}`);
    }

    const previewModelKeys = new Set();
    const previewBodyTypes = new Set();

    for (const group of previewGroups) {
      const modelKey = text(`${group.make || group.brand || ""} ${group.modelKey || group.model || group.displayName || ""}`);
      if (modelKey) {
        if (previewModelKeys.has(modelKey)) {
          failures.push(`budget discovery preview duplicated model: ${group.displayName || group.modelKey}`);
        }
        previewModelKeys.add(modelKey);
      }

      const groupBodyText = text(`${group.bodyType || ""} ${group.bodyTypeKey || ""} ${group.segment || ""}`);
      if (groupBodyText) previewBodyTypes.add(groupBodyText);

      const variants = Array.isArray(group.qualifyingVariants) ? group.qualifyingVariants : [];
      if (!group.startsFromVariant || !group.bestUnderBudgetVariant) {
        failures.push(`budget model group missing start/best variants for ${group.displayName || group.modelKey}`);
      }

      for (const variant of variants) {
        const price = numberFromValue(variant.exShowroomPrice || variant.exShowroomPriceLabel);
        if (!price || price > item.budgetMax) {
          failures.push(`budget discovery returned over-budget variant: ${group.displayName || group.modelKey} ${variant.variant || ""} ${variant.exShowroomPriceLabel || variant.exShowroomPrice}`.trim());
        }
      }

      if (item.bodyType) {
        const bodyText = text(`${group.bodyType || ""} ${group.bodyTypeKey || ""}`);
        if (item.bodyType === "suv" && !/\bsuv\b|sport/.test(bodyText)) {
          failures.push(`SUV budget discovery returned non-SUV group: ${group.displayName || group.modelKey} (${bodyText})`);
        }
      }

      if (item.transmission) {
        const transmissionText = text([
          ...(Array.isArray(group.transmissions) ? group.transmissions : []),
          ...variants.map((variant) => variant.transmission),
        ].filter(Boolean).join(" "));
        if (!/\bautomatic\b|\bauto\b|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bat\b|\bdsg\b/.test(transmissionText)) {
          failures.push(`automatic budget discovery returned group without automatic variants: ${group.displayName || group.modelKey}`);
        }
      }
    }

    if (!item.bodyType && previewBodyTypes.size > 1 && !budgetDiscovery.diversifiedPreview) {
      failures.push("generic budget discovery should mark diversified preview when multiple body types are present");
    }

    if (!item.bodyType && previewBodyTypes.size <= 1 && previewGroups.length > 3) {
      failures.push("generic budget discovery preview should include more than one body type when bodyType data is available");
    }

    const bridgePrimaryTask = response.aciCoreBridge?.primaryTask || response.meta?.aciCoreBridge?.primaryTask || "";
    if (
      (item.id === "budget-suvs-under-20l" || item.id === "budget-best-automatic-suvs-under-20l") &&
      bridgePrimaryTask !== "vehicle_discovery"
    ) {
      failures.push(`budget discovery bridge primaryTask should be vehicle_discovery, got ${bridgePrimaryTask}`);
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
        getResponseModules(response),
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
  results.push(...await runSequentialContextIsolationCases());

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
