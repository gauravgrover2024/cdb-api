import mongoose from "mongoose";

import { parseAciFeatureRequestFromMessage } from "./aiAgent.featureRequestParser.js";
import { normalizeAciContextText } from "./aiAgent.contextPriority.js";

const DEFAULT_CITY = "new-delhi";
const DEFAULT_MULTI_FEATURE_MATRIX_CACHE_TTL_MS = Number(
  process.env.ACI_MULTI_FEATURE_MATRIX_CACHE_TTL_MS || 15 * 60 * 1000,
);

let multiFeatureMatrixCache = new Map();

const nowMs = () => Number(process.hrtime.bigint() / 1000000n);

export const clearAciMultiFeatureMatrixCache = () => {
  multiFeatureMatrixCache = new Map();
};

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const slugKey = (value = "") =>
  normalizeAciContextText(value).replace(/\s+/g, "_");

const getDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};

const isAvailableFeatureValue = (feature = {}) => {
  if (!feature || typeof feature !== "object") return false;

  if (feature.available === true) return true;

  const status = normalizeAciContextText(feature.availabilityStatus || "");
  if (["available", "yes", "standard"].includes(status)) return true;

  const value = normalizeAciContextText(feature.value || "");
  if (!value) return false;

  return !["not available", "no", "na", "n a", "n/a"].includes(value);
};

const summarizeFeature = ({ requestedFeature = {}, rows = [] } = {}) => {
  const key = requestedFeature.canonicalKey;
  const featureRows = rows
    .map((row) => ({
      variant: cleanText(row.variant || row.variantFull || ""),
      variantKey: row.variantKey || "",
      activePricelistMatched: row.activePricelistMatched === true,
      feature: row.featuresByKey?.[key] || null,
    }))
    .filter((row) => row.feature);

  const availableRows = featureRows.filter((row) => isAvailableFeatureValue(row.feature));
  const currentAvailableRows = availableRows.filter((row) => row.activePricelistMatched !== false);

  const displayName =
    cleanText(featureRows[0]?.feature?.displayName) ||
    cleanText(requestedFeature.displayName) ||
    cleanText(key.replace(/_/g, " "));

  const totalVariants = rows.length;
  const checkedVariants = featureRows.length;
  const availableCount = availableRows.length;

  const status =
    checkedVariants === 0
      ? "unknown"
      : availableCount > 0
        ? "available"
        : "not_available";

  const previewRows = (currentAvailableRows.length ? currentAvailableRows : availableRows)
    .slice(0, 5)
    .map((row) => ({
      variant: row.variant,
      value: cleanText(row.feature?.value || ""),
      availabilityStatus: cleanText(row.feature?.availabilityStatus || ""),
    }));

  return {
    featureKey: key,
    feature: displayName,
    displayName,
    groupKey: requestedFeature.groupKey || featureRows[0]?.feature?.groupKey || "",
    groupLabel: requestedFeature.groupLabel || featureRows[0]?.feature?.groupLabel || "",
    status,
    available: status === "available",
    checkedVariants,
    totalVariants,
    availableCount,
    unavailableCount: Math.max(0, checkedVariants - availableCount),
    previewVariants: previewRows,
    source: "vehicle_variant_feature_matrix_v2",
  };
};

const buildAnswerLine = (summary = {}) => {
  if (summary.status === "unknown") {
    return `${summary.displayName}: I could not confidently match this feature in the current feature matrix.`;
  }

  if (summary.available) {
    const variants = summary.previewVariants.map((item) => item.variant).filter(Boolean);
    const suffix = variants.length ? ` Available on ${variants.join(", ")}${summary.availableCount > variants.length ? ` +${summary.availableCount - variants.length} more` : ""}.` : "";
    return `${summary.displayName}: Yes.${suffix}`;
  }

  return `${summary.displayName}: Not showing on current variants.`;
};

export const maybeRunAciMultiFeatureAnswer = async ({
  message = "",
  modelEntity = null,
  context = {},
} = {}) => {
  if (!modelEntity?.model) return null;

  const parsed = await parseAciFeatureRequestFromMessage({
    message,
    modelEntity,
  });

  if (!parsed.hasMultiFeatureRequest) return null;

  const db = getDb();
  if (!db) return null;

  const make = cleanText(modelEntity.brand || modelEntity.make || "");
  const model = cleanText(modelEntity.model || "");
  const fullModel = cleanText(
    modelEntity.fullModel ||
      modelEntity.displayName ||
      (make && model ? `${make} ${model}` : model),
  );

  const modelKey = slugKey(model);
  const brandModelKey = make ? `${slugKey(make)}_${slugKey(model)}` : "";

  const projection = {
    brand: 1,
    make: 1,
    model: 1,
    modelKey: 1,
    brandModelKey: 1,
    variant: 1,
    variantKey: 1,
    variantFull: 1,
    activePricelistMatched: 1,
    activeForFeatureExplorer: 1,
  };

  for (const featureKey of parsed.featureKeys) {
    projection[`featuresByKey.${featureKey}`] = 1;
  }

  const cacheKey = brandModelKey || modelKey || slugKey(model);
  const cacheHit = multiFeatureMatrixCache.get(cacheKey);
  const cacheFresh =
    cacheHit && Date.now() - cacheHit.builtAt < DEFAULT_MULTI_FEATURE_MATRIX_CACHE_TTL_MS;

  const queryStartedAt = nowMs();

  const rows = cacheFresh
    ? cacheHit.rows
    : await db
        .collection("vehicle_variant_feature_matrix_v2")
        .find(
          {
            $and: [
              {
                $or: [
                  modelKey ? { modelKey } : null,
                  brandModelKey ? { brandModelKey } : null,
                  model ? { model: new RegExp(`^${model.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i") } : null,
                ].filter(Boolean),
              },
              {
                $or: [
                  { activeForFeatureExplorer: { $exists: false } },
                  { activeForFeatureExplorer: { $ne: false } },
                ],
              },
            ],
          },
          {
            projection,
            limit: Number(process.env.ACI_MULTI_FEATURE_VARIANT_LIMIT || 300),
          },
        )
        .toArray();

  if (!cacheFresh && cacheKey) {
    multiFeatureMatrixCache.set(cacheKey, {
      builtAt: Date.now(),
      rows,
    });
  }

  const matrixQueryMs = nowMs() - queryStartedAt;

  const summarizeStartedAt = nowMs();

  const summaries = parsed.requestedFeatures.map((requestedFeature) =>
    summarizeFeature({
      requestedFeature,
      rows,
    }),
  );

  const summarizeMs = nowMs() - summarizeStartedAt;

  const answerLines = summaries.map(buildAnswerLine);

  const selectedVehicle = {
    make,
    brand: make,
    model,
    fullModel,
    displayName: fullModel,
    variant: "",
    variantName: "",
    selectedVariant: "",
    city: context?.anchorCity || context?.selectedVehicle?.city || DEFAULT_CITY,
    citySlug: context?.anchorCity || context?.selectedVehicle?.citySlug || DEFAULT_CITY,
  };

  return {
    intent: "vehicle_multi_feature_answer",
    tool: "vehicle_multi_feature_answer",
    displayMode: "inline",
    inlineType: "multi_feature_answer_card",
    canvasType: "",
    title: `${fullModel || model} feature check`,
    answer: `I checked ${fullModel || model} for ${summaries.length} features.\n${answerLines.join("\n")}`,
    model,
    make,
    brand: make,
    fullModel,
    features: summaries,
    rows: summaries,
    items: summaries,
    unmatchedFeaturePhrases: parsed.unmatchedFeaturePhrases || [],
    contextPatch: {
      selectedVehicle,
      anchorMake: make,
      anchorModel: model,
      anchorFullModel: fullModel,
      anchorVariant: "",
      anchorCity: selectedVehicle.citySlug,
      selectedColor: null,
    },
    sourceTransparency: {
      responseTool: "vehicle_multi_feature_answer",
      modulesChecked: [
        "vehicle_feature_catalog_v2",
        "vehicle_variant_feature_matrix_v2",
      ],
      dataSource: "vehicle_feature_catalog_v2+vehicle_variant_feature_matrix_v2",
      recordCount: rows.length,
      matched: summaries.length,
      featureKeys: parsed.featureKeys,
    },
    runtimeResultsMeta: [
      {
        tool: "vehicle_multi_feature_answer",
        matched: summaries.length,
        source: "vehicle_feature_catalog_v2+vehicle_variant_feature_matrix_v2",
        modulesChecked: [
          "vehicle_feature_catalog_v2",
          "vehicle_variant_feature_matrix_v2",
        ],
        error: "",
      },
    ],
    meta: {
      multiFeatureQuery: true,
      featureRequestCatalogCounts: parsed.catalogCounts,
      performance: {
        matrixCacheHit: Boolean(cacheFresh),
        matrixQueryMs,
        summarizeMs,
      },
    },
  };
};
