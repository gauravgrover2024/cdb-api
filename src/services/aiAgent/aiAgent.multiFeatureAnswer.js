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


const normalizeVariantText = (value = "") =>
  normalizeAciContextText(value)
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const escapeRegExp = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const buildVariantAliases = (row = {}) => {
  const values = [
    row.variant,
    row.variantFull,
    row.variantName,
    row.displayVariant,
    row.variantKey ? String(row.variantKey).replace(/_/g, " ") : "",
  ];

  return [...new Set(values.map(normalizeVariantText).filter(Boolean))].sort(
    (a, b) => b.length - a.length,
  );
};

const detectRequestedVariantFromRows = ({ message = "", rows = [], model = "", make = "" } = {}) => {
  const rawMessage = normalizeVariantText(message);
  if (!rawMessage || !Array.isArray(rows) || !rows.length) return null;

  let messageWithoutModel = ` ${rawMessage} `;

  for (const value of [make, model, make && model ? `${make} ${model}` : ""]) {
    const normalized = normalizeVariantText(value);
    if (!normalized) continue;

    messageWithoutModel = messageWithoutModel.replace(
      new RegExp(`\\s${escapeRegExp(normalized).replace(/\s+/g, "\\s+")}\\s`, "gi"),
      " ",
    );
  }

  messageWithoutModel = messageWithoutModel
    .replace(
      /\b(does|do|have|has|with|and|or|sunroof|adas|feature|features|available|come|comes|get|gets|check|tell|me|whether|if|the|a|an)\b/g,
      " ",
    )
    .replace(/\s+/g, " ")
    .trim();

  const candidates = [];

  for (const row of rows) {
    for (const alias of buildVariantAliases(row)) {
      if (!alias || alias.length < 2) continue;

      const exactRegex = new RegExp(`(^|\\s)${escapeRegExp(alias).replace(/\s+/g, "\\s+")}(?=\\s|$)`, "i");

      const fullMessageHit = exactRegex.test(rawMessage);
      const strippedHit = exactRegex.test(messageWithoutModel);

      if (!fullMessageHit && !strippedHit) continue;

      candidates.push({
        row,
        alias,
        score:
          (strippedHit ? 40 : 0) +
          (fullMessageHit ? 20 : 0) +
          Math.min(alias.length, 50),
      });
    }
  }

  candidates.sort((a, b) => b.score - a.score || b.alias.length - a.alias.length);

  return candidates[0]?.row || null;
};


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
    const valueRows = summary.previewVariants
      .map((item) => ({
        variant: item.variant,
        value: cleanText(item.value || ""),
      }))
      .filter((item) => item.value && !/^(yes|available)$/i.test(item.value));

    if (valueRows.length) {
      const parts = valueRows.map((item) =>
        item.variant ? `${item.variant}: ${item.value}` : item.value,
      );

      return `${summary.displayName}: ${parts.join(", ")}${summary.availableCount > valueRows.length ? ` +${summary.availableCount - valueRows.length} more` : ""}.`;
    }

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
  allowSingleFeature = false,
} = {}) => {
  if (!modelEntity?.model) return null;

  const parsed = await parseAciFeatureRequestFromMessage({
    message,
    modelEntity,
  });

  if (!parsed.requestedFeatures?.length) return null;
  if (!allowSingleFeature && !parsed.hasMultiFeatureRequest) return null;

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

  const requestedVariantRow = detectRequestedVariantFromRows({
    message,
    rows,
    model,
    make,
  });

  const scopedRows = requestedVariantRow ? [requestedVariantRow] : rows;
  const requestedVariant = cleanText(requestedVariantRow?.variant || "");

  const summarizeStartedAt = nowMs();

  const summaries = parsed.requestedFeatures.map((requestedFeature) =>
    summarizeFeature({
      requestedFeature,
      rows: scopedRows,
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
    variant: requestedVariant,
    variantName: requestedVariant,
    selectedVariant: requestedVariant,
    city: context?.anchorCity || context?.selectedVehicle?.city || DEFAULT_CITY,
    citySlug: context?.anchorCity || context?.selectedVehicle?.citySlug || DEFAULT_CITY,
  };

  const isSingleFeatureFallback = allowSingleFeature && summaries.length === 1;

  return {
    intent: isSingleFeatureFallback ? "vehicle_feature_answer" : "vehicle_multi_feature_answer",
    tool: isSingleFeatureFallback ? "vehicle_feature_answer" : "vehicle_multi_feature_answer",
    displayMode: "inline",
    inlineType: isSingleFeatureFallback ? "feature_answer_card" : "multi_feature_answer_card",
    canvasType: "",
    title: isSingleFeatureFallback
      ? summaries[0]?.displayName || `${fullModel || model} feature check`
      : `${fullModel || model}${requestedVariant ? ` ${requestedVariant}` : ""} feature check`,
    answer: isSingleFeatureFallback
      ? `${fullModel || model}${requestedVariant ? ` ${requestedVariant}` : ""} ${answerLines.join("\n")}`
      : `I checked ${fullModel || model}${requestedVariant ? ` ${requestedVariant}` : ""} for ${summaries.length} features.\n${answerLines.join("\n")}`,
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
      anchorVariant: requestedVariant,
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
      recordCount: scopedRows.length,
      matched: summaries.length,
      variantScoped: Boolean(requestedVariant),
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
      multiFeatureQuery: !isSingleFeatureFallback,
      catalogSingleFeatureFallback: isSingleFeatureFallback,
      featureRequestCatalogCounts: parsed.catalogCounts,
      performance: {
        matrixCacheHit: Boolean(cacheFresh),
        matrixQueryMs,
        summarizeMs,
        variantScoped: Boolean(requestedVariant),
      },
    },
  };
};
