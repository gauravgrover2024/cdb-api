import mongoose from "mongoose";

import { parseAciFeatureRequestFromMessage } from "./aiAgent.featureRequestParser.js";
import { resolveAciExplicitMessageModelEntity } from "./aiAgent.modelContextResolver.js";
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


const hyphenKey = (value = "") =>
  normalizeAciContextText(value).replace(/\s+/g, "-");

const sameModelEntity = (a = {}, b = {}) => {
  const aModel = normalizeAciContextText(a?.model || "");
  const bModel = normalizeAciContextText(b?.model || "");
  const aBrand = normalizeAciContextText(a?.brand || a?.make || "");
  const bBrand = normalizeAciContextText(b?.brand || b?.make || "");

  return Boolean(aModel && bModel && aModel === bModel && (!aBrand || !bBrand || aBrand === bBrand));
};

const resolveExactShortModelEntityFromReadModels = async (query = "") => {
  const cleanQuery = normalizeAciContextText(query);
  const key = hyphenKey(query);

  // Only rescue short exact DB model keys after feature stripping.
  // Example: "ix range" -> strip "range" -> exact model "ix".
  if (!cleanQuery || cleanQuery.length < 2 || cleanQuery.length > 12) return null;

  const db = mongoose.connection?.readyState === 1 ? mongoose.connection?.db : null;
  if (!db) return null;

  const rows = await db
    .collection("aci_vehicle_model_summary")
    .find(
      {
        $or: [
          { modelKey: key },
          { model: new RegExp(`^${String(query).replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}$`, "i") },
        ],
      },
      {
        projection: {
          brand: 1,
          make: 1,
          model: 1,
          modelKey: 1,
          fullModel: 1,
          displayName: 1,
          citySlug: 1,
        },
        limit: 20,
      },
    )
    .toArray();

  const byModel = new Map();

  for (const row of rows) {
    const model = cleanText(row.model || "");
    const brand = cleanText(row.brand || row.make || "");
    if (!model) continue;

    const dedupeKey = `${normalizeAciContextText(brand)}::${normalizeAciContextText(model)}`;

    const entity = {
      brand,
      make: brand,
      model,
      fullModel: cleanText(row.fullModel || row.displayName || `${brand} ${model}`),
      displayName: cleanText(row.displayName || row.fullModel || `${brand} ${model}`),
      modelKey: row.modelKey || key,
      shortModelKey: row.modelKey || key,
      matchedText: query,
      matchText: query,
      method: "db_exact_short_model_feature_stripped_match",
      confidence: 0.96,
      fromMessage: true,
      fromReadModelSummary: true,
    };

    const existing = byModel.get(dedupeKey);
    if (!existing || row.citySlug === "new-delhi") {
      byModel.set(dedupeKey, entity);
    }
  }

  const candidates = [...byModel.values()];

  // If more than one model shares this short key, do not guess.
  if (candidates.length !== 1) return null;

  return candidates[0];
};

const strippedQueryActuallyNamesEntity = (query = "", entity = {}) => {
  const queryKey = normalizeAciContextText(query);
  if (!queryKey || !entity?.model) return false;

  const modelKey = normalizeAciContextText(entity.model || "");
  const brandKey = normalizeAciContextText(entity.brand || entity.make || "");
  const fullModelKey = normalizeAciContextText(entity.fullModel || entity.displayName || "");

  if (queryKey === modelKey) return true;
  if (fullModelKey && queryKey === fullModelKey) return true;
  if (brandKey && modelKey && queryKey === `${brandKey} ${modelKey}`) return true;

  // Exact short-model rescue is intentionally DB-backed and safe.
  if (entity.method === "db_exact_short_model_feature_stripped_match") return true;

  return false;
};

const buildConsistentFullModel = (entity = {}, make = "", model = "") => {
  const rawFullModel = cleanText(entity.fullModel || entity.displayName || "");
  const cleanMake = cleanText(make || entity.brand || entity.make || "");
  const cleanModel = cleanText(model || entity.model || "");

  if (!cleanModel) return rawFullModel || cleanMake;

  const fullModelKey = normalizeAciContextText(rawFullModel);
  const modelKey = normalizeAciContextText(cleanModel);
  const makeKey = normalizeAciContextText(cleanMake);

  const fullModelMentionsModel = Boolean(modelKey && fullModelKey.includes(modelKey));
  const fullModelMentionsMake = !makeKey || fullModelKey.includes(makeKey);

  if (rawFullModel && fullModelMentionsModel && fullModelMentionsMake) {
    return rawFullModel;
  }

  return cleanText(`${cleanMake} ${cleanModel}`) || rawFullModel;
};

const shouldPreferFeatureStrippedModelEntity = ({
  currentEntity = {},
  strippedEntity = {},
  parsed = {},
  strippedQuery = "",
} = {}) => {
  if (!strippedEntity?.model) return false;
  if (!strippedQueryActuallyNamesEntity(strippedQuery, strippedEntity)) return false;
  if (!currentEntity?.model) return true;
  if (sameModelEntity(currentEntity, strippedEntity)) return false;

  const strippedConfidence = Number(strippedEntity.confidence || 0);
  const currentConfidence = Number(currentEntity.confidence || 0);

  // Feature words like "range" can be mistaken as model text.
  // Prefer feature-stripped entity only when the stripped query really names that DB model.
  if (strippedConfidence >= 0.75 && parsed?.requestedFeatures?.length) return true;

  return strippedConfidence > currentConfidence;
};




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

  let parsed = await parseAciFeatureRequestFromMessage({
    message,
    modelEntity,
  });

  if (!parsed.requestedFeatures?.length) return null;
  if (!allowSingleFeature && !parsed.hasMultiFeatureRequest) return null;

  let effectiveModelEntity = modelEntity;

  if (allowSingleFeature && parsed.featureStrippedMessage) {
    let strippedEntity = await resolveAciExplicitMessageModelEntity(
      parsed.featureStrippedMessage,
    );

    if (!strippedEntity?.model) {
      strippedEntity = await resolveExactShortModelEntityFromReadModels(
        parsed.featureStrippedMessage,
      );
    }

    if (
      shouldPreferFeatureStrippedModelEntity({
        currentEntity: modelEntity,
        strippedEntity,
        parsed,
        strippedQuery: parsed.featureStrippedMessage,
      })
    ) {
      effectiveModelEntity = strippedEntity;
      parsed = await parseAciFeatureRequestFromMessage({
        message,
        modelEntity: effectiveModelEntity,
      });
    }
  }

  const db = getDb();
  if (!db) return null;

  const make = cleanText(effectiveModelEntity.brand || effectiveModelEntity.make || "");
  const model = cleanText(effectiveModelEntity.model || "");
  const fullModel = buildConsistentFullModel(effectiveModelEntity, make, model);

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
