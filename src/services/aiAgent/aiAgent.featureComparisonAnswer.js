import mongoose from "mongoose";

import { parseAciFeatureRequestFromMessage } from "./aiAgent.featureRequestParser.js";
import {
  findModelMatches,
  getVehicleEntityIndex,
} from "./aiAgent.vehicleEntityIndex.js";
import { normalizeAciContextText } from "./aiAgent.contextPriority.js";

const DEFAULT_CITY = "new-delhi";

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const slugKey = (value = "") =>
  normalizeAciContextText(value).replace(/\s+/g, "_");

const isComparisonFeatureQuestion = (message = "") =>
  /\b(compare|comparison|vs|versus|v\/s|difference|different)\b/i.test(message) &&
  /\b(feature|features|sunroof|adas|airbags?|cruise|camera|tpms|charging|safety|comfort|convenience)\b/i.test(message);

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

const buildComparisonModelEntity = (entry = {}) => {
  const model = cleanText(entry.model || entry.rawModel || "");
  const brand = cleanText(entry.brand || entry.make || "");
  const fullModel = cleanText(
    entry.displayName ||
      entry.fullModel ||
      (brand && model ? `${brand} ${model}` : model),
  );

  if (!model) return null;

  return {
    make: brand,
    brand,
    model,
    fullModel,
    modelKey: slugKey(entry.shortModelKey || model),
    brandModelKey: brand ? `${slugKey(brand)}_${slugKey(model)}` : slugKey(fullModel),
  };
};

const uniqueModelsFromMatches = (matches = [], limit = 2) => {
  const seen = new Set();
  const models = [];

  for (const match of matches) {
    const entity = buildComparisonModelEntity(match);
    if (!entity) continue;

    const key = `${entity.brand.toLowerCase()}::${entity.model.toLowerCase()}`;
    if (seen.has(key)) continue;

    seen.add(key);
    models.push(entity);

    if (models.length >= limit) break;
  }

  return models;
};

const findMentionedComparisonModelsFromIndex = (index = {}, message = "", limit = 2) => {
  const messageKey = normalizeAciContextText(message);
  const candidates = [];

  for (const modelEntry of index.models || []) {
    const entity = buildComparisonModelEntity(modelEntry);
    if (!entity) continue;

    const aliases = [
      entity.fullModel,
      entity.brand && entity.model ? `${entity.brand} ${entity.model}` : "",
      entity.model,
      ...(Array.isArray(modelEntry.aliases) ? modelEntry.aliases : []),
    ]
      .map(cleanText)
      .filter(Boolean)
      .sort((a, b) => b.length - a.length);

    for (const alias of aliases) {
      const aliasKey = normalizeAciContextText(alias);
      if (!aliasKey || aliasKey.length < 3) continue;

      const regex = new RegExp(
        `(^|\\s)${aliasKey.replace(/[.*+?^${}()|[\\]\\]/g, "\\$&").replace(/\\s+/g, "\\s+")}(?=\\s|$)`,
        "i",
      );

      const match = messageKey.match(regex);
      if (!match) continue;

      candidates.push({
        entity,
        alias,
        aliasKey,
        index: match.index,
        score:
          aliasKey.length +
          (entity.brand && aliasKey.includes(normalizeAciContextText(entity.brand)) ? 60 : 0) +
          (aliasKey === normalizeAciContextText(entity.fullModel) ? 40 : 0),
      });
    }
  }

  const seen = new Set();

  return candidates
    .sort((a, b) => a.index - b.index || b.score - a.score)
    .filter((candidate) => {
      const key = `${candidate.entity.brand.toLowerCase()}::${candidate.entity.model.toLowerCase()}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .slice(0, limit)
    .map((candidate) => candidate.entity);
};

const summarizeModelFeature = ({ modelEntity = {}, feature = {}, rows = [] } = {}) => {
  const key = feature.canonicalKey;
  const featureRows = rows
    .map((row) => ({
      variant: cleanText(row.variant || row.variantFull || ""),
      activePricelistMatched: row.activePricelistMatched === true,
      feature: row.featuresByKey?.[key] || null,
    }))
    .filter((row) => row.feature);

  const availableRows = featureRows.filter((row) => isAvailableFeatureValue(row.feature));
  const currentAvailableRows = availableRows.filter((row) => row.activePricelistMatched !== false);

  const displayName =
    cleanText(featureRows[0]?.feature?.displayName) ||
    cleanText(feature.displayName) ||
    cleanText(key.replace(/_/g, " "));

  const previewVariants = (currentAvailableRows.length ? currentAvailableRows : availableRows)
    .slice(0, 4)
    .map((row) => row.variant)
    .filter(Boolean);

  return {
    make: modelEntity.make || modelEntity.brand || "",
    model: modelEntity.model || "",
    fullModel: modelEntity.fullModel || modelEntity.model || "",
    featureKey: key,
    feature: displayName,
    displayName,
    available: availableRows.length > 0,
    status: featureRows.length === 0 ? "unknown" : availableRows.length > 0 ? "available" : "not_available",
    checkedVariants: featureRows.length,
    totalVariants: rows.length,
    availableCount: availableRows.length,
    previewVariants,
  };
};

const buildFeatureComparisonRows = ({ models = [], features = [], rowsByModel = new Map() } = {}) =>
  features.map((feature) => {
    const modelSummaries = models.map((modelEntity) =>
      summarizeModelFeature({
        modelEntity,
        feature,
        rows: rowsByModel.get(modelEntity.brandModelKey || modelEntity.modelKey) || [],
      }),
    );

    return {
      featureKey: feature.canonicalKey,
      feature: feature.displayName,
      displayName: feature.displayName,
      models: modelSummaries,
    };
  });

const buildAnswer = ({ models = [], rows = [] } = {}) => {
  const names = models.map((model) => model.fullModel || model.model).join(" vs ");
  const lines = rows.map((row) => {
    const parts = row.models.map((model) => {
      if (model.status === "unknown") return `${model.model}: unknown`;
      if (!model.available) return `${model.model}: No`;
      const variants = model.previewVariants.length
        ? ` (${model.previewVariants.join(", ")}${model.availableCount > model.previewVariants.length ? ` +${model.availableCount - model.previewVariants.length} more` : ""})`
        : "";
      return `${model.model}: Yes${variants}`;
    });

    return `${row.displayName}: ${parts.join(" | ")}`;
  });

  return `I compared ${names} on ${rows.length} features.\n${lines.join("\n")}`;
};

export const maybeRunAciFeatureComparisonAnswer = async ({
  message = "",
  context = {},
} = {}) => {
  if (!isComparisonFeatureQuestion(message)) return null;

  const db = getDb();
  if (!db) return null;

  const index = await getVehicleEntityIndex();

  const directlyMentionedModels = findMentionedComparisonModelsFromIndex(index, message, 2);
  const modelMatches = directlyMentionedModels.length >= 2 ? [] : findModelMatches(index, message);
  const models =
    directlyMentionedModels.length >= 2
      ? directlyMentionedModels
      : uniqueModelsFromMatches(modelMatches, 2);

  if (models.length < 2) return null;

  const parsed = await parseAciFeatureRequestFromMessage({
    message,
    modelEntity: models[0],
  });

  if (!parsed.requestedFeatures?.length) return null;

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

  const modelOr = models.flatMap((modelEntity) =>
    [
      modelEntity.modelKey ? { modelKey: modelEntity.modelKey } : null,
      modelEntity.brandModelKey ? { brandModelKey: modelEntity.brandModelKey } : null,
    ].filter(Boolean),
  );

  const allRows = await db
    .collection("vehicle_variant_feature_matrix_v2")
    .find(
      {
        $and: [
          { $or: modelOr },
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
        limit: Number(process.env.ACI_FEATURE_COMPARISON_VARIANT_LIMIT || 800),
      },
    )
    .toArray();

  const rowsByModel = new Map();

  for (const modelEntity of models) {
    const key = modelEntity.brandModelKey || modelEntity.modelKey;
    const matchingRows = allRows.filter((row) => {
      const rowBrandModelKey = cleanText(row.brandModelKey || "");
      const rowModelKey = cleanText(row.modelKey || "");
      return (
        (modelEntity.brandModelKey && rowBrandModelKey === modelEntity.brandModelKey) ||
        (modelEntity.modelKey && rowModelKey === modelEntity.modelKey)
      );
    });

    rowsByModel.set(key, matchingRows);
  }

  const rows = buildFeatureComparisonRows({
    models,
    features: parsed.requestedFeatures,
    rowsByModel,
  });

  const firstModel = models[0];
  const selectedVehicle = {
    make: firstModel.make || firstModel.brand || "",
    brand: firstModel.brand || firstModel.make || "",
    model: firstModel.model || "",
    fullModel: firstModel.fullModel || firstModel.model || "",
    displayName: firstModel.fullModel || firstModel.model || "",
    variant: "",
    variantName: "",
    selectedVariant: "",
    city: context?.anchorCity || context?.selectedVehicle?.city || DEFAULT_CITY,
    citySlug: context?.anchorCity || context?.selectedVehicle?.citySlug || DEFAULT_CITY,
  };

  return {
    intent: "vehicle_feature_comparison",
    tool: "vehicle_feature_comparison",
    displayMode: "canvas",
    inlineType: "feature_comparison_summary",
    canvasType: "feature_comparison_canvas",
    title: `${models.map((model) => model.model).join(" vs ")} feature comparison`,
    answer: buildAnswer({ models, rows }),
    models,
    features: rows,
    rows,
    items: rows,
    contextPatch: {
      selectedVehicle,
      anchorMake: selectedVehicle.make,
      anchorModel: selectedVehicle.model,
      anchorFullModel: selectedVehicle.fullModel,
      anchorVariant: "",
      anchorCity: selectedVehicle.citySlug,
      selectedColor: null,
    },
    sourceTransparency: {
      responseTool: "vehicle_feature_comparison",
      modulesChecked: [
        "vehicle_feature_catalog_v2",
        "vehicle_variant_feature_matrix_v2",
      ],
      dataSource: "vehicle_feature_catalog_v2+vehicle_variant_feature_matrix_v2",
      recordCount: allRows.length,
      matched: rows.length,
      modelCount: models.length,
      featureKeys: parsed.featureKeys,
    },
    runtimeResultsMeta: [
      {
        tool: "vehicle_feature_comparison",
        matched: rows.length,
        source: "vehicle_feature_catalog_v2+vehicle_variant_feature_matrix_v2",
        modulesChecked: [
          "vehicle_feature_catalog_v2",
          "vehicle_variant_feature_matrix_v2",
        ],
        error: "",
      },
    ],
    meta: {
      featureComparisonQuery: true,
      featureRequestCatalogCounts: parsed.catalogCounts,
    },
  };
};
