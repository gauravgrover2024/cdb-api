import mongoose from "mongoose";

import {
  normalizeSearchKey,
  normalizeText,
  normalizeCity,
} from "./aiAgent.planSchema.js";
import {
  discoverCollections,
  finalizeIndex,
  getDb,
  mergeColor,
  mergeModel,
  mergeVariant,
  normalizeColorRecord,
  normalizeModelRecord,
  normalizeVariantRecord,
  safeFindDocs,
  unique,
} from "./aiAgent.vehicleEntityIndex.normalizers.js";
import {
  containsAlias,
  findColorMatches,
  findModelMatches,
  findVariantMatches,
} from "./aiAgent.vehicleEntityIndex.matchers.js";

const DEFAULT_TTL_MS = Number(
  process.env.ACI_ENTITY_INDEX_TTL_MS || 15 * 60 * 1000,
);
const DEFAULT_DOC_LIMIT = Number(
  process.env.ACI_ENTITY_INDEX_DOC_LIMIT || 10000,
);

const DEFAULT_ENTITY_CITY_SLUG =
  String(process.env.ACI_ENTITY_INDEX_CITY_SLUG || "new-delhi").trim() ||
  "new-delhi";

const DEFAULT_COLLECTION_NAMES = [
  "aci_vehicle_model_summary",
  "aci_vehicle_price_rows",
];

let cache = {
  builtAt: 0,
  index: null,
  promise: null,
};

export const buildVehicleEntityIndex = async () => {
  const db = getDb();

  if (!db) {
    return finalizeIndex({
      modelMap: new Map(),
      variantMap: new Map(),
      colorMap: new Map(),
    });
  }

  const collectionNames = await discoverCollections(db);

  const modelMap = new Map();
  const variantMap = new Map();
  const colorMap = new Map();

  for (const collectionName of collectionNames) {
    const docs = await safeFindDocs(db, collectionName);

    for (const doc of docs) {
      const modelRecord = normalizeModelRecord(doc, collectionName);
      const mergedModel = mergeModel(modelMap, modelRecord);

      const variantRecord = normalizeVariantRecord(
        doc,
        collectionName,
        mergedModel,
      );
      mergeVariant(variantMap, variantRecord);

      const colorRecord = normalizeColorRecord(
        doc,
        collectionName,
        mergedModel,
      );
      mergeColor(colorMap, colorRecord);
    }
  }

  return finalizeIndex({
    modelMap,
    variantMap,
    colorMap,
  });
};

export const getVehicleEntityIndex = async ({ forceRefresh = false } = {}) => {
  const now = Date.now();

  if (!forceRefresh && cache.index && now - cache.builtAt < DEFAULT_TTL_MS) {
    return cache.index;
  }

  if (!forceRefresh && cache.promise) return cache.promise;

  cache.promise = buildVehicleEntityIndex()
    .then((index) => {
      cache.index = index;
      cache.builtAt = Date.now();
      cache.promise = null;
      return index;
    })
    .catch((error) => {
      cache.promise = null;
      console.error(
        "[ACI Assist] Failed to build vehicle entity index:",
        error,
      );
      return finalizeIndex({
        modelMap: new Map(),
        variantMap: new Map(),
        colorMap: new Map(),
      });
    });

  return cache.promise;
};

export const clearVehicleEntityIndexCache = () => {
  cache = {
    builtAt: 0,
    index: null,
    promise: null,
  };
};

export {
  findColorMatches,
  findModelMatches,
  findVariantMatches,
} from "./aiAgent.vehicleEntityIndex.matchers.js";

export const resolveVehicleEntities = async ({
  message = "",
  context = {},
  selectedEntity = null,
  forceRefresh = false,
} = {}) => {
  const index = await getVehicleEntityIndex({ forceRefresh });

  const selectedVehicle =
    selectedEntity ||
    context?.selectedVehicle ||
    context?.anchorVehicle ||
    context?.vehicle ||
    {};

  const anchorModel =
    selectedVehicle?.model || context?.anchorModel || context?.model || "";

  const anchorVariant =
    selectedVehicle?.variant ||
    context?.anchorVariant ||
    context?.variant ||
    "";

  const anchorCity =
    selectedVehicle?.city || context?.anchorCity || context?.city || "";

  const modelMatches = findModelMatches(index, message);
  const primaryModel = modelMatches[0]?.model || normalizeText(anchorModel);
  const primaryBrand =
    modelMatches[0]?.brand ||
    selectedVehicle?.brand ||
    selectedVehicle?.make ||
    "";

  let comparisonModels = modelMatches.map((item) => item.model);

  const textKey = normalizeSearchKey(message);

  if (
    anchorModel &&
    /\b(compare|vs|versus|with)\b/.test(textKey) &&
    !comparisonModels.includes(anchorModel)
  ) {
    comparisonModels.unshift(anchorModel);
  }

  comparisonModels = unique(comparisonModels);

  const variantMatches = findVariantMatches(index, message, {
    model: primaryModel,
    brand: primaryBrand,
  });

  const colorMatches = findColorMatches(index, message, {
    model: primaryModel,
    brand: primaryBrand,
  });

  const primaryVariant =
    variantMatches[0]?.variant || normalizeText(anchorVariant);

  return {
    index,
    primaryModel,
    primaryBrand,
    primaryVariant,
    primaryCity: anchorCity ? normalizeCity(anchorCity) : "",
    modelMatches,
    variantMatches,
    colorMatches,
    comparisonModels,
    selectedVehicle,
    counts: index.counts,
  };
};

export const selectRepresentativeVariant = async ({
  model = "",
  brand = "",
  preferredTransmission = "",
  preferredFuel = "",
  targetPrice = null,
  selectedVariant = "",
} = {}) => {
  const index = await getVehicleEntityIndex();

  const shortModelKey = normalizeSearchKey(model);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);

  const candidates = index.variants.filter((variant) => {
    if (!variant.active) return false;

    return (
      variant.shortModelKey === shortModelKey ||
      variant.modelKey === modelKey ||
      normalizeSearchKey(variant.model) === shortModelKey
    );
  });

  if (!candidates.length) {
    return {
      model,
      variantStrategy: "representative_default",
    };
  }

  if (selectedVariant) {
    const selectedKey = normalizeSearchKey(selectedVariant);
    const exact = candidates.find(
      (variant) =>
        variant.shortVariantKey === selectedKey ||
        containsAlias(normalizeSearchKey(variant.variant), selectedKey),
    );

    if (exact) return exact;
  }

  const scored = candidates.map((variant) => {
    let score = 0;

    const transmissionKey = normalizeSearchKey(variant.transmission);
    const fuelKey = normalizeSearchKey(variant.fuelType);

    if (
      preferredTransmission &&
      transmissionKey.includes(normalizeSearchKey(preferredTransmission))
    ) {
      score += 40;
    }

    if (preferredFuel && fuelKey.includes(normalizeSearchKey(preferredFuel))) {
      score += 25;
    }

    if (targetPrice && variant.price) {
      const distance = Math.abs(Number(variant.price) - Number(targetPrice));
      score += Math.max(0, 30 - distance / 50000);
    }

    if (
      /automatic|ivt|cvt|dct|amt|at/i.test(
        `${variant.variant} ${variant.transmission}`,
      )
    ) {
      score += 10;
    }

    if (
      /sx|zx|zxi|htx|gtx|alpha|creative|accomplished|top/i.test(variant.variant)
    ) {
      score += 8;
    }

    if (variant.price) score += 4;

    return {
      ...variant,
      representativeScore: score,
    };
  });

  scored.sort((a, b) => b.representativeScore - a.representativeScore);

  return scored[0];
};

export const getAutocompleteEntityMatches = async ({
  query = "",
  context = {},
  limit = 8,
} = {}) => {
  const index = await getVehicleEntityIndex();
  const q = normalizeSearchKey(query);
  const safeLimit = Math.min(Math.max(Number(limit) || 8, 1), 12);

  if (!q) return [];

  const modelMatches = [];

  for (const model of index.models) {
    const bag = normalizeSearchKey(
      `${model.brand} ${model.model} ${(model.aliases || []).join(" ")}`,
    );

    if (bag.includes(q)) {
      modelMatches.push({
        id: `model-${model.modelKey}`,
        type: "model",
        label: model.displayName || `${model.brand} ${model.model}`.trim(),
        subLabel: [
          model.variantsCount ? `${model.variantsCount} variants` : "",
          model.fuelTypes?.slice(0, 2).join(" / "),
          model.transmissions?.slice(0, 2).join(" / "),
        ]
          .filter(Boolean)
          .join(" • "),
        icon: "car",
        autotypeText: model.displayName || model.model,
        sendOnClick: false,
        priority: model.priority || 50,
        entity: {
          brand: model.brand,
          model: model.model,
        },
      });
    }
  }

  modelMatches.sort((a, b) => b.priority - a.priority);

  const topModel = modelMatches[0];

  const querySuggestions = [];

  if (topModel) {
    const modelLabel = topModel.entity.model;

    querySuggestions.push(
      {
        id: `query-price-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Show ${modelLabel} price`,
        subLabel: "Variant-wise pricelist",
        icon: "tag",
        autotypeText: `Show ${modelLabel} price`,
        sendOnClick: false,
        priority: 80,
        tool: "vehicle_pricelist",
        entity: topModel.entity,
      },
      {
        id: `query-colors-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Show ${modelLabel} colors`,
        subLabel: "Exterior color gallery",
        icon: "paintbrush",
        autotypeText: `Show colors of ${modelLabel}`,
        sendOnClick: false,
        priority: 76,
        tool: "vehicle_colors",
        entity: topModel.entity,
      },
      {
        id: `query-emi-${normalizeSearchKey(modelLabel)}`,
        type: "query",
        label: `Calculate EMI for ${modelLabel}`,
        subLabel: "On-road EMI estimate",
        icon: "calculator",
        autotypeText: `Calculate EMI for ${modelLabel}`,
        sendOnClick: false,
        priority: 72,
        tool: "vehicle_emi",
        entity: topModel.entity,
      },
    );
  }

  return [...modelMatches.slice(0, safeLimit), ...querySuggestions]
    .sort((a, b) => (b.priority || 0) - (a.priority || 0))
    .slice(0, safeLimit);
};

export default getVehicleEntityIndex;
