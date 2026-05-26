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
  findColorMatches,
  findModelMatches,
  findVariantMatches,
} from "./aiAgent.vehicleEntityIndex.matchers.js";
import { buildAutocompleteEntityMatchesFromIndex } from "./aiAgent.vehicleEntityIndex.autocomplete.js";
import { buildRepresentativeVariantFromIndex } from "./aiAgent.vehicleEntityIndex.representativeVariant.js";

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

  return buildRepresentativeVariantFromIndex({
    index,
    model,
    brand,
    preferredTransmission,
    preferredFuel,
    targetPrice,
    selectedVariant,
  });
};

export const getAutocompleteEntityMatches = async ({
  query = "",
  context = {},
  limit = 8,
} = {}) => {
  const index = await getVehicleEntityIndex();

  return buildAutocompleteEntityMatchesFromIndex({
    index,
    query,
    context,
    limit,
  });
};

export default getVehicleEntityIndex;
