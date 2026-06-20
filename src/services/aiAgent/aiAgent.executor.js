import {
  sanitizePlannerPlan,
  validatePlannerPlan,
  normalizeSearchKey,
} from "./aiAgent.planSchema.js";

import {
  buildAciAssistResponseFromPlan,
  validateAciAssistResponseContract,
  asArray,
  cleanText,
  displayName,
  firstMeaningful,
  getCity,
  getFeature,
  getModel,
  getModels,
  getVariant,
  formatMoney,
} from "./aiAgent.responseTools.js";

import { sanitizeAiAgentResponse } from "./aiAgent.responseSanitizer.js";
import { runAciV2Tool } from "./tools/index.js";
import { runVehiclePricelistNewCarsTool } from "./tools/newCars/vehiclePricelist.tool.js";
import { maybeRunAciFeatureComparisonAnswer } from "./aiAgent.featureComparisonAnswer.js";
import { mergeContextPatches } from "../aciCore/context/aciContextManager.service.js";
import { runVehicleSpecAttributeLookup } from "../aciCore/specs/aciVehicleSpecAttributeResolver.service.js";
import mongoose from "mongoose";
import { runVehicleScoreInsightTool } from "./tools/newCars/vehicleScoreInsight.tool.js";

/**
 * ACI Assist Executor
 *
 * Purpose:
 * - Planner decides tool + entities + filters.
 * - Executor runs deterministic runtime-data tools.
 * - ResponseTools converts runtime data into frontend response contract.
 * - ResponseSanitizer performs final customer-facing cleanup.
 *
 * This file intentionally does NOT use:
 * - old aiAgent.tools.js
 * - old aiAgent.toolRegistry.js
 *
 * Flow:
 * userMessage + plannerPlan + context
 * -> execute runtime data tools
 * -> build response contract
 * -> sanitize final answer
 * -> validate response shape
 */

/* -------------------------------------------------------------------------- */
/*  Constants                                                                 */
/* -------------------------------------------------------------------------- */

export const EXECUTOR_VERSION = "aci-assist-executor-v1";

export const DEFAULT_CITY = "new-delhi";

export const DEFAULT_EMI = {
  roi: 9.5,
  tenureMonths: 60,
  loanPercent: 80,
};

export const DEFAULT_LIMITS = {
  pricelist: 120,
  colors: 60,
  featureLookup: 80,
  compare: 12,
  recommend: 24,
  history: 120,
};

export const BUDGET_DISCOVERY_PREVIEW_GROUP_LIMIT = 8;
export const BUDGET_DISCOVERY_FULL_GROUP_LIMIT = 200;
export const BUDGET_DISCOVERY_VARIANTS_PER_GROUP_LIMIT = 6;

export const VEHICLE_COLLECTION_CANDIDATES = [
  "vehicles",
  "vehicle_master_records",
  "vehicle_prices",
  "vehicleprices",
  "vehicle_variants",
  "vehiclevariants",
  "new_car_variants",
  "newcarvariants",
  "car_variants",
  "carvariants",
  "cars",
  "prices",
  "features",
  "vehicle_features",
  "vehiclefeatures",
];

export const COLOR_COLLECTION_CANDIDATES = [
  "vehicle_colors_v2",
  "vehiclecolors",
  "colors",
  "car_colors",
  "carcolors",
  "features",
  "vehicle_features",
  "vehiclefeatures",
  "vehicle_variants",
  "vehiclevariants",
  "cars",
];

export const FEATURE_COLLECTION_CANDIDATES = [
  "vehicle_features",
  "vehiclefeatures",
  "features",
  "car_features",
  "carfeatures",
  "vehicle_variants",
  "vehiclevariants",
  "cars",
];

export const PRICE_HISTORY_COLLECTION_CANDIDATES = [
  "vehicle_price_history",
  "vehiclepricehistory",
  "price_history",
  "pricehistory",
  "vehicle_prices",
  "vehicleprices",
  "prices",
];

/* -------------------------------------------------------------------------- */
/*  Basic Helpers                                                             */
/* -------------------------------------------------------------------------- */

export const searchKey = (value = "") => normalizeSearchKey(value || "");

export const isPlainObject = (value) =>
  Boolean(value) &&
  typeof value === "object" &&
  !Array.isArray(value) &&
  !(value instanceof Date);

export const unique = (items = []) => [...new Set(asArray(items).filter(Boolean))];

export const uniqueBy = (items = [], getKey = (item) => item) => {
  const seen = new Set();
  const out = [];

  for (const item of asArray(items)) {
    const key = searchKey(getKey(item));
    if (!key || seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }

  return out;
};

export const numberFromValue = (value) => {
  if (typeof value === "number") return Number.isFinite(value) ? value : null;

  const text = String(value ?? "")
    .replace(/,/g, "")
    .trim();

  if (!text) return null;

  const match = text.match(/-?\d+(?:\.\d+)?/);
  if (!match) return null;

  const number = Number(match[0]);
  return Number.isFinite(number) ? number : null;
};

export const amountFromValue = (value) => {
  if (typeof value === "number") {
    return Number.isFinite(value) ? Math.round(value) : 0;
  }

  const text = String(value ?? "").toLowerCase();
  const number = numberFromValue(value);

  if (number === null) return 0;

  if (/\bcr|crore|crores\b/.test(text) && number <= 100) {
    return Math.round(number * 10000000);
  }

  if (/\blakh|lakhs|lac|lacs\b/.test(text) && number <= 300) {
    return Math.round(number * 100000);
  }

  return Math.round(number);
};

export const firstNumber = (...values) => {
  for (const value of values) {
    const number = amountFromValue(value);
    if (number > 0) return number;
  }

  return 0;
};

export const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const regexFor = (value = "") => {
  const text = cleanText(value);
  if (!text) return null;
  return new RegExp(escapeRegex(text), "i");
};

export const compactObject = (object = {}) =>
  Object.fromEntries(
    Object.entries(object || {}).filter(([, value]) => {
      if (Array.isArray(value)) return value.length > 0;
      if (isPlainObject(value)) return Object.keys(value).length > 0;
      return value !== undefined && value !== null && value !== "";
    }),
  );

export const safeJsonText = (value = {}) => {
  try {
    return searchKey(JSON.stringify(value || {}));
  } catch {
    return "";
  }
};

/* -------------------------------------------------------------------------- */
/*  Optional Mongo Access                                                     */
/* -------------------------------------------------------------------------- */

const executorDecisionLanguageText = (templateKey = "", input = {}) => {
  if (templateKey === "decision_score_module_summary_note") {
    return "Use this as directional module-score diagnostics, not as a final purchase verdict.";
  }
  return "";
};

const ACI_EXECUTOR_COLLECTION_CACHE_TTL_MS =
  Number(process.env.ACI_COLLECTION_DISCOVERY_CACHE_TTL_MS || 10 * 60 * 1000);

let executorCollectionListCache = null;
let executorCollectionListCachedAt = 0;
const executorCollectionNameCache = new Map();

export const getMongooseDb = async () => {
  try {
    const module = await import("mongoose");
    const mongoose = module.default || module;

    if (mongoose.connection?.readyState === 1 && mongoose.connection?.db) {
      return mongoose.connection.db;
    }

    return null;
  } catch {
    return null;
  }
};

export const listDbCollections = async (db, { force = false } = {}) => {
  if (!db) return [];

  const now = Date.now();

  if (
    !force &&
    executorCollectionListCache &&
    now - executorCollectionListCachedAt < ACI_EXECUTOR_COLLECTION_CACHE_TTL_MS
  ) {
    return executorCollectionListCache;
  }

  try {
    const collections = await db.listCollections({}, { nameOnly: true }).toArray();
    executorCollectionListCache = collections;
    executorCollectionListCachedAt = now;
    return collections;
  } catch {
    return executorCollectionListCache || [];
  }
};

export const findCollectionName = async (db, candidates = []) => {
  const cleanCandidates = (candidates || [])
    .map((item) => String(item || "").trim())
    .filter(Boolean);

  const cacheKey = cleanCandidates.join("|").toLowerCase();
  const cached = executorCollectionNameCache.get(cacheKey);

  if (
    cached &&
    Date.now() - cached.cachedAt < ACI_EXECUTOR_COLLECTION_CACHE_TTL_MS
  ) {
    return cached.collectionName;
  }

  const collections = await listDbCollections(db);
  const names = collections.map((item) => item.name).filter(Boolean);

  if (!names.length) return "";

  const exactCandidate = cleanCandidates.find((candidate) =>
    names.includes(candidate),
  );

  if (exactCandidate) {
    executorCollectionNameCache.set(cacheKey, {
      collectionName: exactCandidate,
      cachedAt: Date.now(),
    });
    return exactCandidate;
  }

  const normalizedNames = names.map((name) => ({
    name,
    key: searchKey(name),
  }));

  for (const candidate of cleanCandidates) {
    const candidateKey = searchKey(candidate);
    if (!candidateKey) continue;

    const exact = normalizedNames.find((item) => item.key === candidateKey);
    if (exact) {
      executorCollectionNameCache.set(cacheKey, {
        collectionName: exact.name,
        cachedAt: Date.now(),
      });
      return exact.name;
    }

    const contains = normalizedNames.find(
      (item) => item.key.includes(candidateKey) || candidateKey.includes(item.key),
    );
    if (contains) {
      executorCollectionNameCache.set(cacheKey, {
        collectionName: contains.name,
        cachedAt: Date.now(),
      });
      return contains.name;
    }
  }

  const collectionName = normalizedNames[0]?.name || "";

  executorCollectionNameCache.set(cacheKey, {
    collectionName,
    cachedAt: Date.now(),
  });

  return collectionName;
};

export const getCollection = async (candidates = []) => {
  const db = await getMongooseDb();
  if (!db) {
    return {
      db: null,
      collection: null,
      collectionName: "",
      reason: "mongoose_not_connected",
    };
  }

  const collectionName = await findCollectionName(db, candidates);

  if (!collectionName) {
    return {
      db,
      collection: null,
      collectionName: "",
      reason: "collection_not_found",
    };
  }

  return {
    db,
    collection: db.collection(collectionName),
    collectionName,
    reason: "",
  };
};

/* -------------------------------------------------------------------------- */
/*  Mongo Query Helpers                                                       */
/* -------------------------------------------------------------------------- */

export const modelQuery = (model = "") => {
  const regex = regexFor(model);
  if (!regex) return null;

  return {
    $or: [
      { model: regex },
      { modelName: regex },
      { model_name: regex },
      { vehicleModel: regex },
      { carModel: regex },
      { displayModel: regex },
      { rootModel: regex },
      { name: regex },
      { title: regex },
    ],
  };
};

export const variantQuery = (variant = "") => {
  const regex = regexFor(variant);
  if (!regex) return null;

  return {
    $or: [
      { variant: regex },
      { variantName: regex },
      { variant_name: regex },
      { vehicleVariant: regex },
      { trim: regex },
      { name: regex },
      { title: regex },
    ],
  };
};

export const brandQuery = (brand = "") => {
  const regex = regexFor(brand);
  if (!regex) return null;

  return {
    $or: [
      { brand: regex },
      { make: regex },
      { makeName: regex },
      { manufacturer: regex },
    ],
  };
};

export const cityQuery = (city = "") => {
  const regex = regexFor(String(city || "").replace(/-/g, " "));
  if (!regex) return null;

  return {
    $or: [
      { city: regex },
      { cityName: regex },
      { citySlug: regex },
      { location: regex },
      { state: regex },
    ],
  };
};

export const buildVehicleMongoQuery = ({
  toolPlan = {},
  context = {},
  includeCity = false,
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const brand = firstMeaningful(toolPlan.entities?.brand, toolPlan.entities?.make);
  const city = getCity(toolPlan, context);

  const and = [
    brandQuery(brand),
    modelQuery(model),
    variantQuery(variant),
    includeCity ? cityQuery(city) : null,
  ].filter(Boolean);

  if (!and.length) return {};

  return { $and: and };
};

const normalizeEntityToken = (value = "") =>
  displayName(cleanText(String(value || "").replace(/-/g, " ")));

const normalizeCitySlug = (value = "") =>
  cleanText(String(value || ""))
    .toLowerCase()
    .replace(/\s+/g, "-");

const fieldEqOrIn = (field, values = []) => {
  const uniq = unique(values.filter(Boolean));
  if (!uniq.length) return null;
  if (uniq.length === 1) return { [field]: uniq[0] };
  return { [field]: { $in: uniq } };
};

export const buildFastVehiclesQuery = ({
  toolPlan = {},
  context = {},
  includeCity = false,
} = {}) => {
  const model = normalizeEntityToken(getModel(toolPlan, context));
  const variant = normalizeEntityToken(getVariant(toolPlan, context));
  const brand = normalizeEntityToken(
    firstMeaningful(toolPlan.entities?.brand, toolPlan.entities?.make),
  );
  const cityRaw = getCity(toolPlan, context);
  const citySlug = normalizeCitySlug(cityRaw);

  const and = [
    fieldEqOrIn("brand_normalized", [brand]),
    fieldEqOrIn("model_normalized", [model]),
    fieldEqOrIn("variant_normalized", [variant]),
    includeCity
      ? fieldEqOrIn("city", [
          citySlug,
          cleanText(cityRaw).toLowerCase(),
          normalizeEntityToken(cityRaw),
        ])
      : null,
  ].filter(Boolean);

  if (!and.length) return {};
  return { $and: and };
};

export const buildFastBrandModelQuery = ({
  toolPlan = {},
  context = {},
  includeCity = false,
} = {}) => {
  const model = normalizeEntityToken(getModel(toolPlan, context));
  const variant = normalizeEntityToken(getVariant(toolPlan, context));
  const brand = normalizeEntityToken(
    firstMeaningful(toolPlan.entities?.brand, toolPlan.entities?.make),
  );
  const cityRaw = getCity(toolPlan, context);
  const citySlug = normalizeCitySlug(cityRaw);

  const modelCandidates = unique(
    [model, brand && model ? `${brand} ${model}` : ""].filter(Boolean),
  );
  const variantCandidates = unique(
    [variant, model && variant ? `${model} ${variant}` : ""].filter(Boolean),
  );

  const and = [
    fieldEqOrIn("brand", [brand]),
    fieldEqOrIn("model", modelCandidates),
    fieldEqOrIn("variant", variantCandidates),
    includeCity
      ? fieldEqOrIn("city", [
          citySlug,
          cleanText(cityRaw).toLowerCase(),
          normalizeEntityToken(cityRaw),
        ])
      : null,
  ].filter(Boolean);

  if (!and.length) return {};
  return { $and: and };
};

export const safeFind = async (
  collection,
  query = {},
  {
    limit = 80,
    projection = {},
    sort = {},
  } = {},
) => {
  if (!collection) return [];

  try {
    return await collection
      .find(query, { projection })
      .sort(sort)
      .limit(limit)
      .toArray();
  } catch {
    try {
      return await collection.find({}).limit(limit).toArray();
    } catch {
      return [];
    }
  }
};

/* -------------------------------------------------------------------------- */
/*  Normalizers                                                               */
/* -------------------------------------------------------------------------- */

export const normalizeVehicleRow = (row = {}) => {
  const model = displayName(
    firstMeaningful(
      row.model,
      row.modelName,
      row.model_name,
      row.vehicleModel,
      row.carModel,
      row.displayModel,
      row.rootModel,
    ),
  );

  const variant = displayName(
    firstMeaningful(
      row.variant_short,
      row.variantShort,
      row.variant_normalized,
      row.variant,
      row.variantName,
      row.variant_name,
      row.vehicleVariant,
      row.trim,
      row.name,
      row.title,
    ),
  );

  const brand = displayName(
    firstMeaningful(row.brand, row.make, row.makeName, row.manufacturer),
  );

  const fuelType = displayName(
    firstMeaningful(row.fuelType, row.fuel, row.fuel_type, row.engineFuel),
  );

  const transmission = displayName(
    firstMeaningful(row.transmission, row.gearbox, row.transmissionType),
  );

  const bodyType = displayName(
    firstMeaningful(row.bodyType, row.body_type, row.segment, row.category),
  );

  const exShowroomPrice = firstNumber(
    row.exShowroomPrice,
    row.ex_showroom_price_cardekho,
    row.ex_showroom_price,
    row.exShowroom,
    row.ex_showroom,
    row.exshowroom,
    row.price,
    row.basePrice,
  );

  const onRoadPrice = firstNumber(
    row.onRoadPrice,
    row.on_road_price_cardekho,
    row.on_road_price,
    row.onRoad,
    row.on_road,
    row.total_on_road_with_accessories,
    row.orp_without_accessories,
    row.finalPrice,
    row.totalPrice,
  );

  const rto = firstNumber(row.rto, row.rtoAmount, row.roadTax, row.road_tax);
  const insurance = firstNumber(
    row.insurance,
    row.insuranceAmount,
    row.insuranceCost,
  );
  const tcs = firstNumber(row.tcs, row.tcsAmount);
  const handling = firstNumber(row.handling, row.handlingCharges);
  const fastag = firstNumber(row.fastag, row.fastTag);
  const accessories = firstNumber(row.accessories, row.optionalAccessories);

  const colors = normalizeColors(row);
  const features = normalizeFeatures(row);

  const discontinued =
    Boolean(row.discontinued) ||
    Boolean(row.isDiscontinued) ||
    row.status === "discontinued" ||
    row.active === false ||
    row.isActive === false;

  return compactObject({
    id: String(row._id || row.id || ""),
    brand,
    make: brand,
    model,
    variant,
    fuelType,
    transmission,
    bodyType,
    exShowroomPrice,
    onRoadPrice,
    rto,
    insurance,
    tcs,
    handling,
    fastag,
    accessories,
    colors,
    features,
    variantShort: row.variant_short || row.variantShort || "",
    variantNormalized: row.variant_normalized || "",
    modelNormalized: row.model_normalized || "",
    brandNormalized: row.brand_normalized || "",
    searchText: row.search_text || "",
    imageUrl: firstMeaningful(
      row.normalizedImageUrl,
      row.cleanImageUrl,
      row.normalized_image_url,
      row.clean_image_url,
      row.normalizedImagePngUrl,
      row.imageUrl,
      row.image_url,
      row.carImageUrl,
      row.car_image_url,
      row.photo,
      row.photo_url,
    ),
    discontinued,
    active: !discontinued,
    raw: row,
  });
};

export const normalizeColors = (row = {}) => {
  const rawColors = firstMeaningful(
    row.colors,
    row.colours,
    row.color_name,
    row.colorName,
    row.availableColors,
    row.availableColours,
    row.colorOptions,
    row.exteriorColors,
    row.exteriorColours,
    row.color,
    row.colour,
  );

  if (!rawColors) return [];

  if (Array.isArray(rawColors)) {
    return unique(
      rawColors
        .map((item) => {
          if (typeof item === "string") return displayName(item);
          return displayName(
            firstMeaningful(item.name, item.color, item.colour, item.title),
          );
        })
        .filter(Boolean),
    );
  }

  if (typeof rawColors === "string") {
    return unique(
      rawColors
        .split(/[,/|]+/)
        .map(displayName)
        .filter(Boolean),
    );
  }

  if (isPlainObject(rawColors)) {
    return unique(
      Object.values(rawColors)
        .flat()
        .map((item) =>
          typeof item === "string"
            ? displayName(item)
            : displayName(firstMeaningful(item?.name, item?.color, item?.colour)),
        )
        .filter(Boolean),
    );
  }

  return [];
};

export const normalizeFeatures = (row = {}) => {
  const buckets = [
    row.features,
    row.keyFeatures,
    row.key_features,
    row.specs,
    row.specifications,
    row.equipment,
    row.featureList,
    row.feature_list,
  ].filter(Boolean);

  const features = [];

  for (const bucket of buckets) {
    if (Array.isArray(bucket)) {
      for (const item of bucket) {
        if (typeof item === "string") features.push(displayName(item));
        else if (isPlainObject(item)) {
          features.push(
            displayName(
              firstMeaningful(
                item.name,
                item.label,
                item.title,
                item.feature,
                item.key,
              ),
            ),
          );
        }
      }
    } else if (isPlainObject(bucket)) {
      for (const [key, value] of Object.entries(bucket)) {
        if (typeof value === "boolean") {
          if (value) features.push(displayName(key));
        } else if (typeof value === "string" || typeof value === "number") {
          features.push(displayName(`${key} ${value}`));
        } else if (Array.isArray(value)) {
          features.push(displayName(key));
          value.forEach((item) => {
            if (typeof item === "string") features.push(displayName(item));
            else if (isPlainObject(item)) {
              features.push(displayName(firstMeaningful(item.name, item.label)));
            }
          });
        } else {
          features.push(displayName(key));
        }
      }
    } else if (typeof bucket === "string") {
      features.push(...bucket.split(/[,/|]+/).map(displayName));
    }
  }

  return unique(features.filter(Boolean));
};


export const normalizeVariantKeyForMatch = (value = "") =>
  searchKey(value)
    .replace(/\s+/g, " ")
    .trim();

export const removeKnownVehicleTerms = (value = "", row = {}) => {
  let output = ` ${normalizeVariantKeyForMatch(value)} `;

  const termsToRemove = [
    row.brand,
    row.make,
    row.brandNormalized,
    row.model,
    row.modelNormalized,
    row.raw?.brand,
    row.raw?.make,
    row.raw?.brand_normalized,
    row.raw?.model,
    row.raw?.model_normalized,
  ]
    .filter(Boolean)
    .flatMap((term) => normalizeVariantKeyForMatch(term).split(" "))
    .filter(Boolean);

  for (const term of unique(termsToRemove)) {
    output = output.replace(new RegExp(`\\\\b${escapeRegex(term)}\\\\b`, "g"), " ");
  }

  return output.replace(/\s+/g, " ").trim();
};

export const variantCandidateValues = (row = {}) =>
  [
    row.variantShort,
    row.variantNormalized,
    row.variant,
    row.raw?.variant_short,
    row.raw?.variantShort,
    row.raw?.variant_normalized,
    row.raw?.variant,
    row.raw?.variantName,
    row.raw?.variant_name,
    row.raw?.vehicleVariant,
    row.raw?.trim,
    row.raw?.name,
    row.raw?.title,
    row.searchText,
    row.raw?.search_text,
  ].filter(Boolean);

export const variantMatchScore = (row = {}, requestedVariant = "") => {
  const requestedFull = normalizeVariantKeyForMatch(requestedVariant);
  const requestedLoose = requestedFull;

  if (!requestedFull) return 0;

  const requestedTokens = requestedLoose.split(" ").filter(Boolean);
  if (!requestedTokens.length) return 0;

  let best = 0;

  for (const value of variantCandidateValues(row)) {
    const candidateFull = normalizeVariantKeyForMatch(value);
    const candidateLoose = removeKnownVehicleTerms(value, row);

    const candidateKeys = unique([candidateFull, candidateLoose].filter(Boolean));

    for (const candidate of candidateKeys) {
      if (!candidate) continue;

      if (candidate === requestedFull) best = Math.max(best, 100);
      if (candidate === requestedLoose) best = Math.max(best, 98);

      if (
        candidate.includes(requestedFull) ||
        requestedFull.includes(candidate)
      ) {
        best = Math.max(best, 92);
      }

      const candidateTokens = candidate.split(" ").filter(Boolean);
      const allRequestedTokensPresent = requestedTokens.every((token) =>
        candidateTokens.includes(token),
      );

      if (allRequestedTokensPresent) {
        best = Math.max(best, 88);
      }
    }
  }

  return best;
};

export const buildVariantResolution = ({
  requestedVariant = "",
  rows = [],
  status = "not_required",
  matchedRows = [],
} = {}) => {
  if (!requestedVariant) {
    return {
      status: "not_required",
      requestedVariant: "",
      candidateVariants: [],
      message: "",
    };
  }

  const candidateVariants = unique(
    rows
      .map((row) => row.variant || row.variantShort || row.variantNormalized)
      .filter(Boolean),
  ).slice(0, 24);

  if (matchedRows.length) {
    return {
      status: "matched",
      requestedVariant,
      candidateVariants,
      message: `Matched requested variant ${requestedVariant}.`,
    };
  }

  return {
    status: "not_found",
    requestedVariant,
    candidateVariants,
    message: `I found the model, but not the exact variant ${requestedVariant} in DB records.`,
  };
};

export const rowMatchesFilters = (row = {}, filters = {}) => {
  const normalized = normalizeVehicleRow(row);

  if (filters.activeOnly && normalized.discontinued) return false;

  if (filters.budgetMax) {
    const price = normalized.onRoadPrice || normalized.exShowroomPrice;
    if (price && price > Number(filters.budgetMax)) return false;
  }

  if (filters.budgetMin) {
    const price = normalized.onRoadPrice || normalized.exShowroomPrice;
    if (price && price < Number(filters.budgetMin)) return false;
  }

  if (filters.transmission) {
    const left = searchKey(normalized.transmission);
    const right = searchKey(filters.transmission);

    if (right && left && !left.includes(right)) return false;
  }

  if (filters.fuelType) {
    const left = searchKey(normalized.fuelType);
    const right = searchKey(filters.fuelType);

    if (right && left && !left.includes(right)) return false;
  }

  if (filters.bodyType) {
    const left = searchKey(normalized.bodyType);
    const right = searchKey(filters.bodyType);

    if (right && left && !left.includes(right)) return false;
  }

  if (filters.mustHaveFeatures?.length) {
    const blob = safeJsonText(normalized.features);
    for (const feature of asArray(filters.mustHaveFeatures)) {
      if (!blob.includes(searchKey(feature))) return false;
    }
  }

  return true;
};

export const sortPriceRows = (rows = [], ranking = "") => {
  const normalized = [...rows];

  if (ranking === "price_high_to_low") {
    return normalized.sort(
      (a, b) =>
        (b.onRoadPrice || b.exShowroomPrice || 0) -
        (a.onRoadPrice || a.exShowroomPrice || 0),
    );
  }

  return normalized.sort(
    (a, b) =>
      (a.onRoadPrice || a.exShowroomPrice || 0) -
      (b.onRoadPrice || b.exShowroomPrice || 0),
  );
};

/* -------------------------------------------------------------------------- */
/*  EMI Helpers                                                               */
/* -------------------------------------------------------------------------- */

export const calculateEmi = ({
  price = 0,
  downPayment,
  loanAmount,
  loanPercent,
  tenureMonths,
  roi,
} = {}) => {
  const principal =
    loanAmount ||
    Math.max(
      0,
      price -
        (downPayment !== undefined
          ? Number(downPayment)
          : price * (1 - Number(loanPercent || DEFAULT_EMI.loanPercent) / 100)),
    );

  const months = Number(tenureMonths || DEFAULT_EMI.tenureMonths);
  const annualRoi = Number(roi || DEFAULT_EMI.roi);
  const monthlyRate = annualRoi / 12 / 100;

  if (!principal || !months || !monthlyRate) {
    return {
      price,
      principal,
      tenureMonths: months,
      roi: annualRoi,
      emi: 0,
      totalPayable: 0,
      totalInterest: 0,
    };
  }

  const emi =
    (principal * monthlyRate * Math.pow(1 + monthlyRate, months)) /
    (Math.pow(1 + monthlyRate, months) - 1);

  const roundedEmi = Math.round(emi);
  const totalPayable = roundedEmi * months;

  return {
    price,
    principal: Math.round(principal),
    tenureMonths: months,
    roi: annualRoi,
    emi: roundedEmi,
    totalPayable,
    totalInterest: Math.max(0, totalPayable - Math.round(principal)),
  };
};

/* -------------------------------------------------------------------------- */
/*  Runtime Data Tools                                                        */
/* -------------------------------------------------------------------------- */

export const runtimeVehiclePricelist = async ({
  toolPlan = {},
  context = {},
  userMessage = "",
  trace = [],
  access = {},
} = {}) => {
  return runVehiclePricelistNewCarsTool({
    toolPlan,
    context,
    userMessage:
      userMessage ||
      toolPlan.message ||
      toolPlan.query ||
      context.message ||
      "",
    trace,
    access,
  });
};

export const buildPriceSummary = (rows = []) => {
  const prices = rows
    .map((row) => row.onRoadPrice || row.exShowroomPrice)
    .filter((value) => value > 0);

  if (!prices.length) return {};

  return {
    minPrice: Math.min(...prices),
    maxPrice: Math.max(...prices),
    minPriceLabel: formatMoney(Math.min(...prices)),
    maxPriceLabel: formatMoney(Math.max(...prices)),
    rowCount: rows.length,
  };
};

export const runtimeVehicleColors = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const { collection, collectionName, reason } = await getCollection(
    COLOR_COLLECTION_CANDIDATES,
  );

  const fastQuery = buildFastBrandModelQuery({ toolPlan, context });
  const fallbackRegexQuery = buildVehicleMongoQuery({ toolPlan, context });
  let rawRows = await safeFind(collection, fastQuery, {
    limit: DEFAULT_LIMITS.colors,
  });
  if (!rawRows.length) {
    rawRows = await safeFind(collection, fallbackRegexQuery, {
      limit: DEFAULT_LIMITS.colors,
    });
  }

  const vehicleRows = rawRows.map(normalizeVehicleRow);

  const directEntries = rawRows
    .map((row) => {
      const name = displayName(
        firstMeaningful(
          row.color_name,
          row.colorName,
          row.color,
          row.colour,
          row.name,
          row.title,
          row.variant,
        ),
      );
      if (!name) return null;

      return {
        name,
        slug: searchKey(name).replace(/\s+/g, "-"),
        imageUrl: firstMeaningful(
          row.normalizedImageUrl,
          row.cleanImageUrl,
          row.normalized_image_url,
          row.clean_image_url,
          row.normalizedImagePngUrl,
          row.image_url,
          row.imageUrl,
          row.car_image_url,
          row.carImageUrl,
          row.swatch_image,
          row.swatchImage,
        ),
        hex: firstMeaningful(
          row.hex,
          row.hex_code,
          row.hexCode,
          row.color_hex,
          row.colorHex,
        ),
      };
    })
    .filter(Boolean);

  const normalizedNameEntries = unique(
    vehicleRows.flatMap((row) => asArray(row.colors)).filter(Boolean),
  ).map((name) => ({
    name,
    slug: searchKey(name).replace(/\s+/g, "-"),
    imageUrl: "",
    hex: "",
  }));

  const colorsBySlug = new Map();
  for (const item of [...directEntries, ...normalizedNameEntries]) {
    if (!item?.slug) continue;
    const existing = colorsBySlug.get(item.slug);
    if (!existing) {
      colorsBySlug.set(item.slug, item);
      continue;
    }
    colorsBySlug.set(item.slug, {
      ...existing,
      imageUrl: existing.imageUrl || item.imageUrl || "",
      hex: existing.hex || item.hex || "",
    });
  }

  const colors = [...colorsBySlug.values()];

  return {
    rows: colors,
    colors,
    count: colors.length,
    matched: colors.length,
    modulesChecked: [collectionName || reason || "vehicle_colors_v2"],
    source: collectionName || "none",
    dataSource: collectionName ? "mongodb" : "empty",
    variantWiseAvailability: false,
  };
};

export const runtimeVehicleFeatureLookup = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const { collection, collectionName, reason } = await getCollection(
    FEATURE_COLLECTION_CANDIDATES,
  );

  const feature = getFeature(toolPlan);
  const fastQuery = buildFastBrandModelQuery({ toolPlan, context });
  const fallbackRegexQuery = buildVehicleMongoQuery({ toolPlan, context });
  let rawRows = await safeFind(collection, fastQuery, {
    limit: DEFAULT_LIMITS.featureLookup,
  });
  if (!rawRows.length) {
    rawRows = await safeFind(collection, fallbackRegexQuery, {
      limit: DEFAULT_LIMITS.featureLookup,
    });
  }

  const normalizedRows = rawRows.map(normalizeVehicleRow);
  const featureKey = searchKey(feature);

  const rows = featureKey
    ? normalizedRows.filter((row) =>
        safeJsonText([row.features, row.raw]).includes(featureKey),
      )
    : normalizedRows;

  return {
    rows,
    count: rows.length,
    matched: rows.length,
    feature,
    modulesChecked: [collectionName || reason || "vehicle_feature_lookup"],
    source: collectionName || "none",
    dataSource: collectionName ? "mongodb" : "empty",
  };
};


const compareCleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const compareFirstText = (...values) => {
  for (const value of values) {
    const text = compareCleanText(value);
    if (text) return text;
  }
  return "";
};

const compareSlug = (value = "") =>
  compareCleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

const compareEscapeRegex = (value = "") =>
  compareCleanText(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const compareAsArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const compareUnique = (items = []) =>
  [...new Set(items.map(compareCleanText).filter(Boolean))];

const parseIndianPriceValue = (value) => {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  const text = compareCleanText(value).toLowerCase();
  if (!text) return null;

  const numeric = Number(text.replace(/[^0-9.]/g, ""));
  if (!Number.isFinite(numeric)) return null;

  if (text.includes("cr")) return Math.round(numeric * 10000000);
  if (text.includes("l")) return Math.round(numeric * 100000);
  return numeric > 10000 ? Math.round(numeric) : null;
};

const getComparablePriceValue = (row = {}) =>
  parseIndianPriceValue(
    row.onRoadPriceValue ??
      row.onRoadPriceNumeric ??
      row.onRoadPrice ??
      row.onRoadPriceLabel ??
      row.priceValue ??
      row.priceNumeric ??
      row.priceLabel,
  );

const formatInrShort = (value) => {
  const amount = Number(value);
  if (!Number.isFinite(amount)) return "";

  if (amount >= 10000000) {
    const cr = amount / 10000000;
    return `₹${cr.toFixed(cr >= 10 || Number.isInteger(cr) ? 0 : 2)}Cr`;
  }

  if (amount >= 100000) {
    const lakh = amount / 100000;
    return `₹${lakh.toFixed(lakh >= 10 || Number.isInteger(lakh) ? 0 : 2)}L`;
  }

  return `₹${Math.round(amount).toLocaleString("en-IN")}`;
};

const getComparisonVehicleLabel = (row = {}, target = {}) =>
  compareCleanText(
    [
      compareFirstText(
        row.fullModel,
        row.displayName,
        row.modelDisplayName,
        target.fullModel,
        target.model,
        row.model,
      ),
      compareFirstText(row.variant, row.variantName, target.variant, target.variantName),
    ]
      .filter(Boolean)
      .join(" "),
  );

const getFeatureEntryValue = (entry = {}) => {
  if (entry === undefined || entry === null) return "Not Available";
  if (typeof entry !== "object") return compareCleanText(entry) || "Not Available";

  return compareFirstText(
    entry.displayValue,
    entry.value,
    entry.availabilityStatus,
    entry.available === true ? "Yes" : "",
    entry.available === false ? "Not Available" : "",
  ) || "Not Available";
};

const isFeatureEntryAvailable = (entry = {}) => {
  if (!entry || typeof entry !== "object") return false;

  const status = compareCleanText(
    entry.displayValue || entry.value || entry.availabilityStatus || "",
  ).toLowerCase();

  // Display/value truth must override stale boolean flags from scraped data.
  if (
    status.includes("not available") ||
    status === "no" ||
    status === "false" ||
    status === "na" ||
    status === "n/a" ||
    status === "-"
  ) {
    return false;
  }

  if (entry.available === true) return true;
  if (entry.present === true) return true;
  if (entry.included === true) return true;

  if (!status) return false;

  return ["yes", "available", "standard", "single pane", "panoramic", "front", "rear"].some((token) =>
    status.includes(token),
  );
};

const getFeatureDisplayName = (key = "", entry = {}, catalog = {}) =>
  compareFirstText(
    catalog.displayName,
    catalog.name,
    catalog.label,
    entry.displayName,
    entry.name,
    key
      .split("_")
      .filter(Boolean)
      .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
      .join(" "),
  );

const getComparisonFeatureEntry = (doc = {}, featureKey = "") =>
  doc?.featuresByKey?.[featureKey] ||
  doc?.decisionSignals?.featuresByKey?.[featureKey] ||
  null;

const getComparisonFeatureKeys = (doc = {}) =>
  compareUnique([
    ...Object.keys(doc?.featuresByKey || {}),
    ...Object.keys(doc?.decisionSignals?.featuresByKey || {}),
  ]);

const COMPARISON_FEATURE_CATALOG_CACHE_TTL_MS = 10 * 60 * 1000;
let comparisonFeatureCatalogCache = {
  loadedAt: 0,
  catalogByKey: null,
  rowCount: 0,
};

const normalizeComparisonKeyText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const getComparisonKeyForms = (...values) => {
  const forms = [];

  for (const value of values) {
    const clean = normalizeComparisonKeyText(value);
    if (!clean) continue;

    const hyphen = clean.replace(/\s+/g, "-");
    const underscore = clean.replace(/\s+/g, "_");

    forms.push(hyphen, underscore);

    if (!clean.includes(" ")) forms.push(clean);
  }

  return compareUnique(forms.filter(Boolean));
};

const normalizeComparisonCitySlug = (value = "") => {
  const city = getComparisonKeyForms(value)[0] || "";
  if (!city) return "new-delhi";
  if (city === "delhi" || city === "new-delhi" || city === "new_delhi") return "new-delhi";
  return city;
};

const isSupportedComparisonCitySlug = (citySlug = "") =>
  ["new-delhi", "noida", "gurgaon"].includes(normalizeComparisonCitySlug(citySlug));

const loadComparisonFeatureCatalogByKey = async () => {
  const db = mongoose.connection?.db;
  if (!db) return new Map();

  const now = Date.now();
  if (
    comparisonFeatureCatalogCache.catalogByKey instanceof Map &&
    now - comparisonFeatureCatalogCache.loadedAt < COMPARISON_FEATURE_CATALOG_CACHE_TTL_MS
  ) {
    return comparisonFeatureCatalogCache.catalogByKey;
  }

  const catalogRows = await db
    .collection("vehicle_feature_catalog_v2")
    .find({})
    .project({
      canonicalKey: 1,
      key: 1,
      featureKey: 1,
      displayName: 1,
      name: 1,
      label: 1,
      category: 1,
      group: 1,
      priority: 1,
    })
    .toArray();

  const catalogByKey = new Map(
    catalogRows.flatMap((row) =>
      [row.canonicalKey, row.key, row.featureKey]
        .filter(Boolean)
        .map((key) => [key, row]),
    ),
  );

  comparisonFeatureCatalogCache = {
    loadedAt: now,
    catalogByKey,
    rowCount: catalogRows.length,
  };

  return catalogByKey;
};

const getComparisonTargetModelKeyForms = (target = {}) =>
  getComparisonKeyForms(
    target.modelKey,
    target.shortModelKey,
    target.canonicalKey,
    target.model,
    target.fullModel,
  );

const getComparisonTargetVariantKeyForms = (target = {}) =>
  getComparisonKeyForms(
    target.variantKey,
    target.variant,
    target.variantName,
    target.fullVariant,
    target.selectedVariant,
  );

const formatComparisonPriceLabel = (value) =>
  Number.isFinite(Number(value)) ? formatInrShort(Number(value)) : "";

const normalizeDirectComparisonRow = ({ row = {}, target = {} } = {}) => {
  const model = compareFirstText(row.model, target.model);
  const make = compareFirstText(row.make, target.make, target.brand);
  const variant = compareFirstText(row.variant, target.variant, target.variantName);

  return {
    ...row,
    make,
    brand: compareFirstText(row.brand, make),
    model,
    fullModel: compareFirstText(row.fullModel, [make, model].filter(Boolean).join(" "), model),
    displayName: compareFirstText(row.displayName, [make, model].filter(Boolean).join(" "), model),
    variant,
    variantName: compareFirstText(row.variantName, variant),
    fuelType: compareFirstText(row.fuelType, row.fuel),
    transmission: compareFirstText(row.transmission),
    exShowroomPriceLabel: compareFirstText(row.exShowroomPriceLabel, formatComparisonPriceLabel(row.exShowroomPrice)),
    onRoadPriceLabel: compareFirstText(row.onRoadPriceLabel, formatComparisonPriceLabel(row.onRoadPrice)),
    dataSource: compareFirstText(row.dataSource, "aci_vehicle_price_rows"),
  };
};

const findMatrixDocForComparisonRow = ({ row = {}, matrixDocs = [] } = {}) => {
  const modelForms = getComparisonKeyForms(row.modelKey, row.model, row.fullModel);
  const variantForms = getComparisonKeyForms(row.variantKey, row.variant, row.variantName);

  return (
    matrixDocs.find((doc = {}) =>
      modelForms.includes(doc.modelKey) && variantForms.includes(doc.variantKey),
    ) || null
  );
};

const resolveDirectComparisonRows = async ({
  targets = [],
  citySlug = "new-delhi",
  isVariantComparison = false,
} = {}) => {
  const db = mongoose.connection?.db;
  const normalizedCitySlug = normalizeComparisonCitySlug(citySlug);

  if (!db || !isSupportedComparisonCitySlug(normalizedCitySlug)) return null;
  if (!Array.isArray(targets) || targets.length < 2) return null;

  const priceProjection = {
    make: 1,
    makeKey: 1,
    brand: 1,
    model: 1,
    modelKey: 1,
    fullModel: 1,
    displayName: 1,
    variant: 1,
    variantName: 1,
    variantKey: 1,
    citySlug: 1,
    fuelType: 1,
    fuel: 1,
    transmission: 1,
    exShowroomPrice: 1,
    onRoadPrice: 1,
    exShowroomPriceLabel: 1,
    onRoadPriceLabel: 1,
    sortOrder: 1,
    bodyType: 1,
    bodyTypeKey: 1,
  };

  const directTargets = targets.slice(0, 2);
  const hasExplicitVariantTarget = directTargets.some((target = {}) =>
    getComparisonTargetVariantKeyForms(target).length,
  );

  const priceRows = await Promise.all(
    directTargets.map((target = {}) => {
      const modelKeyForms = getComparisonTargetModelKeyForms(target);
      const variantKeyForms = getComparisonTargetVariantKeyForms(target);

      if (!modelKeyForms.length) return Promise.resolve(null);

      const query = {
        citySlug: normalizedCitySlug,
        modelKey: { $in: modelKeyForms },
      };

      if (isVariantComparison && hasExplicitVariantTarget) {
        if (!variantKeyForms.length) return Promise.resolve(null);
        query.variantKey = { $in: variantKeyForms };
      }

      return db
        .collection("aci_vehicle_price_rows")
        .find(query)
        .project(priceProjection)
        .sort({ sortOrder: 1, exShowroomPrice: 1, variantKey: 1 })
        // No hard hint here: Atlas free-tier/storage cleanup may remove old named index.
        // The collection is small enough for Mongo planner to choose the available plan safely.
        .limit(1)
        .next();
    }),
  );

  if (priceRows.some((row) => !row)) return null;

  const allModelForms = compareUnique(
    priceRows.flatMap((row = {}) =>
      getComparisonKeyForms(row.modelKey, row.model, row.fullModel),
    ),
  );
  const allVariantForms = compareUnique(
    priceRows.flatMap((row = {}) =>
      getComparisonKeyForms(row.variantKey, row.variant, row.variantName),
    ),
  );

  if (!allModelForms.length || !allVariantForms.length) return null;

  const matrixDocs = await db
    .collection("vehicle_variant_feature_matrix_v2")
    .find({
      activePricelistMatched: true,
      modelKey: { $in: allModelForms },
      variantKey: { $in: allVariantForms },
    })
    .project({
      brandKey: 1,
      makeKey: 1,
      modelKey: 1,
      variant: 1,
      variantKey: 1,
      priceMin: 1,
      activePricelistMatched: 1,
      featuresByKey: 1,
      decisionSignals: 1,
    })
    .toArray();

  const orderedMatrixDocs = priceRows.map((row) =>
    findMatrixDocForComparisonRow({ row, matrixDocs }),
  );

  if (orderedMatrixDocs.some((doc) => !doc)) return null;

  const catalogByKey = await loadComparisonFeatureCatalogByKey();

  return {
    rows: priceRows.map((row, index) =>
      normalizeDirectComparisonRow({ row, target: targets[index] || {} }),
    ),
    matrixDocs: orderedMatrixDocs,
    catalogByKey,
    citySlug: normalizedCitySlug,
    resolutionMode: "direct_comparison_read_model",
  };
};

const fetchComparisonFeatureDoc = async ({ row = {}, target = {} } = {}) => {
  const db = mongoose.connection?.db;
  if (!db) return null;

  const modelKeys = compareUnique([
    row.modelKey,
    target.modelKey,
    compareSlug(row.model),
    compareSlug(target.model),
  ]);

  const variantTexts = compareUnique([
    row.variantKey,
    target.variantKey,
    compareSlug(row.variant),
    compareSlug(row.variantName),
    compareSlug(target.variant),
    compareSlug(target.variantName),
    row.variant,
    row.variantName,
    target.variant,
    target.variantName,
  ]);

  if (!modelKeys.length || !variantTexts.length) return null;

  const variantRegexes = variantTexts
    .filter((value) => !value.includes("_"))
    .map((value) => new RegExp(`^${compareEscapeRegex(value)}$`, "i"));

  const variantKeys = variantTexts
    .map(compareSlug)
    .filter(Boolean);

  const query = {
    modelKey: { $in: modelKeys },
    $or: [
      { variantKey: { $in: variantKeys } },
      ...(variantRegexes.length ? [{ variant: { $in: variantRegexes } }] : []),
    ],
  };

  return mongoose.connection.db
    .collection("vehicle_variant_feature_matrix_v2")
    .find(query)
    .limit(1)
    .next();
};

const buildVehicleComparisonEnrichment = async ({
  rows = [],
  targets = [],
  city = "new-delhi",
  matrixDocs: matrixDocsOverride = null,
  catalogByKey: catalogByKeyOverride = null,
} = {}) => {
  if (!Array.isArray(rows) || rows.length < 2) {
    return {
      comparisonSummary: {},
      featureDifferences: [],
      commonHighlights: [],
      decisionHighlights: [],
      matrixCoverage: [],
    };
  }

  const compared = rows.slice(0, 2).map((row, index) => ({
    row,
    target: targets[index] || {},
    label: getComparisonVehicleLabel(row, targets[index] || {}),
    priceValue: getComparablePriceValue(row),
  }));

  const matrixDocs = Array.isArray(matrixDocsOverride)
    ? matrixDocsOverride
    : await Promise.all(
        compared.map((item) =>
          fetchComparisonFeatureDoc({ row: item.row, target: item.target }),
        ),
      );

  const allFeatureKeys = compareUnique(
    matrixDocs.flatMap((doc) => getComparisonFeatureKeys(doc)),
  );

  let catalogByKey = catalogByKeyOverride instanceof Map ? catalogByKeyOverride : new Map();
  if (!catalogByKey.size && allFeatureKeys.length && mongoose.connection?.db) {
    catalogByKey = await loadComparisonFeatureCatalogByKey();
  }

  const featureComparisons = allFeatureKeys.map((featureKey) => {
    const entries = matrixDocs.map((doc) => getComparisonFeatureEntry(doc, featureKey));
    const catalog = catalogByKey.get(featureKey) || {};
    const values = {};
    const availability = {};

    compared.forEach((item, index) => {
      values[item.label] = getFeatureEntryValue(entries[index]);
      availability[item.label] = isFeatureEntryAvailable(entries[index]);
    });

    const valueSet = new Set(Object.values(values).map((value) => compareCleanText(value).toLowerCase()));
    const availabilitySet = new Set(Object.values(availability));

    return {
      featureKey,
      feature: getFeatureDisplayName(featureKey, entries.find(Boolean) || {}, catalog),
      category: compareFirstText(catalog.category, catalog.group),
      values,
      availability,
      differs: valueSet.size > 1 || availabilitySet.size > 1,
      anyAvailable: Object.values(availability).some(Boolean),
      allAvailable: Object.values(availability).every(Boolean),
      priority: Number.isFinite(Number(catalog.priority)) ? Number(catalog.priority) : 999,
    };
  });

  const featureDifferences = featureComparisons
    .filter((item) => item.differs && item.anyAvailable)
    .sort((a, b) => {
      const availabilityScoreA = Object.values(a.availability).filter(Boolean).length;
      const availabilityScoreB = Object.values(b.availability).filter(Boolean).length;
      if (availabilityScoreA !== availabilityScoreB) return availabilityScoreA - availabilityScoreB;
      if (a.priority !== b.priority) return a.priority - b.priority;
      return a.feature.localeCompare(b.feature);
    })
    .slice(0, 24)
    .map(({ differs, anyAvailable, allAvailable, priority, ...item }) => item);

  const commonHighlights = featureComparisons
    .filter((item) => !item.differs && item.allAvailable)
    .sort((a, b) => {
      if (a.priority !== b.priority) return a.priority - b.priority;
      return a.feature.localeCompare(b.feature);
    })
    .slice(0, 16)
    .map(({ differs, anyAvailable, allAvailable, priority, ...item }) => item);

  const priceA = compared[0]?.priceValue;
  const priceB = compared[1]?.priceValue;
  const priceDelta = Number.isFinite(priceA) && Number.isFinite(priceB)
    ? Math.abs(priceA - priceB)
    : null;
  const cheaperIndex = Number.isFinite(priceA) && Number.isFinite(priceB)
    ? priceA <= priceB ? 0 : 1
    : null;

  const uniqueAvailableDifferences = compared.map((item) => {
    const uniqueFeatures = [];

    for (const diff of featureComparisons.filter((entry) => entry.differs)) {
      const isAvailableHere = diff.availability[item.label] === true;
      const availableElsewhere = compared.some((other) =>
        other.label !== item.label && diff.availability[other.label] === true,
      );

      if (isAvailableHere && !availableElsewhere) {
        uniqueFeatures.push({
          featureKey: diff.featureKey,
          feature: diff.feature,
          value: diff.values?.[item.label] || "",
        });
      }
    }

    return {
      label: item.label,
      uniqueAvailableFeatureCount: uniqueFeatures.length,
      uniqueAvailableFeatures: uniqueFeatures.slice(0, 12),
    };
  });

  const differenceSummary = {
    featureDifferenceCount: featureDifferences.length,
    commonHighlightCount: commonHighlights.length,
    uniqueAvailableByVehicle: uniqueAvailableDifferences,
    evidenceSource: allFeatureKeys.length ? "vehicle_variant_feature_matrix_v2" : "",
    matrixEvidenceComplete: matrixDocs.every(Boolean),
    matrixFeatureKeyCount: allFeatureKeys.length,
  };

  const missingOrUnavailableEvidence = compared
    .map((item, index) => ({
      label: item.label,
      reason: matrixDocs[index] ? "" : "variant_feature_matrix_missing",
    }))
    .filter((item) => item.reason);

  const decisionHighlights = [
    priceDelta !== null && cheaperIndex !== null
      ? {
          type: "price_difference",
          label: "Price difference",
          text: `${compared[cheaperIndex].label} is cheaper by about ${formatInrShort(priceDelta)} on-road.`,
          cheaperVehicle: compared[cheaperIndex].label,
          priceDelta,
          priceDeltaLabel: formatInrShort(priceDelta),
        }
      : null,
    featureDifferences.length
      ? {
          type: "feature_difference_summary",
          label: "Feature/spec differences",
          text: `I found ${featureDifferences.length} feature/spec differences between the compared variants.`,
          featureDifferenceCount: featureDifferences.length,
        }
      : null,
    ...uniqueAvailableDifferences
      .filter((item) => item.uniqueAvailableFeatureCount > 0)
      .map((item) => ({
        type: "unique_available_differences",
        label: item.label,
        text: `${item.label} has ${item.uniqueAvailableFeatureCount} features/specs marked available where the other compared variant does not.`,
        count: item.uniqueAvailableFeatureCount,
        features: item.uniqueAvailableFeatures,
      })),
  ].filter(Boolean);

  return {
    comparisonSummary: {
      city,
      comparedVehicles: compared.map((item) => ({
        label: item.label,
        model: compareFirstText(item.row.fullModel, item.row.displayName, item.row.model),
        variant: compareFirstText(item.row.variant, item.row.variantName),
        exShowroomPriceLabel: compareFirstText(item.row.exShowroomPriceLabel),
        onRoadPriceLabel: compareFirstText(item.row.onRoadPriceLabel),
        fuelType: compareFirstText(item.row.fuelType, item.row.fuel),
        transmission: compareFirstText(item.row.transmission),
      })),
      priceDelta,
      priceDeltaLabel: priceDelta !== null ? formatInrShort(priceDelta) : "",
      cheaperVehicle: cheaperIndex !== null ? compared[cheaperIndex].label : "",
      featureDifferenceCount: featureDifferences.length,
      commonHighlightCount: commonHighlights.length,
    },
    differenceSummary,
    featureDifferences,
    commonHighlights,
    missingOrUnavailableEvidence,
    decisionHighlights,
    matrixCoverage: compared.map((item, index) => ({
      label: item.label,
      modelKey: matrixDocs[index]?.modelKey || "",
      variant: matrixDocs[index]?.variant || "",
      variantKey: matrixDocs[index]?.variantKey || "",
      featureKeyCount: getComparisonFeatureKeys(matrixDocs[index]).length,
      found: Boolean(matrixDocs[index]),
    })),
  };
};

export const runtimeVehicleCompare = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const isVariantComparison =
    toolPlan.resolution?.comparisonLevel === "variant" ||
    toolPlan.output?.canvasType === "variant_comparison_canvas";

  const text = (...values) => {
    for (const value of values) {
      if (typeof value === "string" && value.trim()) return value.trim();
    }
    return "";
  };

  const asList = (value) => {
    if (Array.isArray(value)) return value.filter(Boolean);
    if (value === undefined || value === null || value === "") return [];
    return [value];
  };

  const normalizeIdentityText = (value = "") =>
    String(value || "")
      .toLowerCase()
      .replace(/&/g, " and ")
      .replace(/[^a-z0-9]+/g, " ")
      .replace(/\s+/g, " ")
      .trim();

  const stripMakePrefix = (model = "", make = "") => {
    const cleanModel = text(model);
    const cleanMake = text(make);
    if (!cleanModel || !cleanMake) return cleanModel;

    const normalizedModel = normalizeIdentityText(cleanModel);
    const normalizedMake = normalizeIdentityText(cleanMake);
    if (!normalizedModel || !normalizedMake) return cleanModel;

    if (normalizedModel === normalizedMake) return cleanModel;

    if (normalizedModel.startsWith(`${normalizedMake} `)) {
      return cleanModel.slice(cleanMake.length).trim() || cleanModel;
    }

    return cleanModel;
  };

  const normalizeVehicleTarget = (target = {}) => {
    if (!target || typeof target !== "object") return null;

    const make = text(target.make, target.brand);
    const brand = text(target.brand, target.make);
    const rawModel = text(
      target.model,
      target.displayName,
      target.name,
      target.fullModel,
    );
    const rawFullModel = text(
      target.fullModel,
      make && rawModel ? `${make} ${stripMakePrefix(rawModel, make)}` : "",
      rawModel,
    );
    const model = stripMakePrefix(rawModel || rawFullModel, make);
    const fullModel = rawFullModel || (make && model ? `${make} ${model}` : model);

    if (!model && !fullModel) return null;

    const variant = text(
      target.variantName,
      target.variant,
      target.fullVariant,
      target.selectedVariant,
    );

    return {
      make,
      brand,
      model: model || stripMakePrefix(fullModel, make),
      fullModel,
      modelKey: text(target.modelKey, target.canonicalKey),
      shortModelKey: text(target.shortModelKey),
      variant,
      variantName: variant,
      fuel: text(target.fuel, target.fuelType),
      transmission: text(target.transmission),
      city: text(target.city, target.citySlug),
    };
  };

  const stripKnownMakePrefix = (model = "", knownMakeKeys = []) => {
    let normalized = normalizeIdentityText(model);
    if (!normalized) return "";

    const orderedMakes = [...new Set(knownMakeKeys.filter(Boolean))]
      .sort((a, b) => b.length - a.length);

    for (const makeKey of orderedMakes) {
      if (normalized === makeKey) continue;
      if (normalized.startsWith(`${makeKey} `)) {
        normalized = normalized.slice(makeKey.length).trim();
        break;
      }
    }

    return normalized;
  };

  const targetIdentityKey = (target = {}, knownMakeKeys = []) => {
    const ownMakeKey = normalizeIdentityText(target.make || target.brand);
    const allKnownMakes = [...knownMakeKeys, ownMakeKey].filter(Boolean);

    const rawModelKey = normalizeIdentityText(
      target.shortModelKey ||
        target.modelKey ||
        target.model ||
        target.fullModel,
    );

    const modelKey = stripKnownMakePrefix(rawModelKey, allKnownMakes);
    const variantKey = normalizeIdentityText(target.variant || target.variantName);
    return `${modelKey}|${variantKey}`;
  };

  const targetQuality = (target = {}) =>
    [
      target.make,
      target.brand,
      target.fullModel,
      target.modelKey,
      target.shortModelKey,
      target.variant,
      target.fuel,
      target.transmission,
      target.city,
    ].filter(Boolean).length;

  const mergeVehicleTarget = (current = {}, incoming = {}) => {
    const preferred = targetQuality(incoming) >= targetQuality(current) ? incoming : current;
    const fallback = preferred === incoming ? current : incoming;

    return {
      ...fallback,
      ...preferred,
      make: text(preferred.make, fallback.make),
      brand: text(preferred.brand, fallback.brand, preferred.make, fallback.make),
      model: stripMakePrefix(text(preferred.model, fallback.model, preferred.fullModel, fallback.fullModel), text(preferred.make, fallback.make)),
      fullModel: text(
        preferred.fullModel,
        fallback.fullModel,
        [text(preferred.make, fallback.make), stripMakePrefix(text(preferred.model, fallback.model), text(preferred.make, fallback.make))].filter(Boolean).join(" "),
      ),
      variant: text(preferred.variant, fallback.variant),
      variantName: text(preferred.variantName, fallback.variantName, preferred.variant, fallback.variant),
      fuel: text(preferred.fuel, fallback.fuel),
      transmission: text(preferred.transmission, fallback.transmission),
      city: text(preferred.city, fallback.city),
    };
  };

  const dedupeVehicleTargets = (targets = []) => {
    const knownMakeKeys = [...new Set(
      targets
        .flatMap((target = {}) => [target.make, target.brand])
        .map(normalizeIdentityText)
        .filter(Boolean),
    )];

    const byKey = new Map();

    for (const target of targets) {
      const key = targetIdentityKey(target, knownMakeKeys);
      if (!key || key === "|") continue;

      byKey.set(key, byKey.has(key) ? mergeVehicleTarget(byKey.get(key), target) : target);
    }

    return [...byKey.values()];
  };

  const explicitTargets = [
    ...asList(toolPlan.entities?.comparisonVehicles),
    ...asList(toolPlan.resolution?.selectedComparisonVehicles),
    ...asList(toolPlan.contextPatch?.activeComparison?.vehicles),
    ...asList(toolPlan.contextPatch?.selectedComparisonSet?.vehicles),

    // Critical for follow-up questions like "which one is better?"
    // The ACI context manager stores comparison targets in runtime context,
    // not necessarily inside the single toolPlan.
    ...asList(context?.activeComparison?.vehicles),
    ...asList(context?.selectedComparisonSet?.vehicles),
    ...asList(context?.contextState?.activeComparison?.vehicles),
    ...asList(context?.aciContextState?.activeComparison?.vehicles),
  ]
    .map(normalizeVehicleTarget)
    .filter(Boolean);

  const dedupeModelLevelTargets = (targets = []) => {
    const byKey = new Map();

    for (const target of asList(targets)) {
      const normalized = normalizeVehicleTarget(target);
      if (!normalized) continue;

      const make = normalizeIdentityText(normalized.make || normalized.brand);
      let model = normalizeIdentityText(normalized.model || normalized.fullModel);

      if (make && model.startsWith(`${make} `)) {
        model = model.slice(make.length + 1).trim();
      }

      const key = [make, model].filter(Boolean).join("|");
      if (!key) continue;

      byKey.set(key, byKey.has(key) ? mergeVehicleTarget(byKey.get(key), normalized) : normalized);
    }

    return [...byKey.values()];
  };

  const uniqueTargetsRaw = dedupeVehicleTargets(explicitTargets);
  const uniqueTargets = isVariantComparison
    ? uniqueTargetsRaw
    : dedupeModelLevelTargets(uniqueTargetsRaw);

  const fallbackModels = getModels(toolPlan, context);
  const fallbackVariants = [
    ...asList(toolPlan.entities?.variants),
    ...asList(toolPlan.filters?.variants),
    ...asList(toolPlan.resolution?.selectedVariants).map((item) =>
      typeof item === "string" ? item : item?.variant,
    ),
  ].filter(Boolean);

  const fallbackTargets = dedupeVehicleTargets([
    ...explicitTargets,
    ...fallbackModels.map((model, index) =>
      normalizeVehicleTarget({
        model,
        fullModel: model,
        variant:
          fallbackVariants[index] ||
          (index === 0
            ? text(
                toolPlan.entities?.variant,
                toolPlan.entities?.primaryVariant,
                toolPlan.filters?.variant,
              )
            : ""),
        variantName:
          fallbackVariants[index] ||
          (index === 0
            ? text(
                toolPlan.entities?.variant,
                toolPlan.entities?.primaryVariant,
                toolPlan.filters?.variant,
              )
            : ""),
        city: text(toolPlan.filters?.city, context?.anchorCity, context?.selectedVehicle?.city),
      }),
    ).filter(Boolean),
  ]);

  const targets = uniqueTargets.length >= 2
    ? uniqueTargets
    : fallbackTargets;

  if (targets.length <= 1) {
    const data = await runtimeVehiclePricelist({ toolPlan, context });
    return {
      ...data,
      rows: data.rows.slice(0, 4),
      comparisonLevel: toolPlan.resolution?.comparisonLevel || "model",
    };
  }

  const comparisonCitySlug = normalizeComparisonCitySlug(
    text(toolPlan.filters?.city, context?.anchorCity, context?.selectedVehicle?.city, "new-delhi"),
  );

  const directComparisonRows = await resolveDirectComparisonRows({
    targets,
    citySlug: comparisonCitySlug,
    isVariantComparison,
  });

  let rows = directComparisonRows?.rows || [];
  let comparisonMatrixDocs = directComparisonRows?.matrixDocs || null;
  let comparisonCatalogByKey = directComparisonRows?.catalogByKey || null;

  if (!rows.length) {
    rows = await Promise.all(
      targets.map(async (target) => {
        const model = text(target.fullModel, target.model);
        const variant = text(target.variantName, target.variant);

        const modelTool = {
          ...toolPlan,
          entities: {
            ...(toolPlan.entities || {}),
            model,
            primaryModel: model,
          },
          filters: {
            ...(toolPlan.filters || {}),
            model,
          },
        };

        if (isVariantComparison && variant) {
          modelTool.entities.variant = variant;
          modelTool.entities.primaryVariant = variant;
          modelTool.filters.variant = variant;
        } else if (!isVariantComparison) {
          delete modelTool.entities.variant;
          delete modelTool.entities.primaryVariant;
          delete modelTool.filters.variant;
        }

        const comparisonContext = {
          ...(context || {}),
          anchorModel: model,
          anchorVariant: isVariantComparison ? variant : "",
          variant: isVariantComparison ? variant : "",
          selectedVehicle: {
            ...((context || {}).selectedVehicle || {}),
            model,
            variant: isVariantComparison ? variant : "",
            selectedVariant: isVariantComparison ? variant : "",
            city: target.city || context?.anchorCity || context?.selectedVehicle?.city || "new-delhi",
          },
        };

        const data = await runtimeVehiclePricelist({
          toolPlan: modelTool,
          context: comparisonContext,
        });

        return data.rows[0] || {
          model,
          variant,
          unavailable: true,
          variantResolution: data.variantResolution || null,
          candidateVariants: data.candidateVariants || [],
        };
      }),
    );
  }

  const unresolvedComparisonEvidence = rows
    .map((row, index) =>
      row.unavailable
        ? {
            label: getComparisonVehicleLabel(row, targets[index] || {}) || `vehicle ${index + 1}`,
            reason: "comparison_variant_unresolved",
          }
        : null,
    )
    .filter(Boolean);

  const hasComparableRows = rows.filter((row) => !row.unavailable).length >= 2;
  const comparisonEnrichment = hasComparableRows
    ? await buildVehicleComparisonEnrichment({
        rows,
        targets,
        city: comparisonCitySlug,
        matrixDocs: comparisonMatrixDocs,
        catalogByKey: comparisonCatalogByKey,
      })
    : {
        comparisonSummary: {},
        featureDifferences: [],
        commonHighlights: [],
        missingOrUnavailableEvidence: unresolvedComparisonEvidence,
        decisionHighlights: [],
        matrixCoverage: [],
      };

  return {
    rows,
    count: rows.length,
    matched: rows.filter((row) => !row.unavailable).length,
    comparisonSummary: comparisonEnrichment.comparisonSummary,
    differenceSummary: comparisonEnrichment.differenceSummary,
    featureDifferences: comparisonEnrichment.featureDifferences,
    commonHighlights: comparisonEnrichment.commonHighlights,
    missingOrUnavailableEvidence: comparisonEnrichment.missingOrUnavailableEvidence,
    decisionHighlights: comparisonEnrichment.decisionHighlights,
    matrixCoverage: comparisonEnrichment.matrixCoverage,
    selectedComparisonSet: {
      vehicles: targets,
      models: targets.map((target) => text(target.fullModel, target.model)).filter(Boolean),
      variantSelectionMode:
        toolPlan.resolution?.variantSelectionMode ||
        (isVariantComparison ? "exact" : "representative_default"),
    },
    contextPatch: {
      activeComparison: {
        type: "vehicle_compare",
        vehicles: targets,
        city: text(toolPlan.filters?.city, context?.anchorCity, "new-delhi"),
      },
      selectedComparisonSet: {
        vehicles: targets,
      },
      anchorCity: text(toolPlan.filters?.city, context?.anchorCity, "new-delhi"),
    },
    modulesChecked: ["vehicle_compare", "vehicle_pricelist"],
    comparisonResolutionMode: directComparisonRows?.resolutionMode || "pricelist_runtime_fallback",
    dataSource: "executor_composed",
  };
};

export const runtimeVehicleRecommend = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const filters = toolPlan.filters || {};
  const mustHaveFeatures = asArray(filters.mustHaveFeatures);
  const hasBroadBudgetDiscoveryFilters =
    !getModel(toolPlan, context) &&
    !getVariant(toolPlan, context) &&
    Boolean(
      mustHaveFeatures.length ||
        filters.budgetMax ||
        filters.bodyType ||
        filters.transmission ||
        filters.fuelType ||
        filters.make ||
        filters.brand,
    );

  if (hasBroadBudgetDiscoveryFilters) {
    const budgetDiscovery = await runtimeBudgetVehicleDiscovery({
      toolPlan,
      context,
    });

    if (
      budgetDiscovery.dataSource === "aci_vehicle_read_models" ||
      budgetDiscovery.budgetDiscovery?.enabled ||
      budgetDiscovery.noResultRecovery
    ) {
      return budgetDiscovery;
    }
  }

  const data = await runtimeVehiclePricelist({ toolPlan, context });

  const grouped = uniqueBy(
    data.rows,
    (row) => row.model || row.variant || row.id,
  ).slice(0, 12);

  return {
    ...data,
    rows: grouped,
    cars: grouped,
    count: grouped.length,
    matched: grouped.length,
    ranking: toolPlan.ranking || "value",
  };
};

const slugForReadModel = (value = "") =>
  searchKey(value)
    .replace(/\s+/g, "-")
    .replace(/^-|-$/g, "");

const normalizeBodyTypeForMatch = (value = "") =>
  searchKey(value)
    .replace(/-/g, " ")
    .replace(/\s+/g, " ")
    .trim();


const normalizeBudgetBodyTypeGroup = (value = "") => {
  const key = normalizeBodyTypeForMatch(value);
  if (!key) return "unknown";

  if (
    /\bsuv\b/.test(key) ||
    key.includes("sport util") ||
    key.includes("sports util") ||
    key.includes("utility vehicle") ||
    key.includes("compact suv") ||
    key.includes("crossover")
  ) {
    return "suv";
  }

  if (key.includes("sedan")) return "sedan";
  if (key.includes("hatch")) return "hatchback";

  if (
    /\bmpv\b/.test(key) ||
    /\bmuv\b/.test(key) ||
    key.includes("multi utility") ||
    key.includes("minivan") ||
    key.includes("mini van") ||
    key === "van" ||
    key.endsWith(" van")
  ) {
    return "mpv";
  }

  if (key.includes("pickup") || key.includes("pick up")) return "pickup";
  if (key.includes("coupe")) return "coupe";
  if (key.includes("convertible")) return "convertible";

  return key;
};

const getBudgetBodyTypeGroup = (value = "", fallback = "") =>
  normalizeBudgetBodyTypeGroup(firstMeaningful(value, fallback));

const withBudgetBodyTypeGroup = (group = {}) => ({
  ...group,
  bodyTypeGroup:
    group.bodyTypeGroup ||
    getBudgetBodyTypeGroup(group.bodyType, group.bodyTypeKey || group.segment),
});

const bodyTypeMatchesBudgetFilter = (row = {}, requestedBodyType = "") => {
  const requested = normalizeBodyTypeForMatch(requestedBodyType);
  if (!requested) return true;

  const haystack = normalizeBodyTypeForMatch(
    [
      row.bodyType,
      row.bodyTypeKey,
      row.segment,
      row.category,
    ].filter(Boolean).join(" "),
  );

  if (!haystack) return false;
  if (haystack.includes(requested)) return true;

  if (requested === "suv") {
    return /\bsuv\b|\bsport utilit/.test(haystack);
  }

  if (requested === "sedan") {
    return /\bsedan/.test(haystack);
  }

  if (requested === "hatchback") {
    return /\bhatch/.test(haystack);
  }

  if (requested === "mpv" || requested === "muv") {
    return /\bmpv\b|\bmuv\b|\bmini\s*van|\bminivan/.test(haystack);
  }

  return false;
};

const transmissionMatchesBudgetFilter = (row = {}, requestedTransmission = "") => {
  const requested = searchKey(requestedTransmission);
  if (!requested) return true;

  const haystack = searchKey(
    [
      row.transmission,
      row.transmissionKey,
      row.gearbox,
      row.gearboxKey,
    ].filter(Boolean).join(" "),
  );

  if (!haystack) return false;
  if (haystack.includes(requested)) return true;

  if (requested === "automatic" || requested === "auto") {
    return /\bautomatic\b|\bauto\b|\bamt\b|\bcvt\b|\bdct\b|\bivt\b|\bat\b|\bdsg\b/.test(haystack);
  }

  if (requested === "manual") {
    return /\bmanual\b|\bmt\b/.test(haystack);
  }

  return false;
};

const fuelTypeMatchesBudgetFilter = (row = {}, requestedFuelType = "") => {
  const requested = searchKey(requestedFuelType);
  if (!requested) return true;

  const haystack = searchKey([row.fuel, row.fuelType, row.fuelKey].filter(Boolean).join(" "));
  return Boolean(haystack && haystack.includes(requested));
};


const normalizeBudgetDiscoveryRow = (row = {}) => {
  const make = displayName(firstMeaningful(row.make, row.brand));
  const model = displayName(firstMeaningful(row.model, row.fullModel));
  const fullModel = displayName(
    firstMeaningful(
      row.fullModel,
      [make, model].filter(Boolean).join(" "),
      model,
    ),
  );

  const bodyType = displayName(
    firstMeaningful(row.bodyType, row.body_type, row.segment, row.category),
  );
  const bodyTypeKey = row.bodyTypeKey || slugForReadModel(bodyType);
  const segment = displayName(firstMeaningful(row.segment, bodyType));

  const fuelType = displayName(firstMeaningful(row.fuelType, row.fuel, row.fuelKey));
  const transmission = displayName(
    firstMeaningful(row.transmission, row.gearbox, row.transmissionKey, row.gearboxKey),
  );

  const exShowroomPrice = firstNumber(
    row.exShowroomPrice,
    row.ex_showroom,
    row.exShowroomPriceValue,
    row.price,
  );

  const onRoadPrice = firstNumber(
    row.onRoadPrice,
    row.total_on_road_with_accessories,
    row.on_road_price_cardekho,
  );

  return {
    ...row,
    make,
    brand: make,
    makeKey: row.makeKey || slugForReadModel(make),
    model,
    modelKey: row.modelKey || slugForReadModel(model || fullModel),
    fullModel,
    displayName: fullModel || [make, model].filter(Boolean).join(" "),
    variant: displayName(firstMeaningful(row.variant, row.variantName)),
    variantName: displayName(firstMeaningful(row.variantName, row.variant)),
    variantKey: row.variantKey || slugForReadModel(firstMeaningful(row.variantKey, row.variant, row.variantName)),
    city: displayName(row.city),
    citySlug: row.citySlug || slugForReadModel(row.city || DEFAULT_CITY),
    fuelType,
    fuel: fuelType,
    fuelKey: row.fuelKey || slugForReadModel(fuelType),
    transmission,
    transmissionKey: row.transmissionKey || slugForReadModel(transmission),
    gearbox: displayName(firstMeaningful(row.gearbox, transmission)),
    gearboxKey: row.gearboxKey || slugForReadModel(firstMeaningful(row.gearbox, transmission)),
    bodyType,
    bodyTypeKey,
    bodyTypeGroup: getBudgetBodyTypeGroup(bodyType, bodyTypeKey || segment),
    segment,
    exShowroomPrice,
    exShowroomPriceLabel: row.exShowroomPriceLabel || (exShowroomPrice ? formatMoney(exShowroomPrice) : ""),
    onRoadPrice,
    onRoadPriceLabel: row.onRoadPriceLabel || (onRoadPrice ? formatMoney(onRoadPrice) : ""),
  };
};


const BUDGET_DISCOVERY_CACHE_TTL_MS = Number(
  process.env.ACI_BUDGET_DISCOVERY_CACHE_TTL_MS || 5 * 60 * 1000,
);
const BUDGET_DISCOVERY_RESULT_CACHE_MAX = Number(
  process.env.ACI_BUDGET_DISCOVERY_RESULT_CACHE_MAX || 80,
);

const budgetDiscoveryRowCacheByCity = new Map();
const budgetDiscoveryRowCacheInFlightByCity = new Map();

const budgetDiscoveryResultCache = new Map();
let featureDiscoveryCatalogCache = {
  expiresAt: 0,
  rows: null,
};

const cloneBudgetDiscoveryPayload = (value) => {
  if (!value) return value;

  if (typeof structuredClone === "function") {
    return structuredClone(value);
  }

  return JSON.parse(JSON.stringify(value));
};

const getBudgetDiscoveryNow = () => Date.now();

const getBudgetDiscoveryResultCacheKey = ({
  budgetMin = 0,
  budgetMax = 0,
  citySlug = "",
  make = "",
  filters = {},
  ranking = "",
} = {}) =>
  JSON.stringify({
    budgetMin: Number(budgetMin || 0) || 0,
    budgetMax: Number(budgetMax || 0) || 0,
    citySlug: slugForReadModel(citySlug || DEFAULT_CITY),
    makeKey: slugForReadModel(make),
    bodyType: slugForReadModel(filters.bodyType || ""),
    transmission: slugForReadModel(filters.transmission || ""),
    fuelType: slugForReadModel(filters.fuelType || ""),
    mustHaveFeatures: asArray(filters.mustHaveFeatures).map((feature) =>
      slugForReadModel(feature),
    ),
    ranking: slugForReadModel(ranking || "value"),
  });

const getCachedBudgetDiscoveryResult = (cacheKey = "") => {
  if (!cacheKey) return null;

  const cached = budgetDiscoveryResultCache.get(cacheKey);
  if (!cached) return null;

  if (cached.expiresAt <= getBudgetDiscoveryNow()) {
    budgetDiscoveryResultCache.delete(cacheKey);
    return null;
  }

  return cloneBudgetDiscoveryPayload({
    ...cached.value,
    cache: {
      ...(cached.value.cache || {}),
      hit: true,
      key: cacheKey,
      type: "budget_discovery_result",
    },
    budgetDiscovery: {
      ...(cached.value.budgetDiscovery || {}),
      cacheHit: true,
      cacheType: "budget_discovery_result",
    },
  });
};

const setCachedBudgetDiscoveryResult = (cacheKey = "", value = {}) => {
  if (!cacheKey || !BUDGET_DISCOVERY_CACHE_TTL_MS) return value;

  if (budgetDiscoveryResultCache.size >= BUDGET_DISCOVERY_RESULT_CACHE_MAX) {
    const oldestKey = budgetDiscoveryResultCache.keys().next().value;
    if (oldestKey) budgetDiscoveryResultCache.delete(oldestKey);
  }

  budgetDiscoveryResultCache.set(cacheKey, {
    expiresAt: getBudgetDiscoveryNow() + BUDGET_DISCOVERY_CACHE_TTL_MS,
    value: cloneBudgetDiscoveryPayload(value),
  });

  return value;
};

const getCachedBudgetDiscoveryRows = async ({
  collection,
  projection,
  citySlug = DEFAULT_CITY,
  force = false,
} = {}) => {
  const now = getBudgetDiscoveryNow();
  const normalizedCitySlug = slugForReadModel(citySlug || DEFAULT_CITY) || "new-delhi";
  const cached = budgetDiscoveryRowCacheByCity.get(normalizedCitySlug);

  if (
    !force &&
    cached?.rows &&
    Array.isArray(cached.rows) &&
    cached.expiresAt > now
  ) {
    return cached.rows;
  }

  if (!force && budgetDiscoveryRowCacheInFlightByCity.has(normalizedCitySlug)) {
    return budgetDiscoveryRowCacheInFlightByCity.get(normalizedCitySlug);
  }

  const loadPromise = (async () => {
    const queryStartedAt = Date.now();
    const cursor = collection
      .find(
        {
          citySlug: normalizedCitySlug,
          exShowroomPrice: { $gt: 0 },
        },
        { projection },
      )
      .sort({ citySlug: 1, exShowroomPrice: 1 })
      .limit(4000)
      .batchSize(1000);

    try {
      cursor.hint({ citySlug: 1, exShowroomPrice: 1 });
    } catch {
      // Some local/dev DBs may not have the hinted index yet.
    }

    const rawRows = await cursor.toArray();
    const queryDurationMs = Date.now() - queryStartedAt;

    const normalizeStartedAt = Date.now();
    const rows = rawRows
      .map(normalizeBudgetDiscoveryRow)
      .filter((row) => Number(row.exShowroomPrice || 0) > 0)
      .sort((a, b) => {
        const priceDelta = Number(a.exShowroomPrice || 0) - Number(b.exShowroomPrice || 0);
        if (priceDelta) return priceDelta;
        return String(a.displayName || a.fullModel || a.model || "").localeCompare(
          String(b.displayName || b.fullModel || b.model || ""),
        );
      });
    const normalizeDurationMs = Date.now() - normalizeStartedAt;

    budgetDiscoveryRowCacheByCity.set(normalizedCitySlug, {
      expiresAt: getBudgetDiscoveryNow() + BUDGET_DISCOVERY_CACHE_TTL_MS,
      rows,
    });

    if (process.env.ACI_BUDGET_DISCOVERY_PROFILE === "true") {
      console.log("[ACI BudgetDiscovery] row cache profile", JSON.stringify({
        citySlug: normalizedCitySlug,
        rawRows: rawRows.length,
        rows: rows.length,
        queryDurationMs,
        normalizeDurationMs,
        totalDurationMs: queryDurationMs + normalizeDurationMs,
      }));
    }

    return rows;
  })().finally(() => {
    budgetDiscoveryRowCacheInFlightByCity.delete(normalizedCitySlug);
  });

  budgetDiscoveryRowCacheInFlightByCity.set(normalizedCitySlug, loadPromise);

  return loadPromise;
};

const getBudgetDiscoveryProjection = () => ({
  make: 1,
  makeKey: 1,
  model: 1,
  modelKey: 1,
  fullModel: 1,
  variant: 1,
  variantKey: 1,
  city: 1,
  citySlug: 1,
  fuel: 1,
  fuelType: 1,
  fuelKey: 1,
  transmission: 1,
  transmissionKey: 1,
  gearbox: 1,
  gearboxKey: 1,
  bodyType: 1,
  bodyTypeKey: 1,
  segment: 1,
  exShowroomPrice: 1,
  exShowroomPriceLabel: 1,
  onRoadPrice: 1,
  onRoadPriceLabel: 1,
});

const getScopedBudgetDiscoveryRows = async ({
  collection,
  projection,
  citySlug = DEFAULT_CITY,
  budgetMin = 0,
  budgetMax = 0,
  make = "",
  filters = {},
} = {}) => {
  const normalizedCitySlug = slugForReadModel(citySlug || DEFAULT_CITY) || "new-delhi";
  const cached = budgetDiscoveryRowCacheByCity.get(normalizedCitySlug);

  if (
    cached?.rows &&
    Array.isArray(cached.rows) &&
    cached.expiresAt > getBudgetDiscoveryNow()
  ) {
    return {
      rows: cached.rows,
      source: "city_row_cache",
      cacheHit: true,
      queryDurationMs: 0,
      normalizeDurationMs: 0,
    };
  }

  const priceQuery = {
    $gt: budgetMin > 0 ? budgetMin : 0,
  };
  if (budgetMax > 0) priceQuery.$lte = budgetMax;

  const query = {
    citySlug: normalizedCitySlug,
    exShowroomPrice: priceQuery,
  };

  const makeKey = slugForReadModel(make);
  if (makeKey) query.makeKey = makeKey;

  const bodyTypeKey = slugForReadModel(filters.bodyType || "");
  if (bodyTypeKey === "suv" || bodyTypeKey === "suvs") {
    query.$or = [
      { bodyTypeKey: { $in: ["suv", "suvs", "sport-utilities"] } },
      { bodyType: /suv|sport util/i },
    ];
  }

  const fuelKey = slugForReadModel(filters.fuelType || "");
  if (fuelKey) query.fuelKey = fuelKey;

  const transmissionKey = slugForReadModel(filters.transmission || "");
  if (transmissionKey === "automatic") {
    query.transmissionKey = { $in: ["automatic", "auto", "amt", "cvt", "dct", "ivt", "at", "dsg"] };
  } else if (transmissionKey === "manual") {
    query.transmissionKey = "manual";
  }

  const startedAt = Date.now();
  const cursor = collection
    .find(query, { projection })
    .sort({ citySlug: 1, exShowroomPrice: 1 })
    .limit(2500)
    .batchSize(750);

  try {
    cursor.hint({ citySlug: 1, exShowroomPrice: 1 });
  } catch {
    // Keep local/dev DBs without this read-model index working.
  }

  const rawRows = await cursor.toArray();
  const queryDurationMs = Date.now() - startedAt;

  const normalizeStartedAt = Date.now();
  const rows = rawRows
    .map(normalizeBudgetDiscoveryRow)
    .filter((row) => Number(row.exShowroomPrice || 0) > 0)
    .sort((a, b) => {
      const priceDelta = Number(a.exShowroomPrice || 0) - Number(b.exShowroomPrice || 0);
      if (priceDelta) return priceDelta;
      return String(a.displayName || a.fullModel || a.model || "").localeCompare(
        String(b.displayName || b.fullModel || b.model || ""),
      );
    });
  const normalizeDurationMs = Date.now() - normalizeStartedAt;

  if (process.env.ACI_BUDGET_DISCOVERY_PROFILE === "true") {
    console.log("[ACI BudgetDiscovery] scoped query profile", JSON.stringify({
      citySlug: normalizedCitySlug,
      query,
      rawRows: rawRows.length,
      rows: rows.length,
      queryDurationMs,
      normalizeDurationMs,
      totalDurationMs: queryDurationMs + normalizeDurationMs,
    }));
  }

  return {
    rows,
    source: "scoped_db_query",
    cacheHit: false,
    queryDurationMs,
    normalizeDurationMs,
  };
};

const getCachedFeatureDiscoveryCatalog = async (db) => {
  const now = getBudgetDiscoveryNow();

  if (
    featureDiscoveryCatalogCache.rows &&
    Array.isArray(featureDiscoveryCatalogCache.rows) &&
    featureDiscoveryCatalogCache.expiresAt > now
  ) {
    return featureDiscoveryCatalogCache.rows;
  }

  const rows = await db
    .collection("vehicle_feature_catalog_v2")
    .find({})
    .project({
      canonicalKey: 1,
      key: 1,
      featureKey: 1,
      displayName: 1,
      normalizedName: 1,
      aliases: 1,
      groupKey: 1,
      category: 1,
    })
    .toArray();

  featureDiscoveryCatalogCache = {
    expiresAt: now + BUDGET_DISCOVERY_CACHE_TTL_MS,
    rows,
  };

  return rows;
};

const buildFeatureDiscoveryAliasKeys = (row = {}) => {
  const aliases = [
    row.canonicalKey,
    row.key,
    row.featureKey,
    row.displayName,
    row.normalizedName,
    ...(Array.isArray(row.aliases) ? row.aliases : []),
  ];

  const baseKeys = aliases
    .map((alias) => searchKey(String(alias || "").replace(/[_-]+/g, " ")))
    .filter(Boolean);

  if (baseKeys.includes("turbo charger")) {
    baseKeys.push("turbo", "turbocharged", "turbo charged");
  }

  return unique(baseKeys);
};

const findFeatureCatalogRowsByRequestedKeys = async ({
  db,
  requestedFeatures = [],
} = {}) => {
  const requestedKeys = unique(
    asArray(requestedFeatures)
      .flatMap((feature) => {
        const spaced = searchKey(String(feature || "").replace(/[_-]+/g, " "));
        const underscored = spaced.replace(/\s+/g, "_");
        const compacted = spaced.replace(/\s+/g, "");
        return [spaced, underscored, compacted, String(feature || "").trim()].filter(Boolean);
      }),
  );

  if (!db || !requestedKeys.length) return [];

  const rows = await db
    .collection("vehicle_feature_catalog_v2")
    .find({
      $or: [
        { aliases: { $in: requestedKeys } },
        { canonicalKey: { $in: requestedKeys } },
        { key: { $in: requestedKeys } },
        { featureKey: { $in: requestedKeys } },
        { normalizedName: { $in: requestedKeys } },
      ],
    })
    .project({
      canonicalKey: 1,
      key: 1,
      featureKey: 1,
      displayName: 1,
      normalizedName: 1,
      aliases: 1,
      groupKey: 1,
      category: 1,
    })
    .limit(20)
    .toArray();

  return rows;
};

const resolveBudgetDiscoveryFeatureFilters = async ({
  db,
  mustHaveFeatures = [],
} = {}) => {
  const requestedFeatures = asArray(mustHaveFeatures)
    .map((feature) => cleanText(feature))
    .filter(Boolean);

  if (!requestedFeatures.length) {
    return {
      requestedFeatures: [],
      featureKeys: [],
      resolvedFeatures: [],
      unresolvedFeatures: [],
    };
  }

  const targetedCatalog = await findFeatureCatalogRowsByRequestedKeys({
    db,
    requestedFeatures,
  });
  const catalog = targetedCatalog.length
    ? targetedCatalog
    : await getCachedFeatureDiscoveryCatalog(db);
  const catalogEntries = catalog.map((row) => ({
    row,
    aliasKeys: buildFeatureDiscoveryAliasKeys(row),
  }));

  const resolvedFeatures = [];
  const unresolvedFeatures = [];

  for (const requestedFeature of requestedFeatures) {
    const requestedKey = searchKey(String(requestedFeature).replace(/[_-]+/g, " "));
    const match = catalogEntries.find((entry) =>
      entry.aliasKeys.includes(requestedKey),
    );

    if (!match) {
      unresolvedFeatures.push(requestedFeature);
      continue;
    }

    const featureKey = firstMeaningful(
      match.row.canonicalKey,
      match.row.key,
      match.row.featureKey,
    );

    if (!featureKey) {
      unresolvedFeatures.push(requestedFeature);
      continue;
    }

    resolvedFeatures.push({
      requestedFeature,
      featureKey,
      displayName: firstMeaningful(match.row.displayName, featureKey).replace(/_/g, " "),
      groupKey: match.row.groupKey || "",
      category: match.row.category || "",
    });
  }

  return {
    requestedFeatures,
    featureKeys: unique(resolvedFeatures.map((feature) => feature.featureKey)),
    resolvedFeatures: uniqueBy(resolvedFeatures, (feature) => feature.featureKey),
    unresolvedFeatures,
  };
};

const buildBudgetDiscoveryNoResultRecovery = ({
  reason = "no_confident_feature_budget_matches",
  filters = {},
  budgetMax = 0,
  budgetMin = 0,
  featureResolution = {},
} = {}) => {
  const featureNames = asArray(featureResolution.resolvedFeatures)
    .map((feature) => feature.displayName || feature.featureKey)
    .filter(Boolean);
  const unresolved = asArray(featureResolution.unresolvedFeatures);

  return {
    reason,
    appliedFilters: compactObject({
      features: featureNames.length ? featureNames : featureResolution.requestedFeatures,
      featureKeys: featureResolution.featureKeys,
      unresolvedFeatures: unresolved,
      budgetMin,
      budgetMax,
      bodyType: filters.bodyType,
      transmission: filters.transmission,
      fuelType: filters.fuelType,
      make: firstMeaningful(filters.make, filters.brand),
    }),
    suggestedRelaxations: [
      budgetMax ? "Try a higher budget." : "",
      featureNames.length ? `Remove ${featureNames.join(", ")} as a must-have feature.` : "",
      filters.bodyType ? "Try all body types instead of only this body type." : "",
      filters.transmission ? "Try both manual and automatic options." : "",
    ].filter(Boolean),
  };
};

const fetchFeatureMatchedVariantKeys = async ({
  db,
  featureKeys = [],
} = {}) => {
  const keys = asArray(featureKeys).filter(Boolean);
  if (!db || !keys.length) return new Map();

  const query = Object.fromEntries(
    keys.map((featureKey) => [`featuresByKey.${featureKey}.available`, true]),
  );
  const projection = {
    modelKey: 1,
    variantKey: 1,
    variant: 1,
  };

  const docs = await db
    .collection("vehicle_variant_feature_matrix_v2")
    .find(query, { projection })
    .limit(12000)
    .toArray();

  const matches = new Map();

  docs.forEach((doc = {}) => {
    [
      `${slugForReadModel(doc.modelKey)}|${slugForReadModel(doc.variantKey)}`,
      `${slugForReadModel(doc.modelKey)}|${slugForReadModel(doc.variant)}`,
    ]
      .filter((key) => key.replace(/\|/g, ""))
      .forEach((key) => {
        matches.set(key, {
          modelKey: doc.modelKey || "",
          variantKey: doc.variantKey || "",
          variant: doc.variant || "",
          featureKeys: keys,
        });
      });
  });

  return matches;
};

const getFeatureVariantMatch = (row = {}, featureVariantKeys = new Map()) => {
  if (!featureVariantKeys?.size) return false;

  const keys = [
    `${slugForReadModel(row.modelKey || row.model)}|${slugForReadModel(row.variantKey)}`,
    `${slugForReadModel(row.modelKey || row.model)}|${slugForReadModel(row.variant)}`,
    `${slugForReadModel(row.modelKey || row.model)}|${slugForReadModel(row.variantName)}`,
  ];

  for (const key of keys) {
    const match = featureVariantKeys.get(key);
    if (match) return match;
  }

  return null;
};

const rowMatchesFeatureVariantKeys = (row = {}, featureVariantKeys = new Map()) =>
  Boolean(getFeatureVariantMatch(row, featureVariantKeys));

const attachFeatureMatchMetadata = ({
  row = {},
  featureVariantKeys = new Map(),
  featureResolution = {},
} = {}) => {
  const match = getFeatureVariantMatch(row, featureVariantKeys);
  if (!match) return row;

  const feature = asArray(featureResolution.resolvedFeatures)[0] || {};
  const featureKey = feature.featureKey || asArray(featureResolution.featureKeys)[0] || "";
  const featureName = feature.displayName || featureKey.replace(/_/g, " ");

  return {
    ...row,
    featureKey,
    featureName,
    matchedFeature: featureName,
    foundMatrixRows: 1,
    featureAvailability: {
      available: true,
      source: "vehicle_variant_feature_matrix_v2",
    },
    featureMatchSource: "vehicle_variant_feature_matrix_v2",
  };
};

const getBudgetVariantUniqueKey = (row = {}) =>
  [
    row.makeKey || row.make || row.brand,
    row.modelKey || row.fullModel || row.model,
    row.variantKey || row.variantName || row.variant,
    row.fuelKey || row.fuelType || row.fuel,
    row.transmissionKey || row.transmission,
  ]
    .map((part) => slugForReadModel(part))
    .filter(Boolean)
    .join("|");

const pickBudgetCityPreferenceScore = (row = {}, preferredCitySlug = DEFAULT_CITY) => {
  const citySlug = slugForReadModel(row.citySlug || row.city);
  const preferred = slugForReadModel(preferredCitySlug || DEFAULT_CITY);

  if (citySlug && preferred && citySlug === preferred) return 0;
  if (citySlug === DEFAULT_CITY) return 1;
  return 2;
};

const dedupeBudgetRowsByVariant = ({
  rows = [],
  preferredCitySlug = DEFAULT_CITY,
} = {}) => {
  const byVariant = new Map();

  for (const row of rows) {
    const key = getBudgetVariantUniqueKey(row);
    if (!key) continue;

    const existing = byVariant.get(key);
    if (!existing) {
      byVariant.set(key, row);
      continue;
    }

    const currentScore = pickBudgetCityPreferenceScore(row, preferredCitySlug);
    const existingScore = pickBudgetCityPreferenceScore(existing, preferredCitySlug);

    if (
      currentScore < existingScore ||
      (currentScore === existingScore &&
        Number(row.exShowroomPrice || 0) < Number(existing.exShowroomPrice || Number.MAX_SAFE_INTEGER))
    ) {
      byVariant.set(key, row);
    }
  }

  return [...byVariant.values()];
};

const getBudgetBodyBucket = (group = {}) => {
  const bodyText = normalizeBodyTypeForMatch(
    [
      group.bodyType,
      group.bodyTypeKey,
      group.segment,
    ].filter(Boolean).join(" "),
  );
  const fuelText = searchKey(asArray(group.fuelTypes).join(" "));

  if (/\bsuv\b|\bsport utilit/.test(bodyText)) return "suv";
  if (/\bhatch/.test(bodyText)) return "hatchback";
  if (/\bsedan/.test(bodyText)) return "sedan";
  if (/\bmpv\b|\bmuv\b|\bmini\s*van|\bminivan/.test(bodyText)) return "mpv";
  if (/\belectric\b|\bev\b/.test(fuelText)) return "ev";
  return bodyText || "other";
};

const buildDiverseBudgetPreviewGroups = ({
  groups = [],
  filters = {},
  limit = BUDGET_DISCOVERY_PREVIEW_GROUP_LIMIT,
} = {}) => {
  const safeLimit = Math.max(1, Number(limit || BUDGET_DISCOVERY_PREVIEW_GROUP_LIMIT));
  const normalizedGroups = groups.map(withBudgetBodyTypeGroup);

  if (!normalizedGroups.length) return [];

  // Body-type-specific discovery must remain inside that body type.
  // Example: "SUVs under 20 lakhs" should not be diversified into sedans/hatchbacks.
  if (filters.bodyType) {
    return normalizedGroups.slice(0, safeLimit);
  }

  const buckets = new Map();

  normalizedGroups.forEach((group, index) => {
    const bodyTypeGroup = group.bodyTypeGroup || "unknown";
    if (!buckets.has(bodyTypeGroup)) buckets.set(bodyTypeGroup, []);
    buckets.get(bodyTypeGroup).push({ group, index });
  });

  const bucketEntries = Array.from(buckets.entries())
    .map(([key, entries]) => ({
      key,
      entries,
      firstIndex: Math.min(...entries.map((entry) => entry.index)),
    }))
    .sort((left, right) => left.firstIndex - right.firstIndex);

  const selected = [];
  const selectedKeys = new Set();

  let cursor = 0;

  while (
    selected.length < safeLimit &&
    bucketEntries.some((bucket) => bucket.entries.length > cursor)
  ) {
    for (const bucket of bucketEntries) {
      const entry = bucket.entries[cursor];
      if (!entry) continue;

      const group = entry.group;
      const uniqueKey = searchKey(
        `${firstMeaningful(group.make, group.brand, "")}|${firstMeaningful(
          group.modelKey,
          group.fullModel,
          group.displayName,
          group.model,
          "",
        )}`,
      );

      if (uniqueKey && selectedKeys.has(uniqueKey)) continue;

      selected.push(group);
      if (uniqueKey) selectedKeys.add(uniqueKey);

      if (selected.length >= safeLimit) break;
    }

    cursor += 1;
  }

  if (selected.length < safeLimit) {
    for (const group of normalizedGroups) {
      const uniqueKey = searchKey(
        `${firstMeaningful(group.make, group.brand, "")}|${firstMeaningful(
          group.modelKey,
          group.fullModel,
          group.displayName,
          group.model,
          "",
        )}`,
      );

      if (uniqueKey && selectedKeys.has(uniqueKey)) continue;

      selected.push(group);
      if (uniqueKey) selectedKeys.add(uniqueKey);

      if (selected.length >= safeLimit) break;
    }
  }

  return selected.slice(0, safeLimit);
};

const buildBudgetDiscoveryModelGroups = ({
  rows = [],
  budgetMax = 0,
  variantLimit = BUDGET_DISCOVERY_VARIANTS_PER_GROUP_LIMIT,
} = {}) => {
  const groups = new Map();

  for (const row of rows) {
    const groupKey = `${row.makeKey || slugForReadModel(row.make)}|${row.modelKey || slugForReadModel(row.model)}`;
    if (!groupKey.replace(/\|/g, "")) continue;

    if (!groups.has(groupKey)) {
      groups.set(groupKey, {
        make: row.make,
        brand: row.make,
        model: row.model,
        modelKey: row.modelKey,
        makeKey: row.makeKey,
        fullModel: row.fullModel,
        displayName: row.fullModel || [row.make, row.model].filter(Boolean).join(" "),
        bodyType: row.bodyType,
        bodyTypeKey: row.bodyTypeKey,
        bodyTypeGroup: row.bodyTypeGroup || getBudgetBodyTypeGroup(row.bodyType, row.bodyTypeKey || row.segment),
        segment: row.segment || row.bodyType,
        city: row.city,
        citySlug: row.citySlug,
        rows: [],
      });
    }

    groups.get(groupKey).rows.push(row);
  }

  return [...groups.values()]
    .map((group) => {
      const variants = uniqueBy(
        group.rows
        .filter((row) => row.exShowroomPrice > 0 && (!budgetMax || row.exShowroomPrice <= budgetMax))
        .sort((left, right) => left.exShowroomPrice - right.exShowroomPrice),
        getBudgetVariantUniqueKey,
      );

      const startsFrom = variants[0] || {};
      const bestUnderBudget = variants[variants.length - 1] || startsFrom;
      const fuelTypes = unique(variants.map((row) => row.fuelType || row.fuel).filter(Boolean));
      const transmissions = unique(variants.map((row) => row.transmission).filter(Boolean));

      return compactObject({
        make: group.make,
        brand: group.brand,
        model: group.model,
        modelKey: group.modelKey,
        makeKey: group.makeKey,
        fullModel: group.fullModel,
        displayName: group.displayName,
        bodyType: group.bodyType,
        bodyTypeKey: group.bodyTypeKey,
        segment: group.segment,
        city: group.city,
        citySlug: group.citySlug,
        startsFromVariant: startsFrom.variant,
        startsFromPrice: startsFrom.exShowroomPrice,
        startsFromPriceLabel: startsFrom.exShowroomPriceLabel,
        bestUnderBudgetVariant: bestUnderBudget.variant,
        bestUnderBudgetPrice: bestUnderBudget.exShowroomPrice,
        bestUnderBudgetPriceLabel: bestUnderBudget.exShowroomPriceLabel,
        qualifyingVariantCount: variants.length,
        fuelTypes,
        transmissions,
        priceRangeLabel:
          startsFrom.exShowroomPriceLabel && bestUnderBudget.exShowroomPriceLabel
            ? startsFrom.exShowroomPrice === bestUnderBudget.exShowroomPrice
              ? startsFrom.exShowroomPriceLabel
              : `${startsFrom.exShowroomPriceLabel} – ${bestUnderBudget.exShowroomPriceLabel}`
            : "",
        qualifyingVariants: variants.slice(0, variantLimit).map((row) => compactObject({
          make: row.make,
          model: row.model,
          fullModel: row.fullModel,
          modelKey: row.modelKey,
          variant: row.variant,
          variantKey: row.variantKey,
          city: row.city,
          citySlug: row.citySlug,
          fuelType: row.fuelType,
          transmission: row.transmission,
          bodyType: row.bodyType,
          exShowroomPrice: row.exShowroomPrice,
          exShowroomPriceLabel: row.exShowroomPriceLabel,
          onRoadPrice: row.onRoadPrice,
          onRoadPriceLabel: row.onRoadPriceLabel,
          featureKey: row.featureKey,
          featureName: row.featureName,
          matchedFeature: row.matchedFeature,
          foundMatrixRows: row.foundMatrixRows,
          featureAvailability: row.featureAvailability,
          featureMatchSource: row.featureMatchSource,
          dataSource: row.dataSource,
        })),
        dataSource: "aci_vehicle_price_rows",
      });
    })
    .filter((group) => group.qualifyingVariantCount > 0)
    .sort((left, right) => {
      const rightBest = Number(right.bestUnderBudgetPrice || 0);
      const leftBest = Number(left.bestUnderBudgetPrice || 0);
      if (rightBest !== leftBest) return rightBest - leftBest;
      return Number(left.startsFromPrice || 0) - Number(right.startsFromPrice || 0);
    });
};

const buildBudgetDiscoveryFacets = ({ rows = [] } = {}) => {
  const prices = rows
    .map((row) => Number(row.exShowroomPrice || 0))
    .filter((price) => Number.isFinite(price) && price > 0);

  const uniqueFacet = (values = []) =>
    unique(values.map(displayName).filter(Boolean)).sort((left, right) =>
      left.localeCompare(right),
    );

  return {
    bodyTypes: uniqueFacet(rows.map((row) => row.bodyType)),
    fuelTypes: uniqueFacet(rows.map((row) => row.fuelType || row.fuel)),
    transmissions: uniqueFacet(rows.map((row) => row.transmission)),
    priceRange: {
      min: prices.length ? Math.min(...prices) : 0,
      max: prices.length ? Math.max(...prices) : 0,
      minLabel: prices.length ? formatMoney(Math.min(...prices)) : "",
      maxLabel: prices.length ? formatMoney(Math.max(...prices)) : "",
    },
  };
};

const buildBudgetPriceRowQuery = ({
  citySlug = DEFAULT_CITY,
  budgetMin = 0,
  budgetMax = 0,
  make = "",
  filters = {},
  modelKeys = [],
} = {}) => {
  const priceQuery = {
    $gt: budgetMin > 0 ? budgetMin : 0,
  };
  if (budgetMax > 0) priceQuery.$lte = budgetMax;

  const query = {
    citySlug: slugForReadModel(citySlug || DEFAULT_CITY) || "new-delhi",
    exShowroomPrice: priceQuery,
  };

  const makeKey = slugForReadModel(make);
  if (makeKey) query.makeKey = makeKey;

  const keys = unique(asArray(modelKeys).map(slugForReadModel).filter(Boolean));
  if (keys.length) query.modelKey = { $in: keys };

  const bodyTypeKey = slugForReadModel(filters.bodyType || "");
  if (bodyTypeKey === "suv" || bodyTypeKey === "suvs") {
    query.$or = [
      { bodyTypeKey: { $in: ["suv", "suvs", "sport-utilities"] } },
      { bodyType: /suv|sport util/i },
    ];
  }

  const fuelKey = slugForReadModel(filters.fuelType || "");
  if (fuelKey) query.fuelKey = fuelKey;

  const transmissionKey = slugForReadModel(filters.transmission || "");
  if (transmissionKey === "automatic") {
    query.transmissionKey = { $in: ["automatic", "auto", "amt", "cvt", "dct", "ivt", "at", "dsg"] };
  } else if (transmissionKey === "manual") {
    query.transmissionKey = "manual";
  }

  return query;
};

const buildBudgetGroupFromSummary = ({
  summary = {},
  budgetMax = 0,
  fallbackVariant = "",
} = {}) => {
  const startsFromVariant = firstMeaningful(
    fallbackVariant,
    asArray(summary.variantsPreview)[0],
    "Base variant",
  );
  const bestUnderBudgetVariant = Number(summary.maxExShowroomPrice || 0) <= Number(budgetMax || 0)
    ? firstMeaningful(
        asArray(summary.variantsPreview).slice(-1)[0],
        startsFromVariant,
      )
    : startsFromVariant;
  const startsFromPrice = Number(summary.minExShowroomPrice || 0) || null;
  const bestUnderBudgetPrice = Number(summary.maxExShowroomPrice || 0) <= Number(budgetMax || 0)
    ? Number(summary.maxExShowroomPrice || 0)
    : startsFromPrice;

  return compactObject({
    make: summary.make,
    brand: summary.make || summary.brand,
    model: summary.model,
    modelKey: summary.modelKey,
    makeKey: summary.makeKey,
    fullModel: summary.fullModel,
    displayName: summary.displayName || summary.fullModel || [summary.make, summary.model].filter(Boolean).join(" "),
    bodyType: summary.bodyType,
    bodyTypeKey: summary.bodyTypeKey,
    bodyTypeGroup: getBudgetBodyTypeGroup(summary.bodyType, summary.bodyTypeKey || summary.segment),
    segment: summary.segment || summary.bodyType,
    city: summary.city,
    citySlug: summary.citySlug,
    startsFromVariant,
    startsFromPrice,
    startsFromPriceLabel: startsFromPrice ? formatMoney(startsFromPrice) : "",
    bestUnderBudgetVariant,
    bestUnderBudgetPrice,
    bestUnderBudgetPriceLabel: bestUnderBudgetPrice ? formatMoney(bestUnderBudgetPrice) : "",
    qualifyingVariantCount: Math.max(1, Number(summary.variantCount || 0) || asArray(summary.variantsPreview).length || 1),
    fuelTypes: asArray(summary.fuels || summary.fuelTypes || summary.fuelText),
    transmissions: asArray(summary.transmissions || summary.transmissionText),
    priceRangeLabel: summary.priceRangeLabel || (startsFromPrice ? formatMoney(startsFromPrice) : ""),
    qualifyingVariants: [],
    dataSource: "aci_vehicle_model_summary",
  });
};

const hydratePreviewBudgetGroups = async ({
  collection,
  previewGroups = [],
  projection,
  citySlug = DEFAULT_CITY,
  budgetMin = 0,
  budgetMax = 0,
  make = "",
  filters = {},
} = {}) => {
  const modelKeys = previewGroups.map((group) => group.modelKey).filter(Boolean);
  if (!modelKeys.length) return previewGroups;

  const query = buildBudgetPriceRowQuery({
    citySlug,
    budgetMin,
    budgetMax,
    make,
    filters,
    modelKeys,
  });
  const cursor = collection
    .find(query, { projection })
    .sort({ modelKey: 1, exShowroomPrice: 1 })
    .limit(500)
    .batchSize(250);

  // No hard hint here: Atlas free-tier/storage cleanup may remove old named index.
  // The collection is small enough for Mongo planner to choose the available plan safely.

  const rows = (await cursor.toArray())
    .map(normalizeBudgetDiscoveryRow)
    .filter((row) => Number(row.exShowroomPrice || 0) > 0)
    .filter((row) => bodyTypeMatchesBudgetFilter(row, filters.bodyType))
    .filter((row) => transmissionMatchesBudgetFilter(row, filters.transmission))
    .filter((row) => fuelTypeMatchesBudgetFilter(row, filters.fuelType));

  const hydratedGroups = buildBudgetDiscoveryModelGroups({
    rows,
    budgetMax,
    variantLimit: BUDGET_DISCOVERY_VARIANTS_PER_GROUP_LIMIT,
  });
  const hydratedByModel = new Map(
    hydratedGroups.map((group) => [group.modelKey || slugForReadModel(group.model), group]),
  );

  return previewGroups.map((group) =>
    hydratedByModel.get(group.modelKey || slugForReadModel(group.model)) || group,
  );
};

const runtimeBudgetVehicleDiscoveryFromSummaries = async ({
  db,
  collection,
  projection,
  filters = {},
  budgetMin = 0,
  budgetMax = 0,
  city = DEFAULT_CITY,
  citySlug = "new-delhi",
  make = "",
  toolPlan = {},
} = {}) => {
  const startedAt = Date.now();
  const modelSummaryCollection = db.collection("aci_vehicle_model_summary");
  const makeKey = slugForReadModel(make);
  const summaryQuery = {
    citySlug,
    minExShowroomPrice: {
      $gt: budgetMin > 0 ? budgetMin : 0,
      ...(budgetMax > 0 ? { $lte: budgetMax } : {}),
    },
  };
  if (makeKey) summaryQuery.makeKey = makeKey;

  const summaryCursor = modelSummaryCollection
    .find(summaryQuery, {
      projection: {
        make: 1,
        makeKey: 1,
        model: 1,
        modelKey: 1,
        fullModel: 1,
        displayName: 1,
        bodyType: 1,
        bodyTypeKey: 1,
        segment: 1,
        city: 1,
        citySlug: 1,
        minExShowroomPrice: 1,
        maxExShowroomPrice: 1,
        priceRangeLabel: 1,
        variantCount: 1,
        variantsPreview: 1,
        fuelText: 1,
        fuels: 1,
        transmissionText: 1,
        transmissions: 1,
      },
    })
    .sort({ minExShowroomPrice: 1, make: 1, model: 1 })
    .limit(250)
    .batchSize(250);

  try {
    summaryCursor.hint("aci_model_summary_make_city_price");
  } catch {
    // Keep local/dev DBs without this read-model index working.
  }

  let summaries = (await summaryCursor.toArray())
    .filter((row) => bodyTypeMatchesBudgetFilter(row, filters.bodyType))
    .filter((row) => {
      if (!filters.transmission) return true;
      const textValue = [
        row.transmissionText,
        ...asArray(row.transmissions),
      ].join(" ");
      return transmissionMatchesBudgetFilter(
        { transmission: textValue, transmissionKey: slugForReadModel(textValue) },
        filters.transmission,
      );
    })
    .filter((row) => {
      if (!filters.fuelType) return true;
      const textValue = [
        row.fuelText,
        ...asArray(row.fuels),
      ].join(" ");
      return fuelTypeMatchesBudgetFilter(
        { fuelType: textValue, fuelKey: slugForReadModel(textValue) },
        filters.fuelType,
      );
    });
  const summaryQueryDurationMs = Date.now() - startedAt;

  const countStartedAt = Date.now();
  const priceRowQuery = buildBudgetPriceRowQuery({
    citySlug,
    budgetMin,
    budgetMax,
    make,
    filters,
  });
  let modelStats = [];
  try {
    modelStats = await collection
      .aggregate(
        [
          { $match: priceRowQuery },
          {
            $group: {
              _id: "$modelKey",
              count: { $sum: 1 },
            },
          },
        ],
        { hint: { citySlug: 1, exShowroomPrice: 1 } },
      )
      .toArray();
  } catch {
    modelStats = [];
  }
  const modelStatByKey = new Map(
    modelStats
      .filter((item) => item?._id)
      .map((item) => [item._id, Number(item.count || 0)]),
  );
  if (modelStatByKey.size) {
    summaries = summaries.filter((summary) => modelStatByKey.has(summary.modelKey));
  }
  const totalQualifyingPriceRows = modelStats.reduce(
    (total, item) => total + Number(item.count || 0),
    0,
  );
  const countDurationMs = Date.now() - countStartedAt;

  const allModelGroups = summaries
    .map((summary) => ({
      ...buildBudgetGroupFromSummary({ summary, budgetMax }),
      qualifyingVariantCount:
        modelStatByKey.get(summary.modelKey) ||
        Math.max(1, Number(summary.variantCount || 0) || asArray(summary.variantsPreview).length || 1),
    }))
    .filter((group) => group.modelKey || group.model);
  const fullModelGroups = allModelGroups.slice(0, BUDGET_DISCOVERY_FULL_GROUP_LIMIT);
  const previewSeedGroups = buildDiverseBudgetPreviewGroups({
    groups: allModelGroups,
    filters,
    limit: BUDGET_DISCOVERY_PREVIEW_GROUP_LIMIT,
  });
  const shouldHydratePreview = process.env.ACI_BUDGET_DISCOVERY_HYDRATE_PREVIEW === "true";
  const hydrateStartedAt = Date.now();
  const previewModelGroups = shouldHydratePreview
    ? await hydratePreviewBudgetGroups({
        collection,
        previewGroups: previewSeedGroups,
        projection,
        citySlug,
        budgetMin,
        budgetMax,
        make,
        filters,
      })
    : previewSeedGroups;
  const hydrateDurationMs = Date.now() - hydrateStartedAt;

  const previewBodyTypeGroups = unique(
    previewModelGroups
      .map((group) => group.bodyTypeGroup || getBudgetBodyTypeGroup(group.bodyType, group.bodyTypeKey || group.segment))
      .filter((group) => group && group !== "unknown"),
  );
  const allBodyTypeGroups = unique(
    allModelGroups
      .map((group) => group.bodyTypeGroup || getBudgetBodyTypeGroup(group.bodyType, group.bodyTypeKey || group.segment))
      .filter((group) => group && group !== "unknown"),
  );
  const totalQualifyingModels = allModelGroups.length;
  const totalUniqueQualifyingVariants = Math.max(
    Number(totalQualifyingPriceRows || 0),
    totalQualifyingModels,
  );
  const diversifiedPreview = !filters.bodyType && previewBodyTypeGroups.length > 1;
  const hasMore = totalQualifyingModels > previewModelGroups.length;
  const facets = buildBudgetDiscoveryFacets({
    rows: previewModelGroups.flatMap((group) => group.qualifyingVariants || []),
  });

  const result = {
    rows: previewModelGroups,
    items: previewModelGroups,
    cars: previewModelGroups,
    previewModelGroups,
    modelGroups: fullModelGroups,
    fullModelGroupCount: fullModelGroups.length,
    allModelGroupCount: totalQualifyingModels,
    totalModelGroupCount: totalQualifyingModels,
    returnedPreviewGroups: previewModelGroups.length,
    returnedModelGroups: previewModelGroups.length,
    diversifiedPreview,
    previewBodyTypeGroups,
    allBodyTypeGroups,
    matchedVariantCount: totalUniqueQualifyingVariants,
    totalQualifyingModels,
    totalUniqueQualifyingVariants,
    totalQualifyingPriceRows: Math.max(totalQualifyingPriceRows, totalUniqueQualifyingVariants),
    totalQualifyingVariants: totalUniqueQualifyingVariants,
    count: previewModelGroups.length,
    matched: totalQualifyingModels,
    facets,
    featureResolution: null,
    noResultRecovery: null,
    ranking: toolPlan.ranking || "value",
    filters: compactObject({
      city,
      citySlug,
      budgetMax,
      bodyType: filters.bodyType,
      transmission: filters.transmission,
      fuelType: filters.fuelType,
      make,
      mustHaveFeatures: [],
    }),
    budgetDiscovery: {
      enabled: true,
      isFeatureDiscovery: false,
      budgetMin,
      budgetMax,
      priceBasis: "ex_showroom",
      budgetBasis: "ex_showroom",
      strictBudget: true,
      totalQualifyingModels,
      totalUniqueQualifyingVariants,
      totalQualifyingPriceRows: Math.max(totalQualifyingPriceRows, totalUniqueQualifyingVariants),
      totalQualifyingVariants: totalUniqueQualifyingVariants,
      matchedVariantCount: totalUniqueQualifyingVariants,
      returnedPreviewGroups: previewModelGroups.length,
      returnedModelGroups: previewModelGroups.length,
      fullModelGroupCount: fullModelGroups.length,
      allModelGroupCount: totalQualifyingModels,
      totalModelGroupCount: totalQualifyingModels,
      diversifiedPreview,
      previewBodyTypeGroups,
      allBodyTypeGroups,
      hasMore,
      featureResolution: null,
      noResultRecovery: null,
      cityScoped: true,
      citySlug,
      rowSource: "model_summary_scoped_query",
      rowCacheHit: false,
      summaryQueryDurationMs,
      hydrateDurationMs,
      previewHydrated: shouldHydratePreview,
      countDurationMs,
    },
    sourceTransparency: {
      responseTool: "vehicle_recommend",
      modulesChecked: ["aci_vehicle_model_summary", "aci_vehicle_price_rows"],
      matched: totalQualifyingModels,
      dataSource: "aci_vehicle_read_models",
    },
    modulesChecked: ["aci_vehicle_model_summary", "aci_vehicle_price_rows"],
    source: "aci_vehicle_model_summary",
    dataSource: "aci_vehicle_read_models",
    cache: {
      cityScoped: true,
      citySlug,
      rowSource: "model_summary_scoped_query",
      rowCacheHit: false,
      summaryQueryDurationMs,
      hydrateDurationMs,
      previewHydrated: shouldHydratePreview,
      countDurationMs,
    },
  };

  if (process.env.ACI_BUDGET_DISCOVERY_PROFILE === "true") {
    console.log("[ACI BudgetDiscovery] summary path profile", JSON.stringify({
      citySlug,
      summaryRows: summaries.length,
      previewGroups: previewModelGroups.length,
      totalQualifyingModels,
      totalQualifyingPriceRows,
      summaryQueryDurationMs,
      hydrateDurationMs,
      previewHydrated: shouldHydratePreview,
      countDurationMs,
      totalDurationMs: Date.now() - startedAt,
    }));
  }

  return result;
};


export const prewarmBudgetDiscoveryCache = async ({ force = false } = {}) => {
  const db = await getMongooseDb();
  if (!db) {
    return {
      ok: false,
      status: "skipped",
      reason: "mongodb_unavailable",
      cache: {
        rows: 0,
        cacheHit: false,
      },
    };
  }

  const now = getBudgetDiscoveryNow();

  const defaultCitySlug = slugForReadModel(DEFAULT_CITY) || "new-delhi";
  const existingCityCache = budgetDiscoveryRowCacheByCity.get(defaultCitySlug);

  if (
    !force &&
    existingCityCache?.rows &&
    Array.isArray(existingCityCache.rows) &&
    existingCityCache.expiresAt > now
  ) {
    return {
      ok: true,
      status: "ready",
      cache: {
        citySlug: defaultCitySlug,
        rows: existingCityCache.rows.length,
        cacheHit: true,
        ttlMs: Math.max(0, existingCityCache.expiresAt - now),
      },
    };
  }

  if (force) {
    budgetDiscoveryRowCacheByCity.clear();
    budgetDiscoveryResultCache.clear();
  }

  const collection = db.collection("aci_vehicle_price_rows");
  const projection = getBudgetDiscoveryProjection();

  const startedAt = Date.now();
  const rows = await getCachedBudgetDiscoveryRows({
    collection,
    projection,
    citySlug: defaultCitySlug,
    force,
  });

  return {
    ok: true,
    status: "ready",
    durationMs: Date.now() - startedAt,
    cache: {
      citySlug: defaultCitySlug,
      rows: rows.length,
      cacheHit: false,
      inFlight: false,
      ttlMs: Math.max(0, (budgetDiscoveryRowCacheByCity.get(defaultCitySlug)?.expiresAt || 0) - getBudgetDiscoveryNow()),
    },
  };
};

export const triggerBudgetDiscoveryCacheWarm = ({ force = false } = {}) => {
  const defaultCitySlug = slugForReadModel(DEFAULT_CITY) || "new-delhi";

  if (!force && budgetDiscoveryRowCacheInFlightByCity.has(defaultCitySlug)) {
    return budgetDiscoveryRowCacheInFlightByCity.get(defaultCitySlug);
  }

  return prewarmBudgetDiscoveryCache({ force }).catch((error) => ({
    ok: false,
    status: "failed",
    error: error?.message || String(error || ""),
  }));
};

export const runtimeBudgetVehicleDiscovery = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const db = await getMongooseDb();
  if (!db) {
    return {
      rows: [],
      modelGroups: [],
      count: 0,
      matched: 0,
      modulesChecked: ["aci_vehicle_price_rows"],
      dataSource: "unavailable",
    };
  }

  const filters = toolPlan.filters || {};
  const budgetMin = Number(filters.budgetMin || 0) || 0;
  const budgetMax = Number(filters.budgetMax || 0);
  const city = getCity(toolPlan, context);
  const citySlug = slugForReadModel(city || DEFAULT_CITY);
  const make = firstMeaningful(filters.make, filters.brand, toolPlan.entities?.make, toolPlan.entities?.brand);
  const mustHaveFeatures = asArray(filters.mustHaveFeatures);
  const collection = db.collection("aci_vehicle_price_rows");
  const projection = getBudgetDiscoveryProjection();

  const cacheKey = getBudgetDiscoveryResultCacheKey({
    budgetMin,
    budgetMax,
    citySlug,
    make,
    filters,
    ranking: toolPlan.ranking || "value",
  });

  const cachedResult = getCachedBudgetDiscoveryResult(cacheKey);
  if (cachedResult) return cachedResult;

  const featureResolution = await resolveBudgetDiscoveryFeatureFilters({
    db,
    mustHaveFeatures,
  });
  const isFeatureDiscovery = mustHaveFeatures.length > 0;

  if (!isFeatureDiscovery) {
    const summaryResult = await runtimeBudgetVehicleDiscoveryFromSummaries({
      db,
      collection,
      projection,
      filters,
      budgetMin,
      budgetMax,
      city,
      citySlug,
      make,
      toolPlan,
    });

    if (summaryResult?.rows?.length || summaryResult?.modelGroups?.length) {
      return setCachedBudgetDiscoveryResult(cacheKey, summaryResult);
    }
  }

  if (isFeatureDiscovery && featureResolution.unresolvedFeatures.length) {
    const noResultRecovery = buildBudgetDiscoveryNoResultRecovery({
      reason: "feature_filter_unavailable",
      filters,
      budgetMax,
      budgetMin,
      featureResolution,
    });

    return setCachedBudgetDiscoveryResult(cacheKey, {
      rows: [],
      items: [],
      cars: [],
      previewModelGroups: [],
      modelGroups: [],
      count: 0,
      matched: 0,
      filters: compactObject({
        city,
        citySlug,
        budgetMax,
        bodyType: filters.bodyType,
        transmission: filters.transmission,
        fuelType: filters.fuelType,
        make,
        mustHaveFeatures,
      }),
      featureResolution,
      noResultRecovery,
      budgetDiscovery: {
        enabled: true,
        isFeatureDiscovery: true,
        budgetMin,
        budgetMax,
        priceBasis: "ex_showroom",
        budgetBasis: "ex_showroom",
        strictBudget: true,
        totalQualifyingModels: 0,
        totalUniqueQualifyingVariants: 0,
        totalQualifyingPriceRows: 0,
        totalQualifyingVariants: 0,
        matchedVariantCount: 0,
        noResultRecovery,
        featureResolution,
      },
      sourceTransparency: {
        responseTool: "vehicle_recommend",
        modulesChecked: ["vehicle_feature_catalog_v2"],
        matched: 0,
        dataSource: "feature_filter_unavailable",
      },
      modulesChecked: ["vehicle_feature_catalog_v2"],
      source: "vehicle_feature_catalog_v2",
      dataSource: "feature_filter_unavailable",
    });
  }

  const scopedRowsResult = await getScopedBudgetDiscoveryRows({
    collection,
    projection,
    citySlug,
    budgetMin,
    budgetMax,
    make,
    filters,
  });
  const normalizedRows = scopedRowsResult.rows || [];

  const makeKey = slugForReadModel(make);
  const featureVariantKeys = isFeatureDiscovery
    ? await fetchFeatureMatchedVariantKeys({
        db,
        featureKeys: featureResolution.featureKeys,
      })
    : new Set();

  const filteredRows = normalizedRows
    .filter((row) => row.exShowroomPrice > 0)
    .filter((row) => !budgetMin || row.exShowroomPrice > budgetMin)
    .filter((row) => !budgetMax || row.exShowroomPrice <= budgetMax)
    .filter((row) => !makeKey || row.makeKey === makeKey)
    .filter((row) => bodyTypeMatchesBudgetFilter(row, filters.bodyType))
    .filter((row) => transmissionMatchesBudgetFilter(row, filters.transmission))
    .filter((row) => fuelTypeMatchesBudgetFilter(row, filters.fuelType))
    .filter((row) => !isFeatureDiscovery || rowMatchesFeatureVariantKeys(row, featureVariantKeys));
  const rows = isFeatureDiscovery
    ? filteredRows.map((row) =>
        attachFeatureMatchMetadata({
          row,
          featureVariantKeys,
          featureResolution,
        }),
      )
    : filteredRows;
  const totalQualifyingPriceRows = rows.length;
  const uniqueVariantRows = dedupeBudgetRowsByVariant({
    rows,
    preferredCitySlug: citySlug,
  });

  const allModelGroups = buildBudgetDiscoveryModelGroups({
    rows: uniqueVariantRows,
    budgetMax,
    variantLimit: isFeatureDiscovery ? 200 : BUDGET_DISCOVERY_VARIANTS_PER_GROUP_LIMIT,
  });
  const fullModelGroups = allModelGroups.slice(0, BUDGET_DISCOVERY_FULL_GROUP_LIMIT);
  const previewModelGroups = buildDiverseBudgetPreviewGroups({
    groups: allModelGroups,
    filters,
    limit: BUDGET_DISCOVERY_PREVIEW_GROUP_LIMIT,
  });
  const previewBodyTypeGroups = unique(
    previewModelGroups
      .map((group) => group.bodyTypeGroup || getBudgetBodyTypeGroup(group.bodyType, group.bodyTypeKey || group.segment))
      .filter((group) => group && group !== "unknown"),
  );
  const allBodyTypeGroups = unique(
    allModelGroups
      .map((group) => group.bodyTypeGroup || getBudgetBodyTypeGroup(group.bodyType, group.bodyTypeKey || group.segment))
      .filter((group) => group && group !== "unknown"),
  );
  const diversifiedPreview = !filters.bodyType && previewBodyTypeGroups.length > 1;
  const totalQualifyingModels = allModelGroups.length;
  const totalUniqueQualifyingVariants = allModelGroups.reduce(
    (total, group) => total + Number(group.qualifyingVariantCount || 0),
    0,
  );
  const totalQualifyingVariants = totalUniqueQualifyingVariants;
  const hasMore = totalQualifyingModels > previewModelGroups.length;
  const facets = buildBudgetDiscoveryFacets({ rows: uniqueVariantRows });
  const noResultRecovery =
    isFeatureDiscovery && totalQualifyingModels === 0
      ? buildBudgetDiscoveryNoResultRecovery({
          reason: "no_confident_feature_budget_matches",
          filters,
          budgetMax,
          budgetMin,
          featureResolution,
        })
      : null;

  const result = {
    rows: previewModelGroups,
    items: previewModelGroups,
    cars: previewModelGroups,
    previewModelGroups,
    modelGroups: fullModelGroups,
    fullModelGroupCount: fullModelGroups.length,
    allModelGroupCount: totalQualifyingModels,
    totalModelGroupCount: totalQualifyingModels,
    returnedPreviewGroups: previewModelGroups.length,
    returnedModelGroups: previewModelGroups.length,
    diversifiedPreview,
    previewBodyTypeGroups,
    allBodyTypeGroups,
    matchedVariantCount: totalUniqueQualifyingVariants,
    totalQualifyingModels,
    totalUniqueQualifyingVariants,
    totalQualifyingPriceRows,
    totalQualifyingVariants,
    count: previewModelGroups.length,
    matched: totalQualifyingModels,
    facets,
    featureResolution: isFeatureDiscovery ? featureResolution : null,
    noResultRecovery,
    ranking: toolPlan.ranking || "value",
    filters: compactObject({
      city,
      citySlug,
      budgetMax,
      bodyType: filters.bodyType,
      transmission: filters.transmission,
      fuelType: filters.fuelType,
      make,
      mustHaveFeatures,
    }),
    budgetDiscovery: {
      enabled: true,
      isFeatureDiscovery,
      budgetMin,
      budgetMax,
      priceBasis: "ex_showroom",
      budgetBasis: "ex_showroom",
      strictBudget: true,
      totalQualifyingModels,
      totalUniqueQualifyingVariants,
      totalQualifyingPriceRows,
      totalQualifyingVariants,
      matchedVariantCount: totalUniqueQualifyingVariants,
      returnedPreviewGroups: previewModelGroups.length,
      returnedModelGroups: previewModelGroups.length,
      fullModelGroupCount: fullModelGroups.length,
      allModelGroupCount: totalQualifyingModels,
      totalModelGroupCount: totalQualifyingModels,
      diversifiedPreview,
      previewBodyTypeGroups,
      allBodyTypeGroups,
      hasMore,
      featureResolution: isFeatureDiscovery ? featureResolution : null,
      noResultRecovery,
      cityScoped: true,
      citySlug,
      rowSource: scopedRowsResult.source,
      rowCacheHit: scopedRowsResult.cacheHit,
      scopedQueryDurationMs: scopedRowsResult.queryDurationMs,
      scopedNormalizeDurationMs: scopedRowsResult.normalizeDurationMs,
    },
    sourceTransparency: {
      responseTool: "vehicle_recommend",
      modulesChecked: isFeatureDiscovery
        ? ["aci_vehicle_price_rows", "vehicle_feature_catalog_v2", "vehicle_variant_feature_matrix_v2"]
        : ["aci_vehicle_price_rows"],
      matched: totalQualifyingModels,
      dataSource: "aci_vehicle_read_models",
    },
    modulesChecked: isFeatureDiscovery
      ? ["aci_vehicle_price_rows", "vehicle_feature_catalog_v2", "vehicle_variant_feature_matrix_v2"]
      : ["aci_vehicle_price_rows"],
    source: "aci_vehicle_price_rows",
    dataSource: "aci_vehicle_read_models",
    cache: {
      cityScoped: true,
      citySlug,
      rowSource: scopedRowsResult.source,
      rowCacheHit: scopedRowsResult.cacheHit,
      queryDurationMs: scopedRowsResult.queryDurationMs,
      normalizeDurationMs: scopedRowsResult.normalizeDurationMs,
    },
  };

  return setCachedBudgetDiscoveryResult(cacheKey, result);
};

export const runtimeVehiclePriceBreakup = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const data = await runtimeVehiclePricelist({ toolPlan, context });
  const row = data.rows[0] || {};
  const exShowroom = row.exShowroomPrice || 0;
  const rto = row.rto || 0;
  const insurance = row.insurance || 0;
  const tcs = row.tcs || 0;
  const handling = row.handling || 0;
  const fastag = row.fastag || 0;
  const accessories = row.accessories || 0;

  const computedOnRoad =
    row.onRoadPrice ||
    exShowroom + rto + insurance + tcs + handling + fastag + accessories;

  return {
    ...data,
    rows: row.model || row.variant ? [row] : [],
    breakup: {
      exShowroom,
      rto,
      insurance,
      tcs,
      handling,
      fastag,
      accessories,
      onRoadPrice: computedOnRoad,
      finalOnRoadPrice: computedOnRoad,
      lineItems: [
        { key: "exShowroom", label: "Ex-showroom", amount: exShowroom },
        { key: "rto", label: "RTO", amount: rto },
        { key: "insurance", label: "Insurance", amount: insurance },
        { key: "tcs", label: "TCS", amount: tcs },
        { key: "handling", label: "Handling charges", amount: handling },
        { key: "fastag", label: "FASTag", amount: fastag },
        { key: "accessories", label: "Optional accessories", amount: accessories },
      ].filter((item) => item.amount > 0),
    },
  };
};

export const runtimeVehicleEmi = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const data = await runtimeVehiclePricelist({ toolPlan, context });
  const row = data.rows[0] || {};
  const filters = toolPlan.filters || {};

  const price =
    filters.priceBasis === "ex_showroom"
      ? row.exShowroomPrice || row.onRoadPrice || 0
      : row.onRoadPrice || row.exShowroomPrice || 0;

  const emi = calculateEmi({
    price,
    downPayment: filters.downPayment,
    loanAmount: filters.loanAmount,
    loanPercent: filters.loanPercent,
    tenureMonths: filters.tenureMonths,
    roi: filters.roi,
  });

  return {
    ...data,
    rows: row.model || row.variant ? [row] : [],
    emi,
    data: emi,
    assumptions: {
      priceBasis: filters.priceBasis || "on_road",
      downPayment: filters.downPayment,
      loanAmount: filters.loanAmount,
      loanPercent: filters.loanPercent || DEFAULT_EMI.loanPercent,
      tenureMonths: filters.tenureMonths || DEFAULT_EMI.tenureMonths,
      roi: filters.roi || DEFAULT_EMI.roi,
    },
  };
};

export const runtimeVehiclePriceHistory = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const { collection, collectionName, reason } = await getCollection(
    PRICE_HISTORY_COLLECTION_CANDIDATES,
  );

  const fastQuery = buildFastBrandModelQuery({
    toolPlan,
    context,
    includeCity: true,
  });
  const fallbackRegexQuery = buildVehicleMongoQuery({
    toolPlan,
    context,
    includeCity: true,
  });
  let rawRows = await safeFind(collection, fastQuery, {
    limit: DEFAULT_LIMITS.history,
    sort: { year: 1, date: 1, createdAt: 1 },
  });
  if (!rawRows.length) {
    rawRows = await safeFind(collection, fallbackRegexQuery, {
      limit: DEFAULT_LIMITS.history,
      sort: { year: 1, date: 1, createdAt: 1 },
    });
  }

  const rows = rawRows.map((row) => ({
    year: row.year || row.modelYear || row.priceYear || "",
    date: row.date || row.createdAt || row.updatedAt || "",
    exShowroomPrice: firstNumber(
      row.exShowroomPrice,
      row.ex_showroom,
      row.price,
    ),
    onRoadPrice: firstNumber(row.onRoadPrice, row.on_road),
    raw: row,
  }));

  return {
    rows,
    count: rows.length,
    matched: rows.length,
    modulesChecked: [collectionName || reason || "vehicle_price_history"],
    source: collectionName || "none",
    dataSource: collectionName ? "mongodb" : "empty",
  };
};

export const runtimeVehicleExplainer = async ({ toolPlan = {} } = {}) => {
  const topic = firstMeaningful(
    toolPlan.entities?.topic,
    asArray(toolPlan.entities?.topics)[0],
    "car_buying",
  );

  return {
    topic,
    title: displayName(String(topic).replace(/_/g, " ")),
    answer: "",
    sections: [],
    matched: 1,
    modulesChecked: ["vehicle_explainer"],
    dataSource: "deterministic_explainer",
  };
};

export const runtimeAciLeadCapture = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const model = getModel(toolPlan, context);
  const variant = getVariant(toolPlan, context);
  const city = getCity(toolPlan, context);
  const leadType =
    toolPlan.entities?.leadType || toolPlan.filters?.leadType || "quotation";

  return {
    leadType,
    selectedServices:
      toolPlan.entities?.selectedServices ||
      toolPlan.filters?.selectedServices ||
      [leadType],
    requiredFields: ["name", "mobile", "city"],
    request: {
      model,
      variant,
      city,
      leadType,
      status: "draft",
    },
    matched: 1,
    modulesChecked: ["aci_lead_capture"],
    dataSource: "lead_payload",
  };
};

export const runtimeUnavailable = async ({ toolPlan = {} } = {}) => ({
  unavailableReason:
    toolPlan.unavailableReason ||
    toolPlan.filters?.unavailableReason ||
    "unsupported_request",
  matched: 0,
  modulesChecked: ["unavailable"],
  dataSource: "executor",
});

export const runtimeClarification = async ({ plan = {} } = {}) => ({
  question: plan.clarification || "Can you clarify what you want to check?",
  options: plan.nextSteps || [],
  matched: 0,
  modulesChecked: ["clarification"],
  dataSource: "planner",
});

export const runtimeGeneralResponse = async () => ({
  matched: 1,
  modulesChecked: ["general_response"],
  dataSource: "executor",
});

export const runtimeInternalPassthrough = async ({
  runtimeHints = {},
} = {}) => ({
  ...(runtimeHints.internalResult || {}),
  matched: runtimeHints.internalResult ? 1 : 0,
  modulesChecked: ["internal_passthrough"],
  dataSource: "internal_router",
});

export const runtimeUsedCarPassthrough = async ({
  runtimeHints = {},
} = {}) => ({
  ...(runtimeHints.usedCarResult || {}),
  matched: runtimeHints.usedCarResult ? 1 : 0,
  modulesChecked: ["used_car_passthrough"],
  dataSource: "used_car_router",
});

export const runtimeModularTool = async (args = {}) =>
  runAciV2Tool({
    ...args,
    runtimeHints: {
      ...(args.runtimeHints || {}),
      executorSource: "runtime_modular_tool",
    },
  });



const SCORE_EXECUTOR_TOOLS = new Set([
  "vehicle_score_insight",
  "vehicle_score_profile",
  "vehicle_model_score_insights",
  "vehicle_same_family_value_insights",
  "vehicle_variant_upgrade_insight",
  "vehicle_top_score_insights",
  "vehicle_cross_model_score_diagnostic",
  "vehicle_model_score_comparison",
]);

const SCORE_OPERATION_BY_TOOL = Object.freeze({
  vehicle_score_insight: "variant_score_insight",
  vehicle_score_profile: "variant_score_insight",
  vehicle_model_score_insights: "model_score_insights",
  vehicle_same_family_value_insights: "same_family_value_insights",
  vehicle_variant_upgrade_insight: "variant_upgrade_insight",
  vehicle_top_score_insights: "top_module_score_insights",
  vehicle_cross_model_score_diagnostic: "cross_model_score_diagnostic",
  vehicle_model_score_comparison: "cross_model_score_diagnostic",
});

const SCORE_EXECUTOR_ALLOWED_OPERATIONS = new Set([
  "coverage",
  "variant_score_insight",
  "model_score_insights",
  "variant_upgrade_insight",
  "same_family_value_insights",
  "cross_model_score_diagnostic",
  "cross_model_score",
  "model_score_comparison",
  "top_module_score_insights",
]);

const pickScoreExecutorValue = (...values) =>
  values.find((value) => value !== undefined && value !== null && value !== "");

const asScoreExecutorArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const buildScoreInsightRuntimeArgs = ({
  toolPlan = {},
  plan = {},
  context = {},
  userMessage = "",
  runtimeHints = {},
  index = 0,
} = {}) => {
  const input = toolPlan.input || {};
  const args = toolPlan.args || {};
  const params = toolPlan.params || {};
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};

  const operation = pickScoreExecutorValue(
    toolPlan.operation,
    input.operation,
    args.operation,
    params.operation,
    entities.operation,
    filters.operation,
    SCORE_OPERATION_BY_TOOL[toolPlan.tool],
    "variant_score_insight",
  );

  const targets = pickScoreExecutorValue(
    toolPlan.targets,
    input.targets,
    args.targets,
    params.targets,
    entities.targets,
    filters.targets,
  );

  const models = pickScoreExecutorValue(
    toolPlan.models,
    input.models,
    args.models,
    params.models,
    entities.models,
    filters.models,
  );

  const comparisonModels = pickScoreExecutorValue(
    toolPlan.comparisonModels,
    input.comparisonModels,
    args.comparisonModels,
    params.comparisonModels,
    entities.comparisonModels,
    filters.comparisonModels,
    models,
  );

  const fuelKey = pickScoreExecutorValue(
    toolPlan.fuelKey,
    input.fuelKey,
    args.fuelKey,
    params.fuelKey,
    entities.fuelKey,
    filters.fuelKey,
    toolPlan.fuel,
    input.fuel,
    args.fuel,
    params.fuel,
    entities.fuel,
    filters.fuel,
  );

  const transmissionKey = pickScoreExecutorValue(
    toolPlan.transmissionKey,
    input.transmissionKey,
    args.transmissionKey,
    params.transmissionKey,
    entities.transmissionKey,
    filters.transmissionKey,
    toolPlan.transmission,
    input.transmission,
    args.transmission,
    params.transmission,
    entities.transmission,
    filters.transmission,
  );

  const makeKey = pickScoreExecutorValue(
    toolPlan.makeKey,
    input.makeKey,
    args.makeKey,
    params.makeKey,
    entities.makeKey,
    filters.makeKey,
    context?.selectedVehicle?.makeKey,
    context?.selectedVehicle?.make,
  );

  const modelKey = pickScoreExecutorValue(
    toolPlan.modelKey,
    input.modelKey,
    args.modelKey,
    params.modelKey,
    entities.modelKey,
    filters.modelKey,
    entities.primaryModelKey,
    entities.primaryModel,
    context?.selectedVehicle?.shortModelKey,
    context?.selectedVehicle?.modelKey,
    context?.selectedVehicle?.model,
  );

  const variantKey = pickScoreExecutorValue(
    toolPlan.variantKey,
    input.variantKey,
    args.variantKey,
    params.variantKey,
    entities.variantKey,
    filters.variantKey,
    entities.primaryVariantKey,
    context?.selectedVehicle?.variantKey,
  );

  const variantName = pickScoreExecutorValue(
    toolPlan.variantName,
    toolPlan.variant,
    input.variantName,
    input.variant,
    args.variantName,
    args.variant,
    params.variantName,
    params.variant,
    entities.variantName,
    entities.variant,
    entities.primaryVariant,
    filters.variantName,
    filters.variant,
    context?.selectedVehicle?.variantName,
    context?.selectedVehicle?.variant,
  );

  const normalizedInput = {
    ...input,
    ...args,
    ...params,
    operation,
    ...(targets ? { targets: asScoreExecutorArray(targets) } : {}),
    ...(models ? { models: asScoreExecutorArray(models) } : {}),
    ...(comparisonModels ? { comparisonModels: asScoreExecutorArray(comparisonModels) } : {}),
    ...(fuelKey ? { fuelKey } : {}),
    ...(transmissionKey ? { transmissionKey } : {}),
    ...(makeKey ? { makeKey } : {}),
    ...(modelKey ? { modelKey } : {}),
    ...(variantKey ? { variantKey } : {}),
    ...(variantName ? { variant: variantName, variantName } : {}),
  };

  const normalizedToolPlan = {
    ...toolPlan,
    operation,
    ...(targets ? { targets: asScoreExecutorArray(targets) } : {}),
    ...(models ? { models: asScoreExecutorArray(models) } : {}),
    ...(comparisonModels ? { comparisonModels: asScoreExecutorArray(comparisonModels) } : {}),
    ...(fuelKey ? { fuelKey } : {}),
    ...(transmissionKey ? { transmissionKey } : {}),
    input: normalizedInput,
    args: {
      ...(toolPlan.args || {}),
      ...normalizedInput,
    },
    params: {
      ...(toolPlan.params || {}),
      ...normalizedInput,
    },
    entities: {
      ...(toolPlan.entities || {}),
      ...(targets ? { targets: asScoreExecutorArray(targets) } : {}),
      ...(models ? { models: asScoreExecutorArray(models) } : {}),
      ...(comparisonModels ? { comparisonModels: asScoreExecutorArray(comparisonModels) } : {}),
      operation,
      ...(fuelKey ? { fuelKey } : {}),
      ...(transmissionKey ? { transmissionKey } : {}),
    },
    filters: {
      ...(toolPlan.filters || {}),
      ...(fuelKey ? { fuelKey } : {}),
      ...(transmissionKey ? { transmissionKey } : {}),
      ...(makeKey ? { makeKey } : {}),
      ...(modelKey ? { modelKey } : {}),
      ...(variantKey ? { variantKey } : {}),
      ...(variantName ? { variant: variantName, variantName } : {}),
    },
    userMessage,
    message: userMessage,
  };

  return {
    ...normalizedInput,
    toolPlan: normalizedToolPlan,
    plan,
    context,
    runtimeHints,
    index,
    userMessage,
    message: userMessage,
    operation,
  };
};

export const runtimeVehicleScoreInsight = async ({
  toolPlan = {},
  plan = {},
  context = {},
  userMessage = "",
  runtimeHints = {},
  index = 0,
} = {}) => {
  const runtimeArgs = buildScoreInsightRuntimeArgs({
    toolPlan,
    plan,
    context,
    userMessage,
    runtimeHints,
    index,
  });

  const isCrossModelScoreOperation =
    runtimeArgs.operation === "cross_model_score_diagnostic" ||
    runtimeArgs.operation === "cross_model_score" ||
    runtimeArgs.operation === "model_score_comparison" ||
    (Array.isArray(runtimeArgs.targets) && runtimeArgs.targets.length >= 2) ||
    (Array.isArray(runtimeArgs.comparisonModels) && runtimeArgs.comparisonModels.length >= 2);

  const normalizedUserMessage = String(userMessage || runtimeArgs.userMessage || "").toLowerCase();
  const hasVariantIdentity = Boolean(
    runtimeArgs.variantKey ||
      runtimeArgs.variant ||
      runtimeArgs.variantName ||
      runtimeArgs.toolPlan?.variant ||
      runtimeArgs.toolPlan?.variantName ||
      runtimeArgs.toolPlan?.entities?.primaryVariant ||
      runtimeArgs.toolPlan?.entities?.variant
  );

  const isModelLevelValueScoreOperation =
    !hasVariantIdentity &&
    !!runtimeArgs.modelKey &&
    /\b(value|worth|value for money|good value|good\s+value)\b/i.test(normalizedUserMessage);

  if (!isCrossModelScoreOperation && !isModelLevelValueScoreOperation) {
    return runtimeModularTool({
      toolPlan,
      plan,
      context,
      userMessage,
      runtimeHints,
      index,
    });
  }

  let result;

  if (isModelLevelValueScoreOperation) {
    result = await runVehicleScoreInsightTool({
      ...runtimeArgs,
      operation: "same_family_value_insights",
    });

    const resultBlob = JSON.stringify(result || {});
    const usable =
      result?.status === "success" &&
      !/I found score insight data for Score insight/i.test(resultBlob) &&
      !/"error"\s*:/i.test(resultBlob);

    if (!usable) {
      result = await runVehicleScoreInsightTool({
        ...runtimeArgs,
        operation: "model_score_insights",
      });
    }

    const fallbackBlob = JSON.stringify(result || {});
    const fallbackUsable =
      result?.status === "success" &&
      !/I found score insight data for Score insight/i.test(fallbackBlob);

    if (!fallbackUsable) {
      const modelLabel = String(context?.selectedVehicle?.fullModel || runtimeArgs.modelKey || "this model")
        .replace(/[_-]+/g, " ")
        .replace(/\b\w/g, (char) => char.toUpperCase());

      return {
        status: "needs_more_detail",
        operation: "same_family_value_insights",
        intent: "vehicle_score_insight",
        canvasType: "score_insight_canvas",
        inlineType: "score_insight_summary",
        answer: `I found ${modelLabel}. To judge value properly, I need the fuel/transmission or variant, because value is scored within the same model family. ${executorDecisionLanguageText("decision_score_module_summary_note", {
          operation: "same_family_value_insights",
          modelLabel,
        })}`,
        data: {
          modelKey: runtimeArgs.modelKey,
          operation: "same_family_value_insights",
          usageGuardrail: {
            canUseForFinalRecommendation: false,
            finalRecommendationEnabled: false,
          },
        },
        usageGuardrail: {
          canUseForFinalRecommendation: false,
          finalRecommendationEnabled: false,
        },
        modulesChecked: ["vehicle_score_insight"],
        source: "vehicle_score_insight",
      };
    }
  } else {
    result = await runVehicleScoreInsightTool(runtimeArgs);
  }
  const data = result?.data || {};
  const count =
    Number(result?.count) ||
    Number(result?.matched) ||
    Number(data.count) ||
    Number(data.recordCount) ||
    (Array.isArray(data.models) ? data.models.length : 0) ||
    (Array.isArray(data.variants) ? data.variants.length : 0) ||
    (Array.isArray(result?.rows) ? result.rows.length : 0) ||
    0;

  return {
    ...(result || {}),
    operation: result?.operation || data.operation || data.diagnosticType || runtimeArgs.operation,
    matched: count,
    count,
    rows: result?.rows || data.rows || data.models || data.variants || [],
    modulesChecked:
      result?.modulesChecked ||
      result?.sourceTransparency?.modulesChecked ||
      data?.sourceTransparency?.modulesChecked ||
      ["vehicle_score_insight"],
    source:
      result?.source ||
      result?.dataSource ||
      result?.sourceTransparency?.dataSource ||
      data?.sourceTransparency?.dataSource ||
      "vehicle_score_insight",
    dataSource:
      result?.dataSource ||
      result?.source ||
      result?.sourceTransparency?.dataSource ||
      data?.sourceTransparency?.dataSource ||
      "vehicle_score_insight",
    meta: {
      ...(result?.meta || {}),
      scoreInsightRuntime: "typed_executor_adapter",
      operation: result?.operation || data.operation || data.diagnosticType || runtimeArgs.operation,
    },
  };
};

const normalizeExecutorText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const FEATURE_V2_TOOL_ALIASES = new Set([
  "vehicle_features",
  "vehicle_feature_lookup",
  "vehicle_feature_answer",
  "vehicle_feature_discovery",
  "vehicle_feature_comparison",
  "vehicle_model_features_explorer",
  "vehicle_features_explorer",
]);

const FEATURE_COMPARISON_TOOLS = new Set([
  "vehicle_compare",
  "vehicle_comparison",
  "vehicle_model_comparison",
  "vehicle_variant_comparison",
]);

const FEATURE_QUERY_HINTS = [
  "feature",
  "features",
  "sunroof",
  "adas",
  "airbag",
  "airbags",
  "camera",
  "reverse camera",
  "rear camera",
  "360 camera",
  "ventilated",
  "wireless charging",
  "wireless charger",
  "cruise control",
  "alloy",
  "alloys",
  "headlamp",
  "headlight",
  "rear ac",
  "rear vents",
  "climate control",
  "hill hold",
  "hill assist",
  "tpms",
  "tyre pressure",
  "tire pressure",
  "speaker",
  "speakers",
  "music system",
  "audio system",
  "sound system",
  "stereo",
  "car stereo",
  "infotainment",
  "infotainment system",
  "touchscreen",
  "touch screen",
  "android auto",
  "apple carplay",

  "speaker system",
  "apple car play",
  "carplay",];

const isFeatureV2RuntimeRequest = ({ toolPlan = {}, userMessage = "" } = {}) => {
  const tool = String(toolPlan.tool || "");
  const intent = String(toolPlan.intent || toolPlan.toolIntent || "");
  const canvasType = String(toolPlan.canvasType || "");
  const text = normalizeExecutorText(
    [
      userMessage,
      tool,
      intent,
      canvasType,
      toolPlan.entities?.feature,
      toolPlan.input?.feature,
      toolPlan.filters?.feature,
    ].join(" "),
  );

  if (FEATURE_V2_TOOL_ALIASES.has(tool)) return true;
  if (intent.includes("vehicle_feature")) return true;
  if (intent.includes("features_explorer")) return true;
  if (canvasType.includes("features_explorer_canvas")) return true;
  if (canvasType.includes("feature_match_builder_canvas")) return true;

  if (FEATURE_COMPARISON_TOOLS.has(tool)) {
    return FEATURE_QUERY_HINTS.some((hint) =>
      text.includes(normalizeExecutorText(hint)),
    );
  }

  // Any new-car model query with a concrete feature word must go through Feature Resolver V2,
  // even if the planner incorrectly chose pricelist/unavailable.
  if (
    /\b(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700|scorpio|thar)\b/i.test(userMessage || "") &&
    FEATURE_QUERY_HINTS.some((hint) => text.includes(normalizeExecutorText(hint)))
  ) {
    return true;
  }

  // Bare variant-family query like "Creta King" should still become a current car canvas,
  // not unavailable.
  if (
    tool === "unavailable" &&
    /\bcreta\s+king\b/i.test(userMessage || "")
  ) {
    return true;
  }

  return false;
};

const toFeatureV2ToolPlan = (toolPlan = {}, userMessage = "") => {
  const originalTool = String(toolPlan.tool || "");
  const text = normalizeExecutorText(
    [userMessage, toolPlan.intent, toolPlan.toolIntent, toolPlan.canvasType].join(" "),
  );

  let tool = "vehicle_features";
  let intent = toolPlan.intent || toolPlan.toolIntent || "";

  const baseEntities = {
    ...(toolPlan.entities || {}),
    ...(toolPlan.input || {}),
  };

  const withoutFeatureFields = (next) => {
    const patched = {
      ...toolPlan,
      ...next,
      entities: { ...(toolPlan.entities || {}), ...(next.entities || {}) },
      input: { ...(toolPlan.input || {}), ...(next.input || {}) },
      filters: { ...(toolPlan.filters || {}), ...(next.filters || {}) },
    };

    delete patched.entities.feature;
    delete patched.entities.features;
    delete patched.entities.featureName;
    delete patched.input.feature;
    delete patched.input.features;
    delete patched.input.featureName;
    delete patched.filters.feature;

    return patched;
  };

  const explicitExplorerRequest =
    /\b(show|list|open)\b.*\bfeatures?\b/i.test(userMessage || "") &&
    !/\b(which|have|has|does|cheapest|compare|vs|versus|with|without|miss)\b/i.test(userMessage || "");

  if (explicitExplorerRequest) {
    const trimmedMessage = String(userMessage || "").trim();
    const modelOnlyExplorer =
      /^(show|list|open)\s+(all\s+)?features\s+(of|for)\s+.+$/i.test(trimmedMessage);

    return withoutFeatureFields({
      tool: "vehicle_model_features_explorer",
      intent: "vehicle_model_features_explorer",
      toolIntent: "vehicle_model_features_explorer",
      originalTool,
      ...(modelOnlyExplorer
        ? {
            entities: { ...(toolPlan.entities || {}), variant: "" },
            input: { ...(toolPlan.input || {}), variant: "" },
            filters: { ...(toolPlan.filters || {}), variant: "" },
          }
        : {}),
    });
  }

  const rawUserText = normalizeExecutorText(userMessage || "");

  const discoveryPhrase =
    text.includes("which variant") ||
    text.includes("which variants") ||
    text.includes("available in which") ||
    text.includes("cheapest") ||
    text.includes("most affordable") ||
    text.includes("do not have") ||
    text.includes("without") ||
    text.includes("miss") ||
    /\bwhich\b.*\bvariants?\b.*\b(have|has|get|gets|with|available)\b/i.test(rawUserText) ||
    /\bvariants?\b.*\bwith\b/i.test(rawUserText) ||
    /\bavailable\b.*\bwhich\b.*\bvariants?\b/i.test(rawUserText);

  if (
    originalTool === "vehicle_feature_discovery" ||
    intent === "vehicle_feature_discovery" ||
    discoveryPhrase
  ) {
    tool = "vehicle_feature_discovery";
    intent = "vehicle_feature_discovery";
  } else if (
    originalTool === "vehicle_feature_answer" ||
    originalTool === "vehicle_feature_lookup" ||
    intent === "vehicle_feature_answer"
  ) {
    tool = "vehicle_feature_answer";
    intent = "vehicle_feature_answer";
  } else if (
    originalTool === "vehicle_feature_comparison" ||
    FEATURE_COMPARISON_TOOLS.has(originalTool) ||
    text.includes("compare") ||
    text.includes(" vs ") ||
    text.includes("difference")
  ) {
    tool = "vehicle_feature_comparison";
    intent = "vehicle_feature_comparison";
  } else if (
    originalTool === "vehicle_model_features_explorer" ||
    text.includes("show all") ||
    text.includes("open feature") ||
    text.includes("feature explorer")
  ) {
    tool = "vehicle_model_features_explorer";
    intent = "vehicle_model_features_explorer";
  }

  return {
    ...toolPlan,
    tool,
    intent,
    toolIntent: intent,
    originalTool,
  };
};

const isV2FeatureRuntimeResult = (item = {}) =>
  item?.meta?.resolver === "featureResolverV2" ||
  item?.meta?.featureComparisonQuery === true ||
  item?.widget?.meta?.resolver === "featureResolverV2" ||
  item?.tool === "vehicle_features" ||
  item?.tool === "vehicle_feature_comparison";

const buildV2FeatureRuntimePassthrough = ({
  runtimeData = {},
  executablePlan = {},
  userMessage = "",
} = {}) => {
  const leadingQuestions =
    runtimeData.leadingQuestions ||
    runtimeData.conversationSuggestions ||
    runtimeData.widget?.leadingQuestions ||
    [];

  const widgets =
    Array.isArray(runtimeData.widgets) && runtimeData.widgets.length
      ? runtimeData.widgets
      : runtimeData.widget
        ? [runtimeData.widget]
        : [];

  return {
    ...runtimeData,
    intent: runtimeData.intent || executablePlan.intent || "vehicle_features",
    displayMode:
      runtimeData.displayMode ||
      (runtimeData.canvasType ? "canvas" : "inline"),
    canvasType: runtimeData.canvasType || runtimeData.widget?.canvasType || "",
    inlineType: runtimeData.inlineType || runtimeData.widget?.inlineType || "",
    title: runtimeData.title || runtimeData.widget?.title || "Vehicle features",
    answer:
      runtimeData.answer ||
      runtimeData.widget?.answer ||
      "I checked the current feature data for this car.",
    widgets,
    widget: runtimeData.widget || widgets[0] || null,
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    actions: runtimeData.actions || leadingQuestions,
    followUpSuggestions:
      runtimeData.followUpSuggestions ||
      leadingQuestions.map((item) => item.query || item.label).filter(Boolean),
    contextPatch: mergeContextPatches({
      managerPatch: executablePlan.contextPatch || {},
      toolPatch: runtimeData.contextPatch || {},
    }),
    sourceTransparency: runtimeData.sourceTransparency || {
      modulesChecked: runtimeData.modulesChecked || [],
      recordCount:
        Number(runtimeData.rows?.length || 0) ||
        Number(runtimeData.features?.length || 0) ||
        Number(runtimeData.variants?.length || 0),
    },
    meta: {
      ...(runtimeData.meta || {}),
      passthrough: "featureResolverV2",
      userMessage,
    },
  };
};


const runtimeVehicleFeatureComparison = async ({
  toolPlan = {},
  userMessage = "",
  context = {},
} = {}) => {
  const result = await maybeRunAciFeatureComparisonAnswer({
    message: userMessage,
    toolPlan,
    context,
  });

  if (result) {
    const sourceTransparency = result.sourceTransparency || {};
    return {
      ...result,
      matched: Number(sourceTransparency.matched || result.rows?.length || 0),
      count: Number(sourceTransparency.matched || result.rows?.length || 0),
      modulesChecked: sourceTransparency.modulesChecked || result.modulesChecked || [],
      source:
        sourceTransparency.dataSource ||
        result.source ||
        "vehicle_feature_catalog_v2+vehicle_variant_feature_matrix_v2",
      dataSource:
        sourceTransparency.dataSource ||
        result.dataSource ||
        "vehicle_feature_catalog_v2+vehicle_variant_feature_matrix_v2",
    };
  }

  return runtimeModularTool({
    toolPlan,
    userMessage,
    context,
  });
};


/* -------------------------------------------------------------------------- */
/*  Runtime Tool Registry                                                     */
/* -------------------------------------------------------------------------- */


export const ACI_RUNTIME_DATA_TOOLS = {
  vehicle_pricelist: runtimeVehiclePricelist,
  vehicle_colors: runtimeModularTool,
  vehicle_feature_lookup: runtimeModularTool,
  vehicle_spec_attribute_lookup: runVehicleSpecAttributeLookup,
  vehicle_feature_answer: runtimeModularTool,
  vehicle_feature_discovery: runtimeModularTool,
  vehicle_feature_comparison: runtimeVehicleFeatureComparison,
  vehicle_model_features_explorer: runtimeModularTool,
  vehicle_compare: runtimeVehicleCompare,
  vehicle_recommend: runtimeVehicleRecommend,
  vehicle_price_breakup: runtimeVehiclePriceBreakup,
  vehicle_emi: runtimeVehicleEmi,
  vehicle_price_history: runtimeVehiclePriceHistory,
  vehicle_explainer: runtimeVehicleExplainer,
  aci_lead_capture: runtimeAciLeadCapture,
  used_car_passthrough: runtimeUsedCarPassthrough,
  internal_passthrough: runtimeInternalPassthrough,
  clarification: runtimeClarification,
  unavailable: runtimeUnavailable,
  general_response: runtimeGeneralResponse,

  // V2 scaffold tool routes (for upcoming modular newCars rollout).
  vehicle_overview: runtimeModularTool,
  vehicle_recommendation: runtimeModularTool,
  vehicle_variant_advisor: runtimeModularTool,
  vehicle_features: runtimeModularTool,
  vehicle_ownership_cost: runtimeModularTool,
  vehicle_offers: runtimeModularTool,
  vehicle_similar: runtimeModularTool,
  vehicle_safety_ranking: runtimeModularTool,
  vehicle_score_insight: runtimeVehicleScoreInsight,
  vehicle_score_profile: runtimeVehicleScoreInsight,
  vehicle_model_score_insights: runtimeVehicleScoreInsight,
  vehicle_same_family_value_insights: runtimeVehicleScoreInsight,
  vehicle_top_score_insights: runtimeVehicleScoreInsight,
  vehicle_variant_upgrade_insight: runtimeVehicleScoreInsight,
  vehicle_cross_model_score_diagnostic: runtimeVehicleScoreInsight,
  vehicle_model_score_comparison: runtimeVehicleScoreInsight,
  quotation_lead: runtimeModularTool,
};

export const getAciRuntimeDataTool = (tool = "") =>
  ACI_RUNTIME_DATA_TOOLS[tool] || ACI_RUNTIME_DATA_TOOLS.general_response;

export const runAciRuntimeDataTool = async ({
  toolPlan = {},
  plan = {},
  context = {},
  userMessage = "",
  runtimeHints = {},
  adapters = {},
  index = 0,
} = {}) => {
  const adapter = adapters?.[toolPlan.tool];

  if (typeof adapter === "function") {
    return adapter({
      toolPlan,
      plan,
      context,
      userMessage,
      runtimeHints,
      index,
    });
  }

  if (toolPlan.tool !== "vehicle_feature_comparison" && isFeatureV2RuntimeRequest({ toolPlan, userMessage })) {
    const featureToolPlan = toFeatureV2ToolPlan(toolPlan, userMessage);

    return runtimeModularTool({
      toolPlan: featureToolPlan,
      plan,
      context,
      userMessage,
      runtimeHints,
      index,
    });
  }

  const runtimeTool = getAciRuntimeDataTool(toolPlan.tool);

  return runtimeTool({
    toolPlan,
    plan,
    context,
    userMessage,
    runtimeHints,
    index,
  });
};


const buildScoreExecutorPatch = (toolPlan = {}) => {
  if (!SCORE_EXECUTOR_TOOLS.has(String(toolPlan.tool || ""))) return {};

  const input = toolPlan.input || {};
  const args = toolPlan.args || {};
  const params = toolPlan.params || {};
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};

  const rawOperation = pickScoreExecutorValue(
    toolPlan.operation,
    input.operation,
    args.operation,
    params.operation,
    entities.operation,
    filters.operation,
  );

  const operation = SCORE_EXECUTOR_ALLOWED_OPERATIONS.has(String(rawOperation || ""))
    ? String(rawOperation)
    : "";

  const targets = pickScoreExecutorValue(
    toolPlan.targets,
    input.targets,
    args.targets,
    params.targets,
    entities.targets,
    filters.targets,
  );

  const models = pickScoreExecutorValue(
    toolPlan.models,
    input.models,
    args.models,
    params.models,
    entities.models,
    filters.models,
  );

  const comparisonModels = pickScoreExecutorValue(
    toolPlan.comparisonModels,
    input.comparisonModels,
    args.comparisonModels,
    params.comparisonModels,
    entities.comparisonModels,
    filters.comparisonModels,
    models,
  );

  const fuelKey = pickScoreExecutorValue(
    toolPlan.fuelKey,
    input.fuelKey,
    args.fuelKey,
    params.fuelKey,
    entities.fuelKey,
    filters.fuelKey,
  );

  const transmissionKey = pickScoreExecutorValue(
    toolPlan.transmissionKey,
    input.transmissionKey,
    args.transmissionKey,
    params.transmissionKey,
    entities.transmissionKey,
    filters.transmissionKey,
  );

  return {
    ...(operation ? { operation } : {}),
    ...(targets ? { targets: asScoreExecutorArray(targets) } : {}),
    ...(models ? { models: asScoreExecutorArray(models) } : {}),
    ...(comparisonModels ? { comparisonModels: asScoreExecutorArray(comparisonModels) } : {}),
    ...(fuelKey ? { fuelKey } : {}),
    ...(transmissionKey ? { transmissionKey } : {}),
  };
};

const mergeTypedScoreExecutorFields = ({ sanitizedTool = {}, originalTool = {} } = {}) => {
  if (!SCORE_EXECUTOR_TOOLS.has(String(sanitizedTool.tool || originalTool.tool || ""))) {
    return sanitizedTool;
  }

  const patch = {
    ...buildScoreExecutorPatch(originalTool),
    ...buildScoreExecutorPatch(sanitizedTool),
  };

  if (!Object.keys(patch).length) return sanitizedTool;

  return {
    ...sanitizedTool,
    ...patch,
    input: {
      ...(sanitizedTool.input || {}),
      ...(originalTool.input || {}),
      ...patch,
    },
    args: {
      ...(sanitizedTool.args || {}),
      ...(originalTool.args || {}),
      ...patch,
    },
    params: {
      ...(sanitizedTool.params || {}),
      ...(originalTool.params || {}),
      ...patch,
    },
    entities: {
      ...(sanitizedTool.entities || {}),
      ...(originalTool.entities || {}),
      ...(patch.models ? { models: patch.models } : {}),
      ...(patch.comparisonModels ? { comparisonModels: patch.comparisonModels } : {}),
      ...(patch.targets ? { targets: patch.targets } : {}),
      ...(patch.operation ? { operation: patch.operation } : {}),
      ...(patch.fuelKey ? { fuelKey: patch.fuelKey } : {}),
      ...(patch.transmissionKey ? { transmissionKey: patch.transmissionKey } : {}),
    },
    filters: {
      ...(sanitizedTool.filters || {}),
      ...(originalTool.filters || {}),
      ...(patch.fuelKey ? { fuelKey: patch.fuelKey } : {}),
      ...(patch.transmissionKey ? { transmissionKey: patch.transmissionKey } : {}),
    },
  };
};

const preserveTypedScoreFieldsInExecutablePlan = ({ executablePlan = {}, originalPlan = {} } = {}) => {
  const executableTools = asArray(executablePlan.tools);
  const originalTools = asArray(originalPlan.tools);

  if (!executableTools.length) return executablePlan;

  return {
    ...executablePlan,
    tools: executableTools.map((toolPlan, index) =>
      mergeTypedScoreExecutorFields({
        sanitizedTool: toolPlan,
        originalTool: originalTools[index] || {},
      })
    ),
  };
};

const getTypedScoreRuntimeOperation = (runtimeResults = []) => {
  const result = asArray(runtimeResults).find((item = {}) =>
    SCORE_EXECUTOR_TOOLS.has(String(item.executorTool || item.tool || ""))
  );

  if (!result) return "";

  return (
    result.operation ||
    result.data?.operation ||
    result.data?.diagnosticType ||
    result.meta?.operation ||
    ""
  );
};

const applyTypedScoreOperationToResponse = ({ response = {}, runtimeResults = [] } = {}) => {
  const operation = getTypedScoreRuntimeOperation(runtimeResults);
  if (!operation) return response;

  return {
    ...response,
    operation: response.operation || operation,
    data: {
      ...(response.data || {}),
      operation: response.data?.operation || operation,
    },
    meta: {
      ...(response.meta || {}),
      scoreInsightOperation: operation,
    },
  };
};

/* -------------------------------------------------------------------------- */
/*  Main Executor                                                             */
/* -------------------------------------------------------------------------- */

const getRuntimeComparisonResolutionMode = (item = {}) =>
  compareFirstText(
    item.comparisonResolutionMode,
    item.data?.comparisonResolutionMode,
    item.meta?.comparisonResolutionMode,
    item.sourceTransparency?.comparisonResolutionMode,
    item.sourceTransparency?.comparisonTrace?.comparisonResolutionMode,
  );

export const executeAciPlannerPlan = async ({
  plan,
  userMessage = "",
  context = {},
  runtimeHints = {},
  adapters = {},
  sanitize = true,
  validate = true,
} = {}) => {
  const sanitizedPlan = sanitizePlannerPlan(plan, { message: userMessage });
  const planValidation = validatePlannerPlan(sanitizedPlan, {
    message: userMessage,
  });

  const executablePlan = preserveTypedScoreFieldsInExecutablePlan({
    executablePlan: planValidation.plan || sanitizedPlan,
    originalPlan: plan,
  });
  const tools = asArray(executablePlan.tools);

  const runtimeResults = [];

  for (let index = 0; index < tools.length; index += 1) {
    const toolPlan = tools[index];

    try {
      const runtimeData = await runAciRuntimeDataTool({
        toolPlan,
        plan: executablePlan,
        context,
        userMessage,
        runtimeHints,
        adapters,
        index,
      });

      runtimeResults[index] = {
        ...(runtimeData || {}),
        executorTool: toolPlan.tool,
        executorIndex: index,
      };
    } catch (error) {
      runtimeResults[index] = {
        executorTool: toolPlan.tool,
        executorIndex: index,
        error: error?.message || "Runtime tool failed",
        rows: [],
        matched: 0,
        modulesChecked: [toolPlan.tool, "runtime_error"],
        dataSource: "runtime_error",
      };
    }
  }

  const v2FeatureRuntimeResult = runtimeResults.find(isV2FeatureRuntimeResult);

  let response = v2FeatureRuntimeResult
    ? buildV2FeatureRuntimePassthrough({
        runtimeData: v2FeatureRuntimeResult,
        executablePlan,
        userMessage,
      })
    : buildAciAssistResponseFromPlan({
        plan: executablePlan,
        runtimeResults,
        context,
        userMessage,
      });

  if (sanitize) {
    response = sanitizeAiAgentResponse(response, {
      message: userMessage,
      context,
    });
  }

  if (executablePlan.contextPatch && Object.keys(executablePlan.contextPatch).length) {
    response = {
      ...response,
      contextPatch: mergeContextPatches({
        previousPatch: context,
        managerPatch: executablePlan.contextPatch || {},
        toolPatch: response.contextPatch || {},
      }),
    };
  }

  response = applyTypedScoreOperationToResponse({
    response,
    runtimeResults,
  });

  const comparisonResolutionMode = compareFirstText(
    ...runtimeResults.map(getRuntimeComparisonResolutionMode),
  );

  if (comparisonResolutionMode) {
    response = {
      ...response,
      comparisonResolutionMode,
      sourceTransparency: {
        ...(response.sourceTransparency || {}),
        comparisonResolutionMode,
        comparisonTrace: {
          ...(response.sourceTransparency?.comparisonTrace || {}),
          comparisonResolutionMode,
        },
      },
      meta: {
        ...(response.meta || {}),
        comparisonResolutionMode,
      },
      data: {
        ...(response.data || {}),
        comparisonResolutionMode,
      },
    };
  }

  const contractValidation = validate
    ? validateAciAssistResponseContract(response)
    : { valid: true, errors: [] };

  return {
    ...response,
    planner: {
      mode: executablePlan.mode,
      domain: executablePlan.domain,
      conversationMode: executablePlan.conversationMode,
      customerStage: executablePlan.customerStage,
      confidence: executablePlan.confidence,
      tools: executablePlan.tools,
      validation: {
        valid: planValidation.valid,
        errors: planValidation.errors || [],
        warnings: planValidation.warnings || [],
      },
    },
    executor: {
      version: EXECUTOR_VERSION,
      runtimeResultsMeta: runtimeResults.map((item) => ({
        tool: item.executorTool,
        index: item.executorIndex,
        matched: item.matched || 0,
        source: item.source || item.dataSource || "",
        modulesChecked: item.modulesChecked || [],
        comparisonResolutionMode: getRuntimeComparisonResolutionMode(item),
        error: item.error || "",
      })),
      contractValidation,
    },
  };
};

export const executeAciPlannerTools = executeAciPlannerPlan;

export const runAciExecutor = executeAciPlannerPlan;

export default executeAciPlannerPlan;
