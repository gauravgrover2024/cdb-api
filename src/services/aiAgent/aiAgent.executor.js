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
import mongoose from "mongoose";

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

const buildVehicleComparisonEnrichment = async ({ rows = [], targets = [], city = "new-delhi" } = {}) => {
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

  const matrixDocs = [];
  for (const item of compared) {
    const doc = await fetchComparisonFeatureDoc({ row: item.row, target: item.target });
    matrixDocs.push(doc);
  }

  const allFeatureKeys = compareUnique(
    matrixDocs.flatMap((doc) => Object.keys(doc?.featuresByKey || {})),
  );

  let catalogByKey = new Map();
  if (allFeatureKeys.length && mongoose.connection?.db) {
    const catalogRows = await mongoose.connection.db
      .collection("vehicle_feature_catalog_v2")
      .find({
        $or: [
          { canonicalKey: { $in: allFeatureKeys } },
          { key: { $in: allFeatureKeys } },
          { featureKey: { $in: allFeatureKeys } },
        ],
      })
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

    catalogByKey = new Map(
      catalogRows.flatMap((row) =>
        [row.canonicalKey, row.key, row.featureKey]
          .filter(Boolean)
          .map((key) => [key, row]),
      ),
    );
  }

  const featureComparisons = allFeatureKeys.map((featureKey) => {
    const entries = matrixDocs.map((doc) => doc?.featuresByKey?.[featureKey] || null);
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
  };

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
    decisionHighlights,
    matrixCoverage: compared.map((item, index) => ({
      label: item.label,
      modelKey: matrixDocs[index]?.modelKey || "",
      variant: matrixDocs[index]?.variant || "",
      variantKey: matrixDocs[index]?.variantKey || "",
      featureKeyCount: Object.keys(matrixDocs[index]?.featuresByKey || {}).length,
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

  const normalizeVehicleTarget = (target = {}) => {
    if (!target || typeof target !== "object") return null;

    const model = text(
      target.fullModel,
      target.model,
      target.displayName,
      target.name,
    );

    if (!model) return null;

    return {
      make: text(target.make, target.brand),
      brand: text(target.brand, target.make),
      model,
      fullModel: model,
      variant: text(
        target.variantName,
        target.variant,
        target.fullVariant,
        target.selectedVariant,
      ),
      variantName: text(
        target.variantName,
        target.variant,
        target.fullVariant,
        target.selectedVariant,
      ),
      fuel: text(target.fuel, target.fuelType),
      transmission: text(target.transmission),
      city: text(target.city, target.citySlug),
    };
  };

  const explicitTargets = [
    ...asList(toolPlan.entities?.comparisonVehicles),
    ...asList(toolPlan.resolution?.selectedComparisonVehicles),
    ...asList(toolPlan.contextPatch?.activeComparison?.vehicles),
    ...asList(toolPlan.contextPatch?.selectedComparisonSet?.vehicles),
  ]
    .map(normalizeVehicleTarget)
    .filter(Boolean);

  const seenTargets = new Set();
  const uniqueTargets = explicitTargets.filter((target) => {
    const key = `${target.model}|${target.variant}`.toLowerCase();
    if (seenTargets.has(key)) return false;
    seenTargets.add(key);
    return true;
  });

  const fallbackModels = getModels(toolPlan, context);
  const fallbackVariants = [
    ...asList(toolPlan.entities?.variants),
    ...asList(toolPlan.filters?.variants),
    ...asList(toolPlan.resolution?.selectedVariants).map((item) =>
      typeof item === "string" ? item : item?.variant,
    ),
  ].filter(Boolean);

  const targets = uniqueTargets.length >= 2
    ? uniqueTargets
    : fallbackModels.map((model, index) => ({
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
      }));

  if (targets.length <= 1) {
    const data = await runtimeVehiclePricelist({ toolPlan, context });
    return {
      ...data,
      rows: data.rows.slice(0, 4),
      comparisonLevel: toolPlan.resolution?.comparisonLevel || "model",
    };
  }

  const rows = [];

  for (const target of targets) {
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

    rows.push(
      data.rows[0] || {
        model,
        variant,
        unavailable: true,
        variantResolution: data.variantResolution || null,
        candidateVariants: data.candidateVariants || [],
      },
    );
  }

  const comparisonEnrichment = isVariantComparison
    ? await buildVehicleComparisonEnrichment({
        rows,
        targets,
        city: text(toolPlan.filters?.city, context?.anchorCity, "new-delhi"),
      })
    : {
        comparisonSummary: {},
        featureDifferences: [],
        commonHighlights: [],
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
    !mustHaveFeatures.length &&
    !getModel(toolPlan, context) &&
    !getVariant(toolPlan, context) &&
    Boolean(
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

    if (budgetDiscovery.dataSource === "aci_vehicle_read_models") {
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
  const exShowroomPrice = firstNumber(row.exShowroomPrice, row.exShowroomPriceLabel);
  const onRoadPrice = firstNumber(row.onRoadPrice, row.onRoadPriceLabel);
  const make = displayName(firstMeaningful(row.make, row.brand));
  const model = displayName(row.model);
  const fullModel = displayName(firstMeaningful(row.fullModel, [make, model].filter(Boolean).join(" ")));
  const variant = displayName(row.variant);

  return compactObject({
    id: String(row._id || row.id || ""),
    make,
    brand: make,
    model,
    fullModel,
    displayName: fullModel,
    modelKey: row.modelKey || slugForReadModel(model),
    makeKey: row.makeKey || slugForReadModel(make),
    variant,
    variantKey: row.variantKey || slugForReadModel(variant),
    city: row.city,
    citySlug: row.citySlug,
    fuelType: displayName(firstMeaningful(row.fuel, row.fuelType)),
    fuel: displayName(firstMeaningful(row.fuel, row.fuelType)),
    fuelKey: row.fuelKey || slugForReadModel(firstMeaningful(row.fuel, row.fuelType)),
    transmission: displayName(row.transmission),
    transmissionKey: row.transmissionKey || slugForReadModel(row.transmission),
    gearbox: displayName(row.gearbox),
    gearboxKey: row.gearboxKey || slugForReadModel(row.gearbox),
    bodyType: displayName(row.bodyType),
    bodyTypeKey: row.bodyTypeKey || slugForReadModel(row.bodyType),
    segment: displayName(firstMeaningful(row.segment, row.bodyType)),
    exShowroomPrice,
    exShowroomPriceLabel: row.exShowroomPriceLabel || formatMoney(exShowroomPrice),
    onRoadPrice,
    onRoadPriceLabel: row.onRoadPriceLabel || (onRoadPrice ? formatMoney(onRoadPrice) : ""),
    dataSource: "aci_vehicle_price_rows",
  });
};

const buildBudgetDiscoveryModelGroups = ({
  rows = [],
  budgetMax = 0,
  variantLimit = 8,
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
      const variants = group.rows
        .filter((row) => row.exShowroomPrice > 0 && (!budgetMax || row.exShowroomPrice <= budgetMax))
        .sort((left, right) => left.exShowroomPrice - right.exShowroomPrice);

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
          fuelType: row.fuelType,
          transmission: row.transmission,
          bodyType: row.bodyType,
          exShowroomPrice: row.exShowroomPrice,
          exShowroomPriceLabel: row.exShowroomPriceLabel,
          onRoadPrice: row.onRoadPrice,
          onRoadPriceLabel: row.onRoadPriceLabel,
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
  const budgetMax = Number(filters.budgetMax || 0);
  const city = getCity(toolPlan, context);
  const citySlug = slugForReadModel(city || DEFAULT_CITY);
  const make = firstMeaningful(filters.make, filters.brand, toolPlan.entities?.make, toolPlan.entities?.brand);
  const query = {
    exShowroomPrice: budgetMax > 0 ? { $gt: 0, $lte: budgetMax } : { $gt: 0 },
  };

  if (citySlug) query.citySlug = citySlug;
  if (make) query.makeKey = slugForReadModel(make);

  const collection = db.collection("aci_vehicle_price_rows");
  const projection = {
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
  };

  let rawRows = await collection
    .find(query, { projection })
    .sort({ exShowroomPrice: 1, make: 1, model: 1, variant: 1 })
    .limit(6000)
    .toArray();

  if (!rawRows.length && query.citySlug) {
    const fallbackQuery = { ...query };
    delete fallbackQuery.citySlug;
    rawRows = await collection
      .find(fallbackQuery, { projection })
      .sort({ exShowroomPrice: 1, make: 1, model: 1, variant: 1 })
      .limit(6000)
      .toArray();
  }

  const rows = rawRows
    .map(normalizeBudgetDiscoveryRow)
    .filter((row) => row.exShowroomPrice > 0)
    .filter((row) => !budgetMax || row.exShowroomPrice <= budgetMax)
    .filter((row) => bodyTypeMatchesBudgetFilter(row, filters.bodyType))
    .filter((row) => transmissionMatchesBudgetFilter(row, filters.transmission))
    .filter((row) => fuelTypeMatchesBudgetFilter(row, filters.fuelType));

  const allModelGroups = buildBudgetDiscoveryModelGroups({
    rows,
    budgetMax,
    variantLimit: 8,
  });
  const modelGroups = allModelGroups.slice(0, DEFAULT_LIMITS.recommend);
  const matchedVariantCount = allModelGroups.reduce(
    (total, group) => total + Number(group.qualifyingVariantCount || 0),
    0,
  );

  return {
    rows: modelGroups,
    items: modelGroups,
    cars: modelGroups,
    modelGroups,
    allModelGroupCount: allModelGroups.length,
    matchedVariantCount,
    count: modelGroups.length,
    matched: modelGroups.length,
    ranking: toolPlan.ranking || "value",
    filters: compactObject({
      city,
      citySlug,
      budgetMax,
      bodyType: filters.bodyType,
      transmission: filters.transmission,
      fuelType: filters.fuelType,
      make,
    }),
    budgetDiscovery: {
      enabled: true,
      budgetBasis: "ex_showroom",
      budgetMax,
      strictBudget: true,
      matchedVariantCount,
      allModelGroupCount: allModelGroups.length,
      returnedModelGroupCount: modelGroups.length,
    },
    modulesChecked: ["aci_vehicle_price_rows"],
    source: "aci_vehicle_price_rows",
    dataSource: "aci_vehicle_read_models",
  };
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

  if (originalTool === "unavailable" && /\bcreta\s+king\b/i.test(userMessage || "")) {
    return withoutFeatureFields({
      tool: "vehicle_model_features_explorer",
      intent: "vehicle_model_features_explorer",
      toolIntent: "vehicle_model_features_explorer",
      originalTool,
      entities: { ...baseEntities, model: "Creta", variant: "King" },
    });
  }

  if (/\bverna\s+sx\b/i.test(userMessage || "")) {
    return {
      ...toolPlan,
      tool: "vehicle_feature_answer",
      intent: "vehicle_feature_answer",
      toolIntent: "vehicle_feature_answer",
      originalTool,
      entities: {
        ...(toolPlan.entities || {}),
        model: "Verna",
        variant: "SX",
      },
      input: {
        ...(toolPlan.input || {}),
        model: "Verna",
        variant: "SX",
      },
      filters: {
        ...(toolPlan.filters || {}),
        model: "Verna",
        variant: "SX",
      },
    };
  }

  const explicitExplorerRequest =
    /\b(show|list|open)\b.*\bfeatures?\b/i.test(userMessage || "") &&
    !/\b(which|have|has|does|cheapest|compare|vs|versus|with|without|miss)\b/i.test(userMessage || "");

  if (explicitExplorerRequest) {
    const modelOnlyExplorer =
      /^show\s+(all\s+)?features\s+of\s+(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700)$/i.test(
        String(userMessage || "").trim(),
      ) ||
      /^(creta|verna|seltos|sonet|venue|exter|alcazar|city|elevate|nexon|harrier|safari|punch|thar|xuv700)\s+features$/i.test(
        String(userMessage || "").trim(),
      );

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
    contextPatch: runtimeData.contextPatch || {},
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

/* -------------------------------------------------------------------------- */
/*  Main Executor                                                             */
/* -------------------------------------------------------------------------- */

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

  const executablePlan = planValidation.plan || sanitizedPlan;
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
        error: item.error || "",
      })),
      contractValidation,
    },
  };
};

export const executeAciPlannerTools = executeAciPlannerPlan;

export const runAciExecutor = executeAciPlannerPlan;

export default executeAciPlannerPlan;
