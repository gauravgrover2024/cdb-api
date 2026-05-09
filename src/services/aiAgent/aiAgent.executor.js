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
  "vehicle_colors",
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

export const listDbCollections = async (db) => {
  if (!db) return [];

  try {
    return await db.listCollections().toArray();
  } catch {
    return [];
  }
};

export const findCollectionName = async (db, candidates = []) => {
  const collections = await listDbCollections(db);
  const names = collections.map((item) => item.name).filter(Boolean);

  if (!names.length) return "";

  const normalizedNames = names.map((name) => ({
    name,
    key: searchKey(name),
  }));

  for (const candidate of candidates) {
    const candidateKey = searchKey(candidate);
    if (!candidateKey) continue;

    const exact = normalizedNames.find((item) => item.key === candidateKey);
    if (exact) return exact.name;

    const contains = normalizedNames.find(
      (item) => item.key.includes(candidateKey) || candidateKey.includes(item.key),
    );
    if (contains) return contains.name;
  }

  return normalizedNames[0]?.name || "";
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
} = {}) => {
  const { collection, collectionName, reason } = await getCollection(
    VEHICLE_COLLECTION_CANDIDATES,
  );

  const shouldIgnoreContextVariant = toolPlan.tool === "vehicle_pricelist";

  const queryToolPlan = shouldIgnoreContextVariant
    ? {
        ...toolPlan,
        entities: {
          ...(toolPlan.entities || {}),
          variant: "",
          primaryVariant: "",
        },
        filters: {
          ...(toolPlan.filters || {}),
          variant: "",
        },
      }
    : toolPlan;

  const queryContext = shouldIgnoreContextVariant
    ? {
        ...(context || {}),
        anchorVariant: "",
        variant: "",
        selectedVehicle: {
          ...((context || {}).selectedVehicle || {}),
          variant: "",
        },
      }
    : context;

  const requestedVariant = shouldIgnoreContextVariant
    ? ""
    : getVariant(toolPlan, context);
  const requestedVariantKey = searchKey(requestedVariant);

  const fastQuery = buildFastVehiclesQuery({
    toolPlan: queryToolPlan,
    context: queryContext,
  });
  const fallbackRegexQuery = buildVehicleMongoQuery({
    toolPlan: queryToolPlan,
    context: queryContext,
  });
  let rawRows = await safeFind(collection, fastQuery, {
    limit: DEFAULT_LIMITS.pricelist,
  });
  if (!rawRows.length) {
    rawRows = await safeFind(collection, fallbackRegexQuery, {
      limit: DEFAULT_LIMITS.pricelist,
    });
  }

  // Exact variant query can be too strict because DB may store full names,
  // short names, normalized names, or old naming. If strict query returns
  // nothing, fetch model rows and resolve variant in JS.
  if (requestedVariant && rawRows.length === 0) {
    const modelOnlyToolPlan = {
      ...queryToolPlan,
      entities: {
        ...(queryToolPlan.entities || {}),
      },
      filters: {
        ...(queryToolPlan.filters || {}),
      },
    };

    delete modelOnlyToolPlan.entities.variant;
    delete modelOnlyToolPlan.entities.primaryVariant;
    delete modelOnlyToolPlan.filters.variant;

    const modelOnlyFastQuery = buildFastVehiclesQuery({
      toolPlan: modelOnlyToolPlan,
      context: queryContext,
    });
    rawRows = await safeFind(collection, modelOnlyFastQuery, {
      limit: DEFAULT_LIMITS.pricelist,
    });
    if (!rawRows.length) {
      rawRows = await safeFind(
        collection,
        buildVehicleMongoQuery({ toolPlan: modelOnlyToolPlan, context: queryContext }),
        {
          limit: DEFAULT_LIMITS.pricelist,
        },
      );
    }
  }

  let normalizedRows = rawRows.map(normalizeVehicleRow);
  let matchedVariantRows = [];

  if (requestedVariantKey) {
    const scoredRows = normalizedRows
      .map((row) => ({
        row,
        score: variantMatchScore(row, requestedVariant),
      }))
      .filter((item) => item.score >= 88)
      .sort((a, b) => b.score - a.score);

    matchedVariantRows = scoredRows.map((item) => item.row);

    // Important: if variant was requested and not found, do NOT silently return
    // all model rows. Return empty rows + candidate variants so UI can ask.
    normalizedRows = matchedVariantRows;
  }

  const rows = sortPriceRows(
    normalizedRows.filter((row) =>
      rowMatchesFilters(row.raw || row, {
        ...(toolPlan.filters || {}),
        variant: "",
      }),
    ),
    toolPlan.ranking || "",
  );

  const allCandidateRows = rawRows.map(normalizeVehicleRow);

  return {
    rows,
    candidateRows: requestedVariantKey ? allCandidateRows.slice(0, 24) : [],
    candidateVariants: requestedVariantKey
      ? unique(
          allCandidateRows
            .map((row) => row.variant || row.variantShort || row.variantNormalized)
            .filter(Boolean),
        ).slice(0, 24)
      : [],
    variantResolution: buildVariantResolution({
      requestedVariant,
      rows: allCandidateRows,
      matchedRows: rows,
      status: requestedVariant ? (rows.length ? "matched" : "not_found") : "not_required",
    }),
    count: rows.length,
    matched: rows.length,
    modulesChecked: [collectionName || reason || "vehicle_pricelist"],
    source: collectionName || "none",
    dataSource: collectionName ? "mongodb" : "empty",
    summary: buildPriceSummary(rows),
  };
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
    modulesChecked: [collectionName || reason || "vehicle_colors"],
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

export const runtimeVehicleCompare = async ({
  toolPlan = {},
  context = {},
} = {}) => {
  const models = getModels(toolPlan, context);

  if (models.length <= 1) {
    const data = await runtimeVehiclePricelist({ toolPlan, context });
    return {
      ...data,
      rows: data.rows.slice(0, 4),
      comparisonLevel: toolPlan.resolution?.comparisonLevel || "model",
    };
  }

  const rows = [];

  for (const model of models) {
    const isVariantComparison =
      toolPlan.resolution?.comparisonLevel === "variant" ||
      toolPlan.output?.canvasType === "variant_comparison_canvas";

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

    if (!isVariantComparison) {
      delete modelTool.entities.variant;
      delete modelTool.entities.primaryVariant;
      delete modelTool.filters.variant;
    }

    const comparisonContext = isVariantComparison
      ? context
      : {
          ...(context || {}),
          anchorVariant: "",
          variant: "",
          selectedVehicle: {
            ...((context || {}).selectedVehicle || {}),
            model,
            variant: "",
          },
        };

    const data = await runtimeVehiclePricelist({
      toolPlan: modelTool,
      context: comparisonContext,
    });

    rows.push(
      data.rows[0] || {
        model,
        unavailable: true,
        variantResolution: data.variantResolution || null,
        candidateVariants: data.candidateVariants || [],
      },
    );
  }

  return {
    rows,
    count: rows.length,
    matched: rows.filter((row) => !row.unavailable).length,
    selectedComparisonSet: {
      models,
      variantSelectionMode:
        toolPlan.resolution?.variantSelectionMode || "representative_default",
    },
    modulesChecked: ["vehicle_compare", "vehicle_pricelist"],
    dataSource: "executor_composed",
  };
};

export const runtimeVehicleRecommend = async ({
  toolPlan = {},
  context = {},
} = {}) => {
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

/* -------------------------------------------------------------------------- */
/*  Runtime Tool Registry                                                     */
/* -------------------------------------------------------------------------- */

export const ACI_RUNTIME_DATA_TOOLS = {
  vehicle_pricelist: runtimeVehiclePricelist,
  vehicle_colors: runtimeVehicleColors,
  vehicle_feature_lookup: runtimeVehicleFeatureLookup,
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

  let response = buildAciAssistResponseFromPlan({
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
