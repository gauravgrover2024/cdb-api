import {
  asArray,
  cleanText,
  firstMeaningful,
  getToolBrand,
  getToolCity,
  getToolModel,
  getToolVariant,
  normalizeVehicleRow,
  safeJsonText,
  searchKey,
  unique,
} from "./normalizers.js";

/**
 * Shared matching/query helpers for ACI Assist V2 tools.
 */

export const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const regexFor = (value = "") => {
  const text = cleanText(value);
  if (!text) return null;
  return new RegExp(escapeRegex(text), "i");
};

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
      { model_normalized: regex },
      { search_text: regex },
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
      { variant_short: regex },
      { variantShort: regex },
      { variant_normalized: regex },
      { variant: regex },
      { variantName: regex },
      { variant_name: regex },
      { vehicleVariant: regex },
      { trim: regex },
      { search_text: regex },
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
      { brand_normalized: regex },
      { search_text: regex },
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
  const model = getToolModel(toolPlan, context);
  const variant = getToolVariant(toolPlan, context);
  const brand = getToolBrand(toolPlan, context);
  const city = getToolCity(toolPlan, context);

  const and = [
    brandQuery(brand),
    modelQuery(model),
    variantQuery(variant),
    includeCity ? cityQuery(city) : null,
  ].filter(Boolean);

  if (!and.length) return {};

  return { $and: and };
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
    output = output.replace(new RegExp(`\\b${escapeRegex(term)}\\b`, "g"), " ");
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

  if (!requestedFull) return 0;

  const requestedTokens = requestedFull.split(" ").filter(Boolean);
  if (!requestedTokens.length) return 0;

  let best = 0;

  for (const value of variantCandidateValues(row)) {
    const candidateFull = normalizeVariantKeyForMatch(value);
    const candidateLoose = removeKnownVehicleTerms(value, row);
    const candidateKeys = unique([candidateFull, candidateLoose].filter(Boolean));

    for (const candidate of candidateKeys) {
      if (!candidate) continue;

      if (candidate === requestedFull) best = Math.max(best, 100);

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

      if (allRequestedTokensPresent) best = Math.max(best, 88);
    }
  }

  return best;
};

export const buildVariantResolution = ({
  requestedVariant = "",
  rows = [],
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
  const normalized = row.raw ? row : normalizeVehicleRow(row);

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
    const blob = safeJsonText([normalized.features, normalized.raw]);

    for (const feature of asArray(filters.mustHaveFeatures)) {
      if (!blob.includes(searchKey(feature))) return false;
    }
  }

  return true;
};

export const stripVariantFromToolPlan = (toolPlan = {}) => {
  const next = {
    ...toolPlan,
    entities: {
      ...(toolPlan.entities || {}),
    },
    filters: {
      ...(toolPlan.filters || {}),
    },
  };

  delete next.entities.variant;
  delete next.entities.primaryVariant;
  delete next.filters.variant;

  return next;
};

export const firstMeaningfulVariant = (...values) => firstMeaningful(...values);
