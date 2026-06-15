import { buildSearchTokens } from "./searchTokens.js";

const MARKETING_WORDS_PATTERN =
  /\b(all\s+new|new|facelift|20\d{2})\b/gi;

const ACRONYMS = new Set([
  "ADAS",
  "AMT",
  "AT",
  "AWD",
  "BMW",
  "BYD",
  "CNG",
  "CVT",
  "DBX",
  "DCT",
  "DT",
  "EV",
  "GT",
  "GTX",
  "HTE",
  "HTK",
  "HTX",
  "IVT",
  "LXI",
  "MG",
  "MT",
  "N",
  "RWD",
  "SUV",
  "SX",
  "SXO",
  "TDI",
  "TFSI",
  "TSI",
  "VXI",
  "XUV",
  "XZ",
  "XZA",
  "ZXI",
]);

export const normalizeSpaces = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const escapeRegex = (value = "") =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const removeMarketingWords = (value = "") =>
  normalizeSpaces(String(value || "").replace(MARKETING_WORDS_PATTERN, " "));

const lowerKey = (value = "") =>
  normalizeSpaces(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();

const stripLeadingPrefix = (value = "", prefix = "") => {
  const source = normalizeSpaces(value);
  const leader = normalizeSpaces(prefix);
  if (!source || !leader) return source;
  const pattern = new RegExp(`^${escapeRegex(leader)}(?:\\s+|$)`, "i");
  return normalizeSpaces(source.replace(pattern, ""));
};

const stripLeadingPrefixes = (value = "", prefixes = []) => {
  let output = normalizeSpaces(value);
  const orderedPrefixes = [...new Set(prefixes.map(normalizeSpaces).filter(Boolean))]
    .sort((a, b) => b.length - a.length);

  let changed = true;
  while (changed) {
    changed = false;
    for (const prefix of orderedPrefixes) {
      const next = stripLeadingPrefix(output, prefix);
      if (next !== output) {
        output = next;
        changed = true;
      }
    }
  }
  return output;
};

const brandPrefixCandidates = (brand = "") => {
  const raw = normalizeSpaces(brand);
  const words = raw.split(" ").filter(Boolean);
  const candidates = [raw];
  if (words.length > 1) candidates.push(words[0]);
  const key = lowerKey(raw);
  if (key === "maruti suzuki") candidates.push("Maruti");
  if (key === "mercedes benz") candidates.push("Mercedes", "Benz");
  return [...new Set(candidates.filter(Boolean))];
};

const smartCaseToken = (token = "") => {
  const raw = String(token || "").trim();
  if (!raw) return "";
  const alphaNum = raw.replace(/[^a-z0-9]/gi, "");
  const upper = alphaNum.toUpperCase();
  if (ACRONYMS.has(upper)) return upper;
  if (/^\d/.test(raw) || /\d/.test(raw)) return raw.toUpperCase();
  if (raw.length <= 2 && raw === raw.toUpperCase()) return raw;
  return raw.charAt(0).toUpperCase() + raw.slice(1).toLowerCase();
};

export const toTitleCase = (value = "") =>
  normalizeSpaces(value)
    .split(" ")
    .map((token) => {
      if (!token.includes("-")) return smartCaseToken(token);
      return token
        .split("-")
        .map(smartCaseToken)
        .join("-");
    })
    .join(" ")
    .trim();

const normalizeTrimName = (value = "") => toTitleCase(removeMarketingWords(value));

export const normalizeColorName = (value = "") => normalizeTrimName(value);

export const normalizeColors = (colors = []) => {
  const seen = new Set();
  const normalized = [];

  for (const color of colors || []) {
    const value =
      typeof color === "string"
        ? color
        : color?.colorName || color?.color_name || color?.name || color?.label;
    const name = normalizeColorName(value);
    const key = lowerKey(name);
    if (!name || seen.has(key)) continue;
    seen.add(key);
    normalized.push(name);
  }

  return normalized.sort((a, b) => a.localeCompare(b));
};

export const normalizeVehicleDatasetRow = (row = {}, options = {}) => {
  const rawBrand = normalizeSpaces(row.brand || row.make);
  const brandNormalized = toTitleCase(rawBrand);
  const rawModel = normalizeSpaces(row.model);
  const rawVariant = normalizeSpaces(row.variant);
  const brandPrefixes = [
    ...brandPrefixCandidates(rawBrand),
    ...brandPrefixCandidates(brandNormalized),
  ];

  const modelWithoutBrand = stripLeadingPrefixes(rawModel, brandPrefixes);
  const modelNormalized =
    normalizeTrimName(modelWithoutBrand) || normalizeTrimName(rawModel);

  const variantWithoutPrefixes = stripLeadingPrefixes(rawVariant, [
    `${rawBrand} ${rawModel}`,
    `${brandNormalized} ${rawModel}`,
    `${rawBrand} ${modelNormalized}`,
    `${brandNormalized} ${modelNormalized}`,
    rawModel,
    modelNormalized,
    ...brandPrefixes,
  ]);
  const variantNormalized =
    normalizeTrimName(variantWithoutPrefixes) || normalizeTrimName(rawVariant);

  const fuel = normalizeTrimName(row.fuel || row.fuel_type || row.fuelType);
  const transmission = normalizeTrimName(
    row.transmission || row.transmission_type || row.gearbox,
  );
  const searchText = [
    brandNormalized,
    modelNormalized,
    variantNormalized,
    fuel,
    transmission,
  ]
    .filter(Boolean)
    .join(" ")
    .toLowerCase();

  const colorsNormalized = normalizeColors(options.colors || row.colors_normalized);
  const searchTokens = buildSearchTokens([
    rawBrand,
    brandNormalized,
    rawModel,
    modelNormalized,
    rawVariant,
    variantNormalized,
    fuel,
    transmission,
    row.city,
    searchText,
  ]);

  return {
    brand: rawBrand,
    model: rawModel,
    variant: rawVariant,
    brand_normalized: brandNormalized,
    model_normalized: modelNormalized,
    variant_normalized: variantNormalized,
    search_text: searchText,
    searchTokens,
    ...(colorsNormalized.length ? { colors_normalized: colorsNormalized } : {}),
  };
};

export const buildVehicleNormalizationUpdate = (row = {}, options = {}) => {
  const normalized = normalizeVehicleDatasetRow(row, options);
  const $set = {
    brand_normalized: normalized.brand_normalized,
    model_normalized: normalized.model_normalized,
    variant_normalized: normalized.variant_normalized,
    search_text: normalized.search_text,
    searchTokens: normalized.searchTokens,
  };
  const $unset = {};

  if (normalized.colors_normalized?.length) {
    $set.colors_normalized = normalized.colors_normalized;
  } else {
    $unset.colors_normalized = "";
  }

  return Object.keys($unset).length ? { $set, $unset } : { $set };
};

export const vehicleNormalizationFields = (row = {}, options = {}) => {
  const update = buildVehicleNormalizationUpdate(row, options);
  return {
    ...update.$set,
    ...(update.$unset?.colors_normalized ? { colors_normalized: undefined } : {}),
  };
};

export const vehicleIdentityKey = ({ brand, model, variant } = {}) =>
  [brand, model, variant]
    .map((value) => lowerKey(value))
    .join("|");

export const vehicleModelKey = ({ brand, model } = {}) =>
  [brand, model]
    .map((value) => lowerKey(value))
    .join("|");
