import { normalizeSearchKey } from "./aiAgent.planSchema.js";

const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

export const normalizeCompactAlphaNumKey = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/([a-z])\s+([0-9])/g, "$1$2")
    .replace(/([0-9])\s+([a-z])/g, "$1$2");

const containsAliasExact = (textKey = "", aliasKey = "") => {
  if (!textKey || !aliasKey) return false;

  const escaped = escapeRegex(aliasKey);
  const pattern = new RegExp(
    `(^|\\s)${escaped.replace(/\s+/g, "\\s+")}(\\s|$)`,
    "i",
  );

  return pattern.test(textKey);
};

export const containsAlias = (textKey = "", aliasKey = "") => {
  if (containsAliasExact(textKey, aliasKey)) return true;

  const compactTextKey = normalizeCompactAlphaNumKey(textKey);
  const compactAliasKey = normalizeCompactAlphaNumKey(aliasKey);

  if (!compactTextKey || !compactAliasKey || compactTextKey === textKey && compactAliasKey === aliasKey) {
    return false;
  }

  return containsAliasExact(compactTextKey, compactAliasKey);
};

export const isGenericCityUse = (textKey = "", aliasKey = "") => {
  if (aliasKey !== "city") return false;

  return /\b(in|my|your|current|this)\s+city\b/.test(textKey);
};

export const findModelMatches = (
  index,
  message = "",
  { includeGeneric = false } = {},
) => {
  const textKey = normalizeSearchKey(message);
  const matches = [];

  for (const item of index.modelAliases || []) {
    if (!includeGeneric && isGenericCityUse(textKey, item.aliasKey)) continue;

    if (containsAlias(textKey, item.aliasKey)) {
      matches.push({
        ...item.model,
        matchedAlias: item.alias,
        confidence: item.aliasKey === item.model.shortModelKey ? 0.94 : 0.98,
      });
    }
  }

  const seen = new Set();

  return matches
    .sort((left, right) => {
      const leftAliasLength = normalizeSearchKey(left.matchedAlias || "").length;
      const rightAliasLength = normalizeSearchKey(right.matchedAlias || "").length;
      if (rightAliasLength !== leftAliasLength) return rightAliasLength - leftAliasLength;
      return (right.confidence || 0) - (left.confidence || 0);
    })
    .filter((item) => {
    const key = item.modelKey || item.shortModelKey;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

export const findVariantMatches = (
  index,
  message = "",
  { model = "", brand = "" } = {},
) => {
  const textKey = normalizeSearchKey(message);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);
  const shortModelKey = normalizeSearchKey(model);

  const matches = [];

  for (const item of index.variantAliases || []) {
    const variant = item.variant;

    if (model || brand) {
      const sameModel =
        variant.shortModelKey === shortModelKey ||
        variant.modelKey === modelKey ||
        normalizeSearchKey(variant.model) === shortModelKey;

      if (!sameModel) continue;
    }

    if (containsAlias(textKey, item.aliasKey)) {
      matches.push({
        ...variant,
        matchedAlias: item.alias,
        confidence: item.aliasKey === variant.shortVariantKey ? 0.9 : 0.96,
      });
    }
  }

  const seen = new Set();

  return matches.filter((item) => {
    const key = item.variantKey || `${item.modelKey}:${item.shortVariantKey}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

export const findColorMatches = (
  index,
  message = "",
  { model = "", brand = "" } = {},
) => {
  const textKey = normalizeSearchKey(message);
  const modelKey = normalizeSearchKey(`${brand} ${model}`);
  const shortModelKey = normalizeSearchKey(model);
  const matches = [];

  for (const item of index.colorAliases || []) {
    const color = item.color;

    if (model || brand) {
      const sameModel =
        color.shortModelKey === shortModelKey ||
        color.modelKey === modelKey ||
        normalizeSearchKey(color.model) === shortModelKey;

      if (!sameModel) continue;
    }

    if (containsAlias(textKey, item.aliasKey)) {
      matches.push({
        ...color,
        matchedAlias: item.alias,
        confidence: item.aliasKey === color.shortColorKey ? 0.9 : 0.96,
      });
    }
  }

  const seen = new Set();

  return matches.filter((item) => {
    const key = item.colorKey || `${item.modelKey}:${item.shortColorKey}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};
