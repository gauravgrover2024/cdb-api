import { normalizeSearchKey } from "./aiAgent.planSchema.js";

export const containsAlias = (textKey = "", aliasKey = "") => {
  if (!textKey || !aliasKey) return false;

  const escaped = aliasKey.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const pattern = new RegExp(
    `(^|\\s)${escaped.replace(/\s+/g, "\\s+")}(\\s|$)`,
    "i",
  );

  return pattern.test(textKey);
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
