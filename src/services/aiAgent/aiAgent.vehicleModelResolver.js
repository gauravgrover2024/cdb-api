let cachedModelIndex = null;
let cachedVariantIndexByModelKey = null;
let cachedAt = 0;

const CACHE_TTL_MS = 10 * 60 * 1000;

const normalizeText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactText = (value = "") => normalizeText(value).replace(/\s+/g, "");

const squashRepeatedLetters = (value = "") =>
  String(value || "").replace(/([a-z0-9])\1+/gi, "$1");

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const isBadVehicleText = (value = "") => {
  const text = String(value || "").trim();
  if (!text) return true;
  if (text.length > 90) return true;
  if (/function\s*\$?model|return\s+this|constructor|modelDbSymbol/i.test(text)) return true;
  return false;
};

const titleSpecial = (value = "") => {
  const text = String(value || "").trim();
  if (!text) return "";

  const compact = compactText(text);
  if (compact === "xuv700" || compact === "xuv7xo") return "XUV 7XO";
  if (compact === "xuv300" || compact === "xuv3xo") return "XUV 3XO";
  if (compact === "xuv400") return "XUV400";
  if (compact === "mgzs") return "ZS";
  if (compact === "ev6") return "EV6";
  if (compact === "eqs") return "EQS";

  return text
    .split(/\s+/)
    .map((word) => {
      if (/^[A-Z0-9()]+$/.test(word)) return word;
      if (/^\d/.test(word)) return word.toUpperCase();
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");
};

const damerauLevenshtein = (a = "", b = "") => {
  const s = String(a || "");
  const t = String(b || "");

  const rows = s.length + 1;
  const cols = t.length + 1;
  const dp = Array.from({ length: rows }, () => Array(cols).fill(0));

  for (let i = 0; i < rows; i += 1) dp[i][0] = i;
  for (let j = 0; j < cols; j += 1) dp[0][j] = j;

  for (let i = 1; i < rows; i += 1) {
    for (let j = 1; j < cols; j += 1) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;

      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost,
      );

      if (
        i > 1 &&
        j > 1 &&
        s[i - 1] === t[j - 2] &&
        s[i - 2] === t[j - 1]
      ) {
        dp[i][j] = Math.min(dp[i][j], dp[i - 2][j - 2] + 1);
      }
    }
  }

  return dp[s.length][t.length];
};

const stripLeadingMake = (value = "", make = "") => {
  let text = String(value || "").trim();
  const brand = String(make || "").trim();

  if (!text || !brand) return text;

  const textNorm = normalizeText(text);
  const brandNorm = normalizeText(brand);

  if (textNorm === brandNorm) return text;

  if (textNorm.startsWith(`${brandNorm} `)) {
    const brandWords = brandNorm.split(" ").length;
    return text.split(/\s+/).slice(brandWords).join(" ").trim();
  }

  return text;
};

const stripLeadingModel = ({ value = "", make = "", model = "", fullModel = "" } = {}) => {
  let text = String(value || "").trim();
  if (!text) return "";

  const candidates = unique([
    fullModel,
    `${make} ${model}`,
    model,
  ]).sort((a, b) => normalizeText(b).length - normalizeText(a).length);

  for (const candidate of candidates) {
    const candidateNorm = normalizeText(candidate);
    const textNorm = normalizeText(text);
    if (!candidateNorm) continue;

    if (textNorm === candidateNorm) return "";

    if (textNorm.startsWith(`${candidateNorm} `)) {
      const words = candidateNorm.split(" ").length;
      return text.split(/\s+/).slice(words).join(" ").trim();
    }
  }

  text = stripLeadingMake(text, make);
  return text;
};

const makeModelEntry = ({
  brand = "",
  modelValue = "",
  modelKey = "",
  source = "",
} = {}) => {
  if (isBadVehicleText(modelValue)) return null;

  const cleanBrand = titleSpecial(String(brand || "").trim());
  const rawModel = titleSpecial(String(modelValue || "").trim());

  const shortModel = titleSpecial(stripLeadingMake(rawModel, cleanBrand));
  if (isBadVehicleText(shortModel)) return null;

  const fullModel =
    cleanBrand && !normalizeText(rawModel).startsWith(normalizeText(cleanBrand))
      ? `${cleanBrand} ${shortModel}`.trim()
      : rawModel;

  const shortModelKey = compactText(shortModel);
  const fullModelKey = compactText(fullModel);

  return {
    brand: cleanBrand,
    model: shortModel,
    fullModel,
    modelKey: modelKey ? compactText(stripLeadingMake(modelKey, cleanBrand)) || shortModelKey : shortModelKey,
    shortModelKey,
    fullModelKey,
    sourceModel: rawModel,
    source,
    aliases: unique([
      normalizeText(shortModel),
      compactText(shortModel),
      squashRepeatedLetters(compactText(shortModel)),
      normalizeText(fullModel),
      compactText(fullModel),
      squashRepeatedLetters(compactText(fullModel)),
      cleanBrand ? normalizeText(`${cleanBrand} ${shortModel}`) : "",
      cleanBrand ? compactText(`${cleanBrand} ${shortModel}`) : "",
    ]),
  };
};

const makeVariantEntry = ({
  brand = "",
  model = "",
  fullModel = "",
  variantValue = "",
  source = "",
} = {}) => {
  if (isBadVehicleText(variantValue)) return null;

  const rawVariant = titleSpecial(String(variantValue || "").trim());

  const shortVariant = titleSpecial(
    stripLeadingModel({
      value: rawVariant,
      make: brand,
      model,
      fullModel,
    }),
  );

  if (isBadVehicleText(shortVariant)) return null;

  const fullVariant =
    normalizeText(rawVariant).startsWith(normalizeText(fullModel))
      ? rawVariant
      : `${fullModel} ${shortVariant}`.trim();

  const variantKey = compactText(shortVariant);
  const fullVariantKey = compactText(fullVariant);

  return {
    brand,
    model,
    fullModel,
    variant: shortVariant,
    fullVariant,
    variantKey,
    fullVariantKey,
    sourceVariant: rawVariant,
    source,
    aliases: unique([
      normalizeText(shortVariant),
      compactText(shortVariant),
      squashRepeatedLetters(compactText(shortVariant)),
      normalizeText(fullVariant),
      compactText(fullVariant),
      squashRepeatedLetters(compactText(fullVariant)),
      normalizeText(`${model} ${shortVariant}`),
      compactText(`${model} ${shortVariant}`),
      normalizeText(`${fullModel} ${shortVariant}`),
      compactText(`${fullModel} ${shortVariant}`),
    ]),
  };
};

const dedupeModelEntries = (entries = []) => {
  const map = new Map();

  for (const entry of entries) {
    if (!entry?.model) continue;

    const key = `${compactText(entry.brand)}:${entry.shortModelKey}`;
    const existing = map.get(key);

    if (!existing) {
      map.set(key, entry);
      continue;
    }

    map.set(key, {
      ...existing,
      modelKey: existing.modelKey || entry.modelKey,
      fullModel: existing.fullModel || entry.fullModel,
      aliases: unique([...(existing.aliases || []), ...(entry.aliases || [])]),
      source: unique([existing.source, entry.source]).join("+"),
    });
  }

  return [...map.values()].sort((a, b) => b.model.length - a.model.length);
};

const safeAggregate = async (db, collectionName, pipeline = []) => {
  try {
    const exists = await db.listCollections({ name: collectionName }).hasNext();
    if (!exists) return [];
    return await db.collection(collectionName).aggregate(pipeline).toArray();
  } catch {
    return [];
  }
};

export const loadVehicleModelIndex = async ({ db, force = false } = {}) => {
  if (!db) throw new Error("MongoDB db is required for vehicle model resolver.");

  const now = Date.now();
  if (!force && cachedModelIndex && now - cachedAt < CACHE_TTL_MS) {
    return cachedModelIndex;
  }

  const entries = [];

  const matrixRows = await safeAggregate(db, "vehicle_variant_feature_matrix_v2", [
    {
      $group: {
        _id: "$modelKey",
        model: { $first: "$model" },
        brand: { $first: "$brand" },
      },
    },
    { $match: { model: { $nin: [null, ""] } } },
  ]);

  for (const row of matrixRows) {
    const entry = makeModelEntry({
      brand: row.brand,
      modelValue: row.model,
      modelKey: row._id,
      source: "vehicle_variant_feature_matrix_v2",
    });
    if (entry) entries.push(entry);
  }

  const vehicleRows = await safeAggregate(db, "vehicles", [
    {
      $group: {
        _id: {
          modelKey: { $ifNull: ["$modelKey", "$model_slug"] },
          model: { $ifNull: ["$model", "$modelName"] },
        },
        model: { $first: { $ifNull: ["$model", "$modelName"] } },
        brand: { $first: { $ifNull: ["$brand", "$make"] } },
      },
    },
    { $match: { model: { $nin: [null, ""] } } },
  ]);

  for (const row of vehicleRows) {
    const entry = makeModelEntry({
      brand: row.brand,
      modelValue: row.model,
      modelKey: row._id?.modelKey,
      source: "vehicles",
    });
    if (entry) entries.push(entry);
  }

  const masterRows = await safeAggregate(db, "vehicle_master_records", [
    {
      $group: {
        _id: {
          modelKey: { $ifNull: ["$model_slug", "$modelKey"] },
          model: "$model",
        },
        model: { $first: "$model" },
        brand: { $first: { $ifNull: ["$brand", "$make"] } },
      },
    },
    { $match: { model: { $nin: [null, ""] } } },
  ]);

  for (const row of masterRows) {
    const entry = makeModelEntry({
      brand: row.brand,
      modelValue: row.model,
      modelKey: row._id?.modelKey,
      source: "vehicle_master_records",
    });
    if (entry) entries.push(entry);
  }

  cachedModelIndex = dedupeModelEntries(entries);
  cachedAt = now;

  return cachedModelIndex;
};

export const loadVehicleVariantIndexByModelKey = async ({
  db,
  force = false,
} = {}) => {
  if (!db) throw new Error("MongoDB db is required for vehicle variant resolver.");

  const now = Date.now();
  if (!force && cachedVariantIndexByModelKey && now - cachedAt < CACHE_TTL_MS) {
    return cachedVariantIndexByModelKey;
  }

  const modelIndex = await loadVehicleModelIndex({ db, force });
  const modelByPossibleKey = new Map();

  for (const model of modelIndex) {
    for (const key of unique([model.modelKey, model.shortModelKey, model.fullModelKey])) {
      modelByPossibleKey.set(key, model);
    }
  }

  const rows = await safeAggregate(db, "vehicle_variant_feature_matrix_v2", [
    {
      $project: {
        brand: 1,
        model: 1,
        modelKey: 1,
        variant: 1,
        variantKey: 1,
        activePricelistMatched: 1,
      },
    },
    { $match: { variant: { $nin: [null, ""] } } },
  ]);

  const byModelKey = new Map();

  for (const row of rows) {
    const modelEntry =
      modelByPossibleKey.get(compactText(row.modelKey)) ||
      makeModelEntry({
        brand: row.brand,
        modelValue: row.model,
        modelKey: row.modelKey,
        source: "vehicle_variant_feature_matrix_v2",
      });

    if (!modelEntry) continue;

    const variantEntry = makeVariantEntry({
      brand: modelEntry.brand,
      model: modelEntry.model,
      fullModel: modelEntry.fullModel,
      variantValue: row.variant,
      source: "vehicle_variant_feature_matrix_v2",
    });

    if (!variantEntry) continue;

    const key = modelEntry.shortModelKey;
    const current = byModelKey.get(key) || [];

    current.push({
      ...variantEntry,
      activePricelistMatched: row.activePricelistMatched === true,
    });

    byModelKey.set(key, current);
  }

  cachedVariantIndexByModelKey = byModelKey;
  return cachedVariantIndexByModelKey;
};

const buildMessageNgrams = (message = "") => {
  const normalized = normalizeText(message);
  const tokens = normalized.split(" ").filter(Boolean);

  const stopWords = new Set([
    "does", "do", "is", "are", "have", "has", "come", "with", "in", "me",
    "hai", "kya", "which", "variant", "variants", "feature", "features",
    "featuers", "show", "open", "list", "all", "price", "pricelist", "on",
    "road", "cheapest", "available", "get", "gets", "of", "the", "and", "or",
    "difference", "between", "compare", "comparison", "extra", "over",

    // Feature/value/advisor words must never become vehicle model candidates.
    // Example: "Which variant has best mileage?" should use context Creta,
    // not fuzzy-match "best" -> Chevrolet Beat.
    "best", "better", "top", "highest", "maximum", "max", "most",
    "mileage", "average", "fuel", "efficiency", "kmpl", "kpl",
    "worth", "upgrade", "buy", "family", "rear", "seat", "night", "driving",
  ]);

  const cleanTokens = tokens.filter((token) => !stopWords.has(token));
  const grams = [];

  for (let size = Math.min(5, cleanTokens.length); size >= 1; size -= 1) {
    for (let i = 0; i <= cleanTokens.length - size; i += 1) {
      grams.push(cleanTokens.slice(i, i + size).join(" "));
    }
  }

  return unique([normalized, ...grams]);
};


const getMatchPriority = (method = "") => {
  if (method === "exact_or_repeated_letter_fix") return 4;
  if (method === "contains_entity") return 3;
  if (method === "partial_entity") return 2;
  if (method === "fuzzy_edit_distance") return 1;
  return 0;
};

const getExactHitLengthInMessage = ({ message = "", keys = [] } = {}) => {
  const messageCompact = compactText(message);
  let best = 0;

  for (const key of keys) {
    const compactKey = compactText(key);
    if (!compactKey) continue;
    if (messageCompact.includes(compactKey)) {
      best = Math.max(best, compactKey.length);
    }
  }

  return best;
};

const scoreEntityCandidate = ({ gram = "", entry } = {}) => {
  const gramNorm = normalizeText(gram);
  const gramCompact = compactText(gramNorm);
  const gramSquashed = squashRepeatedLetters(gramCompact);

  if (!gramCompact || !entry?.aliases?.length) return null;

  // Never allow a brand-only token like "hyundai" or "kia" to resolve as a model.
  // Example: "hyundai vrna" must resolve from "vrna" -> Verna, not "hyundai" -> any Hyundai model.
  const brandCompact = compactText(entry.brand || "");
  if (brandCompact && gramCompact === brandCompact) {
    return null;
  }

  let best = null;

  for (const alias of entry.aliases) {
    const aliasNorm = normalizeText(alias);
    const aliasCompact = compactText(aliasNorm);
    const aliasSquashed = squashRepeatedLetters(aliasCompact);

    if (!aliasCompact) continue;

    let score = 0;
    let method = "";

    if (gramCompact === aliasCompact || gramSquashed === aliasSquashed) {
      score = 1;
      method = "exact_or_repeated_letter_fix";
    } else if (gramCompact.includes(aliasCompact) && aliasCompact.length >= 5) {
      score = 0.96;
      method = "contains_entity";
    } else if (aliasCompact.includes(gramCompact) && gramCompact.length >= 4) {
      score = 0.90;
      method = "partial_entity";
    } else {
      const distance = damerauLevenshtein(gramSquashed, aliasSquashed);
      const maxLen = Math.max(gramSquashed.length, aliasSquashed.length);
      if (maxLen < 4) continue;

      const allowedDistance =
        maxLen <= 4 ? 1 :
        maxLen <= 6 ? 2 :
        Math.max(2, Math.floor(maxLen * 0.25));

      if (distance <= allowedDistance) {
        score = 1 - distance / maxLen;
        method = "fuzzy_edit_distance";

        if (gramSquashed[0] === aliasSquashed[0]) {
          score += 0.08;
        }
      }
    }

    if (!score) continue;

    const candidate = {
      ...entry,
      matchedText: gram,
      confidence: Math.min(1, Number(score.toFixed(3))),
      rawScore: Number(score.toFixed(3)),
      method,
      matchPriority: getMatchPriority(method),
      corrected: gramCompact !== aliasCompact && gramSquashed !== aliasSquashed,
    };

    if (!best || candidate.confidence > best.confidence) {
      best = candidate;
    }
  }

  return best;
};

export const resolveVehicleModelFromText = async ({
  db,
  message = "",
  minConfidence = 0.76,
} = {}) => {
  const index = await loadVehicleModelIndex({ db });
  const grams = buildMessageNgrams(message);

  const candidates = [];

  for (const gram of grams) {
    for (const entry of index) {
      const scored = scoreEntityCandidate({ gram, entry });
      if (scored) candidates.push(scored);
    }
  }

  const sorted = candidates
    .filter((candidate) => candidate.confidence >= minConfidence)
    .sort((a, b) => {
      const aExactHit = getExactHitLengthInMessage({
        message,
        keys: [a.model, a.fullModel, a.modelKey, a.shortModelKey, a.fullModelKey],
      });
      const bExactHit = getExactHitLengthInMessage({
        message,
        keys: [b.model, b.fullModel, b.modelKey, b.shortModelKey, b.fullModelKey],
      });

      if (bExactHit !== aExactHit) return bExactHit - aExactHit;
      if ((b.matchPriority || 0) !== (a.matchPriority || 0)) {
        return (b.matchPriority || 0) - (a.matchPriority || 0);
      }
      if (b.confidence !== a.confidence) return b.confidence - a.confidence;
      return b.model.length - a.model.length;
    });

  return sorted[0] || null;
};

export const resolveVehicleVariantFromText = async ({
  db,
  modelKey = "",
  message = "",
  minConfidence = 0.72,
} = {}) => {
  const byModelKey = await loadVehicleVariantIndexByModelKey({ db });
  const variants = byModelKey.get(compactText(modelKey)) || [];

  if (!variants.length) return null;

  const grams = buildMessageNgrams(message);
  const candidates = [];

  for (const gram of grams) {
    for (const entry of variants) {
      const scored = scoreEntityCandidate({ gram, entry });
      if (scored) candidates.push(scored);
    }
  }

  const sorted = candidates
    .filter((candidate) => candidate.confidence >= minConfidence)
    .sort((a, b) => {
      const aExactHit = getExactHitLengthInMessage({
        message,
        keys: [a.variant, a.fullVariant, a.variantKey, a.fullVariantKey],
      });
      const bExactHit = getExactHitLengthInMessage({
        message,
        keys: [b.variant, b.fullVariant, b.variantKey, b.fullVariantKey],
      });

      // Critical rule:
      // "creta ex diesel" must resolve to EX Diesel, not fuzzy-match EX (O) Diesel.
      if (bExactHit !== aExactHit) return bExactHit - aExactHit;

      if ((b.matchPriority || 0) !== (a.matchPriority || 0)) {
        return (b.matchPriority || 0) - (a.matchPriority || 0);
      }

      if (b.confidence !== a.confidence) return b.confidence - a.confidence;

      // If both are equally exact, prefer the more specific variant.
      return b.variant.length - a.variant.length;
    });

  return sorted[0] || null;
};

export default {
  loadVehicleModelIndex,
  loadVehicleVariantIndexByModelKey,
  resolveVehicleModelFromText,
  resolveVehicleVariantFromText,
};
