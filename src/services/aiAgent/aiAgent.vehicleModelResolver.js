let cachedModelIndex = null;
let cachedVariantIndexByModelKey = null;
let cachedAt = 0;

const CACHE_TTL_MS = 10 * 60 * 1000;

const ACI_MODEL_SUMMARY_COLLECTION = "aci_vehicle_model_summary";
const ACI_PRICE_ROWS_COLLECTION = "aci_vehicle_price_rows";

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

const vehicleModelResolverCollectionExistsCache = new Map();

const collectionExistsCached = async (db, collectionName = "") => {
  if (!db || !collectionName) return false;

  const cached = vehicleModelResolverCollectionExistsCache.get(collectionName);

  if (cached && Date.now() - cached.cachedAt < CACHE_TTL_MS) {
    return cached.exists;
  }

  try {
    const exists = await db
      .listCollections({ name: collectionName }, { nameOnly: true })
      .hasNext();

    vehicleModelResolverCollectionExistsCache.set(collectionName, {
      exists,
      cachedAt: Date.now(),
    });

    return exists;
  } catch {
    return false;
  }
};

const safeAggregate = async (db, collectionName, pipeline = []) => {
  try {
    const exists = await collectionExistsCached(db, collectionName);
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

  // Fast path: use ACI Assist runtime read model first.
  // This avoids aggregating raw scraper/runtime collections during user requests.
  try {
    const readModelRows = await db
      .collection(ACI_MODEL_SUMMARY_COLLECTION)
      .find(
        {},
        {
          projection: {
            make: 1,
            model: 1,
            modelKey: 1,
            fullModel: 1,
            displayName: 1,
            citySlug: 1,
          },
        },
      )
      .sort({ modelKey: 1, citySlug: 1 })
      .hint("aci_model_summary_model_city")
      .batchSize(1200)
      .toArray();

    for (const row of readModelRows) {
      const entry = makeModelEntry({
        brand: row.make,
        modelValue: row.model || row.displayName || row.fullModel,
        modelKey: row.modelKey,
        source: ACI_MODEL_SUMMARY_COLLECTION,
      });

      if (entry) entries.push(entry);
    }

    if (entries.length) {
      cachedModelIndex = dedupeModelEntries(entries);
      cachedAt = now;
      return cachedModelIndex;
    }
  } catch {
    // Fall back to legacy source aggregation below.
  }

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

  // Fast path: use ACI Assist runtime price rows first.
  try {
    const readModelRows = await db
      .collection(ACI_PRICE_ROWS_COLLECTION)
      .find(
        {},
        {
          projection: {
            make: 1,
            model: 1,
            modelKey: 1,
            fullModel: 1,
            variant: 1,
            variantKey: 1,
          },
        },
      )
      .batchSize(10000)
      .toArray();

    const byModelKey = new Map();

    for (const row of readModelRows) {
      const modelEntry =
        modelByPossibleKey.get(compactText(row.modelKey)) ||
        makeModelEntry({
          brand: row.make,
          modelValue: row.model || row.fullModel,
          modelKey: row.modelKey,
          source: ACI_PRICE_ROWS_COLLECTION,
        });

      if (!modelEntry) continue;

      const variantEntry = makeVariantEntry({
        brand: modelEntry.brand || row.make,
        model: modelEntry.model || row.model,
        fullModel: modelEntry.fullModel || row.fullModel,
        variantValue: row.variant,
        source: ACI_PRICE_ROWS_COLLECTION,
      });

      if (!variantEntry) continue;

      const keys = unique([
        modelEntry.modelKey,
        modelEntry.shortModelKey,
        modelEntry.fullModelKey,
        compactText(row.modelKey),
      ]);

      for (const key of keys) {
        if (!key) continue;
        const list = byModelKey.get(key) || [];

        if (!list.some((item) => item.variantKey === variantEntry.variantKey)) {
          list.push(variantEntry);
        }

        byModelKey.set(key, list);
      }
    }

    if (byModelKey.size) {
      cachedVariantIndexByModelKey = byModelKey;
      cachedAt = now;
      return cachedVariantIndexByModelKey;
    }
  } catch {
    // Fall back to legacy feature matrix aggregation below.
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
      brand: modelEntry.brand || row.brand,
      model: modelEntry.model || row.model,
      fullModel: modelEntry.fullModel,
      variantValue: row.variant,
      source: "vehicle_variant_feature_matrix_v2",
    });

    if (!variantEntry) continue;

    const keys = unique([
      modelEntry.modelKey,
      modelEntry.shortModelKey,
      modelEntry.fullModelKey,
      compactText(row.modelKey),
    ]);

    for (const key of keys) {
      if (!key) continue;
      const list = byModelKey.get(key) || [];

      if (!list.some((item) => item.variantKey === variantEntry.variantKey)) {
        list.push(variantEntry);
      }

      byModelKey.set(key, list);
    }
  }

  cachedVariantIndexByModelKey = byModelKey;
  cachedAt = now;

  return cachedVariantIndexByModelKey;
};


const buildMessageNgrams = (message = "") => {
  const normalized = normalizeText(message);
  const tokens = normalized
    .split(/\s+/)
    .map((token) => token.trim())
    .filter(Boolean);

  const grams = new Set();

  for (let start = 0; start < tokens.length; start += 1) {
    for (let size = 1; size <= 5 && start + size <= tokens.length; size += 1) {
      grams.add(tokens.slice(start, start + size).join(" "));
    }
  }

  return [...grams].sort((a, b) => b.length - a.length);
};


const toResolverArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const getEntityCandidateTexts = (entry = {}) => {
  const values = [
    entry.matchedText,
    entry.alias,
    entry.name,
    entry.model,
    entry.modelName,
    entry.displayName,
    entry.fullModel,
    entry.fullName,
    entry.make,
    entry.brand,
    entry.makeName,
    entry.modelKey,
    entry.makeKey,
    entry.searchText,
    ...toResolverArray(entry.aliases),
    ...toResolverArray(entry.tokens),
    ...toResolverArray(entry.searchTokens),
    ...toResolverArray(entry.names),
  ];

  return [...new Set(
    values
      .map((value) => String(value || "").replace(/[-_]+/g, " ").trim())
      .filter(Boolean),
  )];
};


const ACI_MODEL_RESOLVER_GENERIC_NGRAMS = new Set([
  "show",
  "open",
  "tell",
  "check",
  "find",
  "list",
  "price",
  "prices",
  "pricelist",
  "price list",
  "on road",
  "on road price",
  "ex showroom",
  "ex showroom price",
  "emi",
  "loan",
  "offer",
  "offers",
  "discount",
  "quotation",
  "quote",
  "feature",
  "features",
  "color",
  "colors",
  "colour",
  "colours",
  "available",
  "availability",
  "black",
  "white",
  "red",
  "blue",
  "grey",
  "gray",
  "in",
  "for",
  "of",
  "the",
  "a",
  "an",
  "new",
  "car",
  "cars",
  "model",
  "variant",
  "variants",
  "delhi",
  "new delhi",
  "new-delhi",
]);

const isGenericVehicleModelResolverGram = (gram = "") => {
  const norm = normalizeText(gram);
  if (!norm) return true;
  if (norm.length < 3) return true;
  if (ACI_MODEL_RESOLVER_GENERIC_NGRAMS.has(norm)) return true;

  const tokens = norm.split(/\s+/).filter(Boolean);
  if (!tokens.length) return true;

  return tokens.every((token) => ACI_MODEL_RESOLVER_GENERIC_NGRAMS.has(token));
};

const scoreEntityCandidate = ({ gram = "", entry = {} } = {}) => {
  const gramNorm = normalizeText(gram);
  if (!gramNorm || isGenericVehicleModelResolverGram(gramNorm)) return null;

  const texts = getEntityCandidateTexts(entry);
  if (!texts.length) return null;

  let bestScore = 0;
  let bestText = "";

  for (const text of texts) {
    const textNorm = normalizeText(text);
    if (!textNorm) continue;

    let score = 0;

    if (textNorm === gramNorm) score += 120;
    else if (textNorm.replace(/\s+/g, "") === gramNorm.replace(/\s+/g, "")) score += 110;
    else if (textNorm.startsWith(`${gramNorm} `)) score += 80;
    else if (textNorm.endsWith(` ${gramNorm}`)) score += 75;
    else if (` ${textNorm} `.includes(` ${gramNorm} `) && gramNorm.length >= 3) score += 55;
    else if (gramNorm.includes(textNorm) && textNorm.length >= 4) score += 35;

    if (entry.model && normalizeText(entry.model) === gramNorm) score += 35;
    if (entry.fullModel && normalizeText(entry.fullModel) === gramNorm) score += 30;
    if (entry.displayName && normalizeText(entry.displayName) === gramNorm) score += 25;
    if (entry.variantCount || entry.priceCount || entry.rowCount) score += 5;

    if (score > bestScore) {
      bestScore = score;
      bestText = text;
    }
  }

  if (bestScore <= 0) return null;

  const confidence =
    bestScore >= 120
      ? 0.99
      : bestScore >= 110
        ? 0.97
        : bestScore >= 80
          ? 0.92
          : bestScore >= 55
            ? 0.84
            : bestScore >= 35
              ? 0.76
              : 0;

  if (confidence <= 0) return null;

  return {
    ...entry,
    score: bestScore,
    confidence: Math.max(Number(entry.confidence || 0), confidence),
    matchedText: entry.matchedText || bestText || gram,
    matchText: bestText || gram,
    method: entry.method || "db_entity_ngram_match",
  };
};


const getExactHitLengthInMessage = ({ message = "", keys = [] } = {}) => {
  const messageNorm = normalizeText(message);
  const messageCompact = compactText(message);

  if (!messageNorm) return 0;

  let best = 0;

  for (const key of unique(keys)) {
    const keyNorm = normalizeText(key);
    const keyCompact = compactText(key);

    if (!keyNorm) continue;

    const paddedMessage = ` ${messageNorm} `;
    const paddedKey = ` ${keyNorm} `;

    if (paddedMessage.includes(paddedKey)) {
      best = Math.max(best, keyNorm.length);
      continue;
    }

    if (keyCompact && messageCompact.includes(keyCompact)) {
      best = Math.max(best, keyCompact.length);
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
