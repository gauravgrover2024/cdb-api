import mongoose from "mongoose";

const COLLECTION = "vehicle_features";
const CACHE_TTL_MS = 10 * 60 * 1000;

let cachedIndex = null;
let cachedAt = 0;

const clean = (value = "") =>
  String(value ?? "")
    .replace(/&amp;/g, "&")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeText = (value = "") =>
  clean(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactText = (value = "") => normalizeText(value).replace(/\s+/g, "");

const titleCase = (value = "") =>
  clean(value).replace(/\w\S*/g, (word) =>
    word.charAt(0).toUpperCase() + word.slice(1).toLowerCase(),
  );

const uniq = (items = []) => [...new Set(items.filter(Boolean))];

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const getDb = () => {
  if (!mongoose.connection?.db) {
    throw new Error("MongoDB connection is not ready for feature data adapter.");
  }
  return mongoose.connection.db;
};

const isPlainObject = (value) =>
  Boolean(value && typeof value === "object" && !Array.isArray(value));

const parseAvailability = (value = "") => {
  const raw = clean(value);
  const normalized = normalizeText(raw);

  if (!raw) {
    return {
      available: false,
      displayValue: "Not Available",
      rawValue: raw,
    };
  }

  if (
    normalized === "not available" ||
    normalized === "no" ||
    normalized === "false" ||
    normalized === "na" ||
    normalized === "n a" ||
    normalized === "not applicable" ||
    normalized === "-"
  ) {
    return {
      available: false,
      displayValue: "Not Available",
      rawValue: raw,
    };
  }

  return {
    available: true,
    displayValue: raw === "Yes" ? "Yes" : raw,
    rawValue: raw,
  };
};

const parseFeatureKey = (key = "") => {
  const parts = clean(key)
    .split("|")
    .map((item) => clean(item))
    .filter(Boolean);

  if (parts.length >= 2) {
    return {
      section: parts[0],
      featureName: parts.slice(1).join(" | "),
    };
  }

  return {
    section: "",
    featureName: clean(key),
  };
};

const stripVariantPrefix = ({ brand = "", model = "", variant = "" } = {}) => {
  let value = clean(variant);
  const prefixes = [
    clean(`${brand} ${model}`),
    clean(model),
    clean(brand),
  ].filter(Boolean);

  for (const prefix of prefixes) {
    const pattern = new RegExp(`^${prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s+`, "i");
    value = value.replace(pattern, "").trim();
  }

  return value || clean(variant);
};

const buildRow = ({ doc, section, featureName, value, sourceKey, index }) => {
  const brand = clean(doc.brand || doc.make || "");
  const model = clean(doc.model || "");
  const variantFull = clean(doc.variant || doc.variantName || doc.version || "");
  const variant = stripVariantPrefix({ brand, model, variant: variantFull });

  const availability = parseAvailability(value);

  return {
    id: `${String(doc._id)}-${index}`,
    sourceDocId: String(doc._id || ""),
    brand,
    make: brand,
    model,
    variant,
    variantFull,
    section: clean(section),
    featureName: clean(featureName),
    featureKey: clean(sourceKey || [section, featureName].filter(Boolean).join(" | ")),
    value: availability.displayValue,
    displayValue: availability.displayValue,
    rawValue: availability.rawValue,
    available: availability.available,
    present: availability.available,
    included: availability.available,
    normalizedModel: normalizeText(model),
    normalizedBrandModel: normalizeText([brand, model].filter(Boolean).join(" ")),
    normalizedVariant: normalizeText(variant),
    normalizedFeatureName: normalizeText(featureName),
    normalizedSection: normalizeText(section),
  };
};

export const extractFeatureRowsFromDoc = (doc = {}) => {
  const rows = [];
  let index = 0;

  if (isPlainObject(doc.features)) {
    for (const [featureKey, value] of Object.entries(doc.features)) {
      const { section, featureName } = parseFeatureKey(featureKey);

      if (!featureName) continue;

      rows.push(
        buildRow({
          doc,
          section,
          featureName,
          value,
          sourceKey: featureKey,
          index,
        }),
      );

      index += 1;
    }
  }

  const possibleArrays = [
    doc.featureList,
    doc.rows,
    doc.items,
    doc.specs,
    doc.specifications,
  ];

  for (const list of possibleArrays) {
    for (const item of toArray(list)) {
      if (!item || typeof item !== "object") continue;

      const featureName = clean(
        item.featureName ||
          item.feature ||
          item.name ||
          item.label ||
          item.title ||
          item.key ||
          "",
      );

      if (!featureName) continue;

      rows.push(
        buildRow({
          doc,
          section: item.section || item.category || item.group || "",
          featureName,
          value:
            item.value ||
            item.displayValue ||
            item.featureValue ||
            item.status ||
            (item.available === false ? "Not Available" : "Yes"),
          sourceKey: item.key || featureName,
          index,
        }),
      );

      index += 1;
    }
  }

  return rows;
};

const getAcronym = (value = "") => {
  const text = clean(value);

  const bracket = text.match(/\(([A-Z0-9]{2,})\)/);
  if (bracket?.[1]) return bracket[1].toLowerCase();

  const words = text
    .replace(/\([^)]*\)/g, "")
    .split(/\s+/)
    .filter((word) => /^[A-Za-z]/.test(word));

  if (words.length < 2) return "";

  const acronym = words.map((word) => word[0]).join("").toLowerCase();
  return acronym.length >= 2 ? acronym : "";
};

const singularize = (value = "") =>
  normalizeText(value)
    .replace(/\bseats\b/g, "seat")
    .replace(/\bwheels\b/g, "wheel")
    .replace(/\bheadlamps\b/g, "headlamp")
    .replace(/\bheadlights\b/g, "headlight")
    .replace(/\bairbags\b/g, "airbag")
    .trim();

const removeWeakWords = (value = "") =>
  normalizeText(value)
    .replace(/\b(system|control|controls|feature|features|front|rear|the|and|with)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

export const generateFeatureAliasesFromDbName = (featureName = "") => {
  const base = clean(featureName);
  const normalized = normalizeText(base);
  const noBracket = normalizeText(base.replace(/\([^)]*\)/g, " "));
  const acronym = getAcronym(base);

  const aliases = [
    base,
    normalized,
    noBracket,
    singularize(base),
    removeWeakWords(base),
    acronym,
  ];

  const tokens = normalized.split(" ").filter(Boolean);

  if (tokens.length >= 2) {
    aliases.push(tokens.slice(-2).join(" "));
    aliases.push(tokens.slice(0, 2).join(" "));
  }

  if (normalized.includes("parking camera")) {
    aliases.push("rear camera", "reverse camera", "parking camera", "camera");
  }

  if (normalized.includes("sunroof")) {
    aliases.push("sunroof", "sun roof", "moonroof", "panoramic sunroof", "single pane sunroof");
  }

  if (normalized.includes("ventilated")) {
    aliases.push("ventilated seats", "ventilated seat", "cooled seats", "seat ventilation");
  }

  if (normalized.includes("wireless") && normalized.includes("charger")) {
    aliases.push("wireless charger", "wireless charging", "phone charger", "mobile charger");
  }

  if (normalized.includes("alloy")) {
    aliases.push("alloy wheels", "alloys", "alloy");
  }

  if (normalized.includes("speaker") || normalized.includes("sound") || normalized.includes("audio")) {
    aliases.push("speakers", "premium audio", "sound system", "music system", "bose audio", "bose speakers");
  }

  if (normalized.includes("headlamp") || normalized.includes("headlight")) {
    aliases.push("headlamps", "headlights", "led headlamps", "led headlights");
  }

  if (normalized.includes("airbag")) {
    aliases.push("airbags", "6 airbags", "six airbags");
  }

  return uniq(aliases.map(normalizeText).filter(Boolean));
};

const editDistance = (a = "", b = "") => {
  const s = compactText(a);
  const t = compactText(b);

  if (s === t) return 0;
  if (!s) return t.length;
  if (!t) return s.length;

  const dp = Array.from({ length: s.length + 1 }, () =>
    Array(t.length + 1).fill(0),
  );

  for (let i = 0; i <= s.length; i += 1) dp[i][0] = i;
  for (let j = 0; j <= t.length; j += 1) dp[0][j] = j;

  for (let i = 1; i <= s.length; i += 1) {
    for (let j = 1; j <= t.length; j += 1) {
      const cost = s[i - 1] === t[j - 1] ? 0 : 1;

      dp[i][j] = Math.min(
        dp[i - 1][j] + 1,
        dp[i][j - 1] + 1,
        dp[i - 1][j - 1] + cost,
      );
    }
  }

  return dp[s.length][t.length];
};

const tokenScore = (a = "", b = "") => {
  const aTokens = normalizeText(a).split(" ").filter(Boolean);
  const bTokens = normalizeText(b).split(" ").filter(Boolean);

  if (!aTokens.length || !bTokens.length) return 0;

  const aSet = new Set(aTokens);
  const bSet = new Set(bTokens);
  const intersection = [...aSet].filter((token) => bSet.has(token)).length;

  if (!intersection) return 0;

  const precision = intersection / aSet.size;
  const recall = intersection / bSet.size;

  return (2 * precision * recall) / (precision + recall);
};

export const phraseScore = (a = "", b = "") => {
  const x = normalizeText(a);
  const y = normalizeText(b);

  if (!x || !y) return 0;
  if (x === y) return 1;

  const compactX = compactText(x);
  const compactY = compactText(y);

  if (compactX === compactY) return 0.99;

  if (x.includes(y) || y.includes(x)) {
    const small = Math.min(compactX.length, compactY.length);
    const large = Math.max(compactX.length, compactY.length);
    return Math.min(0.96, 0.72 + (small / large) * 0.24);
  }

  const distance = editDistance(x, y);
  const maxLen = Math.max(compactX.length, compactY.length);

  return Math.max(tokenScore(x, y), 1 - distance / maxLen);
};

const buildFeatureNameIndex = (rows = []) => {
  const map = new Map();

  for (const row of rows) {
    const key = normalizeText(row.featureName);

    if (!key) continue;

    if (!map.has(key)) {
      map.set(key, {
        featureName: row.featureName,
        normalizedFeatureName: key,
        aliases: generateFeatureAliasesFromDbName(row.featureName),
        sections: new Set(),
        count: 0,
        availableCount: 0,
      });
    }

    const entry = map.get(key);
    entry.sections.add(row.section);
    entry.count += 1;
    if (row.available) entry.availableCount += 1;
  }

  return [...map.values()].map((entry) => ({
    ...entry,
    sections: [...entry.sections],
  }));
};

const buildIndexes = (rows = []) => {
  const models = new Map();
  const variantsByModel = new Map();
  const sections = new Set();

  for (const row of rows) {
    if (row.model) {
      const modelKey = row.normalizedModel;
      const brandModelKey = row.normalizedBrandModel;

      if (!models.has(modelKey)) {
        models.set(modelKey, {
          brand: row.brand,
          model: row.model,
          aliases: uniq([row.model, `${row.brand} ${row.model}`]),
          normalizedModel: modelKey,
          normalizedBrandModel: brandModelKey,
          rows: 0,
        });
      }

      models.get(modelKey).rows += 1;
    }

    if (row.model && row.variant) {
      const modelKey = row.normalizedModel;

      if (!variantsByModel.has(modelKey)) variantsByModel.set(modelKey, new Map());

      const variantKey = row.normalizedVariant;

      if (!variantsByModel.get(modelKey).has(variantKey)) {
        variantsByModel.get(modelKey).set(variantKey, {
          label: row.variant,
          fullLabel: row.variantFull,
          aliases: uniq([row.variant, row.variantFull]),
          normalizedVariant: variantKey,
        });
      }
    }

    if (row.section) sections.add(row.section);
  }

  return {
    models: [...models.values()],
    variantsByModel: new Map(
      [...variantsByModel.entries()].map(([modelKey, variantMap]) => [
        modelKey,
        [...variantMap.values()],
      ]),
    ),
    featureNames: buildFeatureNameIndex(rows),
    sections: [...sections],
  };
};

export const buildVehicleFeatureDataIndex = async ({ force = false } = {}) => {
  const now = Date.now();

  if (!force && cachedIndex && now - cachedAt < CACHE_TTL_MS) {
    return cachedIndex;
  }

  const db = getDb();

  const docs = await db
    .collection(COLLECTION)
    .find(
      {},
      {
        projection: {
          brand: 1,
          make: 1,
          model: 1,
          variant: 1,
          variantName: 1,
          version: 1,
          features: 1,
          featureList: 1,
          rows: 1,
          items: 1,
          specs: 1,
          specifications: 1,
        },
      },
    )
    .toArray();

  const rows = docs.flatMap(extractFeatureRowsFromDoc);
  const indexes = buildIndexes(rows);

  cachedIndex = {
    collection: COLLECTION,
    builtAt: new Date().toISOString(),
    docsCount: docs.length,
    rows,
    ...indexes,
    stats: {
      docs: docs.length,
      rows: rows.length,
      models: indexes.models.length,
      featureNames: indexes.featureNames.length,
      sections: indexes.sections.length,
    },
  };

  cachedAt = now;
  return cachedIndex;
};

const modelMatches = (row, model = "") => {
  const modelNorm = normalizeText(model);
  const modelCompact = compactText(model);

  if (!modelNorm) return true;

  return (
    row.normalizedModel === modelNorm ||
    row.normalizedBrandModel === modelNorm ||
    compactText(row.model) === modelCompact ||
    compactText(`${row.brand} ${row.model}`) === modelCompact
  );
};

export const resolveDbFeatureName = ({ index, featurePhrase = "", model = "" }) => {
  const rowsForModel = model
    ? index.rows.filter((row) => modelMatches(row, model))
    : index.rows;

  const featureEntries = buildFeatureNameIndex(rowsForModel.length ? rowsForModel : index.rows);

  let best = null;

  for (const entry of featureEntries) {
    const aliases = entry.aliases?.length ? entry.aliases : [entry.featureName];

    for (const alias of aliases) {
      const score = phraseScore(featurePhrase, alias);

      if (!best || score > best.score) {
        best = {
          ...entry,
          matchedAlias: alias,
          score,
        };
      }
    }
  }

  if (!best || best.score < 0.68) return null;

  return best;
};

export const searchDbFeatureRows = ({
  index,
  model = "",
  featurePhrase = "",
  variant = "",
  includeUnavailable = true,
  onlyAvailable = false,
} = {}) => {
  const resolvedFeature = resolveDbFeatureName({ index, featurePhrase, model });

  if (!resolvedFeature) {
    return {
      resolvedFeature: null,
      rows: [],
      availableRows: [],
      unavailableRows: [],
      modelCoverageRows: index.rows.filter((row) => modelMatches(row, model)).length,
    };
  }

  const featureKey = normalizeText(resolvedFeature.featureName);
  const variantNorm = normalizeText(variant);

  const rows = index.rows.filter((row) => {
    if (!modelMatches(row, model)) return false;
    if (normalizeText(row.featureName) !== featureKey) return false;

    if (variantNorm) {
      const rowVariant = normalizeText(row.variant);
      const rowVariantFull = normalizeText(row.variantFull);
      if (rowVariant !== variantNorm && rowVariantFull !== variantNorm) return false;
    }

    return includeUnavailable || row.available;
  });

  const availableRows = rows.filter((row) => row.available);
  const unavailableRows = rows.filter((row) => !row.available);

  return {
    resolvedFeature,
    rows: onlyAvailable ? availableRows : rows,
    availableRows,
    unavailableRows,
    modelCoverageRows: index.rows.filter((row) => modelMatches(row, model)).length,
  };
};

export default {
  buildVehicleFeatureDataIndex,
  extractFeatureRowsFromDoc,
  resolveDbFeatureName,
  searchDbFeatureRows,
  generateFeatureAliasesFromDbName,
  phraseScore,
};
