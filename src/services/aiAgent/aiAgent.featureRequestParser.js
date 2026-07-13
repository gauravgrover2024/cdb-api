import mongoose from "mongoose";

const DEFAULT_FEATURE_REQUEST_CACHE_TTL_MS = Number(
  process.env.ACI_FEATURE_REQUEST_CACHE_TTL_MS || 15 * 60 * 1000,
);

let featureCatalogCache = {
  builtAt: 0,
  promise: null,
  catalog: null,
};

const cleanText = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim();

const normalizeFeatureText = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeFeatureKey = (value = "") =>
  normalizeFeatureText(value).replace(/\s+/g, "_");

const humanizeFeatureKey = (key = "") =>
  cleanText(String(key || "").replace(/_/g, " ")).replace(/\b\w/g, (char) =>
    char.toUpperCase(),
  );

const escapeRegExp = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const getDb = () => {
  if (mongoose.connection?.readyState !== 1 || !mongoose.connection?.db) {
    return null;
  }

  return mongoose.connection.db;
};

const asArray = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean);
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const unique = (items = []) => [...new Set(items.filter(Boolean))];


const ACI_FEATURE_REQUEST_BLOCKED_ALIASES = new Set([
  "and",
  "or",
  "with",
  "without",
  "plus",
  "also",
  "rear",
  "front",
  "seat",
  "seats",
  "door",
  "doors",
  "system",
  "type",
  "size",
  "number",
  "package",
  "feature",
  "features",
  "control",
  "controls",
  "warning",
  "alert",
  "view",
  "vehicle",
  "remote",
  "assist",
]);

const isUsefulFeatureAlias = (alias = "") => {
  const normalized = normalizeFeatureText(alias);
  if (!normalized) return false;
  if (ACI_FEATURE_REQUEST_BLOCKED_ALIASES.has(normalized)) return false;

  const tokens = normalized.split(/\s+/).filter(Boolean);

  if (tokens.length === 1) {
    const token = tokens[0];

    // Allow meaningful short acronyms like ABS, ADAS, TPMS, ESC from display/catalog.
    if (/^[a-z0-9]{2,6}$/.test(token) && alias === String(alias || "").toUpperCase()) {
      return true;
    }

    // Allow strong single-word features like sunroof/touchscreen/turbo if they are long enough.
    return token.length >= 5;
  }

  return true;
};


const getCatalogKey = (doc = {}) =>
  cleanText(
    doc.canonicalKey ||
      doc.featureKey ||
      doc.normalizedKey ||
      doc.key ||
      doc.slug ||
      "",
  );

const getCatalogDisplayName = (doc = {}) =>
  cleanText(
    doc.displayName ||
      doc.featureName ||
      doc.feature ||
      doc.name ||
      doc.label ||
      humanizeFeatureKey(getCatalogKey(doc)),
  );

const getMatrixFeatureDisplayName = (key = "", value = {}) =>
  cleanText(value?.displayName || value?.label || humanizeFeatureKey(key));

const acronymAliasesFromDisplayName = (displayName = "") => {
  const aliases = [];
  const text = String(displayName || "");

  const parenthetical = [...text.matchAll(/\(([A-Za-z0-9]{2,8})\)/g)]
    .map((match) => match[1])
    .filter(Boolean);

  aliases.push(...parenthetical);

  const uppercaseWords = text
    .split(/\s+/)
    .map((word) => word.replace(/[^A-Za-z0-9]/g, ""))
    .filter((word) => /^[A-Z0-9]{2,8}$/.test(word));

  aliases.push(...uppercaseWords);

  return unique(aliases);
};


const extractUppercaseTerms = (value = "") =>
  unique([
    ...String(value || "").matchAll(/\(([A-Za-z0-9]{2,8})\)/g),
  ].map((match) => match[1]).filter(Boolean))
    .concat(
      String(value || "")
        .split(/\s+/)
        .map((word) => word.replace(/[^A-Za-z0-9]/g, ""))
        .filter((word) => /^[A-Z0-9]{2,8}$/.test(word)),
    )
    .map(normalizeFeatureText)
    .filter(Boolean);

const isExactSelfAlias = ({ aliasKey = "", feature = {} } = {}) => {
  const displayKey = normalizeFeatureText(feature.displayName || "");
  const canonicalPhrase = normalizeFeatureText(
    String(feature.canonicalKey || "").replace(/_/g, " "),
  );

  return Boolean(aliasKey && (aliasKey === displayKey || aliasKey === canonicalPhrase));
};

const isStrongAcronymSelfAlias = ({ aliasKey = "", feature = {} } = {}) => {
  if (!/^[a-z0-9]{2,8}$/.test(aliasKey)) return false;

  const displayKey = normalizeFeatureText(feature.displayName || "");
  const canonicalPhrase = normalizeFeatureText(
    String(feature.canonicalKey || "").replace(/_/g, " "),
  );

  if (displayKey === aliasKey || displayKey.startsWith(`${aliasKey} `)) return true;
  if (canonicalPhrase === aliasKey || canonicalPhrase.startsWith(`${aliasKey} `)) return true;

  return extractUppercaseTerms(feature.displayName || "").includes(aliasKey);
};

const scoreAliasFeatureCandidate = ({ alias = {}, feature = {} } = {}) => {
  const aliasKey = normalizeFeatureText(alias.aliasKey || alias.alias || "");

  if (!aliasKey) return 0;
  if (isExactSelfAlias({ aliasKey, feature })) return 100;
  if (isStrongAcronymSelfAlias({ aliasKey, feature })) return 92;

  const displayKey = normalizeFeatureText(feature.displayName || "");
  const canonicalPhrase = normalizeFeatureText(
    String(feature.canonicalKey || "").replace(/_/g, " "),
  );

  if (displayKey.includes(aliasKey)) return 45;
  if (canonicalPhrase.includes(aliasKey)) return 40;

  return 10;
};

const dedupeAmbiguousFeatureAliases = (aliases = [], featureByKey = new Map()) => {
  const grouped = new Map();

  for (const alias of aliases) {
    const aliasKey = normalizeFeatureText(alias.aliasKey || alias.alias || "");
    if (!aliasKey) continue;

    const current = grouped.get(aliasKey) || [];
    current.push(alias);
    grouped.set(aliasKey, current);
  }

  const resolved = [];

  for (const [aliasKey, group] of grouped.entries()) {
    if (group.length <= 1) {
      resolved.push(...group);
      continue;
    }

    const scored = group
      .map((alias) => {
        const feature = featureByKey.get(alias.canonicalKey) || {};
        return {
          alias,
          feature,
          score: scoreAliasFeatureCandidate({ alias, feature }),
        };
      })
      .sort((a, b) => {
        if (b.score !== a.score) return b.score - a.score;

        const aDisplayLength = normalizeFeatureText(a.feature.displayName || "").length;
        const bDisplayLength = normalizeFeatureText(b.feature.displayName || "").length;

        return aDisplayLength - bDisplayLength;
      });

    const topScore = scored[0]?.score || 0;

    // If a generic alias maps to multiple feature rows, keep only the best direct feature.
    // Example: "sunroof" should resolve to Sunroof, not Voice assisted sunroof too.
    if (topScore >= 90) {
      resolved.push(scored[0].alias);
      continue;
    }

    // For genuinely ambiguous low-confidence aliases, keep only the best candidate to avoid explosions.
    resolved.push(scored[0].alias);
  }

  return resolved;
};


const safeKeyTokenAliases = (key = "") => {
  const tokens = normalizeFeatureText(String(key || "").replace(/_/g, " "))
    .split(/\s+/)
    .filter(Boolean);

  const generic = new Set([
    "package",
    "system",
    "type",
    "size",
    "number",
    "front",
    "rear",
    "seat",
    "seats",
    "door",
    "doors",
    "control",
    "controls",
    "features",
    "feature",
    "vehicle",
    "remote",
    "warning",
    "alert",
    "lamp",
    "lamps",
    "light",
    "lights",
    "view",
  ]);

  return tokens.filter(
    (token) =>
      token.length >= 3 &&
      token.length <= 8 &&
      !generic.has(token) &&
      /^[a-z0-9]+$/.test(token),
  );
};

const addFeatureCandidate = (map, candidate = {}) => {
  const canonicalKey = normalizeFeatureKey(candidate.canonicalKey || "");
  if (!canonicalKey) return;

  const existing = map.get(canonicalKey) || {
    canonicalKey,
    displayName: cleanText(candidate.displayName) || humanizeFeatureKey(canonicalKey),
    groupKey: cleanText(candidate.groupKey || ""),
    groupLabel: cleanText(candidate.groupLabel || ""),
    aliases: [],
    sources: [],
  };

  existing.displayName =
    cleanText(candidate.displayName) || existing.displayName || humanizeFeatureKey(canonicalKey);
  existing.groupKey = cleanText(candidate.groupKey || "") || existing.groupKey;
  existing.groupLabel = cleanText(candidate.groupLabel || "") || existing.groupLabel;

  existing.aliases = unique([
    ...(existing.aliases || []),
    canonicalKey.replace(/_/g, " "),
    existing.displayName,
    ...asArray(candidate.aliases),
    ...asArray(candidate.synonyms),
    ...acronymAliasesFromDisplayName(existing.displayName),
  ])
    .map(cleanText)
    .filter(isUsefulFeatureAlias);

  existing.sources = unique([...(existing.sources || []), candidate.source || "unknown"]);

  map.set(canonicalKey, existing);
};

export const loadAciFeatureRequestCatalog = async ({ forceRefresh = false } = {}) => {
  const now = Date.now();

  if (
    !forceRefresh &&
    featureCatalogCache.catalog &&
    now - featureCatalogCache.builtAt < DEFAULT_FEATURE_REQUEST_CACHE_TTL_MS
  ) {
    return featureCatalogCache.catalog;
  }

  if (!forceRefresh && featureCatalogCache.promise) return featureCatalogCache.promise;

  featureCatalogCache.promise = (async () => {
    const db = getDb();

    if (!db) {
      return {
        features: [],
        aliases: [],
        counts: {
          features: 0,
          aliases: 0,
        },
      };
    }

    const featureMap = new Map();

    // Keep parser catalog build light:
    // The parser only needs possible feature keys/aliases, not full per-variant featuresByKey payloads.
    // Full feature values are fetched later by the multi-feature answer runner for the selected model.
    const matrixFeatureKeys = await db
      .collection("vehicle_variant_feature_matrix_v2")
      .distinct("featureKeys");

    for (const key of asArray(matrixFeatureKeys)) {
      addFeatureCandidate(featureMap, {
        canonicalKey: key,
        displayName: humanizeFeatureKey(key),
        source: "vehicle_variant_feature_matrix_v2",
      });
    }

    const catalogDocs = await db
      .collection("vehicle_feature_catalog_v2")
      .find(
        {},
        {
          projection: {
            canonicalKey: 1,
            featureKey: 1,
            normalizedKey: 1,
            key: 1,
            slug: 1,
            displayName: 1,
            featureName: 1,
            feature: 1,
            name: 1,
            label: 1,
            aliases: 1,
            synonyms: 1,
            groupKey: 1,
            groupLabel: 1,
            category: 1,
            group: 1,
          },
          limit: Number(process.env.ACI_FEATURE_REQUEST_CATALOG_DOC_LIMIT || 5000),
        },
      )
      .toArray();

    for (const doc of catalogDocs) {
      const key = getCatalogKey(doc);
      if (!key) continue;

      addFeatureCandidate(featureMap, {
        canonicalKey: key,
        displayName: getCatalogDisplayName(doc),
        groupKey: doc.groupKey || doc.category || doc.group,
        groupLabel: doc.groupLabel || doc.category || doc.group,
        aliases: doc.aliases,
        synonyms: doc.synonyms,
        source: "vehicle_feature_catalog_v2",
      });
    }

    const features = [...featureMap.values()];

    const aliases = [];
    for (const feature of features) {
      for (const alias of feature.aliases || []) {
        const aliasKey = normalizeFeatureText(alias);
        if (!aliasKey) continue;

        aliases.push({
          alias,
          aliasKey,
          canonicalKey: feature.canonicalKey,
          displayName: feature.displayName,
          groupKey: feature.groupKey,
          groupLabel: feature.groupLabel,
          length: aliasKey.length,
          source: feature.sources,
        });
      }
    }

    const featureByKey = new Map(features.map((feature) => [feature.canonicalKey, feature]));

    const dedupedAliases = dedupeAmbiguousFeatureAliases(aliases, featureByKey)
      .sort((a, b) => b.length - a.length || a.displayName.localeCompare(b.displayName));

    return {
      features,
      aliases: dedupedAliases,
      counts: {
        features: features.length,
        aliases: dedupedAliases.length,
      },
      builtAt: new Date().toISOString(),
    };
  })()
    .then((catalog) => {
      featureCatalogCache = {
        builtAt: Date.now(),
        promise: null,
        catalog,
      };
      return catalog;
    })
    .catch((error) => {
      featureCatalogCache.promise = null;
      console.error("[ACI Assist] Failed to build feature request catalog:", error);
      return {
        features: [],
        aliases: [],
        counts: {
          features: 0,
          aliases: 0,
        },
      };
    });

  return featureCatalogCache.promise;
};

export const clearAciFeatureRequestCatalogCache = () => {
  featureCatalogCache = {
    builtAt: 0,
    promise: null,
    catalog: null,
  };
};

const containsAlias = (messageKey = "", aliasKey = "") => {
  if (!messageKey || !aliasKey) return null;

  const escaped = escapeRegExp(aliasKey).replace(/\s+/g, "\\s+");
  const regex = new RegExp(`(^|\\s)${escaped}(?=\\s|$)`, "i");
  const match = messageKey.match(regex);

  if (!match) return null;

  return {
    index: match.index + String(match[1] || "").length,
    length: match[0].trim().length,
    text: match[0].trim(),
  };
};

const removeModelWords = (message = "", modelEntity = {}) => {
  let next = ` ${normalizeFeatureText(message)} `;

  const modelWords = unique([
    modelEntity.fullModel,
    modelEntity.displayName,
    modelEntity.brand && modelEntity.model
      ? `${modelEntity.brand} ${modelEntity.model}`
      : "",
    modelEntity.make && modelEntity.model ? `${modelEntity.make} ${modelEntity.model}` : "",
    modelEntity.model,
    modelEntity.brand,
    modelEntity.make,
  ])
    .map(normalizeFeatureText)
    .filter(Boolean)
    .sort((a, b) => b.length - a.length);

  for (const word of modelWords) {
    next = next.replace(new RegExp(`\\s${escapeRegExp(word).replace(/\s+/g, "\\s+")}\\s`, "gi"), " ");
  }

  return next.replace(/\s+/g, " ").trim();
};

export const parseAciFeatureRequestFromMessage = async ({
  message = "",
  modelEntity = {},
  forceRefresh = false,
} = {}) => {
  const catalog = await loadAciFeatureRequestCatalog({ forceRefresh });
  const messageKey = normalizeFeatureText(message);
  const matches = [];

  for (const item of catalog.aliases || []) {
    const hit = containsAlias(messageKey, item.aliasKey);
    if (!hit) continue;

    matches.push({
      rawText: item.alias,
      matchedText: hit.text,
      matchIndex: hit.index,
      matchLength: hit.length,
      featureKey: item.canonicalKey,
      canonicalKey: item.canonicalKey,
      displayName: item.displayName,
      groupKey: item.groupKey || "",
      groupLabel: item.groupLabel || "",
      confidence: item.aliasKey === normalizeFeatureText(item.displayName) ? 0.98 : 0.94,
      source: item.source || [],
    });
  }

  const seen = new Set();
  const canonicalMatches = matches
    // Validate the original DB/catalog alias, not the lower-cased message match.
    // Example: catalog alias "ADAS" is valid, while matched text becomes "adas".
    .filter((item) => isUsefulFeatureAlias(item.rawText || item.matchedText))
    .sort((a, b) => a.matchIndex - b.matchIndex || b.matchLength - a.matchLength)
    .filter((item) => {
      if (seen.has(item.canonicalKey)) return false;
      seen.add(item.canonicalKey);
      return true;
    });

  const requestedFeatures = canonicalMatches.filter((item, index, list) => {
    const itemStart = Number(item.matchIndex || 0);
    const itemEnd = itemStart + Number(item.matchLength || 0);

    return !list.some((other, otherIndex) => {
      if (index === otherIndex) return false;
      const otherStart = Number(other.matchIndex || 0);
      const otherEnd = otherStart + Number(other.matchLength || 0);
      return (
        other.matchLength > item.matchLength &&
        otherStart <= itemStart &&
        otherEnd >= itemEnd
      );
    });
  });

  let featureStrippedMessage = ` ${messageKey} `;
  for (const item of requestedFeatures) {
    const aliases = unique([item.matchedText, item.rawText, item.displayName, item.canonicalKey.replace(/_/g, " ")])
      .map(normalizeFeatureText)
      .filter(Boolean)
      .sort((a, b) => b.length - a.length);

    for (const alias of aliases) {
      featureStrippedMessage = featureStrippedMessage.replace(
        new RegExp(`\\s${escapeRegExp(alias).replace(/\s+/g, "\\s+")}(?=\\s|$)`, "gi"),
        " ",
      );
    }
  }

  featureStrippedMessage = removeModelWords(featureStrippedMessage, modelEntity)
    .replace(/\b(and|or|with|without|plus|also|does|do|have|has|available|check|show|tell|me|please|whether|if|it|gets|get|comes|come)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  return {
    requestedFeatures,
    featureKeys: requestedFeatures.map((item) => item.canonicalKey),
    hasMultiFeatureRequest: requestedFeatures.length > 1,
    featureStrippedMessage,
    unmatchedTextAfterFeatureExtraction: featureStrippedMessage,
    catalogCounts: catalog.counts,
  };
};
