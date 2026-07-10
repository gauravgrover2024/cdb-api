import mongoose from "mongoose";

export const ACI_FEATURE_EXPLAINER_COLLECTION = "aci_feature_explainers_v1";

const CACHE_TTL_MS = Math.max(
  60_000,
  Number(process.env.ACI_FEATURE_EXPLAINER_CACHE_TTL_MS || 10 * 60 * 1000),
);

let explainerCache = null;
let explainerCacheAt = 0;

const clean = (value = "") =>
  String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();

const normalize = (value = "") =>
  clean(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const unique = (values = []) => [...new Set(values.map(clean).filter(Boolean))];

const publicExplainer = (doc = {}) => ({
  schemaVersion: clean(doc.schemaVersion),
  canonicalKey: clean(doc.canonicalKey),
  displayName: clean(doc.displayName),
  groupKey: clean(doc.groupKey),
  groupLabel: clean(doc.groupLabel),
  buyerSummary: clean(doc.buyerSummary),
  howItWorks: clean(doc.howItWorks),
  whenItMattersSummary: clean(doc.whenItMattersSummary),
  whenItMatters: unique(asArray(doc.whenItMatters)),
  limitationsSummary: clean(doc.limitationsSummary),
  buyerAdvice: clean(doc.buyerAdvice),
  importance: doc.importance && typeof doc.importance === "object"
    ? { ...doc.importance }
    : {},
  sourceRefs: asArray(doc.sourceRefs).map((source = {}) => ({
    title: clean(source.title),
    url: clean(source.url),
    sourceType: clean(source.sourceType),
    verifiedAt: source.verifiedAt || null,
  })),
  sourceCollection: ACI_FEATURE_EXPLAINER_COLLECTION,
  contentVersion: clean(doc.contentVersion),
  reviewedAt: doc.reviewedAt || null,
});

const loadExplainerCache = async ({ force = false } = {}) => {
  const now = Date.now();
  if (!force && explainerCache && now - explainerCacheAt < CACHE_TTL_MS) {
    return explainerCache;
  }

  const db = mongoose.connection?.db;
  if (!db) {
    return {
      byCanonicalKey: new Map(),
      byAlias: new Map(),
      rows: 0,
    };
  }

  const docs = await db.collection(ACI_FEATURE_EXPLAINER_COLLECTION)
    .find(
      { status: "published" },
      {
        projection: {
          _id: 0,
          schemaVersion: 1,
          canonicalKey: 1,
          displayName: 1,
          groupKey: 1,
          groupLabel: 1,
          aliases: 1,
          buyerSummary: 1,
          howItWorks: 1,
          whenItMattersSummary: 1,
          whenItMatters: 1,
          limitationsSummary: 1,
          buyerAdvice: 1,
          importance: 1,
          sourceRefs: 1,
          contentVersion: 1,
          reviewedAt: 1,
        },
      },
    )
    .toArray();

  const byCanonicalKey = new Map();
  const byAlias = new Map();

  for (const doc of docs) {
    const value = publicExplainer(doc);
    const canonicalKey = normalize(doc.canonicalKey).replace(/\s+/g, "_");
    if (canonicalKey) byCanonicalKey.set(canonicalKey, value);

    const aliases = unique([
      doc.canonicalKey,
      doc.displayName,
      ...asArray(doc.aliases),
    ]);
    for (const alias of aliases) {
      const key = normalize(alias);
      if (key && !byAlias.has(key)) byAlias.set(key, value);
    }
  }

  explainerCache = {
    byCanonicalKey,
    byAlias,
    rows: docs.length,
  };
  explainerCacheAt = now;
  return explainerCache;
};

export const resolveAciFeatureExplainer = async ({
  canonicalKey = "",
  featureName = "",
  aliases = [],
} = {}) => {
  const cache = await loadExplainerCache();
  const key = normalize(canonicalKey).replace(/\s+/g, "_");
  if (key && cache.byCanonicalKey.has(key)) return cache.byCanonicalKey.get(key);

  for (const candidate of [featureName, canonicalKey, ...asArray(aliases)]) {
    const alias = normalize(candidate);
    if (alias && cache.byAlias.has(alias)) return cache.byAlias.get(alias);
  }

  return null;
};

export const composeAciFeatureExplanation = (explainer = {}) =>
  unique([
    explainer.buyerSummary,
    explainer.whenItMattersSummary,
  ]).join(" ");

export const prewarmAciFeatureExplainers = async ({ force = false } = {}) => {
  const startedAt = Date.now();
  const cache = await loadExplainerCache({ force });

  return {
    ok: cache.rows > 0,
    durationMs: Date.now() - startedAt,
    cache: {
      publishedExplainers: cache.rows,
      aliases: cache.byAlias.size,
    },
  };
};

export default resolveAciFeatureExplainer;
