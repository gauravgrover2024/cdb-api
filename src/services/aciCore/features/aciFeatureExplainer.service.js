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

const deriveDisplayAliases = (displayName = "") => {
  const raw = clean(displayName);
  if (!raw) return [];

  const parenthetical = [...raw.matchAll(/\(([^)]+)\)/g)]
    .map((match) => clean(match[1]).replace(/[^A-Za-z0-9]+/g, ""))
    .filter((value) => value.length >= 2 && value.length <= 12);
  const uppercaseTokens = raw
    .split(/\s+/)
    .map((token) => token.replace(/[^A-Za-z0-9]+/g, ""))
    .filter((token) => /^[A-Z][A-Z0-9]{3,11}$/.test(token));
  const withoutParenthetical = clean(raw.replace(/\s*\([^)]*\)\s*/g, " "));

  return unique([
    ...parenthetical,
    ...uppercaseTokens,
    ...(withoutParenthetical && withoutParenthetical !== raw ? [withoutParenthetical] : []),
  ]);
};

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
  featureType: clean(doc.featureType),
  decisionCategory: clean(doc.decisionCategory),
  decisionSignals: unique(asArray(doc.decisionSignals)),
  importance: doc.importance && typeof doc.importance === "object"
    ? { ...doc.importance }
    : {},
  qualityScore: Number(doc.qualityScore || 0),
  qualityStatus: clean(doc.qualityStatus),
  contentOrigin: clean(doc.contentOrigin),
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
      aliasEntries: [],
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
          featureType: 1,
          decisionCategory: 1,
          decisionSignals: 1,
          importance: 1,
          qualityScore: 1,
          qualityStatus: 1,
          contentOrigin: 1,
          sourceRefs: 1,
          contentVersion: 1,
          reviewedAt: 1,
        },
      },
    )
    .toArray();

  const byCanonicalKey = new Map();
  const byAlias = new Map();
  const aliasEntries = [];

  for (const doc of docs) {
    const value = publicExplainer(doc);
    const canonicalKey = normalize(doc.canonicalKey).replace(/\s+/g, "_");
    if (canonicalKey) byCanonicalKey.set(canonicalKey, value);

    const aliases = unique([
      doc.canonicalKey,
      doc.displayName,
      ...deriveDisplayAliases(doc.displayName),
      ...asArray(doc.aliases),
    ]);
    for (const alias of aliases) {
      const key = normalize(alias);
      if (key) {
        aliasEntries.push({
          alias: key,
          value,
          primary:
            key === normalize(doc.canonicalKey) ||
            key === normalize(doc.displayName),
        });
      }
    }
  }

  aliasEntries.sort((left, right) =>
    right.alias.split(" ").length - left.alias.split(" ").length ||
    right.alias.length - left.alias.length ||
    Number(right.primary) - Number(left.primary));

  for (const entry of aliasEntries) {
    const current = byAlias.get(entry.alias);
    if (!current || aliasCandidateScore(entry) > aliasCandidateScore({
      alias: entry.alias,
      value: current,
      primary: false,
    })) {
      byAlias.set(entry.alias, entry.value);
    }
  }

  explainerCache = {
    byCanonicalKey,
    byAlias,
    aliasEntries,
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

const aliasCandidateScore = ({ alias = "", value = {}, primary = false } = {}) => {
  const displayName = normalize(value.displayName);
  const canonicalKey = normalize(value.canonicalKey.replace(/_/g, " "));
  let score = alias.split(" ").length * 100 + alias.length;
  if (primary) score += 500;
  if (displayName === alias || canonicalKey === alias) score += 1_000;
  if (displayName.startsWith(`${alias} `)) score += 120;
  if (canonicalKey.startsWith(`${alias} `)) score += 100;
  return score;
};

export const resolveAciFeatureExplainersFromText = async (message = "", { limit = 8 } = {}) => {
  const normalizedMessage = normalize(message);
  if (!normalizedMessage) return [];

  const cache = await loadExplainerCache();
  const paddedMessage = ` ${normalizedMessage} `;
  const matches = [];

  for (const entry of cache.aliasEntries || []) {
    if (entry.alias.length < 3) continue;
    if (!paddedMessage.includes(` ${entry.alias} `)) continue;
    const start = normalizedMessage.indexOf(entry.alias);
    matches.push({
      ...entry,
      score: aliasCandidateScore(entry),
      start,
      end: start + entry.alias.length,
    });
  }

  matches.sort((left, right) =>
    right.alias.split(" ").length - left.alias.split(" ").length ||
    right.alias.length - left.alias.length ||
    right.score - left.score);

  const selected = [];
  const selectedKeys = new Set();
  const selectedSpans = [];
  for (const match of matches) {
    const canonicalKey = match.value.canonicalKey;
    if (!canonicalKey || selectedKeys.has(canonicalKey)) continue;
    if (selectedSpans.some((span) => match.start < span.end && match.end > span.start)) {
      continue;
    }

    const betterSameAlias = matches
      .filter((entry) => entry.alias === match.alias)
      .sort((left, right) => right.score - left.score)[0];
    if (betterSameAlias?.value?.canonicalKey !== canonicalKey) continue;

    selectedKeys.add(canonicalKey);
    selectedSpans.push({ start: match.start, end: match.end });
    selected.push({
      ...match.value,
      matchedAlias: match.alias,
      matchStart: match.start,
    });
    if (selected.length >= Math.max(1, Number(limit) || 1)) break;
  }

  return selected
    .sort((left, right) => left.matchStart - right.matchStart)
    .map(({ matchStart, ...value }) => value);
};

export const resolveAciFeatureExplainerFromText = async (message = "") => {
  const matches = await resolveAciFeatureExplainersFromText(message, { limit: 1 });
  return matches[0] || null;
};

export const composeAciFeatureExplanation = (explainer = {}) =>
  unique([
    explainer.buyerSummary,
    explainer.whenItMattersSummary,
    explainer.buyerAdvice,
  ]).join(" ");

const IMPORTANCE_CONTEXTS = Object.freeze([
  { key: "safety", label: "your safety priority", pattern: /\bsafe(?:ty)?|crash|protect/i },
  { key: "cityUse", label: "regular city use", pattern: /\bcity|traffic|commute/i },
  { key: "highwayUse", label: "highway use", pattern: /\bhighway|expressway|road trip|long drive/i },
  { key: "familyUse", label: "family use", pattern: /\bfamily|kids?|children|parents|occupants?/i },
  { key: "offRoadUse", label: "off-road use", pattern: /\boff.?road|trail|rough road/i },
  { key: "chauffeurUse", label: "chauffeur-driven use", pattern: /\bchauffeur|driver driven/i },
  { key: "firstTimeBuyer", label: "a first-time buyer", pattern: /\bfirst.?time|first car/i },
]);

const buildContextImportanceNote = ({ explainer = {}, message = "", buyerContext = {} } = {}) => {
  const contextText = [message, JSON.stringify(buyerContext || {})].join(" ");
  const matched = IMPORTANCE_CONTEXTS
    .filter((item) => item.pattern.test(contextText))
    .map((item) => ({
      ...item,
      importance: clean(
        item.key === "safety" && explainer.importance?.safetyCritical === true
          ? "critical"
          : explainer.importance?.[item.key],
      ).toLowerCase(),
    }))
    .filter((item) => item.importance);
  if (!matched.length) return "";

  const rank = { critical: 4, high: 3, medium: 2, low: 1, not_applicable: 0 };
  matched.sort((left, right) => (rank[right.importance] || 0) - (rank[left.importance] || 0));
  const strongest = matched[0];
  if (strongest.importance === "critical") {
    return `For ${strongest.label}, this is worth treating as a top-priority feature.`;
  }
  if (strongest.importance === "high") {
    return `For ${strongest.label}, this is worth prioritising while you shortlist variants.`;
  }
  if (strongest.importance === "medium") {
    return `For ${strongest.label}, this is useful, but it need not decide the purchase on its own.`;
  }
  return `For ${strongest.label}, this is usually a lower priority than your core safety and everyday-use needs.`;
};

export const composeAciFeatureDecisionExplanation = ({
  explainer = {},
  message = "",
  buyerContext = {},
} = {}) => {
  const generatedContextAdvice = buildContextImportanceNote({ explainer, message, buyerContext });
  const contextText = [message, JSON.stringify(buyerContext || {})].join(" ");
  const buyerAdviceAlreadyUsesContext = IMPORTANCE_CONTEXTS.some((item) =>
    item.pattern.test(contextText) && item.pattern.test(explainer.buyerAdvice || ""));
  const contextAdvice = buyerAdviceAlreadyUsesContext ? "" : generatedContextAdvice;
  const answer = unique([
    explainer.buyerSummary,
    explainer.howItWorks,
    explainer.whenItMattersSummary,
    explainer.limitationsSummary ? `Keep in mind: ${explainer.limitationsSummary}` : "",
    explainer.buyerAdvice,
    contextAdvice,
  ]).join(" ");

  return {
    answer,
    contextAdvice,
  };
};

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
