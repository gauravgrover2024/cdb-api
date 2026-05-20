import mongoose from "mongoose";

const CATALOG_COLLECTION = "vehicle_feature_catalog_v2";
const MATRIX_COLLECTION = "vehicle_variant_feature_matrix_v2";

const CACHE_TTL_MS = 10 * 60 * 1000;

let cachedCatalog = null;
let cachedModels = null;
let cachedAt = 0;

const clean = (value = "") =>
  String(value ?? "")
    .replace(/&amp;/g, "&")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const escapeRegExp = (value = "") =>
  clean(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const normalizeText = (value = "") =>
  clean(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const slug = (value = "") => normalizeText(value).replace(/\s+/g, "_");

const compactText = (value = "") => normalizeText(value).replace(/\s+/g, "");

const uniq = (items = []) => [...new Set(items.filter(Boolean))];

const formatPrice = (value = 0) => {
  const n = Number(value || 0);
  if (!n) return "";

  if (n >= 10000000) return `₹${(n / 10000000).toFixed(2)} Cr`;
  if (n >= 100000) return `₹${(n / 100000).toFixed(2)} L`;
  return `₹${n.toLocaleString("en-IN")}`;
};

const customerFeatureLabel = (value = "") => {
  const textValue = clean(value);
  const key = slug(textValue);

  const labels = {
    adas_package: "ADAS",
    adas: "ADAS",
    six_airbags: "6 airbags",
    camera_360: "360° camera",
    "360_camera": "360° camera",
    "360_view_camera": "360° camera",
  };

  return labels[key] || textValue;
};


const formatCustomerVariantName = (value = "") =>
  String(value || "")
    .trim()
    .split(/\s+/)
    .map((word) => {
      if (/^ivt$/i.test(word)) return "iVT";
      if (/^(dct|amt|at|mt|cvt)$/i.test(word)) return word.toUpperCase();
      if (/^sx$/i.test(word)) return "SX";
      if (/^htx$/i.test(word)) return "HTX";
      if (/^abs$/i.test(word)) return "ABS";
      if (/^[A-Z0-9()]+$/.test(word)) return word;
      return word.charAt(0).toUpperCase() + word.slice(1);
    })
    .join(" ");

const formatCustomerFeatureName = (value = "") => {
  const text = String(value || "").trim();
  if (/abs|anti[-\s]*lock\s*braking/i.test(text)) return "Anti-lock Braking System (ABS)";
  if (/arai\s*mileage|mileage/i.test(text)) return "ARAI mileage";
  if (/sunroof/i.test(text)) return "sunroof";
  return text;
};


const lowerFirst = (value = "") => {
  const textValue = customerFeatureLabel(value);
  if (!textValue) return "";

  if (/^[0-9]/.test(textValue)) return textValue;
  if (/\bADAS\b/.test(textValue)) return textValue;
  if (/^[A-Z0-9\s()+°.-]+$/.test(textValue)) return textValue;

  return textValue.charAt(0).toLowerCase() + textValue.slice(1);
};

const customerFeatureKey = ({ featureKey = "", displayName = "" } = {}) => {
  const key = slug(featureKey || displayName);

  const aliases = {
    adas_package: "adas",
    adas: "adas",
    camera_360: "camera_360",
    "360_camera": "camera_360",
    "360_view_camera": "camera_360",
    rear_camera: "rear_camera",
    reverse_camera: "rear_camera",
    six_airbags: "six_airbags",
  };

  return aliases[key] || key;
};

const featurePhrase = ({ featureName = "", value = "" } = {}) => {
  const feature = clean(featureName);
  const val = clean(value);

  if (!val || ["yes", "available"].includes(normalizeText(val))) {
    return feature;
  }

  if (["no", "not available", "na", "n/a"].includes(normalizeText(val))) {
    return feature;
  }

  if (normalizeText(val).includes(normalizeText(feature))) {
    return val;
  }

  return `${val} ${lowerFirst(feature)}`;
};

const withPrice = (row = {}) =>
  row?.priceLabel ? `${row.variant} at ${row.priceLabel}` : row?.variant || "";

const previewVariants = (rows = [], limit = 4) => {
  const names = rows.slice(0, limit).map((row) => row.variant).filter(Boolean);
  const extra = rows.length > limit ? ` +${rows.length - limit} more` : "";
  return names.length ? `${names.join(", ")}${extra}` : "";
};

const buildExplorerCopy = ({ model = "", variants = [], features = [], includeArchived = false } = {}) => {
  const activeLabel = includeArchived ? "available" : "current";
  return `Here’s the ${model} feature explorer — ${variants.length} ${activeLabel} variant${variants.length === 1 ? "" : "s"} and ${features.length} searchable features, grouped so you can compare quickly.`;
};

const buildInactiveVariantCopy = ({ model = "", variant = "", featureName = "" } = {}) => {
  const featurePart = featureName
    ? ` Pick a current variant to confirm ${lowerFirst(featureName)}.`
    : "";

  return `${clean(variant)} looks like an older ${model} variant, so I’m only showing current new-car options here.${featurePart}`;
};

const buildFeatureAnswerCopy = ({
  model = "",
  variant = "",
  featureName = "",
  mapped = [],
  availableRows = [],
  unavailableRows = [],
  conflictedRows = [],
  cheapest = null,
} = {}) => {
  const targetName = variant ? `${model} ${clean(variant)}` : model;

  if (variant && !mapped.length) {
    return buildInactiveVariantCopy({ model, variant, featureName });
  }

  if (variant && mapped.length > 1) {
    const first = mapped[0];
    const distinctStates = new Set(
      mapped.map((row) =>
        [
          row.available,
          row.availabilityStatus,
          row.conflictStatus,
          clean(row.value),
        ].join("|"),
      ),
    );

    const allSame = distinctStates.size === 1;

    if (allSame && first.availabilityStatus === "conflicted") {
      return `The active ${targetName} sub-variants show mixed ${lowerFirst(featureName)} data, so I’d confirm the exact fuel/transmission version before saying yes.`;
    }

    if (allSame && first.available) {
      return `Good news — all ${mapped.length} active ${targetName} sub-variants get ${featurePhrase({
        featureName,
        value: first.value,
      })}.`;
    }

    if (allSame) {
      return `None of the ${mapped.length} active ${targetName} sub-variants list ${lowerFirst(featureName)}.`;
    }

    const availableCount = availableRows.length;
    const unavailableCount = unavailableRows.length;
    const conflictedCount = conflictedRows.length;

    return `${featureName} depends on the exact ${targetName} sub-variant — ${availableCount} have it, ${unavailableCount} skip it${conflictedCount ? `, and ${conflictedCount} need a quick check` : ""}. Choose the fuel/transmission version to confirm.`;
  }

  if (variant && mapped.length === 1) {
    const first = mapped[0];

    if (first.availabilityStatus === "conflicted") {
      return `${targetName} has mixed ${lowerFirst(featureName)} data, so I’d confirm the exact variant before saying yes.`;
    }

    if (first.available) {
      return `Yes — ${targetName} gets ${featurePhrase({
        featureName,
        value: first.value,
      })}.`;
    }

    return `No — ${targetName} does not list ${lowerFirst(featureName)}.`;
  }

  if (availableRows.length) {
    const startText = cheapest ? ` The most affordable one is ${withPrice(cheapest)}.` : "";
    return `Yes — ${model} offers ${lowerFirst(featureName)} on ${availableRows.length} current variant${availableRows.length === 1 ? "" : "s"}.${startText}`;
  }

  if (conflictedRows.length) {
    return `${model} has mixed ${lowerFirst(featureName)} data across current variants, so I’d select the exact variant before confirming it.`;
  }

  if (mapped.length) {
    return `No current ${model} variant lists ${lowerFirst(featureName)}.`;
  }

  return `${featureName} is not showing on current ${model} variants right now.`;
};

const buildFeatureDiscoveryCopy = ({
  model = "",
  featureName = "",
  rows = [],
  includeMissing = false,
  cheapestOnly = false,
} = {}) => {
  if (cheapestOnly) {
    if (!rows.length) {
      return `No current ${model} variant shows ${lowerFirst(featureName)}.`;
    }

    return `The most affordable current ${model} with ${lowerFirst(featureName)} is ${withPrice(rows[0])}.`;
  }

  if (includeMissing) {
    const preview = previewVariants(rows);
    return rows.length
      ? `${rows.length} current ${model} variant${rows.length === 1 ? "" : "s"} skip ${lowerFirst(featureName)}${preview ? ` — ${preview}.` : "."}`
      : `Every current ${model} variant shows ${lowerFirst(featureName)}.`;
  }

  const preview = previewVariants(rows);
  return rows.length
    ? `${rows.length} current ${model} variant${rows.length === 1 ? "" : "s"} get ${lowerFirst(featureName)}${preview ? ` — ${preview}.` : "."}`
    : `No current ${model} variant shows ${lowerFirst(featureName)}.`;
};

const buildComparisonCopy = ({ model = "", compareSet = [], differenceRows = [] } = {}) => {
  const names = compareSet.map((row) => row.variant).join(" vs ");

  if (!compareSet.length) {
    return `I could not pick the variants to compare for ${model}.`;
  }

  if (!differenceRows.length) {
    return `${names} are very close on listed features. I’ll still show the full feature-by-feature view.`;
  }

  return `${names}: ${differenceRows.length} feature difference${differenceRows.length === 1 ? "" : "s"} stand out. I’ve lined them up one row per feature so it’s easy to compare.`;
};

const makeLeadingQuestionV2 = ({
  id = "",
  label = "",
  title = "",
  query = "",
  intent = "",
  canvasType = "",
  model = "",
  variant = "",
  city = "new-delhi",
} = {}) => {
  const cleanModel = clean(model);
  const cleanVariant = clean(variant);

  return {
    id: clean(id),
    label: clean(label || title),
    title: clean(title || label),
    query: clean(query),
    intent: clean(intent),
    canvasType: clean(canvasType),
    model: cleanModel,
    variant: cleanVariant,
    contextPatch: {
      anchorModel: cleanModel,
      anchorVariant: cleanVariant,
      anchorCity: clean(city || "new-delhi"),
      selectedVehicle: {
        model: cleanModel,
        variant: cleanVariant,
        city: clean(city || "new-delhi"),
      },
    },
  };
};

const dedupeLeadingQuestionsV2 = (items = []) => {
  const seen = new Set();
  const result = [];

  for (const item of items) {
    const key = normalizeText(item.query || item.title || item.label);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    result.push(item);
  }

  return result.slice(0, 4);
};

const buildLeadingQuestionsV2 = ({
  model = "",
  variant = "",
  city = "new-delhi",
} = {}) => {
  const cleanModel = clean(model);
  const cleanVariant = clean(variant);
  const target = [cleanModel, cleanVariant].filter(Boolean).join(" ");

  if (!cleanModel) return [];

  if (cleanVariant) {
    return dedupeLeadingQuestionsV2([
      makeLeadingQuestionV2({
        id: "open-car-overview",
        label: "Open Car Overview",
        title: "Open Car Overview",
        query: `Open ${target} overview`,
        intent: "vehicle_overview",
        canvasType: "car_overview_canvas",
        model: cleanModel,
        variant: cleanVariant,
        city,
      }),
      makeLeadingQuestionV2({
        id: "check-on-road-price",
        label: `Check ${target} on-road price`,
        title: `Check ${target} on-road price`,
        query: `Check ${target} on-road price`,
        intent: "vehicle_variant_price",
        canvasType: "pricelist_canvas",
        model: cleanModel,
        variant: cleanVariant,
        city,
      }),
      makeLeadingQuestionV2({
        id: "show-all-features",
        label: `Show all ${target} features`,
        title: `Show all ${target} features`,
        query: `Show all ${target} features`,
        intent: "vehicle_model_features_explorer",
        canvasType: "features_explorer_canvas",
        model: cleanModel,
        variant: cleanVariant,
        city,
      }),
      makeLeadingQuestionV2({
        id: "show-colors",
        label: `Which colors are available in ${cleanModel}?`,
        title: `Which colors are available in ${cleanModel}?`,
        query: `Which colors are available in ${cleanModel}?`,
        intent: "vehicle_colors",
        canvasType: "color_gallery_canvas",
        model: cleanModel,
        variant: "",
        city,
      }),
    ]);
  }

  return dedupeLeadingQuestionsV2([
    makeLeadingQuestionV2({
      id: "open-car-overview",
      label: "Open Car Overview",
      title: "Open Car Overview",
      query: `Open ${cleanModel} overview`,
      intent: "vehicle_overview",
      canvasType: "car_overview_canvas",
      model: cleanModel,
      city,
    }),
    makeLeadingQuestionV2({
      id: "check-on-road-price",
      label: `Check ${cleanModel} on-road price`,
      title: `Check ${cleanModel} on-road price`,
      query: `Check ${cleanModel} on-road price`,
      intent: "vehicle_pricelist",
      canvasType: "pricelist_canvas",
      model: cleanModel,
      city,
    }),
    makeLeadingQuestionV2({
      id: "show-all-features",
      label: `Show all ${cleanModel} features`,
      title: `Show all ${cleanModel} features`,
      query: `Show all ${cleanModel} features`,
      intent: "vehicle_model_features_explorer",
      canvasType: "features_explorer_canvas",
      model: cleanModel,
      city,
    }),
    makeLeadingQuestionV2({
      id: "show-colors",
      label: `Which colors are available in ${cleanModel}?`,
      title: `Which colors are available in ${cleanModel}?`,
      query: `Which colors are available in ${cleanModel}?`,
      intent: "vehicle_colors",
      canvasType: "color_gallery_canvas",
      model: cleanModel,
      city,
    }),
  ]);
};

const getDb = () => {
  if (!mongoose.connection?.db) {
    throw new Error("MongoDB connection is not ready for Feature Resolver V2.");
  }

  return mongoose.connection.db;
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

const phraseScore = (a = "", b = "") => {
  const x = normalizeText(a);
  const y = normalizeText(b);

  if (!x || !y) return 0;
  if (x === y) return 1;

  const cx = compactText(x);
  const cy = compactText(y);

  if (cx === cy) return 0.99;

  if (x.includes(y) || y.includes(x)) {
    const small = Math.min(cx.length, cy.length);
    const large = Math.max(cx.length, cy.length);
    return Math.min(0.96, 0.72 + (small / large) * 0.24);
  }

  const distance = editDistance(x, y);
  const maxLen = Math.max(cx.length, cy.length);

  return Math.max(0, 1 - distance / maxLen);
};

const getActiveVariantQuery = ({ modelKey, includeArchived = false } = {}) => {
  const query = { modelKey };

  if (!includeArchived) {
    query.activeForFeatureExplorer = true;
  }

  return query;
};

const loadRuntimeIndexes = async ({ force = false } = {}) => {
  const now = Date.now();

  if (!force && cachedCatalog && cachedModels && now - cachedAt < CACHE_TTL_MS) {
    return {
      catalog: cachedCatalog,
      models: cachedModels,
    };
  }

  const db = getDb();

  const catalog = await db
    .collection(CATALOG_COLLECTION)
    .find({})
    .project({
      canonicalKey: 1,
      displayName: 1,
      groupKey: 1,
      groupLabel: 1,
      aliases: 1,
      rows: 1,
      availableRows: 1,
    })
    .toArray();

  const modelRows = await db
    .collection(MATRIX_COLLECTION)
    .aggregate([
      {
        $group: {
          _id: "$modelKey",
          brand: { $first: "$brand" },
          model: { $first: "$model" },
          totalVariants: { $sum: 1 },
          activeVariants: {
            $sum: { $cond: ["$activeForFeatureExplorer", 1, 0] },
          },
        },
      },
      { $match: { activeVariants: { $gt: 0 } } },
      { $sort: { model: 1 } },
    ])
    .toArray();

  cachedCatalog = catalog;

  cachedModels = modelRows.map((row) => ({
    modelKey: row._id,
    brand: row.brand,
    model: row.model,
    aliases: uniq([row.model, `${row.brand} ${row.model}`]),
    totalVariants: row.totalVariants,
    activeVariants: row.activeVariants,
  }));

  cachedAt = now;

  return {
    catalog: cachedCatalog,
    models: cachedModels,
  };
};


const polishSingleVariantFeatureCopy = (response = {}) => {
  if (!response || typeof response !== "object") return response;

  const answer = String(response.answer || "");
  const singleVariantPattern =
    /Good news\s*—\s*all\s+1\s+current\s+(.+?)\s+variants\s+get\s+(.+?)\./i;

  const match = answer.match(singleVariantPattern);

  if (match) {
    const variantName = formatCustomerVariantName(match[1]);
    const featureName = formatCustomerFeatureName(match[2]);

    response.answer = `Yes — ${variantName} gets ${featureName}.`;
  }

  return response;
};


export const resolveFeatureModelV2 = async ({ model = "" } = {}) => {
  const { models } = await loadRuntimeIndexes();

  const query = clean(model);
  if (!query) return null;

  let best = null;

  for (const item of models) {
    for (const alias of item.aliases || []) {
      const score = phraseScore(query, alias);

      if (!best || score > best.score) {
        best = {
          ...item,
          matchedAlias: alias,
          score,
        };
      }
    }
  }

  if (!best || best.score < 0.72) return null;

  return best;
};

export const resolveFeatureKeyV2 = async ({ feature = "" } = {}) => {
  const { catalog } = await loadRuntimeIndexes();

  const query = clean(feature);
  if (!query) return null;

  let best = null;

  for (const item of catalog) {
    const aliases = uniq([
      item.displayName,
      item.canonicalKey,
      ...(item.aliases || []),
    ]);

    for (const alias of aliases) {
      const score = phraseScore(query, alias);

      if (!best || score > best.score) {
        best = {
          canonicalKey: item.canonicalKey,
          displayName: item.displayName,
          groupKey: item.groupKey,
          groupLabel: item.groupLabel,
          matchedAlias: alias,
          score,
        };
      }
    }
  }

  if (!best || best.score < 0.72) return null;

  return best;
};

const buildVariantMatchQuery = ({ modelKey, variant = "", includeArchived = false } = {}) => {
  const query = getActiveVariantQuery({ modelKey, includeArchived });

  if (!variant) return query;

  const variantNorm = slug(variant);

  query.$or = [
    { variantKey: variantNorm },
    { variant: new RegExp(`^${escapeRegExp(variant)}$`, "i") },
    { variant: new RegExp(escapeRegExp(variant), "i") },
  ];

  return query;
};

const sortVariantRows = (rows = []) =>
  [...rows].sort((a, b) => {
    const ap = Number(a.priceMin || a.priceMax || 0);
    const bp = Number(b.priceMin || b.priceMax || 0);

    if (ap && bp && ap !== bp) return ap - bp;
    if (ap && !bp) return -1;
    if (!ap && bp) return 1;

    return clean(a.variant).localeCompare(clean(b.variant));
  });


const FUEL_ALIASES = [
  { key: "diesel", patterns: [/\bdiesel\b/i] },
  { key: "petrol", patterns: [/\bpetrol\b/i, /\bgasoline\b/i] },
  { key: "cng", patterns: [/\bcng\b/i] },
  { key: "electric", patterns: [/\belectric\b/i, /\bev\b/i] },
];

const detectFuelIntent = (value = "") => {
  const raw = clean(value);

  for (const fuel of FUEL_ALIASES) {
    if (fuel.patterns.some((pattern) => pattern.test(raw))) {
      return fuel.key;
    }
  }

  return "";
};

const stripFuelFromVariantName = (value = "") =>
  clean(value)
    .replace(/\b(diesel|petrol|gasoline|cng|electric|ev)\b/gi, " ")
    .replace(/\s+/g, " ")
    .trim();

const variantHasFuel = (variantName = "", fuel = "") => {
  const raw = normalizeText(variantName);
  if (!fuel) return false;

  if (fuel === "electric") {
    return /\b(electric|ev)\b/i.test(clean(variantName));
  }

  return raw.split(" ").includes(fuel);
};

const isExactVariantTextMatch = (requested = "", candidate = "") =>
  slug(requested) === slug(candidate);

const resolveRequestedComparisonVariants = ({
  requestedVariants = [],
  allRows = [],
} = {}) => {
  const requested = requestedVariants.map((item) => ({
    raw: clean(item),
    key: slug(item),
    fuel: detectFuelIntent(item),
    base: stripFuelFromVariantName(item),
  }));

  const explicitFuels = uniq(requested.map((item) => item.fuel).filter(Boolean));
  const sharedFuel = explicitFuels.length === 1 ? explicitFuels[0] : "";

  const usedKeys = new Set();

  const findBestForRequest = (request) => {
    const candidateTexts = [];

    if (sharedFuel && !request.fuel) {
      candidateTexts.push(`${request.raw} ${sharedFuel}`);
      if (request.base && request.base !== request.raw) {
        candidateTexts.push(`${request.base} ${sharedFuel}`);
      }
    }

    candidateTexts.push(request.raw);

    if (request.base && request.base !== request.raw) {
      candidateTexts.push(request.base);
    }

    // 1) Exact candidate text match, fuel-aligned first.
    for (const text of candidateTexts) {
      const found = allRows.find(
        (row) =>
          !usedKeys.has(row.variantKey) &&
          (isExactVariantTextMatch(text, row.variant) ||
            isExactVariantTextMatch(text, row.variantKey)),
      );

      if (found) return found;
    }

    // 2) Base exact + shared fuel match.
    if (sharedFuel && !request.fuel) {
      const found = allRows.find(
        (row) =>
          !usedKeys.has(row.variantKey) &&
          slug(stripFuelFromVariantName(row.variant)) === slug(request.base || request.raw) &&
          variantHasFuel(row.variant, sharedFuel),
      );

      if (found) return found;
    }

    // 3) Score fallback, but keep it conservative.
    let best = null;

    for (const row of allRows) {
      if (usedKeys.has(row.variantKey)) continue;

      const rowBase = stripFuelFromVariantName(row.variant);
      let score = 0;

      if (slug(row.variant) === request.key) score = 1;
      else if (slug(rowBase) === slug(request.base || request.raw)) score = 0.9;
      else score = phraseScore(request.raw, row.variant);

      if (request.fuel && !variantHasFuel(row.variant, request.fuel)) {
        score -= 0.25;
      }

      if (sharedFuel && !request.fuel && variantHasFuel(row.variant, sharedFuel)) {
        score += 0.12;
      }

      if (!best || score > best.score) {
        best = { row, score };
      }
    }

    return best?.score >= 0.78 ? best.row : null;
  };

  const resolved = [];

  for (const request of requested) {
    const found = findBestForRequest(request);
    if (!found) continue;

    usedKeys.add(found.variantKey);
    resolved.push({
      requested: request.raw,
      resolved: found,
      fuelAligned:
        Boolean(sharedFuel) &&
        !request.fuel &&
        variantHasFuel(found.variant, sharedFuel),
    });
  }

  return {
    sharedFuel,
    requested,
    resolved,
    rows: resolved.map((item) => item.resolved),
  };
};


export const getModelFeatureExplorerV2 = async ({
  model = "",
  variant = "",
  groupKey = "",
  search = "",
  limitFeatures = 300,
  includeArchived = false,
  includeUnavailableFeatures = false,
} = {}) => {
  const db = getDb();
  const resolvedModel = await resolveFeatureModelV2({ model });

  if (!resolvedModel) {
    return {
      ok: false,
      intent: "vehicle_model_features_explorer",
      reason: "model_not_found",
      answer: `I could not identify the car model from “${model}”.`,
      data: null,
    };
  }

  const query = buildVariantMatchQuery({
    modelKey: resolvedModel.modelKey,
    variant,
    includeArchived,
  });

  const rawVariants = await db
    .collection(MATRIX_COLLECTION)
    .find(query)
    .project({
      brand: 1,
      model: 1,
      variant: 1,
      variantKey: 1,
      variantFull: 1,
      lifecycleStatus: 1,
      activeForFeatureExplorer: 1,
      priceMin: 1,
      priceMax: 1,
      activePricelistMatched: 1,
      fuels: 1,
      transmissions: 1,
      imageUrl: 1,
      featureGroups: 1,
      featuresByKey: 1,
    })
    .toArray();

  const variants = sortVariantRows(rawVariants);

  if (!variants.length) {
    return {
      ok: false,
      intent: "vehicle_model_features_explorer",
      reason: variant ? "variant_not_found_or_inactive" : "active_variants_not_found",
      answer: variant
        ? buildInactiveVariantCopy({
            model: resolvedModel.model,
            variant,
          })
        : `I couldn’t find current feature details for ${resolvedModel.model} yet.`,
      data: {
        model: resolvedModel.model,
        modelKey: resolvedModel.modelKey,
        includeArchived,
      },
    };
  }

  const defaultVariantPool = variants.filter(
    (row) => row.activeForFeatureExplorer && row.activePricelistMatched,
  );

  // For feature explorer, default to a practical mid-priced/value variant,
  // not the base variant. The user can still switch variant from dropdown.
  const selectedVariant =
    defaultVariantPool[Math.floor(defaultVariantPool.length * 0.45)] ||
    variants.filter((row) => row.activeForFeatureExplorer)[
      Math.floor(variants.filter((row) => row.activeForFeatureExplorer).length * 0.45)
    ] ||
    variants[Math.floor(variants.length / 2)] ||
    variants[0];

  const normalizedSearch = normalizeText(search);
  const normalizedGroup = normalizeText(groupKey);

  const featureMap = new Map();

  for (const row of variants) {
    for (const [featureKey, feature] of Object.entries(row.featuresByKey || {})) {
      if (!feature) continue;

      if (normalizedGroup && normalizeText(feature.groupKey) !== normalizedGroup) {
        continue;
      }

      if (
        normalizedSearch &&
        !normalizeText([feature.displayName, feature.value, feature.groupLabel].join(" ")).includes(
          normalizedSearch,
        )
      ) {
        continue;
      }

      const customerKey = customerFeatureKey({
        featureKey,
        displayName: feature.displayName,
      });
      const userDisplayName = customerFeatureLabel(feature.displayName);

      if (!featureMap.has(customerKey)) {
        featureMap.set(customerKey, {
          featureKey: customerKey,
          sourceFeatureKeys: [featureKey],
          displayName: userDisplayName,
          groupKey: feature.groupKey,
          groupLabel: feature.groupLabel,
          section: feature.section,
          availableCount: 0,
          values: {},
        });
      }

      const entry = featureMap.get(customerKey);

      if (!entry.sourceFeatureKeys.includes(featureKey)) {
        entry.sourceFeatureKeys.push(featureKey);
      }

      const nextValue = {
        variant: row.variant,
        value: feature.value,
        available: feature.available,
        availabilityStatus: feature.availabilityStatus,
        conflictStatus: feature.conflictStatus,
      };

      const existingValue = entry.values[row.variantKey];

      if (
        !existingValue ||
        (nextValue.available === true && existingValue.available !== true) ||
        (existingValue.value === "Not Available" && nextValue.value !== "Not Available")
      ) {
        entry.values[row.variantKey] = nextValue;
      }
    }
  }

  for (const entry of featureMap.values()) {
    entry.availableCount = Object.values(entry.values || {}).filter(
      (item) => item.available === true,
    ).length;
  }

  const allFeatures = [...featureMap.values()].sort((a, b) => {
    const groupA = a.groupLabel || "";
    const groupB = b.groupLabel || "";
    if (groupA !== groupB) return groupA.localeCompare(groupB);
    return a.displayName.localeCompare(b.displayName);
  });

  const visibleFeatures =
    normalizedSearch || includeUnavailableFeatures
      ? allFeatures
      : allFeatures.filter((feature) => feature.availableCount > 0);

  const features = visibleFeatures.slice(0, limitFeatures);

  const groups = Object.values(
    features.reduce((acc, item) => {
      const key = item.groupKey || "other";

      if (!acc[key]) {
        acc[key] = {
          key,
          label: item.groupLabel || "Other",
          count: 0,
        };
      }

      acc[key].count += 1;
      return acc;
    }, {}),
  );

  const leadingQuestions = buildLeadingQuestionsV2({
    model: resolvedModel.model,
    variant: variant || selectedVariant?.variant || "",
    city: "new-delhi",
  });

  return {
    ok: true,
    intent: "vehicle_model_features_explorer",
    displayMode: "canvas",
    canvasType: "features_explorer_canvas",
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    inlineType: "feature_explorer_summary",
    title: `${resolvedModel.model} features`,
    answer: buildExplorerCopy({
      model: resolvedModel.model,
      variants,
      features,
      includeArchived,
    }),
    data: {
      brand: resolvedModel.brand,
      model: resolvedModel.model,
      modelKey: resolvedModel.modelKey,
      selectedVariantKey: selectedVariant.variantKey,
      selectedVariant: selectedVariant.variant,
      variants: variants.map((row) => ({
        variant: row.variant,
        variantKey: row.variantKey,
        lifecycleStatus: row.lifecycleStatus,
        activeForFeatureExplorer: row.activeForFeatureExplorer,
        priceMin: row.priceMin || 0,
        priceMax: row.priceMax || 0,
        priceLabel: formatPrice(row.priceMin || row.priceMax),
        activePricelistMatched: row.activePricelistMatched,
        fuels: row.fuels || [],
        transmissions: row.transmissions || [],
        imageUrl: row.imageUrl || "",
      })),
      groups,
      features,
      stats: {
        variantCount: variants.length,
        featureCount: features.length,
        activePricelistMatchedCount: variants.filter((row) => row.activePricelistMatched).length,
        archivedIncluded: includeArchived,
      },
    },
  };
};

export const answerModelFeatureV2 = async ({
  model = "",
  feature = "",
  variant = "",
  includeArchived = false,
} = {}) => {
  const db = getDb();
  const resolvedModel = await resolveFeatureModelV2({ model });
  const resolvedFeature = await resolveFeatureKeyV2({ feature });

  if (!resolvedModel || !resolvedFeature) {
    return {
      ok: false,
      intent: "vehicle_feature_answer",
      leadingQuestions: [],
    conversationSuggestions: [],
    inlineType: "feature_answer_card",
    leadingQuestions: [],
    conversationSuggestions: [],
      reason: !resolvedModel ? "model_not_found" : "feature_not_found",
      answer: !resolvedModel
        ? `I could not identify the car model from “${model}”.`
        : `I could not safely match “${feature}” to a feature in the database.`,
      data: {
        resolvedModel,
        resolvedFeature,
      },
    };
  }

  const query = buildVariantMatchQuery({
    modelKey: resolvedModel.modelKey,
    variant,
    includeArchived,
  });

  const rows = await db
    .collection(MATRIX_COLLECTION)
    .find(query)
    .project({
      brand: 1,
      model: 1,
      variant: 1,
      variantKey: 1,
      lifecycleStatus: 1,
      activeForFeatureExplorer: 1,
      priceMin: 1,
      priceMax: 1,
      activePricelistMatched: 1,
      [`featuresByKey.${resolvedFeature.canonicalKey}`]: 1,
    })
    .toArray();

  const sortedRows = sortVariantRows(rows);

  const mapped = sortedRows
    .map((row) => {
      const f = row.featuresByKey?.[resolvedFeature.canonicalKey];
      if (!f) return null;

      return {
        variant: row.variant,
        variantKey: row.variantKey,
        lifecycleStatus: row.lifecycleStatus,
        activeForFeatureExplorer: row.activeForFeatureExplorer,
        priceMin: row.priceMin || 0,
        priceMax: row.priceMax || 0,
        priceLabel: formatPrice(row.priceMin || row.priceMax),
        activePricelistMatched: row.activePricelistMatched,
        featureKey: resolvedFeature.canonicalKey,
        feature: customerFeatureLabel(f.displayName),
        value: f.value,
        available: f.available,
        availabilityStatus: f.availabilityStatus,
        conflictStatus: f.conflictStatus,
      };
    })
    .filter(Boolean);

  const availableRows = mapped.filter((row) => row.available === true);
  const unavailableRows = mapped.filter((row) => row.available === false);
  const conflictedRows = mapped.filter(
    (row) => row.availabilityStatus === "conflicted" || row.conflictStatus === "conflicted",
  );

  const cheapest = availableRows
    .filter((row) => row.priceMin)
    .sort((a, b) => a.priceMin - b.priceMin)[0];

  const targetName = variant
    ? `${resolvedModel.model} ${clean(variant)}`
    : resolvedModel.model;

  const answer = buildFeatureAnswerCopy({
    model: resolvedModel.model,
    variant,
    featureName: customerFeatureLabel(resolvedFeature.displayName),
    mapped,
    availableRows,
    unavailableRows,
    conflictedRows,
    cheapest,
  });

  const leadingQuestions = buildLeadingQuestionsV2({
    model: resolvedModel.model,
    variant,
    city: "new-delhi",
  });
  return {
    ok: true,
    intent: "vehicle_feature_answer",
    displayMode: "inline",
    inlineType: "feature_answer_card",
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    title: customerFeatureLabel(resolvedFeature.displayName),
    answer,
    data: {
      model: resolvedModel.model,
      modelKey: resolvedModel.modelKey,
      requestedVariant: variant || "",
      featureKey: resolvedFeature.canonicalKey,
      featureName: customerFeatureLabel(resolvedFeature.displayName),
      resolvedFeature,
      rows: mapped,
      availableRows,
      unavailableRows,
      conflictedRows,
      cheapestAvailableVariant: cheapest || null,
      stats: {
        totalRows: mapped.length,
        availableRows: availableRows.length,
        unavailableRows: unavailableRows.length,
        conflictedRows: conflictedRows.length,
        activeOnly: !includeArchived,
      },
    },
  };
};

export const discoverFeatureVariantsV2 = async ({
  model = "",
  feature = "",
  includeMissing = false,
  cheapestOnly = false,
  includeArchived = false,
} = {}) => {
  const result = await answerModelFeatureV2({
    model,
    feature,
    includeArchived,
  });

  if (!result.ok) return result;

  const rows = includeMissing
    ? result.data.unavailableRows
    : result.data.availableRows;

  const sorted = sortVariantRows(rows);

  const finalRows = cheapestOnly ? sorted.slice(0, 1) : sorted;
  const titleMode = includeMissing ? "without" : "with";

  const leadingQuestions = buildLeadingQuestionsV2({
    model: result.data.model,
    variant: "",
    city: "new-delhi",
  });

  return {
    ok: true,
    intent: "vehicle_feature_discovery",
    displayMode: "canvas",
    canvasType: "feature_match_builder_canvas",
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    inlineType: "feature_discovery_summary",
    title: cheapestOnly
      ? `Most affordable ${result.data.model} with ${customerFeatureLabel(result.data.featureName)}`
      : `${result.data.model} variants ${titleMode} ${customerFeatureLabel(result.data.featureName)}`,
    answer: buildFeatureDiscoveryCopy({
      model: result.data.model,
      featureName: customerFeatureLabel(result.data.featureName),
      rows: finalRows,
      includeMissing,
      cheapestOnly,
    }),
    data: {
      ...result.data,
      rows: finalRows,
      includeMissing,
      cheapestOnly,
      stats: {
        ...result.data.stats,
        returnedRows: finalRows.length,
      },
    },
  };
};

export const compareVariantFeaturesV2 = async ({
  model = "",
  variants = [],
  featureKeys = [],
  includeArchived = false,
} = {}) => {
  const db = getDb();
  const resolvedModel = await resolveFeatureModelV2({ model });

  if (!resolvedModel) {
    return {
      ok: false,
      intent: "vehicle_feature_comparison",
      reason: "model_not_found",
      answer: `I could not identify the car model from “${model}”.`,
      data: null,
    };
  }

  const query = getActiveVariantQuery({
    modelKey: resolvedModel.modelKey,
    includeArchived,
  });

  const allRows = await db
    .collection(MATRIX_COLLECTION)
    .find(query)
    .project({
      brand: 1,
      model: 1,
      variant: 1,
      variantKey: 1,
      lifecycleStatus: 1,
      activeForFeatureExplorer: 1,
      priceMin: 1,
      priceMax: 1,
      activePricelistMatched: 1,
      featuresByKey: 1,
    })
    .toArray();

  const allSortedRows = sortVariantRows(allRows);

  const resolvedComparison = variants.length
    ? resolveRequestedComparisonVariants({
        requestedVariants: variants,
        allRows: allSortedRows,
      })
    : {
        sharedFuel: "",
        requested: [],
        resolved: [],
        rows: allSortedRows.slice(0, 3),
      };

  const selected = sortVariantRows(resolvedComparison.rows);

  if (variants.length && selected.length < 2) {
    return {
      ok: false,
      intent: "vehicle_feature_comparison",
      reason: "variants_not_found_or_inactive",
      answer: `I could not confidently match both active variants for ${resolvedModel.model}.`,
      data: {
        model: resolvedModel.model,
        requestedVariants: variants,
        matchedVariants: selected.map((row) => row.variant),
        variantResolution: resolvedComparison.resolved?.map((item) => ({
          requested: item.requested,
          resolved: item.resolved.variant,
          fuelAligned: item.fuelAligned,
        })),
        activeOnly: !includeArchived,
      },
    };
  }

  const compareSet = variants.length ? selected : allSortedRows.slice(0, 3);

  const allFeatureKeys = uniq(
    compareSet.flatMap((row) => Object.keys(row.featuresByKey || {})),
  ).filter((key) => !featureKeys.length || featureKeys.includes(key));

  const comparisonRows = allFeatureKeys.map((featureKey) => {
    const firstFeature = compareSet
      .map((row) => row.featuresByKey?.[featureKey])
      .find(Boolean);

    return {
      featureKey,
      displayName: customerFeatureLabel(firstFeature?.displayName || featureKey),
      groupKey: firstFeature?.groupKey || "other",
      groupLabel: firstFeature?.groupLabel || "Other",
      values: Object.fromEntries(
        compareSet.map((row) => {
          const feature = row.featuresByKey?.[featureKey];

          return [
            row.variantKey,
            {
              variant: row.variant,
              value: feature?.value || "Not Available",
              available: feature?.available === true,
              availabilityStatus: feature?.availabilityStatus || "not_available",
              conflictStatus: feature?.conflictStatus || "clean",
            },
          ];
        }),
      ),
    };
  });

  const differenceRows = comparisonRows.filter((row) => {
    const values = Object.values(row.values || {}).map((item) =>
      `${item.value}|${item.available}|${item.availabilityStatus}`,
    );
    return new Set(values).size > 1;
  });

  const leadingQuestions = buildLeadingQuestionsV2({
    model: resolvedModel.model,
    variant: compareSet?.[0]?.variant || "",
    city: "new-delhi",
  });

  return {
    ok: true,
    intent: "vehicle_feature_comparison",
    displayMode: "canvas",
    canvasType: "comparison_canvas",
    leadingQuestions,
    conversationSuggestions: leadingQuestions,
    inlineType: "feature_comparison_summary",
    title: `${resolvedModel.model} feature comparison`,
    answer: buildComparisonCopy({
      model: resolvedModel.model,
      compareSet,
      differenceRows,
    }),
    data: {
      model: resolvedModel.model,
      modelKey: resolvedModel.modelKey,
      requestedVariants: variants,
      variantResolution: resolvedComparison.resolved?.map((item) => ({
        requested: item.requested,
        resolved: item.resolved.variant,
        variantKey: item.resolved.variantKey,
        fuelAligned: item.fuelAligned,
      })) || [],
      sharedFuelContext: resolvedComparison.sharedFuel || "",
      variants: compareSet.map((row) => ({
        variant: row.variant,
        variantKey: row.variantKey,
        lifecycleStatus: row.lifecycleStatus,
        activeForFeatureExplorer: row.activeForFeatureExplorer,
        priceMin: row.priceMin,
        priceMax: row.priceMax,
        priceLabel: formatPrice(row.priceMin || row.priceMax),
        activePricelistMatched: row.activePricelistMatched,
      })),
      rows: comparisonRows,
      differenceRows,
      stats: {
        comparedVariants: compareSet.length,
        totalRows: comparisonRows.length,
        differenceRows: differenceRows.length,
        activeOnly: !includeArchived,
      },
    },
  };
};

export default {
  resolveFeatureModelV2,
  resolveFeatureKeyV2,
  getModelFeatureExplorerV2,
  answerModelFeatureV2,
  discoverFeatureVariantsV2,
  compareVariantFeaturesV2,
};
