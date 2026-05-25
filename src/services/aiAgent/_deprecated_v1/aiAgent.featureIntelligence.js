import mongoose from "mongoose";

const CACHE_TTL_MS = 10 * 60 * 1000;

let cachedLexicon = null;
let cachedAt = 0;

const FEATURE_COLLECTION = "vehicle_features";
const VEHICLE_COLLECTION = "vehicles";

const STOP_WORDS = new Set([
  "does", "do", "is", "are", "have", "has", "had", "with", "without",
  "available", "availability", "come", "comes", "get", "gets", "got",
  "in", "on", "of", "the", "a", "an", "me", "mein", "hai", "h", "kya",
  "ka", "ki", "ke", "kis", "kaun", "kaunsa", "kaunsi", "variant",
  "variants", "model", "car", "cars", "show", "tell", "check", "list",
  "all", "please", "plz", "under", "lakh", "lakhs",
]);

const STATIC_FEATURE_ALIASES = {
  sunroof: [
    "sunroof",
    "sun roof",
    "single pane sunroof",
    "panoramic sunroof",
    "panaromic sunroof",
    "moonroof",
    "moon roof",
    "roof window",
    "sunrrof",
    "sonroof",
    "sunnroof",
    "sunrof",
    "sunarrof",
  ],
  adas: [
    "adas",
    "addas",
    "advanced driver assistance",
    "driver assistance",
    "lane assist",
    "lane keep assist",
    "lane departure warning",
    "adaptive cruise",
    "adaptive cruise control",
    "collision warning",
    "forward collision warning",
    "autonomous emergency braking",
    "aeb",
    "blind spot",
  ],
  airbags: [
    "airbag",
    "airbags",
    "6 airbags",
    "six airbags",
    "side airbags",
    "curtain airbags",
    "driver airbag",
    "passenger airbag",
    "aribag",
    "air bags",
  ],
  rear_camera: [
    "rear camera",
    "reverse camera",
    "reversing camera",
    "parking camera",
    "rear parking camera",
    "back camera",
    "camera",
    "camra",
  ],
  camera_360: [
    "360 camera",
    "360 degree camera",
    "360-degree camera",
    "surround camera",
    "surround view camera",
    "around view camera",
    "bird view camera",
    "360 camra",
  ],
  ventilated_seats: [
    "ventilated seats",
    "ventilated seat",
    "seat ventilation",
    "ventilation seats",
    "cooled seats",
    "cooling seats",
    "ventillated seats",
    "ventillated seets",
    "ventilated seets",
  ],
  wireless_charger: [
    "wireless charger",
    "wireless charging",
    "wireless mobile charger",
    "wireless phone charger",
    "phone charger",
    "mobile charger",
    "wireles charger",
    "wireless chargng",
  ],
  cruise_control: [
    "cruise control",
    "cruise",
    "adaptive cruise",
    "adaptive cruise control",
  ],
  connected_car: [
    "connected car",
    "connected features",
    "connected tech",
    "connected technology",
    "blue link",
    "bluelink",
    "kia connect",
    "connected app",
    "remote start",
    "remote features",
  ],
  alloy_wheels: [
    "alloy wheels",
    "alloys",
    "alloy",
    "diamond cut alloys",
    "diamond cut alloy wheels",
    "wheel alloys",
  ],
  led_headlamps: [
    "led headlamps",
    "led headlights",
    "headlamps",
    "headlights",
    "projector headlamps",
    "projector headlights",
    "led lights",
  ],
  rear_ac_vents: [
    "rear ac vents",
    "rear ac vent",
    "rear vents",
    "back ac vents",
    "rear seat ac vents",
    "ac vents",
  ],
  climate_control: [
    "climate control",
    "automatic climate control",
    "auto ac",
    "automatic ac",
  ],
  hill_hold: [
    "hill hold",
    "hill assist",
    "hill start assist",
    "hill start",
  ],
  tpms: [
    "tpms",
    "tyre pressure monitor",
    "tyre pressure monitoring",
    "tire pressure monitoring",
    "tyre pressure",
  ],
  premium_audio: [
    "bose speakers",
    "bose audio",
    "jbl speakers",
    "premium speakers",
    "branded audio",
    "sound system",
    "music system",
    "speakers",
  ],
  wireless_carplay: [
    "wireless carplay",
    "apple carplay",
    "android auto",
    "wireless android auto",
    "wireless apple carplay",
    "carplay",
  ],
};

const FEATURE_CATEGORY_ALIASES = {
  safety: ["safety", "safe", "family safety", "airbags", "ncap"],
  comfort: ["comfort", "convenience", "rear seat comfort", "seat comfort"],
  infotainment: ["infotainment", "entertainment", "music", "audio", "screen"],
  exterior: ["exterior", "outside", "lights", "headlamps", "wheels"],
  performance: ["engine", "performance", "power", "torque", "mileage"],
  dimensions: ["dimensions", "space", "boot", "ground clearance", "capacity"],
};

const normalizeText = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactText = (value = "") => normalizeText(value).replace(/\s+/g, "");

const titleCase = (value = "") =>
  String(value || "")
    .trim()
    .replace(/\w\S*/g, (word) => word.charAt(0).toUpperCase() + word.slice(1).toLowerCase());

const uniq = (items = []) => [...new Set(items.filter(Boolean))];

const toArray = (value) => {
  if (!value) return [];
  return Array.isArray(value) ? value.filter(Boolean) : [value].filter(Boolean);
};

const clean = (value = "") => String(value || "").replace(/\s+/g, " ").trim();

const numberOrZero = (value) => {
  const num = Number(value || 0);
  return Number.isFinite(num) ? num : 0;
};

const formatCurrencyLakh = (amount) => {
  const num = numberOrZero(amount);
  if (!num) return "";
  if (num >= 100000) {
    const lakh = num / 100000;
    return `₹${lakh.toFixed(lakh >= 10 ? 2 : 2)}L`;
  }
  return `₹${num.toLocaleString("en-IN")}`;
};

const damerauLevenshtein = (a = "", b = "") => {
  const s = normalizeText(a);
  const t = normalizeText(b);

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

      if (
        i > 1 &&
        j > 1 &&
        s[i - 1] === t[j - 2] &&
        s[i - 2] === t[j - 1]
      ) {
        dp[i][j] = Math.min(dp[i][j], dp[i - 2][j - 2] + cost);
      }
    }
  }

  return dp[s.length][t.length];
};

const tokenSetScore = (a = "", b = "") => {
  const aTokens = normalizeText(a).split(" ").filter(Boolean);
  const bTokens = normalizeText(b).split(" ").filter(Boolean);

  if (!aTokens.length || !bTokens.length) return 0;

  const aSet = new Set(aTokens);
  const bSet = new Set(bTokens);
  const intersection = [...aSet].filter((token) => bSet.has(token)).length;

  const precision = intersection / aSet.size;
  const recall = intersection / bSet.size;

  if (!precision || !recall) return 0;

  return (2 * precision * recall) / (precision + recall);
};

const editScore = (a = "", b = "") => {
  const x = compactText(a);
  const y = compactText(b);
  if (!x || !y) return 0;
  if (x === y) return 1;
  if (x.includes(y) || y.includes(x)) {
    const small = Math.min(x.length, y.length);
    const large = Math.max(x.length, y.length);
    return Math.min(0.96, 0.78 + (small / large) * 0.18);
  }

  const maxLen = Math.max(x.length, y.length);
  const distance = damerauLevenshtein(x, y);

  return Math.max(0, 1 - distance / maxLen);
};

const combinedScore = (a = "", b = "") => {
  const exactA = normalizeText(a);
  const exactB = normalizeText(b);
  if (!exactA || !exactB) return 0;
  if (exactA === exactB) return 1;

  const compactA = compactText(a);
  const compactB = compactText(b);
  if (compactA === compactB) return 0.99;

  return Math.max(
    editScore(a, b),
    tokenSetScore(a, b),
    compactA.includes(compactB) || compactB.includes(compactA) ? 0.9 : 0,
  );
};

const getDb = () => {
  if (!mongoose.connection?.db) {
    throw new Error("MongoDB connection is not ready for feature intelligence.");
  }
  return mongoose.connection.db;
};

const extractFeatureRowsFromDoc = (doc = {}) => {
  const variant =
    clean(doc.variant) ||
    clean(doc.variantName) ||
    clean(doc.variant_name) ||
    clean(doc.version) ||
    clean(doc.trim) ||
    "";

  const make = clean(doc.make || doc.brand || doc.manufacturer);
  const model = clean(doc.model || doc.modelName || doc.vehicleModel);

  const featureArrays = [
    doc.features,
    doc.featureList,
    doc.rows,
    doc.items,
    doc.specs,
    doc.specifications,
  ];

  const nested = featureArrays.flatMap(toArray);

  if (nested.length) {
    return nested.map((feature, index) => ({
      make,
      model,
      variant,
      section: clean(feature.section || feature.category || feature.group || doc.section),
      featureName: clean(
        feature.feature ||
          feature.name ||
          feature.label ||
          feature.title ||
          feature.featureName ||
          feature.key ||
          "",
      ),
      value: clean(
        feature.value ||
          feature.displayValue ||
          feature.featureValue ||
          feature.status ||
          "",
      ),
      available:
        feature.available ??
        feature.present ??
        feature.included ??
        !/not available|no|absent|false/i.test(
          clean(feature.value || feature.displayValue || feature.status || ""),
        ),
      raw: feature,
      doc,
      index,
    })).filter((row) => row.featureName);
  }

  const directName = clean(
    doc.feature ||
      doc.matchedFeature ||
      doc.featureName ||
      doc.name ||
      doc.label ||
      doc.title ||
      "",
  );

  if (!directName) return [];

  return [
    {
      make,
      model,
      variant,
      section: clean(doc.section || doc.category || doc.group),
      featureName: directName,
      value: clean(doc.value || doc.displayValue || doc.featureValue || doc.status),
      available:
        doc.available ??
        doc.present ??
        doc.included ??
        !/not available|no|absent|false/i.test(
          clean(doc.value || doc.displayValue || doc.status || ""),
        ),
      raw: doc,
      doc,
      index: 0,
    },
  ];
};

const getVehiclePrice = (row = {}) =>
  numberOrZero(
    row.exShowroomPrice ||
      row.ex_showroom_price ||
      row.exShowroom ||
      row.ex_showroom ||
      row.price ||
      row.onRoadPrice ||
      row.on_road_price ||
      0,
  );

const normalizeVariantLabel = (value = "") =>
  clean(value)
    .replace(/\s+/g, " ")
    .replace(/\s+\)/g, ")")
    .replace(/\(\s+/g, "(");

const maybeActiveVehicle = (row = {}) => {
  if (row.is_discontinued === true) return false;
  if (row.discontinued === true) return false;
  if (row.active === false) return false;
  if (row.is_active === false) return false;
  if (/discontinued/i.test(clean(row.status || row.activeStatus))) return false;
  return true;
};

const buildAliasEntries = (canonical, aliases = [], source = "static") =>
  uniq([canonical, ...aliases]).map((alias) => ({
    canonical,
    alias,
    normalized: normalizeText(alias),
    compact: compactText(alias),
    source,
  }));

export const buildFeatureIntelligenceLexicon = async ({ force = false } = {}) => {
  const now = Date.now();

  if (!force && cachedLexicon && now - cachedAt < CACHE_TTL_MS) {
    return cachedLexicon;
  }

  const db = getDb();

  const [vehicleFeatureDocs, vehicleDocs] = await Promise.all([
    db
      .collection(FEATURE_COLLECTION)
      .find(
        {},
        {
          projection: {
            make: 1,
            brand: 1,
            model: 1,
            variant: 1,
            variantName: 1,
            variant_name: 1,
            version: 1,
            trim: 1,
            features: 1,
            featureList: 1,
            rows: 1,
            items: 1,
            specs: 1,
            specifications: 1,
            feature: 1,
            matchedFeature: 1,
            featureName: 1,
            name: 1,
            label: 1,
            section: 1,
            category: 1,
            value: 1,
            displayValue: 1,
            available: 1,
            present: 1,
            included: 1,
          },
        },
      )
      .limit(25000)
      .toArray(),

    db
      .collection(VEHICLE_COLLECTION)
      .find(
        {},
        {
          projection: {
            make: 1,
            brand: 1,
            model: 1,
            model_normalized: 1,
            variant: 1,
            variant_short: 1,
            variant_normalized: 1,
            version: 1,
            fuel: 1,
            transmission: 1,
            exShowroomPrice: 1,
            ex_showroom_price: 1,
            ex_showroom: 1,
            onRoadPrice: 1,
            on_road_price: 1,
            price: 1,
            imageUrl: 1,
            displayImageUrl: 1,
            normalizedImageUrl: 1,
            heroImageUrl: 1,
            active: 1,
            is_active: 1,
            is_discontinued: 1,
            discontinued: 1,
            status: 1,
          },
        },
      )
      .limit(50000)
      .toArray(),
  ]);

  const featureRows = vehicleFeatureDocs.flatMap(extractFeatureRowsFromDoc);

  const modelMap = new Map();
  const variantMapByModel = new Map();
  const featureNameSet = new Set();
  const sectionSet = new Set();

  for (const row of featureRows) {
    if (row.model) {
      const key = normalizeText(row.model);
      if (!modelMap.has(key)) {
        modelMap.set(key, {
          make: row.make,
          model: row.model,
          aliases: uniq([row.model, `${row.make} ${row.model}`]),
          source: FEATURE_COLLECTION,
        });
      }
    }

    if (row.model && row.variant) {
      const modelKey = normalizeText(row.model);
      if (!variantMapByModel.has(modelKey)) variantMapByModel.set(modelKey, new Map());
      const variantKey = normalizeText(row.variant);
      if (!variantMapByModel.get(modelKey).has(variantKey)) {
        variantMapByModel.get(modelKey).set(variantKey, {
          label: normalizeVariantLabel(row.variant),
          aliases: uniq([row.variant]),
          source: FEATURE_COLLECTION,
        });
      }
    }

    if (row.featureName) featureNameSet.add(row.featureName);
    if (row.section) sectionSet.add(row.section);
  }

  for (const row of vehicleDocs) {
    const model = clean(row.model || row.model_normalized);
    if (model) {
      const key = normalizeText(model);
      const make = clean(row.make || row.brand);
      const existing = modelMap.get(key) || {
        make,
        model,
        aliases: [],
        source: VEHICLE_COLLECTION,
      };

      existing.make = existing.make || make;
      existing.model = existing.model || model;
      existing.aliases = uniq([
        ...existing.aliases,
        model,
        make ? `${make} ${model}` : "",
        row.model_normalized,
      ]);

      modelMap.set(key, existing);
    }

    if (model) {
      const modelKey = normalizeText(model);
      if (!variantMapByModel.has(modelKey)) variantMapByModel.set(modelKey, new Map());

      const variant = clean(row.variant_short || row.variant || row.variant_normalized || row.version);
      if (variant) {
        const variantKey = normalizeText(variant);
        const existing = variantMapByModel.get(modelKey).get(variantKey) || {
          label: normalizeVariantLabel(variant),
          aliases: [],
          source: VEHICLE_COLLECTION,
        };

        existing.aliases = uniq([
          ...existing.aliases,
          row.variant_short,
          row.variant,
          row.variant_normalized,
          row.version,
        ]);

        variantMapByModel.get(modelKey).set(variantKey, existing);
      }
    }
  }

  const featureAliases = [];

  for (const [canonical, aliases] of Object.entries(STATIC_FEATURE_ALIASES)) {
    featureAliases.push(...buildAliasEntries(canonical, aliases, "static"));
  }

  for (const featureName of featureNameSet) {
    const normalized = normalizeText(featureName);

    let canonical = normalized;
    let bestStatic = null;
    let bestScore = 0;

    for (const [staticCanonical, aliases] of Object.entries(STATIC_FEATURE_ALIASES)) {
      for (const alias of aliases) {
        const score = combinedScore(featureName, alias);
        if (score > bestScore) {
          bestScore = score;
          bestStatic = staticCanonical;
        }
      }
    }

    if (bestStatic && bestScore >= 0.84) {
      canonical = bestStatic;
    }

    featureAliases.push(...buildAliasEntries(canonical, [featureName], "db"));
  }

  for (const [category, aliases] of Object.entries(FEATURE_CATEGORY_ALIASES)) {
    featureAliases.push(...buildAliasEntries(`category:${category}`, aliases, "category"));
  }

  const modelEntries = [...modelMap.values()].map((model) => ({
    ...model,
    normalized: normalizeText(model.model),
    compact: compactText(model.model),
    aliasEntries: model.aliases.map((alias) => ({
      alias,
      normalized: normalizeText(alias),
      compact: compactText(alias),
    })),
  }));

  const variantEntriesByModel = new Map(
    [...variantMapByModel.entries()].map(([modelKey, variantMap]) => [
      modelKey,
      [...variantMap.values()].map((variant) => ({
        ...variant,
        normalized: normalizeText(variant.label),
        compact: compactText(variant.label),
        aliasEntries: variant.aliases.map((alias) => ({
          alias,
          normalized: normalizeText(alias),
          compact: compactText(alias),
        })),
      })),
    ]),
  );

  cachedLexicon = {
    builtAt: new Date().toISOString(),
    modelEntries,
    variantEntriesByModel,
    featureAliases,
    featureRows,
    vehicleDocs,
    stats: {
      models: modelEntries.length,
      featureAliases: featureAliases.length,
      featureRows: featureRows.length,
      vehicleDocs: vehicleDocs.length,
      rawFeatureNames: featureNameSet.size,
      sections: sectionSet.size,
    },
  };

  cachedAt = now;
  return cachedLexicon;
};

const scoreCandidateAgainstQuery = (queryNorm, queryCompact, candidate = {}) => {
  let best = 0;
  let matchedAlias = "";

  const aliases = candidate.aliasEntries || [
    {
      alias: candidate.alias,
      normalized: normalizeText(candidate.alias),
      compact: compactText(candidate.alias),
    },
  ];

  for (const alias of aliases) {
    const aliasNorm = alias.normalized || normalizeText(alias.alias);
    const aliasCompact = alias.compact || compactText(alias.alias);

    if (!aliasNorm) continue;

    let score = combinedScore(queryNorm, aliasNorm);

    if (queryNorm.includes(aliasNorm)) score = Math.max(score, 0.96);
    if (queryCompact.includes(aliasCompact)) score = Math.max(score, 0.94);

    const queryTokens = queryNorm.split(" ").filter((token) => !STOP_WORDS.has(token));
    const aliasTokens = aliasNorm.split(" ").filter(Boolean);

    if (aliasTokens.every((token) => queryTokens.includes(token))) {
      score = Math.max(score, 0.93);
    }

    if (score > best) {
      best = score;
      matchedAlias = alias.alias;
    }
  }

  return {
    score: best,
    matchedAlias,
  };
};

const resolveModel = (query, lexicon) => {
  const queryNorm = normalizeText(query);
  const queryCompact = compactText(query);

  let best = null;

  for (const entry of lexicon.modelEntries) {
    const score = scoreCandidateAgainstQuery(queryNorm, queryCompact, entry);

    if (!best || score.score > best.score) {
      best = {
        ...entry,
        score: score.score,
        matchedAlias: score.matchedAlias,
      };
    }
  }

  if (!best || best.score < 0.72) return null;

  return best;
};

const resolveVariant = (query, lexicon, model) => {
  if (!model) return null;

  const queryNorm = normalizeText(query);
  const queryCompact = compactText(query);
  const modelKey = normalizeText(model.model);
  const variants = lexicon.variantEntriesByModel.get(modelKey) || [];

  let best = null;

  for (const entry of variants) {
    const score = scoreCandidateAgainstQuery(queryNorm, queryCompact, entry);

    if (!best || score.score > best.score) {
      best = {
        ...entry,
        score: score.score,
        matchedAlias: score.matchedAlias,
      };
    }
  }

  if (!best || best.score < 0.76) return null;

  return best;
};

const resolveFeature = (query, lexicon) => {
  const queryNorm = normalizeText(query);
  const queryCompact = compactText(query);

  let best = null;

  for (const entry of lexicon.featureAliases) {
    const aliasNorm = entry.normalized;
    const aliasCompact = entry.compact;

    if (!aliasNorm) continue;

    let score = combinedScore(queryNorm, aliasNorm);

    if (queryNorm.includes(aliasNorm)) score = Math.max(score, 0.96);
    if (queryCompact.includes(aliasCompact)) score = Math.max(score, 0.94);

    const aliasTokens = aliasNorm.split(" ").filter((token) => !STOP_WORDS.has(token));
    const queryTokens = queryNorm.split(" ").filter((token) => !STOP_WORDS.has(token));

    if (aliasTokens.length && aliasTokens.every((token) => queryTokens.includes(token))) {
      score = Math.max(score, 0.93);
    }

    if (!best || score > best.score) {
      best = {
        ...entry,
        score,
        matchedAlias: entry.alias,
      };
    }
  }

  if (!best || best.score < 0.7) return null;

  return best;
};

const detectIntentShape = ({ query, model, variant, feature }) => {
  const q = normalizeText(query);
  const compact = compactText(query);

  const hasFeatureWord = /\b(features?|specs?|equipment|kit|loaded)\b/.test(q);
  const hasCategory =
    /\b(safety|comfort|convenience|infotainment|entertainment|exterior|interior|engine|performance|dimensions|space)\b/.test(q) ||
    feature?.canonical?.startsWith("category:");

  const hasDiscoveryPhrase =
    /\b(which|what|list|show|kis|kaun|kaunsa|kaunsi)\b.*\b(variant|variants|trim|trims)\b/.test(q) ||
    /\b(variant|variants|trim|trims)\b.*\b(with|having|have|get|gets|milta|hai)\b/.test(q) ||
    /\bavailable\b.*\bwhich\b.*\bvariant\b/.test(q) ||
    /\bcheapest|lowest|least expensive|starts from|start from|base variant with\b/.test(q) ||
    /\bvariants?\b$/.test(q) ||
    /\bvariant\b/.test(q) && /\bwith|having|have|get|gets|available|hai|milta\b/.test(q);

  const hasNegativeDiscovery =
    /\b(without|do not|dont|don't|not have|miss|missing|lacks|lack)\b/.test(q) &&
    /\bvariant|variants|trims|which\b/.test(q);

  const hasComparison =
    /\b(compare|comparison|vs|versus|difference|extra|upgrade)\b/.test(q);

  const hasExplorer =
    hasFeatureWord &&
    !feature?.canonical?.startsWith("category:") &&
    (
      /\b(show|list|open|all|full|features?|specs?|equipment)\b/.test(q) ||
      q.endsWith("features") ||
      q.endsWith("featuers")
    ) &&
    !feature;

  const hasValueQuestion =
    /\b(type|kind|single pane|panoramic|panaromic|or|only|level|levels|what type)\b/.test(q);

  const hasAvailabilityPhrase =
    /\b(does|do|is|are|have|has|available|come|comes|get|gets|hai|milta|milt[ai]|aata|ata)\b/.test(q);

  if (hasComparison) return "feature_comparison";
  if (hasNegativeDiscovery) return "feature_negative_discovery";
  if (hasDiscoveryPhrase) return "feature_discovery";
  if (hasCategory && model) return "feature_category";
  if (hasExplorer && model) return "feature_explorer";
  if (hasValueQuestion && model && feature) return "feature_value";
  if (model && variant && feature) return "variant_feature_answer";
  if (model && feature && (hasAvailabilityPhrase || compact.length <= 32)) return "model_feature_answer";
  if (model && feature) return "model_feature_answer";
  if (model && hasFeatureWord) return "feature_explorer";

  return "";
};

const getRowsForModel = (lexicon, model) => {
  const modelKey = normalizeText(model?.model || model || "");

  return lexicon.featureRows.filter((row) => normalizeText(row.model) === modelKey);
};

const getVehiclesForModel = (lexicon, model) => {
  const modelKey = normalizeText(model?.model || model || "");

  return lexicon.vehicleDocs
    .filter((row) => normalizeText(row.model || row.model_normalized) === modelKey)
    .filter(maybeActiveVehicle);
};

const scoreFeatureRow = (row, feature) => {
  if (!feature) return 0;

  const rowName = normalizeText(row.featureName);
  const rowValue = normalizeText(row.value);
  const target = normalizeText(feature.canonical?.replace(/^category:/, "") || feature.alias || "");
  const alias = normalizeText(feature.alias || "");

  if (feature.canonical?.startsWith("category:")) {
    const category = feature.canonical.replace("category:", "");
    const section = normalizeText(row.section);

    if (section.includes(category)) return 1;
    return combinedScore(section, category);
  }

  let score = Math.max(
    combinedScore(rowName, target),
    combinedScore(rowName, alias),
    rowName.includes(target) ? 0.96 : 0,
    target.includes(rowName) ? 0.92 : 0,
  );

  if (rowValue.includes(target) || rowValue.includes(alias)) {
    score = Math.max(score, 0.82);
  }

  return score;
};

const findMatchingFeatureRows = ({ lexicon, model, feature, negative = false }) => {
  const allRows = getRowsForModel(lexicon, model);

  const matched = allRows
    .map((row) => ({
      ...row,
      featureScore: scoreFeatureRow(row, feature),
    }))
    .filter((row) => row.featureScore >= 0.74)
    .map((row) => ({
      ...row,
      available: negative ? row.available === false : row.available !== false,
    }));

  if (negative) {
    return matched.filter((row) => row.available === false);
  }

  return matched.filter((row) => row.available !== false);
};

const buildVehiclePriceMap = (lexicon, model) => {
  const vehicles = getVehiclesForModel(lexicon, model);
  const map = new Map();

  for (const row of vehicles) {
    const variant = normalizeText(row.variant_short || row.variant || row.variant_normalized || row.version);
    if (!variant) continue;

    const existing = map.get(variant);
    const price = getVehiclePrice(row);

    if (!existing || (price && price < existing.exShowroomPrice)) {
      map.set(variant, {
        variant: normalizeVariantLabel(row.variant_short || row.variant || row.variant_normalized || row.version),
        fuel: clean(row.fuel),
        transmission: clean(row.transmission),
        exShowroomPrice: price,
        onRoadPrice: numberOrZero(row.onRoadPrice || row.on_road_price),
        priceLabel: formatCurrencyLakh(price),
        imageUrl:
          row.displayImageUrl ||
          row.normalizedImageUrl ||
          row.heroImageUrl ||
          row.imageUrl ||
          "",
      });
    }
  }

  return map;
};

const enrichRows = ({ rows, lexicon, model, feature }) => {
  const priceMap = buildVehiclePriceMap(lexicon, model);
  const vehicleRows = getVehiclesForModel(lexicon, model);

  return rows
    .map((row, index) => {
      const variantKey = normalizeText(row.variant);
      const priceInfo = priceMap.get(variantKey) || {};
      const fallbackVehicle = vehicleRows.find((vehicle) =>
        normalizeText(vehicle.variant_short || vehicle.variant || vehicle.variant_normalized || vehicle.version) === variantKey,
      );

      const exShowroomPrice =
        priceInfo.exShowroomPrice ||
        getVehiclePrice(row.raw || row.doc || {}) ||
        getVehiclePrice(fallbackVehicle || {});

      return {
        id: `${normalizeText(model.model)}-${variantKey || index}-${normalizeText(feature?.canonical || row.featureName)}`,
        variant: normalizeVariantLabel(row.variant || priceInfo.variant || `Variant ${index + 1}`),
        variantName: normalizeVariantLabel(row.variant || priceInfo.variant || `Variant ${index + 1}`),
        label: normalizeVariantLabel(row.variant || priceInfo.variant || `Variant ${index + 1}`),
        make: model.make || row.make || "",
        brand: model.make || row.make || "",
        model: model.model,
        feature: titleCase((feature?.canonical || row.featureName || "").replace(/^category:/, "")),
        matchedFeature: row.featureName,
        section: row.section || "",
        value: row.value || (row.available === false ? "Not Available" : "Available"),
        displayValue: row.value || (row.available === false ? "Not Available" : "Available"),
        available: row.available !== false,
        present: row.available !== false,
        included: row.available !== false,
        fuel: priceInfo.fuel || clean(fallbackVehicle?.fuel),
        transmission: priceInfo.transmission || clean(fallbackVehicle?.transmission),
        exShowroomPrice,
        onRoadPrice: priceInfo.onRoadPrice || numberOrZero(fallbackVehicle?.onRoadPrice || fallbackVehicle?.on_road_price),
        price: exShowroomPrice,
        priceLabel: priceInfo.priceLabel || formatCurrencyLakh(exShowroomPrice),
        imageUrl:
          priceInfo.imageUrl ||
          fallbackVehicle?.displayImageUrl ||
          fallbackVehicle?.normalizedImageUrl ||
          fallbackVehicle?.heroImageUrl ||
          fallbackVehicle?.imageUrl ||
          "",
        featureScore: row.featureScore,
      };
    })
    .sort((a, b) => {
      const priceA = numberOrZero(a.exShowroomPrice);
      const priceB = numberOrZero(b.exShowroomPrice);

      if (priceA && priceB && priceA !== priceB) return priceA - priceB;
      if (priceA && !priceB) return -1;
      if (!priceA && priceB) return 1;

      return normalizeText(a.variant).localeCompare(normalizeText(b.variant));
    });
};

const uniqueByVariant = (rows = []) => {
  const seen = new Set();

  return rows.filter((row) => {
    const key = normalizeText(row.variant);
    if (!key || seen.has(key)) return false;
    seen.add(key);
    return true;
  });
};

const getFeatureLabel = (feature) =>
  titleCase(
    String(feature?.canonical || feature?.alias || "feature")
      .replace(/^category:/, "")
      .replace(/_/g, " "),
  );

const buildAnswerText = ({ routeType, model, variant, feature, rows, checkedRow }) => {
  const modelLabel = model.model;
  const featureLabel = getFeatureLabel(feature);
  const count = rows.length;
  const first = rows[0];

  if (routeType === "variant_feature_answer") {
    if (checkedRow?.available) {
      return `Yes — ${modelLabel} ${variant.label} gets ${checkedRow.displayValue && checkedRow.displayValue !== "Available" ? `${checkedRow.displayValue} ${featureLabel}` : featureLabel}.`;
    }

    const startText = first?.variant ? `${featureLabel} starts from ${first.variant}.` : `I could not find ${featureLabel} on this variant.`;

    return `No — ${modelLabel} ${variant.label} does not get ${featureLabel}. ${startText}`;
  }

  if (routeType === "feature_discovery" || routeType === "feature_negative_discovery") {
    if (!count) return `I could not find current ${modelLabel} variants matching ${featureLabel}.`;

    return `I found ${count} ${modelLabel} variants with ${featureLabel}. Showing the lowest-priced matches first.`;
  }

  if (routeType === "feature_value") {
    if (!count) return `I could not find ${featureLabel} details for current ${modelLabel} variants.`;

    const values = uniq(rows.map((row) => row.displayValue).filter(Boolean)).slice(0, 3);
    return `${modelLabel} comes with ${values.join(" / ") || featureLabel}.`;
  }

  if (!count) {
    return `No — current ${modelLabel} variants do not list ${featureLabel}.`;
  }

  return `Yes — ${modelLabel} offers ${featureLabel} on ${count} current variants. The lowest matched variant is ${first?.variant || "available in selected variants"}.`;
};

const buildFeatureAnswerResponse = ({ resolution, rows, checkedRow = null }) => {
  const { routeType, model, variant, feature, confidence, correctedQuery, originalQuery } = resolution;

  const answerType =
    routeType === "variant_feature_answer"
      ? checkedRow?.available
        ? "variant_feature_positive"
        : "variant_feature_negative"
      : rows.length
        ? "model_feature_available"
        : "model_feature_not_available";

  const answer = buildAnswerText({
    routeType,
    model,
    variant,
    feature,
    rows,
    checkedRow,
  });

  const featureLabel = getFeatureLabel(feature);
  const displayRows =
    routeType === "variant_feature_answer"
      ? [
          checkedRow || {
            variant: variant?.label || "",
            available: false,
            displayValue: "Not Available",
          },
          ...rows.filter((row) => normalizeText(row.variant) !== normalizeText(variant?.label)).slice(0, 5),
        ]
      : rows.slice(0, 8);

  const widget = {
    intent: "vehicle_feature_answer",
    inlineType: "feature_answer_card",
    answerType,
    title: `${featureLabel} in ${model.model}`,
    answer,
    confidence,
    correctedQuery,
    originalQuery,
    vehicle: {
      make: model.make,
      brand: model.make,
      model: model.model,
      displayName: [model.make, model.model].filter(Boolean).join(" "),
      imageUrl: rows[0]?.imageUrl || "",
    },
    feature: featureLabel,
    checkedVariant: variant
      ? {
          label: variant.label,
          hasFeature: Boolean(checkedRow?.available),
          featureValue: checkedRow?.displayValue || "Not Available",
        }
      : null,
    rows: displayRows,
    matchedVariants: rows,
    actions: [
      {
        id: "feature-see-variants",
        label: `See ${featureLabel.toLowerCase()} variants`,
        intent: "vehicle_feature_discovery",
        canvasType: "feature_match_builder_canvas",
        query: `Which ${model.model} variants have ${featureLabel}?`,
      },
      {
        id: "feature-open-explorer",
        label: "Open Features Explorer",
        intent: "vehicle_model_features_explorer",
        canvasType: "features_explorer_canvas",
        query: `Show features of ${model.model}`,
      },
    ],
    leadingQuestions: [
      `Which ${model.model} variant has ${featureLabel}?`,
      `${featureLabel} vs no ${featureLabel}`,
      `Show all features of ${model.model}`,
    ],
  };

  return {
    intent: "vehicle_feature_answer",
    displayMode: "inline",
    inlineType: "feature_answer_card",
    title: widget.title,
    answer,
    answerType,
    widget,
    widgets: [widget],
    rows: displayRows,
    actions: widget.actions,
    leadingQuestions: widget.leadingQuestions,
    contextPatch: {
      selectedVehicle: widget.vehicle,
      anchorMake: model.make,
      anchorModel: model.model,
      anchorVariant: variant?.label || "",
      anchorFeature: featureLabel,
      anchorCity: "new-delhi",
    },
    sourceTransparency: {
      modulesChecked: [FEATURE_COLLECTION, VEHICLE_COLLECTION],
      responseTool: "deterministic_feature_prerouter",
      matched: rows.length,
      dataSource: "mongodb",
    },
    runtimeResultsMeta: [
      {
        tool: "deterministic_feature_prerouter",
        source: FEATURE_COLLECTION,
        modulesChecked: [FEATURE_COLLECTION, VEHICLE_COLLECTION],
        matched: rows.length,
        error: "",
      },
    ],
    meta: {
      deterministic: true,
      confidence,
      correctedQuery,
      originalQuery,
      routeType,
    },
  };
};

const buildFeatureDiscoveryResponse = ({ resolution, rows }) => {
  const { routeType, model, feature, confidence, correctedQuery, originalQuery } = resolution;
  const featureLabel = getFeatureLabel(feature);
  const answer = buildAnswerText({
    routeType,
    model,
    feature,
    rows,
  });

  const widget = {
    intent: "vehicle_feature_discovery",
    canvasType: "feature_match_builder_canvas",
    answerType: rows.length ? "feature_discovery_matches" : "feature_discovery_no_matches",
    title: `${featureLabel} variants in ${model.model}`,
    answer,
    confidence,
    correctedQuery,
    originalQuery,
    vehicle: {
      make: model.make,
      brand: model.make,
      model: model.model,
      displayName: [model.make, model.model].filter(Boolean).join(" "),
      imageUrl: rows[0]?.imageUrl || "",
    },
    feature: featureLabel,
    rows,
    items: rows,
    matchedVariants: rows,
    totalMatches: rows.length,
    actions: [
      {
        id: "feature-open-explorer",
        label: "Open Features Explorer",
        intent: "vehicle_model_features_explorer",
        canvasType: "features_explorer_canvas",
        query: `Show features of ${model.model}`,
      },
    ],
    leadingQuestions: [
      `Cheapest ${model.model} variant with ${featureLabel}`,
      `Does base ${model.model} get ${featureLabel}?`,
      `Show all features of ${model.model}`,
    ],
  };

  return {
    intent: "vehicle_feature_discovery",
    displayMode: "canvas",
    canvasType: "feature_match_builder_canvas",
    title: widget.title,
    answer,
    widget,
    widgets: [widget],
    rows,
    items: rows,
    actions: widget.actions,
    leadingQuestions: widget.leadingQuestions,
    contextPatch: {
      selectedVehicle: widget.vehicle,
      anchorMake: model.make,
      anchorModel: model.model,
      anchorFeature: featureLabel,
      anchorCity: "new-delhi",
    },
    sourceTransparency: {
      modulesChecked: [FEATURE_COLLECTION, VEHICLE_COLLECTION],
      responseTool: "deterministic_feature_prerouter",
      matched: rows.length,
      dataSource: "mongodb",
    },
    runtimeResultsMeta: [
      {
        tool: "deterministic_feature_prerouter",
        source: FEATURE_COLLECTION,
        modulesChecked: [FEATURE_COLLECTION, VEHICLE_COLLECTION],
        matched: rows.length,
        error: "",
      },
    ],
    meta: {
      deterministic: true,
      confidence,
      correctedQuery,
      originalQuery,
      routeType,
    },
  };
};

export const resolveFeatureQueryDeterministically = async ({
  message = "",
  context = {},
  force = false,
} = {}) => {
  const originalQuery = clean(message);
  const contextModel =
    context?.anchorModel ||
    context?.selectedVehicle?.model ||
    context?.vehicle?.model ||
    "";

  const contextMake =
    context?.anchorMake ||
    context?.selectedVehicle?.make ||
    context?.selectedVehicle?.brand ||
    "";

  const queryForResolution = originalQuery || "";
  const lexicon = await buildFeatureIntelligenceLexicon({ force });

  const model =
    resolveModel(queryForResolution, lexicon) ||
    (contextModel
      ? {
          make: contextMake,
          model: contextModel,
          score: 0.82,
          matchedAlias: contextModel,
          source: "context",
        }
      : null);

  const feature =
    resolveFeature(queryForResolution, lexicon) ||
    (context?.anchorFeature
      ? {
          canonical: normalizeText(context.anchorFeature),
          alias: context.anchorFeature,
          matchedAlias: context.anchorFeature,
          score: 0.78,
          source: "context",
        }
      : null);

  const variant = resolveVariant(queryForResolution, lexicon, model);

  const routeType = detectIntentShape({
    query: queryForResolution,
    model,
    variant,
    feature,
  });

  const confidenceParts = [
    model?.score || 0,
    feature?.score || (routeType === "feature_explorer" ? 0.8 : 0),
    routeType ? 0.9 : 0,
  ];

  if (variant) confidenceParts.push(variant.score);

  const confidence =
    confidenceParts.reduce((sum, score) => sum + score, 0) / confidenceParts.length;

  const correctedQuery = [
    model?.model,
    variant?.label,
    feature?.canonical?.replace(/^category:/, "").replace(/_/g, " "),
  ].filter(Boolean).join(" ");

  return {
    handled: Boolean(model && routeType && confidence >= 0.78),
    originalQuery,
    correctedQuery,
    confidence: Number(confidence.toFixed(3)),
    routeType,
    model,
    variant,
    feature,
    lexiconStats: lexicon.stats,
  };
};

export const runDeterministicFeaturePreRouter = async ({
  message = "",
  context = {},
  debug = false,
} = {}) => {
  const resolution = await resolveFeatureQueryDeterministically({
    message,
    context,
  });

  if (!resolution.handled) {
    return {
      handled: false,
      resolution,
    };
  }

  if (
    ![
      "model_feature_answer",
      "variant_feature_answer",
      "feature_discovery",
      "feature_negative_discovery",
      "feature_value",
      "feature_category",
    ].includes(resolution.routeType)
  ) {
    return {
      handled: false,
      resolution,
    };
  }

  const lexicon = await buildFeatureIntelligenceLexicon();

  const negative = resolution.routeType === "feature_negative_discovery";

  let rows = findMatchingFeatureRows({
    lexicon,
    model: resolution.model,
    feature: resolution.feature,
    negative,
  });

  rows = uniqueByVariant(
    enrichRows({
      rows,
      lexicon,
      model: resolution.model,
      feature: resolution.feature,
    }),
  );

  let checkedRow = null;

  if (resolution.variant) {
    checkedRow =
      rows.find((row) => normalizeText(row.variant) === normalizeText(resolution.variant.label)) ||
      null;
  }

  const isDiscovery =
    resolution.routeType === "feature_discovery" ||
    resolution.routeType === "feature_negative_discovery" ||
    /which|variant|variants|cheapest|lowest|kis/i.test(message);

  const response = isDiscovery
    ? buildFeatureDiscoveryResponse({
        resolution,
        rows,
      })
    : buildFeatureAnswerResponse({
        resolution,
        rows,
        checkedRow,
      });

  if (debug) {
    response.debug = {
      ...(response.debug || {}),
      deterministicFeatureResolution: resolution,
    };
  }

  return {
    handled: true,
    resolution,
    response,
  };
};

export default {
  buildFeatureIntelligenceLexicon,
  resolveFeatureQueryDeterministically,
  runDeterministicFeaturePreRouter,
};
