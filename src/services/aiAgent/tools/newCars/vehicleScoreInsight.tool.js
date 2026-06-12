import {
  renderAciLanguageText,
} from "../../../aciCore/language/aciAnswerLanguageComposer.js";
import mongoose from "mongoose";
import scoreInsightService from "../../../aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs";
import crossModelScoreDiagnosticService from "../../../aciCore/scoreProfiles/aciCrossModelScoreDiagnostic.service.cjs";

const {
  getVariantScoreInsight,
  getModelScoreInsights,
  getSameFamilyValueInsights,
  getTopScoreInsights,
  getScoreProfileCoverage,
} = scoreInsightService;

const {
  buildCrossModelScoreDiagnostic,
} = crossModelScoreDiagnosticService;

const decisionLanguageText = (templateKey = "", input = {}) =>
  renderAciLanguageText(templateKey, input, {
    seed: [TOOL_NAME, templateKey, input.operation, input.modelText, input.scope].filter(Boolean).join("|"),
  });

const TOOL_NAME = "vehicle_score_insight";

const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || "aci_vehicle_variant_score_profile";

const SCORE_PATH_BY_MODULE = {
  safety: "safetyScore.score",
  features: "featureScore.score",
  feature: "featureScore.score",
  performance: "performanceScore.score",
  mileage: "mileageRunningCostScore.score",
  running_cost: "mileageRunningCostScore.score",
  practicality: "practicalityScore.score",
  city: "cityUseScore.score",
  city_use: "cityUseScore.score",
  highway: "highwayUseScore.score",
  highway_use: "highwayUseScore.score",
  premium: "premiumComfortScore.score",
  comfort: "premiumComfortScore.score",
  value: "valueScore.score",
  regret: "regretRisk.riskScore",
  regret_risk: "regretRisk.riskScore",
};

const OPERATION_BY_TOOL_NAME = {
  vehicle_score_insight: "variant_score_insight",
  vehicle_score_profile: "variant_score_insight",
  vehicle_model_score_insights: "model_score_insights",
  vehicle_same_family_value_insights: "same_family_value_insights",
  vehicle_variant_upgrade_insight: "variant_upgrade_insight",
  vehicle_top_score_insights: "top_module_score_insights",
  vehicle_cross_model_score_diagnostic: "cross_model_score_diagnostic",
  vehicle_model_score_comparison: "cross_model_score_diagnostic",
};

const normalizeKey = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

const escapeRegex = (value = "") =>
  String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const hasNormalizedPhrase = (normalizedText = "", phrase = "") => {
  const normalizedPhrase = normalizeKey(phrase);
  if (!normalizedText || !normalizedPhrase) return false;
  return new RegExp(`(^|_)${escapeRegex(normalizedPhrase)}(_|$)`).test(normalizedText);
};

const getNormalizedPhraseIndex = (normalizedText = "", phrase = "") => {
  const normalizedPhrase = normalizeKey(phrase);
  if (!normalizedText || !normalizedPhrase) return -1;
  const match = normalizedText.match(
    new RegExp(`(^|_)${escapeRegex(normalizedPhrase)}(?=_|$)`),
  );
  if (!match) return -1;
  return match.index + (match[1] ? 1 : 0);
};

const getScoreProfileDocKey = (doc = {}) =>
  doc.scoreProfileKey || doc.variantProfileKey || [doc.makeKey, doc.modelKey, doc.variantKey, doc.fuelKey, doc.transmissionKey].map(normalizeKey).join("|");

const getFamilyKey = (doc = {}) =>
  [
    normalizeKey(doc.makeKey),
    normalizeKey(doc.modelKey),
    normalizeKey(doc.fuelKey),
    normalizeKey(doc.transmissionKey),
  ].join("|");

const safeLimit = (value, fallback = 20, max = 80) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
};

const firstValue = (...values) =>
  values.find((value) => value !== undefined && value !== null && value !== "");


const inferScoreInsightOperationFromText = (text = "", fallback = "variant_score_insight") => {
  const plainText = String(text || "").trim().toLowerCase();
  const normalized = normalizeKey(text);
  if (!plainText && !normalized) return fallback;

  const hasUpgradeIntent =
    /\b(worth over|worth upgrading|upgrade|gain from|what do i gain|pay extra|extra over|over|vs|versus| or )\b/i.test(plainText);

  const hasVariantCompareIntent =
    /\b(sigma|delta|zeta|alpha|base|top trim|higher trim|lower trim)\b/i.test(plainText) &&
    /\b(worth|gain|upgrade|over|vs|versus| or |buy)\b/i.test(plainText);

  if (hasUpgradeIntent && hasVariantCompareIntent) return "variant_upgrade_insight";

  const hasModelSummaryIntent =
    /\b(overall|how good|good family car|family car|family use|city driving|city use|daily use|best .* city|best value|most sensible|should i consider|which variant should i consider|model summary)\b/i.test(plainText) ||
    (/\bgood\b/i.test(plainText) && /\b(overall|family)\b/i.test(plainText));

  if (hasModelSummaryIntent) return "model_score_insights";

  const hasValueIntent =
    /\b(value|worth|better value|best value|better variant|which variant)\b/i.test(plainText) ||
    normalized.includes("better_value") ||
    normalized.includes("best_value") ||
    normalized.includes("which_variant") ||
    normalized.includes("better_variant");

  const hasFamilyIntent =
    /\b(which|best|better|rank|ranking|ladder|variant|variants|trim|trims)\b/i.test(plainText) ||
    /\b(manual|automatic|amt|cvt|dct|ivt|petrol|diesel|cng|ev)\b/i.test(plainText);

  if (hasValueIntent && hasFamilyIntent) return "same_family_value_insights";

  return fallback;
};

const normalizeRuntimeArgs = (args = {}) => {
  const toolPlan = args.toolPlan || {};
  const input = toolPlan.input || toolPlan.args || toolPlan.params || {};
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};
  const anchors = args.plan?.meaningFrame?.anchors || args.context?.meaningFrame?.anchors || {};
  const primaryVehicle = anchors.primaryVehicle || anchors.vehicle || {};
  const selectedVehicle =
    args.context?.selectedVehicle ||
    args.context?.vehicle ||
    args.context?.aciSelectedVehicle ||
    {};

  const makeValue = firstValue(
    args.makeKey, args.make,
    input.makeKey, input.make,
    toolPlan.makeKey, toolPlan.make,
    entities.makeKey, entities.make,
    filters.makeKey, filters.make,
    primaryVehicle.makeKey, primaryVehicle.make,
    selectedVehicle.makeKey, selectedVehicle.make
  );

  const modelValue = firstValue(
    args.modelKey, args.model,
    input.modelKey, input.model,
    toolPlan.modelKey, toolPlan.model,
    entities.modelKey, entities.model, entities.primaryModel,
    filters.modelKey, filters.model,
    Array.isArray(entities.models) ? entities.models[0] : null,
    Array.isArray(filters.models) ? filters.models[0] : null,
    primaryVehicle.modelKey, primaryVehicle.model,
    selectedVehicle.modelKey, selectedVehicle.model
  );

  const variantValue = firstValue(
    args.variantKey, args.variant,
    input.variantKey, input.variant,
    toolPlan.variantKey, toolPlan.variant,
    entities.variantKey, entities.variant, entities.primaryVariant,
    filters.variantKey, filters.variant,
    Array.isArray(entities.variants) ? entities.variants[0] : null,
    Array.isArray(filters.variants) ? filters.variants[0] : null,
    primaryVehicle.variantKey, primaryVehicle.variant,
    selectedVehicle.variantKey, selectedVehicle.variant
  );

  const fuelValue = firstValue(
    args.fuelKey, args.fuel,
    input.fuelKey, input.fuel,
    toolPlan.fuelKey, toolPlan.fuel,
    entities.fuelKey, entities.fuel,
    filters.fuelKey, filters.fuel,
    primaryVehicle.fuelKey, primaryVehicle.fuel,
    selectedVehicle.fuelKey, selectedVehicle.fuel
  );

  const transmissionValue = firstValue(
    args.transmissionKey, args.transmission,
    input.transmissionKey, input.transmission,
    toolPlan.transmissionKey, toolPlan.transmission,
    entities.transmissionKey, entities.transmission,
    filters.transmissionKey, filters.transmission,
    primaryVehicle.transmissionKey, primaryVehicle.transmission,
    selectedVehicle.transmissionKey, selectedVehicle.transmission
  );

  const userText = firstValue(
    args.userMessage,
    args.message,
    input.userMessage,
    input.message,
    toolPlan.userMessage,
    toolPlan.message
  );

  const explicitOperation = firstValue(
    args.operation,
    input.operation,
    toolPlan.operation
  );

  const inferredOperation = inferScoreInsightOperationFromText(
    userText,
    OPERATION_BY_TOOL_NAME[toolPlan.tool] || OPERATION_BY_TOOL_NAME[args.tool] || "variant_score_insight"
  );

  return {
    ...input,
    ...toolPlan,
    ...args,
    operation: firstValue(
      explicitOperation,
      inferredOperation,
      OPERATION_BY_TOOL_NAME[toolPlan.tool],
      OPERATION_BY_TOOL_NAME[args.tool],
      "variant_score_insight"
    ),
    makeKey: normalizeKey(makeValue),
    modelKey: normalizeKey(modelValue),
    variantKey: normalizeKey(variantValue),
    fuelKey: normalizeKey(fuelValue),
    transmissionKey: normalizeKey(transmissionValue),
    fuelTransmissionFamilyKey: firstValue(
      args.fuelTransmissionFamilyKey,
      input.fuelTransmissionFamilyKey,
      toolPlan.fuelTransmissionFamilyKey
    ),
    scoreProfileKey: firstValue(args.scoreProfileKey, input.scoreProfileKey, toolPlan.scoreProfileKey),
    variantProfileKey: firstValue(args.variantProfileKey, input.variantProfileKey, toolPlan.variantProfileKey),
    module: firstValue(args.module, input.module, toolPlan.module),
    scoreModule: firstValue(args.scoreModule, input.scoreModule, toolPlan.scoreModule),
    scorePath: firstValue(args.scorePath, input.scorePath, toolPlan.scorePath),
    limit: firstValue(args.limit, input.limit, toolPlan.limit),
    direction: firstValue(args.direction, input.direction, toolPlan.direction),
    filters: firstValue(args.filters, input.filters, toolPlan.filters, {}),
    userMessage: firstValue(
      args.userMessage,
      args.message,
      input.userMessage,
      input.message,
      toolPlan.userMessage,
      toolPlan.message
    ),
    db: args.db,
  };
};

const buildScoreProfileKey = ({
  makeKey,
  modelKey,
  variantKey,
  fuelKey,
  transmissionKey,
} = {}) => {
  const make = normalizeKey(makeKey);
  const model = normalizeKey(modelKey);
  const variant = normalizeKey(variantKey);
  const fuel = normalizeKey(fuelKey);
  const transmission = normalizeKey(transmissionKey);

  if (!make || !model || !variant || !fuel || !transmission) return null;
  return `${make}_${model}__${variant}__${fuel}_${transmission}`;
};

const pickScorePath = ({ scorePath, module, scoreModule } = {}) => {
  if (scorePath) return String(scorePath);
  const key = normalizeKey(module || scoreModule);
  return SCORE_PATH_BY_MODULE[key] || null;
};

const normalizeCrossModelTarget = (target = {}) => {
  if (typeof target === "string") {
    return { modelKey: normalizeKey(target) };
  }

  return {
    makeKey: normalizeKey(target.makeKey || target.make_key || target.make || ""),
    modelKey: normalizeKey(target.modelKey || target.model_key || target.model || ""),
    fuelKey: normalizeKey(target.fuelKey || target.fuel_key || target.fuel || ""),
    transmissionKey: normalizeKey(
      target.transmissionKey || target.transmission_key || target.transmission || ""
    ),
  };
};

const collectCrossModelTargets = (args = {}, rawArgs = {}) => {
  const toolPlan = rawArgs.toolPlan || {};
  const input = toolPlan.input || toolPlan.args || toolPlan.params || {};
  const entities = toolPlan.entities || {};
  const filters = toolPlan.filters || {};

  const source =
    rawArgs.targets ||
    rawArgs.models ||
    rawArgs.comparisonModels ||
    input.targets ||
    input.models ||
    input.comparisonModels ||
    entities.targets ||
    entities.models ||
    entities.comparisonModels ||
    filters.targets ||
    filters.models ||
    filters.comparisonModels ||
    [];

  return (Array.isArray(source) ? source : [source])
    .map(normalizeCrossModelTarget)
    .filter((target) => target.modelKey);
};

const buildCrossModelScoreDiagnosticLine = (result = {}) => {
  const models = (Array.isArray(result.models) ? result.models : [])
    .map((model) => model.label || model.modelKey)
    .filter(Boolean);

  const scope = [result.scope?.fuelKey, result.scope?.transmissionKey]
    .filter(Boolean)
    .join(" ");

  const moduleLeaders = (Array.isArray(result.moduleComparisons) ? result.moduleComparisons : [])
    .filter((item) => item.comparedCount >= 2 && item.leader?.label)
    .slice(0, 5)
    .map((item) => {
      const delta = Number.isFinite(Number(item.delta)) ? ` by ${item.delta}` : "";
      return `${item.label}: ${item.leader.label}${delta}`;
    });

  const modelText = models.length >= 2 ? models.join(" vs ") : "selected models";
  const scopeText = scope ? ` (${scope})` : "";
  const leaderText = moduleLeaders.length
    ? ` Module signals: ${moduleLeaders.join("; ")}.`
    : "";

  const note = decisionLanguageText("decision_diagnostic_only_note", {
    operation: "cross_model_score_diagnostic",
    modelText,
    scope,
  });

  return `Diagnostic score comparison for ${modelText}${scopeText}.${leaderText} ${note}`.trim();
};

const createGuardrail = () => ({
  canUseForFinalRecommendation: false,
  reason: decisionLanguageText("decision_score_guardrail_reason", {
    operation: "score_guardrail",
  }),
});

const createSuccess = ({ operation, data, answer = null, meta = {} }) => ({
  tool: TOOL_NAME,
  status: "success",
  operation,
  answer,
  canvasType: "score_insight_canvas",
  inlineType: "score_insight_summary",
  usageGuardrail: createGuardrail(),
  data,
  meta: {
    source: "aci_vehicle_variant_score_profile",
    finalRecommendationEnabled: false,
    ...meta,
  },
});

const createError = ({ operation, code, message, meta = {} }) => ({
  tool: TOOL_NAME,
  status: "error",
  operation,
  error: {
    code,
    message,
  },
  canvasType: "score_insight_error",
  usageGuardrail: createGuardrail(),
  data: null,
  meta: {
    finalRecommendationEnabled: false,
    ...meta,
  },
});


let cachedScoreProfileLookupDocs = null;

const getScoreProfileLookupDb = (db) => {
  if (db) return db;
  if (mongoose.connection?.readyState === 1 && mongoose.connection?.db) {
    return mongoose.connection.db;
  }
  return null;
};

const SCORE_TEXT_STOP_WORDS = new Set([
  "is",
  "the",
  "a",
  "an",
  "good",
  "bad",
  "value",
  "worth",
  "strong",
  "weak",
  "at",
  "what",
  "which",
  "show",
  "score",
  "scores",
  "rating",
  "ratings",
  "variant",
  "car",
  "features",
  "feature",
  "and",
  "or",
  "in",
  "of",
  "for",
  "to",
]);

const getScoreProfileLookupDocs = async (db) => {
  if (cachedScoreProfileLookupDocs) return cachedScoreProfileLookupDocs;

  const resolvedDb = getScoreProfileLookupDb(db);
  if (!resolvedDb) return [];

  const col = resolvedDb.collection(SCORE_PROFILE_COLLECTION);

  cachedScoreProfileLookupDocs = await col
    .find(
      {},
      {
        projection: {
          _id: 0,
          scoreProfileKey: 1,
          variantProfileKey: 1,
          variantFullName: 1,
          makeKey: 1,
          modelKey: 1,
          variantKey: 1,
          fuelKey: 1,
          transmissionKey: 1,
          fuelTransmissionFamilyKey: 1,
        },
      }
    )
    .toArray();

  return cachedScoreProfileLookupDocs;
};

const resolveScoreProfileKeyFromMessage = async ({ db, userMessage = "" } = {}) => {
  const normalizedMessage = normalizeKey(userMessage);
  if (!normalizedMessage) return null;

  const messageTokens = new Set(
    normalizedMessage
      .split("_")
      .filter((token) => token && token.length >= 2 && !SCORE_TEXT_STOP_WORDS.has(token))
  );

  const docs = await getScoreProfileLookupDocs(db);
  let best = null;

  for (const doc of docs) {
    const makeKey = normalizeKey(doc.makeKey);
    const modelKey = normalizeKey(doc.modelKey);
    const variantKey = normalizeKey(doc.variantKey);
    const fuelKey = normalizeKey(doc.fuelKey);
    const transmissionKey = normalizeKey(doc.transmissionKey);
    const fullNameKey = normalizeKey(doc.variantFullName);

    const hasModel =
      (modelKey && messageTokens.has(modelKey)) ||
      (modelKey && normalizedMessage.includes(modelKey));

    const hasVariant =
      (variantKey && messageTokens.has(variantKey)) ||
      (variantKey && normalizedMessage.includes(variantKey));

    if (!hasModel || !hasVariant) continue;

    let score = 0;
    if (hasModel) score += 45;
    if (hasVariant) score += 40;
    if (makeKey && (messageTokens.has(makeKey) || normalizedMessage.includes(makeKey))) score += 8;
    if (fuelKey && (messageTokens.has(fuelKey) || normalizedMessage.includes(fuelKey))) score += 4;
    if (transmissionKey && (messageTokens.has(transmissionKey) || normalizedMessage.includes(transmissionKey))) score += 4;
    if (fullNameKey && normalizedMessage.includes(fullNameKey)) score += 30;

    if (!best || score > best.score) {
      best = { score, doc };
    }
  }

  if (!best || best.score < 80) return null;

  return {
    scoreProfileKey: best.doc.scoreProfileKey,
    variantProfileKey: best.doc.variantProfileKey,
    confidence: best.score,
    matchedVariantFullName: best.doc.variantFullName,
  };
};



const inferSameFamilyParamsFromMessage = async ({
  db,
  userMessage = "",
  makeKey: preferredMakeKey = "",
  modelKey: preferredModelKey = "",
  fuelKey: preferredFuelKey = "",
  transmissionKey: preferredTransmissionKey = "",
} = {}) => {
  const normalizedMessage = normalizeKey(userMessage);
  const normalizedPreferredModel = normalizeKey(preferredModelKey);
  const normalizedPreferredMake = normalizeKey(preferredMakeKey);
  if (!normalizedMessage && !normalizedPreferredModel) return {};

  const docs = await getScoreProfileLookupDocs(db);

  const modelScores = new Map();
  for (const doc of docs) {
    const modelKey = normalizeKey(doc.modelKey);
    if (!modelKey) continue;

    const makeKey = normalizeKey(doc.makeKey);
    let score = 0;

    if (normalizedPreferredModel && modelKey === normalizedPreferredModel) score += 80;
    if (normalizedPreferredMake && makeKey === normalizedPreferredMake) score += 10;
    if (hasNormalizedPhrase(normalizedMessage, modelKey)) score += 50;
    if (makeKey && hasNormalizedPhrase(normalizedMessage, makeKey)) score += 8;

    if (score <= 0) continue;

    const current = modelScores.get(modelKey) || { score: 0, doc };
    if (score > current.score) modelScores.set(modelKey, { score, doc });
  }

  const bestModel = [...modelScores.values()].sort((a, b) => b.score - a.score)[0]?.doc;
  if (!bestModel) return {};

  const fuelKey = normalizeKey(preferredFuelKey) || (
    /\bpetrol\b/i.test(userMessage) ? "petrol" :
    /\bdiesel\b/i.test(userMessage) ? "diesel" :
    /\bcng\b/i.test(userMessage) ? "cng" :
    /\b(?:ev|electric)\b/i.test(userMessage) ? "ev" :
    ""
  );

  const transmissionKey = normalizeKey(preferredTransmissionKey) || (
    /\b(?:manual|mt)\b/i.test(userMessage) ? "manual" :
    /\b(?:automatic|amt|cvt|dct|ivt|at)\b/i.test(userMessage) ? "automatic" :
    ""
  );

  return {
    makeKey: normalizeKey(bestModel.makeKey),
    modelKey: normalizeKey(bestModel.modelKey),
    fuelKey,
    transmissionKey,
    fuelTransmissionFamilyKey: fuelKey && transmissionKey ? `${fuelKey}_${transmissionKey}` : "",
  };
};


const getNumericScore = (insight = {}, moduleKey = "", fallbackPath = "") => {
  const modules = insight.modules || {};
  const value = modules[moduleKey]?.score;
  if (Number.isFinite(Number(value))) return Number(value);

  if (fallbackPath) {
    const parts = fallbackPath.split(".");
    let current = insight;
    for (const part of parts) current = current?.[part];
    if (Number.isFinite(Number(current))) return Number(current);
  }

  return null;
};

const scoreDifference = (targetInsight = {}, baseInsight = {}, moduleKey = "", fallbackPath = "") => {
  const targetScore = getNumericScore(targetInsight, moduleKey, fallbackPath);
  const baseScore = getNumericScore(baseInsight, moduleKey, fallbackPath);

  if (!Number.isFinite(targetScore) || !Number.isFinite(baseScore)) return null;
  return targetScore - baseScore;
};

const formatPriceDelta = (delta = 0) => {
  if (!Number.isFinite(Number(delta))) return "";
  const abs = Math.abs(Number(delta));
  if (abs >= 100000) return `₹${(abs / 100000).toFixed(2)}L`;
  return `₹${Math.round(abs).toLocaleString("en-IN")}`;
};

const scoreDeltaPhrase = (label, delta) => {
  if (!Number.isFinite(Number(delta)) || Math.abs(delta) < 0.5) return "";
  const direction = delta > 0 ? "improves" : "drops";
  return `${label} ${direction} by ${Math.abs(Number(delta)).toFixed(1)} points`;
};

const riskDeltaPhrase = (label, delta) => {
  if (!Number.isFinite(Number(delta)) || Math.abs(delta) < 0.5) return "";
  const direction = delta > 0 ? "rises" : "drops";
  return `${label} ${direction} by ${Math.abs(Number(delta)).toFixed(1)} points`;
};

const formatScore = (value) =>
  Number.isFinite(Number(value)) ? Number(value).toFixed(1).replace(/\.0$/, "") : "NA";

const getVariantScoreSnapshot = (insight = {}) => ({
  price: Number(insight.referenceExShowroomPrice || insight.price || insight.data?.referenceExShowroomPrice),
  features: getNumericScore(insight, "features", "featureScore.score"),
  value: getNumericScore(insight, "value", "valueScore.score"),
  safety: getNumericScore(insight, "safety", "safetyScore.score"),
  cityUse: getNumericScore(insight, "cityUse", "cityUseScore.score"),
  premiumComfort: getNumericScore(insight, "premiumComfort", "premiumComfortScore.score"),
  regretRisk: getNumericScore(insight, "regretRisk", "regretRisk.riskScore"),
});

const getMentionedVariantDocs = ({ docs = [], normalizedMessage = "" } = {}) => {
  const mentioned = docs
    .map((doc) => {
      const variantIndex = getNormalizedPhraseIndex(normalizedMessage, doc.variantKey);
      const fullNameIndex = getNormalizedPhraseIndex(normalizedMessage, doc.variantFullName);
      const index = [variantIndex, fullNameIndex]
        .filter((value) => value >= 0)
        .sort((a, b) => a - b)[0];

      return index >= 0 ? { doc, index } : null;
    })
    .filter(Boolean)
    .sort((a, b) => a.index - b.index);

  const unique = [];
  const seen = new Set();
  for (const item of mentioned) {
    const key = getScoreProfileDocKey(item.doc);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    unique.push(item.doc);
  }

  return unique;
};

const getMentionedVariantKeys = ({ docs = [], normalizedMessage = "" } = {}) => {
  const mentionedKeys = [];
  const seen = new Set();

  const docsByVariant = new Map();
  for (const doc of docs) {
    const variantKey = normalizeKey(doc.variantKey);
    if (!variantKey || docsByVariant.has(variantKey)) continue;
    docsByVariant.set(variantKey, doc);
  }

  const ordered = [...docsByVariant.keys()]
    .map((variantKey) => ({
      variantKey,
      index: getNormalizedPhraseIndex(normalizedMessage, variantKey),
    }))
    .filter((item) => item.index >= 0)
    .sort((a, b) => a.index - b.index);

  for (const item of ordered) {
    if (seen.has(item.variantKey)) continue;
    seen.add(item.variantKey);
    mentionedKeys.push(item.variantKey);
  }

  return mentionedKeys;
};

const inferFamilyDocsFromVariantPair = ({
  docs = [],
  normalizedMessage = "",
  preferredMakeKey = "",
  preferredModelKey = "",
  preferredFuelKey = "",
  preferredTransmissionKey = "",
} = {}) => {
  const mentionedVariantKeys = getMentionedVariantKeys({ docs, normalizedMessage });
  if (mentionedVariantKeys.length < 2) return null;

  const requiredKeys = mentionedVariantKeys.slice(0, 2);
  const groups = new Map();

  for (const doc of docs) {
    const variantKey = normalizeKey(doc.variantKey);
    if (!requiredKeys.includes(variantKey)) continue;

    const familyKey = getFamilyKey(doc);
    const group = groups.get(familyKey) || [];
    group.push(doc);
    groups.set(familyKey, group);
  }

  const candidates = [...groups.values()]
    .map((group, order) => {
      const presentKeys = new Set(group.map((doc) => normalizeKey(doc.variantKey)));
      if (!requiredKeys.every((key) => presentKeys.has(key))) return null;

      const first = group[0] || {};
      let score = 0;
      if (preferredModelKey && normalizeKey(first.modelKey) === normalizeKey(preferredModelKey)) score += 100;
      if (preferredMakeKey && normalizeKey(first.makeKey) === normalizeKey(preferredMakeKey)) score += 20;
      if (preferredFuelKey && normalizeKey(first.fuelKey) === normalizeKey(preferredFuelKey)) score += 10;
      if (preferredTransmissionKey && normalizeKey(first.transmissionKey) === normalizeKey(preferredTransmissionKey)) score += 10;

      return {
        group,
        order,
        score,
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score || a.order - b.order);

  if (!candidates.length) return null;

  return {
    familyDocs: candidates[0].group,
    mentionedVariantKeys: requiredKeys,
  };
};

const findDocForText = (docs = [], text = "") => {
  const normalizedText = normalizeKey(text);
  if (!normalizedText) return null;

  return docs.find((doc) =>
    hasNormalizedPhrase(normalizedText, doc.variantKey) ||
    hasNormalizedPhrase(normalizedText, doc.variantFullName) ||
    normalizeKey(doc.variantFullName).includes(normalizedText),
  ) || null;
};

const inferUpgradeInsightParamsFromMessage = async ({
  db,
  userMessage = "",
  makeKey = "",
  modelKey = "",
  fuelKey = "",
  transmissionKey = "",
} = {}) => {
  const plainText = String(userMessage || "").toLowerCase();
  const normalizedMessage = normalizeKey(userMessage);
  if (!normalizedMessage) return null;

  const docs = await getScoreProfileLookupDocs(db);

  const sameFamily = await inferSameFamilyParamsFromMessage({
    db,
    userMessage,
    makeKey,
    modelKey,
    fuelKey,
    transmissionKey,
  });

  let familyDocs = [];

  if (sameFamily?.modelKey) {
    familyDocs = docs.filter((doc) => {
      if (normalizeKey(doc.modelKey) !== sameFamily.modelKey) return false;
      if (sameFamily.fuelKey && normalizeKey(doc.fuelKey) !== sameFamily.fuelKey) return false;
      if (sameFamily.transmissionKey && normalizeKey(doc.transmissionKey) !== sameFamily.transmissionKey) return false;
      return true;
    });
  }

  let unique = getMentionedVariantDocs({ docs: familyDocs, normalizedMessage });
  let pairFamily = null;

  if (unique.length < 2) {
    pairFamily = inferFamilyDocsFromVariantPair({
      docs,
      normalizedMessage,
      preferredMakeKey: sameFamily.makeKey || makeKey,
      preferredModelKey: sameFamily.modelKey || modelKey,
      preferredFuelKey: sameFamily.fuelKey || fuelKey,
      preferredTransmissionKey: sameFamily.transmissionKey || transmissionKey,
    });

    if (pairFamily?.familyDocs?.length) {
      familyDocs = pairFamily.familyDocs;
      unique = getMentionedVariantDocs({ docs: familyDocs, normalizedMessage });
    }
  }

  if (unique.length < 2) return null;

  let baseDoc = unique[0];
  let targetDoc = unique[1];

  const fromTo = plainText.match(/\bfrom\s+(.+?)\s+to\s+(.+?)(?:\?|$|\s+(?:for|and|with|in)\b)/i);
  if (fromTo) {
    const fromText = normalizeKey(fromTo[1]);
    const toText = normalizeKey(fromTo[2]);
    const fromDoc = findDocForText(unique, fromText);
    const toDoc = findDocForText(unique, toText);
    if (fromDoc && toDoc) {
      baseDoc = fromDoc;
      targetDoc = toDoc;
    }
  } else if (/\bworth over\b|\bover\b/i.test(plainText)) {
    // "Alpha worth over Zeta" = target Alpha, base Zeta.
    targetDoc = unique[0];
    baseDoc = unique[1];
  } else if (/\bor\b|\bvs\b|\bversus\b/i.test(plainText)) {
    // Compare two choices in price order for a practical "which one" answer.
    const sorted = [...unique].sort((a, b) => Number(a.referenceExShowroomPrice || 0) - Number(b.referenceExShowroomPrice || 0));
    baseDoc = sorted[0];
    targetDoc = sorted[1] || unique[1];
  }

  return {
    baseDoc,
    targetDoc,
    familyDocs,
    resolutionSource: pairFamily ? "variant_pair_family" : "model_family",
    ...sameFamily,
  };
};


const FEATURE_LABEL_OVERRIDES = {
  connectedCar: "connected car tech",
  rearAcVents: "rear AC vents",
  rearCamera: "rear camera",
  camera360: "360° camera",
  cruiseControl: "cruise control",
  ledDrls: "LED DRLs",
  androidAuto: "Android Auto",
  appleCarPlay: "Apple CarPlay",
  touchscreen: "touchscreen",
  wirelessCharging: "wireless charging",
  ventilatedSeats: "ventilated seats",
  panoramicSunroof: "panoramic sunroof",
  sunroof: "sunroof",
  premiumSound: "premium sound",
  automaticClimateControl: "automatic climate control",
};

const humanizeFeatureKey = (key = "") => {
  if (FEATURE_LABEL_OVERRIDES[key]) return FEATURE_LABEL_OVERRIDES[key];

  return String(key || "")
    .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
    .replace(/_/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/^./, (char) => char.toUpperCase());
};

const getPresentFeatureKeys = (insight = {}) => {
  const direct =
    insight.modules?.features?.evidence?.presentKeys ||
    insight.modules?.features?.presentKeys ||
    insight.featureScore?.evidence?.presentKeys ||
    insight.featureScore?.presentKeys ||
    insight.data?.featureScore?.evidence?.presentKeys ||
    [];

  return Array.isArray(direct) ? direct.filter(Boolean) : [];
};

const getFeatureDiff = ({ baseInsight = {}, targetInsight = {}, limit = 6 } = {}) => {
  const baseKeys = new Set(getPresentFeatureKeys(baseInsight));
  const targetKeys = new Set(getPresentFeatureKeys(targetInsight));

  const gained = [...targetKeys]
    .filter((key) => !baseKeys.has(key))
    .sort()
    .slice(0, limit);

  const lost = [...baseKeys]
    .filter((key) => !targetKeys.has(key))
    .sort()
    .slice(0, limit);

  return {
    gained,
    lost,
    gainedLabels: gained.map(humanizeFeatureKey),
    lostLabels: lost.map(humanizeFeatureKey),
  };
};


const buildVariantUpgradeInsightLine = ({ baseInsight, targetInsight } = {}) => {
  if (!baseInsight || !targetInsight) {
    return "I could not resolve both variants clearly enough for an upgrade-ladder answer.";
  }

  const baseName = baseInsight.variantFullName;
  const targetName = targetInsight.variantFullName;

  const basePrice = Number(baseInsight.referenceExShowroomPrice || baseInsight.price || baseInsight.data?.referenceExShowroomPrice);
  const targetPrice = Number(targetInsight.referenceExShowroomPrice || targetInsight.price || targetInsight.data?.referenceExShowroomPrice);
  const priceDelta = Number.isFinite(basePrice) && Number.isFinite(targetPrice)
    ? targetPrice - basePrice
    : null;

  const baseSnapshot = getVariantScoreSnapshot(baseInsight);
  const targetSnapshot = getVariantScoreSnapshot(targetInsight);
  const featureDelta = scoreDifference(targetInsight, baseInsight, "features", "featureScore.score");
  const valueDelta = scoreDifference(targetInsight, baseInsight, "value", "valueScore.score");
  const safetyDelta = scoreDifference(targetInsight, baseInsight, "safety", "safetyScore.score");
  const cityDelta = scoreDifference(targetInsight, baseInsight, "cityUse", "cityUseScore.score");
  const premiumDelta = scoreDifference(targetInsight, baseInsight, "premiumComfort", "premiumComfortScore.score");
  const regretDelta = scoreDifference(targetInsight, baseInsight, "regretRisk", "regretRisk.riskScore");

  const deltaParts = [
    scoreDeltaPhrase("features", featureDelta),
    scoreDeltaPhrase("same-model value", valueDelta),
    scoreDeltaPhrase("safety", safetyDelta),
    scoreDeltaPhrase("city-use", cityDelta),
    scoreDeltaPhrase("premium/comfort", premiumDelta),
    riskDeltaPhrase("regret risk", regretDelta),
  ].filter(Boolean);

  let verdict = "";

  if (Number.isFinite(featureDelta) && Number.isFinite(valueDelta) && featureDelta > 3 && valueDelta < -8) {
    verdict = `${targetName} gives you more equipment than ${baseName}, but the value score drops sharply.`;
  } else if (Number.isFinite(featureDelta) && featureDelta > 3) {
    verdict = `${targetName} gives you a clearer equipment upgrade over ${baseName}.`;
  } else if (Number.isFinite(valueDelta) && valueDelta < -8) {
    verdict = `${targetName} is weaker value than ${baseName}; choose it only if the specific extras matter to you.`;
  } else {
    verdict = `${targetName} and ${baseName} are close on diagnostic module scores.`;
  }

  if (Number.isFinite(priceDelta) && priceDelta > 0) {
    verdict += ` The ex-showroom price jump is about ${formatPriceDelta(priceDelta)}.`;
  }

  const moduleLine = deltaParts.length
    ? `Score movement: ${deltaParts.join("; ")}.`
    : "";

  const snapshotLine =
    `Snapshot: ${baseName} is ${Number.isFinite(baseSnapshot.price) ? formatPriceDelta(baseSnapshot.price) : "price NA"} ex-showroom with features ${formatScore(baseSnapshot.features)}, value ${formatScore(baseSnapshot.value)}, safety ${formatScore(baseSnapshot.safety)}, city-use ${formatScore(baseSnapshot.cityUse)}, premium/comfort ${formatScore(baseSnapshot.premiumComfort)}, regret risk ${formatScore(baseSnapshot.regretRisk)}; ${targetName} is ${Number.isFinite(targetSnapshot.price) ? formatPriceDelta(targetSnapshot.price) : "price NA"} ex-showroom with features ${formatScore(targetSnapshot.features)}, value ${formatScore(targetSnapshot.value)}, safety ${formatScore(targetSnapshot.safety)}, city-use ${formatScore(targetSnapshot.cityUse)}, premium/comfort ${formatScore(targetSnapshot.premiumComfort)}, regret risk ${formatScore(targetSnapshot.regretRisk)}.`;

  const conclusion =
    Number.isFinite(valueDelta) && valueDelta < -8
      ? `Practical call: ${baseName} is the safer value pick; ${targetName} is for buyers who specifically want the higher-trim equipment.`
      : `Practical call: ${targetName} can be justified if those upgrades match your use case.`;

  const featureDiff = getFeatureDiff({ baseInsight, targetInsight });

  const featureGainLine = featureDiff.gainedLabels.length
    ? `Feature gains: ${featureDiff.gainedLabels.join(", ")}.`
    : "";

  const featureLossLine = featureDiff.lostLabels.length
    ? `Feature losses: ${featureDiff.lostLabels.join(", ")}.`
    : "";

  return [
    verdict,
    featureGainLine,
    featureLossLine,
    snapshotLine,
    moduleLine,
    conclusion,
  ]
    .filter(Boolean)
    .join(" ")
    .replace(/\s+/g, " ")
    .trim();
};


const buildSameFamilyValueLine = (result = {}, params = {}) => {
  const variants = Array.isArray(result.variants) ? result.variants : [];
  const modelLabel = variants[0]?.variantFullName?.split(" ").slice(0, 2).join(" ") || params.modelKey || "this model";

  if (!variants.length) {
    return `I could not find enough same-family value score data for ${modelLabel}. I can still help with listed variants, model-level price/features, or a basic comparison if you share city and requirements.`;
  }

  const top = variants[0];
  const second = variants[1];
  const last = variants[variants.length - 1];

  const topValue = top.modules?.value?.score ?? "NA";
  const topFeature = top.modules?.features?.score ?? "NA";

  const ranked = variants
    .slice(0, 5)
    .map((variant, index) => {
      const valueScore = variant.modules?.value?.score ?? "NA";
      const featureScore = variant.modules?.features?.score ?? "NA";
      return `${index + 1}. ${variant.variantFullName} — value ${valueScore}, features ${featureScore}`;
    })
    .join("; ");

  let verdict = `${top.variantFullName} has the highest same-family value score in this set at ${topValue}.`;

  if (second) {
    verdict += ` ${second.variantFullName} is the next practical step if you want more equipment without jumping straight to the weakest-value top trim.`;
  }

  if (last && last.scoreProfileKey !== top.scoreProfileKey) {
    verdict += ` ${last.variantFullName} scores lowest on same-family value; treat it as equipment-led rather than value-led and check whether the extra features matter to you.`;
  }

  return `${verdict} Ranked ladder: ${ranked}.`;
};



const getModelLabelFromVariants = (variants = [], fallbackModelKey = "") => {
  const firstName = variants[0]?.variantFullName || "";
  const parts = firstName.split(/\s+/).filter(Boolean);
  if (parts.length >= 2) return parts.slice(0, 2).join(" ");
  return humanizeFeatureKey(fallbackModelKey || "this model");
};

const getModelModuleScoreValue = (insight = {}, key = "") => {
  const value = insight.modules?.[key]?.score;
  return Number.isFinite(Number(value)) ? Number(value) : null;
};

const getBestModelVariantByModule = (variants = [], key = "") =>
  variants
    .filter((item) => Number.isFinite(getModelModuleScoreValue(item, key)))
    .sort((a, b) => getModelModuleScoreValue(b, key) - getModelModuleScoreValue(a, key))[0] || null;

const getBalancedModelVariant = (variants = []) => {
  const weighted = variants
    .map((variant) => {
      const parts = [
        getModelModuleScoreValue(variant, "value"),
        getModelModuleScoreValue(variant, "features"),
        getModelModuleScoreValue(variant, "safety"),
        getModelModuleScoreValue(variant, "cityUse"),
        getModelModuleScoreValue(variant, "mileageRunningCost"),
        getModelModuleScoreValue(variant, "practicality"),
      ].filter((value) => Number.isFinite(value));

      if (!parts.length) return null;

      return {
        variant,
        score: parts.reduce((sum, value) => sum + value, 0) / parts.length,
      };
    })
    .filter(Boolean)
    .sort((a, b) => b.score - a.score);

  return weighted[0]?.variant || null;
};

const uniqueModelStrings = (values = [], limit = 4) => {
  const out = [];
  const seen = new Set();

  for (const value of values) {
    const text = String(value || "").trim();
    const key = text.toLowerCase();
    if (!text || seen.has(key)) continue;
    seen.add(key);
    out.push(text);
    if (out.length >= limit) break;
  }

  return out;
};

const inferModelSummaryContext = (userMessage = "") => {
  const text = String(userMessage || "").toLowerCase();

  if (/\b(city|city driving|daily driving|daily use|traffic|urban)\b/i.test(text)) {
    return "city";
  }

  if (/\b(family|family use|family car|practical|practicality|parents|kids|children)\b/i.test(text)) {
    return "family";
  }

  if (/\b(value|sensible|worth|best buy|best variant)\b/i.test(text)) {
    return "value";
  }

  return "overall";
};

const buildScopeLabel = ({ fuelKey = "", transmissionKey = "" } = {}) => {
  const parts = [fuelKey, transmissionKey]
    .map((part) => humanizeFeatureKey(part))
    .filter(Boolean);

  return parts.join(" ").toLowerCase();
};

const buildModelScoreSummaryLine = (result = {}, params = {}) => {
  const variants = Array.isArray(result.variants) ? result.variants : [];
  const modelLabel = getModelLabelFromVariants(variants, params.modelKey);
  const buyerContext = inferModelSummaryContext(params.userMessage);
  const scopeLabel = buildScopeLabel(params);

  if (!variants.length) {
    return `I could not find enough diagnostic score data for ${modelLabel}. I can still help with price, features, variants, similar cars, or comparison from the DB-backed catalogue.`;
  }

  const bestValue = getBestModelVariantByModule(variants, "value");
  const featureRich = getBestModelVariantByModule(variants, "features");
  const cityStrong = getBestModelVariantByModule(variants, "cityUse");
  const mileageStrong = getBestModelVariantByModule(variants, "mileageRunningCost");
  const balanced = getBalancedModelVariant(variants);
  const practicalStrong = getBestModelVariantByModule(variants, "practicality");

  const watchouts = uniqueModelStrings(
    variants.flatMap((variant) => variant.watchouts || []),
    3
  );

  const summaryParts = [];

  if (scopeLabel) {
    summaryParts.push(`Scope: ${scopeLabel}.`);
  }

  if (buyerContext === "city" && cityStrong) {
    summaryParts.push(
      `For city driving, ${modelLabel} looks strongest where city-use score is highest: ${cityStrong.variantFullName} at ${formatScore(getModelModuleScoreValue(cityStrong, "cityUse"))}.`
    );
  } else if (buyerContext === "family" && practicalStrong) {
    summaryParts.push(
      `For family use, ${modelLabel} should be read through practicality, safety and daily-use scores first. Practicality signal is strongest on ${practicalStrong.variantFullName} at ${formatScore(getModelModuleScoreValue(practicalStrong, "practicality"))}.`
    );
  } else if (buyerContext === "value" && bestValue) {
    summaryParts.push(
      `For value, ${modelLabel} looks strongest on ${bestValue.variantFullName} with same-model value ${formatScore(getModelModuleScoreValue(bestValue, "value"))}.`
    );
  } else {
    summaryParts.push(
      `${modelLabel} looks strongest as a practical, efficient city hatchback in the current diagnostic score data.`
    );
  }

  if (bestValue) {
    summaryParts.push(
      `Best value signal: ${bestValue.variantFullName} with same-model value ${formatScore(getModelModuleScoreValue(bestValue, "value"))}.`
    );
  }

  if (balanced) {
    summaryParts.push(
      `Most balanced diagnostic pick: ${balanced.variantFullName}, because it keeps a better mix of value, features and daily-use scores.`
    );
  }

  if (featureRich) {
    summaryParts.push(
      `Top-trim caveat: ${featureRich.variantFullName} is the most feature-rich at features ${formatScore(getModelModuleScoreValue(featureRich, "features"))}, but value is ${formatScore(getModelModuleScoreValue(featureRich, "value"))}.`
    );
  }

  if (cityStrong) {
    summaryParts.push(
      `City-use signal is strongest on ${cityStrong.variantFullName} at ${formatScore(getModelModuleScoreValue(cityStrong, "cityUse"))}.`
    );
  }

  if (buyerContext === "family" && practicalStrong) {
    summaryParts.push(
      `Family/practicality signal: ${practicalStrong.variantFullName} is strongest on practicality at ${formatScore(getModelModuleScoreValue(practicalStrong, "practicality"))}.`
    );
  }

  if (mileageStrong) {
    summaryParts.push(
      `Mileage/running-cost signal is strong across the family at about ${formatScore(getModelModuleScoreValue(mileageStrong, "mileageRunningCost"))}.`
    );
  }

  if (watchouts.length) {
    summaryParts.push(`Watchouts: ${watchouts.join("; ")}.`);
  }

  summaryParts.push(decisionLanguageText("decision_score_module_summary_note", {
    operation: "model_score_summary",
    modelText: result?.modelLabel || result?.model || "",
  }));

  return summaryParts.join(" ");
};


const compactVariantLine = (insight) => {
  if (!insight) return null;

  const modules = insight.modules || {};
  const featureScore = modules.features?.score ?? "NA";
  const safetyScore = modules.safety?.score ?? "NA";
  const valueScore = modules.value?.score ?? "NA";
  const regretRisk = modules.regretRisk?.score ?? "NA";

  const strengths = Array.isArray(insight.strengths)
    ? insight.strengths.filter(Boolean).slice(0, 2)
    : [];

  const watchouts = Array.isArray(insight.watchouts)
    ? insight.watchouts.filter(Boolean).slice(0, 2)
    : [];

  const valueScoreNumber = Number(modules.value?.score);
  const featureScoreNumber = Number(modules.features?.score);
  const regretRiskNumber = Number(modules.regretRisk?.score);

  const isFeatureRich = Number.isFinite(featureScoreNumber) && featureScoreNumber >= 75;
  const isWeakValue = Number.isFinite(valueScoreNumber) && valueScoreNumber <= 30;
  const isGoodValue = Number.isFinite(valueScoreNumber) && valueScoreNumber >= 70;
  const isLowRegret = Number.isFinite(regretRiskNumber) && regretRiskNumber <= 25;

  let verdict = `${insight.variantFullName} has diagnostic score data available.`;

  if (isWeakValue && isFeatureRich) {
    verdict = `${insight.variantFullName} is feature-rich, but its same-model value signal is weak — the score indicates a top-trim equipment focus rather than the strongest same-model value position.`;
  } else if (isGoodValue && isFeatureRich) {
    verdict = `${insight.variantFullName} has a strong same-model value signal and is also feature-rich.`;
  } else if (isGoodValue) {
    verdict = `${insight.variantFullName} looks like a strong same-model value pick.`;
  } else if (isWeakValue) {
    verdict = `${insight.variantFullName} has a weak same-model value signal compared with other variants in its family.`;
  } else if (isFeatureRich) {
    verdict = `${insight.variantFullName} looks feature-rich, but value should be checked against nearby variants.`;
  }

  if (isLowRegret) {
    verdict += " Regret-risk signal is low.";
  }

  const scoreLine = `Score snapshot: safety ${safetyScore}, features ${featureScore}, same-model value ${valueScore}, regret risk ${regretRisk}.`;

  const watchoutLine = watchouts.length
    ? `Watchouts: ${watchouts.join("; ")}.`
    : "";

  const strengthLine = strengths.length
    ? `Strengths: ${strengths.join("; ")}.`
    : "";

  return [verdict, watchoutLine, strengthLine, scoreLine]
    .filter(Boolean)
    .join(" ");
};

const resolveVariantInsightFromMessage = async ({ db, userMessage = "" } = {}) => {
  const textMatch = await resolveScoreProfileKeyFromMessage({
    db,
    userMessage,
  });

  if (!textMatch?.scoreProfileKey && !textMatch?.variantProfileKey) return null;

  return {
    insight: await getVariantScoreInsight({
      db,
      scoreProfileKey: textMatch.scoreProfileKey,
      variantProfileKey: textMatch.variantProfileKey,
    }),
    error: null,
    scoreProfileKey: textMatch.scoreProfileKey,
    variantProfileKey: textMatch.variantProfileKey,
    meta: {
      resolutionSource: "user_message",
      confidence: textMatch.confidence,
      matchedVariantFullName: textMatch.matchedVariantFullName,
    },
  };
};

const resolveVariantInsight = async (params = {}) => {
  const scoreProfileKey =
    params.scoreProfileKey ||
    params.score_profile_key ||
    buildScoreProfileKey(params);

  const variantProfileKey = params.variantProfileKey || params.variant_profile_key;

  if (scoreProfileKey || variantProfileKey) {
    return {
      insight: await getVariantScoreInsight({
        db: params.db,
        scoreProfileKey,
        variantProfileKey,
      }),
      error: null,
      scoreProfileKey,
      variantProfileKey,
    };
  }

  const modelKey = normalizeKey(params.modelKey || params.model_key);
  const variantKey = normalizeKey(params.variantKey || params.variant_key);
  const userMessage = params.userMessage || params.message || params.query || "";

  if (!modelKey || !variantKey) {
    const messageMatch = await resolveVariantInsightFromMessage({
      db: params.db,
      userMessage,
    });

    if (messageMatch) return messageMatch;

    return {
      insight: null,
      error: {
        code: "missing_variant_key",
        message:
          "Pass scoreProfileKey, variantProfileKey, or at least modelKey and variantKey. Fuel/transmission improves precision.",
      },
    };
  }

  const result = await getModelScoreInsights({
    db: params.db,
    makeKey: params.makeKey || params.make_key,
    modelKey,
    fuelKey: params.fuelKey || params.fuel_key,
    transmissionKey: params.transmissionKey || params.transmission_key,
    fuelTransmissionFamilyKey:
      params.fuelTransmissionFamilyKey || params.fuel_transmission_family_key,
    limit: 100,
  });

  const fuelKey = normalizeKey(params.fuelKey || params.fuel_key);
  const transmissionKey = normalizeKey(params.transmissionKey || params.transmission_key);
  const messageMatch = async () =>
    resolveVariantInsightFromMessage({
      db: params.db,
      userMessage,
    });

  const matches = (result.variants || []).filter((variant) => {
    if (normalizeKey(variant.variantKey) !== variantKey) return false;
    if (fuelKey && normalizeKey(variant.fuelKey) !== fuelKey) return false;
    if (transmissionKey && normalizeKey(variant.transmissionKey) !== transmissionKey) return false;
    return true;
  });

  if (matches.length === 1) {
    return { insight: matches[0], error: null };
  }

  if (matches.length > 1) {
    const textResolved = await messageMatch();
    if (textResolved?.insight) return textResolved;

    return {
      insight: null,
      error: {
        code: "ambiguous_variant_score_profile",
        message:
          "Multiple score profiles match this variant. Add fuelKey and transmissionKey.",
        meta: {
          matches: matches.slice(0, 10).map((item) => ({
            scoreProfileKey: item.scoreProfileKey,
            variantFullName: item.variantFullName,
            fuelKey: item.fuelKey,
            transmissionKey: item.transmissionKey,
          })),
        },
      },
    };
  }

  const textResolved = await messageMatch();
  if (textResolved?.insight) return textResolved;

  return {
    insight: null,
    error: {
      code: "score_profile_not_found",
      message: "No score profile found for the requested variant.",
      meta: { modelKey, variantKey, fuelKey, transmissionKey },
    },
  };
};

export const runVehicleScoreInsightTool = async (rawArgs = {}) => {
  const args = normalizeRuntimeArgs(rawArgs);
  const operation = normalizeKey(args.operation || "variant_score_insight");

  try {
    if (operation === "coverage") {
      const coverage = await getScoreProfileCoverage({ db: args.db });

      return createSuccess({
        operation,
        data: coverage,
        answer: `Score profile coverage: ${coverage.totalScoreProfiles} variants. Use this as diagnostic coverage only, not as a purchase verdict.`,
      });
    }

    if (["variant_score_insight", "variant", "score_profile"].includes(operation)) {
      const resolved = await resolveVariantInsight(args);

      if (resolved.error) {
        return createError({
          operation,
          code: resolved.error.code,
          message: resolved.error.message,
          meta: resolved.error.meta || {
            scoreProfileKey: resolved.scoreProfileKey,
            variantProfileKey: resolved.variantProfileKey,
          },
        });
      }

      return createSuccess({
        operation: "variant_score_insight",
        data: resolved.insight,
        answer: compactVariantLine(resolved.insight),
      });
    }

    if (["model_score_insights", "model", "model_variants"].includes(operation)) {
      const inferred = await inferSameFamilyParamsFromMessage({
        db: args.db,
        userMessage: args.userMessage || args.message || args.query || "",
        makeKey: args.makeKey || args.make_key,
        modelKey: args.modelKey || args.model_key,
        fuelKey: args.fuelKey || args.fuel_key,
        transmissionKey: args.transmissionKey || args.transmission_key,
      });

      const modelKey = inferred.modelKey || args.modelKey || args.model_key;
      if (!modelKey) {
        return createError({
          operation,
          code: "missing_model_key",
          message: "modelKey is required for model_score_insights.",
        });
      }

      const result = await getModelScoreInsights({
        db: args.db,
        makeKey: inferred.makeKey || args.makeKey || args.make_key,
        modelKey,
        fuelKey: inferred.fuelKey || args.fuelKey || args.fuel_key,
        transmissionKey: inferred.transmissionKey || args.transmissionKey || args.transmission_key,
        fuelTransmissionFamilyKey:
          inferred.fuelTransmissionFamilyKey ||
          args.fuelTransmissionFamilyKey ||
          args.fuel_transmission_family_key,
        limit: safeLimit(args.limit, 40, 80),
      });

      return createSuccess({
        operation: "model_score_insights",
        data: result,
        answer: buildModelScoreSummaryLine(result, {
          modelKey,
          ...inferred,
          userMessage: args.userMessage || args.message || args.query || "",
        }),
      });
    }

    if (["variant_upgrade_insight", "upgrade_ladder", "variant_upgrade"].includes(operation)) {
      const inferred = await inferUpgradeInsightParamsFromMessage({
        db: args.db,
        userMessage: args.userMessage || args.message || args.query || "",
        makeKey: args.makeKey || args.make_key,
        modelKey: args.modelKey || args.model_key,
        fuelKey: args.fuelKey || args.fuel_key,
        transmissionKey: args.transmissionKey || args.transmission_key,
      });

      if (!inferred?.baseDoc || !inferred?.targetDoc) {
        return createError({
          operation,
          code: "missing_upgrade_variants",
          message: "Two variants are required for an upgrade-ladder score answer.",
        });
      }

      const baseInsight = await getVariantScoreInsight({
        db: args.db,
        scoreProfileKey: inferred.baseDoc.scoreProfileKey,
        variantProfileKey: inferred.baseDoc.variantProfileKey,
      });

      const targetInsight = await getVariantScoreInsight({
        db: args.db,
        scoreProfileKey: inferred.targetDoc.scoreProfileKey,
        variantProfileKey: inferred.targetDoc.variantProfileKey,
      });

      return createSuccess({
        operation: "variant_upgrade_insight",
        data: {
          base: baseInsight,
          target: targetInsight,
          baseVariantFullName: baseInsight?.variantFullName,
          targetVariantFullName: targetInsight?.variantFullName,
        },
        answer: buildVariantUpgradeInsightLine({ baseInsight, targetInsight }),
      });
    }

    if (["same_family_value_insights", "same_family_value", "value_ladder"].includes(operation)) {
      const inferred = await inferSameFamilyParamsFromMessage({
        db: args.db,
        userMessage: args.userMessage || args.message || args.query || "",
      });

      // Prefer message-inferred canonical keys because live bridge entities can be display labels
      // such as "Maruti Baleno", while score profiles use canonical modelKey like "baleno".
      const modelKey = inferred.modelKey || args.modelKey || args.model_key;
      if (!modelKey) {
        return createError({
          operation,
          code: "missing_model_key",
          message: "modelKey is required for same_family_value_insights.",
        });
      }

      const result = await getSameFamilyValueInsights({
        db: args.db,
        makeKey: inferred.makeKey || args.makeKey || args.make_key,
        modelKey,
        fuelKey: inferred.fuelKey || args.fuelKey || args.fuel_key,
        transmissionKey: inferred.transmissionKey || args.transmissionKey || args.transmission_key,
        fuelTransmissionFamilyKey:
          inferred.fuelTransmissionFamilyKey ||
          args.fuelTransmissionFamilyKey ||
          args.fuel_transmission_family_key,
        limit: safeLimit(args.limit, 20, 50),
      });

      return createSuccess({
        operation: "same_family_value_insights",
        data: result,
        answer: buildSameFamilyValueLine(result, { modelKey, ...inferred }),
      });
    }


    if (["cross_model_score_diagnostic", "cross_model_score", "model_score_comparison"].includes(operation)) {
      const targets = collectCrossModelTargets(args, rawArgs);

      const result = await buildCrossModelScoreDiagnostic({
        db: args.db || rawArgs.db || mongoose.connection.db,
        targets,
        fuelKey: args.fuelKey || args.fuel_key,
        transmissionKey: args.transmissionKey || args.transmission_key,
        limitPerModel: safeLimit(args.limit, 40, 80),
      });

      if (!result.ok) {
        return createError({
          operation,
          code: result.code || "cross_model_score_diagnostic_incomplete",
          message:
            result.message ||
            "Could not build a complete cross-model score diagnostic for the requested targets.",
          meta: {
            targetCount: targets.length,
            missingTargets: result.missingTargets || [],
          },
        });
      }

      return createSuccess({
        operation: "cross_model_score_diagnostic",
        data: result,
        answer: buildCrossModelScoreDiagnosticLine(result),
      });
    }

    if (["top_module_score_insights", "top_scores", "module_ranking"].includes(operation)) {
      const scorePath = pickScorePath({
        scorePath: args.scorePath || args.score_path,
        module: args.module,
        scoreModule: args.scoreModule || args.score_module,
      });

      if (!scorePath) {
        return createError({
          operation,
          code: "missing_score_path",
          message:
            "Pass scorePath or module. Supported modules: safety, features, performance, mileage, practicality, city, highway, premium, value, regret.",
        });
      }

      const result = await getTopScoreInsights({
        db: args.db,
        scorePath,
        direction: args.direction || "desc",
        limit: safeLimit(args.limit, 20, 50),
        filters: args.filters || {},
      });

      return createSuccess({
        operation: "top_module_score_insights",
        data: {
          scorePath,
          ...result,
        },
        answer: `${result.count} top score insight variants found for ${scorePath}.`,
      });
    }

    return createError({
      operation,
      code: "unsupported_operation",
      message:
        "Unsupported score insight operation. Use coverage, variant_score_insight, model_score_insights, same_family_value_insights, cross_model_score_diagnostic, or top_module_score_insights.",
    });
  } catch (error) {
    return createError({
      operation,
      code: "score_insight_tool_failed",
      message: error?.message || "Score insight tool failed.",
    });
  }
};

export default runVehicleScoreInsightTool;
