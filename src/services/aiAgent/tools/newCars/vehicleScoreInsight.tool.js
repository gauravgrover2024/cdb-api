import mongoose from "mongoose";
import scoreInsightService from "../../../aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs";

const {
  getVariantScoreInsight,
  getModelScoreInsights,
  getSameFamilyValueInsights,
  getTopScoreInsights,
  getScoreProfileCoverage,
} = scoreInsightService;

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
  vehicle_top_score_insights: "top_module_score_insights",
};

const normalizeKey = (value) =>
  String(value || "")
    .trim()
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");

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

const createGuardrail = () => ({
  canUseForFinalRecommendation: false,
  reason:
    "These are diagnostic module scores only. Final recommendation needs buyer-context weighting, similar-cars graph, upgrade ladder, service/resale evidence and recommendation policy.",
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



const inferSameFamilyParamsFromMessage = async ({ db, userMessage = "" } = {}) => {
  const normalizedMessage = normalizeKey(userMessage);
  if (!normalizedMessage) return {};

  const docs = await getScoreProfileLookupDocs(db);

  const modelScores = new Map();
  for (const doc of docs) {
    const modelKey = normalizeKey(doc.modelKey);
    if (!modelKey) continue;

    const fullNameKey = normalizeKey(doc.variantFullName);
    let score = 0;

    if (normalizedMessage.includes(modelKey)) score += 50;
    if (fullNameKey && normalizedMessage.includes(fullNameKey.split("_").slice(0, 2).join("_"))) score += 10;

    if (score <= 0) continue;

    const current = modelScores.get(modelKey) || { score: 0, doc };
    if (score > current.score) modelScores.set(modelKey, { score, doc });
  }

  const bestModel = [...modelScores.values()].sort((a, b) => b.score - a.score)[0]?.doc;
  if (!bestModel) return {};

  const fuelKey =
    /\bpetrol\b/i.test(userMessage) ? "petrol" :
    /\bdiesel\b/i.test(userMessage) ? "diesel" :
    /\bcng\b/i.test(userMessage) ? "cng" :
    /\bev|electric\b/i.test(userMessage) ? "ev" :
    "";

  const transmissionKey =
    /\bmanual|mt\b/i.test(userMessage) ? "manual" :
    /\bautomatic|amt|cvt|dct|ivt|at\b/i.test(userMessage) ? "automatic" :
    "";

  return {
    makeKey: normalizeKey(bestModel.makeKey),
    modelKey: normalizeKey(bestModel.modelKey),
    fuelKey,
    transmissionKey,
    fuelTransmissionFamilyKey: fuelKey && transmissionKey ? `${fuelKey}_${transmissionKey}` : "",
  };
};

const buildSameFamilyValueLine = (result = {}, params = {}) => {
  const variants = Array.isArray(result.variants) ? result.variants : [];
  const modelLabel = variants[0]?.variantFullName?.split(" ").slice(0, 2).join(" ") || params.modelKey || "this model";

  if (!variants.length) {
    return `I could not find enough same-family value score data for ${modelLabel}.`;
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

  let verdict = `${top.variantFullName} is the strongest same-family value pick in this set with value score ${topValue}.`;

  if (second) {
    verdict += ` ${second.variantFullName} is the next practical step if you want more equipment without jumping straight to the weakest-value top trim.`;
  }

  if (last && last.scoreProfileKey !== top.scoreProfileKey) {
    verdict += ` ${last.variantFullName} scores lowest on same-family value, so buy it only if its extra features matter to you.`;
  }

  return `${verdict} Ranked ladder: ${ranked}.`;
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
    verdict = `${insight.variantFullName} is feature-rich, but its same-model value is weak — you are paying more for the top-trim experience rather than getting the strongest value pick.`;
  } else if (isGoodValue && isFeatureRich) {
    verdict = `${insight.variantFullName} looks like a strong same-model value pick and is also feature-rich.`;
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
        answer: `Score profile coverage: ${coverage.totalScoreProfiles} variants, final recommendation disabled.`,
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
      const modelKey = args.modelKey || args.model_key;
      if (!modelKey) {
        return createError({
          operation,
          code: "missing_model_key",
          message: "modelKey is required for model_score_insights.",
        });
      }

      const result = await getModelScoreInsights({
        db: args.db,
        makeKey: args.makeKey || args.make_key,
        modelKey,
        fuelKey: args.fuelKey || args.fuel_key,
        transmissionKey: args.transmissionKey || args.transmission_key,
        fuelTransmissionFamilyKey:
          args.fuelTransmissionFamilyKey || args.fuel_transmission_family_key,
        limit: safeLimit(args.limit, 80, 100),
      });

      return createSuccess({
        operation: "model_score_insights",
        data: result,
        answer: `${result.count} score insight variants found for ${modelKey}.`,
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
        "Unsupported score insight operation. Use coverage, variant_score_insight, model_score_insights, same_family_value_insights, or top_module_score_insights.",
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
