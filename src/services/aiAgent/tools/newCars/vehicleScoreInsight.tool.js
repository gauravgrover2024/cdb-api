import scoreInsightService from "../../../aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs";

const {
  getVariantScoreInsight,
  getModelScoreInsights,
  getSameFamilyValueInsights,
  getTopScoreInsights,
  getScoreProfileCoverage,
} = scoreInsightService;

const TOOL_NAME = "vehicle_score_insight";

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

const normalizeRuntimeArgs = (args = {}) => {
  const toolPlan = args.toolPlan || {};
  const input = toolPlan.input || toolPlan.args || toolPlan.params || {};
  const anchors = args.plan?.meaningFrame?.anchors || args.context?.meaningFrame?.anchors || {};
  const primaryVehicle = anchors.primaryVehicle || anchors.vehicle || {};
  const selectedVehicle =
    args.context?.selectedVehicle ||
    args.context?.vehicle ||
    args.context?.aciSelectedVehicle ||
    {};

  return {
    ...input,
    ...toolPlan,
    ...args,
    operation: firstValue(
      args.operation,
      input.operation,
      toolPlan.operation,
      OPERATION_BY_TOOL_NAME[toolPlan.tool],
      OPERATION_BY_TOOL_NAME[args.tool],
      "variant_score_insight"
    ),
    makeKey: firstValue(args.makeKey, input.makeKey, toolPlan.makeKey, primaryVehicle.makeKey, selectedVehicle.makeKey),
    modelKey: firstValue(args.modelKey, input.modelKey, toolPlan.modelKey, primaryVehicle.modelKey, selectedVehicle.modelKey),
    variantKey: firstValue(args.variantKey, input.variantKey, toolPlan.variantKey, primaryVehicle.variantKey, selectedVehicle.variantKey),
    fuelKey: firstValue(args.fuelKey, input.fuelKey, toolPlan.fuelKey, primaryVehicle.fuelKey, selectedVehicle.fuelKey),
    transmissionKey: firstValue(
      args.transmissionKey,
      input.transmissionKey,
      toolPlan.transmissionKey,
      primaryVehicle.transmissionKey,
      selectedVehicle.transmissionKey
    ),
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

const compactVariantLine = (insight) => {
  if (!insight) return null;

  const modules = insight.modules || {};
  const featureScore = modules.features?.score ?? "NA";
  const safetyScore = modules.safety?.score ?? "NA";
  const valueScore = modules.value?.score ?? "NA";
  const regretRisk = modules.regretRisk?.score ?? "NA";

  return `${insight.variantFullName}: safety ${safetyScore}, features ${featureScore}, same-model value ${valueScore}, regret risk ${regretRisk}.`;
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

  if (!modelKey || !variantKey) {
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
      const modelKey = args.modelKey || args.model_key;
      if (!modelKey) {
        return createError({
          operation,
          code: "missing_model_key",
          message: "modelKey is required for same_family_value_insights.",
        });
      }

      const result = await getSameFamilyValueInsights({
        db: args.db,
        makeKey: args.makeKey || args.make_key,
        modelKey,
        fuelKey: args.fuelKey || args.fuel_key,
        transmissionKey: args.transmissionKey || args.transmission_key,
        fuelTransmissionFamilyKey:
          args.fuelTransmissionFamilyKey || args.fuel_transmission_family_key,
        limit: safeLimit(args.limit, 20, 50),
      });

      return createSuccess({
        operation: "same_family_value_insights",
        data: result,
        answer: `${result.count} same-family value insight variants found for ${modelKey}.`,
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
