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

export const runVehicleScoreInsightTool = async (args = {}) => {
  const operation = normalizeKey(args.operation || args.mode || args.queryType || "variant_score_insight");

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
      const scoreProfileKey =
        args.scoreProfileKey ||
        args.score_profile_key ||
        buildScoreProfileKey(args);

      const variantProfileKey = args.variantProfileKey || args.variant_profile_key;

      if (!scoreProfileKey && !variantProfileKey) {
        return createError({
          operation,
          code: "missing_variant_key",
          message:
            "Pass scoreProfileKey, variantProfileKey, or makeKey/modelKey/variantKey/fuelKey/transmissionKey.",
        });
      }

      const insight = await getVariantScoreInsight({
        db: args.db,
        scoreProfileKey,
        variantProfileKey,
      });

      if (!insight) {
        return createError({
          operation,
          code: "score_profile_not_found",
          message: "No score profile found for the requested variant.",
          meta: { scoreProfileKey, variantProfileKey },
        });
      }

      return createSuccess({
        operation: "variant_score_insight",
        data: insight,
        answer: compactVariantLine(insight),
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
