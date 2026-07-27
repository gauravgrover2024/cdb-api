const {
  ALLOWED_ANSWER_TYPES,
  BLOCKED_REASONS,
  DECISION_MODULES,
  EVIDENCE_STATUS,
  MANDATORY_FINAL_RECOMMENDATION_INPUTS,
} = require('./aciDecisionPolicy.constants.cjs');
const { buildBuyerDecisionInputContract } = require('./aciBuyerDecisionInput.contract.cjs');
const { buildBuyerInputClarificationPayload } = require('./aciBuyerInputClarification.service.cjs');
const { buildFinalRecommendationPolicyReadiness } = require('./aciFinalRecommendationReadiness.service.cjs');

const asObject = (value) => (value && typeof value === 'object' && !Array.isArray(value) ? value : {});
const asArray = (value) => (Array.isArray(value) ? value : []);
const textOf = (value) => String(value || '').trim();
const normalize = (value) => textOf(value).toLowerCase();

const FINAL_RECOMMENDATION_REQUEST_PATTERNS = Object.freeze([
  /\bshould\s+i\s+(buy|choose|pick|go\s+for|purchase)\b/i,
  /\bwhich\s+(one|car|variant|model)\s+should\s+i\s+(?:finally\s+|ultimately\s+)?(buy|choose|pick|go\s+for)\b/i,
  /\bwhich\s+(one|car|variant|model)\s+(?:is\s+)?(?:the\s+)?(?:final|better|best)\s+(choice|pick|option)\b/i,
  /\bdecide\s+for\s+me\b/i,
  /\bfinal\s+(answer|recommendation|verdict|call|decision)\b/i,
  /\bwhat\s+should\s+i\s+(?:finally\s+|ultimately\s+)?(buy|choose|pick)\b/i,
  /\bhelp\s+me\s+(?:finally\s+)?(choose|pick|decide)\b/i,
  /\bbest\s+(car|option|choice|variant|model)\s+(for\s+me|to\s+buy|under|within)\b/i,
  /\brecommend\s+(me|one|a\s+car|the\s+best)\b/i,
  /\bworth\s+buying\b/i,
  /\bgo\s+ahead\s+with\b/i,
  /\bshould\s+i\s+stretch\b/i,
  /\bshould\s+i\s+upgrade\b/i,
  /\bworth\s+(?:the\s+)?extra\b/i,
  /\bworth\s+upgrading\b/i,
]);

const valuePresent = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean).length > 0;
  if (value && typeof value === 'object') return Object.keys(value).length > 0;
  return Boolean(textOf(value));
};

const firstPresent = (...values) => values.find(valuePresent);

const getNestedValues = (context = {}, response = {}) => {
  const ctx = asObject(context);
  const data = asObject(response.data);
  const meta = asObject(response.meta);
  const contextState = asObject(ctx.contextState || ctx.aciContextState || response.contextState || response.aciContextState);
  const buyerContext = asObject(ctx.buyerContext || ctx.buyerIntent || contextState.buyerContext || contextState.buyerIntent || data.buyerContext || data.buyerIntent);
  const selectedVehicle = asObject(ctx.selectedVehicle || contextState.selectedVehicle || contextState.anchors?.primaryVehicle || data.selectedVehicle || data.vehicle);
  const filters = asObject(ctx.filters || contextState.filters || data.filters);
  const entities = asObject(ctx.entities || data.entities);
  const priorities = asObject(buyerContext.priorities || ctx.priorities || data.priorities);
  const shortlisted = asArray(buyerContext.shortlistedModels || buyerContext.shortlist || ctx.shortlistedModels || data.shortlistedModels || data.models || response.models);

  return {
    ctx,
    data,
    meta,
    contextState,
    buyerContext,
    selectedVehicle,
    filters,
    entities,
    priorities,
    shortlisted,
  };
};

function detectFinalRecommendationRequest({ message = '', bridge = {}, response = {} } = {}) {
  const text = [
    message,
    bridge.effectiveMessage,
    bridge.originalMessage,
    response.originalMessage,
    response.effectiveMessage,
    response.query,
  ].map(textOf).filter(Boolean).join(' ');

  return FINAL_RECOMMENDATION_REQUEST_PATTERNS.some((pattern) => pattern.test(text));
}

function getFinalRecommendationInputPresence({ context = {}, response = {} } = {}) {
  const {
    ctx,
    data,
    contextState,
    buyerContext,
    selectedVehicle,
    filters,
    entities,
    priorities,
    shortlisted,
  } = getNestedValues(context, response);

  const inputPresence = {
    city: firstPresent(
      buyerContext.city,
      buyerContext.citySlug,
      ctx.city,
      ctx.citySlug,
      filters.city,
      filters.citySlug,
      selectedVehicle.city,
      selectedVehicle.citySlug,
      data.city,
      data.citySlug
    ),

    budgetOrPriceCeiling: firstPresent(
      buyerContext.budget,
      buyerContext.budgetRange,
      buyerContext.maxBudget,
      buyerContext.priceCeiling,
      buyerContext.budgetOrPriceCeiling,
      ctx.budget,
      ctx.maxBudget,
      filters.maxPrice,
      filters.priceCeiling,
      data.budget,
      data.maxBudget
    ),

    bodyPreferenceOrPrimaryUseCase: firstPresent(
      buyerContext.bodyType,
      buyerContext.bodyPreference,
      buyerContext.primaryUseCase,
      buyerContext.useCase,
      buyerContext.bodyPreferenceOrPrimaryUseCase,
      ctx.bodyType,
      ctx.primaryUseCase,
      filters.bodyType,
      data.bodyType,
      data.primaryUseCase
    ),

    familySizeOrOccupancyUse: firstPresent(
      buyerContext.familySize,
      buyerContext.occupancy,
      buyerContext.seatingNeed,
      buyerContext.familySizeOrOccupancyUse,
      ctx.familySize,
      ctx.occupancy,
      data.familySize,
      data.occupancy
    ),

    fuelPreferenceOrMonthlyRunning: firstPresent(
      buyerContext.fuel,
      buyerContext.fuelType,
      buyerContext.fuelPreference,
      buyerContext.monthlyRunning,
      buyerContext.running,
      buyerContext.fuelPreferenceOrMonthlyRunning,
      ctx.fuel,
      ctx.fuelType,
      filters.fuel,
      filters.fuelType,
      data.fuel,
      data.fuelType
    ),

    transmissionPreference: firstPresent(
      buyerContext.transmission,
      buyerContext.transmissionType,
      buyerContext.transmissionPreference,
      ctx.transmission,
      filters.transmission,
      data.transmission
    ),

    safetyPriority: firstPresent(
      buyerContext.safetyPriority,
      priorities.safety,
      ctx.safetyPriority,
      data.safetyPriority
    ),

    featurePriority: firstPresent(
      buyerContext.featurePriority,
      buyerContext.mustHaveFeatures,
      priorities.features,
      ctx.featurePriority,
      ctx.mustHaveFeatures,
      filters.mustHaveFeatures,
      data.featurePriority,
      data.mustHaveFeatures
    ),

    shortlistedModelsOrDiscoveryScope: firstPresent(
      shortlisted,
      buyerContext.shortlistedModelsOrDiscoveryScope,
      buyerContext.discoveryScope,
      selectedVehicle.model,
      selectedVehicle.modelKey,
      selectedVehicle.fullModel,
      entities.model,
      entities.modelKey,
      data.discoveryScope,
      contextState.comparison?.targets
    ),
  };

  return {
    inputPresence,
    presentInputs: Object.entries(inputPresence).filter(([, value]) => valuePresent(value)).map(([key]) => key),
    missingMandatoryInputs: MANDATORY_FINAL_RECOMMENDATION_INPUTS.filter((key) => !valuePresent(inputPresence[key])),
  };
}

function getRuntimeModule({ bridge = {}, response = {} } = {}) {
  const tool = bridge.tool || response.tool || response.executorTool || '';
  const primaryTask = bridge.primaryTask || response.primaryTask || '';

  if (tool === 'vehicle_score_insight' || primaryTask === 'score_insight') return DECISION_MODULES.SCORE_INSIGHT;
  if (tool === 'vehicle_similar' || primaryTask === 'similar_cars') return DECISION_MODULES.SIMILAR_CARS;
  if (tool === 'vehicle_compare' || primaryTask === 'vehicle_comparison') return DECISION_MODULES.COMPARISON;
  if (tool === 'vehicle_recommend' || primaryTask === 'vehicle_recommendation' || primaryTask === 'vehicle_discovery') return DECISION_MODULES.RECOMMENDATION;

  return response.module || '';
}

function getEvidenceGate({ response = {} } = {}) {
  const decisionPolicy = asObject(response.decisionPolicy || response.data?.decisionPolicy || response.meta?.decisionPolicy);
  const evidence = asObject(response.evidence || response.data?.evidence);
  const evidenceStatus = evidence.evidenceStatus || decisionPolicy.evidenceStatus || response.evidenceStatus || response.dataStatus || '';
  const confidence = evidence.confidence || decisionPolicy.confidence || response.evidenceConfidence || '';

  const hasUsefulEvidence =
    evidenceStatus === EVIDENCE_STATUS.COMPLETE ||
    evidenceStatus === EVIDENCE_STATUS.PARTIAL ||
    Number(evidence.usableEvidenceCount || 0) > 0 ||
    Number(response.matched || response.count || response.rowsCount || response.modelCount || 0) > 0 ||
    asArray(response.rows).length > 0 ||
    asArray(response.items).length > 0 ||
    asArray(response.models).length > 0 ||
    asArray(response.variants).length > 0;

  return {
    evidenceStatus: evidenceStatus || EVIDENCE_STATUS.MISSING,
    confidence: confidence || '',
    hasUsefulEvidence,
  };
}

function buildFinalRecommendationEligibilityRuntime({
  message = '',
  bridge = {},
  response = {},
  context = {},
  decisionPolicy = {},
  evidence = {},
} = {}) {
  const requestedFinalRecommendation = detectFinalRecommendationRequest({ message, bridge, response });
  const moduleName = getRuntimeModule({ bridge, response });
  const buyerDecisionInput = buildBuyerDecisionInputContract({ context, response, message });
  const { presentInputs, missingMandatoryInputs } = buyerDecisionInput;
  const buyerGuidanceContext = asObject(buyerDecisionInput.buyerGuidanceContext);
  const provisionalGuidanceMode = textOf(buyerGuidanceContext.guidanceMode);
  const buyerInputClarification = buildBuyerInputClarificationPayload({
    missingMandatoryInputs: requestedFinalRecommendation ? missingMandatoryInputs : [],
    buyerDecisionInput,
    requestedFinalRecommendation,
  });

  const evidenceGate = getEvidenceGate({
    response: {
      ...response,
      decisionPolicy: Object.keys(decisionPolicy || {}).length ? decisionPolicy : response.decisionPolicy,
      evidence: Object.keys(evidence || {}).length ? evidence : response.evidence,
    },
  });
  const finalRecommendation = asObject(
    response.finalRecommendation ||
    response.data?.finalRecommendation ||
    response.meta?.finalRecommendation
  );
  const finalRecommendationReady =
    requestedFinalRecommendation &&
    finalRecommendation.requested === true &&
    finalRecommendation.status === 'final_ready' &&
    finalRecommendation.finalRecommendationEnabled === true &&
    finalRecommendation.canUseForFinalRecommendation === true &&
    moduleName === DECISION_MODULES.RECOMMENDATION &&
    missingMandatoryInputs.length === 0 &&
    evidenceGate.hasUsefulEvidence;
  const effectiveBuyerGuidanceContext = finalRecommendationReady
    ? {
        ...buyerGuidanceContext,
        guidanceMode: 'final_recommendation',
        finalPurchaseVerdictEnabled: true,
      }
    : buyerGuidanceContext;
  const effectiveBuyerDecisionInput = finalRecommendationReady
    ? {
        ...buyerDecisionInput,
        buyerGuidanceContext: effectiveBuyerGuidanceContext,
      }
    : buyerDecisionInput;
  const effectiveBuyerInputClarification = finalRecommendationReady
    ? {
        ...buyerInputClarification,
        finalRecommendationStillDisabled: false,
        buyerGuidanceContext: effectiveBuyerGuidanceContext,
      }
    : buyerInputClarification;

  const blockedReasons = [];

  if (requestedFinalRecommendation) {
    if (moduleName && moduleName !== DECISION_MODULES.RECOMMENDATION) {
      blockedReasons.push(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE);
    }

    if (missingMandatoryInputs.length > 0) {
      blockedReasons.push(BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE);
    }

    if (!evidenceGate.hasUsefulEvidence) {
      blockedReasons.push(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET);
    }

    if (!finalRecommendationReady) {
      blockedReasons.push(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY);
      blockedReasons.push(...asArray(finalRecommendation.blockedReasons));
    }
  }

  const uniqueBlockedReasons = [...new Set(blockedReasons.filter(Boolean))];
  const finalPolicyReadiness = buildFinalRecommendationPolicyReadiness({
    requestedFinalRecommendation,
    buyerDecisionInput: effectiveBuyerDecisionInput,
    buyerInputClarification: effectiveBuyerInputClarification,
    evidenceGate,
    blockedReasons: uniqueBlockedReasons,
    finalRecommendationReady,
  });


  return {
    version: 'aci_final_recommendation_eligibility_runtime_v2',
    dryRun: false,
    requestedFinalRecommendation,
    canUseForFinalRecommendation: finalRecommendationReady,
    finalRecommendationEnabled: finalRecommendationReady,
    composerReady: finalRecommendationReady,
    module: moduleName,
    evaluatedTool: bridge.tool || response.tool || '',
    allowedAnswerType: requestedFinalRecommendation
      ? (finalRecommendationReady
          ? ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED
          : provisionalGuidanceMode
            ? ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY
            : (missingMandatoryInputs.length > 0 ? ALLOWED_ANSWER_TYPES.CLARIFICATION_REQUIRED : ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY))
      : (response.decisionPolicy?.allowedAnswerType || response.data?.decisionPolicy?.allowedAnswerType || ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY),
    blockedReasons: uniqueBlockedReasons,
    missingMandatoryInputs: requestedFinalRecommendation ? missingMandatoryInputs : [],
    buyerDecisionInput: requestedFinalRecommendation ? effectiveBuyerDecisionInput : null,
    buyerInputClarification: requestedFinalRecommendation ? effectiveBuyerInputClarification : null,
    buyerGuidanceContext: requestedFinalRecommendation ? effectiveBuyerGuidanceContext : null,
    provisionalGuidanceMode: requestedFinalRecommendation && !finalRecommendationReady ? provisionalGuidanceMode : '',
    finalPolicyReadiness: requestedFinalRecommendation ? finalPolicyReadiness : null,
    presentInputs,
    evidenceStatus: evidenceGate.evidenceStatus,
    evidenceConfidence: evidenceGate.confidence,
    reason: requestedFinalRecommendation
      ? (finalRecommendationReady
          ? 'Final recommendation is enabled because buyer context, exact-variant evidence, freshness, provenance, and composer gates passed.'
          : 'Final recommendation is blocked because one or more buyer-context or evidence gates did not pass.')
      : 'No final recommendation request detected.',
  };
}

module.exports = {
  detectFinalRecommendationRequest,
  getFinalRecommendationInputPresence,
  buildFinalRecommendationEligibilityRuntime,
};
