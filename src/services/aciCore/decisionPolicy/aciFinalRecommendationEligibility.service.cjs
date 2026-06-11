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
  const buyerDecisionInput = buildBuyerDecisionInputContract({ context, response });
  const { presentInputs, missingMandatoryInputs } = buyerDecisionInput;
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

  const blockedReasons = [];

  if (requestedFinalRecommendation) {
    blockedReasons.push(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY);

    if (moduleName && moduleName !== DECISION_MODULES.RECOMMENDATION) {
      blockedReasons.push(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE);
    }

    if (missingMandatoryInputs.length > 0) {
      blockedReasons.push(BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE);
    }

    if (!evidenceGate.hasUsefulEvidence) {
      blockedReasons.push(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET);
    }
  }

  const uniqueBlockedReasons = [...new Set(blockedReasons.filter(Boolean))];
  const finalPolicyReadiness = buildFinalRecommendationPolicyReadiness({
    requestedFinalRecommendation,
    buyerDecisionInput,
    buyerInputClarification,
    evidenceGate,
    blockedReasons: uniqueBlockedReasons,
  });


  return {
    version: 'aci_final_recommendation_eligibility_runtime_v1',
    dryRun: true,
    requestedFinalRecommendation,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    composerReady: false,
    module: moduleName,
    evaluatedTool: bridge.tool || response.tool || '',
    allowedAnswerType: requestedFinalRecommendation
      ? (missingMandatoryInputs.length > 0 ? ALLOWED_ANSWER_TYPES.CLARIFICATION_REQUIRED : ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY)
      : (response.decisionPolicy?.allowedAnswerType || response.data?.decisionPolicy?.allowedAnswerType || ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY),
    blockedReasons: uniqueBlockedReasons,
    missingMandatoryInputs: requestedFinalRecommendation ? missingMandatoryInputs : [],
    buyerDecisionInput: requestedFinalRecommendation ? buyerDecisionInput : null,
    buyerInputClarification: requestedFinalRecommendation ? buyerInputClarification : null,
    finalPolicyReadiness: requestedFinalRecommendation ? finalPolicyReadiness : null,
    presentInputs,
    evidenceStatus: evidenceGate.evidenceStatus,
    evidenceConfidence: evidenceGate.confidence,
    reason: requestedFinalRecommendation
      ? 'Final recommendation is detected but blocked in runtime dry-run until buyer context, evidence thresholds and central decision composer are production-ready.'
      : 'No final recommendation request detected.',
  };
}

module.exports = {
  detectFinalRecommendationRequest,
  getFinalRecommendationInputPresence,
  buildFinalRecommendationEligibilityRuntime,
};
