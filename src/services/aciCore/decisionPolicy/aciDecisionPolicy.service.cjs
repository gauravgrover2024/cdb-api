const {
  CLAIM_TYPES,
  ALLOWED_ANSWER_TYPES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  DEGRADED_MODES,
  BLOCKED_REASONS,
  MANDATORY_FINAL_RECOMMENDATION_INPUTS,
} = require('./aciDecisionPolicy.constants.cjs');

const {
  normalizeDecisionOutput,
  createBaseDecisionPolicy,
  decisionOutputHasUsefulResult,
} = require('./aciDecisionOutput.contract.cjs');

const asObject = (value) => (value && typeof value === 'object' && !Array.isArray(value) ? value : {});
const uniq = (values) => [...new Set((values || []).filter(Boolean))];

function getMissingFinalRecommendationInputs(buyerContext = {}) {
  const context = asObject(buyerContext);

  const checks = {
    city: Boolean(context.city),
    budgetOrPriceCeiling: Boolean(context.budget || context.priceCeiling || context.budgetMax),
    bodyPreferenceOrPrimaryUseCase: Boolean(context.bodyPreference || context.primaryUseCase || context.useCase),
    familySizeOrOccupancyUse: Boolean(context.familySize || context.occupancyUse || context.occupancy || context.seatingNeed),
    fuelPreferenceOrMonthlyRunning: Boolean(context.fuelPreference || context.monthlyRunning || context.runningPattern),
    transmissionPreference: Boolean(context.transmissionPreference || context.transmission),
    safetyPriority: Boolean(context.safetyPriority),
    featurePriority: Boolean(context.featurePriority || context.priorityFeatures),
    shortlistedModelsOrDiscoveryScope: Boolean(
      context.shortlistedModels ||
      context.discoveryScope ||
      context.models ||
      context.model ||
      context.bodyPreference ||
      context.primaryUseCase
    ),
  };

  return MANDATORY_FINAL_RECOMMENDATION_INPUTS.filter((key) => !checks[key]);
}

function evaluateDecisionPolicy(input = {}) {
  const output = normalizeDecisionOutput(input);
  const buyerContext = asObject(input.buyerContext || output.buyerContext);
  const existingPolicy = createBaseDecisionPolicy(output.decisionPolicy);

  const blockedReasons = [...(existingPolicy.blockedReasons || [])];
  const missingMandatoryInputs = uniq([
    ...(existingPolicy.missingMandatoryInputs || []),
    ...getMissingFinalRecommendationInputs(buyerContext),
  ]);

  const hasUsefulResult = decisionOutputHasUsefulResult(output);
  const evidence = output.evidence || {};
  const provenance = output.provenance || {};

  let degradedMode = output.degradedMode || existingPolicy.degradedMode || null;
  let allowedAnswerType = existingPolicy.allowedAnswerType || ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY;
  let canUseForFinalRecommendation = false;
  let claimType = existingPolicy.claimType || output.claimType || CLAIM_TYPES.DIAGNOSTIC;

  if (!hasUsefulResult) {
    blockedReasons.push(BLOCKED_REASONS.EMPTY_RESULT);
    degradedMode = degradedMode || DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED;
    allowedAnswerType = ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED;
  }

  if (missingMandatoryInputs.length > 0) {
    blockedReasons.push(BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE);
    degradedMode = degradedMode || DEGRADED_MODES.BUYER_CONTEXT_INCOMPLETE;
    allowedAnswerType =
      allowedAnswerType === ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED
        ? allowedAnswerType
        : ALLOWED_ANSWER_TYPES.CLARIFICATION_REQUIRED;
  }

  if (
    evidence.evidenceStatus === EVIDENCE_STATUS.MISSING ||
    Number(evidence.requiredEvidenceCount || 0) > Number(evidence.usableEvidenceCount || 0)
  ) {
    blockedReasons.push(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET);
    degradedMode = degradedMode || DEGRADED_MODES.EVIDENCE_CONFIDENCE_TOO_LOW;
    if (allowedAnswerType !== ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED) {
      allowedAnswerType = ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY;
    }
  }

  if (evidence.evidenceStatus === EVIDENCE_STATUS.CONFLICTING) {
    blockedReasons.push(BLOCKED_REASONS.CONFLICTING_EVIDENCE);
    degradedMode = DEGRADED_MODES.CONFLICTING_EVIDENCE_BLOCKED;
    allowedAnswerType = ALLOWED_ANSWER_TYPES.BLOCKED;
  }

  if (evidence.evidenceStatus === EVIDENCE_STATUS.STALE || provenance.needsRebuild === true) {
    blockedReasons.push(BLOCKED_REASONS.STALE_ARTIFACT);
    degradedMode = degradedMode || DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD;
    if (allowedAnswerType !== ALLOWED_ANSWER_TYPES.BLOCKED) {
      allowedAnswerType = ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY;
    }
  }

  if (input.unsupportedCity === true || degradedMode === DEGRADED_MODES.UNSUPPORTED_CITY) {
    blockedReasons.push(BLOCKED_REASONS.UNSUPPORTED_CITY);
    degradedMode = DEGRADED_MODES.UNSUPPORTED_CITY;
    allowedAnswerType = ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED;
  }

  if (input.sponsoredInfluenceDetected === true) {
    blockedReasons.push(BLOCKED_REASONS.SPONSORED_INFLUENCE_NOT_ALLOWED);
    allowedAnswerType = ALLOWED_ANSWER_TYPES.BLOCKED;
    canUseForFinalRecommendation = false;
  }

  const evidenceReady =
    hasUsefulResult &&
    evidence.evidenceStatus === EVIDENCE_STATUS.COMPLETE &&
    evidence.confidence === CONFIDENCE_LEVELS.HIGH &&
    provenance.needsRebuild !== true &&
    !degradedMode;

  if (
    input.requestedFinalRecommendation === true &&
    evidenceReady &&
    missingMandatoryInputs.length === 0 &&
    blockedReasons.length === 0
  ) {
    canUseForFinalRecommendation = true;
    allowedAnswerType = ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED;
    claimType = CLAIM_TYPES.OPINION;
  }

  if (input.requestedFinalRecommendation === true && !canUseForFinalRecommendation) {
    blockedReasons.push(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY);
    degradedMode = degradedMode || DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED;
    if (allowedAnswerType === ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED) {
      allowedAnswerType = ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY;
    }
  }

  return createBaseDecisionPolicy({
    canUseForFinalRecommendation,
    allowedAnswerType,
    blockedReasons: uniq(blockedReasons),
    missingMandatoryInputs,
    evidenceStatus: evidence.evidenceStatus || existingPolicy.evidenceStatus,
    confidence: evidence.confidence || existingPolicy.confidence,
    degradedMode,
    claimType,
  });
}

function applyDecisionPolicy(input = {}) {
  const output = normalizeDecisionOutput(input);
  const decisionPolicy = evaluateDecisionPolicy(input);

  return {
    ...output,
    claimType: decisionPolicy.claimType,
    degradedMode: decisionPolicy.degradedMode,
    decisionPolicy,
  };
}

module.exports = {
  getMissingFinalRecommendationInputs,
  evaluateDecisionPolicy,
  applyDecisionPolicy,
};
