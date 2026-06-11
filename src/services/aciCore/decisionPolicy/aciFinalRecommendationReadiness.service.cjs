const READINESS_VERSION = 'aci_final_recommendation_policy_readiness_v1';

const unique = (items = []) => [...new Set((items || []).filter(Boolean))];

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const asObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) ? value : {};

function buildFinalRecommendationPolicyReadiness({
  requestedFinalRecommendation = false,
  buyerDecisionInput = {},
  buyerInputClarification = {},
  evidenceGate = {},
  blockedReasons = [],
} = {}) {
  const input = asObject(buyerDecisionInput);
  const clarification = asObject(buyerInputClarification);
  const evidence = asObject(evidenceGate);
  const missingInputs = asArray(input.missingMandatoryInputs || clarification.missingInputs);
  const buyerContextComplete = missingInputs.length === 0;
  const evidenceThresholdMet = Boolean(evidence.hasUsefulEvidence);

  const gates = {
    requestedFinalRecommendation: Boolean(requestedFinalRecommendation),
    buyerContextComplete,
    evidenceThresholdMet,
    finalRecommendationPolicyReady: false,
    finalComposerReady: false,
    recommendationActivationEnabled: false,
    noFloodClarificationReady:
      clarification?.askPolicy?.mode === 'progressive_single_question' &&
      Number(clarification?.askPolicy?.maxBuyerFacingQuestions || 0) === 1 &&
      clarification?.askPolicy?.learnFromSearchAndContext === true,
    buyerFacingRenderingSafe:
      Number(clarification?.buyerFacingRenderingContract?.maxVisibleQuestions || 0) === 1 &&
      Array.isArray(clarification?.buyerFacingRenderingContract?.doNotRenderToBuyer) &&
      clarification.buyerFacingRenderingContract.doNotRenderToBuyer.includes('internalMissingInputMap'),
  };

  const readinessBlockers = [];
  if (!requestedFinalRecommendation) readinessBlockers.push('final_recommendation_not_requested');
  if (!buyerContextComplete) readinessBlockers.push('buyer_context_incomplete');
  if (!evidenceThresholdMet) readinessBlockers.push('evidence_threshold_not_met');
  readinessBlockers.push('final_recommendation_policy_not_ready');
  readinessBlockers.push('final_composer_not_ready');
  readinessBlockers.push('recommendation_activation_disabled');

  return {
    version: READINESS_VERSION,
    status: requestedFinalRecommendation ? 'blocked_not_ready' : 'not_requested',
    canActivateFinalRecommendation: false,
    activationMode: 'disabled_dry_run',
    gates,
    blockedReasons: unique([...asArray(blockedReasons), ...readinessBlockers]),
    missingInputs,
    buyerFacingQuestionCount: asArray(clarification.buyerFacingQuestions).length,
    internalMissingInputCount: asArray(clarification.internalMissingInputMap).length,
    nextAllowedStep: buyerContextComplete
      ? 'continue_diagnostic_decision_support'
      : 'ask_one_next_best_question_or_learn_from_next_search',
  };
}

module.exports = {
  READINESS_VERSION,
  buildFinalRecommendationPolicyReadiness,
};
