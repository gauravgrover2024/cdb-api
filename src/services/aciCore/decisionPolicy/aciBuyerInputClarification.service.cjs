const CLARIFICATION_VERSION = 'aci_buyer_input_clarification_v1';

const INPUT_LABELS = Object.freeze({
  city: 'city',
  budgetOrPriceCeiling: 'budget ceiling',
  bodyPreferenceOrPrimaryUseCase: 'body type or primary use case',
  familySizeOrOccupancyUse: 'family size or occupancy use',
  fuelPreferenceOrMonthlyRunning: 'fuel preference or monthly running',
  transmissionPreference: 'transmission preference',
  safetyPriority: 'safety priority',
  featurePriority: 'feature priorities',
  shortlistedModelsOrDiscoveryScope: 'shortlisted cars or discovery scope',
});

const INPUT_QUESTIONS = Object.freeze({
  city: 'Which city should I use for price and availability?',
  budgetOrPriceCeiling: 'What is your maximum budget or price ceiling?',
  bodyPreferenceOrPrimaryUseCase: 'Is the car mainly for family use, city driving, highway use, bad roads, or mixed use?',
  familySizeOrOccupancyUse: 'How many people will usually sit in the car?',
  fuelPreferenceOrMonthlyRunning: 'What is your monthly or daily running, and do you prefer petrol, diesel, CNG, hybrid, or EV?',
  transmissionPreference: 'Do you prefer manual or automatic?',
  safetyPriority: 'How important is safety for you: high, medium, or normal?',
  featurePriority: 'Which features are must-have for you?',
  shortlistedModelsOrDiscoveryScope: 'Do you already have shortlisted cars, or should I search within a body type/budget scope?',
});

const INPUT_OPTIONS = Object.freeze({
  city: ['Delhi', 'Noida', 'Gurgaon'],
  bodyPreferenceOrPrimaryUseCase: ['family use', 'city use', 'highway use', 'bad-road use', 'mixed use'],
  fuelPreferenceOrMonthlyRunning: ['low running petrol', 'high running CNG', 'highway diesel', 'EV with home charging', 'open to all'],
  transmissionPreference: ['manual', 'automatic', 'open to both'],
  safetyPriority: ['high safety priority', 'balanced safety', 'normal priority'],
  featurePriority: ['6 airbags', 'sunroof', 'ADAS', 'rear camera', 'ventilated seats', 'wireless charging'],
});

const ASK_POLICY = Object.freeze({
  mode: 'progressive_single_question',
  maxBuyerFacingQuestions: 1,
  revealAllMissingInputsToUser: false,
  learnFromSearchAndContext: true,
  askOnlyWhenNotInferable: true,
  suppressRepeatedRecommendationPrompts: true,
});

const BUYER_FACING_RENDERING_CONTRACT = Object.freeze({
  version: 'aci_buyer_facing_question_rendering_contract_v1',
  renderOnly: ['buyerFacingQuestions[0]', 'nextBestQuestion'],
  preferredPath: 'buyerFacingQuestions[0]',
  fallbackPath: 'nextBestQuestion',
  maxVisibleQuestions: 1,
  doNotRenderToBuyer: ['internalMissingInputMap', 'questions', 'missingInputs'],
  internalOnlyPurpose: 'policy_debug_composer_only',
});

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const asObject = (value) => value && typeof value === 'object' && !Array.isArray(value) ? value : {};
const cleanText = (value = '') => String(value || '').trim();

function buildBuyerInputClarificationPayload({
  missingMandatoryInputs = [],
  buyerDecisionInput = null,
  requestedFinalRecommendation = false,
} = {}) {
  const missing = asArray(missingMandatoryInputs).filter(Boolean);
  const inputStatus = asObject(buyerDecisionInput?.inputStatus);
  const presentInputs = asArray(buyerDecisionInput?.presentInputs);

  const questions = missing.map((key) => ({
    key,
    label: INPUT_LABELS[key] || key,
    question: INPUT_QUESTIONS[key] || `Please share ${INPUT_LABELS[key] || key}.`,
    required: true,
    options: INPUT_OPTIONS[key] || [],
    currentValue: cleanText(inputStatus[key]?.value || ''),
    source: cleanText(inputStatus[key]?.source || ''),
  }));

  const nextBestQuestion = questions[0] || null;
  const visibleQuestions = nextBestQuestion ? [nextBestQuestion] : [];

  return {
    version: CLARIFICATION_VERSION,
    requestedFinalRecommendation: Boolean(requestedFinalRecommendation),
    status: missing.length ? 'clarification_required' : 'buyer_context_complete',
    missingInputs: missing,
    presentInputs,
    completionRatio: buyerDecisionInput?.completionRatio ?? null,
    questions,
    nextBestQuestion,
    buyerFacingQuestions: visibleQuestions,
    visibleQuestions,
    askPolicy: ASK_POLICY,
    buyerFacingRenderingContract: BUYER_FACING_RENDERING_CONTRACT,
    questionStrategy: ASK_POLICY.mode,
    shouldAskBuyerNow: Boolean(nextBestQuestion),
    internalMissingInputMap: questions,
    canProceedToFinalRecommendationPolicyEval: missing.length === 0,
    finalRecommendationStillDisabled: true,
  };
}

module.exports = {
  CLARIFICATION_VERSION,
  INPUT_LABELS,
  INPUT_OPTIONS,
  INPUT_QUESTIONS,
  buildBuyerInputClarificationPayload,
};
