#!/usr/bin/env node

const assert = require('assert');

const {
  buildBuyerDecisionInputContract,
} = require('../../services/aciCore/decisionPolicy/aciBuyerDecisionInput.contract.cjs');

const {
  buildBuyerInputClarificationPayload,
} = require('../../services/aciCore/decisionPolicy/aciBuyerInputClarification.service.cjs');

const partialInput = buildBuyerDecisionInputContract({
  context: {
    buyerContext: {
      city: 'Delhi',
      budget: 1500000,
      transmission: 'automatic',
    },
  },
});

const partialPayload = buildBuyerInputClarificationPayload({
  missingMandatoryInputs: partialInput.missingMandatoryInputs,
  buyerDecisionInput: partialInput,
  requestedFinalRecommendation: true,
});

assert.strictEqual(partialPayload.version, 'aci_buyer_input_clarification_v1');
assert.strictEqual(partialPayload.status, 'clarification_required');
assert.strictEqual(partialPayload.finalRecommendationStillDisabled, true);
assert.strictEqual(partialPayload.canProceedToFinalRecommendationPolicyEval, false);
assert(partialPayload.presentInputs.includes('city'));
assert(partialPayload.presentInputs.includes('budgetOrPriceCeiling'));
assert(partialPayload.presentInputs.includes('transmissionPreference'));
assert(partialPayload.missingInputs.includes('bodyPreferenceOrPrimaryUseCase'));
assert(partialPayload.questions.some((item) => item.key === 'safetyPriority'));
assert(partialPayload.nextBestQuestion?.question);

assert.strictEqual(partialPayload.askPolicy.mode, 'progressive_single_question');
assert.strictEqual(partialPayload.askPolicy.maxBuyerFacingQuestions, 1);
assert.strictEqual(partialPayload.askPolicy.revealAllMissingInputsToUser, false);
assert.strictEqual(partialPayload.askPolicy.learnFromSearchAndContext, true);
assert.strictEqual(partialPayload.askPolicy.askOnlyWhenNotInferable, true);
assert.strictEqual(partialPayload.askPolicy.suppressRepeatedRecommendationPrompts, true);
assert.strictEqual(partialPayload.buyerFacingQuestions.length, 1);
assert.strictEqual(partialPayload.buyerFacingRenderingContract.maxVisibleQuestions, 1);
assert(partialPayload.buyerFacingRenderingContract.doNotRenderToBuyer.includes('internalMissingInputMap'));
assert(partialPayload.buyerFacingRenderingContract.doNotRenderToBuyer.includes('questions'));
assert(partialPayload.buyerFacingRenderingContract.doNotRenderToBuyer.includes('missingInputs'));
assert.strictEqual(partialPayload.visibleQuestions.length, 1);
assert(partialPayload.internalMissingInputMap.length >= partialPayload.buyerFacingQuestions.length);

const completeInput = buildBuyerDecisionInputContract({
  context: {
    buyerContext: {
      city: 'Delhi',
      budget: 1500000,
      useCase: 'family use',
      familySize: 4,
      monthlyRunning: '1000 km monthly',
      transmission: 'automatic',
      safetyPriority: 'high',
      featurePriority: ['6 airbags'],
      discoveryScope: 'SUV under 15 lakh',
    },
  },
});

const completePayload = buildBuyerInputClarificationPayload({
  missingMandatoryInputs: completeInput.missingMandatoryInputs,
  buyerDecisionInput: completeInput,
  requestedFinalRecommendation: true,
});

assert.strictEqual(completePayload.status, 'buyer_context_complete');
assert.strictEqual(completePayload.canProceedToFinalRecommendationPolicyEval, true);
assert.deepStrictEqual(completePayload.missingInputs, []);
assert.deepStrictEqual(completePayload.questions, []);

console.log(JSON.stringify({
  suite: 'ACI Buyer Input Clarification Payload Smoke v1',
  ok: true,
  total: 2,
  passed: 2,
  failed: 0,
  cases: [
    {
      id: 'partial-context-clarification-required',
      status: partialPayload.status,
      missingInputs: partialPayload.missingInputs,
      nextBestQuestion: partialPayload.nextBestQuestion,
      buyerFacingQuestions: partialPayload.buyerFacingQuestions,
      askPolicy: partialPayload.askPolicy,
    },
    {
      id: 'complete-context-no-questions',
      status: completePayload.status,
      missingInputs: completePayload.missingInputs,
    },
  ],
}, null, 2));
