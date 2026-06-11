#!/usr/bin/env node

const assert = require('assert');

(async () => {
  const {
    createEmptyAciContextState,
  } = await import('../../services/aciCore/context/aciContextState.contract.js');

  const {
    applyBuyerContextToContextState,
  } = await import('../../services/aciCore/context/aciBuyerContextExtractor.service.js');

  const {
    buildBuyerDecisionInputContract,
  } = require('../../services/aciCore/decisionPolicy/aciBuyerDecisionInput.contract.cjs');

  const {
    buildBuyerInputClarificationPayload,
  } = require('../../services/aciCore/decisionPolicy/aciBuyerInputClarification.service.cjs');

  const {
    buildFinalRecommendationPolicyReadiness,
  } = require('../../services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs');

  const baseState = createEmptyAciContextState({
    buyerContext: {
      city: 'Delhi',
      citySlug: 'new-delhi',
      budgetOrPriceCeiling: 1500000,
      maxBudget: 1500000,
      transmissionPreference: 'automatic',
      featurePriority: ['sunroof'],
      source: 'existing_session_context',
    },
  });

  const noSignal = applyBuyerContextToContextState({
    message: 'what about this one',
    contextState: baseState,
  });

  assert.strictEqual(noSignal.buyerContext.city, 'Delhi');
  assert.strictEqual(noSignal.buyerContext.maxBudget, 1500000);
  assert.strictEqual(noSignal.buyerContext.transmissionPreference, 'automatic');
  assert(noSignal.buyerContext.featurePriority.includes('sunroof'));

  const updated = applyBuyerContextToContextState({
    message: 'Actually Gurgaon and budget 18 lakh, keep automatic, safety is important and need 6 airbags',
    contextState: noSignal,
  });

  assert.strictEqual(updated.buyerContext.city, 'Gurgaon');
  assert.strictEqual(updated.buyerContext.citySlug, 'gurgaon');
  assert.strictEqual(updated.buyerContext.maxBudget, 1800000);
  assert.strictEqual(updated.buyerContext.transmissionPreference, 'automatic');
  assert.strictEqual(updated.buyerContext.safetyPriority, 'high');
  assert(updated.buyerContext.featurePriority.includes('sunroof'));
  assert(updated.buyerContext.featurePriority.includes('6 airbags'));

  const buyerDecisionInput = buildBuyerDecisionInputContract({
    context: {
      contextState: updated,
    },
  });

  const clarification = buildBuyerInputClarificationPayload({
    missingMandatoryInputs: buyerDecisionInput.missingMandatoryInputs,
    buyerDecisionInput,
    requestedFinalRecommendation: true,
  });

  assert.strictEqual(clarification.askPolicy.mode, 'progressive_single_question');
  assert.strictEqual(clarification.askPolicy.maxBuyerFacingQuestions, 1);
  assert.strictEqual(clarification.askPolicy.revealAllMissingInputsToUser, false);
  assert.strictEqual(clarification.buyerFacingQuestions.length <= 1, true);
  assert.strictEqual(clarification.visibleQuestions.length <= 1, true);
  assert.strictEqual(clarification.buyerFacingRenderingContract.maxVisibleQuestions, 1);
  assert(clarification.buyerFacingRenderingContract.doNotRenderToBuyer.includes('internalMissingInputMap'));
  assert(clarification.buyerFacingRenderingContract.doNotRenderToBuyer.includes('questions'));
  assert(clarification.buyerFacingRenderingContract.doNotRenderToBuyer.includes('missingInputs'));

  const readiness = buildFinalRecommendationPolicyReadiness({
    requestedFinalRecommendation: true,
    buyerDecisionInput,
    buyerInputClarification: clarification,
    evidenceGate: {
      hasUsefulEvidence: true,
    },
    blockedReasons: ['final_recommendation_policy_not_ready'],
  });

  assert.strictEqual(readiness.version, 'aci_final_recommendation_policy_readiness_v1');
  assert.strictEqual(readiness.canActivateFinalRecommendation, false);
  assert.strictEqual(readiness.gates.recommendationActivationEnabled, false);
  assert.strictEqual(readiness.gates.finalRecommendationPolicyReady, false);
  assert.strictEqual(readiness.gates.finalComposerReady, false);
  assert.strictEqual(readiness.gates.noFloodClarificationReady, true);
  assert.strictEqual(readiness.gates.buyerFacingRenderingSafe, true);
  assert(readiness.blockedReasons.includes('recommendation_activation_disabled'));

  console.log(JSON.stringify({
    suite: 'ACI Buyer Context Reuse and Final Readiness Smoke v1',
    ok: true,
    total: 4,
    passed: 4,
    failed: 0,
    cases: [
      {
        id: 'no-signal-preserves-existing-context',
        buyerContext: noSignal.buyerContext,
      },
      {
        id: 'new-explicit-signal-updates-context',
        buyerContext: updated.buyerContext,
      },
      {
        id: 'no-flood-rendering-contract',
        buyerFacingQuestions: clarification.buyerFacingQuestions,
        hiddenFromBuyer: clarification.buyerFacingRenderingContract.doNotRenderToBuyer,
      },
      {
        id: 'final-policy-readiness-still-disabled',
        readiness,
      },
    ],
  }, null, 2));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
