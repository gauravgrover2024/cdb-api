#!/usr/bin/env node
'use strict';

require('dotenv/config');
const assert = require('assert');

const fail = (message, extra = {}) => {
  const error = new Error(message);
  error.extra = extra;
  throw error;
};

const asObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) ? value : {};

const asArray = (value) => (Array.isArray(value) ? value : []);

const textOf = (value) => String(value ?? '').trim();

const lower = (value) => textOf(value).toLowerCase();

const getEligibility = (response = {}) =>
  response.finalRecommendationEligibility ||
  response.data?.finalRecommendationEligibility ||
  response.meta?.finalRecommendationEligibility ||
  null;

const assertInputPresent = (inputStatus = {}, key = '', expectedPredicate = null) => {
  const entry = asObject(inputStatus[key]);
  assert.strictEqual(entry.present, true, `${key} should be present`);
  if (expectedPredicate && !expectedPredicate(entry.value)) {
    fail(`${key} value failed expectation`, { key, value: entry.value, entry });
  }
};

(async () => {
  const mongooseModule = await import('mongoose');
  const mongoose = mongooseModule.default || mongooseModule;
  const connectDbModule = await import('../../config/db.js');
  const connectDB = connectDbModule.default || connectDbModule;
  const agentModule = await import('../../services/aiAgent/aiAgent.service.js');
  const { chatWithAgent } = agentModule;

  await connectDB();

  try {
    const message = 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?';
    const response = await chatWithAgent({ message, context: {} });
    const eligibility = getEligibility(response);
    const blob = JSON.stringify(response || {});

    assert(eligibility, 'finalRecommendationEligibility should be attached for final-choice request');
    assert.strictEqual(eligibility.requestedFinalRecommendation, true, 'final-choice request should be detected');
    assert.strictEqual(eligibility.finalRecommendationEnabled, false, 'final recommendation must remain disabled');
    assert.strictEqual(eligibility.canUseForFinalRecommendation, false, 'final recommendation must not be usable yet');
    assert(!/final_recommendation_allowed/i.test(blob), 'final_recommendation_allowed must not leak');

    const buyerInput = asObject(eligibility.buyerDecisionInput);
    const inputStatus = asObject(buyerInput.inputStatus);
    const presentInputs = asArray(buyerInput.presentInputs);
    const missingInputs = asArray(buyerInput.missingMandatoryInputs);

    assertInputPresent(inputStatus, 'city', (value) => lower(value).includes('delhi'));
    assertInputPresent(inputStatus, 'budgetOrPriceCeiling', (value) => Number(value) === 1800000);
    assertInputPresent(inputStatus, 'bodyPreferenceOrPrimaryUseCase', (value) => {
      const text = lower(value);
      return text.includes('family') && text.includes('city');
    });
    assertInputPresent(inputStatus, 'familySizeOrOccupancyUse', (value) => lower(value).includes('4'));
    assertInputPresent(inputStatus, 'transmissionPreference', (value) => lower(value).includes('automatic'));
    assertInputPresent(inputStatus, 'shortlistedModelsOrDiscoveryScope', (value) => {
      const text = lower(value);
      return text.includes('family') && text.includes('automatic') && text.includes('1800000');
    });

    for (const key of [
      'city',
      'budgetOrPriceCeiling',
      'bodyPreferenceOrPrimaryUseCase',
      'familySizeOrOccupancyUse',
      'transmissionPreference',
      'shortlistedModelsOrDiscoveryScope',
    ]) {
      assert(presentInputs.includes(key), `${key} should be listed in presentInputs`);
      assert(!missingInputs.includes(key), `${key} should not be listed in missingMandatoryInputs`);
    }

    for (const key of ['fuelPreferenceOrMonthlyRunning', 'safetyPriority', 'featurePriority']) {
      assert(missingInputs.includes(key), `${key} should remain missing until user provides it`);
    }

    const buyerContext =
      response.buyerContext ||
      response.data?.buyerContext ||
      response.contextPatch?.buyerContext ||
      response.meta?.buyerContext ||
      {};

    assert.strictEqual(Number(buyerContext.budgetOrPriceCeiling), 1800000, 'response buyerContext budget should be propagated');
    assert(lower(buyerContext.familySizeOrOccupancyUse).includes('4'), 'response buyerContext family occupancy should be propagated');
    assert(lower(buyerContext.transmissionPreference).includes('automatic'), 'response buyerContext transmission should be propagated');

    console.log(JSON.stringify({
      suite: 'ACI buyer context propagation runtime smoke v1',
      ok: true,
      message,
      intent: response.intent,
      tool: response.tool,
      requestedFinalRecommendation: eligibility.requestedFinalRecommendation,
      allowedAnswerType: eligibility.allowedAnswerType,
      blockedReasons: eligibility.blockedReasons,
      presentInputs,
      missingInputs,
      normalizedBuyerInputs: buyerInput.normalizedBuyerInputs,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI buyer context propagation runtime smoke v1',
    ok: false,
    error: error.message,
    extra: error.extra || null,
  }, null, 2));
  process.exit(1);
});
