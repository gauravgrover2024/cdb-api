#!/usr/bin/env node

const assert = require('assert');

const {
  buildBuyerDecisionInputContract,
} = require('../../services/aciCore/decisionPolicy/aciBuyerDecisionInput.contract.cjs');

const required = [
  'city',
  'budgetOrPriceCeiling',
  'bodyPreferenceOrPrimaryUseCase',
  'familySizeOrOccupancyUse',
  'fuelPreferenceOrMonthlyRunning',
  'transmissionPreference',
  'safetyPriority',
  'featurePriority',
  'shortlistedModelsOrDiscoveryScope',
];

const empty = buildBuyerDecisionInputContract({});
assert.strictEqual(empty.version, 'aci_buyer_decision_input_contract_v1');
assert.strictEqual(empty.readyForFinalRecommendationPolicyEval, false);
assert.deepStrictEqual(empty.missingMandatoryInputs, required);
assert.strictEqual(empty.completionRatio, 0);

const complete = buildBuyerDecisionInputContract({
  context: {
    buyerIntent: {
      city: 'Delhi',
      budgetMax: 1500000,
      useCase: 'family city and highway',
      familySize: 4,
      monthlyRunning: '1200 km',
      transmission: 'automatic',
      safetyPriority: 'high',
      priorityFeatures: ['six airbags', 'sunroof'],
      discoveryScope: 'SUVs under 15 lakh',
    },
  },
});

assert.strictEqual(complete.readyForFinalRecommendationPolicyEval, true);
assert.deepStrictEqual(complete.missingMandatoryInputs, []);
assert.strictEqual(complete.completionRatio, 1);
assert.strictEqual(complete.inputStatus.city.source, 'buyerContext.city');
assert.strictEqual(complete.inputStatus.budgetOrPriceCeiling.source, 'buyerContext.budgetMax');
assert.strictEqual(complete.inputStatus.featurePriority.present, true);

const mixed = buildBuyerDecisionInputContract({
  context: {
    contextState: {
      buyerContext: {
        citySlug: 'noida',
        primaryUseCase: 'daily family use',
        runningPattern: 'low running',
        transmissionPreference: 'manual',
        priorities: {
          safety: 'medium',
          features: ['rear camera'],
        },
      },
      selectedVehicle: {
        model: 'Baleno',
        city: 'Noida',
      },
    },
    filters: {
      maxPrice: 1000000,
    },
  },
  response: {
    data: {
      familySize: 3,
    },
  },
});

assert.strictEqual(mixed.readyForFinalRecommendationPolicyEval, true);
assert.strictEqual(mixed.normalizedBuyerInputs.city, 'noida');
assert.strictEqual(mixed.inputStatus.shortlistedModelsOrDiscoveryScope.source, 'selectedVehicle.model');
assert.strictEqual(mixed.inputStatus.budgetOrPriceCeiling.source, 'filters.maxPrice');

const partial = buildBuyerDecisionInputContract({
  context: {
    buyerContext: {
      city: 'Delhi',
      budget: 1200000,
    },
  },
});

assert.strictEqual(partial.readyForFinalRecommendationPolicyEval, false);
assert(partial.missingMandatoryInputs.includes('safetyPriority'));
assert(partial.presentInputs.includes('city'));
assert(partial.presentInputs.includes('budgetOrPriceCeiling'));

console.log(JSON.stringify({
  suite: 'ACI Buyer Decision Input Contract Smoke v1',
  ok: true,
  total: 4,
  passed: 4,
  failed: 0,
  cases: [
    {
      id: 'empty-context',
      completionRatio: empty.completionRatio,
      missing: empty.missingMandatoryInputs,
    },
    {
      id: 'complete-buyer-intent',
      completionRatio: complete.completionRatio,
      missing: complete.missingMandatoryInputs,
    },
    {
      id: 'mixed-context-state-response',
      completionRatio: mixed.completionRatio,
      missing: mixed.missingMandatoryInputs,
    },
    {
      id: 'partial-context',
      completionRatio: partial.completionRatio,
      missing: partial.missingMandatoryInputs,
    },
  ],
}, null, 2));
