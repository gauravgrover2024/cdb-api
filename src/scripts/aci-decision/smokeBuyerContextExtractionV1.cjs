#!/usr/bin/env node

const assert = require('assert');

(async () => {
  const {
    createEmptyAciContextState,
  } = await import('../../services/aciCore/context/aciContextState.contract.js');

  const {
    applyBuyerContextToContextState,
    extractBuyerContextFromMessage,
    mergeBuyerContext,
  } = await import('../../services/aciCore/context/aciBuyerContextExtractor.service.js');

  const message = 'Need a family car under 15 lakh automatic in Delhi, safety is important, with sunroof and 6 airbags';

  const extraction = extractBuyerContextFromMessage({ message });
  assert.strictEqual(extraction.buyerContextPatch.city, 'Delhi');
  assert.strictEqual(extraction.buyerContextPatch.citySlug, 'new-delhi');
  assert.strictEqual(extraction.buyerContextPatch.budgetOrPriceCeiling, 1500000);
  assert.strictEqual(extraction.buyerContextPatch.transmissionPreference, 'automatic');
  assert.strictEqual(extraction.buyerContextPatch.safetyPriority, 'high');
  assert(extraction.buyerContextPatch.bodyPreferenceOrPrimaryUseCase.includes('family'));
  assert(extraction.buyerContextPatch.featurePriority.includes('sunroof'));
  assert(extraction.buyerContextPatch.featurePriority.includes('6 airbags'));

  const previous = {
    city: 'Noida',
    citySlug: 'noida',
    featurePriority: ['rear camera'],
  };

  const merged = mergeBuyerContext(previous, extraction.buyerContextPatch);
  assert.strictEqual(merged.city, 'Delhi');
  assert.strictEqual(merged.citySlug, 'new-delhi');
  assert(merged.featurePriority.includes('rear camera'));
  assert(merged.featurePriority.includes('sunroof'));
  assert(merged.featurePriority.includes('6 airbags'));

  const state = createEmptyAciContextState({
    selectedVehicle: {
      model: 'Creta',
      city: 'Noida',
      citySlug: 'noida',
      confidence: 0.8,
    },
  });

  const updated = applyBuyerContextToContextState({
    message: 'I drive 80 km daily and want CNG or petrol manual with safety priority',
    contextState: state,
  });

  assert.strictEqual(updated.buyerContext.transmissionPreference, 'manual');
  assert(updated.buyerContext.fuelPreferenceOrMonthlyRunning.includes('CNG'));
  assert(updated.buyerContext.fuelPreferenceOrMonthlyRunning.includes('petrol'));
  assert(updated.buyerContext.fuelPreferenceOrMonthlyRunning.includes('80 km daily'));
  assert.strictEqual(updated.buyerContext.safetyPriority, 'high');
  assert(updated.provenance.sources.includes('buyer_context_extractor_v1'));

  const noSignal = applyBuyerContextToContextState({
    message: 'what about this one',
    contextState: updated,
  });

  assert.strictEqual(noSignal.buyerContext.transmissionPreference, 'manual');
  assert(noSignal.buyerContext.fuelPreferenceOrMonthlyRunning.includes('80 km daily'));

  console.log(JSON.stringify({
    suite: 'ACI Buyer Context Extraction Smoke v1',
    ok: true,
    total: 4,
    passed: 4,
    failed: 0,
    cases: [
      {
        id: 'extract-rich-buyer-message',
        buyerContextPatch: extraction.buyerContextPatch,
      },
      {
        id: 'merge-preserves-existing-feature-priorities',
        buyerContext: merged,
      },
      {
        id: 'apply-to-context-state',
        buyerContext: updated.buyerContext,
      },
      {
        id: 'no-signal-preserves-existing-context',
        buyerContext: noSignal.buyerContext,
      },
    ],
  }, null, 2));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
