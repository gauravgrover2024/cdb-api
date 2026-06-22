#!/usr/bin/env node
'use strict';

require('dotenv/config');
const assert = require('assert');

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const textOf = (value) => String(value ?? '').trim();

const getRows = (response = {}) =>
  asArray(response.rows).length
    ? asArray(response.rows)
    : asArray(response.data?.rows).length
      ? asArray(response.data.rows)
      : asArray(response.widget?.rows);

const getEligibility = (response = {}) =>
  response.finalRecommendationEligibility ||
  response.data?.finalRecommendationEligibility ||
  response.meta?.finalRecommendationEligibility ||
  null;

(async () => {
  const mongooseModule = await import('mongoose');
  const mongoose = mongooseModule.default || mongooseModule;
  const connectDbModule = await import('../../config/db.js');
  const connectDB = connectDbModule.default || connectDbModule;
  const agentModule = await import('../../services/aiAgent/aiAgent.service.js');
  const { chatWithAgent } = agentModule;

  await connectDB();

  try {
    const cases = [
      {
        id: 'family-auto-suv-under-18',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
        finalChoice: false,
      },
      {
        id: 'safest-suv-under-20',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
        finalChoice: false,
      },
      {
        id: 'final-choice-family-auto',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        finalChoice: true,
      },
    ];

    const results = [];

    for (const testCase of cases) {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const rows = getRows(response);
      const eligibility = getEligibility(response);
      const blob = JSON.stringify(response || {});

      assert(rows.length > 0, `${testCase.id}: recommendation rows should exist`);
      assert(!/final_recommendation_allowed/i.test(blob), `${testCase.id}: final recommendation allowed must not leak`);

      if (testCase.finalChoice) {
        assert(eligibility, `${testCase.id}: final eligibility should be attached`);
        assert.strictEqual(eligibility.requestedFinalRecommendation, true, `${testCase.id}: should be final-choice request`);
        assert.strictEqual(eligibility.finalRecommendationEnabled, false, `${testCase.id}: final recommendation must remain disabled`);
        assert.strictEqual(eligibility.canUseForFinalRecommendation, false, `${testCase.id}: final recommendation must not be usable`);
      } else if (eligibility) {
        assert.notStrictEqual(eligibility.requestedFinalRecommendation, true, `${testCase.id}: should not be final-choice request`);
      }

      const topRows = rows.slice(0, Math.min(rows.length, 5));

      for (const [index, row] of topRows.entries()) {
        assert(row.decisionCandidate, `${testCase.id}: row ${index + 1} missing decisionCandidate`);
        assert.strictEqual(row.decisionCandidate.finalRecommendationEnabled, false, `${testCase.id}: candidate final flag must stay false`);
        assert.strictEqual(row.decisionCandidate.canUseForFinalRecommendation, false, `${testCase.id}: candidate canUse final must stay false`);
        assert(row.evidenceSummary, `${testCase.id}: row ${index + 1} missing evidenceSummary`);
        assert(row.candidateRankReason, `${testCase.id}: row ${index + 1} missing candidateRankReason`);
        assert(row.scoreSignals, `${testCase.id}: row ${index + 1} missing scoreSignals`);
        assert(row.featureSignals, `${testCase.id}: row ${index + 1} missing featureSignals`);
        assert(!/score"\s*:/i.test(JSON.stringify(row.scoreSignals?.signals || [])), `${testCase.id}: raw score value should not be exposed in scoreSignals.signals`);
      }

      const sourceCollections =
        response.sourceCollections ||
        response.meta?.sourceCollections ||
        response.data?.sourceCollections ||
        response.sourceTransparency?.modulesChecked ||
        [];

      for (const collection of [
        'aci_vehicle_model_summary',
        'aci_vehicle_price_rows',
        'aci_vehicle_variant_score_profile',
        'aci_vehicle_model_feature_summary_v1',
      ]) {
        assert(
          sourceCollections.includes(collection),
          `${testCase.id}: sourceCollections should include ${collection}`
        );
      }

      results.push({
        id: testCase.id,
        intent: response.intent,
        rowCount: rows.length,
        sample: topRows.slice(0, 2).map((row) => ({
          model: row.fullModel || row.displayName || row.model,
          candidateRankReason: row.candidateRankReason,
          evidenceStatus: row.decisionCandidate?.evidenceStatus,
          fitSignals: row.evidenceSummary?.fitSignals,
          watchouts: row.evidenceSummary?.watchouts,
          scoreSummary: row.scoreSignals?.summary,
          featureSummary: row.featureSignals?.summary,
        })),
        sourceCollections,
        finalEligibility: eligibility
          ? {
              requestedFinalRecommendation: eligibility.requestedFinalRecommendation,
              allowedAnswerType: eligibility.allowedAnswerType,
              blockedReasons: eligibility.blockedReasons,
            }
          : null,
      });
    }

    console.log(JSON.stringify({
      suite: 'ACI recommendation candidate resolver runtime smoke v1',
      ok: true,
      results,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI recommendation candidate resolver runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
