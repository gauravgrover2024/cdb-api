#!/usr/bin/env node
'use strict';

require('dotenv/config');
const assert = require('assert');

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const lower = (value) => String(value || '').toLowerCase();

const getRows = (response = {}) =>
  asArray(response.rows).length
    ? asArray(response.rows)
    : asArray(response.data?.rows).length
      ? asArray(response.data.rows)
      : [];

const getRanking = (response = {}) =>
  response.candidateDiagnosticRanking ||
  response.data?.candidateDiagnosticRanking ||
  response.meta?.candidateDiagnosticRanking ||
  response.contextPatch?.candidateDiagnosticRanking ||
  null;

const signalBand = (row = {}, key = '') => {
  const signals = [
    ...asArray(row.scoreSignals?.prioritySignals),
    ...asArray(row.scoreSignals?.signals),
  ];
  return signals.find((signal) => lower(signal.key) === lower(key))?.band || '';
};

const bandAllowedForPriority = (band = '') => !['', 'missing', 'limited'].includes(lower(band));

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
        id: 'family-auto-suv-ranking',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
        requiredPriorities: ['family', 'city', 'automatic', 'bodyStyle'],
        topSignalKey: 'practicality',
      },
      {
        id: 'safety-family-suv-ranking',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
        requiredPriorities: ['safety', 'family', 'bodyStyle'],
        topSignalKey: 'safety',
      },
      {
        id: 'final-choice-family-city-auto-ranking',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        requiredPriorities: ['family', 'city', 'automatic'],
        topSignalKey: 'practicality',
        expectsFinalBlocked: true,
      },
    ];

    const results = [];

    for (const testCase of cases) {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const rows = getRows(response);
      const ranking = getRanking(response);

      assert(rows.length > 0, `${testCase.id}: rows should exist`);
      assert(ranking, `${testCase.id}: response-level ranking contract should exist`);
      assert.strictEqual(ranking.finalRecommendationEnabled, false, `${testCase.id}: final recommendation must stay disabled`);
      assert.strictEqual(ranking.canUseForFinalRecommendation, false, `${testCase.id}: final recommendation use must stay false`);

      rows.forEach((row, index) => {
        assert(row.diagnosticRanking, `${testCase.id}: row ${index + 1} missing diagnosticRanking`);
        assert.strictEqual(row.diagnosticRanking.rank, index + 1, `${testCase.id}: row ${index + 1} rank mismatch`);
        assert.strictEqual(row.diagnosticRanking.finalRecommendationEnabled, false, `${testCase.id}: row final enabled leak`);
        assert.strictEqual(row.diagnosticRanking.canUseForFinalRecommendation, false, `${testCase.id}: row final use leak`);
        assert(row.decisionCandidate?.diagnosticRanking, `${testCase.id}: decisionCandidate diagnosticRanking missing`);
      });

      const top = rows[0] || {};
      const topPriorities = asArray(top.diagnosticRanking?.matchedPriorities);

      for (const priority of testCase.requiredPriorities) {
        assert(
          topPriorities.includes(priority),
          `${testCase.id}: top candidate should match priority ${priority}; got ${topPriorities.join(', ')}`
        );
      }

      assert(
        bandAllowedForPriority(signalBand(top, testCase.topSignalKey)),
        `${testCase.id}: top candidate should not have limited/missing ${testCase.topSignalKey} evidence`
      );

      if (testCase.expectsFinalBlocked) {
        const eligibility =
          response.finalRecommendationEligibility ||
          response.meta?.finalRecommendationEligibility ||
          response.data?.finalRecommendationEligibility ||
          null;
        assert(eligibility, `${testCase.id}: final eligibility should exist`);
        assert.strictEqual(eligibility.allowedAnswerType, 'diagnostic_only', `${testCase.id}: final answer should remain diagnostic only`);
        assert(!/final_recommendation_allowed/i.test(JSON.stringify(response)), `${testCase.id}: final allowed must not leak`);
      }

      results.push({
        id: testCase.id,
        ranking,
        top3: rows.slice(0, 3).map((row) => ({
          model: row.fullModel || row.displayName || row.model,
          previousRank: row.diagnosticRanking?.previousRank,
          rank: row.diagnosticRanking?.rank,
          rankBand: row.diagnosticRanking?.rankBand,
          matchedPriorities: row.diagnosticRanking?.matchedPriorities,
          positiveSignals: row.diagnosticRanking?.positiveSignals,
          candidateRankReason: row.candidateRankReason,
          safetyBand: signalBand(row, 'safety'),
          practicalityBand: signalBand(row, 'practicality'),
          cityBand: signalBand(row, 'cityUse'),
        })),
      });
    }

    console.log(JSON.stringify({
      suite: 'ACI candidate diagnostic ranking runtime smoke v1',
      ok: true,
      results,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI candidate diagnostic ranking runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
