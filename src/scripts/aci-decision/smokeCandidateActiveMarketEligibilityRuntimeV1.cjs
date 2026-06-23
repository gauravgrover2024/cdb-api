#!/usr/bin/env node
'use strict';

require('dotenv/config');
const assert = require('assert');

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);

const getRows = (response = {}) =>
  asArray(response.rows).length
    ? asArray(response.rows)
    : asArray(response.data?.rows).length
      ? asArray(response.data.rows)
      : [];

const getActiveMarket = (response = {}) =>
  response.candidateActiveMarketEligibility ||
  response.data?.candidateActiveMarketEligibility ||
  response.meta?.candidateActiveMarketEligibility ||
  response.contextPatch?.candidateActiveMarketEligibility ||
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
        id: 'family-auto-suv-active-market',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
      },
      {
        id: 'safety-family-suv-active-market',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
      },
      {
        id: 'final-choice-active-market-still-blocked',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        expectsFinalBlocked: true,
      },
    ];

    const results = [];

    for (const testCase of cases) {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const rows = getRows(response);
      const activeMarket = getActiveMarket(response);

      assert(rows.length > 0, `${testCase.id}: rows should exist`);
      assert(activeMarket, `${testCase.id}: response-level candidateActiveMarketEligibility should exist`);
      assert.strictEqual(activeMarket.finalRecommendationEnabled, false, `${testCase.id}: final recommendation must stay disabled`);
      assert.strictEqual(activeMarket.canUseForFinalRecommendation, false, `${testCase.id}: final use must stay false`);
      assert.strictEqual(activeMarket.finalEligibleCount, 0, `${testCase.id}: final eligible count must stay zero`);
      assert.strictEqual(
        activeMarket.currentMarketValidationStatus,
        'external_current_market_validation_required_for_final',
        `${testCase.id}: external validation status should be required`,
      );

      rows.forEach((row, index) => {
        assert(row.candidateActiveMarketEligibility, `${testCase.id}: row ${index + 1} missing candidateActiveMarketEligibility`);
        assert(row.decisionCandidate?.activeMarketEligibility, `${testCase.id}: row ${index + 1} missing decisionCandidate.activeMarketEligibility`);
        assert.strictEqual(row.candidateActiveMarketEligibility.finalRecommendationEnabled, false, `${testCase.id}: row final enabled leak`);
        assert.strictEqual(row.candidateActiveMarketEligibility.canUseForFinalRecommendation, false, `${testCase.id}: row final use leak`);
        assert.strictEqual(
          row.candidateActiveMarketEligibility.currentMarketValidationStatus,
          'external_current_market_validation_required_for_final',
          `${testCase.id}: row should require external validation for final`,
        );
        assert(row.diagnosticRanking, `${testCase.id}: row ${index + 1} missing diagnosticRanking after active-market guard`);
      });

      const top = rows[0];
      assert(
        top.candidateActiveMarketEligibility?.diagnosticUseAllowed === true,
        `${testCase.id}: top candidate should be allowed for diagnostic use`,
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
        activeMarket,
        top5: rows.slice(0, 5).map((row) => ({
          model: row.fullModel || row.displayName || row.model,
          rank: row.diagnosticRanking?.rank,
          previousRank: row.diagnosticRanking?.previousRank,
          activeMarketStatus: row.candidateActiveMarketEligibility?.status,
          activeMarketConfidenceBand: row.candidateActiveMarketEligibility?.activeMarketConfidenceBand,
          diagnosticUseAllowed: row.candidateActiveMarketEligibility?.diagnosticUseAllowed,
          currentMarketValidationStatus: row.candidateActiveMarketEligibility?.currentMarketValidationStatus,
          requestedCityPriceRows: row.candidateActiveMarketEligibility?.evidence?.requestedCityPriceRows,
          requestedCityVariantCount: row.candidateActiveMarketEligibility?.evidence?.requestedCityVariantCount,
          risks: row.candidateActiveMarketEligibility?.risks,
          positiveSignals: row.diagnosticRanking?.positiveSignals,
          tradeoffs: row.diagnosticRanking?.tradeoffs,
        })),
      });
    }

    console.log(JSON.stringify({
      suite: 'ACI candidate active-market eligibility runtime smoke v1',
      ok: true,
      results,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI candidate active-market eligibility runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
