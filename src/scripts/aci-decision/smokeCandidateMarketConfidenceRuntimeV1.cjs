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

const getMarket = (response = {}) =>
  response.candidateMarketConfidence ||
  response.data?.candidateMarketConfidence ||
  response.meta?.candidateMarketConfidence ||
  response.contextPatch?.candidateMarketConfidence ||
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
        id: 'family-auto-suv-market-confidence',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
      },
      {
        id: 'safety-family-suv-market-confidence',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
      },
      {
        id: 'final-choice-market-confidence',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        expectsFinalBlocked: true,
      },
    ];

    const results = await Promise.all(cases.map(async (testCase) => {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const rows = getRows(response);
      const market = getMarket(response);

      assert(rows.length > 0, `${testCase.id}: rows should exist`);
      assert(market, `${testCase.id}: response-level candidateMarketConfidence should exist`);
      assert.strictEqual(market.finalRecommendationEnabled, false, `${testCase.id}: final recommendation must stay disabled`);
      assert.strictEqual(market.canUseForFinalRecommendation, false, `${testCase.id}: final use must stay false`);

      rows.forEach((row, index) => {
        assert(row.candidateMarketConfidence, `${testCase.id}: row ${index + 1} missing candidateMarketConfidence`);
        assert(row.decisionCandidate?.marketConfidence, `${testCase.id}: row ${index + 1} missing decisionCandidate.marketConfidence`);
        assert.strictEqual(row.candidateMarketConfidence.finalRecommendationEnabled, false, `${testCase.id}: row final enabled leak`);
        assert.strictEqual(row.candidateMarketConfidence.canUseForFinalRecommendation, false, `${testCase.id}: row final use leak`);
        assert(row.diagnosticRanking, `${testCase.id}: row ${index + 1} missing diagnosticRanking after market confidence`);
      });

      const top = rows[0];
      assert(
        ['strong', 'good'].includes(top.candidateMarketConfidence?.confidenceBand),
        `${testCase.id}: top candidate should have good/strong market confidence; got ${top.candidateMarketConfidence?.confidenceBand}`
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

      return {
        id: testCase.id,
        market,
        top5: rows.slice(0, 5).map((row) => ({
          model: row.fullModel || row.displayName || row.model,
          rank: row.diagnosticRanking?.rank,
          previousRank: row.diagnosticRanking?.previousRank,
          confidenceBand: row.candidateMarketConfidence?.confidenceBand,
          requestedCity: row.candidateMarketConfidence?.requestedCity,
          requestedCityPriceRows: row.candidateMarketConfidence?.evidence?.requestedCityPriceRows,
          requestedCityVariantCount: row.candidateMarketConfidence?.evidence?.requestedCityVariantCount,
          risks: row.candidateMarketConfidence?.risks,
          positiveSignals: row.diagnosticRanking?.positiveSignals,
          tradeoffs: row.diagnosticRanking?.tradeoffs,
        })),
      };
    }));

    console.log(JSON.stringify({
      suite: 'ACI candidate market confidence runtime smoke v1',
      ok: true,
      results,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI candidate market confidence runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
