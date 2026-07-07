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

const getComposer = (response = {}) =>
  response.diagnosticShortlistComposer ||
  response.data?.diagnosticShortlistComposer ||
  response.meta?.diagnosticShortlistComposer ||
  response.contextPatch?.diagnosticShortlistComposer ||
  null;

const unsafeFinalLanguage =
  /\b(you should buy|buy this car|buy it|must buy|clear winner|best final choice|my verdict|final verdict)\b/i;

const internalLeakLanguage =
  /\b(candidateMarketConfidence|candidateActiveMarketEligibility|candidateEvidenceReadiness|candidateDiagnosticRanking|finalRecommendationEnabled|canUseForFinalRecommendation|allowedAnswerType|blockedReasons|diagnosticOnly|currentMarketValidationStatus|db_evidence_|external_current_market_validation_required_for_final)\b/i;

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
        id: 'family-auto-suv-diagnostic-composer',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
        shouldHaveQuestion: false,
      },
      {
        id: 'safety-family-suv-diagnostic-composer',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
        shouldHaveQuestion: false,
      },
      {
        id: 'final-choice-diagnostic-composer',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        shouldHaveQuestion: true,
      },
    ];

    const results = [];

    for (const testCase of cases) {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const rows = getRows(response);
      const composer = getComposer(response);
      const visibleText = [response.title, response.answer, response.data?.title, response.data?.answer]
        .filter(Boolean)
        .join('\n');

      assert(rows.length > 0, `${testCase.id}: rows should exist`);
      assert(composer, `${testCase.id}: diagnosticShortlistComposer should exist`);
      assert.strictEqual(composer.canUseForFinalRecommendation, false, `${testCase.id}: final use must stay false`);
      assert.strictEqual(composer.finalRecommendationEnabled, false, `${testCase.id}: final enabled must stay false`);
      assert(!unsafeFinalLanguage.test(visibleText), `${testCase.id}: unsafe final verdict language leaked: ${visibleText}`);
      assert(!internalLeakLanguage.test(visibleText), `${testCase.id}: internal language leaked: ${visibleText}`);
      assert(/diagnostic shortlist|diagnostic/i.test(visibleText), `${testCase.id}: answer should clearly frame diagnostic shortlist`);

      assert(
        response.answer === composer.answer,
        `${testCase.id}: root buyer-visible answer must equal diagnostic composer answer. root=${response.answer} composer=${composer.answer}`,
      );

      assert(
        response.data?.answer === composer.answer,
        `${testCase.id}: data buyer-visible answer must equal diagnostic composer answer. data=${response.data?.answer} composer=${composer.answer}`,
      );

      assert(
        composer.topModels?.[0] && response.answer.includes(composer.topModels[0]),
        `${testCase.id}: buyer-visible answer should mention top diagnostic model. answer=${response.answer}`,
      );

      assert(
        rows[0]?.diagnosticRanking?.candidateRankReason || rows[0]?.candidateRankReason,
        `${testCase.id}: top row should keep candidate rank reason`,
      );

      if (testCase.shouldHaveQuestion) {
        assert(
          composer.buyerFacingQuestion?.question,
          `${testCase.id}: final-choice diagnostic answer should preserve one buyer-facing question`,
        );
        assert(
          /monthly|daily|running|fuel|petrol|diesel|cng|hybrid|ev/i.test(composer.buyerFacingQuestion.question),
          `${testCase.id}: next best question should be running/fuel first`,
        );
      }

      const eligibility =
        response.finalRecommendationEligibility ||
        response.meta?.finalRecommendationEligibility ||
        response.data?.finalRecommendationEligibility ||
        null;

      if (eligibility) {
        assert.strictEqual(eligibility.canUseForFinalRecommendation, false, `${testCase.id}: eligibility final use leaked`);
        assert.strictEqual(eligibility.finalRecommendationEnabled, false, `${testCase.id}: eligibility final enabled leaked`);
      }

      results.push({
        id: testCase.id,
        title: response.title,
        answer: response.answer,
        composer: {
          status: composer.status,
          requestedFinalRecommendation: composer.requestedFinalRecommendation,
          topModels: composer.topModels,
          limitations: composer.limitations,
          buyerFacingQuestion: composer.buyerFacingQuestion?.question || '',
          safety: composer.safety,
        },
        top3: rows.slice(0, 3).map((row) => ({
          model: row.fullModel || row.displayName || row.model,
          rank: row.diagnosticRanking?.rank,
          reason: row.diagnosticRanking?.candidateRankReason || row.candidateRankReason,
        })),
      });
    }

    console.log(JSON.stringify({
      suite: 'ACI diagnostic shortlist composer runtime smoke v1',
      ok: true,
      results,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI diagnostic shortlist composer runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
