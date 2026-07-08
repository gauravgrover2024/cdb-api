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

const getReadiness = (response = {}) =>
  response.candidateEvidenceReadiness ||
  response.data?.candidateEvidenceReadiness ||
  response.meta?.candidateEvidenceReadiness ||
  response.contextPatch?.candidateEvidenceReadiness ||
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
        id: 'diagnostic-family-auto-suv',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
        expectsFinalChoice: false,
      },
      {
        id: 'safety-family-suv',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
        expectsFinalChoice: false,
      },
      {
        id: 'final-choice-still-blocked',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        expectsFinalChoice: true,
      },
    ];

    const results = await Promise.all(cases.map(async (testCase) => {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const rows = getRows(response);
      const readiness = getReadiness(response);
      const blob = JSON.stringify(response || {});

      assert(rows.length > 0, `${testCase.id}: rows should exist`);
      assert(readiness, `${testCase.id}: candidate evidence readiness contract should exist`);
      assert.strictEqual(readiness.canUseForDiagnosticShortlist, true, `${testCase.id}: diagnostic shortlist should be allowed`);
      assert.strictEqual(readiness.canUseForFinalRecommendation, false, `${testCase.id}: final recommendation must stay blocked`);
      assert.strictEqual(readiness.finalRecommendationEnabled, false, `${testCase.id}: final recommendation must stay disabled`);
      assert.strictEqual(readiness.allowedAnswerType || 'diagnostic_only', 'diagnostic_only', `${testCase.id}: answer type must be diagnostic_only`);
      assert(Array.isArray(readiness.blockedReasons), `${testCase.id}: blocked reasons should be listed`);
      assert(readiness.blockedReasons.includes('final_recommendation_policy_not_ready'), `${testCase.id}: final policy blocker should exist`);
      assert(readiness.blockedReasons.includes('recommendation_activation_disabled'), `${testCase.id}: activation disabled blocker should exist`);
      assert(!/final_recommendation_allowed/i.test(blob), `${testCase.id}: final recommendation allowed must not leak`);

      const rowSample = rows.slice(0, Math.min(rows.length, 5));
      for (const [index, row] of rowSample.entries()) {
        assert(row.candidateEvidenceReadiness, `${testCase.id}: row ${index + 1} missing row readiness`);
        assert.strictEqual(row.candidateEvidenceReadiness.canUseForFinalRecommendation, false, `${testCase.id}: row ${index + 1} final use must be false`);
        assert.strictEqual(row.candidateEvidenceReadiness.finalRecommendationEnabled, false, `${testCase.id}: row ${index + 1} final enabled must be false`);
        assert(row.decisionCandidate?.evidenceReadiness, `${testCase.id}: row ${index + 1} decisionCandidate evidenceReadiness missing`);
        assert(Array.isArray(row.evidenceSummary?.missingBuyerInputsForFinalRecommendation), `${testCase.id}: row ${index + 1} missing buyer input map absent`);
      }

      if (testCase.expectsFinalChoice) {
        assert.strictEqual(readiness.requestedFinalRecommendation, true, `${testCase.id}: readiness should know final choice was requested`);
        assert(readiness.missingBuyerInputsForFinalRecommendation.includes('fuelPreferenceOrMonthlyRunning'), `${testCase.id}: fuel/running missing input should be retained`);
        assert(readiness.missingBuyerInputsForFinalRecommendation.includes('safetyPriority'), `${testCase.id}: safety missing input should be retained`);
        assert(readiness.missingBuyerInputsForFinalRecommendation.includes('featurePriority'), `${testCase.id}: feature missing input should be retained`);
      }

      return {
        id: testCase.id,
        status: readiness.status,
        evidenceStatus: readiness.evidenceStatus,
        usableEvidenceCount: readiness.usableEvidenceCount,
        requiredEvidenceCount: readiness.requiredEvidenceCount,
        canUseForDiagnosticShortlist: readiness.canUseForDiagnosticShortlist,
        canUseForFinalRecommendation: readiness.canUseForFinalRecommendation,
        finalRecommendationEnabled: readiness.finalRecommendationEnabled,
        missingBuyerInputsForFinalRecommendation: readiness.missingBuyerInputsForFinalRecommendation,
        blockedReasons: readiness.blockedReasons,
        rowSample: rowSample.slice(0, 2).map((row) => ({
          model: row.fullModel || row.displayName || row.model,
          readinessStatus: row.candidateEvidenceReadiness?.status,
          candidateRankReason: row.candidateRankReason,
        })),
      };
    }));

    console.log(JSON.stringify({
      suite: 'ACI candidate evidence readiness runtime smoke v1',
      ok: true,
      results,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI candidate evidence readiness runtime smoke v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
