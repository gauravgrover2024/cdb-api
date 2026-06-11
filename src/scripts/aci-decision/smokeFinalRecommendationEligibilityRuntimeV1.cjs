#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const {
  ALLOWED_ANSWER_TYPES,
  BLOCKED_REASONS,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const SAFE_ALLOWED_TYPES = new Set([
  ALLOWED_ANSWER_TYPES.CLARIFICATION_REQUIRED,
  ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
  ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED,
  ALLOWED_ANSWER_TYPES.BLOCKED,
]);

const CASES = [
  {
    id: 'final-score-baleno',
    message: 'Should I buy Baleno?',
    expectRequested: true,
  },
  {
    id: 'final-cross-model-choice',
    message: 'Which one should I finally choose: Tiago or Altroz CNG automatic?',
    expectRequested: true,
  },
  {
    id: 'final-decide-family-budget',
    message: 'Best car under 15 lakh for family, decide for me',
    expectRequested: true,
  },
  {
    id: 'diagnostic-score-not-final',
    message: 'How good is Baleno overall?',
    expectRequested: false,
  },
];

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const { runAciCoreLiveBridge } = await import('../../services/aciCore/integration/aciCoreLiveBridge.service.js');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const results = [];

  for (const testCase of CASES) {
    const response = await runAciCoreLiveBridge({
      message: testCase.message,
      context: {},
      meta: { smokeId: `final-recommendation-eligibility-${testCase.id}` },
    });

    const eligibility =
      response.finalRecommendationEligibility ||
      response.data?.finalRecommendationEligibility ||
      response.meta?.finalRecommendationEligibility;

    if (testCase.expectRequested) {
      assert(eligibility, `${testCase.id}: finalRecommendationEligibility missing`);
      assert.strictEqual(
        eligibility.requestedFinalRecommendation,
        true,
        `${testCase.id}: requestedFinalRecommendation mismatch`
      );

      assert.strictEqual(eligibility.canUseForFinalRecommendation, false, `${testCase.id}: final recommendation unexpectedly enabled`);
      assert.strictEqual(eligibility.finalRecommendationEnabled, false, `${testCase.id}: finalRecommendationEnabled unexpectedly true`);
      assert.strictEqual(eligibility.dryRun, true, `${testCase.id}: dryRun flag missing`);
      assert.notStrictEqual(
        eligibility.allowedAnswerType,
        ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED,
        `${testCase.id}: final recommendation answer type leaked`
      );
      assert(SAFE_ALLOWED_TYPES.has(eligibility.allowedAnswerType), `${testCase.id}: unsafe allowedAnswerType ${eligibility.allowedAnswerType}`);

      assert(
        eligibility.blockedReasons.includes(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY),
        `${testCase.id}: policy-not-ready block missing`
      );
      assert(
        eligibility.blockedReasons.includes(BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE) ||
        eligibility.blockedReasons.includes(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE) ||
        eligibility.blockedReasons.includes(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET),
        `${testCase.id}: specific runtime block reason missing`
      );

      if ((response.intent === 'clarification' || response.tool === 'clarification') && eligibility.requestedFinalRecommendation === true) {
        const answerText = String(response.answer || '');
        assert(!/What would you like to check about the car\?/i.test(answerText), `${testCase.id}: weak generic final-intent clarification leaked`);
        assert(/final|recommend|buyer context|missing|city|budget|compare|diagnostic/i.test(answerText), `${testCase.id}: intelligent final-blocked wording missing`);
      }
    } else {
      assert(!eligibility, `${testCase.id}: finalRecommendationEligibility should not attach for non-final diagnostic request`);
    }

    const blob = JSON.stringify(response);
    assert(!/"canUseForFinalRecommendation"\s*:\s*true/.test(blob), `${testCase.id}: canUseForFinalRecommendation true leaked`);
    assert(!/"finalRecommendationEnabled"\s*:\s*true/.test(blob), `${testCase.id}: finalRecommendationEnabled true leaked`);
    assert(!/"allowedAnswerType"\s*:\s*"final_recommendation_allowed"/.test(blob), `${testCase.id}: final recommendation allowed leaked`);

    if (eligibility?.requestedFinalRecommendation === true) {
      const answerText = String(
        response.answer ||
        response.clarification ||
        response.data?.answer ||
        response.data?.clarification ||
        ''
      );

      assert(
        /final recommendation remains disabled|cannot make a final recommendation yet|cannot give a buy-this verdict yet|cannot turn them into a final recommendation yet|not a final recommendation/i.test(answerText),
        `${testCase.id}: final-blocked readiness wording missing: ${answerText.slice(0, 260)}`
      );

      assert(
        /Safe now|Missing buyer|Missing:|buyer context|diagnostic|discovery|compare/i.test(answerText),
        `${testCase.id}: final-blocked answer missing safe next-step or missing-context wording: ${answerText.slice(0, 260)}`
      );

      assert(
        !/\byou should buy\b|\bbest final choice\b|\bmy final recommendation\b/i.test(answerText),
        `${testCase.id}: unsafe final recommendation wording leaked: ${answerText.slice(0, 260)}`
      );

      assert(
        response.finalBlockedUx ||
          response.data?.finalBlockedUx ||
          response.meta?.finalBlockedUx,
        `${testCase.id}: finalBlockedUx readiness object missing`
      );
    }


    results.push({
      id: testCase.id,
      message: testCase.message,
      requestedFinalRecommendation: eligibility?.requestedFinalRecommendation ?? false,
      canUseForFinalRecommendation: eligibility?.canUseForFinalRecommendation ?? false,
      allowedAnswerType: eligibility?.allowedAnswerType ?? null,
      blockedReasons: eligibility?.blockedReasons ?? [],
      missingMandatoryInputs: eligibility?.missingMandatoryInputs ?? [],
      module: eligibility?.module ?? null,
      evaluatedTool: eligibility?.evaluatedTool ?? null,
      answerPreview: String(response.answer || '').slice(0, 240),
    });
  }

  console.log(JSON.stringify({
    suite: 'ACI Final Recommendation Eligibility Runtime Smoke v1',
    ok: true,
    total: results.length,
    passed: results.length,
    failed: 0,
    failedIds: [],
    results,
  }, null, 2));

  await mongoose.disconnect();
})().catch((error) => {
  console.error(error);
  mongoose.disconnect().catch(() => {}).finally(() => process.exit(1));
});
