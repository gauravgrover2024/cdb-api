#!/usr/bin/env node
'use strict';

require('dotenv').config();

const assert = require('assert');

const FORBIDDEN_VISIBLE_PATTERNS = [
  /\ballowedAnswerType\b/i,
  /\bcandidateEvidenceReadiness\b/i,
  /\bdiagnostic_only\b/i,
  /\bfinalRecommendationEligibility\b/i,
  /\bmissingInputs\b/i,
  /\binternalMissingInputMap\b/i,
  /\bbuyerInputClarification\b/i,
  /\brenderingContract\b/i,
  /\bpolicyTrace\b/i,
  /\bsource_provenance\b/i,
  /\bsource provenance\b/i,
  /\bcandidateSourceProvenance\b/i,
  /\bdb_current_usable\b/i,
  /\bidentity_unverified\b/i,
  /\bderived_only\b/i,
  /\brecommendation_activation_disabled\b/i,
  /\bfinal_composer_not_ready\b/i,
  /\bfinal_recommendation_policy_not_ready\b/i,
  /\bbuyer_context_incomplete\b/i,
  /\bcanUseForFinalRecommendation\b/i,
  /\bfinalRecommendationEnabled\b/i,
  /\bblockedReasons\b/i,
  /\bexternal_current_market_validation_required\b/i,
  /\bcurrentMarketValidationStatus\b/i,
];

const UNSAFE_FINAL_VERDICT_PATTERNS = [
  /\byou should buy\b/i,
  /\byou should go for\b/i,
  /\bmy final recommendation is\b/i,
  /\bfinal recommendation is\b/i,
  /\bgo ahead and buy\b/i,
  /\bbest final choice\b/i,
  /\bclear winner is\b/i,
  /\bthe winner is\b/i,
  /\bbuy this\b/i,
];

const REQUIRED_SAFE_FINAL_INTENT_PATTERNS = [
  /not a final buy verdict yet/i,
  /not a final purchase verdict/i,
  /not a final purchase recommendation/i,
  /diagnostic shortlist/i,
  /diagnostic view/i,
  /practical, diagnostic/i,
  /provisional discovery guidance/i,
  /provisional guidance/i,
  /practical guidance/i,
  /not a final yes\/no yet/i,
  /not a single winner yet/i,
  /trade-off check/i,
  /trade-off comparison/i,
  /buy-now verdict/i,
];

const getText = (value) => String(value || '').trim();

const getVisiblePayload = (response = {}) => {
  const title = getText(response.title || response.data?.title);
  const answer = getText(response.answer || response.data?.answer);
  const buyerFacingQuestion = getText(
    response.buyerFacingQuestion ||
      response.nextBestQuestion ||
      response.data?.buyerFacingQuestion ||
      response.data?.nextBestQuestion
  );

  return {
    title,
    answer,
    buyerFacingQuestion,
    visibleText: [title, answer, buyerFacingQuestion].filter(Boolean).join('\n'),
  };
};

const getEligibility = (response = {}) =>
  response.finalRecommendationEligibility ||
  response.data?.finalRecommendationEligibility ||
  response.meta?.finalRecommendationEligibility ||
  null;

const getFinalBlockedUx = (response = {}) =>
  response.finalBlockedUx ||
  response.data?.finalBlockedUx ||
  response.meta?.finalBlockedUx ||
  null;

const countBuyerPromptQuestions = (text = '') => {
  const raw = String(text || '');
  const withoutRestatedUserQuestion = raw.replace(/^For\s+[^\n?]{1,180}\?\s*/i, 'For ');
  return (withoutRestatedUserQuestion.match(/\?/g) || []).length;
};

const CASES = [
  {
    id: 'decide-family-budget-delhi',
    message: 'Best car under 15 lakh for family in Delhi, decide for me',
  },
  {
    id: 'which-car-should-i-buy-family-auto',
    message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
  },
  {
    id: 'final-pick-safest-suv',
    message: 'Pick the best and safest automatic SUV under 20 lakh in Delhi. I want your final recommendation.',
  },
  {
    id: 'buy-now-between-two-cars',
    message: 'Should I buy Baleno or Altroz? Give me the final answer.',
  },
];

(async () => {
  try {
    const { chatWithAgent } = await import('../../services/aiAgent/aiAgent.service.js');

    const results = await Promise.all(CASES.map(async (testCase) => {
      const response = await chatWithAgent({
        message: testCase.message,
        userMessage: testCase.message,
        context: {},
        meta: {
          smokeId: `final-intent-runtime-snapshot-${testCase.id}`,
        },
      });

      const visible = getVisiblePayload(response);
      const eligibility = getEligibility(response);
      const finalBlockedUx = getFinalBlockedUx(response);

      assert(visible.answer, `${testCase.id}: answer missing`);

      for (const pattern of FORBIDDEN_VISIBLE_PATTERNS) {
        assert(
          !pattern.test(visible.visibleText),
          `${testCase.id}: visible output leaked forbidden internal pattern ${pattern}: ${visible.visibleText}`
        );
      }

      for (const pattern of UNSAFE_FINAL_VERDICT_PATTERNS) {
        assert(
          !pattern.test(visible.visibleText),
          `${testCase.id}: unsafe final verdict wording leaked ${pattern}: ${visible.visibleText}`
        );
      }

      assert(
        REQUIRED_SAFE_FINAL_INTENT_PATTERNS.some((pattern) => pattern.test(visible.visibleText)),
        `${testCase.id}: final-intent answer did not clearly stay diagnostic/provisional: ${visible.visibleText}`
      );

      assert(
        countBuyerPromptQuestions(visible.answer) <= 1,
        `${testCase.id}: answer has more than one question: ${visible.answer}`
      );

      if (eligibility) {
        assert.strictEqual(
          eligibility.finalRecommendationEnabled,
          false,
          `${testCase.id}: finalRecommendationEnabled became true`
        );
        assert.strictEqual(
          eligibility.canUseForFinalRecommendation,
          false,
          `${testCase.id}: canUseForFinalRecommendation became true`
        );
      }

      if (finalBlockedUx) {
        assert.notStrictEqual(
          finalBlockedUx.finalRecommendationEnabled,
          true,
          `${testCase.id}: finalBlockedUx finalRecommendationEnabled became true`
        );
        assert.notStrictEqual(
          finalBlockedUx.canUseForFinalRecommendation,
          true,
          `${testCase.id}: finalBlockedUx canUseForFinalRecommendation became true`
        );
      }

      if (response.data?.answer) {
        assert.strictEqual(
          response.data.answer,
          visible.answer,
          `${testCase.id}: root answer and data.answer diverged`
        );
      }

      return {
        id: testCase.id,
        ok: true,
        requestedFinalRecommendation: eligibility?.requestedFinalRecommendation === true,
        finalRecommendationEnabled: eligibility?.finalRecommendationEnabled === true,
        canUseForFinalRecommendation: eligibility?.canUseForFinalRecommendation === true,
        hasFinalBlockedUx: Boolean(finalBlockedUx),
        title: visible.title,
        answerPreview: visible.answer.slice(0, 280),
        buyerFacingQuestion: visible.buyerFacingQuestion,
        questionMarkCount: countBuyerPromptQuestions(visible.answer),
      };
    }));

    console.log(JSON.stringify({
      suite: 'ACI Final-intent Runtime Snapshot Smoke v1',
      ok: true,
      caseCount: CASES.length,
      forbiddenVisiblePatternCount: FORBIDDEN_VISIBLE_PATTERNS.length,
      unsafeFinalVerdictPatternCount: UNSAFE_FINAL_VERDICT_PATTERNS.length,
      results,
    }, null, 2));
  } catch (error) {
    console.error(JSON.stringify({
      suite: 'ACI Final-intent Runtime Snapshot Smoke v1',
      ok: false,
      error: error?.message || String(error),
      stack: error?.stack || null,
    }, null, 2));
    process.exit(1);
  }
})();
