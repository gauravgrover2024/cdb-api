#!/usr/bin/env node

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
];

const unsafeFinalVerdictPatterns = [
  /\byou should buy\b/i,
  /\bmy final recommendation is\b/i,
  /\bfinal recommendation is\b/i,
  /\bgo ahead and buy\b/i,
  /\bbest final choice\b/i,
];

const fail = (message, extra = {}) => {
  const error = new Error(message);
  error.extra = extra;
  throw error;
};

const asString = (value) => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
};

const getVisiblePayload = (response) => {
  const answer = asString(response.answer || response.data?.answer || response.message || response.text);
  const title = asString(response.title || response.data?.title || response.heading);
  const buyerFacingQuestion = asString(
    response.buyerFacingQuestion ||
      response.nextBestQuestion ||
      response.data?.buyerFacingQuestion ||
      response.data?.nextBestQuestion ||
      response.meta?.buyerFacingQuestion ||
      response.meta?.nextBestQuestion
  );

  return {
    title,
    answer,
    buyerFacingQuestion,
    joined: [title, answer, buyerFacingQuestion].filter(Boolean).join('\n'),
  };
};

const countQuestionMarks = (text) => (text.match(/\?/g) || []).length;

(async () => {
  let mongoose;

  try {
    const mongooseModule = await import('mongoose');
    mongoose = mongooseModule.default || mongooseModule;

    const connectDbModule = await import('../../config/db.js');
    const connectDB = connectDbModule.default || connectDbModule;

    const agentModule = await import('../../services/aiAgent/aiAgent.service.js');
    const { chatWithAgent } = agentModule;

    await connectDB();

    const cases = [
      {
        id: 'family-auto-suv-diagnostic-visible',
        message: 'Recommend automatic SUV for family city use under 18 lakh in Delhi',
        expectsDiagnosticShortlist: true,
        expectsFinalBlocked: false,
      },
      {
        id: 'safety-family-suv-diagnostic-visible',
        message: 'Suggest safest SUVs under 20 lakh for family in Delhi',
        expectsDiagnosticShortlist: true,
        expectsFinalBlocked: false,
      },
      {
        id: 'city-auto-budget-diagnostic-visible',
        message: 'Show me automatic cars under 15 lakh for city use in Delhi',
        expectsDiagnosticShortlist: true,
        expectsFinalBlocked: false,
      },
      {
        id: 'final-choice-buy-verdict-blocked-visible',
        message: 'I drive mostly in city, family of 4, budget 18 lakh, automatic preferred. Which car should I buy?',
        expectsDiagnosticShortlist: true,
        expectsFinalBlocked: true,
      },
    ];

    const results = await Promise.all(cases.map(async (testCase) => {
      const response = await chatWithAgent({ message: testCase.message, context: {} });
      const visible = getVisiblePayload(response);

      assert(visible.answer, `${testCase.id}: visible answer must exist`);

      for (const pattern of FORBIDDEN_VISIBLE_PATTERNS) {
        assert(!pattern.test(visible.joined), `${testCase.id}: internal term leaked to buyer-facing text: ${pattern}`);
      }

      assert(
        countQuestionMarks(visible.answer) <= 1,
        `${testCase.id}: buyer-facing answer should not ask multiple questions`
      );

      if (testCase.expectsDiagnosticShortlist) {
        assert(
          /diagnostic shortlist/i.test(visible.answer),
          `${testCase.id}: buyer-facing answer should clearly frame output as diagnostic shortlist`
        );
      }

      if (testCase.expectsFinalBlocked) {
        assert(
          /not a final buy verdict yet|not a final purchase call|before finalising/i.test(visible.answer),
          `${testCase.id}: final-choice request must clearly remain blocked from final verdict`
        );

        for (const pattern of unsafeFinalVerdictPatterns) {
          assert(!pattern.test(visible.answer), `${testCase.id}: unsafe final verdict language leaked: ${pattern}`);
        }
      }

      if (response.data?.answer) {
        assert.strictEqual(
          asString(response.data.answer),
          visible.answer,
          `${testCase.id}: root/data buyer-facing answer should remain sealed consistently`
        );
      }

      if (response.data?.title && visible.title) {
        assert.strictEqual(
          asString(response.data.title),
          visible.title,
          `${testCase.id}: root/data buyer-facing title should remain sealed consistently`
        );
      }

      return {
        id: testCase.id,
        ok: true,
        title: visible.title,
        answerPreview: visible.answer.slice(0, 260),
        buyerFacingQuestion: visible.buyerFacingQuestion,
        questionMarkCount: countQuestionMarks(visible.answer),
      };
    }));

    console.log(JSON.stringify({
      suite: 'ACI buyer-facing diagnostic leak audit runtime smoke v1',
      ok: true,
      caseCount: cases.length,
      forbiddenVisiblePatternCount: FORBIDDEN_VISIBLE_PATTERNS.length,
      results,
    }, null, 2));
  } catch (error) {
    console.error(JSON.stringify({
      suite: 'ACI buyer-facing diagnostic leak audit runtime smoke v1',
      ok: false,
      error: error.message,
      extra: error.extra || null,
    }, null, 2));
    process.exitCode = 1;
  } finally {
    if (mongoose?.disconnect) {
      await mongoose.disconnect();
    }
  }
})();
