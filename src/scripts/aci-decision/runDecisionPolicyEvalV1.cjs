#!/usr/bin/env node

const assert = require('assert');

const {
  ALLOWED_ANSWER_TYPES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  applyDecisionPolicy,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.service.cjs');

const {
  DECISION_POLICY_EVAL_CORPUS_V1,
} = require('./decisionPolicyEvalCorpusV1.cjs');

function assertArrayIncludesAll(actual, expected, label) {
  for (const item of expected || []) {
    assert(
      Array.isArray(actual) && actual.includes(item),
      `${label} expected to include ${item}; got ${JSON.stringify(actual)}`
    );
  }
}

function assertArrayExcludesAll(actual, expected, label) {
  for (const item of expected || []) {
    assert(
      !Array.isArray(actual) || !actual.includes(item),
      `${label} expected to exclude ${item}; got ${JSON.stringify(actual)}`
    );
  }
}

function evaluateCase(testCase) {
  const result = applyDecisionPolicy(testCase.input);
  const policy = result.decisionPolicy;
  const expect = testCase.expect || {};

  if ('canUseForFinalRecommendation' in expect) {
    assert.strictEqual(
      policy.canUseForFinalRecommendation,
      expect.canUseForFinalRecommendation,
      'canUseForFinalRecommendation mismatch'
    );
  }

  if (expect.allowedAnswerType) {
    assert.strictEqual(policy.allowedAnswerType, expect.allowedAnswerType, 'allowedAnswerType mismatch');
  }

  if (expect.forbiddenAllowedAnswerType) {
    assert.notStrictEqual(
      policy.allowedAnswerType,
      expect.forbiddenAllowedAnswerType,
      'forbidden allowedAnswerType was returned'
    );
  }

  if (expect.claimType) {
    assert.strictEqual(policy.claimType, expect.claimType, 'claimType mismatch');
  }

  if ('degradedMode' in expect) {
    assert.strictEqual(policy.degradedMode, expect.degradedMode, 'degradedMode mismatch');
  }

  if (expect.missingMandatoryInputsMin) {
    assert(
      policy.missingMandatoryInputs.length >= expect.missingMandatoryInputsMin,
      `missingMandatoryInputs expected >= ${expect.missingMandatoryInputsMin}; got ${policy.missingMandatoryInputs.length}`
    );
  }

  assertArrayIncludesAll(policy.blockedReasons, expect.blockedReasonsPresent, 'blockedReasons');
  assertArrayExcludesAll(policy.blockedReasons, expect.blockedReasonsAbsent, 'blockedReasons');

  if (
    policy.canUseForFinalRecommendation &&
    policy.allowedAnswerType !== ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED
  ) {
    throw new Error('Final recommendation true without final_recommendation_allowed answer type');
  }

  return {
    id: testCase.id,
    pass: true,
    allowedAnswerType: policy.allowedAnswerType,
    canUseForFinalRecommendation: policy.canUseForFinalRecommendation,
    degradedMode: policy.degradedMode,
    blockedReasons: policy.blockedReasons,
  };
}

const results = [];
const failures = [];

for (const testCase of DECISION_POLICY_EVAL_CORPUS_V1) {
  try {
    results.push(evaluateCase(testCase));
  } catch (error) {
    failures.push({
      id: testCase.id,
      message: error.message,
      stack: error.stack,
    });
  }
}

const summary = {
  suite: 'ACI Decision Policy Eval v1',
  ok: failures.length === 0,
  total: DECISION_POLICY_EVAL_CORPUS_V1.length,
  passed: DECISION_POLICY_EVAL_CORPUS_V1.length - failures.length,
  failed: failures.length,
  failedIds: failures.map((failure) => failure.id),
  failures,
  results,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
