#!/usr/bin/env node

const assert = require('assert');

const {
  DECISION_MODULES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  ALLOWED_ANSWER_TYPES,
  CLAIM_TYPES,
  BLOCKED_REASONS,
  DEGRADED_MODES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  getDecisionModulePolicyProfile,
  applyDecisionPolicyWithModuleProfile,
} = require('../../services/aciCore/decisionPolicy/aciDecisionModulePolicyProfiles.service.cjs');

const completeBuyerContext = Object.freeze({
  city: 'city_supported',
  budget: 2000000,
  primaryUseCase: 'primary_use_case_generic',
  familySize: 4,
  fuelPreference: 'fuel_preference_generic',
  transmissionPreference: 'transmission_preference_generic',
  safetyPriority: 'high',
  featurePriority: ['feature_a'],
  shortlistedModels: ['model_a', 'model_b'],
});

const completeEvidence = Object.freeze({
  evidenceStatus: EVIDENCE_STATUS.COMPLETE,
  confidence: CONFIDENCE_LEVELS.HIGH,
  usableEvidenceCount: 5,
  requiredEvidenceCount: 5,
});

const freshProvenance = Object.freeze({
  buildVersion: 'synthetic_build_v1',
  builtAt: '2026-06-04T00:00:00.000Z',
  sourceClass: SOURCE_CLASSES.MIXED,
  stalenessDays: 0,
  needsRebuild: false,
});

const usefulTrace = Object.freeze({
  toolRoute: 'synthetic_module_policy_eval',
  collectionsUsed: ['synthetic_read_model'],
  matchedRows: 2,
  candidateCount: 2,
  warnings: [],
});

function makeInput(moduleName, overrides = {}) {
  return {
    module: moduleName,
    intent: 'synthetic_module_policy_eval',
    requestedFinalRecommendation: true,
    buyerContext: completeBuyerContext,
    rows: [{ entityKey: 'entity_a' }, { entityKey: 'entity_b' }],
    evidence: completeEvidence,
    provenance: freshProvenance,
    trace: usefulTrace,
    ...overrides,
  };
}

const cases = [
  {
    id: 'score-insight-cannot-final-recommend',
    input: makeInput(DECISION_MODULES.SCORE_INSIGHT),
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      blockedReasonsPresent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED,
    },
  },
  {
    id: 'similar-cars-cannot-final-recommend',
    input: makeInput(DECISION_MODULES.SIMILAR_CARS),
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      blockedReasonsPresent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED,
    },
  },
  {
    id: 'upgrade-ladder-cannot-final-recommend',
    input: makeInput(DECISION_MODULES.UPGRADE_LADDER),
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      blockedReasonsPresent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED,
    },
  },
  {
    id: 'comparison-cannot-final-recommend-for-now',
    input: makeInput(DECISION_MODULES.COMPARISON),
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      blockedReasonsPresent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED,
    },
  },
  {
    id: 'recommendation-can-final-recommend-when-base-policy-allows',
    input: makeInput(DECISION_MODULES.RECOMMENDATION),
    expect: {
      canUseForFinalRecommendation: true,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED,
      claimType: CLAIM_TYPES.OPINION,
      blockedReasonsAbsent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: null,
    },
  },
  {
    id: 'unknown-module-cannot-final-recommend',
    input: makeInput('unknown_module'),
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      blockedReasonsPresent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED,
    },
  },
  {
    id: 'diagnostic-score-insight-without-final-request-stays-diagnostic',
    input: makeInput(DECISION_MODULES.SCORE_INSIGHT, {
      requestedFinalRecommendation: false,
      buyerContext: {},
      evidence: {
        evidenceStatus: EVIDENCE_STATUS.PARTIAL,
        confidence: CONFIDENCE_LEVELS.MEDIUM,
        usableEvidenceCount: 2,
        requiredEvidenceCount: 4,
      },
    }),
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      blockedReasonsAbsent: [BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE],
      degradedMode: null,
    },
  },
];

function assertArrayIncludesAll(actual, expected, label) {
  for (const item of expected || []) {
    assert(Array.isArray(actual) && actual.includes(item), `${label} missing ${item}`);
  }
}

function assertArrayExcludesAll(actual, expected, label) {
  for (const item of expected || []) {
    assert(!Array.isArray(actual) || !actual.includes(item), `${label} unexpectedly included ${item}`);
  }
}

const results = [];
const failures = [];

for (const testCase of cases) {
  try {
    const result = applyDecisionPolicyWithModuleProfile(testCase.input);
    const policy = result.decisionPolicy;
    const expect = testCase.expect;

    assert.strictEqual(policy.canUseForFinalRecommendation, expect.canUseForFinalRecommendation, 'canUseForFinalRecommendation mismatch');
    assert.strictEqual(policy.allowedAnswerType, expect.allowedAnswerType, 'allowedAnswerType mismatch');
    assert.strictEqual(policy.claimType, expect.claimType, 'claimType mismatch');
    assert.strictEqual(policy.degradedMode, expect.degradedMode, 'degradedMode mismatch');
    assertArrayIncludesAll(policy.blockedReasons, expect.blockedReasonsPresent, 'blockedReasons');
    assertArrayExcludesAll(policy.blockedReasons, expect.blockedReasonsAbsent, 'blockedReasons');

    results.push({
      id: testCase.id,
      pass: true,
      module: testCase.input.module,
      profile: getDecisionModulePolicyProfile(testCase.input.module),
      allowedAnswerType: policy.allowedAnswerType,
      canUseForFinalRecommendation: policy.canUseForFinalRecommendation,
      degradedMode: policy.degradedMode,
      blockedReasons: policy.blockedReasons,
    });
  } catch (error) {
    failures.push({
      id: testCase.id,
      message: error.message,
      stack: error.stack,
    });
  }
}

const summary = {
  suite: 'ACI Decision Module Policy Profiles Eval v1',
  ok: failures.length === 0,
  total: cases.length,
  passed: cases.length - failures.length,
  failed: failures.length,
  failedIds: failures.map((failure) => failure.id),
  failures,
  results,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
