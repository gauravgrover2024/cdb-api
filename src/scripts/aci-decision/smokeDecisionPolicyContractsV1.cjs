#!/usr/bin/env node

const assert = require('assert');

const {
  ALLOWED_ANSWER_TYPES,
  CLAIM_TYPES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  DEGRADED_MODES,
  BLOCKED_REASONS,
  DECISION_MODULES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  normalizeDecisionOutput,
  decisionOutputHasUsefulResult,
} = require('../../services/aciCore/decisionPolicy/aciDecisionOutput.contract.cjs');

const {
  getMissingFinalRecommendationInputs,
  evaluateDecisionPolicy,
  applyDecisionPolicy,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.service.cjs');

const cases = [];

function addCase(id, fn) {
  cases.push({ id, fn });
}

addCase('empty-result-is-not-success', () => {
  const output = normalizeDecisionOutput({
    module: DECISION_MODULES.RECOMMENDATION,
    intent: 'final_buying_advice',
    rows: [],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.MISSING,
      confidence: CONFIDENCE_LEVELS.LOW,
      usableEvidenceCount: 0,
      requiredEvidenceCount: 3,
    },
    trace: {
      matchedRows: 0,
      candidateCount: 0,
    },
    requestedFinalRecommendation: true,
  });

  assert.strictEqual(decisionOutputHasUsefulResult(output), false);

  const policy = evaluateDecisionPolicy({
    ...output,
    requestedFinalRecommendation: true,
  });

  assert.strictEqual(policy.canUseForFinalRecommendation, false);
  assert.strictEqual(policy.allowedAnswerType, ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED);
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.EMPTY_RESULT));
  assert.strictEqual(policy.degradedMode, DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED);
});

addCase('missing-buyer-context-blocks-final-recommendation', () => {
  const policy = evaluateDecisionPolicy({
    module: DECISION_MODULES.SCORE_INSIGHT,
    intent: 'diagnostic_score',
    requestedFinalRecommendation: true,
    buyerContext: {
      city: 'new-delhi',
      budget: 2000000,
    },
    rows: [{ variantProfileKey: 'sample' }],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.COMPLETE,
      confidence: CONFIDENCE_LEVELS.HIGH,
      usableEvidenceCount: 3,
      requiredEvidenceCount: 3,
    },
    provenance: {
      sourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
      needsRebuild: false,
    },
    trace: {
      matchedRows: 1,
      candidateCount: 1,
    },
  });

  assert.strictEqual(policy.canUseForFinalRecommendation, false);
  assert(policy.missingMandatoryInputs.length > 0);
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE));
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY));
});

addCase('diagnostic-output-can-be-allowed-without-final-verdict', () => {
  const policy = evaluateDecisionPolicy({
    module: DECISION_MODULES.SCORE_INSIGHT,
    intent: 'variant_score_diagnostic',
    requestedFinalRecommendation: false,
    buyerContext: {},
    rows: [{ variantProfileKey: 'sample' }],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.PARTIAL,
      confidence: CONFIDENCE_LEVELS.MEDIUM,
      usableEvidenceCount: 2,
      requiredEvidenceCount: 3,
    },
    provenance: {
      sourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
      needsRebuild: false,
    },
    trace: {
      matchedRows: 1,
      candidateCount: 1,
    },
  });

  assert.strictEqual(policy.canUseForFinalRecommendation, false);
  assert.notStrictEqual(policy.allowedAnswerType, ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED);
});

addCase('complete-evidence-and-context-can-allow-final-recommendation', () => {
  const buyerContext = {
    city: 'new-delhi',
    budget: 2000000,
    primaryUseCase: 'family city use',
    familySize: 4,
    fuelPreference: 'petrol',
    transmissionPreference: 'automatic',
    safetyPriority: 'high',
    featurePriority: ['sunroof', 'six_airbags'],
    shortlistedModels: ['model_a', 'model_b'],
  };

  const policy = evaluateDecisionPolicy({
    module: DECISION_MODULES.RECOMMENDATION,
    intent: 'final_buying_advice',
    requestedFinalRecommendation: true,
    buyerContext,
    rows: [{ modelKey: 'creta' }, { modelKey: 'seltos' }],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.COMPLETE,
      confidence: CONFIDENCE_LEVELS.HIGH,
      usableEvidenceCount: 8,
      requiredEvidenceCount: 8,
    },
    provenance: {
      sourceClass: SOURCE_CLASSES.MIXED,
      needsRebuild: false,
    },
    trace: {
      matchedRows: 2,
      candidateCount: 2,
    },
  });

  assert.strictEqual(policy.canUseForFinalRecommendation, true);
  assert.strictEqual(policy.allowedAnswerType, ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED);
  assert.strictEqual(policy.claimType, CLAIM_TYPES.OPINION);
});

addCase('stale-artifact-blocks-final-recommendation', () => {
  const buyerContext = {
    city: 'new-delhi',
    budget: 2000000,
    primaryUseCase: 'family',
    familySize: 4,
    fuelPreference: 'petrol',
    transmissionPreference: 'automatic',
    safetyPriority: 'high',
    featurePriority: ['safety'],
    shortlistedModels: ['model_a', 'model_b'],
  };

  const policy = evaluateDecisionPolicy({
    module: DECISION_MODULES.SIMILAR_CARS,
    requestedFinalRecommendation: true,
    buyerContext,
    rows: [{ modelKey: 'a' }],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.STALE,
      confidence: CONFIDENCE_LEVELS.HIGH,
      usableEvidenceCount: 3,
      requiredEvidenceCount: 3,
    },
    provenance: {
      sourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
      needsRebuild: true,
    },
    trace: {
      matchedRows: 1,
      candidateCount: 1,
    },
  });

  assert.strictEqual(policy.canUseForFinalRecommendation, false);
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.STALE_ARTIFACT));
  assert.strictEqual(policy.degradedMode, DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD);
});

addCase('sponsored-influence-is-blocked', () => {
  const policy = evaluateDecisionPolicy({
    module: DECISION_MODULES.RECOMMENDATION,
    requestedFinalRecommendation: true,
    sponsoredInfluenceDetected: true,
    buyerContext: {
      city: 'new-delhi',
      budget: 2000000,
      primaryUseCase: 'family',
      familySize: 4,
      fuelPreference: 'petrol',
      transmissionPreference: 'automatic',
      safetyPriority: 'high',
      featurePriority: ['safety'],
      shortlistedModels: ['model_a', 'model_b'],
    },
    rows: [{ modelKey: 'sponsored-car' }],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.COMPLETE,
      confidence: CONFIDENCE_LEVELS.HIGH,
      usableEvidenceCount: 3,
      requiredEvidenceCount: 3,
    },
    trace: {
      matchedRows: 1,
      candidateCount: 1,
    },
  });

  assert.strictEqual(policy.canUseForFinalRecommendation, false);
  assert.strictEqual(policy.allowedAnswerType, ALLOWED_ANSWER_TYPES.BLOCKED);
  assert(policy.blockedReasons.includes(BLOCKED_REASONS.SPONSORED_INFLUENCE_NOT_ALLOWED));
});

addCase('apply-policy-normalizes-output-envelope', () => {
  const result = applyDecisionPolicy({
    module: DECISION_MODULES.SCORE_INSIGHT,
    intent: 'score_diagnostic',
    rows: [{ variantProfileKey: 'sample' }],
    evidence: {
      evidenceStatus: EVIDENCE_STATUS.PARTIAL,
      confidence: CONFIDENCE_LEVELS.MEDIUM,
      usableEvidenceCount: 1,
      requiredEvidenceCount: 2,
    },
    trace: {
      matchedRows: 1,
      candidateCount: 1,
    },
  });

  assert.strictEqual(result.schemaVersion.startsWith('aci_decision_output_v1'), true);
  assert(result.decisionPolicy);
  assert.strictEqual(result.decisionPolicy.canUseForFinalRecommendation, false);
});

addCase('missing-input-helper-is-strict', () => {
  const missing = getMissingFinalRecommendationInputs({
    city: 'new-delhi',
    budget: 2000000,
  });

  assert(missing.includes('bodyPreferenceOrPrimaryUseCase'));
  assert(missing.includes('safetyPriority'));
  assert(!missing.includes('city'));
  assert(!missing.includes('budgetOrPriceCeiling'));
});

let passed = 0;
const failures = [];

for (const testCase of cases) {
  try {
    testCase.fn();
    passed += 1;
  } catch (error) {
    failures.push({
      id: testCase.id,
      message: error.message,
      stack: error.stack,
    });
  }
}

const summary = {
  suite: 'ACI Decision Policy Contracts v1 smoke',
  ok: failures.length === 0,
  total: cases.length,
  passed,
  failed: failures.length,
  failedIds: failures.map((entry) => entry.id),
  failures,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
