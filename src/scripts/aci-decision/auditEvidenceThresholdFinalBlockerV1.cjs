#!/usr/bin/env node
'use strict';

const assert = require('assert');

const {
  ALLOWED_ANSWER_TYPES,
  BLOCKED_REASONS,
  EVIDENCE_STATUS,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');
const {
  buildFinalRecommendationPolicyReadiness,
} = require('../../services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs');
const {
  buildFinalRecommendationEligibilityRuntime,
} = require('../../services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs');

const FULL_BUYER_CONTEXT = Object.freeze({
  city: 'new-delhi',
  budgetOrPriceCeiling: 1500000,
  bodyPreferenceOrPrimaryUseCase: 'family city use',
  familySizeOrOccupancyUse: 'family of 4',
  fuelPreferenceOrMonthlyRunning: 'petrol, 800 km monthly',
  transmissionPreference: 'automatic',
  safetyPriority: 'high',
  featurePriority: ['6 airbags', 'rear camera'],
  shortlistedModelsOrDiscoveryScope: ['Tata Punch', 'Maruti Baleno'],
});

const assertNoFinalActivation = (value, id) => {
  const blob = JSON.stringify(value || {});
  assert(!/"canUseForFinalRecommendation"\s*:\s*true/.test(blob), `${id}: canUseForFinalRecommendation true leaked`);
  assert(!/"finalRecommendationEnabled"\s*:\s*true/.test(blob), `${id}: finalRecommendationEnabled true leaked`);
  assert(!/"finalRecommendationActivationReady"\s*:\s*true/.test(blob), `${id}: finalRecommendationActivationReady true leaked`);
  assert(!/"activationAllowed"\s*:\s*true/.test(blob), `${id}: activationAllowed true leaked`);
};

(async () => {
  const candidateEvidenceModule = await import('../../services/aciCore/candidates/aciCandidateEvidenceReadiness.service.js');
  const buildCandidateEvidenceReadinessContract =
    candidateEvidenceModule.buildCandidateEvidenceReadinessContract ||
    candidateEvidenceModule.default;

  const checks = [];

  const add = (id, fn) => {
    try {
      const detail = fn();
      checks.push({ id, ok: true, detail });
    } catch (error) {
      checks.push({ id, ok: false, error: error.message });
    }
  };

  add('final-readiness-missing-evidence-threshold-blocks', () => {
    const readiness = buildFinalRecommendationPolicyReadiness({
      requestedFinalRecommendation: true,
      buyerDecisionInput: {
        missingMandatoryInputs: [],
        buyerGuidanceContext: { guidanceMode: 'sharpened_recommendation' },
      },
      buyerInputClarification: {
        missingInputs: [],
        askPolicy: {
          mode: 'progressive_single_question',
          maxBuyerFacingQuestions: 1,
          learnFromSearchAndContext: true,
        },
        buyerFacingRenderingContract: {
          maxVisibleQuestions: 1,
          doNotRenderToBuyer: ['internalMissingInputMap'],
        },
      },
      evidenceGate: {
        evidenceStatus: EVIDENCE_STATUS.MISSING,
        hasUsefulEvidence: false,
      },
    });

    assert.strictEqual(readiness.canActivateFinalRecommendation, false);
    assert.strictEqual(readiness.gates.evidenceThresholdMet, false);
    assert(readiness.blockedReasons.includes(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET));
    assert(readiness.blockedReasons.includes(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY));
    assertNoFinalActivation(readiness, 'final-readiness-missing-evidence-threshold-blocks');

    return {
      evidenceThresholdMet: readiness.gates.evidenceThresholdMet,
      blockedReasons: readiness.blockedReasons,
    };
  });

  add('final-eligibility-missing-evidence-threshold-blocks', () => {
    const message = 'Which car should I buy?';
    const eligibility = buildFinalRecommendationEligibilityRuntime({
      message,
      bridge: {
        tool: 'vehicle_recommend',
        originalMessage: message,
        effectiveMessage: message,
      },
      context: {
        buyerContext: FULL_BUYER_CONTEXT,
      },
      response: {
        tool: 'vehicle_recommend',
        primaryTask: 'vehicle_recommendation',
        rows: [],
        evidence: {
          evidenceStatus: EVIDENCE_STATUS.MISSING,
          usableEvidenceCount: 0,
        },
      },
    });

    assert.strictEqual(eligibility.requestedFinalRecommendation, true);
    assert.strictEqual(eligibility.canUseForFinalRecommendation, false);
    assert.strictEqual(eligibility.finalRecommendationEnabled, false);
    assert(eligibility.blockedReasons.includes(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET));
    assert(eligibility.blockedReasons.includes(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY));
    assertNoFinalActivation(eligibility, 'final-eligibility-missing-evidence-threshold-blocks');

    return {
      allowedAnswerType: eligibility.allowedAnswerType,
      blockedReasons: eligibility.blockedReasons,
      evidenceStatus: eligibility.evidenceStatus,
    };
  });

  add('candidate-evidence-empty-rows-explicit-threshold-block', () => {
    const contract = buildCandidateEvidenceReadinessContract({
      rows: [],
      buyerContext: FULL_BUYER_CONTEXT,
      bridge: {
        originalMessage: 'Which car should I buy?',
        effectiveMessage: 'Which car should I buy?',
      },
      response: {
        originalMessage: 'Which car should I buy?',
      },
    });

    assert.strictEqual(contract.canUseForDiagnosticShortlist, false);
    assert.strictEqual(contract.canUseForFinalRecommendation, false);
    assert.strictEqual(contract.finalRecommendationEnabled, false);
    assert.strictEqual(contract.allowedAnswerType, ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY);
    assert.strictEqual(contract.evidenceStatus, EVIDENCE_STATUS.MISSING);
    assert(contract.blockedReasons.includes(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET));
    assert(contract.blockedReasons.includes(BLOCKED_REASONS.USEFUL_EVIDENCE_MISSING));
    assertNoFinalActivation(contract, 'candidate-evidence-empty-rows-explicit-threshold-block');

    return {
      status: contract.status,
      evidenceStatus: contract.evidenceStatus,
      blockedReasons: contract.blockedReasons,
    };
  });

  add('candidate-evidence-useful-diagnostic-only-final-blocked', () => {
    const contract = buildCandidateEvidenceReadinessContract({
      rows: [
        {
          fullModel: 'Tata Punch',
          decisionCandidate: { evidenceStatus: EVIDENCE_STATUS.PARTIAL },
          scoreSignals: { status: 'available' },
          featureSignals: { status: 'available' },
        },
      ],
      buyerContext: FULL_BUYER_CONTEXT,
      bridge: {
        originalMessage: 'Which car should I buy?',
        effectiveMessage: 'Which car should I buy?',
      },
      response: {
        originalMessage: 'Which car should I buy?',
      },
    });

    assert.strictEqual(contract.canUseForDiagnosticShortlist, true);
    assert.strictEqual(contract.canUseForFinalRecommendation, false);
    assert.strictEqual(contract.finalRecommendationEnabled, false);
    assert.strictEqual(contract.allowedAnswerType, ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY);
    assert.strictEqual(contract.evidenceStatus, EVIDENCE_STATUS.PARTIAL);
    assert(!contract.blockedReasons.includes(BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET));
    assertNoFinalActivation(contract, 'candidate-evidence-useful-diagnostic-only-final-blocked');

    return {
      status: contract.status,
      evidenceStatus: contract.evidenceStatus,
      blockedReasons: contract.blockedReasons,
    };
  });

  const failed = checks.filter((check) => !check.ok);

  console.log(JSON.stringify({
    suite: 'ACI Evidence Threshold Final Blocker Audit v1',
    ok: failed.length === 0,
    total: checks.length,
    passed: checks.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((check) => check.id),
    checks,
  }, null, 2));

  if (failed.length) process.exit(1);
})().catch((error) => {
  console.error(JSON.stringify({
    suite: 'ACI Evidence Threshold Final Blocker Audit v1',
    ok: false,
    error: error.message,
  }, null, 2));
  process.exit(1);
});
