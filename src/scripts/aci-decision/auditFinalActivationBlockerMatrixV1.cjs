#!/usr/bin/env node
'use strict';

const fs = require('fs');

const read = (file) => fs.readFileSync(file, 'utf8');

const files = {
  finalEligibility: 'src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs',
  finalReadiness: 'src/services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs',
  evidenceReadiness: 'src/services/aciCore/candidates/aciCandidateEvidenceReadiness.service.js',
  evidenceThresholdAudit: 'src/scripts/aci-decision/auditEvidenceThresholdFinalBlockerV1.cjs',
  decisionGate: 'src/scripts/aci-decision/runDecisionGateParallelV1.cjs',
  activeMarket: 'src/services/aciCore/candidates/aciCandidateActiveMarketEligibility.service.js',
  marketConfidence: 'src/services/aciCore/candidates/aciCandidateMarketConfidence.service.js',
  liveBridge: 'src/services/aciCore/integration/aciCoreLiveBridge.service.js',
  finalComposerReadinessAudit: 'src/scripts/aci-decision/auditFinalComposerReadinessV1.cjs',
};

const content = Object.fromEntries(
  Object.entries(files).map(([key, file]) => [key, read(file)])
);

const has = (fileKey, pattern) => {
  const text = content[fileKey] || '';
  return pattern instanceof RegExp ? pattern.test(text) : text.includes(pattern);
};

const blockerMatrix = [
  {
    id: 'policy-final-recommendation-dry-run-block',
    owner: 'aciFinalRecommendationEligibility.service.cjs',
    blockerType: 'policy',
    blocks: 'final recommendation activation',
    expectedRuntimeState: {
      finalRecommendationEnabled: false,
      canUseForFinalRecommendation: false,
      dryRun: true,
    },
    checks: [
      ['dry-run-present', 'finalEligibility', 'dryRun: true'],
      ['policy-not-ready-reason-present', 'finalEligibility', 'FINAL_RECOMMENDATION_POLICY_NOT_READY'],
      ['final-enabled-false', 'finalEligibility', /finalRecommendationEnabled:\s*false/],
      ['can-use-final-false', 'finalEligibility', /canUseForFinalRecommendation:\s*false/],
    ],
  },
  {
    id: 'readiness-service-activation-disabled',
    owner: 'aciFinalRecommendationReadiness.service.cjs',
    blockerType: 'readiness',
    blocks: 'global activation switch',
    expectedRuntimeState: {
      finalComposerReady: false,
      recommendationActivationEnabled: false,
      canActivateFinalRecommendation: false,
    },
    checks: [
      ['composer-ready-false', 'finalReadiness', 'finalComposerReady: false'],
      ['activation-enabled-false', 'finalReadiness', 'recommendationActivationEnabled: false'],
      ['can-activate-false', 'finalReadiness', 'canActivateFinalRecommendation: false'],
    ],
  },
  {
    id: 'candidate-evidence-readiness-final-block',
    owner: 'aciCandidateEvidenceReadiness.service.js',
    blockerType: 'candidate evidence',
    blocks: 'candidate final eligibility',
    expectedRuntimeState: {
      finalRecommendationEnabled: false,
      canUseForFinalRecommendation: false,
      allowedAnswerType: 'diagnostic_only',
    },
    checks: [
      ['final-composer-not-ready-reason', 'evidenceReadiness', 'final_composer_not_ready'],
      ['activation-disabled-reason', 'evidenceReadiness', 'recommendation_activation_disabled'],
      ['buyer-context-incomplete-reason', 'evidenceReadiness', 'buyer_context_incomplete'],
      ['final-enabled-false', 'evidenceReadiness', /finalRecommendationEnabled:\s*false/],
      ['can-use-final-false', 'evidenceReadiness', /canUseForFinalRecommendation:\s*false/],
      ['diagnostic-only-answer-type', 'evidenceReadiness', 'diagnostic_only'],
    ],
  },
  {
    id: 'evidence-threshold-final-blocker-audit',
    owner: 'auditEvidenceThresholdFinalBlockerV1.cjs',
    blockerType: 'evidence threshold',
    blocks: 'final recommendation when useful evidence is missing or below threshold',
    expectedRuntimeState: {
      evidenceThresholdMet: false,
      finalRecommendationEnabled: false,
      canUseForFinalRecommendation: false,
    },
    checks: [
      ['audit-script-present', 'evidenceThresholdAudit', 'ACI Evidence Threshold Final Blocker Audit v1'],
      ['final-readiness-threshold-check', 'evidenceThresholdAudit', 'final-readiness-missing-evidence-threshold-blocks'],
      ['final-eligibility-threshold-check', 'evidenceThresholdAudit', 'final-eligibility-missing-evidence-threshold-blocks'],
      ['candidate-empty-rows-threshold-check', 'evidenceThresholdAudit', 'candidate-evidence-empty-rows-explicit-threshold-block'],
      ['useful-evidence-positive-control-check', 'evidenceThresholdAudit', 'candidate-evidence-useful-diagnostic-only-final-blocked'],
      ['evidence-threshold-blocker-present', 'evidenceThresholdAudit', /EVIDENCE_THRESHOLD_NOT_MET|evidence_threshold_not_met/],
      ['useful-evidence-missing-blocker-present', 'evidenceThresholdAudit', /USEFUL_EVIDENCE_MISSING|useful_evidence_missing/],
      ['phase0-includes-evidence-threshold-audit', 'decisionGate', 'evidence-threshold-final-blocker-audit'],
    ],
  },
  {
    id: 'active-market-external-validation-required',
    owner: 'aciCandidateActiveMarketEligibility.service.js',
    blockerType: 'current market validation',
    blocks: 'final use of market/current-availability claims',
    expectedRuntimeState: {
      finalRecommendationEnabled: false,
      currentMarketValidationStatus: 'external_current_market_validation_required_for_final',
    },
    checks: [
      ['external-validation-status', 'activeMarket', 'external_current_market_validation_required_for_final'],
      ['external-validation-blocker', 'activeMarket', 'external_current_market_validation_required'],
      ['final-policy-not-ready-reason', 'activeMarket', 'final_recommendation_policy_not_ready'],
      ['final-composer-not-ready-reason', 'activeMarket', 'final_composer_not_ready'],
      ['activation-disabled-reason', 'activeMarket', 'recommendation_activation_disabled'],
      ['final-enabled-false', 'activeMarket', /finalRecommendationEnabled:\s*false/],
    ],
  },
  {
    id: 'market-confidence-remains-diagnostic-only',
    owner: 'aciCandidateMarketConfidence.service.js',
    blockerType: 'market confidence',
    blocks: 'market confidence being used as final recommendation evidence',
    expectedRuntimeState: {
      finalRecommendationEnabled: false,
      canUseForFinalRecommendation: false,
    },
    checks: [
      ['final-enabled-false', 'marketConfidence', /finalRecommendationEnabled:\s*false/],
      ['can-use-final-false', 'marketConfidence', /canUseForFinalRecommendation:\s*false/],
    ],
  },
  {
    id: 'live-bridge-final-blocked-ux',
    owner: 'aciCoreLiveBridge.service.js',
    blockerType: 'runtime response sealing',
    blocks: 'buyer-facing final verdict leakage',
    expectedRuntimeState: {
      finalRecommendationEnabled: false,
      finalBlockedUx: 'attached for final-intent requests',
    },
    checks: [
      ['final-blocked-reason-present', 'liveBridge', 'final_recommendation_blocked'],
      ['final-blocked-ux-present', 'liveBridge', 'finalBlockedUx'],
      ['diagnostic-only-present', 'liveBridge', 'diagnostic_only'],
      ['final-enabled-false', 'liveBridge', /finalRecommendationEnabled:\s*false/],
      ['can-use-final-false', 'liveBridge', /canUseForFinalRecommendation:\s*false/],
    ],
  },
  {
    id: 'final-composer-contract-centralized-but-blocked',
    owner: 'auditFinalComposerReadinessV1.cjs',
    blockerType: 'audit contract',
    blocks: 'accidental final activation after diagnostic wording cleanup',
    expectedRuntimeState: {
      diagnosticVerdictWordingCentralized: true,
      activationAllowed: false,
    },
    checks: [
      ['diagnostic-wording-centralized-check', 'finalComposerReadinessAudit', 'diagnostic-verdict-wording-centralized'],
      ['activation-still-blocked-check', 'finalComposerReadinessAudit', 'final-activation-still-blocked-after-wording-centralization'],
      ['activation-allowed-false', 'finalComposerReadinessAudit', 'activationAllowed: false'],
    ],
  },
];

const matrixResults = blockerMatrix.map((entry) => {
  const checks = entry.checks.map(([id, fileKey, pattern]) => ({
    id,
    fileKey,
    ok: has(fileKey, pattern),
    pattern: String(pattern),
  }));

  return {
    id: entry.id,
    owner: entry.owner,
    blockerType: entry.blockerType,
    blocks: entry.blocks,
    expectedRuntimeState: entry.expectedRuntimeState,
    ok: checks.every((check) => check.ok),
    checks,
  };
});

const runtimeActivationLeakagePatterns = [
  /finalRecommendationEnabled\s*:\s*true/,
  /canUseForFinalRecommendation\s*:\s*true/,
  /finalComposerReady\s*:\s*true/,
  /recommendationActivationEnabled\s*:\s*true/,
  /canActivateFinalRecommendation\s*:\s*true/,
  /activationAllowed\s*:\s*true/,
];

const runtimeActivationLeakage = [];
for (const fileKey of [
  'finalEligibility',
  'finalReadiness',
  'evidenceReadiness',
  'activeMarket',
  'marketConfidence',
  'liveBridge',
]) {
  for (const pattern of runtimeActivationLeakagePatterns) {
    if (pattern.test(content[fileKey])) {
      runtimeActivationLeakage.push({
        fileKey,
        pattern: String(pattern),
      });
    }
  }
}

const failed = matrixResults.filter((entry) => !entry.ok);
const ok = failed.length === 0 && runtimeActivationLeakage.length === 0;

console.log(JSON.stringify({
  suite: 'ACI Final Activation Blocker Matrix Audit v1',
  ok,
  finalRecommendationActivationAllowed: false,
  total: matrixResults.length,
  passed: matrixResults.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((entry) => entry.id),
  runtimeActivationLeakage,
  blockerMatrix: matrixResults,
}, null, 2));

if (!ok) {
  process.exit(1);
}
