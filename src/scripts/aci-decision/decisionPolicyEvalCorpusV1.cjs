const {
  DECISION_MODULES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  ALLOWED_ANSWER_TYPES,
  CLAIM_TYPES,
  DEGRADED_MODES,
  BLOCKED_REASONS,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const completeBuyerContext = Object.freeze({
  city: 'city_supported',
  budget: 2000000,
  primaryUseCase: 'primary_use_case_generic',
  familySize: 4,
  fuelPreference: 'fuel_preference_generic',
  transmissionPreference: 'transmission_preference_generic',
  safetyPriority: 'high',
  featurePriority: ['feature_a', 'feature_b'],
  shortlistedModels: ['model_a', 'model_b'],
});

const baseEvidenceComplete = Object.freeze({
  evidenceStatus: EVIDENCE_STATUS.COMPLETE,
  confidence: CONFIDENCE_LEVELS.HIGH,
  usableEvidenceCount: 5,
  requiredEvidenceCount: 5,
});

const baseTraceUseful = Object.freeze({
  toolRoute: 'synthetic_policy_eval',
  collectionsUsed: ['synthetic_read_model'],
  matchedRows: 2,
  candidateCount: 2,
  warnings: [],
});

const baseProvenanceFresh = Object.freeze({
  buildVersion: 'synthetic_eval_build_v1',
  builtAt: '2026-06-04T00:00:00.000Z',
  sourceClass: SOURCE_CLASSES.MIXED,
  stalenessDays: 0,
  needsRebuild: false,
});

const DECISION_POLICY_EVAL_CORPUS_V1 = [
  {
    id: 'final-recommendation-allowed-with-complete-context-and-evidence',
    description: 'Final recommendation may be allowed only when buyer context and evidence are complete, fresh and high-confidence.',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      intent: 'final_recommendation_request',
      requestedFinalRecommendation: true,
      buyerContext: completeBuyerContext,
      rows: [{ entityKey: 'model_a' }, { entityKey: 'model_b' }],
      evidence: baseEvidenceComplete,
      provenance: baseProvenanceFresh,
      trace: baseTraceUseful,
    },
    expect: {
      canUseForFinalRecommendation: true,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED,
      claimType: CLAIM_TYPES.OPINION,
      blockedReasonsAbsent: [BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE],
    },
  },
  {
    id: 'final-recommendation-blocked-with-missing-buyer-context',
    description: 'Final recommendation must be blocked when mandatory buyer context is missing.',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      intent: 'final_recommendation_request',
      requestedFinalRecommendation: true,
      buyerContext: {
        city: 'city_supported',
        budget: 2000000,
      },
      rows: [{ entityKey: 'model_a' }],
      evidence: baseEvidenceComplete,
      provenance: baseProvenanceFresh,
      trace: { ...baseTraceUseful, matchedRows: 1, candidateCount: 1 },
    },
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.CLARIFICATION_REQUIRED,
      blockedReasonsPresent: [
        BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE,
        BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY,
      ],
      missingMandatoryInputsMin: 1,
    },
  },
  {
    id: 'empty-result-is-recovery-not-success',
    description: 'Empty rows with no useful evidence must become recovery-required, not success.',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      intent: 'final_recommendation_request',
      requestedFinalRecommendation: true,
      buyerContext: completeBuyerContext,
      rows: [],
      diagnostics: [],
      recoveryOptions: [],
      evidence: {
        evidenceStatus: EVIDENCE_STATUS.MISSING,
        confidence: CONFIDENCE_LEVELS.LOW,
        usableEvidenceCount: 0,
        requiredEvidenceCount: 5,
      },
      provenance: baseProvenanceFresh,
      trace: {
        ...baseTraceUseful,
        matchedRows: 0,
        candidateCount: 0,
      },
    },
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED,
      degradedMode: DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED,
      blockedReasonsPresent: [
        BLOCKED_REASONS.EMPTY_RESULT,
        BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET,
      ],
    },
  },
  {
    id: 'stale-artifact-blocks-final-recommendation',
    description: 'Stale or rebuild-required artifacts must not drive final recommendations.',
    input: {
      module: DECISION_MODULES.SIMILAR_CARS,
      intent: 'similar_alternatives',
      requestedFinalRecommendation: true,
      buyerContext: completeBuyerContext,
      rows: [{ entityKey: 'model_a' }],
      evidence: {
        ...baseEvidenceComplete,
        evidenceStatus: EVIDENCE_STATUS.STALE,
      },
      provenance: {
        ...baseProvenanceFresh,
        stalenessDays: 45,
        needsRebuild: true,
      },
      trace: { ...baseTraceUseful, matchedRows: 1, candidateCount: 1 },
    },
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
      degradedMode: DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD,
      blockedReasonsPresent: [
        BLOCKED_REASONS.STALE_ARTIFACT,
        BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY,
      ],
    },
  },
  {
    id: 'conflicting-evidence-blocks-answer',
    description: 'Conflicting evidence must block confident decision advice.',
    input: {
      module: DECISION_MODULES.COMPARISON,
      intent: 'comparison_advice',
      requestedFinalRecommendation: true,
      buyerContext: completeBuyerContext,
      rows: [{ entityKey: 'variant_a' }, { entityKey: 'variant_b' }],
      evidence: {
        ...baseEvidenceComplete,
        evidenceStatus: EVIDENCE_STATUS.CONFLICTING,
      },
      provenance: baseProvenanceFresh,
      trace: baseTraceUseful,
    },
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.BLOCKED,
      degradedMode: DEGRADED_MODES.CONFLICTING_EVIDENCE_BLOCKED,
      blockedReasonsPresent: [BLOCKED_REASONS.CONFLICTING_EVIDENCE],
    },
  },
  {
    id: 'unsupported-city-is-recovery-required',
    description: 'Unsupported city must return recovery-required, not fallback pricing or final advice.',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      intent: 'city_specific_advice',
      requestedFinalRecommendation: true,
      unsupportedCity: true,
      buyerContext: {
        ...completeBuyerContext,
        city: 'city_unsupported',
      },
      rows: [{ entityKey: 'model_a' }],
      evidence: baseEvidenceComplete,
      provenance: baseProvenanceFresh,
      trace: { ...baseTraceUseful, matchedRows: 1, candidateCount: 1 },
    },
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED,
      degradedMode: DEGRADED_MODES.UNSUPPORTED_CITY,
      blockedReasonsPresent: [
        BLOCKED_REASONS.UNSUPPORTED_CITY,
        BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY,
      ],
    },
  },
  {
    id: 'sponsored-influence-is-blocked',
    description: 'Sponsored influence must block organic final recommendation logic.',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      intent: 'final_recommendation_request',
      requestedFinalRecommendation: true,
      sponsoredInfluenceDetected: true,
      buyerContext: completeBuyerContext,
      rows: [{ entityKey: 'model_a' }],
      evidence: baseEvidenceComplete,
      provenance: baseProvenanceFresh,
      trace: { ...baseTraceUseful, matchedRows: 1, candidateCount: 1 },
    },
    expect: {
      canUseForFinalRecommendation: false,
      allowedAnswerType: ALLOWED_ANSWER_TYPES.BLOCKED,
      blockedReasonsPresent: [BLOCKED_REASONS.SPONSORED_INFLUENCE_NOT_ALLOWED],
    },
  },
  {
    id: 'diagnostic-score-insight-does-not-become-final-verdict',
    description: 'Score insight can return diagnostics without becoming a final recommendation.',
    input: {
      module: DECISION_MODULES.SCORE_INSIGHT,
      intent: 'score_diagnostic',
      requestedFinalRecommendation: false,
      buyerContext: {},
      rows: [{ entityKey: 'variant_a' }],
      diagnostics: [{ key: 'score_a', value: 75 }],
      evidence: {
        evidenceStatus: EVIDENCE_STATUS.PARTIAL,
        confidence: CONFIDENCE_LEVELS.MEDIUM,
        usableEvidenceCount: 3,
        requiredEvidenceCount: 5,
      },
      provenance: {
        ...baseProvenanceFresh,
        sourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
      },
      trace: { ...baseTraceUseful, matchedRows: 1, candidateCount: 1 },
    },
    expect: {
      canUseForFinalRecommendation: false,
      forbiddenAllowedAnswerType: ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED,
      claimType: CLAIM_TYPES.DIAGNOSTIC,
      degradedMode: null,
      blockedReasonsAbsent: [
        BLOCKED_REASONS.BUYER_CONTEXT_INCOMPLETE,
        BLOCKED_REASONS.EVIDENCE_THRESHOLD_NOT_MET,
      ],
    },
  },
];

module.exports = {
  DECISION_POLICY_EVAL_CORPUS_V1,
};
