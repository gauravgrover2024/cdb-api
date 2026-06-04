const DECISION_POLICY_VERSION = 'aci_decision_policy_v1_2026_06_04';
const DECISION_OUTPUT_SCHEMA_VERSION = 'aci_decision_output_v1_2026_06_04';

const CLAIM_TYPES = Object.freeze({
  FACT: 'fact',
  DIAGNOSTIC: 'diagnostic',
  OPINION: 'opinion',
});

const ALLOWED_ANSWER_TYPES = Object.freeze({
  FACT_ONLY: 'fact_only',
  DIAGNOSTIC_ONLY: 'diagnostic_only',
  CLARIFICATION_REQUIRED: 'clarification_required',
  RECOVERY_REQUIRED: 'recovery_required',
  FINAL_RECOMMENDATION_ALLOWED: 'final_recommendation_allowed',
  BLOCKED: 'blocked',
});

const EVIDENCE_STATUS = Object.freeze({
  COMPLETE: 'complete',
  PARTIAL: 'partial',
  MISSING: 'missing',
  STALE: 'stale',
  CONFLICTING: 'conflicting',
  UNVERIFIED: 'unverified',
});

const CONFIDENCE_LEVELS = Object.freeze({
  HIGH: 'high',
  MEDIUM: 'medium',
  LOW: 'low',
});

const SOURCE_CLASSES = Object.freeze({
  DIRECT_DB_FACT: 'direct_db_fact',
  INTERNAL_SCORE_PROFILE: 'internal_score_profile',
  SIMILAR_GRAPH_INFERENCE: 'similar_graph_inference',
  ESTIMATED_VALUE: 'estimated_value',
  INHERITED_MODEL_LEVEL_EVIDENCE: 'inherited_model_level_evidence',
  MANUAL_CURATED_EVIDENCE: 'manual_curated_evidence',
  MIXED: 'mixed',
});

const DEGRADED_MODES = Object.freeze({
  RESOLVER_SUCCEEDED_NO_CANDIDATES: 'resolver_succeeded_no_candidates',
  CANDIDATES_FOUND_NO_SCORE_PROFILES: 'candidates_found_no_score_profiles',
  SCORE_PROFILES_FOUND_NO_CONTEXT_SCORE: 'score_profiles_found_no_context_score',
  EVIDENCE_CONFIDENCE_TOO_LOW: 'evidence_confidence_too_low',
  STALE_ARTIFACT_NEEDS_REBUILD: 'stale_artifact_needs_rebuild',
  CONFLICTING_EVIDENCE_BLOCKED: 'conflicting_evidence_blocked',
  BUYER_CONTEXT_INCOMPLETE: 'buyer_context_incomplete',
  UNSUPPORTED_CITY: 'unsupported_city',
  FINAL_RECOMMENDATION_BLOCKED: 'final_recommendation_blocked',
  DIAGNOSTIC_ONLY_AVAILABLE: 'diagnostic_only_available',
  EMPTY_RESULT_RECOVERY_REQUIRED: 'empty_result_recovery_required',
});

const BLOCKED_REASONS = Object.freeze({
  BUYER_CONTEXT_INCOMPLETE: 'buyer_context_incomplete',
  EVIDENCE_THRESHOLD_NOT_MET: 'evidence_threshold_not_met',
  USEFUL_EVIDENCE_MISSING: 'useful_evidence_missing',
  EMPTY_RESULT: 'empty_result',
  STALE_ARTIFACT: 'stale_artifact',
  CONFLICTING_EVIDENCE: 'conflicting_evidence',
  UNSUPPORTED_CITY: 'unsupported_city',
  FINAL_RECOMMENDATION_POLICY_NOT_READY: 'final_recommendation_policy_not_ready',
  MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE: 'module_not_final_recommendation_eligible',
  SPONSORED_INFLUENCE_NOT_ALLOWED: 'sponsored_influence_not_allowed',
});

const MANDATORY_FINAL_RECOMMENDATION_INPUTS = Object.freeze([
  'city',
  'budgetOrPriceCeiling',
  'bodyPreferenceOrPrimaryUseCase',
  'familySizeOrOccupancyUse',
  'fuelPreferenceOrMonthlyRunning',
  'transmissionPreference',
  'safetyPriority',
  'featurePriority',
  'shortlistedModelsOrDiscoveryScope',
]);

const DECISION_MODULES = Object.freeze({
  SCORE_INSIGHT: 'score_insight',
  SIMILAR_CARS: 'similar_cars',
  UPGRADE_LADDER: 'upgrade_ladder',
  RECOMMENDATION: 'recommendation',
  COMPARISON: 'comparison',
});

module.exports = {
  DECISION_POLICY_VERSION,
  DECISION_OUTPUT_SCHEMA_VERSION,
  CLAIM_TYPES,
  ALLOWED_ANSWER_TYPES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  DEGRADED_MODES,
  BLOCKED_REASONS,
  MANDATORY_FINAL_RECOMMENDATION_INPUTS,
  DECISION_MODULES,
};
