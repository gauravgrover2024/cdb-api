const {
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  DEGRADED_MODES,
} = require('./aciDecisionPolicy.constants.cjs');

const {
  normalizeDecisionOutput,
  getDecisionOutputUsefulness,
} = require('./aciDecisionOutput.contract.cjs');

const {
  evaluateDecisionProvenance,
} = require('./aciDecisionProvenance.service.cjs');

function inferDecisionDegradedMode(input = {}, options = {}) {
  const output = normalizeDecisionOutput(input);
  const usefulness = getDecisionOutputUsefulness(output);
  const evidence = output.evidence || {};
  const provenance = output.provenance || {};
  const trace = output.trace || {};

  if (input.unsupportedCity === true || output.degradedMode === DEGRADED_MODES.UNSUPPORTED_CITY) {
    return DEGRADED_MODES.UNSUPPORTED_CITY;
  }

  if (evidence.evidenceStatus === EVIDENCE_STATUS.CONFLICTING) {
    return DEGRADED_MODES.CONFLICTING_EVIDENCE_BLOCKED;
  }

  if (evidence.evidenceStatus === EVIDENCE_STATUS.STALE) {
    return DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD;
  }

  if (evidence.evidenceStatus === EVIDENCE_STATUS.UNVERIFIED) {
    return DEGRADED_MODES.UNVERIFIED_EVIDENCE_REVIEW_REQUIRED;
  }

  const provenanceStatus = evaluateDecisionProvenance(provenance, options.provenance || {});
  if (provenanceStatus.status === 'stale_or_rebuild_required') {
    return DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD;
  }

  if (
    !usefulness.hasRows &&
    !usefulness.hasDiagnostics &&
    !usefulness.hasRecoveryOptions &&
    !usefulness.hasUsableEvidence &&
    !usefulness.hasMatchedRows &&
    !usefulness.hasCandidates
  ) {
    return DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED;
  }

  if (
    Number(trace.candidateCount || 0) === 0 &&
    Number(trace.matchedRows || 0) === 0 &&
    !usefulness.hasRows
  ) {
    return DEGRADED_MODES.RESOLVER_SUCCEEDED_NO_CANDIDATES;
  }

  if (
    Number(trace.candidateCount || 0) > 0 &&
    evidence.evidenceStatus === EVIDENCE_STATUS.MISSING
  ) {
    return DEGRADED_MODES.CANDIDATES_FOUND_NO_SCORE_PROFILES;
  }

  if (
    evidence.evidenceStatus === EVIDENCE_STATUS.PARTIAL &&
    Number(evidence.requiredEvidenceCount || 0) > Number(evidence.usableEvidenceCount || 0)
  ) {
    return DEGRADED_MODES.SCORE_PROFILES_FOUND_NO_CONTEXT_SCORE;
  }

  if (evidence.confidence === CONFIDENCE_LEVELS.LOW) {
    return DEGRADED_MODES.EVIDENCE_CONFIDENCE_TOO_LOW;
  }

  if (output.decisionPolicy && output.decisionPolicy.canUseForFinalRecommendation === false && input.requestedFinalRecommendation === true) {
    return DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED;
  }

  if (output.degradedMode) {
    return output.degradedMode;
  }

  return null;
}

function applyDecisionDegradedMode(input = {}, options = {}) {
  const output = normalizeDecisionOutput(input);
  const degradedMode = inferDecisionDegradedMode(input, options);

  return {
    ...output,
    degradedMode,
    decisionPolicy: {
      ...output.decisionPolicy,
      degradedMode: output.decisionPolicy.degradedMode || degradedMode,
    },
  };
}

module.exports = {
  inferDecisionDegradedMode,
  applyDecisionDegradedMode,
};
