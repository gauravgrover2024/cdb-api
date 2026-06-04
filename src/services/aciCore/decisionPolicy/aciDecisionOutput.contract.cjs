const {
  DECISION_OUTPUT_SCHEMA_VERSION,
  DECISION_POLICY_VERSION,
  CLAIM_TYPES,
  ALLOWED_ANSWER_TYPES,
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
} = require('./aciDecisionPolicy.constants.cjs');

const asArray = (value) => (Array.isArray(value) ? value : []);
const asObject = (value) => (value && typeof value === 'object' && !Array.isArray(value) ? value : {});
const hasRows = (value) => Array.isArray(value) && value.length > 0;

function createBaseDecisionPolicy(overrides = {}) {
  return {
    decisionPolicyVersion: DECISION_POLICY_VERSION,
    canUseForFinalRecommendation: false,
    allowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
    blockedReasons: [],
    missingMandatoryInputs: [],
    evidenceStatus: EVIDENCE_STATUS.PARTIAL,
    confidence: CONFIDENCE_LEVELS.MEDIUM,
    degradedMode: null,
    claimType: CLAIM_TYPES.DIAGNOSTIC,
    ...asObject(overrides),
  };
}

function createBaseEvidence(overrides = {}) {
  return {
    evidenceStatus: EVIDENCE_STATUS.PARTIAL,
    confidence: CONFIDENCE_LEVELS.MEDIUM,
    sourceTransparency: [],
    missingData: [],
    usableEvidenceCount: 0,
    requiredEvidenceCount: 0,
    ...asObject(overrides),
  };
}

function createBaseProvenance(overrides = {}) {
  return {
    buildVersion: '',
    builtAt: '',
    sourceClass: SOURCE_CLASSES.MIXED,
    stalenessDays: null,
    needsRebuild: false,
    ...asObject(overrides),
  };
}

function createBaseTrace(overrides = {}) {
  return {
    toolRoute: '',
    collectionsUsed: [],
    matchedRows: 0,
    candidateCount: 0,
    warnings: [],
    ...asObject(overrides),
  };
}

function normalizeDecisionOutput(input = {}) {
  const value = asObject(input);

  return {
    schemaVersion: value.schemaVersion || DECISION_OUTPUT_SCHEMA_VERSION,
    module: value.module || '',
    intent: value.intent || '',
    comparisonScope: {
      scopeType: '',
      city: '',
      entities: [],
      ...asObject(value.comparisonScope),
    },
    claimType: value.claimType || CLAIM_TYPES.DIAGNOSTIC,
    decisionPolicy: createBaseDecisionPolicy(value.decisionPolicy),
    evidence: createBaseEvidence(value.evidence),
    provenance: createBaseProvenance(value.provenance),
    degradedMode: value.degradedMode || null,
    rows: asArray(value.rows),
    diagnostics: asArray(value.diagnostics),
    recoveryOptions: asArray(value.recoveryOptions),
    trace: createBaseTrace(value.trace),
  };
}

function getDecisionOutputUsefulness(output = {}) {
  const normalized = normalizeDecisionOutput(output);
  const usableEvidenceCount = Number(normalized.evidence.usableEvidenceCount || 0);
  const matchedRows = Number(normalized.trace.matchedRows || 0);
  const candidateCount = Number(normalized.trace.candidateCount || 0);

  return {
    hasRows: hasRows(normalized.rows),
    hasDiagnostics: hasRows(normalized.diagnostics),
    hasRecoveryOptions: hasRows(normalized.recoveryOptions),
    hasUsableEvidence: usableEvidenceCount > 0,
    hasMatchedRows: matchedRows > 0,
    hasCandidates: candidateCount > 0,
    hasDegradedMode: Boolean(normalized.degradedMode || normalized.decisionPolicy.degradedMode),
  };
}

function decisionOutputHasUsefulResult(output = {}) {
  const usefulness = getDecisionOutputUsefulness(output);

  return (
    usefulness.hasRows ||
    usefulness.hasDiagnostics ||
    usefulness.hasRecoveryOptions ||
    usefulness.hasUsableEvidence ||
    usefulness.hasMatchedRows ||
    usefulness.hasCandidates
  );
}

module.exports = {
  createBaseDecisionPolicy,
  createBaseEvidence,
  createBaseProvenance,
  createBaseTrace,
  normalizeDecisionOutput,
  getDecisionOutputUsefulness,
  decisionOutputHasUsefulResult,
};
