#!/usr/bin/env node

const assert = require('assert');

const {
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  DEGRADED_MODES,
  DECISION_MODULES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  inferDecisionDegradedMode,
  applyDecisionDegradedMode,
} = require('../../services/aciCore/decisionPolicy/aciDecisionDegradedMode.service.cjs');

const freshProvenance = Object.freeze({
  buildVersion: 'synthetic_build_v1',
  builtAt: '2026-06-03T00:00:00.000Z',
  sourceClass: SOURCE_CLASSES.MIXED,
  stalenessDays: 1,
  needsRebuild: false,
});

const freshOptions = Object.freeze({
  provenance: {
    now: new Date('2026-06-04T00:00:00.000Z'),
    maxStalenessDays: 30,
  },
});

const cases = [
  {
    id: 'unsupported-city-wins',
    input: {
      unsupportedCity: true,
      module: DECISION_MODULES.RECOMMENDATION,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.COMPLETE, confidence: CONFIDENCE_LEVELS.HIGH, usableEvidenceCount: 3, requiredEvidenceCount: 3 },
      rows: [{ entityKey: 'model_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    expected: DEGRADED_MODES.UNSUPPORTED_CITY,
  },
  {
    id: 'conflicting-evidence-blocked',
    input: {
      module: DECISION_MODULES.COMPARISON,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.CONFLICTING, confidence: CONFIDENCE_LEVELS.MEDIUM, usableEvidenceCount: 2, requiredEvidenceCount: 3 },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    expected: DEGRADED_MODES.CONFLICTING_EVIDENCE_BLOCKED,
  },
  {
    id: 'stale-provenance-needs-rebuild',
    input: {
      module: DECISION_MODULES.SIMILAR_CARS,
      provenance: {
        buildVersion: 'synthetic_build_v1',
        builtAt: '2026-04-01T00:00:00.000Z',
        sourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
        stalenessDays: 64,
        needsRebuild: false,
      },
      evidence: { evidenceStatus: EVIDENCE_STATUS.COMPLETE, confidence: CONFIDENCE_LEVELS.HIGH, usableEvidenceCount: 3, requiredEvidenceCount: 3 },
      rows: [{ entityKey: 'model_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    expected: DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD,
  },
  {
    id: 'empty-result-recovery-required',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.MISSING, confidence: CONFIDENCE_LEVELS.LOW, usableEvidenceCount: 0, requiredEvidenceCount: 3 },
      rows: [],
      diagnostics: [],
      recoveryOptions: [],
      trace: { matchedRows: 0, candidateCount: 0 },
    },
    expected: DEGRADED_MODES.EMPTY_RESULT_RECOVERY_REQUIRED,
  },
  {
    id: 'candidates-found-no-score-profiles',
    input: {
      module: DECISION_MODULES.SCORE_INSIGHT,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.MISSING, confidence: CONFIDENCE_LEVELS.LOW, usableEvidenceCount: 0, requiredEvidenceCount: 3 },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 2 },
    },
    expected: DEGRADED_MODES.CANDIDATES_FOUND_NO_SCORE_PROFILES,
  },
  {
    id: 'partial-context-score-missing',
    input: {
      module: DECISION_MODULES.SCORE_INSIGHT,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.PARTIAL, confidence: CONFIDENCE_LEVELS.MEDIUM, usableEvidenceCount: 2, requiredEvidenceCount: 4 },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    expected: DEGRADED_MODES.SCORE_PROFILES_FOUND_NO_CONTEXT_SCORE,
  },
  {
    id: 'low-confidence-degraded',
    input: {
      module: DECISION_MODULES.SCORE_INSIGHT,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.COMPLETE, confidence: CONFIDENCE_LEVELS.LOW, usableEvidenceCount: 4, requiredEvidenceCount: 4 },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    expected: DEGRADED_MODES.EVIDENCE_CONFIDENCE_TOO_LOW,
  },
  {
    id: 'healthy-output-has-no-degraded-mode',
    input: {
      module: DECISION_MODULES.RECOMMENDATION,
      provenance: freshProvenance,
      evidence: { evidenceStatus: EVIDENCE_STATUS.COMPLETE, confidence: CONFIDENCE_LEVELS.HIGH, usableEvidenceCount: 4, requiredEvidenceCount: 4 },
      rows: [{ entityKey: 'model_a' }, { entityKey: 'model_b' }],
      trace: { matchedRows: 2, candidateCount: 2 },
    },
    expected: null,
  },
];

const results = [];
const failures = [];

for (const testCase of cases) {
  try {
    const degradedMode = inferDecisionDegradedMode(testCase.input, freshOptions);
    assert.strictEqual(degradedMode, testCase.expected, 'degraded mode mismatch');

    const applied = applyDecisionDegradedMode(testCase.input, freshOptions);
    assert.strictEqual(applied.degradedMode, testCase.expected, 'applied degraded mode mismatch');

    results.push({
      id: testCase.id,
      pass: true,
      degradedMode,
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
  suite: 'ACI Decision Degraded Mode Eval v1',
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
