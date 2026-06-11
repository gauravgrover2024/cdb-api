#!/usr/bin/env node

const assert = require('assert');
const path = require('path');
const fs = require('fs');

const {
  EVIDENCE_STATUS,
  CONFIDENCE_LEVELS,
  SOURCE_CLASSES,
  DEGRADED_MODES,
  DECISION_MODULES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  DEFAULT_REQUIRED_PROVENANCE_FIELDS,
  evaluateDecisionProvenance,
} = require('../../services/aciCore/decisionPolicy/aciDecisionProvenance.service.cjs');

const {
  inferDecisionDegradedMode,
} = require('../../services/aciCore/decisionPolicy/aciDecisionDegradedMode.service.cjs');

const FIXED_NOW = new Date('2026-06-04T00:00:00.000Z');

const failures = [];
const results = [];

function check(id, fn) {
  try {
    fn();
    results.push({ id, pass: true });
  } catch (error) {
    failures.push({ id, message: error.message, stack: error.stack });
    results.push({ id, pass: false, message: error.message });
  }
}

const freshProvenance = Object.freeze({
  buildVersion: 'synthetic_build_v1',
  builtAt: '2026-06-03T00:00:00.000Z',
  sourceClass: SOURCE_CLASSES.MIXED,
  needsRebuild: false,
});

const provenanceOptions = Object.freeze({
  now: FIXED_NOW,
  maxStalenessDays: 30,
});

check('required-provenance-fields-locked', () => {
  assert.deepStrictEqual(
    DEFAULT_REQUIRED_PROVENANCE_FIELDS,
    ['buildVersion', 'builtAt', 'sourceClass']
  );
});

check('evidence-status-taxonomy-includes-freshness-states', () => {
  for (const key of ['COMPLETE', 'PARTIAL', 'MISSING', 'STALE', 'CONFLICTING', 'UNVERIFIED']) {
    assert(EVIDENCE_STATUS[key], `missing evidence status ${key}`);
  }
});

check('confidence-taxonomy-locked', () => {
  assert.strictEqual(CONFIDENCE_LEVELS.HIGH, 'high');
  assert.strictEqual(CONFIDENCE_LEVELS.MEDIUM, 'medium');
  assert.strictEqual(CONFIDENCE_LEVELS.LOW, 'low');
});

check('stale-provenance-degrades-by-threshold', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_build_v1',
      builtAt: '2026-04-01T00:00:00.000Z',
      sourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
      needsRebuild: false,
    },
    provenanceOptions
  );

  assert.strictEqual(result.ok, false);
  assert.strictEqual(result.status, 'stale_or_rebuild_required');
  assert.strictEqual(result.staleByThreshold, true);
  assert(result.issues.includes('provenance_stale_by_threshold'));
});

check('stale-evidence-status-degrades-even-with-fresh-provenance', () => {
  const degradedMode = inferDecisionDegradedMode(
    {
      module: DECISION_MODULES.SCORE_INSIGHT,
      provenance: freshProvenance,
      evidence: {
        evidenceStatus: EVIDENCE_STATUS.STALE,
        confidence: CONFIDENCE_LEVELS.MEDIUM,
        usableEvidenceCount: 2,
        requiredEvidenceCount: 3,
      },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    { provenance: provenanceOptions }
  );

  assert.strictEqual(degradedMode, DEGRADED_MODES.STALE_ARTIFACT_NEEDS_REBUILD);
});

check('unverified-evidence-status-degrades-to-review-required', () => {
  const degradedMode = inferDecisionDegradedMode(
    {
      module: DECISION_MODULES.SCORE_INSIGHT,
      provenance: freshProvenance,
      evidence: {
        evidenceStatus: EVIDENCE_STATUS.UNVERIFIED,
        confidence: CONFIDENCE_LEVELS.MEDIUM,
        usableEvidenceCount: 2,
        requiredEvidenceCount: 3,
      },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    { provenance: provenanceOptions }
  );

  assert.strictEqual(degradedMode, DEGRADED_MODES.UNVERIFIED_EVIDENCE_REVIEW_REQUIRED);
});

check('conflicting-evidence-takes-precedence-over-staleness', () => {
  const degradedMode = inferDecisionDegradedMode(
    {
      module: DECISION_MODULES.COMPARISON,
      provenance: {
        buildVersion: 'synthetic_build_v1',
        builtAt: '2026-04-01T00:00:00.000Z',
        sourceClass: SOURCE_CLASSES.MIXED,
        needsRebuild: false,
      },
      evidence: {
        evidenceStatus: EVIDENCE_STATUS.CONFLICTING,
        confidence: CONFIDENCE_LEVELS.MEDIUM,
        usableEvidenceCount: 2,
        requiredEvidenceCount: 3,
      },
      rows: [{ entityKey: 'variant_a' }],
      trace: { matchedRows: 1, candidateCount: 1 },
    },
    { provenance: provenanceOptions }
  );

  assert.strictEqual(degradedMode, DEGRADED_MODES.CONFLICTING_EVIDENCE_BLOCKED);
});

check('external-evidence-contract-audit-exists', () => {
  const auditPath = path.resolve(__dirname, 'auditVariantExternalEvidenceContractV1.cjs');
  assert(fs.existsSync(auditPath), 'external evidence contract audit missing');
});

const summary = {
  suite: 'ACI Decision Evidence/Freshness Contract Audit v1',
  ok: failures.length === 0,
  total: results.length,
  passed: results.filter((item) => item.pass).length,
  failed: failures.length,
  failedIds: failures.map((failure) => failure.id),
  failures,
  results,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
