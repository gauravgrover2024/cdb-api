#!/usr/bin/env node

const assert = require('assert');

const {
  SOURCE_CLASSES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const {
  computeStalenessDays,
  getMissingProvenanceFields,
  evaluateDecisionProvenance,
} = require('../../services/aciCore/decisionPolicy/aciDecisionProvenance.service.cjs');

const FIXED_NOW = new Date('2026-06-04T00:00:00.000Z');

const cases = [];

function addCase(id, fn) {
  cases.push({ id, fn });
}

addCase('computes-staleness-days-from-built-at', () => {
  const days = computeStalenessDays({
    builtAt: '2026-06-01T00:00:00.000Z',
    now: FIXED_NOW,
  });

  assert.strictEqual(days, 3);
});

addCase('fresh-direct-db-fact-provenance-passes', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_build_v1',
      builtAt: '2026-06-03T00:00:00.000Z',
      sourceClass: SOURCE_CLASSES.DIRECT_DB_FACT,
      needsRebuild: false,
    },
    {
      now: FIXED_NOW,
      maxStalenessDays: 30,
    }
  );

  assert.strictEqual(result.ok, true);
  assert.strictEqual(result.status, 'fresh');
  assert.strictEqual(result.stalenessDays, 1);
  assert.deepStrictEqual(result.missingFields, []);
});

addCase('fresh-score-profile-provenance-passes', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_score_profile_build_v1',
      builtAt: '2026-06-02T00:00:00.000Z',
      sourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
      needsRebuild: false,
    },
    {
      now: FIXED_NOW,
      maxStalenessDays: 30,
    }
  );

  assert.strictEqual(result.ok, true);
  assert.strictEqual(result.sourceClassValid, true);
  assert.strictEqual(result.needsRebuild, false);
});

addCase('missing-required-fields-fails', () => {
  const missingFields = getMissingProvenanceFields({
    buildVersion: '',
    builtAt: '',
    sourceClass: SOURCE_CLASSES.MIXED,
  });

  assert.deepStrictEqual(missingFields, ['buildVersion', 'builtAt']);

  const result = evaluateDecisionProvenance(
    {
      sourceClass: SOURCE_CLASSES.MIXED,
    },
    {
      now: FIXED_NOW,
      maxStalenessDays: 30,
    }
  );

  assert.strictEqual(result.ok, false);
  assert.strictEqual(result.status, 'missing_or_invalid');
  assert(result.issues.includes('provenance_missing_required_fields'));
});

addCase('invalid-source-class-fails', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_build_v1',
      builtAt: '2026-06-03T00:00:00.000Z',
      sourceClass: 'unsupported_source_class',
      needsRebuild: false,
    },
    {
      now: FIXED_NOW,
      maxStalenessDays: 30,
    }
  );

  assert.strictEqual(result.ok, false);
  assert.strictEqual(result.sourceClassValid, false);
  assert(result.issues.includes('provenance_invalid_source_class'));
});

addCase('stale-by-threshold-fails-and-needs-rebuild', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_build_v1',
      builtAt: '2026-04-01T00:00:00.000Z',
      sourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
      needsRebuild: false,
    },
    {
      now: FIXED_NOW,
      maxStalenessDays: 30,
    }
  );

  assert.strictEqual(result.ok, false);
  assert.strictEqual(result.status, 'stale_or_rebuild_required');
  assert.strictEqual(result.staleByThreshold, true);
  assert.strictEqual(result.needsRebuild, true);
});

addCase('declared-needs-rebuild-fails-even-if-recent', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_build_v1',
      builtAt: '2026-06-03T00:00:00.000Z',
      sourceClass: SOURCE_CLASSES.MANUAL_CURATED_EVIDENCE,
      needsRebuild: true,
    },
    {
      now: FIXED_NOW,
      maxStalenessDays: 30,
    }
  );

  assert.strictEqual(result.ok, false);
  assert.strictEqual(result.needsRebuild, true);
  assert(result.issues.includes('provenance_declared_needs_rebuild'));
});

addCase('old-artifact-does-not-fail-without-explicit-threshold', () => {
  const result = evaluateDecisionProvenance(
    {
      buildVersion: 'synthetic_build_v1',
      builtAt: '2026-01-01T00:00:00.000Z',
      sourceClass: SOURCE_CLASSES.MIXED,
      needsRebuild: false,
    },
    {
      now: FIXED_NOW,
    }
  );

  assert.strictEqual(result.ok, true);
  assert.strictEqual(result.needsRebuild, false);
  assert.strictEqual(result.staleByThreshold, false);
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
  suite: 'ACI Decision Provenance Eval v1',
  ok: failures.length === 0,
  total: cases.length,
  passed,
  failed: failures.length,
  failedIds: failures.map((failure) => failure.id),
  failures,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
