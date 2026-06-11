#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const {
  ALLOWED_ANSWER_TYPES,
  CONFIDENCE_LEVELS,
  DECISION_MODULES,
  EVIDENCE_STATUS,
  SOURCE_CLASSES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');

const CASES = [
  {
    id: 'score-baleno-overall',
    message: 'How good is Baleno overall?',
    expectedTool: 'vehicle_score_insight',
    expectedModule: DECISION_MODULES.SCORE_INSIGHT,
    expectedSourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
  },
  {
    id: 'score-baleno-alpha-worth-over-zeta',
    message: 'Is Baleno Alpha worth over Zeta?',
    expectedTool: 'vehicle_score_insight',
    expectedModule: DECISION_MODULES.SCORE_INSIGHT,
    expectedSourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
  },
  {
    id: 'cross-model-score',
    message: 'Tiago vs Altroz cng automatic overall score comparison',
    expectedTool: 'vehicle_score_insight',
    expectedModule: DECISION_MODULES.SCORE_INSIGHT,
    expectedSourceClass: SOURCE_CLASSES.INTERNAL_SCORE_PROFILE,
  },
  {
    id: 'similar-creta',
    message: 'Cars similar to Creta',
    expectedTool: 'vehicle_similar',
    expectedModule: DECISION_MODULES.SIMILAR_CARS,
    expectedSourceClass: SOURCE_CLASSES.SIMILAR_GRAPH_INFERENCE,
  },
];

const okEvidenceStatuses = new Set([
  EVIDENCE_STATUS.COMPLETE,
  EVIDENCE_STATUS.PARTIAL,
  EVIDENCE_STATUS.MISSING,
  EVIDENCE_STATUS.STALE,
  EVIDENCE_STATUS.CONFLICTING,
  EVIDENCE_STATUS.UNVERIFIED,
]);

const okNonFinalAnswerTypes = new Set([
  ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
  ALLOWED_ANSWER_TYPES.RECOVERY_REQUIRED,
]);

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const { runAciCoreLiveBridge } = await import('../../services/aciCore/integration/aciCoreLiveBridge.service.js');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const results = [];

  for (const testCase of CASES) {
    const response = await runAciCoreLiveBridge({
      message: testCase.message,
      context: {},
      meta: { smokeId: `decision-runtime-envelope-${testCase.id}` },
    });

    const bridge = response.aciCoreBridge || response.meta?.aciCoreBridge || {};
    const decisionPolicy = response.decisionPolicy || {};
    const evidence = response.evidence || {};
    const provenance = response.provenance || {};
    const sourceCollections = response.sourceCollections || [];

    assert.strictEqual(bridge.tool, testCase.expectedTool, `${testCase.id}: wrong bridge tool`);
    assert.strictEqual(response.module, testCase.expectedModule, `${testCase.id}: wrong decision module`);
    assert.ok(
      okNonFinalAnswerTypes.has(decisionPolicy.allowedAnswerType),
      `${testCase.id}: unsafe or unexpected allowedAnswerType ${decisionPolicy.allowedAnswerType}`
    );
    assert.strictEqual(decisionPolicy.canUseForFinalRecommendation, false, `${testCase.id}: final recommendation not blocked`);
    assert.ok(okEvidenceStatuses.has(evidence.evidenceStatus), `${testCase.id}: invalid evidenceStatus ${evidence.evidenceStatus}`);
    assert.ok(
      [CONFIDENCE_LEVELS.HIGH, CONFIDENCE_LEVELS.MEDIUM, CONFIDENCE_LEVELS.LOW].includes(evidence.confidence),
      `${testCase.id}: invalid evidence confidence ${evidence.confidence}`
    );
    assert.strictEqual(provenance.sourceClass, testCase.expectedSourceClass, `${testCase.id}: wrong provenance sourceClass`);
    assert.ok(provenance.status, `${testCase.id}: provenance status missing`);
    assert.ok(Array.isArray(sourceCollections) && sourceCollections.length > 0, `${testCase.id}: sourceCollections missing`);
    assert.strictEqual(response.data?.decisionPolicy?.canUseForFinalRecommendation, false, `${testCase.id}: data decisionPolicy missing`);
    assert.strictEqual(response.meta?.decisionPolicy?.canUseForFinalRecommendation, false, `${testCase.id}: meta decisionPolicy missing`);

    results.push({
      id: testCase.id,
      message: testCase.message,
      tool: bridge.tool,
      module: response.module,
      operation: bridge.operation || response.operation || '',
      allowedAnswerType: decisionPolicy.allowedAnswerType,
      canUseForFinalRecommendation: decisionPolicy.canUseForFinalRecommendation,
      evidenceStatus: evidence.evidenceStatus,
      evidenceConfidence: evidence.confidence,
      provenanceStatus: provenance.status,
      provenanceSourceClass: provenance.sourceClass,
      degradedMode: response.degradedMode,
      sourceCollections,
      answerPreview: String(response.answer || '').slice(0, 260),
    });
  }

  const summary = {
    suite: 'ACI Decision Runtime Envelope Smoke v1',
    ok: true,
    total: results.length,
    passed: results.length,
    failed: 0,
    failedIds: [],
    results,
  };

  console.log(JSON.stringify(summary, null, 2));
  await mongoose.disconnect();
})().catch((error) => {
  console.error(error);
  mongoose.disconnect().catch(() => {}).finally(() => process.exit(1));
});
