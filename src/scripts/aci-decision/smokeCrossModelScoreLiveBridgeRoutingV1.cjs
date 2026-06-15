#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const titleCase = (value = '') =>
  String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();

async function findDynamicFixture(db) {
  const rows = await db.collection(SCORE_PROFILE_COLLECTION)
    .aggregate([
      {
        $match: {
          fuelKey: { $type: 'string', $ne: '' },
          transmissionKey: { $type: 'string', $ne: '' },
          modelKey: { $type: 'string', $ne: '' },
        },
      },
      {
        $group: {
          _id: {
            modelKey: '$modelKey',
            fuelKey: '$fuelKey',
            transmissionKey: '$transmissionKey',
          },
          count: { $sum: 1 },
        },
      },
      { $match: { count: { $gte: 2 } } },
      { $sort: { '_id.fuelKey': 1, '_id.transmissionKey': 1, count: -1 } },
      { $limit: 80 },
    ])
    .toArray();

  for (const row of rows) {
    const peer = rows.find((candidate) =>
      candidate._id.modelKey !== row._id.modelKey &&
      candidate._id.fuelKey === row._id.fuelKey &&
      candidate._id.transmissionKey === row._id.transmissionKey
    );

    if (peer) {
      const message = `${titleCase(row._id.modelKey)} vs ${titleCase(peer._id.modelKey)} ${row._id.fuelKey} ${row._id.transmissionKey} overall score comparison`;
      return {
        message,
        fuelKey: row._id.fuelKey,
        transmissionKey: row._id.transmissionKey,
        models: [row._id.modelKey, peer._id.modelKey],
      };
    }
  }

  throw new Error('No usable live bridge cross-model score routing fixture found.');
}

function bridgeOf(output = {}) {
  return output.aciCoreBridge || output.meta?.aciCoreBridge || output.data?.aciCoreBridge || null;
}

function assertNoUnsafeRecommendationLanguage(output) {
  const blob = JSON.stringify(output || {});
  assert(
    !/\bmust buy\b|\bbuy this\b|\bbuy it\b|\bclear winner\b|\brecommended buy\b/i.test(blob),
    'Unsafe recommendation wording leaked'
  );
}

function assertSingleDiagnosticOnlyNote(output, message = '') {
  const answer = String(output?.answer || '');
  const count = (
    answer.match(/This score view is diagnostic-only|This is diagnostic-only module scoring|This is diagnostic-only, not a final recommendation|Treat this as diagnostic-only guidance|This is a diagnostic signal only|Use this as diagnostic context/gi) || []
  ).length;

  assert(
    count <= 1,
    `${message}: duplicate diagnostic-only note leaked in answer: ${answer}`
  );
}


async function assertCrossModelScoreRoute({ runAciCoreLiveBridge, message, fixture }) {
  const output = await runAciCoreLiveBridge({
    message,
    context: {},
    user: null,
    session: {},
    meta: { source: 'smokeCrossModelScoreLiveBridgeRoutingV1' },
  });

  const bridge = bridgeOf(output);
  const blob = JSON.stringify(output || {});

  assert(bridge, `${message}: aciCoreBridge metadata missing`);
  assert.strictEqual(bridge.tool, 'vehicle_score_insight', `${message}: wrong bridge tool`);
  assert.strictEqual(bridge.primaryTask, 'score_insight', `${message}: wrong primary task`);
  assert.strictEqual(bridge.operation, 'cross_model_score_diagnostic', `${message}: wrong bridge operation`);
  assert.strictEqual(output.canvasType, 'score_insight_canvas', `${message}: wrong canvas type`);
  assert.strictEqual(output.inlineType, 'score_insight_summary', `${message}: wrong inline type`);
  assert.strictEqual(
    output.operation || output.data?.operation || output.data?.diagnosticType,
    'cross_model_score_diagnostic',
    `${message}: actual output operation missing`
  );
  assert(Array.isArray(output.data?.models), `${message}: actual output models array missing`);
  assert.strictEqual(output.data.models.length, 2, `${message}: actual output should include exactly two model summaries`);
  assert(Array.isArray(output.data?.moduleComparisons), `${message}: actual output moduleComparisons missing`);
  assert(output.data.moduleComparisons.length >= 5, `${message}: actual output should include module comparisons`);

  const answerText = String(output.answer || '');
  for (const model of fixture.models) {
    assert(
      new RegExp(model.replace(/[-_]+/g, ' '), 'i').test(answerText) ||
        blob.toLowerCase().includes(model.toLowerCase()),
      `${message}: answer/output missing compared model: ${model}`
    );
  }

  assert(blob.includes('cross_model_score_diagnostic'), `${message}: cross-model diagnostic marker missing`);
  assert(/diagnostic-only/i.test(output.answer || blob), `${message}: diagnostic-only wording missing`);
  assert(
    blob.includes('"canUseForFinalRecommendation":false') ||
      blob.includes('"finalRecommendationEnabled":false'),
    `${message}: final recommendation guardrail missing`
  );
  assertNoUnsafeRecommendationLanguage(output);
  assertSingleDiagnosticOnlyNote(output, message);

  return {
    message,
    answerPreview: String(output.answer || '').slice(0, 500),
    bridge: {
      tool: bridge.tool,
      primaryTask: bridge.primaryTask,
      operation: bridge.operation,
      routingReason: bridge.routingReason,
      planMode: bridge.planMode,
      usedGemini: bridge.usedGemini,
    },
  };
}

(async () => {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const mod = await import('../../services/aciCore/integration/aciCoreLiveBridge.service.js');
  const runAciCoreLiveBridge = mod.runAciCoreLiveBridge || mod.default;
  if (typeof runAciCoreLiveBridge !== 'function') {
    throw new Error('runAciCoreLiveBridge export not found.');
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;
  const fixture = await findDynamicFixture(db);

  const displayModels = fixture.models.map(titleCase);
  const messages = [
    fixture.message,
    `${displayModels[0]} vs ${displayModels[1]}: which scores better overall?`,
    `Compare ${displayModels[0]} and ${displayModels[1]} on overall score`,
    `Which one scores better, ${displayModels[0]} or ${displayModels[1]}?`,
  ];

  const cases = [];
  for (const message of messages) {
    cases.push(await assertCrossModelScoreRoute({ runAciCoreLiveBridge, message, fixture }));
  }

  console.log(JSON.stringify({
    suite: 'ACI Cross-Model Score Live Bridge Routing Smoke v1',
    ok: true,
    fixture,
    total: cases.length,
    passed: cases.length,
    cases,
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
