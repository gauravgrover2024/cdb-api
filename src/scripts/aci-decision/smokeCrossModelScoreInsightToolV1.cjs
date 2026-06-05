#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

async function findDynamicFixture(db) {
  const rows = await db.collection(SCORE_PROFILE_COLLECTION)
    .aggregate([
      {
        $match: {
          fuelKey: { $type: 'string', $ne: '' },
          transmissionKey: { $type: 'string', $ne: '' },
          modelKey: { $type: 'string', $ne: '' },
          makeKey: { $type: 'string', $ne: '' },
        },
      },
      {
        $group: {
          _id: {
            makeKey: '$makeKey',
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
      return {
        fuelKey: row._id.fuelKey,
        transmissionKey: row._id.transmissionKey,
        targets: [row._id, peer._id],
      };
    }
  }

  throw new Error('No cross-model score tool fixture found.');
}

function assertNoUnsafeRecommendationLanguage(output) {
  const blob = JSON.stringify(output || {});
  assert(
    !/\bmust buy\b|\bbuy this\b|\bbuy it\b|\bclear winner\b|\brecommended buy\b/i.test(blob),
    'Unsafe recommendation wording leaked'
  );
}

(async () => {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const { runVehicleScoreInsightTool } = await import(
    '../../services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js'
  );

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const fixture = await findDynamicFixture(db);

  const output = await runVehicleScoreInsightTool({
    operation: 'cross_model_score_diagnostic',
    targets: fixture.targets,
    fuelKey: fixture.fuelKey,
    transmissionKey: fixture.transmissionKey,
    limit: 40,
  });

  assert.strictEqual(output.status, 'success', `Cross-model score tool failed: ${output.error?.message}`);
  assert.strictEqual(output.operation, 'cross_model_score_diagnostic', 'wrong operation');
  assert.strictEqual(output.canvasType, 'score_insight_canvas', 'wrong canvasType');
  assert.strictEqual(output.inlineType, 'score_insight_summary', 'wrong inlineType');
  assert.strictEqual(output.usageGuardrail.canUseForFinalRecommendation, false, 'tool guardrail must block final recommendation');
  assert.strictEqual(output.data.usageGuardrail.canUseForFinalRecommendation, false, 'data guardrail must block final recommendation');
  assert.strictEqual(output.data.models.length, 2, 'expected two models');
  assert(output.data.moduleComparisons.filter((item) => item.comparedCount >= 2).length >= 5, 'expected module comparisons');
  assert(/diagnostic-only/i.test(output.answer), 'answer should say diagnostic-only');
  assertNoUnsafeRecommendationLanguage(output);

  console.log(JSON.stringify({
    suite: 'ACI Cross-Model Score Insight Tool Smoke v1',
    ok: true,
    fixture,
    output: {
      status: output.status,
      operation: output.operation,
      canvasType: output.canvasType,
      inlineType: output.inlineType,
      answer: output.answer,
      modelCount: output.data.models.length,
      moduleComparisons: output.data.moduleComparisons
        .filter((item) => item.comparedCount >= 2)
        .slice(0, 5)
        .map((item) => ({
          key: item.key,
          leader: item.leader?.modelKey,
          delta: item.delta,
        })),
      canUseForFinalRecommendation: output.usageGuardrail.canUseForFinalRecommendation,
    },
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
