#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const {
  buildCrossModelScoreDiagnostic,
} = require('../../services/aciCore/scoreProfiles/aciCrossModelScoreDiagnostic.service.cjs');

const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

async function findDynamicFixture(db) {
  const col = db.collection(SCORE_PROFILE_COLLECTION);

  const preferred = await col
    .aggregate([
      {
        $match: {
          fuelKey: 'petrol',
          transmissionKey: 'manual',
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
      { $sort: { count: -1, '_id.makeKey': 1, '_id.modelKey': 1 } },
      { $limit: 12 },
    ])
    .toArray();

  if (preferred.length >= 2) {
    return {
      fuelKey: preferred[0]._id.fuelKey,
      transmissionKey: preferred[0]._id.transmissionKey,
      targets: preferred.slice(0, 2).map((item) => item._id),
      source: 'dynamic_petrol_manual_fixture',
    };
  }

  const fallback = await col
    .aggregate([
      {
        $match: {
          modelKey: { $type: 'string', $ne: '' },
          makeKey: { $type: 'string', $ne: '' },
          fuelKey: { $type: 'string', $ne: '' },
          transmissionKey: { $type: 'string', $ne: '' },
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
      { $limit: 20 },
    ])
    .toArray();

  for (const item of fallback) {
    const pair = fallback.filter(
      (candidate) =>
        candidate._id.fuelKey === item._id.fuelKey &&
        candidate._id.transmissionKey === item._id.transmissionKey &&
        candidate._id.modelKey !== item._id.modelKey
    );

    if (pair.length > 0) {
      return {
        fuelKey: item._id.fuelKey,
        transmissionKey: item._id.transmissionKey,
        targets: [item._id, pair[0]._id],
        source: 'dynamic_any_family_fixture',
      };
    }
  }

  throw new Error('No usable cross-model score diagnostic fixture found.');
}

function assertNoFinalRecommendation(output) {
  const blob = JSON.stringify(output);
  assert.strictEqual(
    output?.usageGuardrail?.canUseForFinalRecommendation,
    false,
    'cross-model diagnostic must not allow final recommendation'
  );
  assert(!/"canUseForFinalRecommendation"\s*:\s*true/.test(blob), 'final recommendation leaked true');
  assert(!/\bmust buy\b|\bbuy this\b|\bbuy it\b|\bclear winner\b|\brecommended buy\b/i.test(blob), 'unsafe recommendation wording leaked');
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const fixture = await findDynamicFixture(db);
  const output = await buildCrossModelScoreDiagnostic({
    db,
    targets: fixture.targets,
    fuelKey: fixture.fuelKey,
    transmissionKey: fixture.transmissionKey,
    limitPerModel: 40,
  });

  assert.strictEqual(output.diagnosticType, 'cross_model_score_diagnostic', 'wrong diagnostic type');
  assert.strictEqual(output.canvasType, 'score_insight_canvas', 'wrong canvas type');
  assert.strictEqual(output.inlineType, 'score_insight_summary', 'wrong inline type');
  assert.strictEqual(output.models.length, 2, 'expected two model summaries');
  assert.strictEqual(output.missingTargets.length, 0, 'expected zero missing targets');

  for (const model of output.models) {
    assert(model.profileCount >= 2, `expected at least 2 profiles for ${model.modelKey}`);
    assert(model.modules.length >= 8, `expected module summaries for ${model.modelKey}`);
  }

  const usableComparisons = output.moduleComparisons.filter(
    (item) => item.comparedCount >= 2 && Number.isFinite(Number(item.delta))
  );
  assert(usableComparisons.length >= 5, 'expected at least 5 usable module comparisons');

  assertNoFinalRecommendation(output);

  console.log(JSON.stringify({
    suite: 'ACI Cross-Model Score Diagnostic Eval v1',
    ok: true,
    fixture: {
      source: fixture.source,
      fuelKey: fixture.fuelKey,
      transmissionKey: fixture.transmissionKey,
      targets: fixture.targets,
    },
    summary: {
      diagnosticType: output.diagnosticType,
      canvasType: output.canvasType,
      inlineType: output.inlineType,
      modelCount: output.models.length,
      profileCounts: output.models.map((model) => ({
        makeKey: model.makeKey,
        modelKey: model.modelKey,
        profileCount: model.profileCount,
      })),
      usableModuleComparisons: usableComparisons.length,
      topModuleComparisons: usableComparisons.slice(0, 5).map((item) => ({
        key: item.key,
        leader: item.leader?.modelKey,
        delta: item.delta,
      })),
      canUseForFinalRecommendation: output.usageGuardrail.canUseForFinalRecommendation,
    },
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch {}
  process.exit(1);
});
