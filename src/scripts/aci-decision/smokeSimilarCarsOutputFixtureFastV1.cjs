#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');
const mongoose = require('mongoose');

const getRows = (output = {}) =>
  output.similarModels ||
  output.rows ||
  output.items ||
  output.data?.similarModels ||
  output.data?.rows ||
  output.data?.items ||
  [];

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  try {
    const { runVehicleSimilarTool } = await import('../../services/aiAgent/tools/newCars/vehicleSimilar.tool.js');

    const output = await runVehicleSimilarTool({
      userMessage: 'Show similar cars to Baleno',
      toolPlan: {
        tool: 'vehicle_similar',
        entities: { model: 'Baleno' },
        filters: {},
      },
    });

    const rows = getRows(output);
    const blob = JSON.stringify(output);

    assert.strictEqual(output.tool, 'vehicle_similar', 'similar fast fixture should use vehicle_similar tool');
    assert(rows.length >= 3, `similar fast fixture expected >=3 rows, got ${rows.length}`);
    assert(!rows.some((row) => row.modelKey === 'baleno'), 'similar fast fixture includes anchor duplicate');
    assert(rows.every((row) => Array.isArray(row.reasons) && row.reasons.length > 0), 'similar fast fixture rows should include reasons');
    assert(rows.every((row) => row.matchLabel), 'similar fast fixture rows should include buyer match labels');
    assert(!/"canUseForFinalRecommendation"\s*:\s*true/.test(blob), 'similar fast fixture leaked final recommendation true');
    assert(!/"allowedAnswerType"\s*:\s*"final_recommendation_allowed"/.test(blob), 'similar fast fixture leaked final recommendation allowed');
    assert(!/\byou should buy\b|\bbest final choice\b|\bmy final recommendation\b/i.test(blob), 'similar fast fixture leaked unsafe buy language');

    console.log(JSON.stringify({
      suite: 'ACI Similar Cars Output Fixture Fast Smoke v1',
      ok: true,
      rows: rows.length,
      top: rows.slice(0, 5).map((row) => row.displayName || row.modelKey),
      canUseForFinalRecommendation:
        output.usageGuardrail?.canUseForFinalRecommendation ??
        output.data?.usageGuardrail?.canUseForFinalRecommendation ??
        false,
    }, null, 2));
  } finally {
    await mongoose.disconnect();
  }
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
