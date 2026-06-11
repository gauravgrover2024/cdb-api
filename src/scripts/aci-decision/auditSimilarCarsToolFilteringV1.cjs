#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const GRAPH_COLLECTION = process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';
const DEFAULT_GRAPH_SAMPLE_LIMIT = Math.max(1, Number(process.env.ACI_SIMILAR_AUDIT_SAMPLE_LIMIT || 80));
const GRAPH_VERSION = 'similar_model_graph_v1';

function getMongoUri() {
  return process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
}

function getDb() {
  return mongoose.connection.db;
}

function asArray(value) {
  return Array.isArray(value) ? value.filter(Boolean) : [];
}

function getRowsFromSimilarOutput(output = {}) {
  return output.similarModels || output.data?.similarModels || output.rows || [];
}

function countBy(items = [], keyGetter) {
  const out = {};
  for (const item of items) {
    const key = keyGetter(item) || 'unknown';
    out[key] = (out[key] || 0) + 1;
  }
  return out;
}

function rowId(row = {}) {
  return row.modelKey || row.displayName || '';
}

function summarizeDroppedRows(rawRows = [], directRows = []) {
  const directKeys = new Set(asArray(directRows).map(rowId).filter(Boolean));
  return asArray(rawRows)
    .filter((row) => !directKeys.has(rowId(row)))
    .slice(0, 12)
    .map((row) => ({
      modelKeyPresent: Boolean(row.modelKey),
      displayNamePresent: Boolean(row.displayName),
      relationType: row.relationType || '',
      similarityScore: row.similarityScore ?? null,
      reasonCount: Array.isArray(row.reasons) ? row.reasons.length : 0,
    }));
}

async function getCandidateGraphs(db, limit = DEFAULT_GRAPH_SAMPLE_LIMIT) {
  return db.collection(GRAPH_COLLECTION)
    .find(
      {
        graphVersion: GRAPH_VERSION,
        'anchor.modelKey': { $exists: true, $type: 'string', $ne: '' },
        similarModels: { $exists: true, $type: 'array', $ne: [] },
      },
      {
        projection: {
          _id: 0,
          graphVersion: 1,
          formulaVersion: 1,
          buildVersion: 1,
          updatedAt: 1,
          createdAt: 1,
          anchor: 1,
          similarModels: 1,
        },
      }
    )
    .sort({ updatedAt: -1, 'anchor.modelKey': 1 })
    .limit(limit)
    .toArray();
}

async function runToolForMode({ runVehicleSimilarTool, db, modelKey, mode }) {
  const messageByMode = {
    default: 'Show similar cars',
    cheaper: 'Show cheaper alternatives',
    premium: 'Show premium alternatives',
    ev: 'Show EV alternatives',
  };

  const output = await runVehicleSimilarTool({
    userMessage: messageByMode[mode] || messageByMode.default,
    toolPlan: {
      tool: 'vehicle_similar',
      input: {
        modelKey,
      },
    },
    context: {},
    db,
  });

  const rows = getRowsFromSimilarOutput(output);

  return {
    mode,
    rowCount: rows.length,
    relationTypes: countBy(rows, (row) => row.relationType),
    rows: rows.slice(0, 8).map((row) => ({
      modelKeyPresent: Boolean(row.modelKey),
      displayNamePresent: Boolean(row.displayName),
      relationType: row.relationType || '',
      similarityScore: row.similarityScore ?? null,
      matchLabel: row.matchLabel || '',
    })),
    answerPreview: String(output.answer || '').slice(0, 220),
    usageGuardrailFinal: output.usageGuardrail?.canUseForFinalRecommendation,
  };
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) {
    throw new Error('Missing Mongo URI.');
  }

  await mongoose.connect(mongoUri);
  const db = getDb();
  const { runVehicleSimilarTool } = await import('../../services/aiAgent/tools/newCars/vehicleSimilar.tool.js');

  const graphs = await getCandidateGraphs(db);
  const audits = [];

  for (const graph of graphs) {
    const rawRows = asArray(graph.similarModels);
    const modelKey = graph.anchor?.modelKey;
    if (!modelKey || rawRows.length === 0) continue;

    const modeResults = {};
    for (const mode of ['default', 'cheaper', 'premium', 'ev']) {
      modeResults[mode] = await runToolForMode({
        runVehicleSimilarTool,
        db,
        modelKey,
        mode,
      });
    }

    const defaultRows = modeResults.default?.rows || [];

    audits.push({
      anchor: {
        modelKeyPresent: Boolean(graph.anchor?.modelKey),
        displayNamePresent: Boolean(graph.anchor?.displayName),
        bodyTypeKey: graph.anchor?.bodyTypeKey || '',
      },
      graphVersion: graph.graphVersion,
      rawGraphRowCount: rawRows.length,
      rawRelationTypes: countBy(rawRows, (row) => row.relationType),
      modeRowCounts: {
        default: modeResults.default.rowCount,
        cheaper: modeResults.cheaper.rowCount,
        premium: modeResults.premium.rowCount,
        ev: modeResults.ev.rowCount,
      },
      defaultRelationTypes: modeResults.default.relationTypes,
      defaultGuardrailFinal: modeResults.default.usageGuardrailFinal,
      droppedFromDefaultSample: summarizeDroppedRows(rawRows, defaultRows),
      answerPreview: modeResults.default.answerPreview,
    });
  }

  const rawWithRows = audits.filter((item) => item.rawGraphRowCount > 0);
  const defaultZero = rawWithRows.filter((item) => item.modeRowCounts.default === 0);
  const severeDefaultShrink = rawWithRows.filter(
    (item) => item.rawGraphRowCount >= 8 && item.modeRowCounts.default <= 1
  );
  const guardrailFailures = audits.filter((item) => item.defaultGuardrailFinal !== false);

  const sortedByShrink = [...audits].sort((a, b) => {
    const aShrink = a.rawGraphRowCount - a.modeRowCounts.default;
    const bShrink = b.rawGraphRowCount - b.modeRowCounts.default;
    return bShrink - aShrink;
  });

  const summary = {
    suite: 'ACI Similar Cars Tool Filtering Audit v1',
    ok: guardrailFailures.length === 0,
    graphCollection: GRAPH_COLLECTION,
    graphVersion: GRAPH_VERSION,
    sampleLimit: DEFAULT_GRAPH_SAMPLE_LIMIT,
    inspectedGraphCount: audits.length,
    issueCounts: {
      rawWithRows: rawWithRows.length,
      defaultZeroCount: defaultZero.length,
      severeDefaultShrinkCount: severeDefaultShrink.length,
      guardrailFailures: guardrailFailures.length,
    },
    aggregate: {
      rawRowsTotal: audits.reduce((sum, item) => sum + item.rawGraphRowCount, 0),
      defaultRowsTotal: audits.reduce((sum, item) => sum + item.modeRowCounts.default, 0),
      cheaperRowsTotal: audits.reduce((sum, item) => sum + item.modeRowCounts.cheaper, 0),
      premiumRowsTotal: audits.reduce((sum, item) => sum + item.modeRowCounts.premium, 0),
      evRowsTotal: audits.reduce((sum, item) => sum + item.modeRowCounts.ev, 0),
    },
    topShrinkCases: sortedByShrink.slice(0, 12),
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) {
    process.exit(1);
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
