#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const GRAPH_COLLECTION =
  process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';
const GRAPH_VERSION = 'similar_model_graph_v1';
const FAST_MODE =
  process.env.ACI_SIMILAR_GRAPH_SMOKE_FAST === '1' ||
  String(process.env.ACI_SIMILAR_GRAPH_SMOKE_MODE || '').toLowerCase() === 'fast';

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

const stringify = (value) => {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value || '');
  }
};

const getBridge = (response = {}) =>
  response.aciCoreBridge ||
  response.meta?.aciCoreBridge ||
  response.data?.aciCoreBridge ||
  null;

const getSimilarRows = (result = {}) =>
  result.similarModels ||
  result.rows ||
  result.items ||
  result.data?.similarModels ||
  result.data?.rows ||
  result.data?.items ||
  [];

const rowNames = (rows = []) => rows.map((row) => row.displayName || row.modelKey).filter(Boolean);

const rowKeys = (rows = []) => rows.map((row) => row.modelKey).filter(Boolean);

const assertNoModelKeys = (rows = [], blockedKeys = [], label = 'rows') => {
  const keys = new Set(rowKeys(rows));
  const found = blockedKeys.filter((key) => keys.has(key));
  assert(!found.length, `${label} should not include ${found.join(', ')}.`);
};

const evHatchbackKeys = ['comet-ev', 'tiago-ev', 'ec3'];

const assertNoPowertrainShiftDirectRival = async ({ graphCol, anchorKey }) => {
  const graph = await graphCol.findOne({
    graphVersion: GRAPH_VERSION,
    'anchor.modelKey': anchorKey,
  });
  assert(graph, `${anchorKey} graph missing.`);
  const badRows = (graph.similarModels || []).filter(
    (model) =>
      evHatchbackKeys.includes(model.modelKey) &&
      model.relationType === 'direct_rival',
  );
  assert(!badRows.length, `${anchorKey} labels EV hatchbacks as direct_rival.`);
  return graph;
};

const allowedDefaultRelations = new Set([
  'direct_rival',
  'platform_twin',
  'nearby_alternative',
  'adjacent_crossover',
]);

const assertLiveSimilarCase = async ({
  message,
  allowedRelations,
  minRows = 2,
  anchorModelKey = 'baleno',
  runAciCoreLiveBridge,
}) => {
  const live = await runAciCoreLiveBridge({
    message,
    context: {},
    meta: {
      smokeId: `similar-model-graph-v1-${anchorModelKey}`,
      source: 'smokeSimilarModelGraphV1',
    },
  });

  const bridge = getBridge(live);
  const rows = getSimilarRows(live);
  const liveText = stringify(live);

  assert(bridge?.tool === 'vehicle_similar', `Expected ${message} to route vehicle_similar, got ${bridge?.tool}`);
  assert(bridge?.primaryTask === 'similar_cars', `Expected ${message} primaryTask similar_cars, got ${bridge?.primaryTask}`);
  assert(rows.length >= minRows, `${message} should include at least ${minRows} similar models.`);
  assert(rows.every((model) => Array.isArray(model.reasons) && model.reasons.length > 0), `${message} rows should include reasons.`);
  assert(!rows.some((model) => model.modelKey === anchorModelKey), `${message} includes same-model duplicate.`);
  if (allowedRelations?.length) {
    const allowed = new Set(allowedRelations);
    assert(
      rows.every((model) => allowed.has(model.relationType)),
      `${message} returned unexpected relation types: ${[...new Set(rows.map((row) => row.relationType))].join(', ')}`,
    );
  }
  assert(rows.every((model) => model.matchLabel), `${message} rows should include buyer-facing match labels.`);
  assert(
    !/final recommendation|you should buy|best final choice/i.test(liveText),
    `${message} contains final recommendation language.`,
  );

  return { live, bridge, rows };
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const graphCol = db.collection(GRAPH_COLLECTION);

  const balenoGraph = await graphCol.findOne({
    graphVersion: GRAPH_VERSION,
    'anchor.modelKey': 'baleno',
  });

  assert(balenoGraph, 'Baleno similar model graph missing.');
  assert(balenoGraph.anchor?.modelKey === 'baleno', 'Baleno graph anchor mismatch.');
  assert(Array.isArray(balenoGraph.similarModels), 'Baleno graph similarModels missing.');
  assert(balenoGraph.similarModels.length >= 3, 'Baleno graph should include multiple similar models.');
  assert(
    !balenoGraph.similarModels.some((model) => model.modelKey === 'baleno'),
    'Baleno graph includes same-model duplicate.',
  );
  assert(
    balenoGraph.similarModels.every((model) => Array.isArray(model.reasons) && model.reasons.length > 0),
    'Every similar model should include similarity reasons.',
  );
  const cometInBalenoGraph = balenoGraph.similarModels.find((model) => model.modelKey === 'comet-ev');
  if (cometInBalenoGraph) {
    assert(
      cometInBalenoGraph.relationType === 'powertrain_shift',
      `MG Comet EV should be powertrain_shift for Baleno, got ${cometInBalenoGraph.relationType}.`,
    );
  }
  assert(
    balenoGraph.similarModels.find((model) => model.modelKey === 'exter')?.relationType === 'adjacent_crossover',
    'Exter should be labelled adjacent_crossover for Baleno when present.',
  );
  await assertNoPowertrainShiftDirectRival({ graphCol, anchorKey: 'i20' });
  await assertNoPowertrainShiftDirectRival({ graphCol, anchorKey: 'altroz' });
  await assertNoPowertrainShiftDirectRival({ graphCol, anchorKey: 'glanza' });

  const { runVehicleSimilarTool } = await import('../../services/aiAgent/tools/newCars/vehicleSimilar.tool.js');
  const directTool = await runVehicleSimilarTool({
    userMessage: 'Show similar cars to Baleno',
    toolPlan: {
      tool: 'vehicle_similar',
      entities: { model: 'Baleno' },
      filters: {},
    },
  });

  const directRows = getSimilarRows(directTool);
  assert(directTool.tool === 'vehicle_similar', 'Direct similar tool name mismatch.');
  assert(directRows.length >= 3, 'Direct similar tool should return multiple similar models.');
  assert(
    directRows.every((model) => Array.isArray(model.reasons) && model.reasons.length > 0),
    'Direct similar tool rows should include reasons.',
  );
  assert(
    !directRows.some((model) => model.modelKey === 'baleno'),
    'Direct similar tool includes same-model duplicate.',
  );
  assert(
    directRows.every((model) => allowedDefaultRelations.has(model.relationType)),
    'Default Baleno similar should only include direct/platform/nearby/adjacent relations.',
  );
  assertNoModelKeys(directRows, evHatchbackKeys, 'Default Baleno similar');
  for (const expected of ['swift', 'glanza', 'i20', 'altroz', 'c3']) {
    assert(
      directRows.some((model) => model.modelKey === expected),
      `Default Baleno similar should include ${expected} when available.`,
    );
  }
  assert(
    !/final recommendation|you should buy|best final choice/i.test(directTool.answer || ''),
    `Direct similar answer contains final recommendation language: ${directTool.answer}`,
  );
  assert(
    !/\b\d+\.\d+\b/.test(directTool.answer || ''),
    `Direct similar answer should not expose decimal scores: ${directTool.answer}`,
  );

  const { runAciCoreLiveBridge } = await import(
    '../../services/aciCore/integration/aciCoreLiveBridge.service.js'
  );
  const { live, bridge, rows: liveRows } = await assertLiveSimilarCase({
    message: 'Show similar cars to Baleno',
    allowedRelations: [...allowedDefaultRelations],
    runAciCoreLiveBridge,
  });

  if (FAST_MODE) {
    console.log(JSON.stringify({
      status: 'ok',
      mode: 'fast',
      graphCollection: GRAPH_COLLECTION,
      graphVersion: GRAPH_VERSION,
      balenoSimilarCount: balenoGraph.similarModels.length,
      directTool: {
        count: directRows.length,
        top: rowNames(directRows).slice(0, 5),
      },
      liveBridge: {
        tool: bridge?.tool,
        primaryTask: bridge?.primaryTask,
        count: liveRows.length,
        top: rowNames(liveRows).slice(0, 5),
      },
    }, null, 2));
    await mongoose.disconnect();
    return;
  }

  const cheaperLive = await assertLiveSimilarCase({
    message: 'Cheaper alternatives to Baleno',
    allowedRelations: ['cheaper_step_down', 'direct_rival', 'nearby_alternative', 'platform_twin', 'adjacent_crossover'],
    runAciCoreLiveBridge,
  });
  const premiumLive = await assertLiveSimilarCase({
    message: 'Premium alternatives to Baleno',
    allowedRelations: ['premium_step_up'],
    minRows: 0,
    runAciCoreLiveBridge,
  });
  assertNoModelKeys(premiumLive.rows, ['glanza', 'exter', 'c3'], 'Premium Baleno alternatives');
  assert(
    premiumLive.rows.length === 0 ||
      premiumLive.rows.some((model) => model.relationType === 'premium_step_up'),
    'Premium alternatives to Baleno should include premium_step_up or return no clean step-up.',
  );
  if (premiumLive.rows.length === 0) {
    assert(
      /I understood/i.test(premiumLive.live.answer || '') &&
        /current graph does not have a clean premium step-up bucket/i.test(premiumLive.live.answer || '') &&
        /close rivals/i.test(premiumLive.live.answer || '') &&
        /cheaper step-downs/i.test(premiumLive.live.answer || '') &&
        /EV\/powertrain alternatives/i.test(premiumLive.live.answer || ''),
      `Premium empty answer should use safe no-clean-step-up wording: ${premiumLive.live.answer}`,
    );
    assert(
      /Maruti Baleno/.test(premiumLive.live.answer || ''),
      `Premium empty answer should mention resolved anchor: ${premiumLive.live.answer}`,
    );
  }
  const cometPremium = premiumLive.rows.find((model) => model.modelKey === 'comet-ev');
  if (cometPremium) {
    assert(cometPremium.relationType !== 'direct_rival', 'MG Comet EV must not be direct_rival in Baleno premium alternatives.');
  }
  const evLive = await assertLiveSimilarCase({
    message: 'EV alternatives to Baleno',
    allowedRelations: ['powertrain_shift'],
    runAciCoreLiveBridge,
  });
  assert(
    evLive.rows.some((model) => ['comet-ev', 'ec3', 'tiago-ev'].includes(model.modelKey)),
    'EV alternatives to Baleno should include Comet EV/eC3/Tiago EV when available.',
  );
  assert(
    evLive.rows.every((model) => model.matchLabel === 'Powertrain-shift option'),
    'EV alternatives should use powertrain-shift buyer labels.',
  );

  const golfDefault = await assertLiveSimilarCase({
    message: 'Show similar cars to Golf GTI',
    allowedRelations: [...allowedDefaultRelations],
    minRows: 0,
    anchorModelKey: 'golf-gti',
    runAciCoreLiveBridge,
  });
  assertNoModelKeys(
    golfDefault.rows,
    ['baleno', 'swift', 'glanza', 'i20', 'ignis', 'kwid'],
    'Default Golf GTI similar',
  );

  const golfCheaper = await assertLiveSimilarCase({
    message: 'Cheaper alternatives to Golf GTI',
    allowedRelations: ['cheaper_step_down', 'direct_rival', 'nearby_alternative', 'platform_twin', 'adjacent_crossover'],
    minRows: 1,
    anchorModelKey: 'golf-gti',
    runAciCoreLiveBridge,
  });
  assert(
    golfCheaper.rows.every((model) => model.matchLabel === 'Budget step-down' || model.relationType === 'direct_rival'),
    'Golf GTI cheaper alternatives should use budget step-down wording.',
  );

  console.log(JSON.stringify({
    status: 'ok',
    graphCollection: GRAPH_COLLECTION,
    graphVersion: GRAPH_VERSION,
    balenoGraph: {
      anchor: balenoGraph.anchor,
      similarCount: balenoGraph.similarModels.length,
      top: balenoGraph.similarModels.slice(0, 5).map((model) => ({
        displayName: model.displayName,
        modelKey: model.modelKey,
        relationType: model.relationType,
        similarityScore: model.similarityScore,
        reasons: model.reasons,
      })),
    },
    directTool: {
      count: directRows.length,
      answer: directTool.answer,
      top: directRows.slice(0, 5).map((model) => model.displayName),
    },
    liveBridge: {
      tool: bridge.tool,
      primaryTask: bridge.primaryTask,
      count: liveRows.length,
      answerPreview: String(live.answer || '').slice(0, 260),
    },
    relationCases: {
      cheaper: {
        message: 'Cheaper alternatives to Baleno',
        count: cheaperLive.rows.length,
        top: cheaperLive.rows.slice(0, 3).map((model) => model.displayName),
        relationTypes: [...new Set(cheaperLive.rows.map((model) => model.relationType))],
      },
      premium: {
        message: 'Premium alternatives to Baleno',
        count: premiumLive.rows.length,
        top: premiumLive.rows.slice(0, 3).map((model) => model.displayName),
        relationTypes: [...new Set(premiumLive.rows.map((model) => model.relationType))],
      },
      evAlternatives: {
        message: 'EV alternatives to Baleno',
        count: evLive.rows.length,
        top: rowNames(evLive.rows.slice(0, 3)),
        relationTypes: [...new Set(evLive.rows.map((model) => model.relationType))],
      },
      golfDefault: {
        message: 'Show similar cars to Golf GTI',
        count: golfDefault.rows.length,
        top: rowNames(golfDefault.rows.slice(0, 3)),
        relationTypes: [...new Set(golfDefault.rows.map((model) => model.relationType))],
      },
      golfCheaper: {
        message: 'Cheaper alternatives to Golf GTI',
        count: golfCheaper.rows.length,
        top: rowNames(golfCheaper.rows.slice(0, 3)),
        relationTypes: [...new Set(golfCheaper.rows.map((model) => model.relationType))],
      },
    },
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
