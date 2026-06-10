#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const GRAPH_COLLECTION =
  process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';

const EXPECTED_GRAPH_VERSION = 'similar_model_graph_v1';
const EXPECTED_FORMULA_VERSION = 'similarity_model_v1_relation_guardrails';

const MAX_MUST_HAVE_RANK = 8;

const ALLOWED_RELATION_TYPES = new Set([
  'true_platform_twin',
  'platform_twin',
  'direct_rival',
  'nearby_alternative',
  'adjacent_alternative',
  'adjacent_crossover',
  'lifestyle_alternative',
  'cheaper_step_down',
  'premium_step_up',
  'powertrain_shift',
]);

const MUST_HAVE_EDGES = [
  {
    anchor: 'creta',
    target: 'seltos',
    allowedRelations: ['direct_rival', 'nearby_alternative'],
    reason: 'Creta and Seltos are core compact SUV cross-shop rivals.',
  },
  {
    anchor: 'seltos',
    target: 'creta',
    allowedRelations: ['direct_rival', 'nearby_alternative'],
    reason: 'Seltos and Creta are core compact SUV cross-shop rivals.',
  },
  {
    anchor: 'grand-vitara',
    target: 'hyryder',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Grand Vitara and Hyryder are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'hyryder',
    target: 'grand-vitara',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Hyryder and Grand Vitara are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'baleno',
    target: 'glanza',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Baleno and Glanza are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'glanza',
    target: 'baleno',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Glanza and Baleno are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'fronx',
    target: 'taisor',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Fronx and Taisor are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'taisor',
    target: 'fronx',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Taisor and Fronx are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'taigun',
    target: 'kushaq',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Taigun and Kushaq are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'kushaq',
    target: 'taigun',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Kushaq and Taigun are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'slavia',
    target: 'virtus',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Slavia and Virtus are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'virtus',
    target: 'slavia',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Virtus and Slavia are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'ertiga',
    target: 'rumion',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Ertiga and Rumion are expected twin/near-twin alternatives.',
  },
  {
    anchor: 'rumion',
    target: 'ertiga',
    allowedRelations: ['true_platform_twin', 'platform_twin'],
    reason: 'Rumion and Ertiga are expected twin/near-twin alternatives.',
  },
];

const FORBIDDEN_PLATFORM_TWIN_EDGES = [
  ['creta', 'curvv'],
  ['creta', 'sierra'],
  ['creta', 'duster'],
  ['creta', 'hector'],
  ['seltos', 'taigun'],
  ['seltos', 'kushaq'],
  ['grand-vitara', 'taigun'],
  ['grand-vitara', 'kushaq'],
  ['venue', 'nexon'],
  ['venue', 'xuv-3xo'],
  ['venue', 'aircross'],
  ['sonet', 'nexon'],
  ['sonet', 'xuv-3xo'],
  ['sonet', 'kylaq'],
  ['nexon', 'sonet'],
  ['nexon', 'venue'],
  ['nexon', 'taisor'],
  ['brezza', 'kylaq'],
  ['brezza', 'taisor'],
  ['hyryder', 'hector'],
  ['hyryder', 'duster'],
  ['sonet', 'aircross'],
  ['nexon', 'xuv-3xo'],
  ['punch', 'magnite'],
  ['fronx', 'kylaq'],
];

const FORBIDDEN_DEFAULT_DIRECT_RIVALS = [
  {
    anchor: 'creta',
    target: 'thar',
    reason: 'Thar is a lifestyle/off-road alternative, not a default family compact SUV direct rival.',
  },
  {
    anchor: 'seltos',
    target: 'thar-roxx',
    reason: 'Thar Roxx should not be a default direct rival for Seltos unless lifestyle/off-road intent is explicit.',
  },
  {
    anchor: 'hyryder',
    target: 'thar',
    reason: 'Thar is not a default direct rival for Hyryder family/hybrid-oriented cross-shopping.',
  },
];

const QUALITY_ANCHORS = [
  'creta',
  'seltos',
  'grand-vitara',
  'hyryder',
  'brezza',
  'venue',
  'sonet',
  'nexon',
  'punch',
  'fronx',
  'baleno',
  'swift',
  'i20',
  'dzire',
  'amaze',
  'city',
  'verna',
  'xuv-7xo',
  'scorpio-n',
  'harrier',
  'safari',
  'ertiga',
  'carens',
  'innova-hycross',
  'tiago-ev',
  'nexon-ev',
  'comet-ev',
];

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function getRows(doc) {
  return asArray(doc?.similarModels);
}

function findEdge(doc, targetModelKey) {
  return getRows(doc).find((row) => row?.modelKey === targetModelKey) || null;
}

function edgeRank(doc, targetModelKey) {
  const index = getRows(doc).findIndex((row) => row?.modelKey === targetModelKey);
  return index >= 0 ? index + 1 : null;
}

function addFailure(failures, type, detail) {
  failures.push({ type, ...detail });
}

async function main() {
  await mongoose.connect(process.env.MONGO_URI || process.env.MONGODB_URI || process.env.DATABASE_URL);
  const db = mongoose.connection.db;
  const graph = db.collection(GRAPH_COLLECTION);

  const exists = await db.listCollections({ name: GRAPH_COLLECTION }).hasNext();
  const failures = [];

  if (!exists) {
    addFailure(failures, 'missing_graph_collection', { collection: GRAPH_COLLECTION });
    console.log(JSON.stringify({
      suite: 'ACI Similar Model Graph Buyer Quality Audit v1',
      ok: false,
      graphCollection: GRAPH_COLLECTION,
      failures,
    }, null, 2));
    process.exit(1);
  }

  const docs = await graph.find(
    { 'anchor.modelKey': { $in: Array.from(new Set([
      ...QUALITY_ANCHORS,
      ...MUST_HAVE_EDGES.flatMap((edge) => [edge.anchor, edge.target]),
      ...FORBIDDEN_PLATFORM_TWIN_EDGES.flat(),
      ...FORBIDDEN_DEFAULT_DIRECT_RIVALS.flatMap((edge) => [edge.anchor, edge.target]),
    ])) } },
    { projection: { _id: 0, anchor: 1, graphVersion: 1, formulaVersion: 1, similarModels: 1, updatedAt: 1 } },
  ).toArray();

  const byAnchor = new Map(docs.map((doc) => [doc.anchor?.modelKey, doc]));

  const missingAnchors = QUALITY_ANCHORS.filter((modelKey) => !byAnchor.has(modelKey));
  if (missingAnchors.length) {
    addFailure(failures, 'missing_quality_anchor_graphs', { missingAnchors });
  }

  for (const doc of docs) {
    const anchor = doc.anchor || {};
    const rows = getRows(doc);

    if (doc.graphVersion !== EXPECTED_GRAPH_VERSION) {
      addFailure(failures, 'unexpected_graph_version', {
        anchor: anchor.modelKey,
        graphVersion: doc.graphVersion,
      });
    }

    if (doc.formulaVersion !== EXPECTED_FORMULA_VERSION) {
      addFailure(failures, 'unexpected_formula_version', {
        anchor: anchor.modelKey,
        formulaVersion: doc.formulaVersion,
      });
    }

    if (!rows.length) {
      addFailure(failures, 'empty_similar_rows', {
        anchor: anchor.modelKey,
        displayName: anchor.displayName,
      });
    }

    const selfEdge = rows.find((row) => row?.modelKey === anchor.modelKey);
    if (selfEdge) {
      addFailure(failures, 'self_edge_detected', {
        anchor: anchor.modelKey,
        displayName: anchor.displayName,
      });
    }

    for (const row of rows) {
      if (!row?.modelKey || !row?.displayName) {
        addFailure(failures, 'missing_row_identity', {
          anchor: anchor.modelKey,
          row,
        });
      }

      if (!ALLOWED_RELATION_TYPES.has(row?.relationType)) {
        addFailure(failures, 'unsafe_relation_type', {
          anchor: anchor.modelKey,
          target: row?.modelKey,
          relationType: row?.relationType,
        });
      }

      if (!Array.isArray(row?.reasons) || row.reasons.length === 0) {
        addFailure(failures, 'missing_reasons', {
          anchor: anchor.modelKey,
          target: row?.modelKey,
        });
      }

      if (typeof row?.similarityScore !== 'number' || row.similarityScore < 0 || row.similarityScore > 100) {
        addFailure(failures, 'invalid_similarity_score', {
          anchor: anchor.modelKey,
          target: row?.modelKey,
          similarityScore: row?.similarityScore,
        });
      }
    }
  }

  for (const edge of MUST_HAVE_EDGES) {
    const doc = byAnchor.get(edge.anchor);
    if (!doc) {
      addFailure(failures, 'must_have_anchor_missing', edge);
      continue;
    }

    const found = findEdge(doc, edge.target);
    const rank = edgeRank(doc, edge.target);

    if (!found) {
      addFailure(failures, 'must_have_edge_missing', {
        ...edge,
        anchorDisplayName: doc.anchor?.displayName,
        availableTop10: getRows(doc).slice(0, 10).map((row) => ({
          modelKey: row.modelKey,
          displayName: row.displayName,
          relationType: row.relationType,
          similarityScore: row.similarityScore,
        })),
      });
      continue;
    }

    if (!edge.allowedRelations.includes(found.relationType)) {
      addFailure(failures, 'must_have_edge_wrong_relation', {
        ...edge,
        actualRelation: found.relationType,
        rank,
        targetDisplayName: found.displayName,
      });
    }

    if (rank && rank > MAX_MUST_HAVE_RANK) {
      addFailure(failures, 'must_have_edge_rank_too_low', {
        ...edge,
        actualRelation: found.relationType,
        rank,
        maxAllowedRank: MAX_MUST_HAVE_RANK,
        targetDisplayName: found.displayName,
      });
    }
  }

  for (const [anchorKey, targetKey] of FORBIDDEN_PLATFORM_TWIN_EDGES) {
    const doc = byAnchor.get(anchorKey);
    if (!doc) continue;

    const found = findEdge(doc, targetKey);
    if (found?.relationType === 'platform_twin' || found?.relationType === 'true_platform_twin') {
      addFailure(failures, 'false_platform_twin_relation', {
        anchor: anchorKey,
        anchorDisplayName: doc.anchor?.displayName,
        target: targetKey,
        targetDisplayName: found.displayName,
        relationType: found.relationType,
        similarityScore: found.similarityScore,
        reasons: found.reasons,
      });
    }
  }

  for (const edge of FORBIDDEN_DEFAULT_DIRECT_RIVALS) {
    const doc = byAnchor.get(edge.anchor);
    if (!doc) continue;

    const found = findEdge(doc, edge.target);
    if (found?.relationType === 'direct_rival') {
      addFailure(failures, 'lifestyle_model_marked_default_direct_rival', {
        ...edge,
        anchorDisplayName: doc.anchor?.displayName,
        targetDisplayName: found.displayName,
        similarityScore: found.similarityScore,
        reasons: found.reasons,
      });
    }
  }

  const inspected = QUALITY_ANCHORS.map((modelKey) => {
    const doc = byAnchor.get(modelKey);
    return {
      modelKey,
      found: Boolean(doc),
      displayName: doc?.anchor?.displayName || null,
      top5: getRows(doc).slice(0, 5).map((row) => ({
        modelKey: row.modelKey,
        displayName: row.displayName,
        relationType: row.relationType,
        similarityScore: row.similarityScore,
      })),
    };
  });

  const failureCounts = failures.reduce((acc, failure) => {
    acc[failure.type] = (acc[failure.type] || 0) + 1;
    return acc;
  }, {});

  const summary = {
    suite: 'ACI Similar Model Graph Buyer Quality Audit v1',
    ok: failures.length === 0,
    graphCollection: GRAPH_COLLECTION,
    expectedGraphVersion: EXPECTED_GRAPH_VERSION,
    expectedFormulaVersion: EXPECTED_FORMULA_VERSION,
    checkedQualityAnchors: QUALITY_ANCHORS.length,
    mustHaveEdgeCount: MUST_HAVE_EDGES.length,
    forbiddenPlatformTwinEdgeCount: FORBIDDEN_PLATFORM_TWIN_EDGES.length,
    forbiddenDefaultDirectRivalCount: FORBIDDEN_DEFAULT_DIRECT_RIVALS.length,
    failureCounts,
    failureCount: failures.length,
    failures: failures.slice(0, 120),
    inspected,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) process.exit(1);
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
