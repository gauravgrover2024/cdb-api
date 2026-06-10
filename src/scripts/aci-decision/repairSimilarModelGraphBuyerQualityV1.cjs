#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const GRAPH_COLLECTION =
  process.env.ACI_SIMILAR_MODEL_GRAPH_COLLECTION || 'aci_vehicle_similar_model_graph_v1';

const REPAIR_VERSION = 'similar_model_graph_buyer_quality_repair_v1_2026_06_10';

const WRITE = process.argv.includes('--write');

const KNOWN_PLATFORM_TWINS = [
  ['grand-vitara', 'hyryder'],
  ['hyryder', 'grand-vitara'],
  ['baleno', 'glanza'],
  ['glanza', 'baleno'],
  ['fronx', 'taisor'],
  ['taisor', 'fronx'],
  ['taigun', 'kushaq'],
  ['kushaq', 'taigun'],
  ['slavia', 'virtus'],
  ['virtus', 'slavia'],
  ['ertiga', 'rumion'],
  ['rumion', 'ertiga'],
];

const KNOWN_CORE_DIRECT_RIVALS = [
  ['creta', 'seltos'],
  ['seltos', 'creta'],
];

const FALSE_PLATFORM_TWIN_RELATIONS = [
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

const LIFESTYLE_NOT_DEFAULT_DIRECT = [
  ['creta', 'thar'],
  ['seltos', 'thar-roxx'],
  ['hyryder', 'thar'],
];

const relationPriority = {
  platform_twin: 10,
  direct_rival: 20,
  nearby_alternative: 30,
  adjacent_crossover: 40,
  adjacent_alternative: 40,
  lifestyle_alternative: 50,
  cheaper_step_down: 60,
  premium_step_up: 70,
  powertrain_shift: 80,
};

function asArray(value) {
  return Array.isArray(value) ? value : [];
}

function uniq(values) {
  return Array.from(new Set(values.filter(Boolean)));
}

function cloneAnchorAsSimilar(anchor = {}) {
  return {
    makeKey: anchor.makeKey || null,
    modelKey: anchor.modelKey || null,
    displayName: anchor.displayName || null,
    bodyTypeKey: anchor.bodyTypeKey || null,
    bodyType: anchor.bodyType || null,
    minExShowroomPrice: anchor.minExShowroomPrice ?? null,
    maxExShowroomPrice: anchor.maxExShowroomPrice ?? null,
    fuels: asArray(anchor.fuels),
    transmissions: asArray(anchor.transmissions),
  };
}

function sortRows(rows) {
  return asArray(rows)
    .filter((row) => row && row.modelKey)
    .sort((a, b) => {
      const pa = relationPriority[a.relationType] ?? 99;
      const pb = relationPriority[b.relationType] ?? 99;
      if (pa !== pb) return pa - pb;

      const scoreDiff = Number(b.similarityScore || 0) - Number(a.similarityScore || 0);
      if (Math.abs(scoreDiff) > 0.0001) return scoreDiff;

      return String(a.displayName || a.modelKey).localeCompare(String(b.displayName || b.modelKey));
    })
    .slice(0, 24);
}

function addOrUpdateEdge(doc, targetDoc, relationType, minScore, repairReason) {
  if (!doc || !targetDoc?.anchor?.modelKey) return { changed: false, action: 'missing_target_doc' };

  const rows = asArray(doc.similarModels).map((row) => ({ ...row }));
  const targetModelKey = targetDoc.anchor.modelKey;
  const existingIndex = rows.findIndex((row) => row.modelKey === targetModelKey);

  const base = cloneAnchorAsSimilar(targetDoc.anchor);

  if (existingIndex >= 0) {
    const current = rows[existingIndex];
    rows[existingIndex] = {
      ...current,
      ...base,
      relationType,
      similarityScore: Math.max(Number(current.similarityScore || 0), minScore),
      reasons: uniq([
        repairReason,
        ...(Array.isArray(current.reasons) ? current.reasons : []),
      ]),
      relationQualityOverride: {
        repairVersion: REPAIR_VERSION,
        previousRelationType: current.relationType || null,
        previousSimilarityScore: current.similarityScore ?? null,
        appliedAt: new Date(),
      },
    };

    doc.similarModels = sortRows(rows);
    return { changed: true, action: 'updated_existing_edge' };
  }

  rows.push({
    ...base,
    relationType,
    similarityScore: minScore,
    reasons: [
      repairReason,
      'Added by controlled buyer-quality graph repair after relation audit.',
    ],
    relationQualityOverride: {
      repairVersion: REPAIR_VERSION,
      previousRelationType: null,
      previousSimilarityScore: null,
      appliedAt: new Date(),
    },
  });

  doc.similarModels = sortRows(rows);
  return { changed: true, action: 'added_missing_edge' };
}

function demoteFalsePlatformTwin(doc, targetKey, replacementRelationType, repairReason) {
  const rows = asArray(doc?.similarModels).map((row) => ({ ...row }));
  const index = rows.findIndex((row) => row.modelKey === targetKey);

  if (index < 0) return { changed: false, action: 'edge_not_found' };

  const current = rows[index];

  if (current.relationType !== 'platform_twin') {
    return { changed: false, action: 'edge_not_platform_twin' };
  }

  rows[index] = {
    ...current,
    relationType: replacementRelationType,
    reasons: uniq([
      repairReason,
      ...(Array.isArray(current.reasons)
        ? current.reasons.filter((reason) => !/very similar price band and configuration spread/i.test(reason))
        : []),
    ]),
    relationQualityOverride: {
      repairVersion: REPAIR_VERSION,
      previousRelationType: current.relationType || null,
      previousSimilarityScore: current.similarityScore ?? null,
      appliedAt: new Date(),
    },
  };

  doc.similarModels = sortRows(rows);
  return { changed: true, action: 'demoted_false_platform_twin' };
}

function demoteLifestyleDirect(doc, targetKey, repairReason) {
  const rows = asArray(doc?.similarModels).map((row) => ({ ...row }));
  const index = rows.findIndex((row) => row.modelKey === targetKey);

  if (index < 0) return { changed: false, action: 'edge_not_found' };

  const current = rows[index];

  if (current.relationType !== 'direct_rival') {
    return { changed: false, action: 'edge_not_direct_rival' };
  }

  rows[index] = {
    ...current,
    relationType: 'lifestyle_alternative',
    similarityScore: Math.min(Number(current.similarityScore || 0), 74),
    reasons: uniq([
      repairReason,
      'Lifestyle/off-road alternative separated from default family/city direct-rival set.',
      ...(Array.isArray(current.reasons) ? current.reasons : []),
    ]),
    relationQualityOverride: {
      repairVersion: REPAIR_VERSION,
      previousRelationType: current.relationType || null,
      previousSimilarityScore: current.similarityScore ?? null,
      appliedAt: new Date(),
    },
  };

  doc.similarModels = sortRows(rows);
  return { changed: true, action: 'demoted_lifestyle_direct_rival' };
}

function summarizeTop(doc) {
  return asArray(doc?.similarModels).slice(0, 8).map((row) => ({
    modelKey: row.modelKey,
    displayName: row.displayName,
    relationType: row.relationType,
    similarityScore: row.similarityScore,
  }));
}

async function main() {
  const mongoUri = process.env.MONGO_URI || process.env.MONGODB_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;

  const exists = await db.listCollections({ name: GRAPH_COLLECTION }).hasNext();
  if (!exists) throw new Error(`Missing graph collection: ${GRAPH_COLLECTION}`);

  const graph = db.collection(GRAPH_COLLECTION);
  const docs = await graph.find({}, { projection: { _id: 0 } }).toArray();
  const byModelKey = new Map(docs.map((doc) => [doc.anchor?.modelKey, doc]));

  const touched = new Map();
  const actions = [];

  function markTouched(doc, action) {
    if (!doc?.anchor?.modelKey) return;
    touched.set(doc.anchor.modelKey, doc);
    actions.push({
      anchor: doc.anchor.modelKey,
      anchorDisplayName: doc.anchor.displayName,
      ...action,
    });
  }

  for (const [anchorKey, targetKey] of KNOWN_PLATFORM_TWINS) {
    const doc = byModelKey.get(anchorKey);
    const targetDoc = byModelKey.get(targetKey);
    const result = addOrUpdateEdge(
      doc,
      targetDoc,
      'platform_twin',
      98,
      'Known twin/near-twin relation from controlled buyer-quality seed.',
    );
    markTouched(doc, { target: targetKey, relationType: 'platform_twin', ...result });
  }

  for (const [anchorKey, targetKey] of KNOWN_CORE_DIRECT_RIVALS) {
    const doc = byModelKey.get(anchorKey);
    const targetDoc = byModelKey.get(targetKey);
    const result = addOrUpdateEdge(
      doc,
      targetDoc,
      'direct_rival',
      96,
      'Known high-volume compact SUV cross-shop relation from controlled buyer-quality seed.',
    );
    markTouched(doc, { target: targetKey, relationType: 'direct_rival', ...result });
  }

  for (const [anchorKey, targetKey] of FALSE_PLATFORM_TWIN_RELATIONS) {
    const doc = byModelKey.get(anchorKey);
    const result = demoteFalsePlatformTwin(
      doc,
      targetKey,
      'direct_rival',
      'Demoted: similar price/configuration does not mean platform/twin relationship.',
    );
    markTouched(doc, { target: targetKey, replacementRelationType: 'direct_rival', ...result });
  }

  for (const [anchorKey, targetKey] of LIFESTYLE_NOT_DEFAULT_DIRECT) {
    const doc = byModelKey.get(anchorKey);
    const result = demoteLifestyleDirect(
      doc,
      targetKey,
      'Lifestyle/off-road model should not be labelled as a default direct rival without explicit lifestyle intent.',
    );
    markTouched(doc, { target: targetKey, replacementRelationType: 'lifestyle_alternative', ...result });
  }

  const now = new Date();
  const ops = [];

  for (const doc of touched.values()) {
    ops.push({
      updateOne: {
        filter: { 'anchor.modelKey': doc.anchor.modelKey },
        update: {
          $set: {
            similarModels: doc.similarModels,
            updatedAt: now,
            buyerQualityRepair: {
              repairVersion: REPAIR_VERSION,
              appliedAt: now,
              mode: 'post_build_relation_quality_overlay',
            },
          },
          $addToSet: {
            repairVersions: REPAIR_VERSION,
          },
        },
      },
    });
  }

  let writeResult = null;
  if (WRITE && ops.length) {
    writeResult = await graph.bulkWrite(ops, { ordered: false });
  }

  const previewAnchorKeys = [
    'creta',
    'seltos',
    'grand-vitara',
    'hyryder',
    'ertiga',
    'rumion',
    'venue',
    'sonet',
    'nexon',
    'brezza',
  ];

  const previewDocs = WRITE
    ? await graph.find(
      { 'anchor.modelKey': { $in: previewAnchorKeys } },
      { projection: { _id: 0, anchor: 1, similarModels: 1, buyerQualityRepair: 1 } },
    ).toArray()
    : previewAnchorKeys
      .map((modelKey) => byModelKey.get(modelKey))
      .filter(Boolean);

  console.log(JSON.stringify({
    suite: 'ACI Similar Model Graph Buyer Quality Repair v1',
    graphCollection: GRAPH_COLLECTION,
    repairVersion: REPAIR_VERSION,
    write: WRITE,
    graphDocs: docs.length,
    touchedAnchors: touched.size,
    actions,
    writeResult: writeResult ? {
      matchedCount: writeResult.matchedCount,
      modifiedCount: writeResult.modifiedCount,
    } : null,
    preview: previewDocs.map((doc) => ({
      anchor: doc.anchor?.modelKey,
      displayName: doc.anchor?.displayName,
      buyerQualityRepair: doc.buyerQualityRepair || null,
      top8: summarizeTop(doc),
    })),
  }, null, 2));
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
