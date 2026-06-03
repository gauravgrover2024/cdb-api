#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const { loadFeatureScoreTaxonomy } = require('../../services/aciCore/scoreProfiles/featureScoreTaxonomy.loader.cjs');

const SOURCE_COLLECTION =
  process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';

const TARGET_COLLECTION =
  process.env.ACI_FEATURE_SCORE_MATRIX_PROJECTION_COLLECTION || 'aci_feature_score_matrix_projection_v1';

const args = process.argv.slice(2);
const WRITE = args.includes('--write');
const RESET = args.includes('--reset');

const normKey = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const safeArray = (value) => (Array.isArray(value) ? value : []);

const getProjectedFeatureKeys = (taxonomy) => {
  const keys = new Set();

  for (const feature of taxonomy.features || []) {
    for (const alias of feature.aliases || []) {
      const key = normKey(alias);
      if (key) keys.add(key);
    }

    for (const sourceKey of feature.sourceKeys || []) {
      const key = normKey(sourceKey);
      if (key) keys.add(key);
    }
  }

  keys.add('parking_sensors');

  return [...keys].sort();
};

const buildSourceProjection = (projectedFeatureKeys) => {
  const projection = {
    _id: 0,
    modelKey: 1,
    variantKey: 1,
    variantFullName: 1,
    activePricelistMatched: 1,
    discontinuedPricelistMatched: 1,
    featureKeys: 1,
    buildId: 1,
    updatedAt: 1,
  };

  for (const key of projectedFeatureKeys) {
    projection[`featuresByKey.${key}`] = 1;
  }

  return projection;
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const taxonomy = loadFeatureScoreTaxonomy();
  const projectedFeatureKeys = getProjectedFeatureKeys(taxonomy);

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const sourceCol = db.collection(SOURCE_COLLECTION);
  const targetCol = db.collection(TARGET_COLLECTION);

  console.error(`[load] source=${SOURCE_COLLECTION}`);
  console.error(`[load] taxonomy=${taxonomy.taxonomyVersion}; projectedFeatureKeys=${projectedFeatureKeys.length}`);

  console.time('[load] feature_score_projection_source_scan');

  const sourceDocs = await sourceCol.find(
    {},
    { projection: buildSourceProjection(projectedFeatureKeys) }
  ).maxTimeMS(120000).toArray();

  console.timeEnd('[load] feature_score_projection_source_scan');

  const docs = sourceDocs.map((doc) => {
    const featuresByKey = doc.featuresByKey || {};
    const loadedFeatureKeys = Object.keys(featuresByKey).filter((key) => featuresByKey[key] !== undefined);

    return {
      projectionKey: `${normKey(doc.modelKey)}__${normKey(doc.variantKey)}`,
      modelKey: normKey(doc.modelKey),
      variantKey: normKey(doc.variantKey),
      variantFullName: doc.variantFullName || null,
      activePricelistMatched: doc.activePricelistMatched === true,
      discontinuedPricelistMatched: doc.discontinuedPricelistMatched === true,
      featureKeys: loadedFeatureKeys,
      featureKeysInMatrixCount: safeArray(doc.featureKeys).length,
      featuresByKey,
      sourceCollection: SOURCE_COLLECTION,
      sourceBuildId: doc.buildId || null,
      sourceUpdatedAt: doc.updatedAt || null,
      taxonomyVersion: taxonomy.taxonomyVersion,
      taxonomySourcePath: taxonomy.sourcePath,
      projectedFeatureKeysCount: projectedFeatureKeys.length,
      builtAt: new Date(),
    };
  }).filter((doc) => doc.modelKey && doc.variantKey);

  let writeResult = null;

  if (WRITE) {
    if (RESET) {
      console.time('[write] delete target');
      const del = await targetCol.deleteMany({});
      console.timeEnd('[write] delete target');
      console.error(`[write] deleted=${del.deletedCount || 0}`);
    }

    console.time('[write] bulk upsert projection');
    const result = await targetCol.bulkWrite(
      docs.map((doc) => ({
        replaceOne: {
          filter: { projectionKey: doc.projectionKey },
          replacement: doc,
          upsert: true,
        },
      })),
      { ordered: false, writeConcern: { w: 1 } }
    );
    console.timeEnd('[write] bulk upsert projection');

    await targetCol.createIndex({ projectionKey: 1 }, { unique: true, name: 'projection_key_unique' });
    await targetCol.createIndex({ modelKey: 1, variantKey: 1 }, { name: 'model_variant_projection_idx' });
    await targetCol.createIndex({ taxonomyVersion: 1 }, { name: 'taxonomy_version_projection_idx' });

    writeResult = {
      matched: result.matchedCount || 0,
      modified: result.modifiedCount || 0,
      upserted: result.upsertedCount || 0,
    };
  }

  console.log(JSON.stringify({
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    reset: RESET,
    sourceCollection: SOURCE_COLLECTION,
    targetCollection: TARGET_COLLECTION,
    sourceDocs: sourceDocs.length,
    projectionDocs: docs.length,
    taxonomyVersion: taxonomy.taxonomyVersion,
    taxonomySourcePath: taxonomy.sourcePath,
    projectedFeatureKeysCount: projectedFeatureKeys.length,
    featureKeysInMatrixCountDistribution: {
      min: Math.min(...docs.map((d) => d.featureKeysInMatrixCount)),
      max: Math.max(...docs.map((d) => d.featureKeysInMatrixCount)),
    },
    writeResult,
    samples: docs.slice(0, 5).map((doc) => ({
      projectionKey: doc.projectionKey,
      variantFullName: doc.variantFullName,
      loadedFeatureKeys: doc.featureKeys.length,
      featureKeysInMatrixCount: doc.featureKeysInMatrixCount,
    })),
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
