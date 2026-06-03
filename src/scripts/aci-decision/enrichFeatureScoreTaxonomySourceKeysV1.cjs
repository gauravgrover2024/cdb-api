#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const mongoose = require('mongoose');

const TAXONOMY_PATH =
  'src/services/aciCore/scoreProfiles/config/featureScoreTaxonomy.v1.json';

const normKey = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const safeArray = (value) => (Array.isArray(value) ? value : []);

const isAliasSourceMatch = (sourceKey, alias) => {
  const source = normKey(sourceKey);
  const a = normKey(alias);
  if (!source || !a) return false;

  // Match scorer's conservative behavior:
  // exact source key OR source key is more specific than alias.
  // Do NOT use alias.includes(source), because that creates false positives.
  return source === a || source.includes(a);
};

(async () => {
  const taxonomy = JSON.parse(fs.readFileSync(TAXONOMY_PATH, 'utf8'));

  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const matrix = db.collection('vehicle_variant_feature_matrix_v2');

  console.error('[load] Reading featureKeys only from feature matrix...');
  console.time('[load] featureKeys_scan');

  const sourceKeySet = new Set();
  const cursor = matrix.find({}, { projection: { _id: 0, featureKeys: 1 } });

  let docs = 0;
  for await (const doc of cursor) {
    docs += 1;
    for (const key of safeArray(doc.featureKeys)) {
      const normalized = normKey(key);
      if (normalized) sourceKeySet.add(normalized);
    }
  }

  console.timeEnd('[load] featureKeys_scan');

  const allSourceKeys = [...sourceKeySet].sort();

  let featuresWithNoSourceKeys = 0;
  let expandedFeatureCount = 0;
  let totalSourceKeys = 0;

  for (const feature of taxonomy.featureDefinitions || []) {
    const aliases = safeArray(feature.aliases);
    const matched = new Set();

    for (const alias of aliases) {
      const normalizedAlias = normKey(alias);
      if (normalizedAlias) matched.add(normalizedAlias);

      for (const sourceKey of allSourceKeys) {
        if (isAliasSourceMatch(sourceKey, alias)) {
          matched.add(sourceKey);
        }
      }
    }

    // Special position detector depends on this generic key.
    if (feature.key === 'parkingSensorsRear' || feature.key === 'parkingSensorsFront') {
      matched.add('parking_sensors');
    }

    feature.sourceKeys = [...matched].sort();

    if (!feature.sourceKeys.length) featuresWithNoSourceKeys += 1;
    if (feature.sourceKeys.length > aliases.length) expandedFeatureCount += 1;
    totalSourceKeys += feature.sourceKeys.length;
  }

  taxonomy.taxonomyVersion = 'feature_score_taxonomy_v1_1_2026_06_03_source_keys';
  taxonomy.status = 'interim_curated_from_v2_1_with_source_keys';
  taxonomy.note =
    'External taxonomy source for feature scoring. Includes sourceKeys discovered from vehicle_variant_feature_matrix_v2.featureKeys to preserve compact-projection detection behavior.';
  taxonomy.generatedAt = new Date().toISOString();
  taxonomy.sourceKeyDiscovery = {
    matrixCollection: 'vehicle_variant_feature_matrix_v2',
    scannedDocs: docs,
    uniqueMatrixFeatureKeys: allSourceKeys.length,
    featuresWithNoSourceKeys,
    expandedFeatureCount,
    totalFeatureSourceKeys: totalSourceKeys,
  };

  fs.writeFileSync(TAXONOMY_PATH, JSON.stringify(taxonomy, null, 2) + '\n');

  console.log(JSON.stringify({
    taxonomyPath: TAXONOMY_PATH,
    taxonomyVersion: taxonomy.taxonomyVersion,
    scannedDocs: docs,
    uniqueMatrixFeatureKeys: allSourceKeys.length,
    featureDefinitions: taxonomy.featureDefinitions.length,
    featuresWithNoSourceKeys,
    expandedFeatureCount,
    totalFeatureSourceKeys: totalSourceKeys,
    samples: taxonomy.featureDefinitions
      .filter((f) => ['connectedCar', 'rearCamera', 'camera360', 'parkingSensorsRear'].includes(f.key))
      .map((f) => ({ key: f.key, aliases: f.aliases, sourceKeys: f.sourceKeys }))
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
