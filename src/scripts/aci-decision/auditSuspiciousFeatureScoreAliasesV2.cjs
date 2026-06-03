#!/usr/bin/env node
require('dotenv').config();

const fs = require('fs');
const mongoose = require('mongoose');

const OUT = '/tmp/aci_suspicious_feature_alias_audit_v2.json';

const normKey = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const safeArray = (value) => (Array.isArray(value) ? value : []);

const textOf = (value) => {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
};

const looksAvailable = (value) => {
  if (value === true) return true;
  if (value === false || value === null || value === undefined) return false;
  if (typeof value === 'number') return Number.isFinite(value) && value > 0;

  const text = textOf(value).toLowerCase().trim();
  if (!text) return false;
  if (text.includes('not available')) return false;
  if (text.includes('"available":false')) return false;
  if (text.includes('"value":"not available"')) return false;
  if (['no', 'false', 'na', 'n/a', '-', '0'].includes(text)) return false;

  return true;
};

const buildFeatureMatrixIndex = (docs) => {
  const index = new Map();

  const preference = (doc) =>
    (doc.activePricelistMatched === true ? 100 : 0) +
    (doc.discontinuedPricelistMatched === true ? -50 : 0) +
    safeArray(doc.featureKeys).length +
    Object.keys(doc.featuresByKey || {}).length;

  for (const doc of docs) {
    const key = `${normKey(doc.modelKey)}__${normKey(doc.variantKey)}`;
    if (!key || key === '__') continue;

    const existing = index.get(key);
    const enriched = { ...doc, __joinKey: key };

    if (!existing || preference(enriched) > preference(existing)) {
      index.set(key, enriched);
    }
  }

  return index;
};

const SPEC_OR_NON_BUYER_KEYS = [
  'max_power',
  'max_torque',
  'transmission_type',
  'gearbox',
  'drive_type',
  'fuel_type',
  'displacement',
  'engine_type',
  'number_of_cylinders',
  'valves_per_cylinder',
  'emission_norm_compliance',
  'length',
  'width',
  'height',
  'wheel_base',
  'turning_radius',
  'front_suspension',
  'rear_suspension',
  'front_brake_type',
  'rear_brake_type',
  'petrol_mileage_arai',
  'engine_displacement',
  'number_of_doors',
  'tyre_size',
  'tyre_type',
  'wheel_size',
  'boot_space',
  'seating_capacity',
  'petrol_fuel_tank_capacity'
];

const BUYER_FEATURE_PATTERNS = [
  'airbag',
  'abs',
  'ebd',
  'esc',
  'stability',
  'isofix',
  'tpms',
  'hill',
  'camera',
  'parking',
  'sensor',
  'rear_ac',
  'climate',
  'air_conditioning',
  'heater',
  'power_steering',
  'power_windows',
  'central_locking',
  'keyless',
  'cruise',
  'touch',
  'screen',
  'display',
  'cluster',
  'speaker',
  'sound',
  'usb',
  'charging',
  'wireless',
  'connected',
  'sunroof',
  'ventilated',
  'seat',
  'headrest',
  'foldable',
  'leather',
  'upholstery',
  'lamp',
  'led',
  'headlamp',
  'orvm',
  'wiper',
  'defogger'
];

const isSpecOrNonBuyerKey = (key) => {
  const k = normKey(key);
  return SPEC_OR_NON_BUYER_KEYS.some((x) => k === x || k.includes(x));
};

const isBuyerFeatureKey = (key, value) => {
  const k = normKey(key);
  const v = normKey(textOf(value));
  if (isSpecOrNonBuyerKey(k)) return false;
  return BUYER_FEATURE_PATTERNS.some((p) => k.includes(p) || v.includes(p));
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const scores = db.collection('aci_vehicle_variant_score_profile');
  const matrix = db.collection('vehicle_variant_feature_matrix_v2');

  const suspiciousScores = await scores
    .find(
      { 'scoreReadiness.knownSourceLimitations': 'feature_alias_diagnostic' },
      {
        projection: {
          _id: 0,
          variantFullName: 1,
          makeKey: 1,
          modelKey: 1,
          variantKey: 1,
          scoreProfileKey: 1,
          priceSegment: 1,
          featureScore: 1,
          valueScore: 1,
          cityUseScore: 1,
          premiumComfortScore: 1,
          scoreReadiness: 1
        },
      },
    )
    .sort({ 'featureScore.score': 1, variantFullName: 1 })
    .toArray();

  const matrixDocs = await matrix
    .find(
      {},
      {
        projection: {
          _id: 0,
          modelKey: 1,
          variantKey: 1,
          variantFullName: 1,
          activePricelistMatched: 1,
          discontinuedPricelistMatched: 1,
          featureKeys: 1,
          featuresByKey: 1,
          buildId: 1
        },
      },
    )
    .toArray();

  const matrixIndex = buildFeatureMatrixIndex(matrixDocs);

  const rows = [];

  for (const score of suspiciousScores) {
    const joinKey = `${normKey(score.modelKey)}__${normKey(score.variantKey)}`;
    const doc = matrixIndex.get(joinKey) || null;
    const featuresByKey = doc?.featuresByKey || {};

    const availableBuyerFeatures = Object.entries(featuresByKey)
      .filter(([key, value]) => looksAvailable(value) && isBuyerFeatureKey(key, value))
      .map(([key, value]) => ({ key, value }))
      .slice(0, 120);

    const unavailableBuyerFeatures = Object.entries(featuresByKey)
      .filter(([key, value]) => !looksAvailable(value) && isBuyerFeatureKey(key, value))
      .map(([key, value]) => ({ key, value }))
      .slice(0, 60);

    rows.push({
      variantFullName: score.variantFullName,
      scoreProfileKey: score.scoreProfileKey,
      priceSegment: score.priceSegment,
      modelKey: score.modelKey,
      variantKey: score.variantKey,
      joinKey,
      matrixFound: Boolean(doc),
      matrixBuildId: doc?.buildId || null,
      featureScore: score.featureScore?.score,
      presentKeys: score.featureScore?.evidence?.presentKeys || [],
      featureKeysInMatrix: score.featureScore?.featureDetectionDiagnostic?.featureKeysInMatrix,
      valueScore: score.valueScore?.score,
      cityUseScore: score.cityUseScore?.score,
      premiumComfortScore: score.premiumComfortScore?.score,
      availableBuyerFeatures,
      unavailableBuyerFeatures
    });
  }

  const buyerKeyCounts = new Map();

  for (const row of rows) {
    for (const candidate of row.availableBuyerFeatures) {
      buyerKeyCounts.set(candidate.key, (buyerKeyCounts.get(candidate.key) || 0) + 1);
    }
  }

  const frequentAvailableBuyerKeys = [...buyerKeyCounts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 150)
    .map(([key, count]) => ({ key, count }));

  const report = {
    generatedAt: new Date().toISOString(),
    suspiciousCount: rows.length,
    matrixFoundCount: rows.filter((r) => r.matrixFound).length,
    matrixMissingCount: rows.filter((r) => !r.matrixFound).length,
    frequentAvailableBuyerKeys,
    samples: rows.slice(0, 50),
    fullRows: rows
  };

  fs.writeFileSync(OUT, JSON.stringify(report, null, 2));

  console.log(JSON.stringify({
    generatedAt: report.generatedAt,
    suspiciousCount: report.suspiciousCount,
    matrixFoundCount: report.matrixFoundCount,
    matrixMissingCount: report.matrixMissingCount,
    out: OUT,
    frequentAvailableBuyerKeys: report.frequentAvailableBuyerKeys.slice(0, 80),
    samples: report.samples.slice(0, 12).map((r) => ({
      variantFullName: r.variantFullName,
      featureScore: r.featureScore,
      presentKeys: r.presentKeys,
      matrixFound: r.matrixFound,
      availableBuyerFeatureKeys: r.availableBuyerFeatures.slice(0, 35).map((x) => x.key)
    }))
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
