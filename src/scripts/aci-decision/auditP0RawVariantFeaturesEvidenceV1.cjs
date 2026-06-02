#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const EVIDENCE_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';
const RAW_FEATURE_COLLECTIONS = (process.env.ACI_RAW_VARIANT_FEATURE_COLLECTIONS || 'variant_features,vehicle_features')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const uniq = (items) => [...new Set(items.filter(Boolean))];

const getFeature = (features, names) => {
  if (!features || typeof features !== 'object') return null;

  const exact = names.find((name) => Object.prototype.hasOwnProperty.call(features, name));
  if (exact) return features[exact];

  const entries = Object.entries(features);
  const normalizedNames = names.map(normalizeKey);

  for (const [key, value] of entries) {
    const nk = normalizeKey(key);
    if (normalizedNames.some((name) => nk.endsWith(name) || nk.includes(name))) {
      return value;
    }
  }

  return null;
};

const extractRawEvidence = (doc) => {
  const features = doc.features || {};

  return {
    transmission: getFeature(features, [
      'Engine & Transmission | Transmission Type',
      'Key specifications of Hyundai i20 N-Line | Transmission Type',
      'Transmission Type',
    ]),
    gearbox: getFeature(features, [
      'Engine & Transmission | Gearbox',
      'Gearbox',
    ]),
    engineCc: getFeature(features, [
      'Engine & Transmission | Displacement',
      'Key specifications of Hyundai i20 N-Line | Engine Displacement',
      'Engine Displacement',
      'Displacement',
    ]),
    power: getFeature(features, [
      'Engine & Transmission | Max Power',
      'Key specifications of Hyundai i20 N-Line | Max Power',
      'Max Power',
    ]),
    torque: getFeature(features, [
      'Engine & Transmission | Max Torque',
      'Key specifications of Hyundai i20 N-Line | Max Torque',
      'Max Torque',
    ]),
    fuelType: getFeature(features, [
      'Fuel & Performance | Fuel Type',
      'Key specifications of Hyundai i20 N-Line | Fuel Type',
      'Fuel Type',
    ]),
    araiMileage: getFeature(features, [
      'Fuel & Performance | Petrol Mileage ARAI',
      'Fuel & Performance | ARAI Mileage',
      'Key specifications of Hyundai i20 N-Line | ARAI Mileage',
      'ARAI Mileage',
    ]),
    fuelTankCapacity: getFeature(features, [
      'Fuel & Performance | Petrol Fuel Tank Capacity',
      'Key specifications of Hyundai i20 N-Line | Fuel Tank Capacity',
      'Fuel Tank Capacity',
    ]),
    length: getFeature(features, ['Dimensions & Capacity | Length', 'Length']),
    width: getFeature(features, ['Dimensions & Capacity | Width', 'Width']),
    height: getFeature(features, ['Dimensions & Capacity | Height', 'Height']),
    bootSpace: getFeature(features, ['Dimensions & Capacity | Boot Space', 'Boot Space']),
    seatingCapacity: getFeature(features, ['Dimensions & Capacity | Seating Capacity', 'Seating Capacity']),
    wheelBase: getFeature(features, ['Dimensions & Capacity | Wheel Base', 'Wheel Base']),
    groundClearance: getFeature(features, [
      'Dimensions & Capacity | Reported Ground Clearance (Unladen)',
      'Ground Clearance',
    ]),
    featureCount: Object.keys(features).length,
  };
};

const exactVariantMatch = (seed, rawDoc) => {
  const seedVariantKey = normalizeKey(seed.variantKey || seed.variant);
  const rawVariantKey = normalizeKey(rawDoc.variant || rawDoc.variantName);
  if (!seedVariantKey || !rawVariantKey) return false;

  return rawVariantKey === seedVariantKey || rawVariantKey.endsWith(`_${seedVariantKey}`);
};

const sameModelBrand = (seed, rawDoc) => {
  const seedMake = normalizeKey(seed.make || seed.makeKey);
  const rawMake = normalizeKey(rawDoc.brand || rawDoc.make || rawDoc.brandKey || rawDoc.makeKey);

  const seedModel = normalizeKey(seed.model || seed.modelKey);
  const rawModel = normalizeKey(rawDoc.model || rawDoc.modelKey);

  return (
    (!seedMake || !rawMake || rawMake.includes(seedMake) || seedMake.includes(rawMake)) &&
    (!seedModel || !rawModel || rawModel.includes(seedModel) || seedModel.includes(rawModel))
  );
};

async function collectionExists(db, name) {
  return db.listCollections({ name }).hasNext();
}

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const evidenceCol = db.collection(EVIDENCE_COLLECTION);

  const seeds = await evidenceCol.find({
    priority: 'P0',
    status: 'needs_external_source',
    evidenceType: { $in: ['feature_matrix', 'transmission_spec'] },
  }, {
    projection: {
      _id: 0,
      evidenceKey: 1,
      evidenceType: 1,
      gapType: 1,
      variantProfileKey: 1,
      variantFullName: 1,
      make: 1,
      makeKey: 1,
      model: 1,
      modelKey: 1,
      variant: 1,
      variantKey: 1,
      fuel: 1,
      fuelKey: 1,
      transmission: 1,
      transmissionKey: 1,
    }
  }).sort({ makeKey: 1, modelKey: 1, variantKey: 1, evidenceType: 1 }).toArray();

  const rawDocsByCollection = new Map();

  const seedModels = uniq(seeds.flatMap((seed) => [seed.model, seed.modelKey]));
  const seedModelKeys = uniq(seedModels.map(normalizeKey));
  const seedMakes = uniq(seeds.flatMap((seed) => [seed.make, seed.makeKey]));
  const seedMakeKeys = uniq(seedMakes.map(normalizeKey));

  for (const collectionName of RAW_FEATURE_COLLECTIONS) {
    if (!(await collectionExists(db, collectionName))) {
      rawDocsByCollection.set(collectionName, []);
      continue;
    }

    const col = db.collection(collectionName);
    const docs = await col.find({
      $and: [
        {
          $or: [
            { model: { $in: seedModels } },
            { modelKey: { $in: seedModelKeys } },
          ],
        },
        {
          $or: [
            { brand: { $in: seedMakes } },
            { make: { $in: seedMakes } },
            { brandKey: { $in: seedMakeKeys } },
            { makeKey: { $in: seedMakeKeys } },
          ],
        },
      ],
    }, {
        projection: {
          _id: 1,
          brand: 1,
          make: 1,
          model: 1,
          variant: 1,
          features: 1,
          last_updated: 1,
          scrape_timestamp: 1,
          body_type_bucket: 1,
          seating_capacity: 1,
        }
      }).limit(Number(process.env.ACI_RAW_AUDIT_MAX_DOCS || 15000)).toArray();

    rawDocsByCollection.set(collectionName, docs);
  }

  const results = [];

  for (const seed of seeds) {
    const candidates = [];

    for (const [collectionName, docs] of rawDocsByCollection.entries()) {

      for (const doc of docs) {
        const modelBrandOk = sameModelBrand(seed, doc);
        const variantOk = exactVariantMatch(seed, doc);

        if (!modelBrandOk || !variantOk) continue;

        const extracted = extractRawEvidence(doc);

        candidates.push({
          collectionName,
          id: String(doc._id),
          brand: doc.brand || doc.make || null,
          model: doc.model || null,
          variant: doc.variant || null,
          exactVariantMatch: variantOk,
          sameModelBrand: modelBrandOk,
          extracted,
          last_updated: doc.last_updated || null,
          scrape_timestamp: doc.scrape_timestamp || null,
        });
      }
    }

    results.push({
      evidenceKey: seed.evidenceKey,
      evidenceType: seed.evidenceType,
      variantProfileKey: seed.variantProfileKey,
      variantFullName: seed.variantFullName,
      candidatesFound: candidates.length,
      usableTransmissionCandidates: candidates.filter((c) => c.extracted.transmission).length,
      usableFeatureCandidates: candidates.filter((c) => c.extracted.featureCount > 0).length,
      candidates: candidates.slice(0, 5),
    });
  }

  console.log(JSON.stringify({
    sourceCollectionsChecked: RAW_FEATURE_COLLECTIONS,
    seeds: seeds.length,
    candidatesFoundSeeds: results.filter((r) => r.candidatesFound > 0).length,
    transmissionResolvedSeeds: results.filter((r) => r.usableTransmissionCandidates > 0).length,
    featureResolvedSeeds: results.filter((r) => r.usableFeatureCandidates > 0).length,
    unresolvedSeeds: results.filter((r) => r.candidatesFound === 0).length,
    items: results,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
