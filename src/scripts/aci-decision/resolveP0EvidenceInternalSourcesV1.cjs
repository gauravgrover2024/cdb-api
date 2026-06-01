#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const EVIDENCE_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';
const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const PRICE_COLLECTION = process.env.ACI_PRICE_ROWS_COLLECTION || 'aci_vehicle_price_rows';

const args = process.argv.slice(2);
const write = args.includes('--write');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const exactVariantKeyMatch = ({ seedVariantKey, seedVariant, candidateVariantKey, candidateVariant }) => {
  const seedKey = normalizeKey(seedVariantKey || seedVariant);
  const candidateKey = normalizeKey(candidateVariantKey || candidateVariant);

  if (!seedKey || !candidateKey) return false;

  // Exact variant suffix match:
  // seed n6 -> feature i20_n_line_n6 ✅
  // seed n6 -> feature i20_n_line_n6_dct ❌
  // seed n6_dual_tone -> feature i20_n_line_n6_dual_tone ✅
  return candidateKey === seedKey || candidateKey.endsWith(`_${seedKey}`);
};

const inferTransmissionFromVariantName = (variant) => {
  const v = String(variant || '').toLowerCase();

  if (/\b(dct|dsg|cvt|ivt|amt)\b/i.test(v)) {
    return { transmission: 'Automatic', transmissionKey: 'automatic', confidence: 'high', reason: 'explicit automatic gearbox token' };
  }

  if (/\b(at|automatic)\b/i.test(v)) {
    return { transmission: 'Automatic', transmissionKey: 'automatic', confidence: 'high', reason: 'explicit automatic token' };
  }

  if (/\b(mt|manual)\b/i.test(v)) {
    return { transmission: 'Manual', transmissionKey: 'manual', confidence: 'high', reason: 'explicit manual token' };
  }

  return null;
};

const safeCreateIndex = async (collection, keys, options = {}) => {
  let indexes = [];
  try {
    indexes = await collection.indexes();
  } catch (error) {
    if (error?.code !== 26 && error?.codeName !== 'NamespaceNotFound') throw error;
  }

  const wanted = JSON.stringify(keys);
  if (indexes.some((idx) => JSON.stringify(idx.key) === wanted)) return;

  const name = options.name || Object.entries(keys).map(([k, v]) => `${k}_${v}`).join('_');
  await collection.createIndex(keys, { ...options, name });
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const evidenceCol = db.collection(EVIDENCE_COLLECTION);
  const featureCol = db.collection(FEATURE_COLLECTION);
  const priceCol = db.collection(PRICE_COLLECTION);

  const seeds = await evidenceCol.find({
    priority: 'P0',
    status: { $in: ['needs_source', 'needs_external_source'] },
    evidenceType: { $in: ['feature_matrix', 'transmission_spec'] },
  }, {
    projection: {
      _id: 0,
      evidenceKey: 1,
      gapKey: 1,
      gapType: 1,
      evidenceType: 1,
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
      fuelTransmissionFamilyKey: 1,
    }
  }).sort({ makeKey: 1, modelKey: 1, variantKey: 1, evidenceType: 1 }).toArray();

  const results = [];

  for (const seed of seeds) {
    const modelKeys = [...new Set([normalizeKey(seed.modelKey), normalizeKey(seed.model)].filter(Boolean))];
    const makeKeys = [...new Set([normalizeKey(seed.makeKey), normalizeKey(seed.make)].filter(Boolean))];

    const featureDocs = await featureCol.find({
      $and: [
        {
          $or: [
            { modelKey: { $in: modelKeys } },
            { model: seed.model },
          ],
        },
        {
          $or: [
            { makeKey: { $in: makeKeys } },
            { brandKey: { $in: makeKeys } },
            { make: seed.make },
            { brand: seed.make },
          ],
        },
      ],
    }, {
      projection: {
        _id: 1,
        make: 1,
        makeKey: 1,
        brand: 1,
        brandKey: 1,
        model: 1,
        modelKey: 1,
        variant: 1,
        variantName: 1,
        variantKey: 1,
        fuel: 1,
        fuelKey: 1,
        transmission: 1,
        transmissionKey: 1,
        gearbox: 1,
        activePricelistMatched: 1,
        featureKeys: 1,
        featuresByKey: 1,
      },
    }).limit(300).toArray();

    const exactFeatureMatches = featureDocs.filter((doc) =>
      exactVariantKeyMatch({
        seedVariantKey: seed.variantKey,
        seedVariant: seed.variant,
        candidateVariantKey: doc.variantKey,
        candidateVariant: doc.variant || doc.variantName,
      })
    );

    const priceRows = await priceCol.find({
      $and: [
        {
          $or: [
            { makeKey: normalizeKey(seed.makeKey) },
            { make: seed.make },
          ],
        },
        {
          $or: [
            { modelKey: seed.modelKey },
            { modelKey: normalizeKey(seed.modelKey) },
            { model: seed.model },
          ],
        },
        {
          $or: [
            { variantKey: seed.variantKey },
            { variantKey: normalizeKey(seed.variantKey) },
            { variant: seed.variant },
          ],
        },
      ],
    }, {
      projection: {
        _id: 1,
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
        gearbox: 1,
        citySlug: 1,
        exShowroomPrice: 1,
      },
    }).limit(20).toArray();

    let status = 'needs_external_source';
    let reviewStatus = 'internal_unresolved';
    let confidence = 'none';
    let normalizedFields = {};
    let sourceName = null;
    let sourceType = null;
    let notes = '';

    if (seed.evidenceType === 'feature_matrix') {
      if (exactFeatureMatches.length === 1) {
        const doc = exactFeatureMatches[0];
        status = 'internal_source_ready';
        reviewStatus = 'auto_exact_feature_matrix_match';
        confidence = doc.activePricelistMatched === true ? 'high' : 'medium';
        sourceName = FEATURE_COLLECTION;
        sourceType = 'internal_db';

        normalizedFields = {
          sourceFeatureDocId: String(doc._id),
          sourceFeatureVariant: doc.variant || doc.variantName || null,
          sourceFeatureVariantKey: doc.variantKey || null,
          sourceFeatureModel: doc.model || null,
          sourceFeatureModelKey: doc.modelKey || null,
          activePricelistMatched: doc.activePricelistMatched ?? null,
          featureKeyCount: Array.isArray(doc.featureKeys) ? doc.featureKeys.length : null,
          hasFeaturesByKey: Boolean(doc.featuresByKey),
        };

        notes = 'Resolved by exact variant-key suffix match against internal feature matrix. Safe for controlled profile patcher.';
      } else if (exactFeatureMatches.length > 1) {
        status = 'needs_manual_review';
        reviewStatus = 'ambiguous_exact_feature_matrix_match';
        notes = `Multiple exact internal feature matches found: ${exactFeatureMatches.length}`;
      } else {
        notes = 'No exact internal feature matrix variant-key match found.';
      }
    }

    if (seed.evidenceType === 'transmission_spec') {
      const transmissionFromPrice = priceRows.find((row) => row.transmission || row.transmissionKey || row.gearbox);
      const transmissionFromFeature = exactFeatureMatches.find((doc) => doc.transmission || doc.transmissionKey || doc.gearbox);
      const inferred = inferTransmissionFromVariantName(seed.variantFullName || seed.variant || seed.variantKey);

      if (transmissionFromPrice) {
        status = 'internal_source_ready';
        reviewStatus = 'auto_price_transmission_match';
        confidence = 'high';
        sourceName = PRICE_COLLECTION;
        sourceType = 'internal_db';
        normalizedFields = {
          transmission: transmissionFromPrice.transmission || null,
          transmissionKey: transmissionFromPrice.transmissionKey || null,
          gearbox: transmissionFromPrice.gearbox || null,
          sourcePriceRowId: String(transmissionFromPrice._id),
          citySlug: transmissionFromPrice.citySlug || null,
        };
        notes = 'Resolved from internal price row transmission fields.';
      } else if (transmissionFromFeature) {
        status = 'internal_source_ready';
        reviewStatus = 'auto_feature_transmission_match';
        confidence = 'high';
        sourceName = FEATURE_COLLECTION;
        sourceType = 'internal_db';
        normalizedFields = {
          transmission: transmissionFromFeature.transmission || null,
          transmissionKey: transmissionFromFeature.transmissionKey || null,
          gearbox: transmissionFromFeature.gearbox || null,
          sourceFeatureDocId: String(transmissionFromFeature._id),
        };
        notes = 'Resolved from internal feature matrix transmission fields.';
      } else if (inferred) {
        status = 'internal_source_ready';
        reviewStatus = 'auto_variant_name_transmission_inference';
        confidence = inferred.confidence;
        sourceName = 'variant_name';
        sourceType = 'internal_inference';
        normalizedFields = inferred;
        notes = 'Transmission inferred from explicit gearbox token in variant name. No model-specific assumption used.';
      } else {
        normalizedFields = {
          exactFeatureMatchCount: exactFeatureMatches.length,
          exactPriceRowCount: priceRows.length,
          priceRowsHaveTransmission: false,
          featureRowsHaveTransmission: false,
        };
        notes = 'Internal rows exist but do not contain transmission. Needs OEM/source verification. No hardcoded model assumption applied.';
      }
    }

    results.push({
      seed,
      status,
      reviewStatus,
      confidence,
      sourceName,
      sourceType,
      normalizedFields,
      notes,
      exactFeatureMatches: exactFeatureMatches.map((doc) => ({
        id: String(doc._id),
        variant: doc.variant || doc.variantName,
        variantKey: doc.variantKey,
        activePricelistMatched: doc.activePricelistMatched,
        featureKeyCount: Array.isArray(doc.featureKeys) ? doc.featureKeys.length : null,
      })),
      priceRows: priceRows.map((row) => ({
        id: String(row._id),
        variant: row.variant,
        variantKey: row.variantKey,
        citySlug: row.citySlug,
        transmission: row.transmission || null,
        transmissionKey: row.transmissionKey || null,
        gearbox: row.gearbox || null,
      })),
    });
  }

  const updates = results.map((r) => ({
    updateOne: {
      filter: { evidenceKey: r.seed.evidenceKey },
      update: {
        $set: {
          status: r.status,
          reviewStatus: r.reviewStatus,
          confidence: r.confidence,
          sourceName: r.sourceName,
          sourceType: r.sourceType,
          sourceFetchedAt: new Date(),
          normalizedFields: r.normalizedFields,
          notes: r.notes,
          updatedAt: new Date(),
        },
      },
    },
  }));

  let writeResult = null;

  if (write) {
    await safeCreateIndex(evidenceCol, { status: 1, reviewStatus: 1, priority: 1, evidenceType: 1 }, { name: 'external_evidence_work_queue_idx' });
    if (updates.length) {
      const result = await evidenceCol.bulkWrite(updates, { ordered: false });
      writeResult = {
        matched: result.matchedCount || 0,
        modified: result.modifiedCount || 0,
      };
    } else {
      writeResult = { matched: 0, modified: 0 };
    }
  }

  const summary = {
    mode: write ? 'WRITE' : 'DRY_RUN',
    seeds: seeds.length,
    featureMatrixSeeds: seeds.filter((s) => s.evidenceType === 'feature_matrix').length,
    transmissionSeeds: seeds.filter((s) => s.evidenceType === 'transmission_spec').length,
    internalSourceReady: results.filter((r) => r.status === 'internal_source_ready').length,
    featureMatrixReady: results.filter((r) => r.seed.evidenceType === 'feature_matrix' && r.status === 'internal_source_ready').length,
    transmissionReady: results.filter((r) => r.seed.evidenceType === 'transmission_spec' && r.status === 'internal_source_ready').length,
    needsExternalSource: results.filter((r) => r.status === 'needs_external_source').length,
    needsManualReview: results.filter((r) => r.status === 'needs_manual_review').length,
    writeResult,
  };

  console.log(JSON.stringify({
    summary,
    items: results.map((r) => ({
      evidenceKey: r.seed.evidenceKey,
      variantFullName: r.seed.variantFullName,
      evidenceType: r.seed.evidenceType,
      status: r.status,
      reviewStatus: r.reviewStatus,
      confidence: r.confidence,
      sourceName: r.sourceName,
      normalizedFields: r.normalizedFields,
      notes: r.notes,
      exactFeatureMatchCount: r.exactFeatureMatches.length,
      exactFeatureMatches: r.exactFeatureMatches,
      priceRows: r.priceRows.slice(0, 5),
    })),
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
