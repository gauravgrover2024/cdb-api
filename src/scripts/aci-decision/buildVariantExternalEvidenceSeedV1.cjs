#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const {
  isInactiveDecisionProfile,
} = require('../../services/aciCore/lifecycle/aciVehicleLifecycle.cjs');

const GAP_COLLECTION = process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const TARGET_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';

const args = process.argv.slice(2);
const write = args.includes('--write');
const reset = args.includes('--reset');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const now = () => new Date();

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

const evidenceTypeForGap = (gapType) => {
  if (gapType === 'unknown_transmission') return 'transmission_spec';
  if (gapType === 'feature_matrix_missing') return 'feature_matrix';
  if (gapType === 'performance_specs_missing') return 'performance_spec';
  if (gapType === 'mileage_specs_missing') return 'mileage_spec';
  if (gapType === 'dimensions_missing') return 'dimension_spec';
  if (gapType === 'crash_rating_missing') return 'crash_rating';
  if (gapType === 'upgrade_edge_needs_review') return 'upgrade_edge_review';
  return 'unknown';
};

const sourcePriorityForGap = (gapType) => {
  if (gapType === 'crash_rating_missing') {
    return ['bharat_ncap', 'global_ncap', 'oem_brochure', 'trusted_portal'];
  }

  return ['oem_brochure', 'oem_spec_page', 'trusted_portal'];
};

const buildSeed = (gap) => ({
  evidenceKey: `${gap.gapKey}__seed`,
  gapKey: gap.gapKey,
  gapType: gap.gapType,
  evidenceType: evidenceTypeForGap(gap.gapType),

  variantProfileKey: gap.variantProfileKey,
  variantFullName: gap.variantFullName,
  make: gap.make,
  makeKey: gap.makeKey,
  model: gap.model,
  modelKey: gap.modelKey,
  brandModelKey: gap.brandModelKey,
  variant: gap.variant,
  variantKey: gap.variantKey,
  fuel: gap.fuel,
  fuelKey: gap.fuelKey,
  transmission: gap.transmission,
  transmissionKey: gap.transmissionKey,
  fuelTransmissionFamilyKey: gap.fuelTransmissionFamilyKey,

  priority: gap.priority,
  status: 'needs_source',
  reviewStatus: 'not_started',

  sourcePriority: sourcePriorityForGap(gap.gapType),
  sourceName: null,
  sourceType: null,
  sourceUrl: null,
  sourceFetchedAt: null,

  rawExtractedData: null,
  normalizedFields: null,
  fieldEvidence: [],
  confidence: 'none',
  applicabilityScope: null,
  applicabilityNotes: null,

  notes: 'Seeded from aci_variant_data_gap_queue. Do not patch decision profiles until evidence is reviewed.',
  sourceVersion: 'aci_variant_external_evidence_seed_v1_2026_05_31',
  createdAt: now(),
  updatedAt: now(),
});

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const gaps = db.collection(GAP_COLLECTION);
  const profiles = db.collection(PROFILE_COLLECTION);
  const target = db.collection(TARGET_COLLECTION);

  const sourceGapsRaw = await gaps.find({
    status: 'open',
    priority: 'P0',
    gapType: { $in: ['feature_matrix_missing', 'unknown_transmission'] }
  }).sort({ makeKey: 1, modelKey: 1, variantKey: 1, gapType: 1 }).toArray();

  const profileKeys = [...new Set(sourceGapsRaw.map((gap) => gap.variantProfileKey).filter(Boolean))];
  const inactiveProfileKeys = new Set();

  if (profileKeys.length) {
    const profileCursor = profiles.find(
      { variantProfileKey: { $in: profileKeys } },
      {
        projection: {
          _id: 0,
          variantProfileKey: 1,
          lifecycleStatus: 1,
          dataStatus: 1,
        },
      },
    );

    for await (const profile of profileCursor) {
      if (isInactiveDecisionProfile(profile)) {
        inactiveProfileKeys.add(profile.variantProfileKey);
      }
    }
  }

  const sourceGaps = sourceGapsRaw.filter(
    (gap) =>
      !isInactiveDecisionProfile(gap) &&
      !inactiveProfileKeys.has(gap.variantProfileKey),
  );

  const seeds = sourceGaps.map(buildSeed);

  const duplicateKeys = seeds.reduce((acc, seed) => {
    acc[seed.evidenceKey] = (acc[seed.evidenceKey] || 0) + 1;
    return acc;
  }, {});

  const duplicateEvidenceKeyCount = Object.values(duplicateKeys).filter((count) => count > 1).length;

  let writeResult = null;

  if (write) {
    if (reset) {
      await target.deleteMany({
        sourceVersion: 'aci_variant_external_evidence_seed_v1_2026_05_31',
        status: 'needs_source'
      });
      console.log(`[reset] cleared seeded needs_source evidence from ${TARGET_COLLECTION}`);
    }

    await safeCreateIndex(target, { evidenceKey: 1 }, { unique: true, name: 'external_evidence_key_unique' });
    await safeCreateIndex(target, { status: 1, reviewStatus: 1, priority: 1, evidenceType: 1 }, { name: 'external_evidence_work_queue_idx' });
    await safeCreateIndex(target, { variantProfileKey: 1, evidenceType: 1 }, { name: 'external_evidence_variant_type_idx' });
    await safeCreateIndex(target, { makeKey: 1, modelKey: 1, priority: 1 }, { name: 'external_evidence_make_model_priority_idx' });

    if (seeds.length) {
      const result = await target.bulkWrite(seeds.map((seed) => {
        const { createdAt, ...setDoc } = seed;
        return {
          updateOne: {
            filter: { evidenceKey: seed.evidenceKey },
            update: {
              $set: setDoc,
              $setOnInsert: { createdAt },
            },
            upsert: true,
          },
        };
      }), { ordered: false });

      writeResult = {
        upserted: result.upsertedCount || 0,
        modified: result.modifiedCount || 0,
      };
    } else {
      writeResult = { upserted: 0, modified: 0 };
    }
  }

  const byType = seeds.reduce((acc, seed) => {
    acc[seed.evidenceType] = (acc[seed.evidenceType] || 0) + 1;
    return acc;
  }, {});

  const byModel = seeds.reduce((acc, seed) => {
    const key = `${seed.makeKey}_${seed.modelKey}`;
    acc[key] = (acc[key] || 0) + 1;
    return acc;
  }, {});

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    sourceGaps: sourceGaps.length,
    skippedInactiveSourceGaps: sourceGapsRaw.length - sourceGaps.length,
    seeds: seeds.length,
    duplicateEvidenceKeyCount,
    byType,
    byModel,
    samples: seeds.slice(0, 50).map((seed) => ({
      evidenceKey: seed.evidenceKey,
      variantFullName: seed.variantFullName,
      evidenceType: seed.evidenceType,
      priority: seed.priority,
      status: seed.status,
      sourcePriority: seed.sourcePriority,
    })),
    writeResult,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
