#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const LADDER_COLLECTION = process.env.ACI_VARIANT_UPGRADE_LADDER_COLLECTION || 'aci_vehicle_variant_upgrade_ladder';
const TARGET_COLLECTION = process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const args = process.argv.slice(2);
const write = args.includes('--write');
const reset = args.includes('--reset');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const now = () => new Date();

const isLikelyMainstream = (profile) => {
  const make = String(profile.makeKey || '').toLowerCase();
  const model = String(profile.modelKey || '').toLowerCase();

  const mainstreamMakes = new Set([
    'maruti', 'hyundai', 'kia', 'tata', 'mahindra', 'honda', 'toyota',
    'skoda', 'volkswagen', 'renault', 'nissan', 'mg', 'citroen'
  ]);

  if (mainstreamMakes.has(make)) return true;
  if (/(creta|seltos|nexon|venue|brezza|sonet|scorpio|thar|xuv|harrier|safari|city|amaze|baleno|fronx|swift|wagon|ertiga|innova|fortuner|virtus|slavia|taigun|kushaq)/i.test(model)) return true;

  return false;
};

const gapPriority = (profile, gapType) => {
  if (gapType === 'crash_rating_missing' && isLikelyMainstream(profile)) return 'P0';
  if (gapType === 'feature_matrix_missing' && isLikelyMainstream(profile)) return 'P0';
  if (gapType === 'unknown_transmission' && isLikelyMainstream(profile)) return 'P0';
  if (gapType === 'upgrade_edge_needs_review' && isLikelyMainstream(profile)) return 'P1';

  if (gapType === 'crash_rating_missing') return 'P1';
  if (gapType === 'feature_matrix_missing') return 'P2';
  if (gapType === 'unknown_transmission') return 'P2';
  if (gapType.endsWith('_missing')) return 'P2';
  if (gapType === 'upgrade_edge_needs_review') return 'P2';

  return 'P3';
};

const sourcePlanForGap = (gapType) => {
  if (gapType === 'crash_rating_missing') {
    return [
      'Bharat NCAP / Global NCAP official result where available',
      'OEM safety brochure only for variant applicability',
      'Do not apply rating to all variants unless source explicitly supports it'
    ];
  }

  if (gapType === 'feature_matrix_missing') {
    return [
      'Official OEM brochure/spec page first',
      'Cardekho/CarWale secondary only if OEM is unavailable',
      'Store raw source URL, extraction date, and field-level evidence'
    ];
  }

  if (gapType === 'unknown_transmission') {
    return [
      'Official OEM brochure/spec page',
      'Use variant name inference only when obvious: MT/manual, AT/CVT/DCT/AMT/IVT, EV automatic',
      'Otherwise keep unknown'
    ];
  }

  if (gapType === 'upgrade_edge_needs_review') {
    return [
      'Review feature evidence for both variants',
      'Confirm whether apparent lost features are true, naming mismatch, or feature extraction issue',
      'Do not use as confident buyer advice until reviewed'
    ];
  }

  return [
    'Official OEM source first',
    'Trusted automotive portal secondary',
    'Keep field-level evidence and confidence'
  ];
};

const makeGap = ({ profile, gapType, evidence = {}, notes = '' }) => ({
  gapKey: `${profile.variantProfileKey}__${gapType}`,
  variantProfileKey: profile.variantProfileKey,
  variantFullName: profile.variantFullName,
  make: profile.make,
  makeKey: profile.makeKey,
  model: profile.model,
  modelKey: profile.modelKey,
  brandModelKey: profile.brandModelKey,
  variant: profile.variant,
  variantKey: profile.variantKey,
  fuel: profile.fuel,
  fuelKey: profile.fuelKey,
  transmission: profile.transmission,
  transmissionKey: profile.transmissionKey,
  fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,
  gapType,
  priority: gapPriority(profile, gapType),
  status: 'open',
  sourcePlan: sourcePlanForGap(gapType),
  evidence,
  notes,
  sourceVersion: 'aci_variant_data_gap_queue_v1_2026_05_31',
  createdAt: now(),
  updatedAt: now(),
});

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

  const profilesCol = db.collection(PROFILE_COLLECTION);
  const ladderCol = db.collection(LADDER_COLLECTION);
  const target = db.collection(TARGET_COLLECTION);

  const profiles = await profilesCol.find({}, {
    projection: {
      _id: 0,
      variantProfileKey: 1,
      variantFullName: 1,
      make: 1,
      makeKey: 1,
      model: 1,
      modelKey: 1,
      brandModelKey: 1,
      variant: 1,
      variantKey: 1,
      fuel: 1,
      fuelKey: 1,
      transmission: 1,
      transmissionKey: 1,
      fuelTransmissionFamilyKey: 1,
      dataQuality: 1,
      safetyBasis: 1,
      performanceBasis: 1,
      mileageBasis: 1,
      practicalityBasis: 1,
      comfortBasis: 1,
      scores: 1
    }
  }).toArray();

  const ladderDocs = await ladderCol.find({}, {
    projection: {
      _id: 0,
      variantProfileKey: 1,
      upgradeEdgeNeedsReview: 1,
      upgradeEdgeQuality: 1,
      nextPricedVariantFullName: 1,
      nextMeaningfulUpgradeVariantFullName: 1,
      nextUpgradeVariantFullName: 1,
      gainedFeatureKeys: 1,
      lostFeatureKeys: 1
    }
  }).toArray();

  const ladderByVariant = new Map(ladderDocs.map((doc) => [doc.variantProfileKey, doc]));

  const gaps = [];

  for (const profile of profiles) {
    const dq = profile.dataQuality || {};
    const safety = profile.safetyBasis || {};
    const perf = profile.performanceBasis || {};
    const mileage = profile.mileageBasis || {};
    const practical = profile.practicalityBasis || {};
    const comfort = profile.comfortBasis || {};

    if (dq.hasFeatureMatrix !== true) {
      gaps.push(makeGap({
        profile,
        gapType: 'feature_matrix_missing',
        evidence: { hasFeatureMatrix: dq.hasFeatureMatrix },
        notes: 'No internal feature matrix candidate found during repair.'
      }));
    }

    if (!profile.transmissionKey || String(profile.fuelTransmissionFamilyKey || '').includes('unknown_transmission')) {
      gaps.push(makeGap({
        profile,
        gapType: 'unknown_transmission',
        evidence: {
          transmission: profile.transmission || null,
          transmissionKey: profile.transmissionKey || null,
          fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey || null
        }
      }));
    }

    if (!safety.crashRatingAppliesToVariant && !safety.crashRatingAppliesToAllVariants) {
      gaps.push(makeGap({
        profile,
        gapType: 'crash_rating_missing',
        evidence: {
          crashRatingAppliesToVariant: safety.crashRatingAppliesToVariant || null,
          crashRatingAppliesToAllVariants: safety.crashRatingAppliesToAllVariants || null
        }
      }));
    }

    if (!perf.powerBhp || !perf.torqueNm) {
      gaps.push(makeGap({
        profile,
        gapType: 'performance_specs_missing',
        evidence: {
          powerBhp: perf.powerBhp || null,
          torqueNm: perf.torqueNm || null
        }
      }));
    }

    if (!mileage.araiMileage && !mileage.evClaimedRange) {
      gaps.push(makeGap({
        profile,
        gapType: 'mileage_specs_missing',
        evidence: {
          araiMileage: mileage.araiMileage || null,
          evClaimedRange: mileage.evClaimedRange || null
        }
      }));
    }

    if (!practical.seatingCapacity || !practical.lengthMm || !practical.widthMm) {
      gaps.push(makeGap({
        profile,
        gapType: 'dimensions_missing',
        evidence: {
          seatingCapacity: practical.seatingCapacity || null,
          lengthMm: practical.lengthMm || null,
          widthMm: practical.widthMm || null
        }
      }));
    }

    const ladder = ladderByVariant.get(profile.variantProfileKey);
    if (ladder?.upgradeEdgeNeedsReview === true) {
      gaps.push(makeGap({
        profile,
        gapType: 'upgrade_edge_needs_review',
        evidence: {
          upgradeEdgeQuality: ladder.upgradeEdgeQuality,
          nextPricedVariantFullName: ladder.nextPricedVariantFullName,
          nextMeaningfulUpgradeVariantFullName: ladder.nextMeaningfulUpgradeVariantFullName,
          nextUpgradeVariantFullName: ladder.nextUpgradeVariantFullName,
          gainedFeatureKeys: ladder.gainedFeatureKeys || [],
          lostFeatureKeys: ladder.lostFeatureKeys || []
        }
      }));
    }
  }

  const duplicateGapKeys = gaps.reduce((acc, gap) => {
    acc[gap.gapKey] = (acc[gap.gapKey] || 0) + 1;
    return acc;
  }, {});

  const duplicateGapKeyCount = Object.values(duplicateGapKeys).filter((count) => count > 1).length;

  let writeResult = null;

  if (write) {
    if (reset) {
      await target.deleteMany({});
      console.log(`[reset] cleared ${TARGET_COLLECTION}`);
    }

    await safeCreateIndex(target, { gapKey: 1 }, { unique: true, name: 'gap_key_unique' });
    await safeCreateIndex(target, { status: 1, priority: 1, gapType: 1 }, { name: 'gap_status_priority_type_idx' });
    await safeCreateIndex(target, { variantProfileKey: 1, gapType: 1 }, { name: 'gap_variant_type_idx' });
    await safeCreateIndex(target, { makeKey: 1, modelKey: 1, priority: 1 }, { name: 'gap_make_model_priority_idx' });

    let upserted = 0;
    let modified = 0;
    let bulk = [];

    for (const gap of gaps) {
      const { createdAt, ...setDoc } = gap;

      bulk.push({
        updateOne: {
          filter: { gapKey: gap.gapKey },
          update: {
            $set: setDoc,
            $setOnInsert: { createdAt },
          },
          upsert: true,
        },
      });

      if (bulk.length >= 500) {
        const result = await target.bulkWrite(bulk, { ordered: false });
        upserted += result.upsertedCount || 0;
        modified += result.modifiedCount || 0;
        bulk = [];
      }
    }

    if (bulk.length) {
      const result = await target.bulkWrite(bulk, { ordered: false });
      upserted += result.upsertedCount || 0;
      modified += result.modifiedCount || 0;
    }

    writeResult = { upserted, modified };
  }

  const byType = gaps.reduce((acc, gap) => {
    acc[gap.gapType] = (acc[gap.gapType] || 0) + 1;
    return acc;
  }, {});

  const byPriority = gaps.reduce((acc, gap) => {
    acc[gap.priority] = (acc[gap.priority] || 0) + 1;
    return acc;
  }, {});

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    profiles: profiles.length,
    ladderDocs: ladderDocs.length,
    gaps: gaps.length,
    duplicateGapKeyCount,
    byType,
    byPriority,
    samples: gaps.slice(0, 30).map((gap) => ({
      gapKey: gap.gapKey,
      variantFullName: gap.variantFullName,
      gapType: gap.gapType,
      priority: gap.priority,
      status: gap.status,
      evidence: gap.evidence
    })),
    writeResult
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
