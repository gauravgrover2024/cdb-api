#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const PRICE_COLLECTION = process.env.ACI_PRICE_ROWS_COLLECTION || 'aci_vehicle_price_rows';

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const hyphenKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const uniq = (arr) => [...new Set(arr.filter(Boolean))];

const inferTransmissionFromText = (profile) => {
  const text = [
    profile.variant,
    profile.variantKey,
    profile.variantFullName,
    profile.fuelTransmissionFamilyKey,
    profile.gearbox,
    profile.fuel,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  if (profile.fuelKey === 'electric' || text.includes('electric')) {
    return {
      transmission: 'Automatic',
      transmissionKey: 'automatic',
      reason: 'electric_vehicle_defaults_to_automatic_user_facing',
    };
  }

  if (/\b(dct|cvt|ivt|amt|at|automatic|auto)\b/i.test(text)) {
    return {
      transmission: 'Automatic',
      transmissionKey: 'automatic',
      reason: 'variant_text_contains_automatic_signal',
    };
  }

  if (/\b(mt|manual)\b/i.test(text)) {
    return {
      transmission: 'Manual',
      transmissionKey: 'manual',
      reason: 'variant_text_contains_manual_signal',
    };
  }

  return {
    transmission: null,
    transmissionKey: null,
    reason: 'no_safe_signal',
  };
};

const buildFeatureCandidateQuery = (profile) => {
  const modelKeys = uniq([
    profile.modelKey,
    normalizeKey(profile.modelKey),
    hyphenKey(profile.modelKey),
    normalizeKey(profile.model),
    hyphenKey(profile.model),
  ]);

  const variantKeys = uniq([
    profile.variantKey,
    normalizeKey(profile.variantKey),
    hyphenKey(profile.variantKey),
    normalizeKey(profile.variant),
    hyphenKey(profile.variant),
    normalizeKey(String(profile.variantKey || '').replace(/dual[-_ ]tone/g, '').trim()),
    normalizeKey(String(profile.variant || '').replace(/dual[-_ ]tone/gi, '').trim()),
  ]);

  const makeKeys = uniq([
    profile.makeKey,
    normalizeKey(profile.makeKey),
    normalizeKey(profile.make),
  ]);

  return {
    makeKeys,
    modelKeys,
    variantKeys,
    query: {
      $or: [
        { modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys } },
        { makeKey: { $in: makeKeys }, modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys } },
        { brandKey: { $in: makeKeys }, modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys } },
      ],
    },
  };
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI.');
    process.exit(1);
  }

  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;

  const profiles = db.collection(PROFILE_COLLECTION);
  const features = db.collection(FEATURE_COLLECTION);
  const prices = db.collection(PRICE_COLLECTION);

  const total = await profiles.countDocuments();

  const missingFeatureProfiles = await profiles
    .find({ 'dataQuality.hasFeatureMatrix': false })
    .project({
      _id: 0,
      variantProfileKey: 1,
      make: 1,
      makeKey: 1,
      model: 1,
      modelKey: 1,
      variant: 1,
      variantKey: 1,
      variantFullName: 1,
      fuel: 1,
      fuelKey: 1,
      transmission: 1,
      transmissionKey: 1,
      fuelTransmissionFamilyKey: 1,
      referenceExShowroomPrice: 1,
      dataQuality: 1,
    })
    .toArray();

  const featureRepairCandidates = [];

  for (const profile of missingFeatureProfiles) {
    const { query, makeKeys, modelKeys, variantKeys } = buildFeatureCandidateQuery(profile);

    const candidates = await features
      .find(query)
      .project({
        _id: 1,
        make: 1,
        makeKey: 1,
        brand: 1,
        brandKey: 1,
        model: 1,
        modelKey: 1,
        variant: 1,
        variantKey: 1,
        variantName: 1,
        activePricelistMatched: 1,
        discontinuedPricelistMatched: 1,
        featureKeys: 1,
        featuresByKey: 1,
      })
      .limit(5)
      .toArray();

    featureRepairCandidates.push({
      variantProfileKey: profile.variantProfileKey,
      variantFullName: profile.variantFullName,
      makeKey: profile.makeKey,
      modelKey: profile.modelKey,
      variantKey: profile.variantKey,
      fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,
      attemptedKeys: {
        makeKeys,
        modelKeys,
        variantKeys,
      },
      candidateCount: candidates.length,
      candidates: candidates.map((doc) => ({
        _id: String(doc._id),
        make: doc.make || doc.brand,
        makeKey: doc.makeKey || doc.brandKey,
        model: doc.model,
        modelKey: doc.modelKey,
        variant: doc.variant || doc.variantName,
        variantKey: doc.variantKey,
        activePricelistMatched: doc.activePricelistMatched,
        discontinuedPricelistMatched: doc.discontinuedPricelistMatched,
        featureKeyCount: Array.isArray(doc.featureKeys) ? doc.featureKeys.length : 0,
        hasFeaturesByKey: Boolean(doc.featuresByKey && Object.keys(doc.featuresByKey).length),
        sampleFeatureKeys: Array.isArray(doc.featureKeys) ? doc.featureKeys.slice(0, 12) : [],
      })),
    });
  }

  const unknownTransmissionProfiles = await profiles
    .find({ fuelTransmissionFamilyKey: /unknown_transmission/ })
    .project({
      _id: 0,
      variantProfileKey: 1,
      variantFullName: 1,
      makeKey: 1,
      modelKey: 1,
      variantKey: 1,
      variant: 1,
      fuel: 1,
      fuelKey: 1,
      transmission: 1,
      transmissionKey: 1,
      gearbox: 1,
      fuelTransmissionFamilyKey: 1,
    })
    .toArray();

  const unknownTransmissionRepair = unknownTransmissionProfiles.map((profile) => ({
    ...profile,
    inferred: inferTransmissionFromText(profile),
  }));

  const byModel = await profiles
    .aggregate([
      {
        $group: {
          _id: { makeKey: '$makeKey', modelKey: '$modelKey' },
          total: { $sum: 1 },
          high: { $sum: { $cond: [{ $eq: ['$dataQuality.confidenceTier', 'high'] }, 1, 0] } },
          medium: { $sum: { $cond: [{ $eq: ['$dataQuality.confidenceTier', 'medium'] }, 1, 0] } },
          missingFeatureMatrix: { $sum: { $cond: ['$dataQuality.hasFeatureMatrix', 0, 1] } },
          unknownTransmission: {
            $sum: {
              $cond: [
                { $regexMatch: { input: '$fuelTransmissionFamilyKey', regex: /unknown_transmission/ } },
                1,
                0,
              ],
            },
          },
        },
      },
      { $sort: { missingFeatureMatrix: -1, unknownTransmission: -1, total: -1 } },
    ])
    .toArray();

  const priceCityCoverage = await prices
    .aggregate([
      {
        $group: {
          _id: '$citySlug',
          rows: { $sum: 1 },
          uniqueModels: { $addToSet: '$modelKey' },
        },
      },
      {
        $project: {
          citySlug: '$_id',
          rows: 1,
          uniqueModelCount: { $size: '$uniqueModels' },
          _id: 0,
        },
      },
      { $sort: { rows: -1 } },
    ])
    .toArray();

  const output = {
    totalProfiles: total,
    missingFeatureProfiles: missingFeatureProfiles.length,
    featureRepairCandidateSummary: {
      withCandidates: featureRepairCandidates.filter((x) => x.candidateCount > 0).length,
      withoutCandidates: featureRepairCandidates.filter((x) => x.candidateCount === 0).length,
    },
    unknownTransmissionCount: unknownTransmissionProfiles.length,
    unknownTransmissionRepair,
    byModel,
    priceCityCoverage,
    featureRepairCandidates,
  };

  console.log(JSON.stringify(output, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
