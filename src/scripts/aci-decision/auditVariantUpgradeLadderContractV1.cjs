#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const LADDER_COLLECTION =
  process.env.ACI_VARIANT_UPGRADE_LADDER_COLLECTION || 'aci_vehicle_variant_upgrade_ladder';

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const MIN_EXPECTED_ROWS = Number(process.env.ACI_VARIANT_UPGRADE_LADDER_MIN_COUNT || 1800);

const REQUIRED_FIELDS = [
  'ladderKey',
  'variantProfileKey',
  'make',
  'makeKey',
  'model',
  'modelKey',
  'variantFullName',
  'brandModelKey',
  'fuel',
  'fuelKey',
  'transmission',
  'transmissionKey',
  'fuelTransmissionFamilyKey',
  'groupKey',
  'priceRank',
  'equipmentRank',
  'totalInGroup',
  'structuralRole',
  'featureCount',
  'knownFeatureCount',
  'skipInUpgradeLadder',
  'upgradeEdgeQuality',
  'upgradeEdgeNeedsReview',
  'evidence.method',
  'evidence.noCarJudgementHardcoded',
  'evidence.priceSource',
  'evidence.featureSource',
  'evidence.upgradeEdgeRule',
  'evidence.upgradeTargetRule',
  'sourceVersion',
];

const ARRAY_FIELDS = [
  'gainedFeatureKeys',
  'lostFeatureKeys',
  'newlyKnownFeatureKeys',
  'nextPricedVariantGainedFeatureKeys',
  'nextPricedVariantLostFeatureKeys',
  'nextMeaningfulUpgradeGainedFeatureKeys',
];

const FORBIDDEN_FIELDS = [
  'scores',
  'recommendationScore',
  'buyerFit',
  'decisionPolicy',
  'usageGuardrail',
  'scoreReadiness',
  'canUseForFinalRecommendation',
];

const SAFE_EDGE_QUALITIES = new Set([
  'single_variant',
  'clean',
  'caveated_next_priced_variant_has_losses',
  'needs_feature_evidence',
  'needs_review_no_clear_gain',
  'caveated',
  'no_higher_variant',
]);

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const missingPathQuery = (path) => ({
  $or: [
    { [path]: { $exists: false } },
    { [path]: null },
    { [path]: '' },
  ],
});

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;
  const col = db.collection(LADDER_COLLECTION);

  const total = await col.countDocuments();

  const missingRequiredFields = [];
  for (const field of REQUIRED_FIELDS) {
    const count = await col.countDocuments(missingPathQuery(field));
    if (count > 0) missingRequiredFields.push({ field, count });
  }

  const nonArrayFields = [];
  for (const field of ARRAY_FIELDS) {
    const count = await col.countDocuments({
      [field]: { $exists: true, $not: { $type: 'array' } },
    });
    if (count > 0) nonArrayFields.push({ field, count });
  }

  const duplicateLadderKeys = await col.aggregate([
    { $group: { _id: '$ladderKey', count: { $sum: 1 } } },
    { $match: { _id: { $ne: null }, count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const duplicateVariantProfileKeys = await col.aggregate([
    { $group: { _id: '$variantProfileKey', count: { $sum: 1 } } },
    { $match: { _id: { $ne: null }, count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const orphanVariantProfiles = await col.aggregate([
    {
      $lookup: {
        from: PROFILE_COLLECTION,
        localField: 'variantProfileKey',
        foreignField: 'variantProfileKey',
        as: 'profile',
      },
    },
    { $match: { profile: { $size: 0 } } },
    {
      $project: {
        _id: 0,
        ladderKey: 1,
        variantProfileKey: 1,
        groupKey: 1,
      },
    },
    { $limit: 20 },
  ]).toArray();

  const nextPricedMismatches = await col.aggregate([
    {
      $match: {
        nextPricedVariantProfileKey: { $exists: true, $ne: null, $ne: '' },
      },
    },
    {
      $lookup: {
        from: PROFILE_COLLECTION,
        localField: 'nextPricedVariantProfileKey',
        foreignField: 'variantProfileKey',
        as: 'nextProfile',
      },
    },
    { $unwind: '$nextProfile' },
    {
      $match: {
        $expr: {
          $or: [
            { $ne: ['$brandModelKey', '$nextProfile.brandModelKey'] },
            { $ne: ['$fuelTransmissionFamilyKey', '$nextProfile.fuelTransmissionFamilyKey'] },
          ],
        },
      },
    },
    {
      $project: {
        _id: 0,
        ladderKey: 1,
        variantProfileKey: 1,
        nextPricedVariantProfileKey: 1,
        brandModelKey: 1,
        nextBrandModelKey: '$nextProfile.brandModelKey',
        fuelTransmissionFamilyKey: 1,
        nextFuelTransmissionFamilyKey: '$nextProfile.fuelTransmissionFamilyKey',
      },
    },
    { $limit: 20 },
  ]).toArray();

  const nextMeaningfulMismatches = await col.aggregate([
    {
      $match: {
        nextMeaningfulUpgradeVariantProfileKey: { $exists: true, $ne: null, $ne: '' },
      },
    },
    {
      $lookup: {
        from: PROFILE_COLLECTION,
        localField: 'nextMeaningfulUpgradeVariantProfileKey',
        foreignField: 'variantProfileKey',
        as: 'nextProfile',
      },
    },
    { $unwind: '$nextProfile' },
    {
      $match: {
        $expr: {
          $or: [
            { $ne: ['$brandModelKey', '$nextProfile.brandModelKey'] },
            { $ne: ['$fuelTransmissionFamilyKey', '$nextProfile.fuelTransmissionFamilyKey'] },
          ],
        },
      },
    },
    {
      $project: {
        _id: 0,
        ladderKey: 1,
        variantProfileKey: 1,
        nextMeaningfulUpgradeVariantProfileKey: 1,
        brandModelKey: 1,
        nextBrandModelKey: '$nextProfile.brandModelKey',
        fuelTransmissionFamilyKey: 1,
        nextFuelTransmissionFamilyKey: '$nextProfile.fuelTransmissionFamilyKey',
      },
    },
    { $limit: 20 },
  ]).toArray();

  const negativeNextPricedDeltas = await col.countDocuments({
    nextPricedVariantProfileKey: { $exists: true, $ne: null, $ne: '' },
    globalExShowroomDelta: { $exists: true, $lt: 0 },
  });

  const negativeNextUpgradeRanks = await col.countDocuments({
    upgradeEdgeQuality: { $ne: 'no_higher_variant' },
    nextUpgradeVariantProfileKey: { $exists: true, $ne: null, $ne: '' },
    $expr: {
      $and: [
        { $ne: ['$nextUpgradeVariantProfileKey', '$variantProfileKey'] },
        { $lte: ['$nextUpgradePriceRank', '$priceRank'] },
      ],
    },
  });

  const unsafeEdgeQualities = await col.aggregate([
    { $group: { _id: '$upgradeEdgeQuality', count: { $sum: 1 } } },
    { $match: { _id: { $nin: [...SAFE_EDGE_QUALITIES] } } },
  ]).toArray();

  const edgeQualityCounts = await col.aggregate([
    { $group: { _id: '$upgradeEdgeQuality', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const forbiddenFieldCounts = [];
  for (const field of FORBIDDEN_FIELDS) {
    const count = await col.countDocuments({ [field]: { $exists: true } });
    if (count > 0) forbiddenFieldCounts.push({ field, count });
  }

  const finalRecommendationFlags = await col.countDocuments({
    $or: [
      { canUseForFinalRecommendation: true },
      { 'decisionPolicy.canUseForFinalRecommendation': true },
      { 'usageGuardrail.canUseForFinalRecommendation': true },
      { 'scoreReadiness.finalOverallScoreReady': true },
    ],
  });

  const missingMetadata = await col.countDocuments({
    $or: [
      { buildVersion: { $exists: false } },
      { buildVersion: null },
      { buildVersion: '' },
      { builtAt: { $exists: false } },
      { builtAt: null },
      { builtAt: '' },
      { sourceCollections: { $exists: false } },
      { sourceCollections: null },
      { sourceCollections: { $size: 0 } },
    ],
  });

  const indexes = await col.indexes();

  const hasUniqueLadderKeyIndex = indexes.some(
    (idx) => idx.unique === true && JSON.stringify(idx.key) === JSON.stringify({ ladderKey: 1 })
  );

  const hasVariantProfileIndex = indexes.some(
    (idx) => JSON.stringify(idx.key) === JSON.stringify({ variantProfileKey: 1 })
  );

  const sample = await col.findOne({}, { projection: { _id: 0 } });

  const failures = [];

  if (total < MIN_EXPECTED_ROWS) {
    failures.push(`expected at least ${MIN_EXPECTED_ROWS} upgrade ladder rows, got ${total}`);
  }

  if (missingRequiredFields.length > 0) {
    failures.push(`missing required fields: ${JSON.stringify(missingRequiredFields.slice(0, 30))}`);
  }

  if (nonArrayFields.length > 0) {
    failures.push(`feature gain/loss fields must be arrays: ${JSON.stringify(nonArrayFields)}`);
  }

  if (duplicateLadderKeys.length > 0) {
    failures.push(`duplicate ladderKey rows found: ${duplicateLadderKeys.length}`);
  }

  if (duplicateVariantProfileKeys.length > 0) {
    failures.push(`duplicate variantProfileKey rows found: ${duplicateVariantProfileKeys.length}`);
  }

  if (orphanVariantProfiles.length > 0) {
    failures.push(`upgrade ladder rows without matching variant profile found: ${orphanVariantProfiles.length}`);
  }

  if (nextPricedMismatches.length > 0) {
    failures.push(`nextPricedVariant crosses model/fuel/transmission boundary: ${JSON.stringify(nextPricedMismatches)}`);
  }

  if (nextMeaningfulMismatches.length > 0) {
    failures.push(`nextMeaningfulUpgrade crosses model/fuel/transmission boundary: ${JSON.stringify(nextMeaningfulMismatches)}`);
  }

  if (negativeNextPricedDeltas > 0) {
    failures.push(`next priced variant has negative global ex-showroom delta in ${negativeNextPricedDeltas} row(s)`);
  }

  if (negativeNextUpgradeRanks > 0) {
    failures.push(`next upgrade rank is not above current price rank in ${negativeNextUpgradeRanks} row(s)`);
  }

  if (unsafeEdgeQualities.length > 0) {
    failures.push(`unsafe upgradeEdgeQuality values: ${JSON.stringify(unsafeEdgeQualities)}`);
  }

  if (forbiddenFieldCounts.length > 0) {
    failures.push(`decision/recommendation fields leaked into upgrade ladder: ${JSON.stringify(forbiddenFieldCounts)}`);
  }

  if (finalRecommendationFlags > 0) {
    failures.push(`final recommendation flags found in ${finalRecommendationFlags} row(s)`);
  }

  if (missingMetadata > 0) {
    failures.push(`buildVersion/builtAt/sourceCollections missing in ${missingMetadata} row(s)`);
  }

  if (!hasUniqueLadderKeyIndex) {
    failures.push('missing unique index on ladderKey');
  }

  if (!hasVariantProfileIndex) {
    failures.push('missing index on variantProfileKey');
  }

  const summary = {
    suite: 'ACI Variant Upgrade Ladder Contract Audit v1',
    ok: failures.length === 0,
    collection: LADDER_COLLECTION,
    total,
    minExpectedRows: MIN_EXPECTED_ROWS,
    edgeQualityCounts,
    missingRequiredFields,
    nonArrayFields,
    duplicateLadderKeyCount: duplicateLadderKeys.length,
    duplicateVariantProfileKeyCount: duplicateVariantProfileKeys.length,
    orphanVariantProfileCount: orphanVariantProfiles.length,
    orphanVariantProfiles,
    nextPricedMismatchCount: nextPricedMismatches.length,
    nextPricedMismatches,
    nextMeaningfulMismatchCount: nextMeaningfulMismatches.length,
    nextMeaningfulMismatches,
    negativeNextPricedDeltas,
    negativeNextUpgradeRanks,
    unsafeEdgeQualities,
    forbiddenFieldCounts,
    finalRecommendationFlags,
    missingMetadata,
    indexes: indexes.map((idx) => ({
      name: idx.name,
      key: idx.key,
      unique: idx.unique === true,
    })),
    sample,
    failures,
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
