#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const SCORE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const MIN_EXPECTED_ROWS = Number(process.env.ACI_VARIANT_SCORE_PROFILE_MIN_COUNT || 1800);

const REQUIRED_FIELDS = [
  'scoreProfileKey',
  'variantProfileKey',
  'variantFullName',
  'makeKey',
  'modelKey',
  'variantKey',
  'fuelKey',
  'transmissionKey',
  'fuelTransmissionFamilyKey',
  'referenceExShowroomPrice',
  'priceSegment',
  'buildVersion',
  'formulaVersion',
  'featureScoreTaxonomyVersion',
  'featureScoreTaxonomySourcePath',
  'builtAt',
  'sourceCollections',
  'safetyScore',
  'featureScore',
  'performanceScore',
  'mileageRunningCostScore',
  'practicalityScore',
  'cityUseScore',
  'highwayUseScore',
  'premiumComfortScore',
  'valueScore',
  'regretRisk',
  'scoreReadiness',
  'scoreReadiness.finalOverallScoreReady',
  'scoreReadiness.finalOverallScoreReason',
];

const SCORE_OBJECT_FIELDS = [
  'safetyScore',
  'featureScore',
  'performanceScore',
  'mileageRunningCostScore',
  'practicalityScore',
  'cityUseScore',
  'highwayUseScore',
  'premiumComfortScore',
  'valueScore',
];

const FORBIDDEN_FIELDS = [
  'recommendationScore',
  'finalRecommendation',
  'winner',
  'overallWinner',
  'buyerFit',
  'sponsoredInfluenceDetected',
  'canUseForFinalRecommendation',
];

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
  const col = db.collection(SCORE_COLLECTION);

  const total = await col.countDocuments();

  const missingRequiredFields = [];
  for (const field of REQUIRED_FIELDS) {
    const count = await col.countDocuments(missingPathQuery(field));
    if (count > 0) missingRequiredFields.push({ field, count });
  }

  const invalidScoreObjects = [];
  for (const field of SCORE_OBJECT_FIELDS) {
    const missingObject = await col.countDocuments({
      $or: [
        { [field]: { $exists: false } },
        { [field]: null },
      ],
    });

    const missingScoreType = await col.countDocuments(missingPathQuery(`${field}.scoreType`));
    const missingStatus = await col.countDocuments(missingPathQuery(`${field}.status`));
    const missingConfidence = await col.countDocuments(missingPathQuery(`${field}.confidence`));

    const nonNumericScore = await col.countDocuments({
      [`${field}.score`]: { $exists: true, $ne: null, $not: { $type: 'number' } },
    });

    const scoredButNull = await col.countDocuments({
      [`${field}.status`]: /^scored/i,
      $or: [
        { [`${field}.score`]: { $exists: false } },
        { [`${field}.score`]: null },
      ],
    });

    if (
      missingObject > 0 ||
      missingScoreType > 0 ||
      missingStatus > 0 ||
      missingConfidence > 0 ||
      nonNumericScore > 0 ||
      scoredButNull > 0
    ) {
      invalidScoreObjects.push({
        field,
        missingObject,
        missingScoreType,
        missingStatus,
        missingConfidence,
        nonNumericScore,
        scoredButNull,
      });
    }
  }

  const duplicateScoreProfileKeys = await col.aggregate([
    { $group: { _id: '$scoreProfileKey', count: { $sum: 1 } } },
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
        scoreProfileKey: 1,
        variantProfileKey: 1,
        modelKey: 1,
        variantKey: 1,
      },
    },
    { $limit: 20 },
  ]).toArray();

  const finalOverallReady = await col.countDocuments({
    $or: [
      { 'scoreReadiness.finalOverallScoreReady': true },
      { finalOverallScoreReady: true },
    ],
  });

  const finalRecommendationEnabled = await col.countDocuments({
    $or: [
      { canUseForFinalRecommendation: true },
      { 'scoreReadiness.canUseForFinalRecommendation': true },
      { 'decisionPolicy.canUseForFinalRecommendation': true },
      { 'usageGuardrail.canUseForFinalRecommendation': true },
    ],
  });

  const forbiddenFieldCounts = [];
  for (const field of FORBIDDEN_FIELDS) {
    const count = await col.countDocuments({ [field]: { $exists: true } });
    if (count > 0) forbiddenFieldCounts.push({ field, count });
  }

  const missingBuildMetadata = await col.countDocuments({
    $or: [
      { buildVersion: { $exists: false } },
      { buildVersion: null },
      { buildVersion: '' },
      { formulaVersion: { $exists: false } },
      { formulaVersion: null },
      { formulaVersion: '' },
      { builtAt: { $exists: false } },
      { builtAt: null },
      { builtAt: '' },
    ],
  });

  const missingSourceCollections = await col.countDocuments({
    $or: [
      { sourceCollections: { $exists: false } },
      { sourceCollections: null },
      { sourceCollections: { $size: 0 } },
    ],
  });

  const readinessCounts = await col.aggregate([
    {
      $group: {
        _id: {
          finalOverallScoreReady: '$scoreReadiness.finalOverallScoreReady',
          canUseForFinalRecommendation: '$scoreReadiness.canUseForFinalRecommendation',
        },
        count: { $sum: 1 },
      },
    },
    { $sort: { count: -1 } },
  ]).toArray();

  const formulaCounts = await col.aggregate([
    { $group: { _id: '$formulaVersion', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const buildCounts = await col.aggregate([
    { $group: { _id: '$buildVersion', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const indexes = await col.indexes();

  const hasUniqueScoreProfileIndex = indexes.some(
    (idx) => idx.unique === true && JSON.stringify(idx.key) === JSON.stringify({ scoreProfileKey: 1 })
  );

  const hasVariantProfileIndex = indexes.some(
    (idx) => JSON.stringify(idx.key) === JSON.stringify({ variantProfileKey: 1 })
  );

  const sample = await col.findOne({}, { projection: { _id: 0 } });

  const failures = [];

  if (total < MIN_EXPECTED_ROWS) {
    failures.push(`expected at least ${MIN_EXPECTED_ROWS} score profiles, got ${total}`);
  }

  if (missingRequiredFields.length > 0) {
    failures.push(`missing required fields: ${JSON.stringify(missingRequiredFields.slice(0, 30))}`);
  }

  if (invalidScoreObjects.length > 0) {
    failures.push(`invalid score objects: ${JSON.stringify(invalidScoreObjects)}`);
  }

  if (duplicateScoreProfileKeys.length > 0) {
    failures.push(`duplicate scoreProfileKey rows found: ${duplicateScoreProfileKeys.length}`);
  }

  if (duplicateVariantProfileKeys.length > 0) {
    failures.push(`duplicate variantProfileKey rows found: ${duplicateVariantProfileKeys.length}`);
  }

  if (orphanVariantProfiles.length > 0) {
    failures.push(`score profiles without matching variant profile found: ${orphanVariantProfiles.length}`);
  }

  if (finalOverallReady > 0) {
    failures.push(`final overall score readiness is enabled in ${finalOverallReady} row(s)`);
  }

  if (finalRecommendationEnabled > 0) {
    failures.push(`final recommendation eligibility is enabled in ${finalRecommendationEnabled} row(s)`);
  }

  if (forbiddenFieldCounts.length > 0) {
    failures.push(`forbidden recommendation/winner fields found: ${JSON.stringify(forbiddenFieldCounts)}`);
  }

  if (missingBuildMetadata > 0) {
    failures.push(`buildVersion/formulaVersion/builtAt missing in ${missingBuildMetadata} row(s)`);
  }

  if (missingSourceCollections > 0) {
    failures.push(`sourceCollections missing in ${missingSourceCollections} row(s)`);
  }

  if (!hasUniqueScoreProfileIndex) {
    failures.push('missing unique index on scoreProfileKey');
  }

  if (!hasVariantProfileIndex) {
    failures.push('missing index on variantProfileKey');
  }

  const summary = {
    suite: 'ACI Variant Score Profile Diagnostic Contract Audit v1',
    ok: failures.length === 0,
    collection: SCORE_COLLECTION,
    total,
    minExpectedRows: MIN_EXPECTED_ROWS,
    missingRequiredFields,
    invalidScoreObjects,
    duplicateScoreProfileKeyCount: duplicateScoreProfileKeys.length,
    duplicateVariantProfileKeyCount: duplicateVariantProfileKeys.length,
    orphanVariantProfileCount: orphanVariantProfiles.length,
    orphanVariantProfiles,
    finalOverallReady,
    finalRecommendationEnabled,
    forbiddenFieldCounts,
    missingBuildMetadata,
    missingSourceCollections,
    readinessCounts,
    formulaCounts,
    buildCounts,
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
