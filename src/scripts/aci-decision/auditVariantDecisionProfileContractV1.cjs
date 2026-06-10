#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const MIN_EXPECTED_PROFILES = Number(
  process.env.ACI_VARIANT_DECISION_PROFILE_MIN_COUNT || 1800
);

const REQUIRED_TOP_LEVEL_FIELDS = [
  'variantProfileKey',
  'make',
  'makeKey',
  'model',
  'modelKey',
  'variant',
  'variantKey',
  'variantFullName',
  'brandModelKey',
  'lifecycleStatus',
  'fuelTransmissionFamilyKey',
  'referenceExShowroomPrice',
  'fuel',
  'fuelKey',
  'transmission',
  'transmissionKey',
  'dataStatus',
  'dataQuality',
  'scores',
];

const REQUIRED_DATA_QUALITY_FIELDS = [
  'hasPrice',
  'hasFeatureMatrix',
  'hasSafetyData',
  'hasPerformanceData',
  'hasMileageData',
  'hasDimensionsData',
  'confidenceTier',
  'needsReview',
];

const FUTURE_SCORE_FIELDS_THAT_MUST_STAY_NULL = [
  'scores.safetyScore',
  'scores.featureScore',
  'scores.valueScore',
  'scores.familyScore',
  'scores.cityUseScore',
  'scores.highwayScore',
  'scores.performanceScore',
  'scores.mileageScore',
  'scores.comfortScore',
  'scores.premiumScore',
  'scores.featureToPriceRatio',
  'scores.runningCostScore',
  'scores.regretRiskScore',
  'scores.recommendationScore',
];

const SAFE_CONFIDENCE_TIERS = new Set(['high', 'medium', 'low']);
const SAFE_DATA_STATUSES = new Set(['complete', 'partial', 'missing', 'needs_review']);

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const getPath = (obj, dotted) =>
  String(dotted || '')
    .split('.')
    .reduce((acc, key) => (acc && Object.prototype.hasOwnProperty.call(acc, key) ? acc[key] : undefined), obj);

const isMissing = (value) =>
  value === undefined ||
  value === null ||
  (typeof value === 'string' && value.trim() === '');

const countMissingPath = async (col, path) =>
  col.countDocuments({
    $or: [
      { [path]: { $exists: false } },
      { [path]: null },
      { [path]: '' },
    ],
  });

const countNonNullPath = async (col, path) =>
  col.countDocuments({
    [path]: {
      $exists: true,
      $ne: null,
    },
  });

const countNonEmptyArrayPath = async (col, path) =>
  col.countDocuments({
    [path]: {
      $exists: true,
      $type: 'array',
      $ne: [],
    },
  });

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;
  const col = db.collection(PROFILE_COLLECTION);

  const total = await col.countDocuments();

  const duplicateKeys = await col.aggregate([
    {
      $group: {
        _id: '$variantProfileKey',
        count: { $sum: 1 },
      },
    },
    {
      $match: {
        _id: { $ne: null },
        count: { $gt: 1 },
      },
    },
    {
      $limit: 20,
    },
  ]).toArray();

  const missingTopLevel = [];
  for (const field of REQUIRED_TOP_LEVEL_FIELDS) {
    const count = await countMissingPath(col, field);
    if (count > 0) missingTopLevel.push({ field, count });
  }

  const missingDataQuality = [];
  for (const field of REQUIRED_DATA_QUALITY_FIELDS) {
    const count = await countMissingPath(col, `dataQuality.${field}`);
    if (count > 0) missingDataQuality.push({ field: `dataQuality.${field}`, count });
  }

  const nonNullFutureScores = [];
  for (const field of FUTURE_SCORE_FIELDS_THAT_MUST_STAY_NULL) {
    const count = await countNonNullPath(col, field);
    if (count > 0) nonNullFutureScores.push({ field, count });
  }

  const unsafeConfidenceTiers = await col.aggregate([
    {
      $group: {
        _id: '$dataQuality.confidenceTier',
        count: { $sum: 1 },
      },
    },
    {
      $match: {
        _id: { $nin: [...SAFE_CONFIDENCE_TIERS] },
      },
    },
  ]).toArray();

  const unsafeDataStatuses = await col.aggregate([
    {
      $group: {
        _id: '$dataStatus',
        count: { $sum: 1 },
      },
    },
    {
      $match: {
        _id: { $nin: [...SAFE_DATA_STATUSES] },
      },
    },
  ]).toArray();

  const nonEmptyIdealFor = await countNonEmptyArrayPath(col, 'buyerFit.idealFor');
  const nonEmptySkipIf = await countNonEmptyArrayPath(col, 'buyerFit.skipIf');

  const finalRecommendationFlags = await col.countDocuments({
    $or: [
      { canUseForFinalRecommendation: true },
      { 'usageGuardrail.canUseForFinalRecommendation': true },
      { 'decisionPolicy.canUseForFinalRecommendation': true },
      { 'scoreReadiness.finalOverallScoreReady': true },
    ],
  });

  const missingSourceCollections = await col.countDocuments({
    $or: [
      { sourceCollections: { $exists: false } },
      { sourceCollections: null },
      { sourceCollections: { $size: 0 } },
    ],
  });

  const missingBuildMetadata = await col.countDocuments({
    $or: [
      { buildVersion: { $exists: false } },
      { buildVersion: null },
      { buildVersion: '' },
      { builtAt: { $exists: false } },
      { builtAt: null },
      { builtAt: '' },
    ],
  });

  const byConfidenceTier = await col.aggregate([
    {
      $group: {
        _id: '$dataQuality.confidenceTier',
        count: { $sum: 1 },
      },
    },
    { $sort: { count: -1 } },
  ]).toArray();

  const byDataStatus = await col.aggregate([
    {
      $group: {
        _id: '$dataStatus',
        count: { $sum: 1 },
      },
    },
    { $sort: { count: -1 } },
  ]).toArray();

  const sample = await col.findOne(
    {},
    {
      projection: {
        _id: 0,
        variantProfileKey: 1,
        makeKey: 1,
        modelKey: 1,
        variantKey: 1,
        dataStatus: 1,
        dataQuality: 1,
        scores: 1,
        buyerFit: 1,
        sourceCollections: 1,
        buildVersion: 1,
        builtAt: 1,
      },
    }
  );

  const failures = [];

  if (total < MIN_EXPECTED_PROFILES) {
    failures.push(`expected at least ${MIN_EXPECTED_PROFILES} profiles, got ${total}`);
  }

  if (duplicateKeys.length > 0) {
    failures.push(`duplicate variantProfileKey values found: ${duplicateKeys.length}`);
  }

  if (missingTopLevel.length > 0) {
    failures.push(`missing required top-level fields: ${JSON.stringify(missingTopLevel.slice(0, 20))}`);
  }

  if (missingDataQuality.length > 0) {
    failures.push(`missing required dataQuality fields: ${JSON.stringify(missingDataQuality.slice(0, 20))}`);
  }

  if (nonNullFutureScores.length > 0) {
    failures.push(`future score fields must remain null: ${JSON.stringify(nonNullFutureScores.slice(0, 20))}`);
  }

  if (unsafeConfidenceTiers.length > 0) {
    failures.push(`unsafe confidence tiers: ${JSON.stringify(unsafeConfidenceTiers)}`);
  }

  if (unsafeDataStatuses.length > 0) {
    failures.push(`unsafe dataStatus values: ${JSON.stringify(unsafeDataStatuses)}`);
  }

  if (nonEmptyIdealFor > 0 || nonEmptySkipIf > 0) {
    failures.push(`future persona fields must remain empty: idealFor=${nonEmptyIdealFor}, skipIf=${nonEmptySkipIf}`);
  }

  if (finalRecommendationFlags > 0) {
    failures.push(`final recommendation activation flags found in ${finalRecommendationFlags} profile(s)`);
  }

  if (missingSourceCollections > 0) {
    failures.push(`sourceCollections missing/empty in ${missingSourceCollections} profile(s)`);
  }

  if (missingBuildMetadata > 0) {
    failures.push(`buildVersion/builtAt missing in ${missingBuildMetadata} profile(s)`);
  }

  const summary = {
    suite: 'ACI Variant Decision Profile Contract Audit v1',
    ok: failures.length === 0,
    collection: PROFILE_COLLECTION,
    total,
    minExpectedProfiles: MIN_EXPECTED_PROFILES,
    byConfidenceTier,
    byDataStatus,
    duplicateKeyCount: duplicateKeys.length,
    missingTopLevel,
    missingDataQuality,
    nonNullFutureScores,
    unsafeConfidenceTiers,
    unsafeDataStatuses,
    nonEmptyFuturePersonaFields: {
      buyerFitIdealFor: nonEmptyIdealFor,
      buyerFitSkipIf: nonEmptySkipIf,
    },
    finalRecommendationFlags,
    missingSourceCollections,
    missingBuildMetadata,
    sample,
    failures,
  };

  console.log(JSON.stringify(summary, null, 2));

  if (!summary.ok) {
    process.exit(1);
  }
}

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
