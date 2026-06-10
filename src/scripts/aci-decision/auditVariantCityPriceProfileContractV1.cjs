#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const COLLECTION =
  process.env.ACI_VARIANT_CITY_PRICE_PROFILE_COLLECTION || 'aci_vehicle_variant_city_price_profile';

const MIN_EXPECTED_ROWS = Number(
  process.env.ACI_VARIANT_CITY_PRICE_PROFILE_MIN_COUNT || 5000
);

const SUPPORTED_CITY_SLUGS = new Set(['new-delhi', 'noida', 'gurgaon']);

const REQUIRED_FIELDS = [
  'cityPriceProfileKey',
  'variantProfileKey',
  'make',
  'makeKey',
  'model',
  'modelKey',
  'variant',
  'variantKey',
  'variantFullName',
  'fuel',
  'fuelKey',
  'transmission',
  'transmissionKey',
  'fuelTransmissionFamilyKey',
  'city',
  'citySlug',
  'supportedCity',
  'availabilityStatus',
  'exShowroomPrice',
  'onRoadPrice',
  'onRoadPriceWithoutOptional',
  'sourceCollection',
  'sourcePriceRowId',
  'sourceVersion',
  'priceUpdatedAt',
];

const FUTURE_DECISION_FIELDS_THAT_MUST_NOT_EXIST = [
  'scores',
  'recommendationScore',
  'buyerFit',
  'variantRole',
  'decisionPolicy',
  'usageGuardrail',
  'scoreReadiness',
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

const positiveNumberMissingQuery = (path) => ({
  $or: [
    { [path]: { $exists: false } },
    { [path]: null },
    { [path]: { $lte: 0 } },
  ],
});

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const col = db.collection(COLLECTION);

  const total = await col.countDocuments();

  const byCity = await col.aggregate([
    { $group: { _id: '$citySlug', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const unsupportedCityRows = await col.aggregate([
    { $match: { citySlug: { $nin: [...SUPPORTED_CITY_SLUGS] } } },
    { $group: { _id: '$citySlug', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const supportedCityFalseRows = await col.countDocuments({
    $or: [
      { supportedCity: { $ne: true } },
      { supportedCity: { $exists: false } },
    ],
  });

  const missingRequiredFields = [];
  for (const field of REQUIRED_FIELDS) {
    const count = await col.countDocuments(missingPathQuery(field));
    if (count > 0) missingRequiredFields.push({ field, count });
  }

  const invalidPriceFields = [];
  for (const field of ['exShowroomPrice', 'onRoadPrice', 'onRoadPriceWithoutOptional']) {
    const count = await col.countDocuments(positiveNumberMissingQuery(field));
    if (count > 0) invalidPriceFields.push({ field, count });
  }

  const onRoadLessThanExShowroom = await col.countDocuments({
    $expr: {
      $lt: ['$onRoadPrice', '$exShowroomPrice'],
    },
  });

  const duplicateVariantCity = await col.aggregate([
    {
      $group: {
        _id: {
          variantProfileKey: '$variantProfileKey',
          citySlug: '$citySlug',
        },
        count: { $sum: 1 },
      },
    },
    { $match: { count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const duplicateCityPriceProfileKeys = await col.aggregate([
    {
      $group: {
        _id: '$cityPriceProfileKey',
        count: { $sum: 1 },
      },
    },
    {
      $match: {
        _id: { $ne: null },
        count: { $gt: 1 },
      },
    },
    { $limit: 20 },
  ]).toArray();

  const orphanVariantProfiles = await col.aggregate([
    {
      $lookup: {
        from: process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile',
        localField: 'variantProfileKey',
        foreignField: 'variantProfileKey',
        as: 'profile',
      },
    },
    {
      $match: {
        profile: { $size: 0 },
      },
    },
    {
      $project: {
        _id: 0,
        cityPriceProfileKey: 1,
        variantProfileKey: 1,
        citySlug: 1,
      },
    },
    { $limit: 20 },
  ]).toArray();

  const futureDecisionFieldCounts = [];
  for (const field of FUTURE_DECISION_FIELDS_THAT_MUST_NOT_EXIST) {
    const count = await col.countDocuments({ [field]: { $exists: true } });
    if (count > 0) futureDecisionFieldCounts.push({ field, count });
  }

  const missingBuildMetadata = await col.countDocuments({
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

  const hasUniqueCityPriceKeyIndex = indexes.some(
    (idx) => idx.unique === true && JSON.stringify(idx.key) === JSON.stringify({ cityPriceProfileKey: 1 })
  );

  const hasUniqueVariantCityIndex = indexes.some(
    (idx) => idx.unique === true && JSON.stringify(idx.key) === JSON.stringify({ variantProfileKey: 1, citySlug: 1 })
  );

  const sample = await col.findOne({}, { projection: { _id: 0 } });

  const failures = [];

  if (total < MIN_EXPECTED_ROWS) {
    failures.push(`expected at least ${MIN_EXPECTED_ROWS} city price rows, got ${total}`);
  }

  if (unsupportedCityRows.length > 0) {
    failures.push(`unsupported city rows found: ${JSON.stringify(unsupportedCityRows)}`);
  }

  if (supportedCityFalseRows > 0) {
    failures.push(`supportedCity is not true for ${supportedCityFalseRows} row(s)`);
  }

  if (missingRequiredFields.length > 0) {
    failures.push(`missing required fields: ${JSON.stringify(missingRequiredFields.slice(0, 30))}`);
  }

  if (invalidPriceFields.length > 0) {
    failures.push(`invalid positive price fields: ${JSON.stringify(invalidPriceFields)}`);
  }

  if (onRoadLessThanExShowroom > 0) {
    failures.push(`onRoadPrice below exShowroomPrice in ${onRoadLessThanExShowroom} row(s)`);
  }

  if (duplicateVariantCity.length > 0) {
    failures.push(`duplicate variantProfileKey+citySlug rows found: ${duplicateVariantCity.length}`);
  }

  if (duplicateCityPriceProfileKeys.length > 0) {
    failures.push(`duplicate cityPriceProfileKey rows found: ${duplicateCityPriceProfileKeys.length}`);
  }

  if (orphanVariantProfiles.length > 0) {
    failures.push(`city price rows without matching decision profile found: ${orphanVariantProfiles.length}`);
  }

  if (futureDecisionFieldCounts.length > 0) {
    failures.push(`decision/recommendation fields leaked into city price profile: ${JSON.stringify(futureDecisionFieldCounts)}`);
  }

  if (missingBuildMetadata > 0) {
    failures.push(`buildVersion/builtAt/sourceCollections missing in ${missingBuildMetadata} row(s)`);
  }

  if (!hasUniqueCityPriceKeyIndex) {
    failures.push('missing unique index on cityPriceProfileKey');
  }

  if (!hasUniqueVariantCityIndex) {
    failures.push('missing unique index on variantProfileKey+citySlug');
  }

  const summary = {
    suite: 'ACI Variant City Price Profile Contract Audit v1',
    ok: failures.length === 0,
    collection: COLLECTION,
    total,
    minExpectedRows: MIN_EXPECTED_ROWS,
    byCity,
    unsupportedCityRows,
    supportedCityFalseRows,
    missingRequiredFields,
    invalidPriceFields,
    onRoadLessThanExShowroom,
    duplicateVariantCityCount: duplicateVariantCity.length,
    duplicateCityPriceProfileKeyCount: duplicateCityPriceProfileKeys.length,
    orphanVariantProfileCount: orphanVariantProfiles.length,
    orphanVariantProfiles,
    futureDecisionFieldCounts,
    missingBuildMetadata,
    indexes: indexes.map((idx) => ({
      name: idx.name,
      key: idx.key,
      unique: idx.unique === true,
    })),
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
