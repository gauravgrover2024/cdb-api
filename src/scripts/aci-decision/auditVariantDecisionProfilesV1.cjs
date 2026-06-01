#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const TARGET_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

if (!mongoUri) {
  console.error('Missing Mongo URI. Set MONGODB_URI or MONGO_URI in .env');
  process.exit(1);
}

async function main() {
  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;
  const collection = db.collection(TARGET_COLLECTION);

  const total = await collection.countDocuments();

  const byConfidence = await collection
    .aggregate([
      {
        $group: {
          _id: '$dataQuality.confidenceTier',
          count: { $sum: 1 },
        },
      },
      { $sort: { count: -1 } },
    ])
    .toArray();

  const missingCritical = await collection
    .aggregate([
      { $unwind: '$dataQuality.missingCriticalFields' },
      {
        $group: {
          _id: '$dataQuality.missingCriticalFields',
          count: { $sum: 1 },
        },
      },
      { $sort: { count: -1 } },
    ])
    .toArray();

  const duplicateKeys = await collection
    .aggregate([
      {
        $group: {
          _id: '$variantProfileKey',
          count: { $sum: 1 },
        },
      },
      { $match: { count: { $gt: 1 } } },
      { $limit: 20 },
    ])
    .toArray();

  const samples = await collection
    .find(
      {},
      {
        projection: {
          _id: 0,
          variantProfileKey: 1,
          variantFullName: 1,
          fuelTransmissionFamilyKey: 1,
          referenceExShowroomPrice: 1,
          featureFlags: 1,
          safetyBasis: 1,
          performanceBasis: 1,
          mileageBasis: 1,
          dataQuality: 1,
        },
      }
    )
    .limit(5)
    .toArray();

  console.log(
    JSON.stringify(
      {
        collection: TARGET_COLLECTION,
        total,
        byConfidence,
        missingCritical,
        duplicateKeys,
        samples,
      },
      null,
      2
    )
  );

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
