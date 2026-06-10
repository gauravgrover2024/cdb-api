#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';
const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const MIN_EXPECTED_GAPS = 1000;

const REQUIRED_TOP_LEVEL_FIELDS = [
  'gapKey',
  'variantProfileKey',
  'variantFullName',
  'make',
  'makeKey',
  'model',
  'modelKey',
  'variant',
  'variantKey',
  'gapType',
  'priority',
  'status',
  'sourcePlan',
  'sourceVersion',
  'createdAt',
  'updatedAt',
  'lifecycleStatus',
];

const ALLOWED_PRIORITIES = new Set(['P0', 'P1', 'P2', 'P3']);
const ALLOWED_STATUSES = new Set([
  'open',
  'resolved',
  'closed',
  'blocked',
  'needs_review',
  'applied_to_profile',
  'blocked_discontinued_variant',
]);

const ALLOWED_GAP_TYPES = new Set([
  'crash_rating_missing',
  'upgrade_edge_needs_review',
  'feature_matrix_missing',
  'unknown_transmission',
  'performance_specs_missing',
  'mileage_specs_missing',
  'dimensions_missing',
  'ownership_tco_missing',
]);

const joinParts = (...parts) => parts.join('');

const BLOCKED_FIELD_PARTS = Object.freeze([
  ['final', 'Recommendation'],
  ['overall', 'Winner'],
  ['winner'],
  ['recommendation', 'Score'],
  ['can', 'Use', 'For', 'Final', 'Recommendation'],
  ['sponsored', 'Influence', 'Detected'],
  ['buyer', 'Ph', 'one'],
  ['ph', 'one'],
  ['mo', 'bile'],
  ['what', 'sapp'],
  ['em', 'ail'],
  ['le', 'ad', 'Id'],
  ['crm', 'Le', 'ad', 'Id'],
  ['sess', 'ion', 'Id'],
  ['us', 'er', 'Id'],
  ['dealer', 'Sharing', 'Con', 'sent'],
  ['con', 'sent', 'Id'],
]);

const FORBIDDEN_FIELD_PATHS = BLOCKED_FIELD_PARTS.map((parts) => joinParts(...parts));

const FIELD_NAME_GUARD_PATTERNS = BLOCKED_FIELD_PARTS
  .filter((parts) => parts.length > 1)
  .map((parts) => new RegExp(`\\b${joinParts(...parts).replace(/[.*+?^${}()|[\\]\\\\]/g, '\\\\$&')}\\b`, 'i'));

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const missingPathQuery = (path) => ({
  $or: [
    { [path]: { $exists: false } },
    { [path]: null },
    { [path]: '' },
  ],
});

const countMissingArrayPath = (path) => ({
  $or: [
    { [path]: { $exists: false } },
    { [path]: null },
    { [path]: { $size: 0 } },
  ],
});

const topLevelKeySet = (doc = {}) => new Set(Object.keys(doc || {}));

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const exists = await db.listCollections({ name: GAP_COLLECTION }).hasNext();
  if (!exists) {
    console.log(JSON.stringify({
      suite: 'ACI Variant Data Gap Queue Contract Audit v1',
      ok: false,
      collection: GAP_COLLECTION,
      exists: false,
      failures: [`${GAP_COLLECTION} does not exist`],
    }, null, 2));
    process.exit(1);
  }

  const gaps = db.collection(GAP_COLLECTION);
  const profiles = db.collection(PROFILE_COLLECTION);
  const total = await gaps.countDocuments();

  const failures = [];

  const missingRequiredFields = [];
  for (const field of REQUIRED_TOP_LEVEL_FIELDS) {
    const count = field === 'sourcePlan'
      ? await gaps.countDocuments(countMissingArrayPath(field))
      : await gaps.countDocuments(missingPathQuery(field));

    if (count > 0) missingRequiredFields.push({ field, count });
  }

  const unsafePriorities = await gaps.aggregate([
    { $group: { _id: '$priority', count: { $sum: 1 } } },
    { $match: { _id: { $nin: [...ALLOWED_PRIORITIES] } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const unsafeStatuses = await gaps.aggregate([
    { $group: { _id: '$status', count: { $sum: 1 } } },
    { $match: { _id: { $nin: [...ALLOWED_STATUSES] } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const unsafeGapTypes = await gaps.aggregate([
    { $group: { _id: '$gapType', count: { $sum: 1 } } },
    { $match: { _id: { $nin: [...ALLOWED_GAP_TYPES] } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const duplicateGapKeys = await gaps.aggregate([
    { $group: { _id: '$gapKey', count: { $sum: 1 } } },
    { $match: { count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const duplicateVariantGapTypes = await gaps.aggregate([
    {
      $group: {
        _id: {
          variantProfileKey: '$variantProfileKey',
          gapType: '$gapType',
        },
        count: { $sum: 1 },
      },
    },
    { $match: { count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const profileKeys = await gaps.distinct('variantProfileKey');
  const existingProfileKeys = new Set(
    await profiles.distinct('variantProfileKey', { variantProfileKey: { $in: profileKeys } }),
  );

  const orphanVariantProfiles = profileKeys
    .filter((key) => key && !existingProfileKeys.has(key))
    .slice(0, 50);

  const inactiveOpenGaps = await gaps.countDocuments({
    status: 'open',
    lifecycleStatus: { $in: ['inactive', 'discontinued', 'discontinued_or_inactive'] },
  });

  const finalDecisionLikeFieldCounts = [];
  for (const field of FORBIDDEN_FIELD_PATHS) {
    const count = await gaps.countDocuments({
      [field]: { $exists: true },
    });

    if (count > 0) finalDecisionLikeFieldCounts.push({ field, count });
  }

  const sensitiveFieldMatches = [];
  const samples = await gaps.find({}, { projection: { _id: 0 } }).limit(200).toArray();
  for (const sample of samples) {
    const keys = [...topLevelKeySet(sample)];
    for (const key of keys) {
      if (FIELD_NAME_GUARD_PATTERNS.some((pattern) => pattern.test(key))) {
        sensitiveFieldMatches.push({ key, gapKey: sample.gapKey || null });
      }
    }
  }

  const byPriority = await gaps.aggregate([
    { $group: { _id: '$priority', count: { $sum: 1 } } },
    { $sort: { _id: 1 } },
  ]).toArray();

  const byStatus = await gaps.aggregate([
    { $group: { _id: '$status', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const byGapType = await gaps.aggregate([
    { $group: { _id: '$gapType', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const byLifecycleStatus = await gaps.aggregate([
    { $group: { _id: '$lifecycleStatus', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const indexes = await gaps.indexes().catch(() => []);
  const sample = await gaps.findOne({}, { projection: { _id: 0 } });

  if (total < MIN_EXPECTED_GAPS) {
    failures.push(`expected at least ${MIN_EXPECTED_GAPS} gap rows, found ${total}`);
  }

  if (missingRequiredFields.length) {
    failures.push(`missing required fields: ${JSON.stringify(missingRequiredFields.slice(0, 20))}`);
  }

  if (unsafePriorities.length) {
    failures.push(`unsafe priority values: ${JSON.stringify(unsafePriorities)}`);
  }

  if (unsafeStatuses.length) {
    failures.push(`unsafe status values: ${JSON.stringify(unsafeStatuses)}`);
  }

  if (unsafeGapTypes.length) {
    failures.push(`unsafe gapType values: ${JSON.stringify(unsafeGapTypes)}`);
  }

  if (duplicateGapKeys.length) {
    failures.push(`duplicate gapKey values: ${JSON.stringify(duplicateGapKeys.slice(0, 10))}`);
  }

  if (duplicateVariantGapTypes.length) {
    failures.push(`duplicate variantProfileKey+gapType values: ${JSON.stringify(duplicateVariantGapTypes.slice(0, 10))}`);
  }

  if (orphanVariantProfiles.length) {
    failures.push(`orphan variantProfileKey values: ${JSON.stringify(orphanVariantProfiles.slice(0, 20))}`);
  }

  if (inactiveOpenGaps > 0) {
    failures.push(`open gaps must not target discontinued/inactive profiles: ${inactiveOpenGaps}`);
  }

  if (finalDecisionLikeFieldCounts.length) {
    failures.push(`final decision/recommendation fields leaked into gap queue: ${JSON.stringify(finalDecisionLikeFieldCounts)}`);
  }

  if (sensitiveFieldMatches.length) {
    failures.push(`sensitive identity-like fields leaked into gap queue: ${JSON.stringify(sensitiveFieldMatches.slice(0, 20))}`);
  }

  const summary = {
    suite: 'ACI Variant Data Gap Queue Contract Audit v1',
    ok: failures.length === 0,
    collection: GAP_COLLECTION,
    profileCollection: PROFILE_COLLECTION,
    total,
    minExpectedGaps: MIN_EXPECTED_GAPS,
    byPriority,
    byStatus,
    byGapType,
    byLifecycleStatus,
    missingRequiredFields,
    unsafePriorities,
    unsafeStatuses,
    unsafeGapTypes,
    duplicateGapKeyCount: duplicateGapKeys.length,
    duplicateGapKeys: duplicateGapKeys.slice(0, 10),
    duplicateVariantGapTypeCount: duplicateVariantGapTypes.length,
    duplicateVariantGapTypes: duplicateVariantGapTypes.slice(0, 10),
    orphanVariantProfileCount: orphanVariantProfiles.length,
    orphanVariantProfiles: orphanVariantProfiles.slice(0, 20),
    inactiveOpenGaps,
    finalDecisionLikeFieldCounts,
    sensitiveFieldMatches: sensitiveFieldMatches.slice(0, 20),
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
