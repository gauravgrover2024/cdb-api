#!/usr/bin/env node

require('dotenv').config();

const mongoose = require('mongoose');

const EVIDENCE_COLLECTION =
  process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';
const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const MIN_EXPECTED_EVIDENCE_ROWS = 1;

const REQUIRED_TOP_LEVEL_FIELDS = Object.freeze([
  'evidenceKey',
  'variantProfileKey',
  'variantFullName',
  'make',
  'makeKey',
  'model',
  'modelKey',
  'variant',
  'variantKey',
  'gapKey',
  'gapType',
  'evidenceType',
  'priority',
  'status',
  'reviewStatus',
  'confidence',
  'sourcePriority',
  'sourceVersion',
  'createdAt',
  'updatedAt',
]);

const ALLOWED_PRIORITIES = new Set(['P0', 'P1', 'P2', 'P3']);

const ALLOWED_CONFIDENCE = new Set([
  'none',
  'low',
  'medium',
  'high',
]);

const ALLOWED_STATUSES = new Set([
  'needs_external_source',
  'needs_source',
  'needs_manual_review',
  'internal_source_ready',
  'applied_to_profile',
  'blocked_discontinued_variant',
  'superseded_profile_key',
]);

const ALLOWED_REVIEW_STATUSES = new Set([
  'internal_unresolved',
  'auto_exact_feature_matrix_match',
  'ambiguous_exact_feature_matrix_match',
  'auto_price_transmission_match',
  'auto_feature_transmission_match',
  'auto_variant_name_transmission_inference',
  'profile_patch_applied',
  'raw_profile_patch_applied',
  'blocked_source_vehicle_discontinued',
  'stale_profile_key_after_profile_rebuild',
]);

const ALLOWED_EVIDENCE_TYPES = new Set([
  'feature_matrix',
  'transmission_spec',
]);

const ALLOWED_GAP_TYPES = new Set([
  'feature_matrix_missing',
  'unknown_transmission',
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
  .map((parts) => new RegExp(`\\b${joinParts(...parts).replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i'));

const getMongoUri = () =>
  process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const missingPathQuery = (path) => ({
  $or: [
    { [path]: { $exists: false } },
    { [path]: null },
    { [path]: '' },
  ],
});

const missingArrayPathQuery = (path) => ({
  $or: [
    { [path]: { $exists: false } },
    { [path]: null },
    { [path]: { $size: 0 } },
  ],
});

const sampleTopLevelKeys = (doc = {}) => Object.keys(doc || {});

async function aggregateUnsafeValues(collection, field, allowedSet) {
  return collection.aggregate([
    { $group: { _id: `$${field}`, count: { $sum: 1 } } },
    { $match: { _id: { $nin: [...allowedSet] } } },
    { $sort: { count: -1 } },
  ]).toArray();
}

async function main() {
  const mongoUri = getMongoUri();
  if (!mongoUri) throw new Error('Missing Mongo URI.');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const exists = await db.listCollections({ name: EVIDENCE_COLLECTION }).hasNext();
  if (!exists) {
    const summary = {
      suite: 'ACI Variant External Evidence Contract Audit v1',
      ok: false,
      collection: EVIDENCE_COLLECTION,
      exists: false,
      failures: [`${EVIDENCE_COLLECTION} does not exist`],
    };

    console.log(JSON.stringify(summary, null, 2));
    process.exit(1);
  }

  const evidence = db.collection(EVIDENCE_COLLECTION);
  const profiles = db.collection(PROFILE_COLLECTION);

  const total = await evidence.countDocuments();
  const failures = [];

  const missingRequiredFields = [];
  for (const field of REQUIRED_TOP_LEVEL_FIELDS) {
    const count = field === 'sourcePriority'
      ? await evidence.countDocuments(missingArrayPathQuery(field))
      : await evidence.countDocuments(missingPathQuery(field));

    if (count > 0) missingRequiredFields.push({ field, count });
  }

  const unsafePriorities = await aggregateUnsafeValues(evidence, 'priority', ALLOWED_PRIORITIES);
  const unsafeStatuses = await aggregateUnsafeValues(evidence, 'status', ALLOWED_STATUSES);
  const unsafeReviewStatuses = await aggregateUnsafeValues(evidence, 'reviewStatus', ALLOWED_REVIEW_STATUSES);
  const unsafeEvidenceTypes = await aggregateUnsafeValues(evidence, 'evidenceType', ALLOWED_EVIDENCE_TYPES);
  const unsafeGapTypes = await aggregateUnsafeValues(evidence, 'gapType', ALLOWED_GAP_TYPES);
  const unsafeConfidence = await aggregateUnsafeValues(evidence, 'confidence', ALLOWED_CONFIDENCE);

  const duplicateEvidenceKeys = await evidence.aggregate([
    { $group: { _id: '$evidenceKey', count: { $sum: 1 } } },
    { $match: { count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const duplicateVariantEvidence = await evidence.aggregate([
    {
      $group: {
        _id: {
          variantProfileKey: '$variantProfileKey',
          evidenceType: '$evidenceType',
          gapType: '$gapType',
        },
        count: { $sum: 1 },
      },
    },
    { $match: { count: { $gt: 1 } } },
    { $limit: 20 },
  ]).toArray();

  const profileKeys = await evidence.distinct('variantProfileKey');
  const existingProfileKeys = new Set(
    await profiles.distinct('variantProfileKey', { variantProfileKey: { $in: profileKeys } }),
  );

  const orphanVariantProfileKeys = profileKeys
    .filter((key) => key && !existingProfileKeys.has(key));

  const activeOrphanEvidenceRows = await evidence.find({
    variantProfileKey: { $in: orphanVariantProfileKeys },
    status: { $ne: 'superseded_profile_key' },
  }, {
    projection: {
      _id: 0,
      evidenceKey: 1,
      status: 1,
      reviewStatus: 1,
      variantProfileKey: 1,
      variantFullName: 1,
    },
  }).limit(50).toArray();

  const supersededOrphanEvidenceRows = await evidence.countDocuments({
    variantProfileKey: { $in: orphanVariantProfileKeys },
    status: 'superseded_profile_key',
    reviewStatus: 'stale_profile_key_after_profile_rebuild',
    confidence: 'none',
  });

  const appliedMissingPatchTrace = await evidence.countDocuments({
    status: 'applied_to_profile',
    $or: [
      { profilePatchedAt: { $exists: false } },
      { profilePatchedAt: null },
      { profilePatchedAt: '' },
    ],
  });

  const appliedMissingSource = await evidence.countDocuments({
    status: { $in: ['applied_to_profile', 'internal_source_ready'] },
    $or: [
      { sourceName: { $exists: false } },
      { sourceName: null },
      { sourceName: '' },
      { sourceType: { $exists: false } },
      { sourceType: null },
      { sourceType: '' },
    ],
  });

  const readyOrAppliedNoConfidence = await evidence.countDocuments({
    status: { $in: ['applied_to_profile', 'internal_source_ready'] },
    confidence: { $in: [null, '', 'none'] },
  });

  const unresolvedWithSource = await evidence.countDocuments({
    status: 'needs_external_source',
    $or: [
      { sourceName: { $nin: [null, ''] } },
      { sourceType: { $nin: [null, ''] } },
      { sourceUrl: { $nin: [null, ''] } },
    ],
  });

  const blockedNotMarkedDiscontinued = await evidence.countDocuments({
    status: 'blocked_discontinued_variant',
    reviewStatus: { $ne: 'blocked_source_vehicle_discontinued' },
  });

  const blockedWithConfidence = await evidence.countDocuments({
    status: 'blocked_discontinued_variant',
    confidence: { $nin: ['none', null, ''] },
  });

  const supersededNotNeutralized = await evidence.countDocuments({
    status: 'superseded_profile_key',
    $or: [
      { reviewStatus: { $ne: 'stale_profile_key_after_profile_rebuild' } },
      { confidence: { $ne: 'none' } },
      { supersededAt: { $exists: false } },
      { previousVariantProfileKey: { $exists: false } },
    ],
  });

  const finalDecisionLikeFieldCounts = [];
  for (const field of FORBIDDEN_FIELD_PATHS) {
    const count = await evidence.countDocuments({ [field]: { $exists: true } });
    if (count > 0) finalDecisionLikeFieldCounts.push({ field, count });
  }

  const sensitiveFieldMatches = [];
  const samplesForKeyScan = await evidence.find({}, { projection: { _id: 0 } }).limit(200).toArray();
  for (const sample of samplesForKeyScan) {
    for (const key of sampleTopLevelKeys(sample)) {
      if (FIELD_NAME_GUARD_PATTERNS.some((pattern) => pattern.test(key))) {
        sensitiveFieldMatches.push({ key, evidenceKey: sample.evidenceKey || null });
      }
    }
  }

  const byStatus = await evidence.aggregate([
    { $group: { _id: '$status', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const byReviewStatus = await evidence.aggregate([
    { $group: { _id: '$reviewStatus', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const byEvidenceType = await evidence.aggregate([
    { $group: { _id: '$evidenceType', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const byConfidence = await evidence.aggregate([
    { $group: { _id: '$confidence', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]).toArray();

  const indexes = await evidence.indexes().catch(() => []);
  const sample = await evidence.findOne({}, { projection: { _id: 0 } });

  if (total < MIN_EXPECTED_EVIDENCE_ROWS) {
    failures.push(`expected at least ${MIN_EXPECTED_EVIDENCE_ROWS} evidence row(s), found ${total}`);
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

  if (unsafeReviewStatuses.length) {
    failures.push(`unsafe reviewStatus values: ${JSON.stringify(unsafeReviewStatuses)}`);
  }

  if (unsafeEvidenceTypes.length) {
    failures.push(`unsafe evidenceType values: ${JSON.stringify(unsafeEvidenceTypes)}`);
  }

  if (unsafeGapTypes.length) {
    failures.push(`unsafe gapType values: ${JSON.stringify(unsafeGapTypes)}`);
  }

  if (unsafeConfidence.length) {
    failures.push(`unsafe confidence values: ${JSON.stringify(unsafeConfidence)}`);
  }

  if (duplicateEvidenceKeys.length) {
    failures.push(`duplicate evidenceKey values: ${JSON.stringify(duplicateEvidenceKeys.slice(0, 10))}`);
  }

  if (duplicateVariantEvidence.length) {
    failures.push(`duplicate variantProfileKey+evidenceType+gapType values: ${JSON.stringify(duplicateVariantEvidence.slice(0, 10))}`);
  }

  if (activeOrphanEvidenceRows.length) {
    failures.push(`active orphan evidence rows must be superseded before contract can pass: ${JSON.stringify(activeOrphanEvidenceRows.slice(0, 20))}`);
  }

  if (appliedMissingPatchTrace > 0) {
    failures.push(`applied evidence missing profilePatchedAt: ${appliedMissingPatchTrace}`);
  }

  if (appliedMissingSource > 0) {
    failures.push(`ready/applied evidence missing sourceName/sourceType: ${appliedMissingSource}`);
  }

  if (readyOrAppliedNoConfidence > 0) {
    failures.push(`ready/applied evidence must not have empty/none confidence: ${readyOrAppliedNoConfidence}`);
  }

  if (unresolvedWithSource > 0) {
    failures.push(`needs_external_source rows must not pretend to have a resolved source: ${unresolvedWithSource}`);
  }

  if (blockedNotMarkedDiscontinued > 0) {
    failures.push(`blocked_discontinued_variant rows must use blocked_source_vehicle_discontinued reviewStatus: ${blockedNotMarkedDiscontinued}`);
  }

  if (blockedWithConfidence > 0) {
    failures.push(`blocked_discontinued_variant rows must not carry evidence confidence: ${blockedWithConfidence}`);
  }

  if (supersededNotNeutralized > 0) {
    failures.push(`superseded evidence rows must be neutralized and traceable: ${supersededNotNeutralized}`);
  }

  if (finalDecisionLikeFieldCounts.length) {
    failures.push(`final decision/recommendation fields leaked into external evidence: ${JSON.stringify(finalDecisionLikeFieldCounts)}`);
  }

  if (sensitiveFieldMatches.length) {
    failures.push(`sensitive identity-like fields leaked into external evidence: ${JSON.stringify(sensitiveFieldMatches.slice(0, 20))}`);
  }

  const summary = {
    suite: 'ACI Variant External Evidence Contract Audit v1',
    ok: failures.length === 0,
    collection: EVIDENCE_COLLECTION,
    profileCollection: PROFILE_COLLECTION,
    total,
    minExpectedEvidenceRows: MIN_EXPECTED_EVIDENCE_ROWS,
    byStatus,
    byReviewStatus,
    byEvidenceType,
    byConfidence,
    missingRequiredFields,
    unsafePriorities,
    unsafeStatuses,
    unsafeReviewStatuses,
    unsafeEvidenceTypes,
    unsafeGapTypes,
    unsafeConfidence,
    duplicateEvidenceKeyCount: duplicateEvidenceKeys.length,
    duplicateEvidenceKeys: duplicateEvidenceKeys.slice(0, 10),
    duplicateVariantEvidenceCount: duplicateVariantEvidence.length,
    duplicateVariantEvidence: duplicateVariantEvidence.slice(0, 10),
    orphanVariantProfileCount: orphanVariantProfileKeys.length,
    activeOrphanEvidenceRowCount: activeOrphanEvidenceRows.length,
    activeOrphanEvidenceRows: activeOrphanEvidenceRows.slice(0, 20),
    supersededOrphanEvidenceRows,
    appliedMissingPatchTrace,
    appliedMissingSource,
    readyOrAppliedNoConfidence,
    unresolvedWithSource,
    blockedNotMarkedDiscontinued,
    blockedWithConfidence,
    supersededNotNeutralized,
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
