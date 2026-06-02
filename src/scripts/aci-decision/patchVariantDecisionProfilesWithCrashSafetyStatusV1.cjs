#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const WRITE = process.argv.includes('--write');

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const CRASH_COLLECTION =
  process.env.ACI_CRASH_SAFETY_PROFILE_COLLECTION || 'aci_vehicle_crash_safety_profile';

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const ratingNumber = (value) => {
  if (value === null || value === undefined || value === '') return null;
  if (typeof value === 'number') return value;
  const match = String(value).match(/(\d+(?:\.\d+)?)/);
  return match ? Number(match[1]) : null;
};

const ratingSignatureObject = (doc = {}) => ({
  globalAdult: ratingNumber(doc.globalNcapAdult?.stars ?? doc.globalNcapAdult?.value),
  globalChild: ratingNumber(doc.globalNcapChild?.stars ?? doc.globalNcapChild?.value),
  bharatAdult: ratingNumber(doc.bharatNcapAdult?.stars ?? doc.bharatNcapAdult?.value),
  bharatChild: ratingNumber(doc.bharatNcapChild?.stars ?? doc.bharatNcapChild?.value),
});

const signatureKey = (doc = {}) => JSON.stringify(ratingSignatureObject(doc));

const hasAnyRating = (doc = {}) =>
  Object.values(ratingSignatureObject(doc)).some((value) => value !== null && value !== undefined);

const statusForCrashDoc = (doc = {}) => {
  if (doc.applicabilityScope === 'inherited_model_level_consistent') {
    return 'inherited_model_level_consistent_needs_official_verification';
  }

  if (doc.needsOfficialVerification || doc.reviewStatus === 'needs_official_applicability_check') {
    return 'internal_variant_feature_matrix_needs_official_verification';
  }

  return 'verified_variant';
};

(async () => {
  const uri = process.env.MONGODB_URI || process.env.MONGO_URI;
  if (!uri) throw new Error('Missing Mongo URI');

  await mongoose.connect(uri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const profiles = db.collection(PROFILE_COLLECTION);
  const crash = db.collection(CRASH_COLLECTION);
  const gaps = db.collection(GAP_COLLECTION);

  console.log(`[mode] ${WRITE ? 'WRITE' : 'DRY_RUN'}`);
  console.log(`[source] profiles=${PROFILE_COLLECTION}, crash=${CRASH_COLLECTION}, gaps=${GAP_COLLECTION}`);

  const profileDocs = await profiles.find(
    {},
    {
      projection: {
        _id: 0,
        variantProfileKey: 1,
        variantFullName: 1,
        makeKey: 1,
        modelKey: 1,
        safetyBasis: 1,
      },
    },
  ).toArray();

  const crashDocs = await crash.find(
    { hasCrashRating: true },
    {
      projection: {
        _id: 0,
        crashSafetyProfileKey: 1,
        variantProfileKey: 1,
        variantFullName: 1,
        makeKey: 1,
        modelKey: 1,
        globalNcapAdult: 1,
        globalNcapChild: 1,
        bharatNcapAdult: 1,
        bharatNcapChild: 1,
        confidence: 1,
        applicabilityScope: 1,
        reviewStatus: 1,
        needsOfficialVerification: 1,
        sourceVersion: 1,
        sourceCollection: 1,
        sourceCrashProfileCount: 1,
      },
    },
  ).toArray();

  const gapDocs = await gaps.find(
    { status: 'open', gapType: 'crash_rating_missing' },
    {
      projection: {
        _id: 0,
        variantProfileKey: 1,
        makeKey: 1,
        modelKey: 1,
        priority: 1,
      },
    },
  ).toArray();

  const crashByVariant = new Map();
  const sourceCrashByModel = new Map();
  const openCrashGapByVariant = new Map();

  for (const gap of gapDocs) {
    openCrashGapByVariant.set(gap.variantProfileKey, gap);
  }

  for (const doc of crashDocs) {
    if (doc.variantProfileKey && !crashByVariant.has(doc.variantProfileKey)) {
      crashByVariant.set(doc.variantProfileKey, doc);
    }

    if (doc.applicabilityScope !== 'inherited_model_level_consistent') {
      const modelKey = `${doc.makeKey || ''}__${doc.modelKey || ''}`;
      if (!sourceCrashByModel.has(modelKey)) sourceCrashByModel.set(modelKey, []);
      sourceCrashByModel.get(modelKey).push(doc);
    }
  }

  const getModelCrashClass = (profile) => {
    const key = `${profile.makeKey || ''}__${profile.modelKey || ''}`;
    const docs = sourceCrashByModel.get(key) || [];

    if (!docs.length) {
      return {
        status: 'unknown_or_not_publicly_verified',
        reason: 'no_internal_crash_source',
        signatureCount: 0,
      };
    }

    const signatures = new Set(docs.map(signatureKey));

    if (signatures.size > 1) {
      return {
        status: 'blocked_mixed_internal_ratings',
        reason: 'mixed_internal_crash_signatures',
        signatureCount: signatures.size,
      };
    }

    const [sig] = Array.from(signatures);
    const parsedSig = JSON.parse(sig);

    if (!Object.values(parsedSig).some((value) => value !== null && value !== undefined)) {
      return {
        status: 'unknown_or_not_publicly_verified',
        reason: 'internal_crash_source_without_rating_values',
        signatureCount: 1,
      };
    }

    return {
      status: 'candidate_model_level_rating_not_applied',
      reason: 'consistent_internal_model_rating_exists_but_no_variant_crash_doc',
      signatureCount: 1,
      signature: parsedSig,
    };
  };

  const updates = [];
  const counts = {};
  const samples = [];

  for (const profile of profileDocs) {
    const exactCrash = crashByVariant.get(profile.variantProfileKey);

    let statusPayload;

    if (exactCrash && hasAnyRating(exactCrash)) {
      statusPayload = {
        crashRatingStatus: statusForCrashDoc(exactCrash),
        crashRatingApplicabilityScope: exactCrash.applicabilityScope || null,
        crashRatingConfidence: exactCrash.confidence || null,
        crashRatingReviewStatus: exactCrash.reviewStatus || null,
        crashRatingNeedsOfficialVerification: Boolean(exactCrash.needsOfficialVerification),
        crashRatingSourceProfileKey: exactCrash.crashSafetyProfileKey || null,
        crashRatingSourceCollection: exactCrash.sourceCollection || CRASH_COLLECTION,
        crashRatingSourceVersion: exactCrash.sourceVersion || null,
        crashRatingSourceProfileCount: exactCrash.sourceCrashProfileCount || 1,
        crashRatingStatusReason: exactCrash.applicabilityScope === 'inherited_model_level_consistent'
          ? 'same_model_internal_crash_signature_consistent'
          : 'variant_has_internal_feature_matrix_crash_rating',
      };
    } else {
      const modelClass = getModelCrashClass(profile);
      const openGap = openCrashGapByVariant.get(profile.variantProfileKey);

      statusPayload = {
        crashRatingStatus: modelClass.status,
        crashRatingApplicabilityScope: null,
        crashRatingConfidence: null,
        crashRatingReviewStatus: modelClass.status === 'blocked_mixed_internal_ratings'
          ? 'blocked_mixed_internal_ratings'
          : 'not_publicly_verified_in_internal_sources',
        crashRatingNeedsOfficialVerification: false,
        crashRatingSourceProfileKey: null,
        crashRatingSourceCollection: null,
        crashRatingSourceVersion: null,
        crashRatingSourceProfileCount: 0,
        crashRatingStatusReason: modelClass.reason,
        crashRatingOpenGapPriority: openGap?.priority || null,
        crashRatingModelSignatureCount: modelClass.signatureCount,
      };
    }

    counts[statusPayload.crashRatingStatus] =
      (counts[statusPayload.crashRatingStatus] || 0) + 1;

    if (samples.length < 40) {
      samples.push({
        variantProfileKey: profile.variantProfileKey,
        variantFullName: profile.variantFullName,
        status: statusPayload.crashRatingStatus,
        reason: statusPayload.crashRatingStatusReason,
      });
    }

    updates.push({
      updateOne: {
        filter: { variantProfileKey: profile.variantProfileKey },
        update: {
          $set: Object.fromEntries(
            Object.entries(statusPayload).map(([key, value]) => [`safetyBasis.${key}`, value]),
          ),
        },
      },
    });
  }

  let writeResult = null;

  if (WRITE && updates.length) {
    const result = await profiles.bulkWrite(updates, { ordered: false });
    writeResult = {
      matched: result.matchedCount || 0,
      modified: result.modifiedCount || 0,
    };
  }

  console.log(JSON.stringify({
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    profilesScanned: profileDocs.length,
    crashProfilesLoaded: crashDocs.length,
    openCrashGapsLoaded: gapDocs.length,
    statusCounts: counts,
    writeResult,
    samples,
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
