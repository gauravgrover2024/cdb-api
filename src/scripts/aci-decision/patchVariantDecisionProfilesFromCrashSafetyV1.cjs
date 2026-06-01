#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const CRASH_COLLECTION = process.env.ACI_CRASH_SAFETY_PROFILE_COLLECTION || 'aci_vehicle_crash_safety_profile';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const args = process.argv.slice(2);
const write = args.includes('--write');
const force = args.includes('--force');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const hasRating = (doc) =>
  Boolean(doc.globalNcapAdult || doc.globalNcapChild || doc.bharatNcapAdult || doc.bharatNcapChild);

const compactSet = (obj) => {
  const out = {};
  for (const [key, value] of Object.entries(obj)) {
    if (value !== undefined) out[key] = value;
  }
  return out;
};

const buildPatch = (crashDoc) => {
  const nowDate = new Date();

  return compactSet({
    'safetyBasis.globalNcapAdult': crashDoc.globalNcapAdult || null,
    'safetyBasis.globalNcapChild': crashDoc.globalNcapChild || null,
    'safetyBasis.bharatNcapAdult': crashDoc.bharatNcapAdult || null,
    'safetyBasis.bharatNcapChild': crashDoc.bharatNcapChild || null,

    'safetyBasis.crashRatingSource': 'internal_feature_matrix',
    'safetyBasis.crashRatingSourceCollection': crashDoc.sourceCollection || CRASH_COLLECTION,
    'safetyBasis.crashRatingSourceProfileKey': crashDoc.crashSafetyProfileKey,
    'safetyBasis.crashRatingSourceFeatureDocId': crashDoc.sourceFeatureDocId || null,
    'safetyBasis.crashRatingTestedVariant': crashDoc.sourceFeatureVariant || null,

    // This means our internal variant feature matrix has a variant-level rating row.
    // It does NOT mean official tested-variant applicability has been fully verified.
    'safetyBasis.crashRatingAppliesToVariant': true,
    'safetyBasis.crashRatingAppliesToAllVariants': false,
    'safetyBasis.crashRatingNeedsOfficialVerification': true,
    'safetyBasis.crashRatingApplicabilityCaveat':
      'Imported from internal variant feature matrix; official tested-variant applicability still needs verification before final safety judgement.',

    'dataQuality.hasSafetyData': true,

    updatedAt: nowDate,
  });
};

const flushBulk = async (collection, bulk) => {
  if (!bulk.length) return { matched: 0, modified: 0 };
  const result = await collection.bulkWrite(bulk, { ordered: false });
  return {
    matched: result.matchedCount || 0,
    modified: result.modifiedCount || 0,
  };
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  const db = mongoose.connection.db;

  const crashCol = db.collection(CRASH_COLLECTION);
  const profilesCol = db.collection(PROFILE_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, force=${force}`);
  console.log(`[source] ${CRASH_COLLECTION}`);
  console.log(`[target] ${PROFILE_COLLECTION}`);

  const crashDocs = await crashCol.find({
    hasCrashRating: true,
  }, {
    projection: {
      _id: 0,
      crashSafetyProfileKey: 1,
      variantProfileKey: 1,
      variantFullName: 1,
      globalNcapAdult: 1,
      globalNcapChild: 1,
      bharatNcapAdult: 1,
      bharatNcapChild: 1,
      sourceCollection: 1,
      sourceFeatureDocId: 1,
      sourceFeatureVariant: 1,
      sourceActivePricelistMatched: 1,
      needsOfficialVerification: 1,
      reviewStatus: 1,
    }
  }).toArray();

  const profileKeys = crashDocs.map((doc) => doc.variantProfileKey).filter(Boolean);

  const existingProfiles = await profilesCol.find({
    variantProfileKey: { $in: profileKeys },
  }, {
    projection: {
      _id: 0,
      variantProfileKey: 1,
      variantFullName: 1,
      safetyBasis: 1,
    }
  }).toArray();

  const profileByKey = new Map(existingProfiles.map((p) => [p.variantProfileKey, p]));

  const updates = [];
  const missingProfiles = [];
  const skippedExisting = [];
  const samples = [];

  for (const crashDoc of crashDocs) {
    if (!hasRating(crashDoc)) continue;

    const profile = profileByKey.get(crashDoc.variantProfileKey);

    if (!profile) {
      missingProfiles.push({
        variantProfileKey: crashDoc.variantProfileKey,
        variantFullName: crashDoc.variantFullName,
      });
      continue;
    }

    const alreadyHasCrash =
      profile.safetyBasis?.globalNcapAdult ||
      profile.safetyBasis?.globalNcapChild ||
      profile.safetyBasis?.bharatNcapAdult ||
      profile.safetyBasis?.bharatNcapChild ||
      profile.safetyBasis?.crashRatingSource;

    if (alreadyHasCrash && !force) {
      skippedExisting.push({
        variantProfileKey: crashDoc.variantProfileKey,
        variantFullName: crashDoc.variantFullName,
      });
      continue;
    }

    const set = buildPatch(crashDoc);

    updates.push({
      variantProfileKey: crashDoc.variantProfileKey,
      variantFullName: crashDoc.variantFullName,
      set,
    });

    if (samples.length < 30) {
      samples.push({
        variantFullName: crashDoc.variantFullName,
        globalNcapAdult: crashDoc.globalNcapAdult?.value || null,
        globalNcapChild: crashDoc.globalNcapChild?.value || null,
        bharatNcapAdult: crashDoc.bharatNcapAdult?.value || null,
        bharatNcapChild: crashDoc.bharatNcapChild?.value || null,
        sourceActivePricelistMatched: crashDoc.sourceActivePricelistMatched,
        caveat: set['safetyBasis.crashRatingApplicabilityCaveat'],
      });
    }
  }

  let writeResult = null;

  if (write) {
    let bulk = [];
    let matched = 0;
    let modified = 0;

    for (const update of updates) {
      bulk.push({
        updateOne: {
          filter: { variantProfileKey: update.variantProfileKey },
          update: { $set: update.set },
        },
      });

      if (bulk.length >= 500) {
        const result = await flushBulk(profilesCol, bulk);
        matched += result.matched;
        modified += result.modified;
        bulk = [];
      }
    }

    const finalResult = await flushBulk(profilesCol, bulk);
    matched += finalResult.matched;
    modified += finalResult.modified;

    writeResult = { matched, modified };
  }

  const byProgram = {
    globalAdult: updates.filter((u) => u.set['safetyBasis.globalNcapAdult']).length,
    globalChild: updates.filter((u) => u.set['safetyBasis.globalNcapChild']).length,
    bharatAdult: updates.filter((u) => u.set['safetyBasis.bharatNcapAdult']).length,
    bharatChild: updates.filter((u) => u.set['safetyBasis.bharatNcapChild']).length,
  };

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    sourceCrashProfiles: crashDocs.length,
    existingDecisionProfiles: existingProfiles.length,
    updateCandidates: updates.length,
    missingProfiles: missingProfiles.length,
    skippedExisting: skippedExisting.length,
    byProgram,
    missingProfileSamples: missingProfiles.slice(0, 20),
    skippedExistingSamples: skippedExisting.slice(0, 20),
    samples,
    writeResult,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
