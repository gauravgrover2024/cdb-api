#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const WRITE = process.argv.includes('--write');

const GAP_COLLECTION =
  process.env.ACI_VARIANT_DATA_GAP_QUEUE_COLLECTION || 'aci_variant_data_gap_queue';

const CRASH_COLLECTION =
  process.env.ACI_CRASH_SAFETY_PROFILE_COLLECTION || 'aci_vehicle_crash_safety_profile';

const SOURCE_VERSION = 'aci_inherited_model_level_crash_safety_v1_2026_06_02';

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

const hasAnyRating = (signature = {}) =>
  Object.values(signature).some((value) => value !== null && value !== undefined);

const clone = (value) => (value ? JSON.parse(JSON.stringify(value)) : null);

const buildInheritedDoc = ({ gap, representative, signature, sourceDocs }) => {
  const now = new Date();

  return {
    crashSafetyProfileKey: `${gap.variantProfileKey}__inherited_model_level_crash_rating`,
    variantProfileKey: gap.variantProfileKey,
    variantFullName: gap.variantFullName || null,
    variantKey: gap.variantKey || null,

    makeKey: gap.makeKey || representative.makeKey || null,
    make: representative.make || null,
    modelKey: gap.modelKey || representative.modelKey || null,
    model: representative.model || null,
    fullModel: representative.fullModel || null,

    fuelKey: gap.fuelKey || null,
    transmissionKey: gap.transmissionKey || null,
    fuelTransmissionFamilyKey:
      gap.fuelKey && gap.transmissionKey ? `${gap.fuelKey}_${gap.transmissionKey}` : null,

    hasCrashRating: true,
    globalNcapAdult: clone(representative.globalNcapAdult),
    globalNcapChild: clone(representative.globalNcapChild),
    bharatNcapAdult: clone(representative.bharatNcapAdult),
    bharatNcapChild: clone(representative.bharatNcapChild),

    confidence: 'internal_model_level_consistent',
    applicabilityScope: 'inherited_model_level_consistent',
    applicabilityNotes:
      'Inherited from a consistent same-model internal crash-rating signature. This is not official tested-variant confirmation and must remain flagged until official applicability is verified.',
    needsOfficialVerification: true,
    reviewStatus: 'needs_official_applicability_check',

    sourceCollection: CRASH_COLLECTION,
    sourceVersion: SOURCE_VERSION,
    sourceCrashSafetyProfileKeys: sourceDocs
      .map((doc) => doc.crashSafetyProfileKey)
      .filter(Boolean)
      .slice(0, 100),
    sourceVariantProfileKeys: sourceDocs
      .map((doc) => doc.variantProfileKey)
      .filter(Boolean)
      .slice(0, 100),
    sourceCrashProfileCount: sourceDocs.length,
    inheritedSignature: signature,

    createdAt: now,
    updatedAt: now,
  };
};

(async () => {
  const uri = process.env.MONGODB_URI || process.env.MONGO_URI;
  if (!uri) throw new Error('Missing Mongo URI');

  await mongoose.connect(uri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const gaps = db.collection(GAP_COLLECTION);
  const crash = db.collection(CRASH_COLLECTION);

  console.log(`[mode] ${WRITE ? 'WRITE' : 'DRY_RUN'}`);
  console.log(`[source] gaps=${GAP_COLLECTION}, crash=${CRASH_COLLECTION}`);
  console.log(`[target] ${CRASH_COLLECTION}`);

  const missing = await gaps.find(
    { status: 'open', priority: 'P0', gapType: 'crash_rating_missing' },
    {
      projection: {
        _id: 0,
        variantProfileKey: 1,
        variantFullName: 1,
        makeKey: 1,
        modelKey: 1,
        variantKey: 1,
        fuelKey: 1,
        transmissionKey: 1,
      },
    },
  ).toArray();

  const byModel = new Map();

  for (const gap of missing) {
    const key = `${gap.makeKey || ''}__${gap.modelKey || ''}`;
    if (!byModel.has(key)) {
      byModel.set(key, {
        makeKey: gap.makeKey,
        modelKey: gap.modelKey,
        gaps: [],
      });
    }
    byModel.get(key).gaps.push(gap);
  }

  const docsToWrite = [];
  const blockedMixedModels = [];
  const noInternalSourceModels = [];
  const candidateModels = [];
  const skippedAlreadyHasCrash = [];

  for (const model of byModel.values()) {
    const sourceDocs = await crash.find(
      {
        makeKey: model.makeKey,
        modelKey: model.modelKey,
        hasCrashRating: true,
        applicabilityScope: { $ne: 'inherited_model_level_consistent' },
      },
      {
        projection: {
          _id: 0,
          crashSafetyProfileKey: 1,
          variantProfileKey: 1,
          variantFullName: 1,
          make: 1,
          makeKey: 1,
          model: 1,
          modelKey: 1,
          fullModel: 1,
          globalNcapAdult: 1,
          globalNcapChild: 1,
          bharatNcapAdult: 1,
          bharatNcapChild: 1,
          confidence: 1,
          applicabilityScope: 1,
          reviewStatus: 1,
          needsOfficialVerification: 1,
          sourceCollection: 1,
          updatedAt: 1,
        },
      },
    ).toArray();

    if (!sourceDocs.length) {
      noInternalSourceModels.push({
        makeKey: model.makeKey,
        modelKey: model.modelKey,
        missingCount: model.gaps.length,
      });
      continue;
    }

    const bySignature = new Map();
    for (const doc of sourceDocs) {
      const sigKey = signatureKey(doc);
      if (!bySignature.has(sigKey)) bySignature.set(sigKey, []);
      bySignature.get(sigKey).push(doc);
    }

    if (bySignature.size !== 1) {
      blockedMixedModels.push({
        makeKey: model.makeKey,
        modelKey: model.modelKey,
        missingCount: model.gaps.length,
        signatureCount: bySignature.size,
        signatures: Array.from(bySignature.entries()).map(([key, docs]) => ({
          signature: JSON.parse(key),
          count: docs.length,
        })),
      });
      continue;
    }

    const [[sigKey, docs]] = Array.from(bySignature.entries());
    const signature = JSON.parse(sigKey);

    if (!hasAnyRating(signature)) {
      noInternalSourceModels.push({
        makeKey: model.makeKey,
        modelKey: model.modelKey,
        missingCount: model.gaps.length,
        reason: 'consistent_signature_without_rating_values',
      });
      continue;
    }

    const representative = docs[0];

    candidateModels.push({
      makeKey: model.makeKey,
      modelKey: model.modelKey,
      missingCount: model.gaps.length,
      sourceCrashProfiles: docs.length,
      signature,
    });

    for (const gap of model.gaps) {
      const alreadyHasCrash = await crash.countDocuments({
        variantProfileKey: gap.variantProfileKey,
        hasCrashRating: true,
      });

      if (alreadyHasCrash > 0) {
        skippedAlreadyHasCrash.push({
          variantProfileKey: gap.variantProfileKey,
          variantFullName: gap.variantFullName,
        });
        continue;
      }

      docsToWrite.push(buildInheritedDoc({
        gap,
        representative,
        signature,
        sourceDocs: docs,
      }));
    }
  }

  const summary = {
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    openP0CrashGapsScanned: missing.length,
    modelsScanned: byModel.size,
    candidateConsistentModels: candidateModels.length,
    noInternalSourceModels: noInternalSourceModels.length,
    blockedMixedModels: blockedMixedModels.length,
    inheritedDocsPrepared: docsToWrite.length,
    skippedAlreadyHasCrash: skippedAlreadyHasCrash.length,
    candidateMissingVariants: candidateModels.reduce((sum, row) => sum + row.missingCount, 0),
    noInternalSourceMissingVariants: noInternalSourceModels.reduce((sum, row) => sum + row.missingCount, 0),
    blockedMixedMissingVariants: blockedMixedModels.reduce((sum, row) => sum + row.missingCount, 0),
  };

  let writeResult = null;

  if (WRITE && docsToWrite.length) {
    const ops = docsToWrite.map((doc) => {
      const { createdAt, ...setDoc } = doc;

      return {
        updateOne: {
          filter: { crashSafetyProfileKey: doc.crashSafetyProfileKey },
          update: {
            $set: setDoc,
            $setOnInsert: { createdAt },
          },
          upsert: true,
        },
      };
    });

    const result = await crash.bulkWrite(ops, { ordered: false });
    writeResult = {
      upserted: result.upsertedCount || 0,
      modified: result.modifiedCount || 0,
      matched: result.matchedCount || 0,
    };
  }

  console.log(JSON.stringify({
    ...summary,
    writeResult,
    candidateModels: candidateModels
      .sort((a, b) => b.missingCount - a.missingCount)
      .slice(0, 40),
    blockedMixedModels,
    topNoInternalSourceModels: noInternalSourceModels
      .sort((a, b) => b.missingCount - a.missingCount)
      .slice(0, 40),
    sampleInheritedDocs: docsToWrite.slice(0, 10).map((doc) => ({
      crashSafetyProfileKey: doc.crashSafetyProfileKey,
      variantFullName: doc.variantFullName,
      signature: doc.inheritedSignature,
      applicabilityScope: doc.applicabilityScope,
      reviewStatus: doc.reviewStatus,
    })),
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
