#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');
const { buildVariantDecisionProfileFromSources } = require('../../services/aciCore/decisionProfiles/aciVariantDecisionProfile.builder.cjs');

const EVIDENCE_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';
const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const args = process.argv.slice(2);
const write = args.includes('--write');
const force = args.includes('--force');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const preserveCrashSafetyFields = (existingSafety = {}, candidateSafety = {}) => ({
  ...candidateSafety,

  globalNcapAdult: existingSafety.globalNcapAdult ?? candidateSafety.globalNcapAdult ?? null,
  globalNcapChild: existingSafety.globalNcapChild ?? candidateSafety.globalNcapChild ?? null,
  bharatNcapAdult: existingSafety.bharatNcapAdult ?? candidateSafety.bharatNcapAdult ?? null,
  bharatNcapChild: existingSafety.bharatNcapChild ?? candidateSafety.bharatNcapChild ?? null,

  crashRatingSource: existingSafety.crashRatingSource ?? candidateSafety.crashRatingSource ?? null,
  crashRatingSourceCollection: existingSafety.crashRatingSourceCollection ?? candidateSafety.crashRatingSourceCollection ?? null,
  crashRatingSourceProfileKey: existingSafety.crashRatingSourceProfileKey ?? candidateSafety.crashRatingSourceProfileKey ?? null,
  crashRatingSourceFeatureDocId: existingSafety.crashRatingSourceFeatureDocId ?? candidateSafety.crashRatingSourceFeatureDocId ?? null,
  crashRatingTestedVariant: existingSafety.crashRatingTestedVariant ?? candidateSafety.crashRatingTestedVariant ?? null,
  crashRatingAppliesToVariant: existingSafety.crashRatingAppliesToVariant ?? candidateSafety.crashRatingAppliesToVariant ?? null,
  crashRatingAppliesToAllVariants: existingSafety.crashRatingAppliesToAllVariants ?? candidateSafety.crashRatingAppliesToAllVariants ?? null,
  crashRatingNeedsOfficialVerification: existingSafety.crashRatingNeedsOfficialVerification ?? candidateSafety.crashRatingNeedsOfficialVerification ?? null,
  crashRatingApplicabilityCaveat: existingSafety.crashRatingApplicabilityCaveat ?? candidateSafety.crashRatingApplicabilityCaveat ?? null,
});

const pseudoPriceRowFromProfile = (profile) => ({
  ...profile,
  make: profile.make,
  makeKey: profile.makeKey,
  model: profile.model,
  modelKey: profile.modelKey,
  fullModel: profile.fullModel,
  variant: profile.variant,
  variantKey: profile.variantKey,
  variantFullName: profile.variantFullName,
  fuel: profile.fuel,
  fuelKey: profile.fuelKey,
  transmission: profile.transmission,
  transmissionKey: profile.transmissionKey,
  gearbox: profile.gearbox,

  exShowroomPrice: profile.referenceExShowroomPrice,
  onRoadPrice: profile.referenceOnRoadPrice,
  citySlug: profile.referencePriceCitySlug,
  updatedAt: profile.referencePriceUpdatedAt,

  sourceVehicleId: profile.sourceVehicleId,
  bodyType: profile.bodyType,
  bodyTypeKey: profile.bodyTypeKey,
});

const pickPatch = ({ existing, candidate, evidence }) => {
  const nowDate = new Date();

  const mergedSafetyBasis = preserveCrashSafetyFields(existing.safetyBasis || {}, candidate.safetyBasis || {});

  const dataQuality = {
    ...(existing.dataQuality || {}),
    ...(candidate.dataQuality || {}),
    hasPrice: existing.dataQuality?.hasPrice ?? candidate.dataQuality?.hasPrice ?? true,
    hasFeatureMatrix: true,
    confidenceTier: 'high',
    needsReview: existing.dataQuality?.needsReview ?? false,
    sourceFeatureDocId: evidence.normalizedFields?.sourceFeatureDocId || null,
    sourceFeatureVariantKey: evidence.normalizedFields?.sourceFeatureVariantKey || null,
    sourceFeatureEvidenceStatus: 'internal_source_ready',
  };

  const missingCriticalFields = Array.isArray(dataQuality.missingCriticalFields)
    ? dataQuality.missingCriticalFields.filter((field) => field !== 'feature_matrix')
    : [];

  dataQuality.missingCriticalFields = missingCriticalFields;

  return {
    engineCc: candidate.engineCc ?? existing.engineCc ?? null,
    cylinders: candidate.cylinders ?? existing.cylinders ?? null,
    turbo: candidate.turbo ?? existing.turbo ?? null,
    supercharged: candidate.supercharged ?? existing.supercharged ?? null,
    powerBhp: candidate.powerBhp ?? existing.powerBhp ?? null,
    torqueNm: candidate.torqueNm ?? existing.torqueNm ?? null,
    batteryCapacityKwh: candidate.batteryCapacityKwh ?? existing.batteryCapacityKwh ?? null,
    motorPowerBhp: candidate.motorPowerBhp ?? existing.motorPowerBhp ?? null,
    motorTorqueNm: candidate.motorTorqueNm ?? existing.motorTorqueNm ?? null,
    claimedRangeKm: candidate.claimedRangeKm ?? existing.claimedRangeKm ?? null,

    featureFlags: candidate.featureFlags || existing.featureFlags || {},
    featureEvidence: candidate.featureEvidence || existing.featureEvidence || {},
    missingFeatureKeys: candidate.missingFeatureKeys || existing.missingFeatureKeys || [],
    conflictedFeatureKeys: candidate.conflictedFeatureKeys || existing.conflictedFeatureKeys || [],

    safetyBasis: mergedSafetyBasis,
    safetyTier: candidate.safetyTier ?? existing.safetyTier ?? null,
    safetyStrengths: candidate.safetyStrengths || existing.safetyStrengths || [],
    safetyMissingCriticals: candidate.safetyMissingCriticals || existing.safetyMissingCriticals || [],
    safetyCaveats: candidate.safetyCaveats || existing.safetyCaveats || [],

    performanceBasis: candidate.performanceBasis || existing.performanceBasis || {},
    performanceTier: candidate.performanceTier ?? existing.performanceTier ?? null,
    performanceStrengths: candidate.performanceStrengths || existing.performanceStrengths || [],
    performanceCaveats: candidate.performanceCaveats || existing.performanceCaveats || [],

    mileageBasis: candidate.mileageBasis || existing.mileageBasis || {},
    mileageTier: candidate.mileageTier ?? existing.mileageTier ?? null,
    runningCostTier: candidate.runningCostTier ?? existing.runningCostTier ?? null,
    mileageCaveats: candidate.mileageCaveats || existing.mileageCaveats || [],

    practicalityBasis: candidate.practicalityBasis || existing.practicalityBasis || {},
    practicalityTier: candidate.practicalityTier ?? existing.practicalityTier ?? null,
    practicalityStrengths: candidate.practicalityStrengths || existing.practicalityStrengths || [],
    practicalityCaveats: candidate.practicalityCaveats || existing.practicalityCaveats || [],

    comfortBasis: candidate.comfortBasis || existing.comfortBasis || {},
    comfortTier: candidate.comfortTier ?? existing.comfortTier ?? null,
    premiumFeelTier: candidate.premiumFeelTier ?? existing.premiumFeelTier ?? null,
    comfortStrengths: candidate.comfortStrengths || existing.comfortStrengths || [],
    comfortCaveats: candidate.comfortCaveats || existing.comfortCaveats || [],

    dataStatus: 'enriched',
    dataQuality,

    'scoreEvidence.missingDataWarnings': Array.isArray(existing.scoreEvidence?.missingDataWarnings)
      ? existing.scoreEvidence.missingDataWarnings.filter((warning) => warning !== 'Missing feature_matrix')
      : [],

    updatedAt: nowDate,
  };
};

const readFeatureDoc = async (featureCol, id) => {
  if (!id) return null;

  const queries = [];

  if (mongoose.Types.ObjectId.isValid(id)) {
    queries.push({ _id: new mongoose.Types.ObjectId(id) });
  }

  queries.push({ _id: id });

  for (const query of queries) {
    const doc = await featureCol.findOne(query);
    if (doc) return doc;
  }

  return null;
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const evidenceCol = db.collection(EVIDENCE_COLLECTION);
  const featureCol = db.collection(FEATURE_COLLECTION);
  const profileCol = db.collection(PROFILE_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, force=${force}`);
  console.log(`[source] evidence=${EVIDENCE_COLLECTION}, features=${FEATURE_COLLECTION}`);
  console.log(`[target] ${PROFILE_COLLECTION}`);

  const evidenceRows = await evidenceCol.find({
    evidenceType: 'feature_matrix',
    status: 'internal_source_ready',
    reviewStatus: 'auto_exact_feature_matrix_match',
  }).sort({ variantProfileKey: 1 }).toArray();

  const updates = [];
  const missingProfiles = [];
  const missingFeatureDocs = [];
  const skippedAlreadyHasFeature = [];
  const samples = [];

  for (const evidence of evidenceRows) {
    const existing = await profileCol.findOne({ variantProfileKey: evidence.variantProfileKey });

    if (!existing) {
      missingProfiles.push(evidence.variantProfileKey);
      continue;
    }

    if (existing.dataQuality?.hasFeatureMatrix === true && !force) {
      skippedAlreadyHasFeature.push(evidence.variantProfileKey);
      continue;
    }

    const featureDoc = await readFeatureDoc(featureCol, evidence.normalizedFields?.sourceFeatureDocId);

    if (!featureDoc) {
      missingFeatureDocs.push({
        variantProfileKey: evidence.variantProfileKey,
        sourceFeatureDocId: evidence.normalizedFields?.sourceFeatureDocId || null,
      });
      continue;
    }

    const candidate = buildVariantDecisionProfileFromSources({
      priceRow: pseudoPriceRowFromProfile(existing),
      featureDoc,
      modelSummary: null,
    });

    const patch = pickPatch({ existing, candidate, evidence });

    updates.push({
      variantProfileKey: evidence.variantProfileKey,
      variantFullName: evidence.variantFullName,
      sourceFeatureDocId: evidence.normalizedFields?.sourceFeatureDocId,
      patch,
    });

    if (samples.length < 30) {
      samples.push({
        variantProfileKey: evidence.variantProfileKey,
        variantFullName: evidence.variantFullName,
        sourceFeatureVariant: evidence.normalizedFields?.sourceFeatureVariant,
        hasFeatureMatrixBefore: existing.dataQuality?.hasFeatureMatrix,
        hasFeatureMatrixAfter: patch.dataQuality.hasFeatureMatrix,
        engineCc: patch.engineCc,
        powerBhp: patch.powerBhp,
        torqueNm: patch.torqueNm,
        featureFlagCount: Object.values(patch.featureFlags || {}).filter((v) => v !== null && v !== undefined).length,
        missingFeatureKeys: patch.missingFeatureKeys?.length || 0,
        hasSafetyData: patch.dataQuality.hasSafetyData,
        hasPerformanceData: patch.dataQuality.hasPerformanceData,
        hasMileageData: patch.dataQuality.hasMileageData,
        hasDimensionsData: patch.dataQuality.hasDimensionsData,
      });
    }
  }

  let writeResult = null;

  if (write) {
    let matched = 0;
    let modified = 0;

    if (updates.length) {
      const bulk = updates.map((update) => ({
        updateOne: {
          filter: { variantProfileKey: update.variantProfileKey },
          update: { $set: update.patch },
        },
      }));

      const result = await profileCol.bulkWrite(bulk, { ordered: false });
      matched += result.matchedCount || 0;
      modified += result.modifiedCount || 0;

      const evidenceBulk = updates.map((update) => ({
        updateOne: {
          filter: {
            evidenceType: 'feature_matrix',
            variantProfileKey: update.variantProfileKey,
            status: 'internal_source_ready',
          },
          update: {
            $set: {
              status: 'applied_to_profile',
              reviewStatus: 'profile_patch_applied',
              profilePatchedAt: new Date(),
              updatedAt: new Date(),
            },
          },
        },
      }));

      await evidenceCol.bulkWrite(evidenceBulk, { ordered: false });
    }

    writeResult = { matched, modified };
  }

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    evidenceRows: evidenceRows.length,
    updateCandidates: updates.length,
    missingProfiles: missingProfiles.length,
    missingFeatureDocs: missingFeatureDocs.length,
    skippedAlreadyHasFeature: skippedAlreadyHasFeature.length,
    samples,
    missingProfilesSamples: missingProfiles.slice(0, 20),
    missingFeatureDocSamples: missingFeatureDocs.slice(0, 20),
    skippedSamples: skippedAlreadyHasFeature.slice(0, 20),
    writeResult,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
