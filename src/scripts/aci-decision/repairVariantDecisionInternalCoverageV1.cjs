#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const {
  buildVariantDecisionProfileFromSources,
} = require('../../services/aciCore/decisionProfiles/aciVariantDecisionProfile.builder.cjs');

const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const FEATURE_COLLECTION = process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const MODEL_SUMMARY_COLLECTION = process.env.ACI_MODEL_SUMMARY_COLLECTION || 'aci_vehicle_model_summary';

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const args = process.argv.slice(2);
const write = args.includes('--write');

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const hyphenKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const uniq = (arr) => [...new Set(arr.filter(Boolean))];

const makeBrandModelKey = (profile) => {
  const makeKey = profile.makeKey || normalizeKey(profile.make);
  const modelKey = profile.modelKey || normalizeKey(profile.model);
  return makeKey && modelKey ? `${makeKey}_${modelKey}` : null;
};

const inferTransmission = (profile) => {
  const text = [
    profile.variant,
    profile.variantKey,
    profile.variantFullName,
    profile.gearbox,
    profile.fuelTransmissionFamilyKey,
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();

  if (profile.fuelKey === 'electric' || String(profile.fuel || '').toLowerCase() === 'electric') {
    return {
      transmission: 'Automatic',
      transmissionKey: 'automatic',
      reason: 'electric_vehicle_defaults_to_automatic_user_facing',
      safe: true,
    };
  }

  if (/\b(dct|cvt|ivt|amt|automatic|auto)\b/i.test(text) || /(^|[-_\s])at($|[-_\s])/i.test(text)) {
    return {
      transmission: 'Automatic',
      transmissionKey: 'automatic',
      reason: 'variant_text_contains_automatic_signal',
      safe: true,
    };
  }

  if (/\b(mt|manual)\b/i.test(text)) {
    return {
      transmission: 'Manual',
      transmissionKey: 'manual',
      reason: 'variant_text_contains_manual_signal',
      safe: true,
    };
  }

  return {
    transmission: null,
    transmissionKey: null,
    reason: 'no_safe_signal',
    safe: false,
  };
};

const buildFeatureLookup = (profile) => {
  const makeKeys = uniq([
    profile.makeKey,
    normalizeKey(profile.makeKey),
    normalizeKey(profile.make),
    hyphenKey(profile.makeKey),
  ]);

  const modelKeys = uniq([
    profile.modelKey,
    normalizeKey(profile.modelKey),
    hyphenKey(profile.modelKey),
    normalizeKey(profile.model),
    hyphenKey(profile.model),
  ]);

  const variantBaseWithoutDualTone = String(profile.variantKey || profile.variant || '')
    .replace(/dual[-_ ]tone/gi, '')
    .replace(/dt$/i, '')
    .trim();

  const variantKeys = uniq([
    profile.variantKey,
    normalizeKey(profile.variantKey),
    hyphenKey(profile.variantKey),
    normalizeKey(profile.variant),
    hyphenKey(profile.variant),
    normalizeKey(variantBaseWithoutDualTone),
    hyphenKey(variantBaseWithoutDualTone),
  ]);

  return {
    makeKeys,
    modelKeys,
    variantKeys,
    query: {
      $or: [
        { modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys }, activePricelistMatched: true },
        { modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys } },
        { makeKey: { $in: makeKeys }, modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys } },
        { brandKey: { $in: makeKeys }, modelKey: { $in: modelKeys }, variantKey: { $in: variantKeys } },
      ],
    },
  };
};

const chooseFeatureDoc = (docs) => {
  if (!docs.length) return null;
  const active = docs.find((doc) => doc.activePricelistMatched === true);
  if (active) return active;

  const notDiscontinued = docs.find((doc) => doc.discontinuedPricelistMatched !== true);
  if (notDiscontinued) return notDiscontinued;

  return docs[0];
};

const getModelSummary = async (modelSummaries, profile) => {
  const brandModelKey = makeBrandModelKey(profile);
  const modelKeyCandidates = uniq([profile.modelKey, normalizeKey(profile.modelKey), hyphenKey(profile.modelKey)]);

  if (brandModelKey) {
    const byBrandModel = await modelSummaries.findOne({ brandModelKey });
    if (byBrandModel) return byBrandModel;
  }

  return modelSummaries.findOne({
    $or: [
      { modelKey: { $in: modelKeyCandidates } },
      { makeKey: profile.makeKey, modelKey: { $in: modelKeyCandidates } },
      { brandKey: profile.makeKey, modelKey: { $in: modelKeyCandidates } },
    ],
  });
};

const buildRepairPriceLike = (profile) => {
  const inferred = inferTransmission(profile);
  const shouldApplyTransmission =
    (!profile.transmissionKey || profile.fuelTransmissionFamilyKey?.includes('unknown_transmission')) &&
    inferred.safe;

  const repaired = {
    ...profile,

    // Existing decision profiles store price under reference* fields.
    // The profile builder expects price-row-style fields.
    exShowroomPrice: profile.referenceExShowroomPrice ?? profile.exShowroomPrice ?? null,
    onRoadPrice: profile.referenceOnRoadPrice ?? profile.onRoadPrice ?? null,
    onRoadPriceWithoutOptional:
      profile.referenceOnRoadPriceWithoutOptional ??
      profile.onRoadPriceWithoutOptional ??
      profile.referenceOnRoadPrice ??
      profile.onRoadPrice ??
      null,
    citySlug: profile.referencePriceCitySlug ?? profile.citySlug ?? null,
    priceUpdatedAt: profile.referencePriceUpdatedAt ?? profile.priceUpdatedAt ?? profile.updatedAt ?? null,

    // Keep existing identifiers explicit.
    make: profile.make,
    makeKey: profile.makeKey,
    model: profile.model,
    modelKey: profile.modelKey,
    fullModel: profile.fullModel,
    variant: profile.variant,
    variantKey: profile.variantKey,
    variantFullName: profile.variantFullName,
    sourceVehicleId: profile.sourceVehicleId,
    lifecycleStatus: profile.lifecycleStatus,
    fuel: profile.fuel,
    fuelKey: profile.fuelKey,
    transmission: profile.transmission,
    transmissionKey: profile.transmissionKey,
    gearbox: profile.gearbox,
  };

  if (shouldApplyTransmission) {
    repaired.transmission = inferred.transmission;
    repaired.transmissionKey = inferred.transmissionKey;
    repaired.transmissionRepairEvidence = {
      source: 'internal_variant_text_inference',
      reason: inferred.reason,
      repairedAt: new Date(),
    };
  }

  return {
    repaired,
    inferred,
    appliedTransmissionRepair: shouldApplyTransmission,
  };
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI.');
    process.exit(1);
  }

  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;

  const profiles = db.collection(PROFILE_COLLECTION);
  const features = db.collection(FEATURE_COLLECTION);
  const modelSummaries = db.collection(MODEL_SUMMARY_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}`);

  const candidates = await profiles
    .find({
      $or: [
        { 'dataQuality.hasFeatureMatrix': false },
        { fuelTransmissionFamilyKey: /unknown_transmission/ },
      ],
    })
    .toArray();

  const repaired = [];
  const skipped = [];
  const deletedOldKeys = [];

  for (const profile of candidates) {
    const lookup = buildFeatureLookup(profile);
    const docs = await features.find(lookup.query).limit(5).toArray();
    const featureDoc = chooseFeatureDoc(docs);

    const { repaired: priceLike, inferred, appliedTransmissionRepair } = buildRepairPriceLike(profile);
    const modelSummary = await getModelSummary(modelSummaries, profile);

    const shouldRebuild = Boolean(featureDoc) || appliedTransmissionRepair;

    if (!shouldRebuild) {
      skipped.push({
        variantProfileKey: profile.variantProfileKey,
        variantFullName: profile.variantFullName,
        reason: 'no_internal_feature_candidate_and_no_safe_transmission_repair',
        inferred,
        lookupAttempt: {
          makeKeys: lookup.makeKeys,
          modelKeys: lookup.modelKeys,
          variantKeys: lookup.variantKeys,
        },
      });
      continue;
    }

    const newProfile = buildVariantDecisionProfileFromSources({
      priceRow: priceLike,
      featureDoc,
      modelSummary,
    });

    if (priceLike.transmissionRepairEvidence) {
      newProfile.transmissionRepairEvidence = priceLike.transmissionRepairEvidence;
    }

    newProfile.internalRepair = {
      repairedAt: new Date(),
      featureMatrixRepaired: Boolean(featureDoc),
      transmissionRepaired: appliedTransmissionRepair,
      oldVariantProfileKey: profile.variantProfileKey,
      featureDocId: featureDoc ? String(featureDoc._id) : null,
      featureLookup: {
        modelKey: featureDoc?.modelKey || null,
        variantKey: featureDoc?.variantKey || null,
        activePricelistMatched: featureDoc?.activePricelistMatched ?? null,
        discontinuedPricelistMatched: featureDoc?.discontinuedPricelistMatched ?? null,
      },
    };

    repaired.push({
      oldVariantProfileKey: profile.variantProfileKey,
      newVariantProfileKey: newProfile.variantProfileKey,
      variantFullName: profile.variantFullName,
      featureMatrixRepaired: Boolean(featureDoc),
      transmissionRepaired: appliedTransmissionRepair,
      inferred,
      featureDoc: featureDoc
        ? {
            _id: String(featureDoc._id),
            modelKey: featureDoc.modelKey,
            variantKey: featureDoc.variantKey,
            activePricelistMatched: featureDoc.activePricelistMatched,
            discontinuedPricelistMatched: featureDoc.discontinuedPricelistMatched,
          }
        : null,
      newDataQuality: newProfile.dataQuality,
    });

    if (write) {
      const { createdAt, ...setDoc } = newProfile;

      await profiles.updateOne(
        { variantProfileKey: newProfile.variantProfileKey },
        {
          $set: setDoc,
          $setOnInsert: { createdAt: createdAt || new Date() },
        },
        { upsert: true }
      );

      if (newProfile.variantProfileKey !== profile.variantProfileKey) {
        await profiles.deleteOne({ variantProfileKey: profile.variantProfileKey });
        deletedOldKeys.push(profile.variantProfileKey);
      }
    }
  }

  const postSummary = write
    ? {
        total: await profiles.countDocuments(),
        missingFeatureMatrix: await profiles.countDocuments({ 'dataQuality.hasFeatureMatrix': false }),
        unknownTransmission: await profiles.countDocuments({ fuelTransmissionFamilyKey: /unknown_transmission/ }),
        high: await profiles.countDocuments({ 'dataQuality.confidenceTier': 'high' }),
        medium: await profiles.countDocuments({ 'dataQuality.confidenceTier': 'medium' }),
      }
    : null;

  console.log(
    JSON.stringify(
      {
        mode: write ? 'WRITE' : 'DRY_RUN',
        scanned: candidates.length,
        repairedCount: repaired.length,
        skippedCount: skipped.length,
        deletedOldKeys,
        repaired,
        skipped,
        postSummary,
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
