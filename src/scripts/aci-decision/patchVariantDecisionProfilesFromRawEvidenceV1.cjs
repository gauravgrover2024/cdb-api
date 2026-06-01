#!/usr/bin/env node

try { require('dotenv').config(); } catch (_) {}

const mongoose = require('mongoose');

const EVIDENCE_COLLECTION = process.env.ACI_VARIANT_EXTERNAL_EVIDENCE_COLLECTION || 'aci_variant_external_evidence';
const PROFILE_COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const args = process.argv.slice(2);
const write = args.includes('--write');
const force = args.includes('--force');

const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;

const normalizeKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const hasValue = (value) => value !== null && value !== undefined && value !== '';

const setIfMissing = (patch, path, currentValue, nextValue) => {
  if (!hasValue(nextValue)) return;
  if (force || !hasValue(currentValue)) {
    patch[path] = nextValue;
  }
};

const removeWarnings = (warnings = []) =>
  Array.isArray(warnings)
    ? warnings.filter((warning) => {
        const w = String(warning || '').toLowerCase();
        return !(
          w.includes('unknown transmission') ||
          w.includes('missing transmission') ||
          w.includes('transmission missing')
        );
      })
    : [];

const buildPatch = ({ profile, evidence }) => {
  const nf = evidence.normalizedFields || {};
  const patch = {};
  const nowDate = new Date();

  const fuelKey = normalizeKey(profile.fuelKey || profile.fuel || nf.fuelType);
  const transmissionKey = normalizeKey(nf.transmissionKey || nf.transmission);

  if (hasValue(nf.transmission)) patch.transmission = nf.transmission;
  if (hasValue(transmissionKey)) patch.transmissionKey = transmissionKey;
  if (hasValue(nf.gearbox)) patch.gearbox = nf.gearbox;

  if (fuelKey && transmissionKey) {
    patch.fuelTransmissionFamilyKey = `${fuelKey}_${transmissionKey}`;
  }

  setIfMissing(patch, 'engineCc', profile.engineCc, nf.engineCc);
  setIfMissing(patch, 'powerBhp', profile.powerBhp, nf.powerBhp);
  setIfMissing(patch, 'torqueNm', profile.torqueNm, nf.torqueNm);

  setIfMissing(patch, 'performanceBasis.engineCc', profile.performanceBasis?.engineCc, nf.engineCc);
  setIfMissing(patch, 'performanceBasis.powerBhp', profile.performanceBasis?.powerBhp, nf.powerBhp);
  setIfMissing(patch, 'performanceBasis.torqueNm', profile.performanceBasis?.torqueNm, nf.torqueNm);
  if (hasValue(nf.transmission)) patch['performanceBasis.transmissionType'] = nf.transmission;

  setIfMissing(patch, 'mileageBasis.araiMileage', profile.mileageBasis?.araiMileage, nf.araiMileage);
  setIfMissing(patch, 'mileageBasis.fuelTankCapacity', profile.mileageBasis?.fuelTankCapacity, nf.fuelTankCapacity);

  setIfMissing(patch, 'practicalityBasis.seatingCapacity', profile.practicalityBasis?.seatingCapacity, nf.seatingCapacity);
  setIfMissing(patch, 'practicalityBasis.bootSpaceLitres', profile.practicalityBasis?.bootSpaceLitres, nf.bootSpaceLitres);
  setIfMissing(patch, 'practicalityBasis.lengthMm', profile.practicalityBasis?.lengthMm, nf.lengthMm);
  setIfMissing(patch, 'practicalityBasis.widthMm', profile.practicalityBasis?.widthMm, nf.widthMm);
  setIfMissing(patch, 'practicalityBasis.heightMm', profile.practicalityBasis?.heightMm, nf.heightMm);
  setIfMissing(patch, 'practicalityBasis.wheelbaseMm', profile.practicalityBasis?.wheelbaseMm, nf.wheelbaseMm);
  setIfMissing(patch, 'practicalityBasis.groundClearanceMm', profile.practicalityBasis?.groundClearanceMm, nf.groundClearanceMm);

  const hasPerformanceData =
    hasValue(profile.powerBhp) ||
    hasValue(profile.torqueNm) ||
    hasValue(profile.engineCc) ||
    hasValue(nf.powerBhp) ||
    hasValue(nf.torqueNm) ||
    hasValue(nf.engineCc);

  const hasMileageData =
    hasValue(profile.mileageBasis?.araiMileage) ||
    hasValue(profile.mileageBasis?.fuelTankCapacity) ||
    hasValue(nf.araiMileage) ||
    hasValue(nf.fuelTankCapacity);

  const hasDimensionsData =
    hasValue(profile.practicalityBasis?.lengthMm) ||
    hasValue(profile.practicalityBasis?.widthMm) ||
    hasValue(profile.practicalityBasis?.heightMm) ||
    hasValue(profile.practicalityBasis?.wheelbaseMm) ||
    hasValue(profile.practicalityBasis?.seatingCapacity) ||
    hasValue(nf.lengthMm) ||
    hasValue(nf.widthMm) ||
    hasValue(nf.heightMm) ||
    hasValue(nf.wheelbaseMm) ||
    hasValue(nf.seatingCapacity);

  patch['dataQuality.rawEvidenceStatus'] = 'internal_raw_source_ready';
  patch['dataQuality.rawEvidenceSourceCollection'] = nf.sourceCollection || evidence.sourceName || null;
  patch['dataQuality.rawEvidenceSourceDocId'] = nf.sourceFeatureDocId || null;
  patch['dataQuality.rawEvidencePatchedAt'] = nowDate;

  if (hasValue(nf.transmission)) patch['dataQuality.hasTransmissionData'] = true;
  if (hasPerformanceData) patch['dataQuality.hasPerformanceData'] = true;
  if (hasMileageData) patch['dataQuality.hasMileageData'] = true;
  if (hasDimensionsData) patch['dataQuality.hasDimensionsData'] = true;

  const missingCriticalFields = Array.isArray(profile.dataQuality?.missingCriticalFields)
    ? profile.dataQuality.missingCriticalFields.filter((field) => {
        const f = String(field || '').toLowerCase();
        return !(f.includes('transmission') || f === 'unknown_transmission');
      })
    : [];

  patch['dataQuality.missingCriticalFields'] = missingCriticalFields;

  if (profile.scoreEvidence?.missingDataWarnings) {
    patch['scoreEvidence.missingDataWarnings'] = removeWarnings(profile.scoreEvidence.missingDataWarnings);
  }

  patch['lookupKeys.transmissionResolvedFromRawEvidence'] = true;
  patch['lookupKeys.rawEvidenceSourceDocId'] = nf.sourceFeatureDocId || null;

  patch.updatedAt = nowDate;

  return patch;
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI');
    process.exit(1);
  }

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const evidenceCol = db.collection(EVIDENCE_COLLECTION);
  const profileCol = db.collection(PROFILE_COLLECTION);

  console.log(`[mode] ${write ? 'WRITE' : 'DRY_RUN'}, force=${force}`);
  console.log(`[source] ${EVIDENCE_COLLECTION}`);
  console.log(`[target] ${PROFILE_COLLECTION}`);

  const evidenceRows = await evidenceCol.find({
    evidenceType: 'transmission_spec',
    status: 'internal_raw_source_ready',
    reviewStatus: 'auto_raw_vehicle_features_exact_match',
  }).sort({ variantProfileKey: 1 }).toArray();

  const updates = [];
  const missingProfiles = [];
  const skipped = [];
  const samples = [];

  for (const evidence of evidenceRows) {
    const profile = await profileCol.findOne({ variantProfileKey: evidence.variantProfileKey });

    if (!profile) {
      missingProfiles.push({
        variantProfileKey: evidence.variantProfileKey,
        variantFullName: evidence.variantFullName,
      });
      continue;
    }

    const nf = evidence.normalizedFields || {};
    const alreadyResolved =
      profile.transmissionKey &&
      !String(profile.fuelTransmissionFamilyKey || '').includes('unknown_transmission');

    if (alreadyResolved && !force) {
      skipped.push({
        variantProfileKey: evidence.variantProfileKey,
        variantFullName: evidence.variantFullName,
      });
      continue;
    }

    const patch = buildPatch({ profile, evidence });

    updates.push({
      variantProfileKey: evidence.variantProfileKey,
      variantFullName: evidence.variantFullName,
      patch,
    });

    if (samples.length < 20) {
      samples.push({
        variantProfileKey: evidence.variantProfileKey,
        variantFullName: evidence.variantFullName,
        before: {
          transmission: profile.transmission,
          transmissionKey: profile.transmissionKey,
          gearbox: profile.gearbox,
          fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,
          engineCc: profile.engineCc,
          powerBhp: profile.powerBhp,
          torqueNm: profile.torqueNm,
        },
        after: {
          transmission: patch.transmission ?? profile.transmission,
          transmissionKey: patch.transmissionKey ?? profile.transmissionKey,
          gearbox: patch.gearbox ?? profile.gearbox,
          fuelTransmissionFamilyKey: patch.fuelTransmissionFamilyKey ?? profile.fuelTransmissionFamilyKey,
          engineCc: patch.engineCc ?? profile.engineCc,
          powerBhp: patch.powerBhp ?? profile.powerBhp,
          torqueNm: patch.torqueNm ?? profile.torqueNm,
          sourceDoc: nf.sourceFeatureDocId,
        },
      });
    }
  }

  let writeResult = null;

  if (write) {
    let matched = 0;
    let modified = 0;

    if (updates.length) {
      const result = await profileCol.bulkWrite(
        updates.map((update) => ({
          updateOne: {
            filter: { variantProfileKey: update.variantProfileKey },
            update: { $set: update.patch },
          },
        })),
        { ordered: false }
      );

      matched = result.matchedCount || 0;
      modified = result.modifiedCount || 0;

      await evidenceCol.bulkWrite(
        updates.map((update) => ({
          updateOne: {
            filter: {
              evidenceType: 'transmission_spec',
              variantProfileKey: update.variantProfileKey,
              status: 'internal_raw_source_ready',
            },
            update: {
              $set: {
                status: 'applied_to_profile',
                reviewStatus: 'raw_profile_patch_applied',
                profilePatchedAt: new Date(),
                updatedAt: new Date(),
              },
            },
          },
        })),
        { ordered: false }
      );
    }

    writeResult = { matched, modified };
  }

  console.log(JSON.stringify({
    mode: write ? 'WRITE' : 'DRY_RUN',
    evidenceRows: evidenceRows.length,
    updateCandidates: updates.length,
    missingProfiles: missingProfiles.length,
    skippedAlreadyResolved: skipped.length,
    samples,
    missingProfileSamples: missingProfiles.slice(0, 20),
    skippedSamples: skipped.slice(0, 20),
    writeResult,
  }, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
