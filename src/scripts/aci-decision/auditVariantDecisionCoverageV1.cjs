#!/usr/bin/env node

try {
  require('dotenv').config();
} catch (_) {}

const mongoose = require('mongoose');

const {
  MANIFEST_VERSION,
  FIELD_MANIFEST,
  SCORE_REQUIREMENTS,
  EXTERNAL_SOURCE_QUEUE_RULES,
} = require('../../services/aciCore/decisionProfiles/contracts/aciVariantDecisionProfile.manifest.cjs');

const COLLECTION = process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';

const mongoUri =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DATABASE_URL;

const getByPath = (obj, path) => {
  if (!obj || !path) return undefined;
  return path.split('.').reduce((acc, key) => {
    if (acc === undefined || acc === null) return undefined;
    return acc[key];
  }, obj);
};

const isPresent = (value) => {
  if (value === undefined || value === null) return false;
  if (typeof value === 'string' && value.trim() === '') return false;
  if (Array.isArray(value) && value.length === 0) return false;
  return true;
};

const isBooleanKnown = (value) => value === true || value === false;

const isRequirementMet = (doc, expr) => {
  if (expr.includes('|')) {
    return expr.split('|').some((path) => isRequirementMet(doc, path.trim()));
  }

  const value = getByPath(doc, expr);

  if (expr === 'featureFlags') {
    const flags = value || {};
    return Object.values(flags).some((v) => isBooleanKnown(v));
  }

  return isPresent(value);
};

const getScoreReadiness = (doc) => {
  const result = {};

  for (const [scoreKey, requirements] of Object.entries(SCORE_REQUIREMENTS)) {
    const missing = requirements.filter((expr) => !isRequirementMet(doc, expr));
    result[scoreKey] = {
      ready: missing.length === 0,
      missing,
    };
  }

  return result;
};

const collectExternalGaps = (doc) => {
  const gaps = [];

  for (const rule of EXTERNAL_SOURCE_QUEUE_RULES) {
    const missingFields = rule.fields.filter((field) => {
      if (field === 'featureFlags' || field === 'featureEvidence') {
        return getByPath(doc, 'dataQuality.hasFeatureMatrix') !== true;
      }

      if (field === 'powertrain') {
        return !isPresent(getByPath(doc, 'powerBhp')) && !isPresent(getByPath(doc, 'motorPowerBhp'));
      }

      if (field === 'performanceBasis') {
        return getByPath(doc, 'dataQuality.hasPerformanceData') !== true;
      }

      if (field === 'mileageBasis') {
        return getByPath(doc, 'dataQuality.hasMileageData') !== true;
      }

      if (field === 'practicalityBasis') {
        return getByPath(doc, 'dataQuality.hasDimensionsData') !== true;
      }

      return !isPresent(getByPath(doc, field));
    });

    if (missingFields.length) {
      gaps.push({
        gapKey: rule.gapKey,
        missingFields,
        sourcePriority: rule.sourcePriority,
        neededBeforeScoring: rule.neededBeforeScoring,
      });
    }
  }

  return gaps;
};

async function main() {
  if (!mongoUri) {
    console.error('Missing Mongo URI. Set MONGODB_URI or MONGO_URI.');
    process.exit(1);
  }

  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;
  const col = db.collection(COLLECTION);

  const total = await col.countDocuments();

  const docs = await col.find({}).toArray();

  const confidenceSummary = await col.aggregate([
    {
      $group: {
        _id: '$dataQuality.confidenceTier',
        count: { $sum: 1 },
        hasFeatureMatrix: { $sum: { $cond: ['$dataQuality.hasFeatureMatrix', 1, 0] } },
        hasSafetyData: { $sum: { $cond: ['$dataQuality.hasSafetyData', 1, 0] } },
        hasPerformanceData: { $sum: { $cond: ['$dataQuality.hasPerformanceData', 1, 0] } },
        hasMileageData: { $sum: { $cond: ['$dataQuality.hasMileageData', 1, 0] } },
        hasDimensionsData: { $sum: { $cond: ['$dataQuality.hasDimensionsData', 1, 0] } },
      },
    },
    { $sort: { count: -1 } },
  ]).toArray();

  const fieldCoverage = FIELD_MANIFEST.map((field) => {
    let present = 0;
    let missing = 0;

    for (const doc of docs) {
      const value = getByPath(doc, field.path);
      if (isPresent(value)) present += 1;
      else missing += 1;
    }

    return {
      path: field.path,
      group: field.group,
      priority: field.priority,
      required: field.required,
      source: field.source,
      present,
      missing,
      coveragePct: total ? Number(((present / total) * 100).toFixed(2)) : 0,
      usage: field.usage,
    };
  });

  const lowCoverageCore = fieldCoverage
    .filter((row) => row.priority === 'core_v1' && row.coveragePct < 90)
    .sort((a, b) => a.coveragePct - b.coveragePct);

  const missingFeatureSamples = docs
    .filter((doc) => getByPath(doc, 'dataQuality.hasFeatureMatrix') !== true)
    .slice(0, 30)
    .map((doc) => ({
      variantProfileKey: doc.variantProfileKey,
      variantFullName: doc.variantFullName,
      makeKey: doc.makeKey,
      modelKey: doc.modelKey,
      variantKey: doc.variantKey,
      fuelTransmissionFamilyKey: doc.fuelTransmissionFamilyKey,
      referenceExShowroomPrice: doc.referenceExShowroomPrice,
      missingCriticalFields: getByPath(doc, 'dataQuality.missingCriticalFields') || [],
    }));

  const scoreReadinessCounts = {};
  const scoreMissingSamples = {};

  for (const doc of docs) {
    const readiness = getScoreReadiness(doc);

    for (const [scoreKey, info] of Object.entries(readiness)) {
      if (!scoreReadinessCounts[scoreKey]) {
        scoreReadinessCounts[scoreKey] = { ready: 0, notReady: 0 };
      }

      if (info.ready) {
        scoreReadinessCounts[scoreKey].ready += 1;
      } else {
        scoreReadinessCounts[scoreKey].notReady += 1;
        if (!scoreMissingSamples[scoreKey]) scoreMissingSamples[scoreKey] = [];
        if (scoreMissingSamples[scoreKey].length < 10) {
          scoreMissingSamples[scoreKey].push({
            variantProfileKey: doc.variantProfileKey,
            variantFullName: doc.variantFullName,
            missing: info.missing,
          });
        }
      }
    }
  }

  const externalGapSummary = {};
  const externalGapSamples = {};

  for (const doc of docs) {
    const gaps = collectExternalGaps(doc);

    for (const gap of gaps) {
      if (!externalGapSummary[gap.gapKey]) {
        externalGapSummary[gap.gapKey] = {
          count: 0,
          neededBeforeScoring: gap.neededBeforeScoring,
          sourcePriority: gap.sourcePriority,
          missingFields: {},
        };
      }

      externalGapSummary[gap.gapKey].count += 1;

      for (const field of gap.missingFields) {
        externalGapSummary[gap.gapKey].missingFields[field] =
          (externalGapSummary[gap.gapKey].missingFields[field] || 0) + 1;
      }

      if (!externalGapSamples[gap.gapKey]) externalGapSamples[gap.gapKey] = [];
      if (externalGapSamples[gap.gapKey].length < 15) {
        externalGapSamples[gap.gapKey].push({
          variantProfileKey: doc.variantProfileKey,
          variantFullName: doc.variantFullName,
          makeKey: doc.makeKey,
          modelKey: doc.modelKey,
          variantKey: doc.variantKey,
          missingFields: gap.missingFields,
        });
      }
    }
  }

  const unknownTransmissionCount = await col.countDocuments({
    fuelTransmissionFamilyKey: /unknown_transmission/,
  });

  const output = {
    manifestVersion: MANIFEST_VERSION,
    collection: COLLECTION,
    total,
    confidenceSummary,
    headlineGaps: {
      missingFeatureMatrix: await col.countDocuments({ 'dataQuality.hasFeatureMatrix': false }),
      crashRatingMissing: await col.countDocuments({
        'safetyBasis.globalNcapAdult': null,
        'safetyBasis.bharatNcapAdult': null,
      }),
      recommendationScoreNull: await col.countDocuments({ 'scores.recommendationScore': null }),
      unknownTransmissionCount,
    },
    lowCoverageCore,
    scoreReadinessCounts,
    scoreMissingSamples,
    externalGapSummary,
    externalGapSamples,
    missingFeatureSamples,
  };

  console.log(JSON.stringify(output, null, 2));

  await mongoose.disconnect();
}

main().catch(async (error) => {
  console.error(error);
  try {
    await mongoose.disconnect();
  } catch (_) {}
  process.exit(1);
});
