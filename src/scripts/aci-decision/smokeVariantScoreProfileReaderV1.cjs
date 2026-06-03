#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const {
  getVariantScoreProfile,
  getModelScoreProfiles,
  getSameFamilyValueProfiles,
  getTopScoreProfiles,
  getScoreProfileCoverage,
} = require('../../services/aciCore/scoreProfiles/aciVariantScoreProfile.reader.cjs');

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const coverage = await getScoreProfileCoverage();
  assert(coverage.totalScoreProfiles === 2012, `Expected 2012 score profiles, found ${coverage.totalScoreProfiles}`);
  assert(coverage.finalOverallScoreReadyCount === 0, 'Final overall score must remain disabled.');
  assert(coverage.featureJoinMissing === 0, 'Feature matrix joins should not be missing.');

  const balenoAlpha = await getVariantScoreProfile({
    scoreProfileKey: 'maruti_baleno__alpha__petrol_manual',
  });

  assert(balenoAlpha, 'Baleno Alpha score profile missing.');
  assert(balenoAlpha.scoreReadiness.finalOverallScoreReady === false, 'Baleno Alpha final overall score should be disabled.');
  assert(balenoAlpha.usageGuardrail.canUseForFinalRecommendation === false, 'Reader must block direct final recommendation usage.');
  assert(balenoAlpha.safetyScore?.score > 0, 'Baleno Alpha safety score missing.');
  assert(balenoAlpha.featureScore?.evidence?.presentKeys?.includes('camera360'), 'Baleno Alpha should detect 360 camera after strict matching.');

  const balenoProfiles = await getModelScoreProfiles({
    makeKey: 'maruti',
    modelKey: 'baleno',
    limit: 50,
  });

  assert(balenoProfiles.length >= 5, `Expected Baleno model scores, found ${balenoProfiles.length}`);

  const balenoValue = await getSameFamilyValueProfiles({
    makeKey: 'maruti',
    modelKey: 'baleno',
    fuelTransmissionFamilyKey: 'petrol_manual',
    limit: 10,
  });

  assert(Array.isArray(balenoValue.profiles), 'Value profiles response malformed.');
  assert(balenoValue.usageGuardrail.canUseForFinalRecommendation === false, 'Value reader must not allow final recommendation.');

  const topSafety = await getTopScoreProfiles({
    scorePath: 'safetyScore.score',
    limit: 5,
  });

  assert(topSafety.profiles.length === 5, 'Top safety score query failed.');
  assert(topSafety.usageGuardrail.canUseForFinalRecommendation === false, 'Top score reader must not allow final recommendation.');

  console.log(JSON.stringify({
    status: 'ok',
    coverage,
    balenoAlpha: {
      variantFullName: balenoAlpha.variantFullName,
      safetyScore: balenoAlpha.safetyScore.score,
      featureScore: balenoAlpha.featureScore.score,
      valueScore: balenoAlpha.valueScore.score,
      finalOverallScoreReady: balenoAlpha.scoreReadiness.finalOverallScoreReady,
      usageGuardrail: balenoAlpha.usageGuardrail,
    },
    balenoProfileCount: balenoProfiles.length,
    balenoValueTop: balenoValue.profiles.slice(0, 5).map((p) => ({
      variantFullName: p.variantFullName,
      valueScore: p.valueScore.score,
      rawValueScore: p.valueScore.rawScore,
      price: p.referenceExShowroomPrice,
      caveats: p.valueScore.caveats,
    })),
    topSafety: topSafety.profiles.map((p) => ({
      variantFullName: p.variantFullName,
      safetyScore: p.safetyScore.score,
      safetyConfidence: p.safetyScore.confidence,
      caveats: p.safetyScore.caveats,
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
