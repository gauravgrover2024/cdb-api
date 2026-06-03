#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const {
  getVariantScoreInsight,
  getModelScoreInsights,
  getSameFamilyValueInsights,
  getTopScoreInsights,
  getScoreProfileCoverage,
} = require('../../services/aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs');

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

  const balenoAlpha = await getVariantScoreInsight({
    scoreProfileKey: 'maruti_baleno__alpha__petrol_manual',
  });

  assert(balenoAlpha, 'Baleno Alpha insight missing.');
  assert(balenoAlpha.modules.features.score === 80.5, `Unexpected Baleno Alpha feature score: ${balenoAlpha.modules.features.score}`);
  assert(balenoAlpha.modules.features.evidence.presentKeys.includes('connectedCar'), 'Baleno Alpha connectedCar should remain detected.');
  assert(balenoAlpha.usageGuardrail.canUseForFinalRecommendation === false, 'Insight must block final recommendation usage.');

  const nexonSmart = await getVariantScoreInsight({
    scoreProfileKey: 'tata_nexon__smart__petrol_manual',
  });

  assert(nexonSmart, 'Nexon Smart insight missing.');
  assert(nexonSmart.modules.features.score === 47.5, `Unexpected Nexon Smart feature score: ${nexonSmart.modules.features.score}`);
  assert(nexonSmart.modules.value.score === 84.6, `Unexpected Nexon Smart value score: ${nexonSmart.modules.value.score}`);

  const balenoModel = await getModelScoreInsights({
    makeKey: 'maruti',
    modelKey: 'baleno',
    limit: 20,
  });

  assert(balenoModel.count >= 5, `Expected Baleno model variants, found ${balenoModel.count}`);
  assert(balenoModel.usageGuardrail.canUseForFinalRecommendation === false, 'Model insights must block final recommendation usage.');

  const balenoValue = await getSameFamilyValueInsights({
    makeKey: 'maruti',
    modelKey: 'baleno',
    fuelTransmissionFamilyKey: 'petrol_manual',
    limit: 10,
  });

  assert(balenoValue.count >= 3, `Expected Baleno same-family value variants, found ${balenoValue.count}`);

  const topSafety = await getTopScoreInsights({
    scorePath: 'safetyScore.score',
    limit: 5,
  });

  assert(topSafety.count === 5, `Expected 5 top safety insights, found ${topSafety.count}`);

  console.log(JSON.stringify({
    status: 'ok',
    coverage,
    balenoAlpha: {
      variantFullName: balenoAlpha.variantFullName,
      featureScore: balenoAlpha.modules.features.score,
      valueScore: balenoAlpha.modules.value.score,
      regretRisk: balenoAlpha.modules.regretRisk.score,
      strengths: balenoAlpha.strengths,
      watchouts: balenoAlpha.watchouts,
      canUseForFinalRecommendation: balenoAlpha.usageGuardrail.canUseForFinalRecommendation,
    },
    nexonSmart: {
      variantFullName: nexonSmart.variantFullName,
      featureScore: nexonSmart.modules.features.score,
      valueScore: nexonSmart.modules.value.score,
      strengths: nexonSmart.strengths,
      watchouts: nexonSmart.watchouts,
    },
    balenoModelCount: balenoModel.count,
    balenoValueTop: balenoValue.variants.slice(0, 5).map((v) => ({
      variantFullName: v.variantFullName,
      valueScore: v.modules.value.score,
      featureScore: v.modules.features.score,
      watchouts: v.watchouts,
    })),
    topSafety: topSafety.variants.map((v) => ({
      variantFullName: v.variantFullName,
      safetyScore: v.modules.safety.score,
      safetyConfidence: v.modules.safety.confidence,
      watchouts: v.watchouts,
    })),
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
