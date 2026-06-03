#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const { runVehicleScoreInsightTool } = await import(
    '../../services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js'
  );

  const toolIndex = await import('../../services/aiAgent/tools/index.js');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const coverage = await runVehicleScoreInsightTool({ operation: 'coverage' });
  assert(coverage.status === 'success', 'Coverage operation failed.');
  assert(coverage.data.totalScoreProfiles === 2012, `Expected 2012 profiles, found ${coverage.data.totalScoreProfiles}`);
  assert(coverage.usageGuardrail.canUseForFinalRecommendation === false, 'Coverage guardrail missing.');

  const baleno = await runVehicleScoreInsightTool({
    operation: 'variant_score_insight',
    scoreProfileKey: 'maruti_baleno__alpha__petrol_manual',
  });

  assert(baleno.status === 'success', 'Baleno score insight failed.');
  assert(baleno.data.modules.features.score === 80.5, `Unexpected Baleno feature score: ${baleno.data.modules.features.score}`);
  assert(baleno.data.modules.features.evidence.presentKeys.includes('connectedCar'), 'Baleno connectedCar missing.');
  assert(baleno.data.usageGuardrail.canUseForFinalRecommendation === false, 'Data guardrail missing.');
  assert(baleno.usageGuardrail.canUseForFinalRecommendation === false, 'Tool guardrail missing.');

  const builtKey = await runVehicleScoreInsightTool({
    operation: 'variant_score_insight',
    makeKey: 'maruti',
    modelKey: 'baleno',
    variantKey: 'alpha',
    fuelKey: 'petrol',
    transmissionKey: 'manual',
  });

  assert(builtKey.status === 'success', 'Built scoreProfileKey lookup failed.');
  assert(builtKey.data.scoreProfileKey === 'maruti_baleno__alpha__petrol_manual', 'Built scoreProfileKey mismatch.');

  const model = await runVehicleScoreInsightTool({
    operation: 'model_score_insights',
    makeKey: 'maruti',
    modelKey: 'baleno',
    limit: 20,
  });

  assert(model.status === 'success', 'Model score insights failed.');
  assert(model.data.count >= 5, `Expected Baleno model count >=5, found ${model.data.count}`);

  const value = await runVehicleScoreInsightTool({
    operation: 'same_family_value_insights',
    makeKey: 'maruti',
    modelKey: 'baleno',
    fuelTransmissionFamilyKey: 'petrol_manual',
    limit: 10,
  });

  assert(value.status === 'success', 'Same-family value insights failed.');
  assert(value.data.count >= 3, `Expected same-family value count >=3, found ${value.data.count}`);

  const topSafety = await runVehicleScoreInsightTool({
    operation: 'top_module_score_insights',
    module: 'safety',
    limit: 5,
  });

  assert(topSafety.status === 'success', 'Top safety insights failed.');
  assert(topSafety.data.count === 5, `Expected 5 top safety insights, found ${topSafety.data.count}`);

  const registry = toolIndex.ACI_V2_TOOL_REGISTRY || toolIndex.default || {};
  assert(registry.vehicle_score_insight, 'vehicle_score_insight missing from ACI V2 tool registry.');

  console.log(JSON.stringify({
    status: 'ok',
    coverage: {
      totalScoreProfiles: coverage.data.totalScoreProfiles,
      finalOverallScoreReadyCount: coverage.data.finalOverallScoreReadyCount,
      featureAliasDiagnostic: coverage.data.featureAliasDiagnostic,
    },
    baleno: {
      answer: baleno.answer,
      featureScore: baleno.data.modules.features.score,
      valueScore: baleno.data.modules.value.score,
      canUseForFinalRecommendation: baleno.data.usageGuardrail.canUseForFinalRecommendation,
    },
    modelCount: model.data.count,
    valueCount: value.data.count,
    topSafety: topSafety.data.variants.map((v) => ({
      variantFullName: v.variantFullName,
      safetyScore: v.modules.safety.score,
      safetyConfidence: v.modules.safety.confidence,
    })),
    registryHasScoreTool: Boolean(registry.vehicle_score_insight),
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
