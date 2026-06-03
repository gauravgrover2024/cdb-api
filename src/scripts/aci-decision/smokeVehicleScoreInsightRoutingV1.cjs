#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const { runAciV2Tool } = await import('../../services/aiAgent/tools/index.js');
  const { getAciRuntimeDataTool } = await import('../../services/aiAgent/aiAgent.executor.js');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const runtimeTool = getAciRuntimeDataTool('vehicle_score_insight');
  assert(typeof runtimeTool === 'function', 'vehicle_score_insight missing from runtime data tools.');

  const directRuntime = await runAciV2Tool({
    toolPlan: {
      tool: 'vehicle_score_insight',
      operation: 'variant_score_insight',
      makeKey: 'maruti',
      modelKey: 'baleno',
      variantKey: 'alpha',
    },
    userMessage: 'Is Baleno Alpha good value?',
  });

  assert(directRuntime.status === 'success', `Direct runtime score insight failed: ${directRuntime.error?.message}`);
  assert(directRuntime.data.modules.features.score === 80.5, 'Baleno Alpha feature score mismatch.');
  assert(directRuntime.data.modules.features.evidence.presentKeys.includes('connectedCar'), 'Baleno connectedCar missing.');
  assert(directRuntime.usageGuardrail.canUseForFinalRecommendation === false, 'Tool guardrail missing.');
  assert(directRuntime.data.usageGuardrail.canUseForFinalRecommendation === false, 'Data guardrail missing.');

  const sameFamily = await runAciV2Tool({
    toolPlan: {
      tool: 'vehicle_same_family_value_insights',
      operation: 'same_family_value_insights',
      makeKey: 'maruti',
      modelKey: 'baleno',
      fuelTransmissionFamilyKey: 'petrol_manual',
      limit: 10,
    },
    userMessage: 'Which Baleno petrol manual variant is better value?',
  });

  assert(sameFamily.status === 'success', `Same-family score insight failed: ${sameFamily.error?.message}`);
  assert(sameFamily.data.count >= 3, `Expected same-family variants >=3, found ${sameFamily.data.count}`);
  assert(sameFamily.usageGuardrail.canUseForFinalRecommendation === false, 'Same-family guardrail missing.');

  const topSafety = await runAciV2Tool({
    toolPlan: {
      tool: 'vehicle_top_score_insights',
      operation: 'top_module_score_insights',
      module: 'safety',
      limit: 5,
    },
    userMessage: 'Show safety score leaders.',
  });

  assert(topSafety.status === 'success', `Top safety score insight failed: ${topSafety.error?.message}`);
  assert(topSafety.data.count === 5, `Expected 5 top safety rows, found ${topSafety.data.count}`);

  console.log(JSON.stringify({
    status: 'ok',
    runtimeToolRegistered: typeof runtimeTool === 'function',
    directRuntime: {
      answer: directRuntime.answer,
      featureScore: directRuntime.data.modules.features.score,
      valueScore: directRuntime.data.modules.value.score,
      canUseForFinalRecommendation: directRuntime.data.usageGuardrail.canUseForFinalRecommendation,
    },
    sameFamily: {
      count: sameFamily.data.count,
      top: sameFamily.data.variants.slice(0, 4).map((v) => ({
        variantFullName: v.variantFullName,
        valueScore: v.modules.value.score,
        featureScore: v.modules.features.score,
      })),
    },
    topSafety: topSafety.data.variants.map((v) => ({
      variantFullName: v.variantFullName,
      safetyScore: v.modules.safety.score,
      safetyConfidence: v.modules.safety.confidence,
    })),
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
