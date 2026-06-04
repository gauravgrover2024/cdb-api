#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

const getBridge = (response = {}) =>
  response.aciCoreBridge ||
  response.meta?.aciCoreBridge ||
  response.data?.aciCoreBridge ||
  null;

const stringify = (value) => {
  try {
    return JSON.stringify(value);
  } catch (_) {
    return String(value || '');
  }
};

const hasGuardrailFalse = (value) => {
  const text = stringify(value);
  return (
    text.includes('"canUseForFinalRecommendation":false') ||
    text.includes('"finalRecommendationEnabled":false') ||
    text.includes('Final recommendation') ||
    text.includes('diagnostic module scores')
  );
};

const runCase = async ({
  runAciCoreLiveBridge,
  id,
  message,
  context = {},
  expectedTools = [],
  requireGuardrail = false,
  answerMustInclude = [],
  answerMustNotInclude = [],
}) => {
  const startedAt = Date.now();

  const response = await runAciCoreLiveBridge({
    message,
    context,
    meta: {
      smokeId: id,
      source: 'smokeAciLiveBridgeScoreInsightV1',
    },
  });

  const bridge = getBridge(response);
  const tool = bridge?.tool || null;
  const durationMs = Date.now() - startedAt;

  assert(response, `${id}: response missing`);
  assert(bridge, `${id}: aciCoreBridge metadata missing`);
  assert(bridge.enabled === true, `${id}: bridge not enabled`);

  if (expectedTools.length) {
    assert(
      expectedTools.includes(tool),
      `${id}: expected tool ${expectedTools.join(' or ')}, got ${tool}`
    );
  }

  if (requireGuardrail) {
    assert(
      hasGuardrailFalse(response),
      `${id}: score guardrail not found in response payload`
    );

    const answerText = String(
      response.answer ||
      response.text ||
      response.message ||
      response.finalAnswer ||
      ''
    );

    assert(
      !/not available in the current ACI Assist backend yet/i.test(answerText),
      `${id}: score route rendered unavailable fallback instead of score answer`
    );

    assert(
      /score|safety|features|value|regret|diagnostic/i.test(answerText),
      `${id}: score answer does not look like a score insight answer: ${answerText}`
    );

    for (const requiredText of answerMustInclude) {
      assert(
        answerText.includes(requiredText),
        `${id}: expected answer to include "${requiredText}", got: ${answerText}`
      );
    }

    for (const forbiddenText of answerMustNotInclude) {
      assert(
        !answerText.includes(forbiddenText),
        `${id}: expected answer not to include "${forbiddenText}", got: ${answerText}`
      );
    }

    assert(
      !/I found score insight data for Score insight/i.test(answerText),
      `${id}: score answer is generic and missing resolved vehicle details`
    );

    const payloadText = stringify(response);
    assert(
      !payloadText.includes('missing_variant_key'),
      `${id}: score tool did not receive model/variant identifiers`
    );

    assert(
      /Strengths:|Watchouts:|weak same-model value|good same-model value|feature-rich|Ranked ladder|strongest same-family value pick|next practical step|Score movement|Practical call|price jump/i.test(answerText),
      `${id}: score answer is not buyer-readable enough: ${answerText}`
    );

    assert(
      !/looks weak same-model value, feature-rich/i.test(answerText),
      `${id}: score answer still sounds like stitched score labels: ${answerText}`
    );

    const bridge = getBridge(response);
    assert(
      bridge?.primaryTask === 'score_insight',
      `${id}: expected score_insight primaryTask, got ${bridge?.primaryTask}`
    );
  }

  return {
    id,
    message,
    tool,
    primaryTask: bridge.primaryTask,
    planMode: bridge.planMode,
    selectedParser: bridge.selectedParser,
    usedGemini: bridge.usedGemini,
    contextIsolation: bridge.contextIsolation,
    durationMs,
    guardrailDetected: hasGuardrailFalse(response),
    responseKeys: Object.keys(response || {}),
    answerPreview: String(
      response.answer ||
      response.text ||
      response.message ||
      response.finalAnswer ||
      ''
    ).slice(0, 260),
  };
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  const { runAciCoreLiveBridge } = await import(
    '../../services/aciCore/integration/aciCoreLiveBridge.service.js'
  );

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const cases = [
    {
      id: 'score-baleno-alpha-value',
      message: 'Is Baleno Alpha good value?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
    },
    {
      id: 'score-baleno-alpha-strengths',
      message: 'What is Baleno Alpha strong and weak at?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
    },
    {
      id: 'score-baleno-petrol-manual-value-ladder',
      message: 'Which Baleno petrol manual variant is better value?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['Ranked ladder', 'Maruti Baleno'],
    },
    {
      id: 'score-baleno-alpha-worth-over-zeta',
      message: 'Is Baleno Alpha worth over Zeta?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['Maruti Baleno Alpha', 'Maruti Baleno Zeta', 'Feature gains', '360° camera'],
    },
    {
      id: 'score-baleno-delta-to-zeta-gain',
      message: 'What do I gain from Baleno Delta to Zeta?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['Maruti Baleno Delta', 'Maruti Baleno Zeta', 'Feature gains', 'rear camera'],
    },
    {
      id: 'score-baleno-overall-summary',
      message: 'How good is Baleno overall?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['Maruti Baleno', 'Best value signal', 'diagnostic'],
    },
    {
      id: 'score-baleno-family-car',
      message: 'Is Baleno a good family car?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['Maruti Baleno', 'Watchouts'],
    },
    {
      id: 'score-baleno-petrol-manual-overall',
      message: 'How good is Baleno petrol manual overall?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['Maruti Baleno', 'Scope: petrol manual'],
      answerMustNotInclude: ['CNG'],
    },
    {
      id: 'score-baleno-city-driving',
      message: 'Best Baleno for city driving?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['For city driving', 'City-use signal'],
    },
    {
      id: 'score-baleno-family-use',
      message: 'Which Baleno variant should I consider for family use?',
      expectedTools: ['vehicle_score_insight'],
      requireGuardrail: true,
      answerMustInclude: ['family', 'practicality', 'Watchouts'],
    },
    {
      id: 'price-creta-sx-delhi',
      message: 'Creta SX on-road price Delhi',
      expectedTools: ['vehicle_pricelist'],
    },
    {
      id: 'feature-baleno-connected-car',
      message: 'Does Baleno Alpha have connected car features?',
      expectedTools: [
        'vehicle_feature_lookup',
        'vehicle_feature_answer',
        'vehicle_features',
      ],
    },
  ];

  const results = [];

  for (const testCase of cases) {
    results.push(await runCase({ runAciCoreLiveBridge, ...testCase }));
  }

  console.log(JSON.stringify({
    status: 'ok',
    cases: results,
  }, null, 2));

  await mongoose.disconnect();
})().catch((error) => {
  console.error(error);
  mongoose.disconnect().catch(() => {}).finally(() => process.exit(1));
});
