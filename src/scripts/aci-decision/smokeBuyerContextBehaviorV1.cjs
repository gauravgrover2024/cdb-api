#!/usr/bin/env node
'use strict';

require('dotenv').config();

const assert = require('assert');
const fs = require('fs');
const mongoose = require('mongoose');

const {
  ALLOWED_ANSWER_TYPES,
} = require('../../services/aciCore/decisionPolicy/aciDecisionPolicy.constants.cjs');
const {
  buildFinalRecommendationEligibilityRuntime,
} = require('../../services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs');

const INTERNAL_BLOCKER_PATTERNS = [
  /\bfinal recommendation disabled\b/i,
  /\bfinal recommendation remains disabled\b/i,
  /\bmissing buyer context\b/i,
  /\bbuyer context incomplete\b/i,
  /\bevidence threshold not met\b/i,
  /\bpolicy not ready\b/i,
  /\bblockedReasons\b/,
  /\binternalMissingInputMap\b/,
  /\bmissing mandatory inputs\b/i,
];

const FORBIDDEN_BUYER_UX_KEYS = [
  'buyerGuidanceContext',
  'blockedReasons',
  'missingMandatoryInputs',
  'internalMissingInputMap',
  'missingEvidence',
  'evidenceSources',
  'inferredContext',
];

const assertNoForbiddenUxKeys = (value, path = 'finalBlockedUx') => {
  if (!value || typeof value !== 'object') return;
  for (const [key, nested] of Object.entries(value)) {
    assert(!FORBIDDEN_BUYER_UX_KEYS.includes(key), `${path} exposed ${key}`);
    if (nested && typeof nested === 'object') {
      assertNoForbiddenUxKeys(nested, `${path}.${key}`);
    }
  }
};

const ALLOWED_GUIDANCE_MODES = new Set([
  'practical_first_view',
  'conditional_guidance',
  'sharpened_recommendation',
]);

const TEMPLATE_BY_MODE = {
  practical_first_view: 'decision_buyer_guidance_practical_first_view',
  conditional_guidance: 'decision_buyer_guidance_conditional',
  sharpened_recommendation: 'decision_buyer_guidance_sharpened_recommendation',
};

const countQuestions = (text = '') => (String(text).match(/\?/g) || []).length;

const join = (items = []) => {
  const values = (Array.isArray(items) ? items : [items])
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  if (values.length <= 1) return values[0] || '';
  if (values.length === 2) return `${values[0]} and ${values[1]}`;
  return `${values.slice(0, -1).join(', ')}, and ${values[values.length - 1]}`;
};

const labelOf = (vehicle = {}) =>
  String(vehicle.label || vehicle.fullModel || [vehicle.make, vehicle.model, vehicle.variant].filter(Boolean).join(' ')).trim();

const subjectLabel = ({ facts = {}, evidencePack = {}, fallback = 'this choice' } = {}) => {
  const subject = evidencePack.subject || {};
  const scope = evidencePack.scope || '';
  if (scope === 'comparison_scope' && Array.isArray(subject.comparisonTargets) && subject.comparisonTargets.length >= 2) {
    return subject.comparisonTargets.map(labelOf).filter(Boolean).join(' vs ');
  }
  if (scope === 'upgrade_scope') {
    const base = labelOf(subject.upgradeBase || {});
    const target = labelOf(subject.upgradeTarget || {});
    if (base && target) return `${base} to ${target}`;
  }
  if (scope === 'make_scope') return subject.make || facts.make || facts.brand || fallback;
  if (scope === 'discovery_scope') return subject.discoveryLabel || fallback;
  return facts.fullModel || [facts.make || facts.brand, facts.model, facts.variant].filter(Boolean).join(' ') || fallback;
};

const formatScoreSignals = (scoreSignals = {}) => {
  const labels = {
    safety: 'safety',
    features: 'features',
    value: 'value',
    runningCost: 'running cost',
    familyPracticality: 'family practicality',
    comfort: 'comfort',
    regretRisk: 'regret risk',
  };

  return join(Object.entries(scoreSignals).map(([key, value]) => {
    if (value && typeof value === 'object') {
      const score = value.score !== undefined && value.score !== null && value.score !== '' ? ` score ${value.score}` : '';
      const band = value.band || value.status || value.label || '';
      return `${labels[key] || key}${score}${band ? ` (${band})` : ''}`.trim();
    }
    return value ? `${labels[key] || key}: ${value}` : '';
  }).filter(Boolean));
};

const renderGuidance = async ({ id, message, context = {}, response = {}, bridge = {} }) => {
  const eligibility = buildFinalRecommendationEligibilityRuntime({
    message,
    bridge: {
      tool: 'vehicle_recommend',
      primaryTask: 'vehicle_recommendation',
      originalMessage: message,
      effectiveMessage: message,
      ...bridge,
    },
    context,
    response: {
      ...response,
      matched: response.matched ?? 1,
      evidence: {
        evidenceStatus: 'partial',
        usableEvidenceCount: 1,
        ...(response.evidence || {}),
      },
    },
  });

  const {
    renderAciTemplate,
  } = await import('../../services/aciCore/language/aciAnswerLanguageComposer.js');

  const guidance = eligibility.buyerGuidanceContext || {};
  const facts = guidance.selectedVehicleFacts || {};
  const evidencePack = guidance.decisionEvidencePack || {};
  const templateKey = TEMPLATE_BY_MODE[guidance.guidanceMode];
  const factLine = [
    facts.bodyType ? `body type ${facts.bodyType}` : '',
    facts.seatingCapacity ? `${facts.seatingCapacity}-seater` : '',
    facts.fuelTypes?.length ? `available fuels ${join(facts.fuelTypes)}` : '',
    facts.transmissions?.length ? `available transmissions ${join(facts.transmissions)}` : '',
    facts.safetyFeatures?.length ? `known safety features ${join(facts.safetyFeatures)}` : '',
    facts.featureHighlights?.length ? `known feature highlights ${join(facts.featureHighlights)}` : '',
    facts.similarAlternatives?.length ? `similar alternatives ${join(facts.similarAlternatives)}` : '',
  ].filter(Boolean).join('; ');
  const buyerContextLine = Object.entries(guidance.explicitBuyerContext || {})
    .filter(([, value]) => value !== undefined && value !== null && String(value).trim() !== '')
    .map(([key, value]) => `${key}: ${Array.isArray(value) ? join(value) : value}`)
    .join('; ') || 'not enough buyer preferences yet';
  const scoreLine = formatScoreSignals(evidencePack.scoreSignals || {});

  const rendered = renderAciTemplate(
    templateKey,
    {
      model: subjectLabel({ facts, evidencePack, fallback: id }),
      vehicleFactsLine: factLine || 'selected facts are available',
      strengthsLine: join([...(evidencePack.strengths || []), scoreLine].filter(Boolean)) || 'not enough scored strength evidence yet',
      watchoutsLine: join(evidencePack.watchouts || []) || 'nothing specific in the supplied evidence',
      fitLine: join(evidencePack.fitSignals || []) || 'your use case matches the available facts and trade-offs',
      alternativeLine: join(evidencePack.alternativeSignals || []) || 'your top priority is not covered by the available evidence',
      upgradeLine: join(evidencePack.upgradeSignals || []) || 'no upgrade-ladder evidence supplied for this question',
      buyerContextLine,
      assumptionsLine: join(guidance.softAssumptions || []) || 'none beyond the buyer context already shared',
      softQuestion: guidance.softQuestion || 'Share the one priority you want me to weigh most.',
    },
    {
      seed: `buyer-context-behavior|${id}|${message}|${guidance.guidanceMode}`,
    },
  );

  return {
    eligibility,
    rendered,
    text: rendered.text,
  };
};

const assertBuyerSafe = ({ id, result, expectedScope }) => {
  const { eligibility, rendered, text } = result;
  const guidance = eligibility.buyerGuidanceContext || {};

  assert.strictEqual(eligibility.requestedFinalRecommendation, true, `${id}: final-choice request not detected`);
  assert.strictEqual(eligibility.finalRecommendationEnabled, false, `${id}: final recommendation unexpectedly enabled`);
  assert.strictEqual(eligibility.canUseForFinalRecommendation, false, `${id}: final recommendation unexpectedly usable`);
  assert.notStrictEqual(eligibility.allowedAnswerType, ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED, `${id}: final recommendation allowed`);
  assert.strictEqual(guidance.finalPurchaseVerdictEnabled, false, `${id}: final purchase verdict unexpectedly enabled`);
  assert(ALLOWED_GUIDANCE_MODES.has(guidance.guidanceMode), `${id}: unexpected guidance mode ${guidance.guidanceMode}`);
  assert.strictEqual(guidance.decisionEvidencePack?.scope, expectedScope, `${id}: expected scope ${expectedScope}, got ${guidance.decisionEvidencePack?.scope}`);
  assert(rendered && !rendered.missingTemplate, `${id}: central composer template missing`);
  assert(TEMPLATE_BY_MODE[guidance.guidanceMode] === rendered.templateKey, `${id}: rendered outside buyer guidance templates`);
  assert(countQuestions(text) <= 1, `${id}: expected at most one buyer-facing question: ${text}`);

  for (const pattern of INTERNAL_BLOCKER_PATTERNS) {
    assert(!pattern.test(text), `${id}: internal blocker wording leaked: ${pattern} :: ${text}`);
  }
};

const CASES = [
  {
    id: 'make-scope',
    message: 'Should I buy a Maruti car?',
    expectedScope: 'make_scope',
    context: {
      selectedVehicleContext: {
        make: 'Maruti Suzuki',
      },
      decisionEvidencePack: {
        scope: 'make_scope',
        subject: { make: 'Maruti Suzuki' },
        strengths: ['broad service-network evidence supplied by fixture'],
        fitSignals: ['works when brand-level ownership reach matters'],
        evidenceSources: ['fixture_make_evidence'],
      },
    },
  },
  {
    id: 'model-scope',
    message: 'Should I buy Baleno?',
    expectedScope: 'model_scope',
    context: {
      selectedVehicleContext: {
        model: 'Baleno',
        brand: 'Maruti Suzuki',
        bodyType: 'hatchback',
        seatingCapacity: 5,
        fuelTypes: ['Petrol', 'CNG'],
        transmissions: ['Manual', 'Automatic'],
      },
      decisionEvidencePack: {
        scope: 'model_scope',
        scoreSignals: {
          value: { score: 74, band: 'good', source: 'fixture_model_score_profile' },
          runningCost: { score: 81, band: 'strong', source: 'fixture_model_score_profile' },
        },
        strengths: ['model-level value signal supplied by fixture'],
        evidenceSources: ['fixture_model_decision_profile'],
      },
    },
    assertText: (text) => {
      assert(/\bCNG\b/.test(text), 'CNG should be mentioned when fixture facts include CNG');
    },
  },
  {
    id: 'variant-scope',
    message: 'Should I buy Baleno Alpha?',
    expectedScope: 'variant_scope',
    context: {
      selectedVehicleContext: {
        model: 'Baleno',
        variant: 'Alpha',
        bodyType: 'hatchback',
        seatingCapacity: 5,
      },
      decisionEvidencePack: {
        scope: 'variant_scope',
        scoreSignals: {
          features: { score: 86, band: 'strong', source: 'fixture_variant_score_profile' },
          regretRisk: { score: 22, band: 'low', source: 'fixture_variant_score_profile' },
        },
        strengths: ['variant feature score supplied by fixture'],
        watchouts: ['price step needs buyer budget confirmation'],
        evidenceSources: ['fixture_variant_decision_profile'],
      },
    },
  },
  {
    id: 'comparison-scope',
    message: 'Should I buy Baleno or Altroz?',
    expectedScope: 'comparison_scope',
    context: {
      decisionEvidencePack: {
        scope: 'comparison_scope',
        subject: {
          comparisonTargets: [
            { model: 'Baleno', label: 'Baleno' },
            { model: 'Altroz', label: 'Altroz' },
          ],
        },
        alternativeSignals: ['compare safety, features and ownership signals from supplied evidence'],
        evidenceSources: ['fixture_similar_model_graph'],
      },
    },
  },
  {
    id: 'upgrade-scope',
    message: 'Should I stretch from Baleno Zeta to Alpha?',
    expectedScope: 'upgrade_scope',
    context: {
      decisionEvidencePack: {
        scope: 'upgrade_scope',
        subject: {
          upgradeBase: { model: 'Baleno', variant: 'Zeta', label: 'Baleno Zeta' },
          upgradeTarget: { model: 'Baleno', variant: 'Alpha', label: 'Baleno Alpha' },
        },
        upgradeSignals: ['upgrade ladder fixture shows added equipment worth reviewing against price gap'],
        watchouts: ['stretch only if added equipment matters to the buyer'],
        evidenceSources: ['fixture_upgrade_ladder'],
      },
    },
  },
  {
    id: 'discovery-scope',
    message: 'Best car under 15 lakh for family?',
    expectedScope: 'discovery_scope',
    context: {
      buyerContext: {
        budgetOrPriceCeiling: 1500000,
        primaryUseCase: 'family use',
      },
      decisionEvidencePack: {
        scope: 'discovery_scope',
        subject: {
          discoveryLabel: 'cars under 15 lakh for family',
        },
        fitSignals: ['family-use budget discovery evidence supplied by fixture'],
        alternativeSignals: ['compare shortlisted models after budget filtering'],
        evidenceSources: ['fixture_discovery_read_model'],
      },
    },
  },
  {
    id: 'city-automatic-signals',
    message: 'Should I buy Baleno automatic for city use?',
    expectedScope: 'model_scope',
    context: {
      buyerContext: {
        primaryUseCase: 'city use',
        transmissionPreference: 'automatic',
      },
      selectedVehicleContext: {
        model: 'Baleno',
        fuelTypes: ['Petrol'],
        transmissions: ['Automatic'],
      },
      decisionEvidencePack: {
        scope: 'model_scope',
        fitSignals: ['city automatic preference captured from message and fixture'],
        evidenceSources: ['fixture_city_automatic_context'],
      },
    },
    assertText: (text, guidance) => {
      assert.strictEqual(guidance.inferredContext.cityAutomaticPreference, true, 'city automatic signal missing');
      assert(!/\bpetrol automatic\b/i.test(text), `petrol automatic combo should not be synthesized: ${text}`);
    },
  },
  {
    id: 'family-highway-safety-signals',
    message: 'Should I buy Baleno for family highway use?',
    expectedScope: 'model_scope',
    context: {
      buyerContext: {
        primaryUseCase: 'family use, highway use',
      },
      selectedVehicleContext: {
        model: 'Baleno',
        seatingCapacity: 5,
      },
      decisionEvidencePack: {
        scope: 'model_scope',
        fitSignals: ['family highway signal captured from message and fixture'],
        watchouts: ['verify safety evidence before long highway-family use'],
        evidenceSources: ['fixture_family_highway_context'],
      },
    },
    assertText: (text, guidance) => {
      assert.strictEqual(guidance.inferredContext.safetySensitiveUse, true, 'family highway safety signal missing');
      assert(!/\bAltroz\b/i.test(text), `named alternative leaked without fixture alternative evidence: ${text}`);
    },
  },
];

const auditRuntimeHardcoding = () => {
  const runtimeSections = [
    {
      file: 'src/services/aciCore/decisionPolicy/aciBuyerDecisionInput.contract.cjs',
    },
    {
      file: 'src/services/aciCore/integration/aciCoreLiveBridge.service.js',
      start: 'const BUYER_GUIDANCE_TEMPLATE_BY_MODE',
      end: 'const isWeakGenericClarificationAnswer',
    },
    {
      file: 'src/services/aciCore/language/aciAnswerLanguageRegistry.js',
      start: 'decision_buyer_guidance_practical_first_view',
      end: 'decision_no_useful_evidence_recovery',
    },
    {
      file: 'src/services/aciCore/context/aciBuyerContextSignals.service.cjs',
    },
  ];
  const modelNames = /\b(Baleno|Altroz|Creta|Maruti)\b/i;
  const judgement = /\b(good for city|safer|best|should buy|worth buying|lacks safety|easy ownership)\b/i;

  for (const section of runtimeSections) {
    const source = fs.readFileSync(section.file, 'utf8');
    const startIndex = section.start ? source.indexOf(section.start) : 0;
    const endIndex = section.end ? source.indexOf(section.end, Math.max(0, startIndex)) : source.length;
    const text = source.slice(Math.max(0, startIndex), endIndex > startIndex ? endIndex : source.length);
    const lines = text.split('\n');
    lines.forEach((line, index) => {
      assert(
        !(modelNames.test(line) && judgement.test(line)),
        `${section.file}:${index + 1}: model-specific buyer judgement found in new guidance logic: ${line.trim()}`
      );
    });
  }
};

const runLiveBridgeCautionSmoke = async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) {
    return {
      skipped: true,
      reason: 'Mongo URI not configured',
    };
  }

  const { runAciCoreLiveBridge } = await import('../../services/aciCore/integration/aciCoreLiveBridge.service.js');
  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  try {
    const response = await runAciCoreLiveBridge({
      message: 'Should I buy Baleno?',
      context: {},
      meta: { smokeId: 'buyer-context-live-baleno' },
    });
    const answer = String(response.answer || response.data?.answer || '');
    const ux = response.finalBlockedUx || response.data?.finalBlockedUx || response.meta?.finalBlockedUx || {};
    const eligibility = response.finalRecommendationEligibility || response.data?.finalRecommendationEligibility || response.meta?.finalRecommendationEligibility;

    assert(eligibility, 'live path finalRecommendationEligibility missing');
    assert.strictEqual(eligibility.finalRecommendationEnabled, false, 'live path final recommendation enabled');
    assert.strictEqual(ux.status, 'provisional_buyer_guidance', `live path expected provisional guidance status, got ${ux.status}`);
    assert.strictEqual(ux.finalRecommendationEnabled, false, 'live UX final recommendation enabled');
    assert.strictEqual(ux.requestedFinalRecommendation, true, 'live UX missing requestedFinalRecommendation');
    assert.strictEqual(ux.canUseForFinalRecommendation, false, 'live UX canUseForFinalRecommendation should be false');
    assert(ux.provisionalGuidanceMode, 'live UX missing provisionalGuidanceMode');
    assert(Object.prototype.hasOwnProperty.call(ux, 'decisionScope'), 'live UX missing decisionScope summary field');
    assert(ux.allowedAnswerType, 'live UX missing allowedAnswerType');
    assert(Array.isArray(ux.safeAnswerTypesNow), 'live UX missing safeAnswerTypesNow');
    assertNoForbiddenUxKeys(ux);

    for (const pattern of INTERNAL_BLOCKER_PATTERNS) {
      assert(!pattern.test(answer), `live answer leaked old blocker wording: ${pattern} :: ${answer}`);
    }

    return {
      skipped: false,
      status: ux.status,
      finalRecommendationEnabled: eligibility.finalRecommendationEnabled,
      answerPreview: answer.slice(0, 280),
    };
  } finally {
    await mongoose.disconnect();
  }
};

(async () => {
  auditRuntimeHardcoding();

  const results = [];
  for (const testCase of CASES) {
    const result = await renderGuidance(testCase);
    assertBuyerSafe({ id: testCase.id, result, expectedScope: testCase.expectedScope });
    if (testCase.assertText) testCase.assertText(result.text, result.eligibility.buyerGuidanceContext);

    results.push({
      id: testCase.id,
      message: testCase.message,
      scope: result.eligibility.buyerGuidanceContext.decisionEvidencePack.scope,
      guidanceMode: result.eligibility.provisionalGuidanceMode,
      finalRecommendationEnabled: result.eligibility.finalRecommendationEnabled,
      finalPurchaseVerdictEnabled: result.eligibility.buyerGuidanceContext.finalPurchaseVerdictEnabled,
      allowedAnswerType: result.eligibility.allowedAnswerType,
      templateKey: result.rendered.templateKey,
      variantId: result.rendered.variantId,
      evidenceSources: result.eligibility.buyerGuidanceContext.decisionEvidencePack.evidenceSources,
      answerPreview: result.text.slice(0, 360),
    });
  }

  const liveBridge = await runLiveBridgeCautionSmoke();

  console.log(JSON.stringify({
    suite: 'ACI Buyer Context Behavior Smoke v1',
    ok: true,
    total: results.length,
    passed: results.length,
    failed: 0,
    liveBridge,
    runtimeHardcodingAudit: {
      ok: true,
      checked: [
        'aciBuyerDecisionInput.contract.cjs',
        'aciCoreLiveBridge.service.js',
        'aciAnswerLanguageRegistry.js',
        'aciBuyerContextSignals.service.cjs',
      ],
    },
    results,
  }, null, 2));
})().catch((error) => {
  console.error(error);
  mongoose.disconnect().catch(() => {}).finally(() => process.exit(1));
});
