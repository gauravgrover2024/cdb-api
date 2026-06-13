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
  /\bscore profile\b/i,
  /\bground-clearance normalization\b/i,
  /\bdiagnostic-only module scoring\b/i,
  /\bperformance score v2\b/i,
  /\bsafetyScore\b/i,
  /\bnormalization\b/i,
  /\bglobal-percentile\b/i,
  /\btaxonomy-driven\b/i,
  /\bscore snapshot\b/i,
  /\bMaruti Baleno Zeta looks feature-rich\b/i,
  /\bTata Altroz looks strongest\b/i,
  /\bscore profile coverage includes\b/i,
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

const MECHANICAL_GUIDANCE_PATTERNS = [
  /\bKnown facts:/i,
  /\bStrong signals I can see:/i,
  /\bnot enough scored strength evidence\b/i,
  /\bnothing specific in the supplied evidence\b/i,
  /\bno upgrade-ladder evidence supplied\b/i,
  /\bselected model facts are available\b/i,
  /\bthis search can be assessed\b/i,
  /\byour use case matches the available facts and trade-offs\b/i,
  /\byour top priority is not covered by the available evidence\b/i,
  /\bI still need grounded vehicle evidence\b/i,
  /\bbefore treating this as more than provisional guidance\b/i,
  /\bnot enough evidence\b/i,
  /\bbackend\/debug-style evidence\b/i,
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

const optionalLine = (value = '') => String(value || '').trim() || ' ';

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
  if (scope === 'variant_scope' && (facts.variant || subject.variant)) {
    return [facts.make || facts.brand || subject.make, facts.model || subject.model, facts.variant || subject.variant].filter(Boolean).join(' ') || fallback;
  }
  return facts.fullModel || [facts.make || facts.brand, facts.model, facts.variant].filter(Boolean).join(' ') || fallback;
};

const factLineForGuidance = (facts = {}) => [
  facts.bodyType ? `body type ${facts.bodyType}` : '',
  facts.seatingCapacity ? `${facts.seatingCapacity}-seater` : '',
  facts.fuelTypes?.length ? `available fuels ${join(facts.fuelTypes)}` : '',
  facts.transmissions?.length ? `available transmissions ${join(facts.transmissions)}` : '',
  facts.safetyFeatures?.length ? `known safety features ${join(facts.safetyFeatures)}` : '',
  facts.featureHighlights?.length ? `known feature highlights ${join(facts.featureHighlights)}` : '',
  facts.similarAlternatives?.length ? `similar alternatives ${join(facts.similarAlternatives)}` : '',
].filter(Boolean).join('; ');

const buyerContextLineForGuidance = (guidance = {}) =>
  Object.entries(guidance.explicitBuyerContext || {})
    .filter(([, value]) => value !== undefined && value !== null && String(value).trim() !== '')
    .map(([key, value]) => `${key}: ${Array.isArray(value) ? join(value) : value}`)
    .join('; ');

const openingLineForGuidance = ({ scope = '', model = '' } = {}) => {
  if (scope === 'make_scope') return `For ${model}, I would keep this at brand level first: the exact model, budget, use case, and any ownership or resale evidence matter more than the badge alone.`;
  if (scope === 'variant_scope') return `For ${model}, I would judge the variant by the confirmed feature gains, price gap, and regret-risk evidence.`;
  if (scope === 'comparison_scope') return `For ${model}, the useful view is a trade-off comparison, not a single winner yet.`;
  if (scope === 'upgrade_scope') return `For ${model}, the decision is whether the upgrade evidence justifies the extra spend for your use case.`;
  if (scope === 'discovery_scope') return `For ${model}, I can keep this as provisional discovery guidance around budget, use case, and shortlist quality.`;
  return `For ${model}, I can give a provisional buying view from the evidence available.`;
};

const usefulViewLineForGuidance = ({ factsLine = '', buyerContextLine = '', scope = '' } = {}) => {
  if (factsLine && buyerContextLine) return `What I can use now: ${factsLine}. Buyer context captured: ${buyerContextLine}.`;
  if (factsLine) return `What I can use now: ${factsLine}. I would keep this modest until your use case and priorities are clearer.`;
  if (buyerContextLine) return `Buyer context captured: ${buyerContextLine}. I can keep this provisional for now.`;
  if (scope === 'make_scope') return 'The next step is to pin down the model, budget, and use case, because make-level guidance should stay broad.';
  if (scope === 'discovery_scope') return 'The next step is to compare shortlisted options on safety, features, value, running cost, and family practicality once those signals are available.';
  return 'I would keep this provisional until the exact use case, budget, and priority are clearer.';
};

const buyerSafeScoreSignalText = (key = '', value = {}) => {
  const band = String(value?.band || '').toLowerCase().replace(/_/g, ' ');
  const strong = /\b(strong|good|high)\b/.test(band);
  const weak = /\b(weak|very weak|poor|low)\b/.test(band);
  const average = /\b(average|moderate|ok)\b/.test(band);

  if (key === 'features') return strong ? 'feature evidence looks positive' : weak ? 'feature evidence needs comparison' : average ? 'feature evidence looks adequate' : '';
  if (key === 'value') return strong ? 'value evidence looks positive versus nearby variants' : weak ? 'value evidence needs nearby-variant comparison' : average ? 'value evidence needs nearby-variant comparison' : '';
  if (key === 'runningCost') return strong ? 'running-cost evidence looks positive' : weak ? 'running-cost evidence is not the main reason to choose it' : average ? 'running-cost evidence looks acceptable' : '';
  if (key === 'safety') return strong ? 'safety evidence looks positive, but verify source applicability' : 'safety evidence needs verified-source review';
  if (key === 'familyPracticality') return strong ? 'family-practicality evidence looks positive' : weak ? 'family-practicality evidence needs use-case review' : average ? 'family-practicality evidence looks acceptable' : '';
  if (key === 'comfort') return strong ? 'comfort evidence looks positive' : weak ? 'comfort evidence needs comparison' : average ? 'comfort evidence looks acceptable' : '';
  if (key === 'regretRisk') return strong || weak || average ? 'regret-risk evidence needs use-case review' : '';

  return '';
};

const formatScoreSignals = (scoreSignals = {}) => {
  return join(
    Object.entries(scoreSignals || {})
      .map(([key, value]) => buyerSafeScoreSignalText(key, value))
      .filter(Boolean)
  );
};


const composerInputForGuidance = ({ id = '', guidance = {} } = {}) => {
  const facts = guidance.selectedVehicleFacts || {};
  const evidencePack = guidance.decisionEvidencePack || {};
  const model = subjectLabel({ facts, evidencePack, fallback: id });
  const factsLine = factLineForGuidance(facts);
  const buyerContextLine = buyerContextLineForGuidance(guidance);
  const scoreLine = formatScoreSignals(evidencePack.scoreSignals || {});
  const strengths = join([...(evidencePack.strengths || []), scoreLine].filter(Boolean));
  const watchouts = join(evidencePack.watchouts || []);
  const fit = join(evidencePack.fitSignals || []);
  const alternatives = join(evidencePack.alternativeSignals || []);
  const upgrade = join(evidencePack.upgradeSignals || []);
  const assumptions = join(guidance.softAssumptions || []);
  const softQuestion = evidencePack.scope === 'make_scope'
    ? `Which ${model} model are you considering?`
    : guidance.softQuestion || 'Is your use mostly city, highway, or mixed?';

  return {
    model,
    openingLine: optionalLine(openingLineForGuidance({ scope: evidencePack.scope, model })),
    usefulViewLine: optionalLine(usefulViewLineForGuidance({ factsLine, buyerContextLine, scope: evidencePack.scope })),
    strengthLine: optionalLine(strengths ? `What looks good: ${strengths}.` : ''),
    watchoutLine: optionalLine(watchouts ? `What to check: ${watchouts}.` : ''),
    fitLine: optionalLine(fit ? `This fits better when: ${fit}.` : ''),
    alternativeLine: optionalLine(alternatives ? `Compare alternatives if: ${alternatives}.` : ''),
    upgradeLine: optionalLine(upgrade ? `For the upgrade: ${upgrade}.` : ''),
    assumptionLine: optionalLine(assumptions ? `Assumption: ${assumptions}.` : ''),
    softQuestion: optionalLine(`Best next question: ${softQuestion}`),
  };
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
  const templateKey = TEMPLATE_BY_MODE[guidance.guidanceMode];

  const rendered = renderAciTemplate(
    templateKey,
    composerInputForGuidance({ id, guidance }),
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
  for (const pattern of MECHANICAL_GUIDANCE_PATTERNS) {
    assert(!pattern.test(text), `${id}: mechanical guidance wording leaked: ${pattern} :: ${text}`);
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

const LIVE_PROXY_CASES = [
  {
    id: 'proxy-live-baleno-model',
    message: 'Should I buy Baleno?',
    expectedScope: 'model_scope',
    context: {
      candidateSnapshot: {
        vehicles: {
          models: [
            {
              displayName: 'Baleno',
              metadata: {
                make: 'Maruti Suzuki',
                model: 'Baleno',
              },
            },
          ],
        },
      },
      selectedVehicleContext: {
        make: 'Maruti Suzuki',
        model: 'Baleno',
        bodyType: 'hatchback',
        seatingCapacity: 5,
      },
      decisionEvidencePack: {
        scope: 'model_scope',
        strengths: ['model decision profile evidence is present in fixture'],
        evidenceSources: ['proxy_model_decision_profile'],
      },
    },
    assertText: (text) => {
      assert(/\bBaleno\b/.test(text), `model label missing in proxy answer: ${text}`);
    },
  },
  {
    id: 'proxy-live-baleno-alpha-variant',
    message: 'Should I buy Baleno Alpha?',
    expectedScope: 'variant_scope',
    context: {
      candidateSnapshot: {
        vehicles: {
          variants: [
            {
              displayName: 'Alpha',
              metadata: {
                make: 'Maruti Suzuki',
                model: 'Baleno',
                variant: 'Alpha',
              },
            },
          ],
        },
      },
      selectedVehicleContext: {
        make: 'Maruti Suzuki',
        model: 'Baleno',
        variant: 'Alpha',
      },
      decisionEvidencePack: {
        scope: 'variant_scope',
        scoreSignals: {
          features: { score: 86, band: 'strong', source: 'proxy_variant_score_profile' },
        },
        strengths: ['variant decision profile evidence is present in fixture'],
        evidenceSources: ['proxy_variant_decision_profile'],
      },
    },
    assertText: (text, guidance) => {
      assert(/\bAlpha\b/.test(text), `variant label missing in proxy answer: ${text}`);
      assert.strictEqual(guidance.selectedVehicleFacts.variant, 'Alpha', 'variant not retained in buyerGuidanceContext');
    },
  },
  {
    id: 'proxy-live-maruti-make',
    message: 'Should I buy Maruti?',
    expectedScope: 'make_scope',
    context: {
      candidateSnapshot: {
        vehicles: {
          makes: [
            {
              displayName: 'Maruti Suzuki',
              metadata: {
                make: 'Maruti Suzuki',
              },
            },
          ],
        },
      },
      decisionEvidencePack: {
        scope: 'make_scope',
        subject: { make: 'Maruti Suzuki' },
        fitSignals: ['make-level evidence is present in fixture'],
        evidenceSources: ['proxy_make_decision_profile'],
      },
    },
    assertText: (text) => {
      assert(/\bMaruti\b/.test(text), `make label missing in proxy answer: ${text}`);
      assert(!/\bthis search\b/i.test(text), `make answer fell back to search wording: ${text}`);
    },
  },
  {
    id: 'proxy-live-baleno-altroz-comparison',
    message: 'Should I buy Baleno or Altroz?',
    expectedScope: 'comparison_scope',
    context: {
      candidateSnapshot: {
        vehicles: {
          models: [
            { displayName: 'Baleno', metadata: { model: 'Baleno' } },
            { displayName: 'Altroz', metadata: { model: 'Altroz' } },
          ],
        },
      },
      decisionEvidencePack: {
        scope: 'comparison_scope',
        subject: {
          comparisonTargets: [
            { label: 'Baleno', model: 'Baleno' },
            { label: 'Altroz', model: 'Altroz' },
          ],
        },
        alternativeSignals: ['similar model graph evidence is present in fixture'],
        evidenceSources: ['proxy_similar_model_graph'],
      },
    },
    assertText: (text) => {
      assert(/\bBaleno\b/.test(text) && /\bAltroz\b/.test(text), `comparison labels missing in proxy answer: ${text}`);
    },
  },
  {
    id: 'proxy-live-baleno-zeta-alpha-upgrade',
    message: 'Should I stretch from Baleno Zeta to Alpha?',
    expectedScope: 'upgrade_scope',
    context: {
      selectedVehicleContext: {
        model: 'Baleno',
      },
      decisionEvidencePack: {
        scope: 'upgrade_scope',
        upgradeSignals: ['upgrade ladder evidence is present in fixture'],
        evidenceSources: ['proxy_upgrade_ladder'],
      },
    },
    assertText: (text, guidance) => {
      assert(/\bBaleno Zeta\b/.test(text) && /\bBaleno Alpha\b/.test(text), `upgrade labels missing in proxy answer: ${text}`);
      assert.strictEqual(guidance.decisionEvidencePack.subject.upgradeTarget.label, 'Baleno Alpha', 'upgrade target label not inferred generically');
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
  const liveCases = [
    {
      id: 'live-baleno-model',
      message: 'Should I buy Baleno?',
      expectedScope: 'model_scope',
      assertAnswer: (answer) => {
        assert(/\bBaleno\b/.test(answer), `live Baleno answer missing model: ${answer}`);
      },
    },
    {
      id: 'live-baleno-alpha-variant',
      message: 'Should I buy Baleno Alpha?',
      expectedScope: 'variant_scope',
      assertAnswer: (answer) => {
        assert(/\bAlpha\b/.test(answer), `live Baleno Alpha answer missing Alpha: ${answer}`);
      },
    },
    {
      id: 'live-maruti-make',
      message: 'Should I buy Maruti?',
      expectedScope: 'make_scope',
      assertAnswer: (answer) => {
        assert(/\bMaruti\b/.test(answer), `live Maruti answer missing make: ${answer}`);
        assert(/\bWhich Maruti model are you considering\?/i.test(answer), `live Maruti answer should ask model question: ${answer}`);
      },
    },
    {
      id: 'live-baleno-altroz-comparison',
      message: 'Should I buy Baleno or Altroz?',
      expectedScope: 'comparison_scope',
      assertAnswer: (answer) => {
        const guidancePrefix = answer.split('\n\n')[0] || answer;
        assert(/\bBaleno\b/.test(guidancePrefix) && /\bAltroz\b/.test(guidancePrefix), `live comparison guidance must include both cars: ${answer}`);
        assert(!/^\s*Tata Altroz looks strongest/i.test(answer), `live comparison answered as only Altroz: ${answer}`);
      },
    },
    {
      id: 'live-baleno-zeta-alpha-upgrade',
      message: 'Should I stretch from Baleno Zeta to Alpha?',
      expectedScope: 'upgrade_scope',
      assertAnswer: (answer) => {
        const guidancePrefix = answer.split('\n\n')[0] || answer;
        assert(/\bZeta\b/.test(guidancePrefix) && /\bAlpha\b/.test(guidancePrefix), `live upgrade guidance must include Zeta and Alpha: ${answer}`);
        assert(!/^\s*Maruti Baleno Zeta looks/i.test(answer), `live upgrade answered as only Zeta: ${answer}`);
      },
    },
  ];

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });
  try {
    const results = [];
    for (const testCase of liveCases) {
      const response = await runAciCoreLiveBridge({
        message: testCase.message,
        context: {},
        meta: { smokeId: `buyer-context-${testCase.id}` },
      });
      const answer = String(response.answer || response.data?.answer || '');
      const ux = response.finalBlockedUx || response.data?.finalBlockedUx || response.meta?.finalBlockedUx || {};
      const eligibility = response.finalRecommendationEligibility || response.data?.finalRecommendationEligibility || response.meta?.finalRecommendationEligibility;

      assert(eligibility, `${testCase.id}: live path finalRecommendationEligibility missing`);
      assert.strictEqual(eligibility.finalRecommendationEnabled, false, `${testCase.id}: live path final recommendation enabled`);
      assert.strictEqual(ux.status, 'provisional_buyer_guidance', `${testCase.id}: expected provisional guidance status, got ${ux.status}`);
      assert.strictEqual(ux.decisionScope, testCase.expectedScope, `${testCase.id}: expected ${testCase.expectedScope}, got ${ux.decisionScope}`);
      assert.strictEqual(ux.finalRecommendationEnabled, false, `${testCase.id}: live UX final recommendation enabled`);
      assert.strictEqual(ux.requestedFinalRecommendation, true, `${testCase.id}: live UX missing requestedFinalRecommendation`);
      assert.strictEqual(ux.canUseForFinalRecommendation, false, `${testCase.id}: live UX canUseForFinalRecommendation should be false`);
      assert(ux.provisionalGuidanceMode, `${testCase.id}: live UX missing provisionalGuidanceMode`);
      assert(Object.prototype.hasOwnProperty.call(ux, 'decisionScope'), `${testCase.id}: live UX missing decisionScope summary field`);
      assert(ux.allowedAnswerType, `${testCase.id}: live UX missing allowedAnswerType`);
      assert(Array.isArray(ux.safeAnswerTypesNow), `${testCase.id}: live UX missing safeAnswerTypesNow`);
      assertNoForbiddenUxKeys(ux);
      assert(countQuestions(answer) <= 1, `${testCase.id}: live answer has more than one question: ${answer}`);
      testCase.assertAnswer(answer);

      for (const pattern of INTERNAL_BLOCKER_PATTERNS) {
        assert(!pattern.test(answer), `${testCase.id}: live answer leaked old blocker wording: ${pattern} :: ${answer}`);
      }
      for (const pattern of MECHANICAL_GUIDANCE_PATTERNS) {
        assert(!pattern.test(answer), `${testCase.id}: live answer leaked mechanical guidance wording: ${pattern} :: ${answer}`);
      }

      const diagnosticNoteCount = (answer.match(/This score view is diagnostic-only|This is diagnostic-only module scoring|This is diagnostic-only, not a final recommendation|Treat this as diagnostic-only guidance/gi) || []).length;
      assert(diagnosticNoteCount <= 1, `${testCase.id}: duplicate diagnostic-only notes leaked: ${answer}`);

      results.push({
        id: testCase.id,
        message: testCase.message,
        status: ux.status,
        scope: ux.decisionScope,
        finalRecommendationEnabled: eligibility.finalRecommendationEnabled,
        evidenceSources: eligibility.buyerGuidanceContext?.decisionEvidencePack?.evidenceSources || [],
        answerPreview: answer.slice(0, 320),
      });
    }

    return {
      skipped: false,
      total: results.length,
      passed: results.length,
      results,
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
    if (testCase.id === 'live-maruti-make') {
      assert(!/\b(Tata Altroz|Baleno Sigma|I compared)\b/i.test(result.text), 'Maruti make query leaked stale comparison context.');
    }
    assert(
      !/\b(?:safety|features?|value|running\s*cost|mileage|family\s*practicality|comfort|regret\s*risk)\s+score\s+(?:is\s+)?(?:not\s+available|not\s+fully\s+scored|unavailable|\d)/i.test(result.text),
      `${testCase.id}: raw score dump leaked in live buyer guidance: ${result.text}`
    );
    assert(
      !/,\s*,|,\s*and\.|\band\s*\./i.test(result.text),
      `${testCase.id}: broken punctuation leaked in live buyer guidance: ${result.text}`
    );
    assert(
      !/\b(?:Feature score is|Safety-critical equipment|Ground-clearance|ground clearance|Highway score v2|Boot space data missing|CNG tank placement|NVH|tyre quality|braking feel|highway-assist features|taxonomy-driven|global-percentile|normalization|safetyScore|performance score v2|score snapshot|score profile|score excludes|not yet scored|diagnostic-only module scoring|power-to-weight unavailable|data missing or reduced|unavailable; practicality)\b/i.test(result.text),
      `${testCase.id}: technical score caveat leaked in live buyer guidance: ${result.text}`
    );
    if (testCase.id === 'live-baleno-zeta-alpha-upgrade') {
      assert(
        /^For Baleno Zeta to Baleno Alpha\b/.test(result.text),
        `${testCase.id}: upgrade opening label casing regressed: ${result.text}`
      );
    }

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

  const proxyResults = [];
  for (const testCase of LIVE_PROXY_CASES) {
    const result = await renderGuidance(testCase);
    assertBuyerSafe({ id: testCase.id, result, expectedScope: testCase.expectedScope });
    if (testCase.assertText) testCase.assertText(result.text, result.eligibility.buyerGuidanceContext);

    proxyResults.push({
      id: testCase.id,
      message: testCase.message,
      scope: result.eligibility.buyerGuidanceContext.decisionEvidencePack.scope,
      guidanceMode: result.eligibility.provisionalGuidanceMode,
      finalRecommendationEnabled: result.eligibility.finalRecommendationEnabled,
      templateKey: result.rendered.templateKey,
      variantId: result.rendered.variantId,
      answerPreview: result.text.slice(0, 360),
    });
  }

  const liveBridge = await runLiveBridgeCautionSmoke();

  console.log(JSON.stringify({
    suite: 'ACI Buyer Context Behavior Smoke v1',
    ok: true,
    total: results.length + proxyResults.length,
    passed: results.length + proxyResults.length,
    failed: 0,
    liveBridge,
    liveProxyCases: proxyResults,
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
