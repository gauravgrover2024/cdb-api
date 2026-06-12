#!/usr/bin/env node
'use strict';

const assert = require('assert');

const FORBIDDEN_ALWAYS = [
  /\byou should buy\b/i,
  /\bbuy this\b/i,
  /\bbest final choice\b/i,
  /\bcanUseForFinalRecommendation\s*:\s*true\b/i,
  /\bfinal recommendation remains disabled\b/i,
  /\bfinal recommendation disabled\b/i,
  /\bmissing buyer context\b/i,
  /\bevidence threshold not met\b/i,
  /\bpolicy not ready\b/i,
  /\bblockedReasons\b/,
  /\binternalMissingInputMap\b/,
  /\bmissing mandatory inputs\b/i,
];

const TEMPLATE_CASES = [
  {
    key: 'decision_final_blocked_missing_context',
    input: {
      missingInputs: ['city', 'budget ceiling', 'safety priority'],
      nextCapabilities: ['price', 'safety', 'running cost'],
    },
    mustMatch: /\b(practical|diagnostic|compare|guidance|trade-offs)\b/i,
  },
  {
    key: 'decision_final_blocked_partial_results',
    input: {
      missingInputs: ['city', 'budget ceiling', 'safety priority'],
    },
    mustMatch: /\b(results|starting points|discovery|diagnostic|trade-off|verdict)\b/i,
  },
  {
    key: 'decision_buyer_guidance_practical_first_view',
    input: {
      model: 'Baleno',
      vehicleFactsLine: 'body type hatchback; 5-seater; available fuels Petrol and CNG',
      strengthsLine: 'value score supplied by fixture',
      watchoutsLine: 'verify safety evidence',
      fitLine: 'city use with moderate running',
      alternativeLine: 'safety is the top priority',
      upgradeLine: 'no upgrade-ladder evidence supplied',
      buyerContextLine: 'city: Delhi',
      assumptionsLine: 'assuming monthly running is not very high',
      softQuestion: 'Is your use mostly city, highway, or mixed?',
    },
    mustMatch: /\b(practical first view|Strong signals|Watchouts|Assumption|Baleno)\b/i,
  },
  {
    key: 'decision_buyer_guidance_conditional',
    input: {
      model: 'Baleno',
      vehicleFactsLine: 'body type hatchback; 5-seater',
      strengthsLine: 'not enough scored strength evidence yet',
      watchoutsLine: 'nothing specific in the supplied evidence',
      fitLine: 'usage is still open',
      alternativeLine: 'buyer priority is not covered',
      upgradeLine: 'no upgrade-ladder evidence supplied',
      assumptionsLine: 'assuming normal 4-5 person use',
      softQuestion: 'Is your use mostly city, highway, or mixed?',
    },
    mustMatch: /\b(conditional guidance|Strong signals|Watchouts|Assumption|Baleno)\b/i,
  },
  {
    key: 'decision_buyer_guidance_sharpened_recommendation',
    input: {
      model: 'Baleno',
      vehicleFactsLine: 'body type hatchback; available transmissions Manual and Automatic',
      strengthsLine: 'city-use score supplied by fixture',
      watchoutsLine: 'highway evidence needs review',
      fitLine: 'city use with automatic preference',
      alternativeLine: 'family highway safety is top priority',
      upgradeLine: 'no upgrade-ladder evidence supplied',
      buyerContextLine: 'use case: city use; safety priority: high',
      assumptionsLine: 'assuming monthly running is not very high',
      softQuestion: 'Share the one priority you want me to weigh most.',
    },
    mustMatch: /\b(sharpened guidance|Strong signals|Watchouts|Assumption|Baleno)\b/i,
  },
  {
    key: 'decision_no_useful_evidence_recovery',
    input: {
      topic: 'final recommendation',
      nextCapabilities: ['price', 'features', 'comparison'],
    },
    mustMatch: /\b(evidence|data|guess|recover|help)\b/i,
  },
  {
    key: 'decision_exact_variant_unavailable_recovery',
    input: {
      model: 'Baleno',
      variant: 'Alpha Plus',
      nextCapabilities: ['listed variants', 'model-level price', 'features'],
    },
    mustMatch: /\b(exact|variant|catalog|listed|model)\b/i,
  },
  {
    key: 'decision_diagnostic_only_note',
    mustMatch: /\bdiagnostic-only\b/i,
  },
  {
    key: 'decision_score_module_summary_note',
    mustMatch: /\bdiagnostic-only|module-score|module scores|diagnostics\b/i,
  },
  {
    key: 'decision_score_guardrail_reason',
    mustMatch: /\bdiagnostic|score\b/i,
  },
  {
    key: 'decision_similar_graph_guardrail_reason',
    mustMatch: /\bdiscovery|similar|graph|alternatives\b/i,
    mustNotMatch: /\bfinal recommendation\b/i,
  },
  {
    key: 'decision_similar_graph_note',
    mustMatch: /\bdiscovery|alternatives|purchase verdict|buy\/not-buy\b/i,
    mustNotMatch: /\bfinal recommendation\b/i,
  },
];

(async () => {
  const {
    renderAciTemplate,
    renderAciLanguageText,
  } = await import('../../services/aciCore/language/aciAnswerLanguageComposer.js');

  const results = [];

  for (const item of TEMPLATE_CASES) {
    const rendered = renderAciTemplate(item.key, item.input || {}, { seed: `decision-language-smoke|${item.key}` });
    const text = renderAciLanguageText(item.key, item.input || {}, { seed: `decision-language-smoke|${item.key}` });

    assert(rendered && !rendered.missingTemplate, `${item.key}: template missing`);
    assert(text, `${item.key}: rendered text missing`);

    assert(item.mustMatch.test(text), `${item.key}: expected wording missing: ${text}`);
    if (item.mustNotMatch) {
      assert(!item.mustNotMatch.test(text), `${item.key}: forbidden wording found: ${text}`);
    }

    for (const pattern of FORBIDDEN_ALWAYS) {
      assert(!pattern.test(text), `${item.key}: unsafe buy/final flag language found: ${text}`);
    }

    results.push({
      key: item.key,
      variantId: rendered.variantId,
      text,
    });
  }

  console.log(JSON.stringify({
    suite: 'ACI Decision Language Composer Smoke v1',
    ok: true,
    total: results.length,
    passed: results.length,
    failed: 0,
    results,
  }, null, 2));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
