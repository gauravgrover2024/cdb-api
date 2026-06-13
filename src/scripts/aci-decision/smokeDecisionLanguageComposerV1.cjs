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
  /\bKnown facts:/i,
  /\bStrong signals I can see:/i,
  /\bnot enough scored strength evidence\b/i,
  /\bnothing specific in the supplied evidence\b/i,
  /\bno upgrade-ladder evidence supplied\b/i,
  /\bselected model facts are available\b/i,
  /\bthis search can be assessed\b/i,
  /\byour use case matches the available facts and trade-offs\b/i,
  /\byour top priority is not covered by the available evidence\b/i,
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
      openingLine: 'For Baleno, I would not treat this as a final yes/no yet.',
      usefulViewLine: 'What I can use now: body type hatchback; 5-seater; available fuels Petrol and CNG.',
      strengthLine: 'What looks good: value score supplied by fixture.',
      watchoutLine: 'What to check: verify safety evidence.',
      fitLine: 'This fits better when: city use with moderate running.',
      alternativeLine: 'Compare alternatives if: safety is the top priority.',
      upgradeLine: ' ',
      assumptionLine: '',
      softQuestion: 'One useful next question: Is your use mostly city, highway, or mixed?',
    },
    mustMatch: /\b(final yes\/no|What looks good|What to check|Baleno)\b/i,
  },
  {
    key: 'decision_buyer_guidance_conditional',
    input: {
      openingLine: 'For Baleno, I would not treat this as a final yes/no yet.',
      usefulViewLine: 'What I can use now: body type hatchback; 5-seater.',
      strengthLine: ' ',
      watchoutLine: ' ',
      fitLine: 'This fits better when: usage is still open.',
      alternativeLine: 'Compare alternatives if: buyer priority is not covered.',
      upgradeLine: ' ',
      assumptionLine: '',
      softQuestion: 'One useful next question: Is your use mostly city, highway, or mixed?',
    },
    mustMatch: /\b(final yes\/no|What looks good|What to check|Baleno)\b/i,
  },
  {
    key: 'decision_buyer_guidance_sharpened_recommendation',
    input: {
      openingLine: 'For Baleno automatic, I would not treat this as a final yes/no yet.',
      usefulViewLine: 'What I can use now: body type hatchback; available transmissions Manual and Automatic.',
      strengthLine: 'What looks good: city-use score supplied by fixture.',
      watchoutLine: 'What to check: highway evidence needs review.',
      fitLine: 'This fits better when: city use with automatic preference.',
      alternativeLine: 'Compare alternatives if: family highway safety is top priority.',
      upgradeLine: ' ',
      assumptionLine: '',
      softQuestion: 'One useful next question: Share the one priority you want me to weigh most.',
    },
    mustMatch: /\b(final yes\/no|What looks good|What to check|Baleno)\b/i,
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
