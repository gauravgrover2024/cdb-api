#!/usr/bin/env node
'use strict';

const assert = require('assert');

const FORBIDDEN_ALWAYS = [
  /\byou should buy\b/i,
  /\bbuy this\b/i,
  /\bbest final choice\b/i,
  /\bcanUseForFinalRecommendation\s*:\s*true\b/i,
];

const TEMPLATE_CASES = [
  {
    key: 'decision_final_blocked_missing_context',
    input: {
      missingInputs: ['city', 'budget ceiling', 'safety priority'],
      nextCapabilities: ['price', 'safety', 'running cost'],
    },
    mustMatch: /\b(cannot|need|missing|recommendation|compare)\b/i,
  },
  {
    key: 'decision_final_blocked_partial_results',
    input: {
      missingInputs: ['city', 'budget ceiling', 'safety priority'],
    },
    mustMatch: /\b(results|starting points|discovery|recommendation|missing|blocked)\b/i,
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
