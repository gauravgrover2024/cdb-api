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
    const rendered = renderAciTemplate(item.key, {}, { seed: `decision-language-smoke|${item.key}` });
    const text = renderAciLanguageText(item.key, {}, { seed: `decision-language-smoke|${item.key}` });

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
