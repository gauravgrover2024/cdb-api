#!/usr/bin/env node
require('dotenv').config();

const assert = require('assert');

const unsafePattern =
  /\b(must buy|buy this|buy it|clear winner|recommended buy|best final choice|final recommendation disabled|missing buyer context|evidence threshold|score snapshot|safetyScore|global-percentile|taxonomy-driven|normalization|diagnostic-only module scoring)\b/i;

const cases = [
  {
    id: 'tiago-altroz-final-choice',
    message: 'Which one should I finally choose: Tiago or Altroz CNG automatic?',
    answerMustInclude: [/Tiago/i, /Altroz/i],
  },
  {
    id: 'baleno-altroz-buy-choice',
    message: 'Should I buy Baleno or Altroz?',
    answerMustInclude: [/Baleno/i, /Altroz/i],
  },
  {
    id: 'tiago-cng-altroz-cng-pick',
    message: 'Which should I pick, Tiago CNG or Altroz CNG?',
    answerMustInclude: [/Tiago/i, /Altroz/i],
  },
];

const getBridge = (output = {}) =>
  output.aciCoreBridge || output.meta?.aciCoreBridge || output.data?.aciCoreBridge || {};

(async () => {
  const mod = await import('../../services/aciCore/integration/aciCoreLiveBridge.service.js');
  const runAciCoreLiveBridge = mod.runAciCoreLiveBridge || mod.default;
  assert.strictEqual(typeof runAciCoreLiveBridge, 'function', 'runAciCoreLiveBridge export missing');

  const results = [];

  for (const testCase of cases) {
    const output = await runAciCoreLiveBridge({
      message: testCase.message,
      context: {},
      user: null,
      session: {},
      meta: { source: 'smokePlainFinalChoiceComparisonEnvelopeV1' },
    });

    const bridge = getBridge(output);
    const answer = String(output.answer || output.text || '');

    assert(
      testCase.answerMustInclude.every((pattern) => pattern.test(answer)),
      `${testCase.id}: answer must mention both compared targets. Answer: ${answer}`
    );

    assert(
      bridge.operation !== 'cross_model_score_diagnostic' && output.operation !== 'cross_model_score_diagnostic',
      `${testCase.id}: plain final-choice comparison must not route to score diagnostic without score language`
    );

    assert(!unsafePattern.test(answer), `${testCase.id}: unsafe/internal wording leaked: ${answer}`);
    assert(!/\bFor This Comparison\b/i.test(answer), `${testCase.id}: generic comparison label leaked: ${answer}`);
    assert(!/\bFor your car search\b/i.test(answer), `${testCase.id}: generic discovery label leaked: ${answer}`);
    assert(!/\bFor\s+(?:Buy|Pick|Choose|Go For),?\s/i.test(answer), `${testCase.id}: generic action prefix leaked: ${answer}`);

    results.push({
      id: testCase.id,
      message: testCase.message,
      answerPreview: answer.slice(0, 550),
      bridge: {
        tool: bridge.tool || '',
        primaryTask: bridge.primaryTask || '',
        operation: bridge.operation || '',
        routingReason: bridge.routingReason || '',
        contextIsolation: bridge.contextIsolation || '',
      },
    });
  }

  console.log(JSON.stringify({
    suite: 'ACI Plain Final-Choice Comparison Envelope Smoke v1',
    ok: true,
    total: results.length,
    passed: results.length,
    results,
  }, null, 2));
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
