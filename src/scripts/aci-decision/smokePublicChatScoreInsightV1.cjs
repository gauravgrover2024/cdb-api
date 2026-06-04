#!/usr/bin/env node

const BASE_URL = process.env.ACI_API_BASE_URL || 'http://localhost:5050';
const ENDPOINT = '/api/ai-agent/public-chat';
const QUERY = 'How good is Baleno petrol manual overall?';

const bridgeOf = (json = {}) =>
  json.aciCoreBridge ||
  json.meta?.aciCoreBridge ||
  json.data?.aciCoreBridge ||
  null;

const hasUnsafeLanguage = (json = {}) =>
  /\bmust buy\b|\bbuy this\b|\bbuy it\b|\bgo for this\b|\bbest choice\b|\bbest pick\b|\bclear winner\b|\brecommended buy\b/i
    .test(JSON.stringify(json || {}));

const assert = (condition, message) => {
  if (!condition) throw new Error(message);
};

async function main() {
  const res = await fetch(`${BASE_URL}${ENDPOINT}`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      accept: 'application/json',
    },
    body: JSON.stringify({
      message: QUERY,
      context: {},
      meta: { source: 'smokePublicChatScoreInsightV1' },
    }),
  });

  const raw = await res.text();
  let json = null;
  try {
    json = raw ? JSON.parse(raw) : null;
  } catch (_) {}

  const bridge = bridgeOf(json || {});
  const text = JSON.stringify(json || {});
  const answer = String(
    json?.answer ||
    json?.message ||
    json?.text ||
    json?.finalAnswer ||
    ''
  );

  const summary = {
    suite: 'ACI Public Chat Score Insight Smoke v1',
    ok: false,
    endpoint: ENDPOINT,
    httpStatus: res.status,
    httpOk: res.ok,
    query: QUERY,
    intent: json?.intent || null,
    tool: bridge?.tool || json?.tool || null,
    primaryTask: bridge?.primaryTask || null,
    canvasType: json?.canvasType || json?.data?.canvasType || null,
    inlineType: json?.inlineType || json?.data?.inlineType || null,
    usedGemini: bridge?.usedGemini ?? null,
    contextIsolation: bridge?.contextIsolation || null,
    answerPreview: answer.slice(0, 500),
    hasAciCoreBridge: Boolean(bridge),
    hasGuardrailFalse:
      text.includes('"canUseForFinalRecommendation":false') ||
      text.includes('"finalRecommendationEnabled":false') ||
      /diagnostic/i.test(answer),
    hasUnsafeLanguage: hasUnsafeLanguage(json || {}),
  };

  try {
    assert(res.ok, `HTTP failed: ${res.status} ${raw.slice(0, 300)}`);
    assert(json, 'Response is not JSON.');
    assert(bridge, 'aciCoreBridge metadata missing.');
    assert(bridge.tool === 'vehicle_score_insight', `Expected vehicle_score_insight, got ${bridge.tool}`);
    assert(bridge.primaryTask === 'score_insight', `Expected score_insight, got ${bridge.primaryTask}`);
    assert(
      json.canvasType === 'score_insight_canvas' ||
        json.inlineType === 'score_insight_summary' ||
        text.includes('score_insight_canvas'),
      `Expected score insight canvas/inline response, got canvasType=${json.canvasType} inlineType=${json.inlineType}`
    );
    assert(/Baleno/i.test(answer) || /Baleno/i.test(text), 'Response does not mention Baleno.');
    assert(/petrol manual|Scope: petrol manual/i.test(answer) || /petrol_manual/i.test(text), 'Response does not preserve petrol manual scope.');
    assert(summary.hasGuardrailFalse, 'Diagnostic/final recommendation guardrail not found.');
    assert(!summary.hasUnsafeLanguage, 'Unsafe recommendation-like wording found.');
    assert(!/pricelist_canvas/i.test(json.canvasType || ''), 'Wrongly routed to pricelist_canvas.');
    summary.ok = true;
  } finally {
    console.log(JSON.stringify(summary, null, 2));
  }

  if (!summary.ok) process.exit(1);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
