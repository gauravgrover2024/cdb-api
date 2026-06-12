#!/usr/bin/env node

const fs = require('fs');

const checks = [];

function read(file) {
  return fs.readFileSync(file, 'utf8');
}

function addCheck(id, ok, details = {}) {
  checks.push({ id, ok: Boolean(ok), ...details });
}

const registry = read('src/services/aciCore/language/aciAnswerLanguageRegistry.js');
const liveBridge = read('src/services/aciCore/integration/aciCoreLiveBridge.service.js');
const finalSmoke = read('src/scripts/aci-decision/smokeFinalRecommendationEligibilityRuntimeV1.cjs');

addCheck(
  'final-choice-language-avoids-disabled-wording',
  !/Final recommendation remains disabled|final recommendation disabled|cannot give a buy-this verdict|Missing buyer inputs|Missing buyer context/i.test(registry)
);

addCheck(
  'final-choice-language-has-practical-guidance',
  /practical first view|conditional guidance|sharpened guidance/i.test(registry)
);

addCheck(
  'final-choice-language-labels-assumptions',
  /Assumption:/i.test(registry)
);

addCheck(
  'live-bridge-preserves-existing-answer-under-blocked-prefix',
  liveBridge.includes('shouldPreserveExisting') &&
    liveBridge.includes('`${blockedAnswer}\\n\\n${existingAnswer}`')
);

addCheck(
  'live-bridge-attaches-final-blocked-ux-object',
  liveBridge.includes('finalBlockedUx') &&
    liveBridge.includes('safeAnswerTypesNow') &&
    liveBridge.includes('provisional_buyer_guidance') &&
    liveBridge.includes('final_recommendation_blocked')
);

addCheck(
  'runtime-smoke-asserts-buyer-facing-readiness',
  finalSmoke.includes('final-blocked readiness wording missing') &&
    finalSmoke.includes('finalBlockedUx readiness object missing')
);

addCheck(
  'runtime-smoke-blocks-unsafe-buy-language',
  finalSmoke.includes('you should buy') &&
    finalSmoke.includes('unsafe final recommendation wording leaked')
);

const failed = checks.filter((check) => !check.ok);
const summary = {
  suite: 'ACI Decision Final Blocked Readiness UX Smoke v1',
  ok: failed.length === 0,
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((check) => check.id),
  checks,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) {
  process.exit(1);
}
