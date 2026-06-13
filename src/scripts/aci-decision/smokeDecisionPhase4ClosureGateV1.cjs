#!/usr/bin/env node

const fs = require('fs');
const assert = require('assert');

const read = (file) => fs.readFileSync(file, 'utf8');

const packageJson = JSON.parse(read('package.json'));
const scripts = packageJson.scripts || {};
const gateRunner = read('src/scripts/aci-decision/runDecisionGateParallelV1.cjs');
const progress = read('src/services/aciProgress/aciProgress.registry.cjs');
const finalEligibility = read('src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs');
const liveBridge = read('src/services/aciCore/integration/aciCoreLiveBridge.service.js');
const languageRegistry = read('src/services/aciCore/language/aciAnswerLanguageRegistry.js');

const checks = [];

function add(id, ok, details = {}) {
  checks.push({ id, ok: Boolean(ok), ...details });
}

function between(source, startNeedle, endNeedle) {
  const start = source.indexOf(startNeedle);
  if (start < 0) return '';
  const end = source.indexOf(endNeedle, start + startNeedle.length);
  if (end < 0) return source.slice(start);
  return source.slice(start, end);
}

const phase0Block = between(gateRunner, 'phase0: [', '],\n};');
const similarFullBlock = between(gateRunner, 'similarFull: [', '],\n  phase0');

add('closure-script-wired', scripts['aci:decision:closure:smoke'] === 'node src/scripts/aci-decision/smokeDecisionPhase4ClosureGateV1.cjs');
add('phase0-has-closure-smoke', phase0Block.includes('decision-phase4-closure-smoke'));

add(
  'phase0-uses-fast-runtime-envelope',
  phase0Block.includes('decision-runtime-envelope-smoke-fast') &&
    phase0Block.includes('runtime-envelope:smoke:fast')
);

add(
  'phase0-uses-fast-final-eligibility',
  phase0Block.includes('decision-final-eligibility-smoke-fast') &&
    phase0Block.includes('final-eligibility:smoke:fast')
);

add(
  'gate-runner-has-no-stale-max-workers-reference',
  !/\bMAX_WORKERS\b/.test(gateRunner)
);

add(
  'phase0-has-safe-default-worker-count',
  /phase0:\s*1/.test(gateRunner) &&
    /function getGateWorkers/.test(gateRunner) &&
    /ACI_DECISION_GATE_WORKERS/.test(gateRunner)
);

add('phase0-uses-fast-similar-output', phase0Block.includes('similar-output-fixture-fast'));
add('phase0-uses-fast-similar-graph', phase0Block.includes('similar-graph-smoke-fast'));
add('phase0-uses-fast-similar-filter', phase0Block.includes('similar-filter-audit-fast'));
const similarGateStart = gateRunner.indexOf('similar: [');
const similarFullGateStart = gateRunner.indexOf('similarFull: [');
const phase0GateStart = gateRunner.indexOf('phase0: [');
const relationFastTaskIndex = gateRunner.indexOf('similar-relation-mode-eval-fast');
const relationFastScriptIndex = gateRunner.indexOf('aci:decision:similar-relation-mode:eval:fast');

add(
  'similar-gate-uses-fast-similar-relation-outside-phase0',
  similarGateStart >= 0 &&
    similarFullGateStart > similarGateStart &&
    phase0GateStart > similarFullGateStart &&
    relationFastTaskIndex > similarGateStart &&
    relationFastTaskIndex < similarFullGateStart &&
    relationFastScriptIndex > similarGateStart &&
    relationFastScriptIndex < similarFullGateStart
);

add('similar-full-keeps-full-output', similarFullBlock.includes('similar-output-fixture-full'));
add('similar-full-keeps-full-graph', similarFullBlock.includes('similar-graph-smoke-full'));
add('similar-full-keeps-full-filter', similarFullBlock.includes('similar-filter-audit-full'));
add('similar-full-keeps-full-relation', similarFullBlock.includes('similar-relation-mode-eval-full'));

add('fast-relation-sample-reduced', /ACI_SIMILAR_RELATION_SAMPLE_LIMIT=8/.test(scripts['aci:decision:similar-relation-mode:eval:fast'] || ''));
add('fast-filter-sample-reduced', /ACI_SIMILAR_AUDIT_SAMPLE_LIMIT=8/.test(scripts['aci:decision:similar-filter:audit:fast'] || ''));
add('full-relation-sample-preserved', /ACI_SIMILAR_RELATION_SAMPLE_LIMIT=80/.test(scripts['aci:decision:similar-relation-mode:eval:full'] || ''));
add('full-filter-sample-preserved', /ACI_SIMILAR_AUDIT_SAMPLE_LIMIT=80/.test(scripts['aci:decision:similar-filter:audit:full'] || ''));

add('final-eligibility-stays-disabled', /canUseForFinalRecommendation:\s*false/.test(finalEligibility));
add('final-eligibility-dry-run-present', /dryRun:\s*true/.test(finalEligibility));
add('final-eligibility-policy-not-ready-block-present', /FINAL_RECOMMENDATION_POLICY_NOT_READY/.test(finalEligibility));

add('live-bridge-attaches-final-blocked-ux', liveBridge.includes('finalBlockedUx') && liveBridge.includes('final_recommendation_blocked'));
add(
  'language-registry-has-final-choice-guidance',
  /decision_buyer_guidance_practical_first_view|decision_buyer_guidance_conditional|decision_buyer_guidance_sharpened_recommendation/i.test(languageRegistry) &&
    /openingLine/.test(languageRegistry) &&
    /usefulViewLine/.test(languageRegistry) &&
    /assumptionLine/.test(languageRegistry) &&
    /Best next question:/.test(liveBridge)
);
add('progress-tracker-has-phase4e', /Phase 4E adds final-blocked readiness UX/i.test(progress));
add('progress-tracker-points-to-phase4f-or-phase5', /Phase 4F closure gate|Phase 5|Buyer Context/i.test(progress));

const riskyRuntimeFiles = [
  'src/services/aciCore/integration/aciCoreLiveBridge.service.js',
  'src/services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js',
  'src/services/aiAgent/tools/newCars/vehicleSimilar.tool.js',
  'src/services/aiAgent/aiAgent.responseTools.js',
  'src/services/aiAgent/aiAgent.executor.js',
  'src/services/aciCore/scoreProfiles/aciCrossModelScoreDiagnostic.service.cjs',
  'src/services/aciCore/scoreProfiles/aciVariantScoreInsight.service.cjs',
  'src/services/aciCore/scoreProfiles/aciVariantScoreProfile.reader.cjs',
];

const leakage = [];
for (const file of riskyRuntimeFiles) {
  const text = read(file);
  const lines = text.split('\n');
  lines.forEach((line, idx) => {
    if (
      /\bcanUseForFinalRecommendation\s*:\s*true\b/.test(line) ||
      /\bcanUseForFinalRecommendation\s*=\s*true\b/.test(line) ||
      /\ballowedAnswerType\s*:\s*['"]final_recommendation_allowed['"]/.test(line) ||
      /\bFINAL_RECOMMENDATION_ALLOWED\b/.test(line)
    ) {
      leakage.push({ file, line: idx + 1, text: line.trim() });
    }
  });
}

add('no-runtime-final-recommendation-leakage', leakage.length === 0, { leakage });

const failed = checks.filter((check) => !check.ok);

const summary = {
  suite: 'ACI Decision Phase 4F Closure Gate Smoke v1',
  ok: failed.length === 0,
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((check) => check.id),
  checks,
};

console.log(JSON.stringify(summary, null, 2));

if (!summary.ok) process.exit(1);
