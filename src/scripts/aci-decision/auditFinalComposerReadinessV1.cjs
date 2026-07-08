#!/usr/bin/env node

const assert = require('assert');
const fs = require('fs');

const read = (file) => fs.readFileSync(file, 'utf8');

const files = {
  languageComposer: 'src/services/aciCore/language/aciAnswerLanguageComposer.js',
  languageRegistry: 'src/services/aciCore/language/aciAnswerLanguageRegistry.js',
  liveBridge: 'src/services/aciCore/integration/aciCoreLiveBridge.service.js',
  finalEligibility: 'src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs',
  finalReadiness: 'src/services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs',
  scoreTool: 'src/services/aiAgent/tools/newCars/vehicleScoreInsight.tool.js',
  responseTools: 'src/services/aiAgent/aiAgent.responseTools.js',
  executor: 'src/services/aiAgent/aiAgent.executor.js',
  phase5Closure: 'src/scripts/aci-decision/smokeDecisionPhase5ClosureGateV1.cjs',
};

const content = Object.fromEntries(
  Object.entries(files).map(([key, file]) => [key, read(file)])
);

const checks = [];
const warnings = [];

const add = (id, ok, extra = {}) => checks.push({ id, ok: Boolean(ok), ...extra });
const warn = (id, details = {}) => warnings.push({ id, ...details });

add(
  'central-language-composer-exists',
  content.languageComposer.includes('renderAciLanguageText') &&
    content.languageComposer.includes('renderAciTemplate')
);

add(
  'central-language-registry-has-final-blocked-templates',
  content.languageRegistry.includes('decision_final_blocked_missing_context') &&
    content.languageRegistry.includes('decision_final_blocked_partial_results')
);

add(
  'central-language-registry-has-diagnostic-guardrails',
  content.languageRegistry.includes('decision_diagnostic_only_note') &&
    content.languageRegistry.includes('decision_score_guardrail_reason') &&
    content.languageRegistry.includes('decision_similar_graph_guardrail_reason')
);

add(
  'final-readiness-still-marks-composer-not-ready',
  content.finalReadiness.includes('finalComposerReady: false') &&
    content.finalReadiness.includes('recommendationActivationEnabled: false') &&
    content.finalReadiness.includes('canActivateFinalRecommendation: false')
);

add(
  'final-eligibility-still-dry-run',
  content.finalEligibility.includes('dryRun: true') &&
    content.finalEligibility.includes('finalRecommendationEnabled: false') &&
    content.finalEligibility.includes('composerReady: false') &&
    content.finalEligibility.includes('canUseForFinalRecommendation: false')
);

add(
  'live-bridge-still-blocks-final-recommendation',
  content.liveBridge.includes('finalRecommendationEnabled: false') &&
    content.liveBridge.includes('final_recommendation_blocked') &&
    content.liveBridge.includes('finalBlockedUx')
);

add(
  'phase5-closure-still-protects-activation-leakage',
  content.phase5Closure.includes('no-final-recommendation-activation-leakage') &&
    content.phase5Closure.includes('/finalRecommendationEnabled:\\s*true/') &&
    content.phase5Closure.includes('/composerReady:\\s*true/')
);

const unsafeActivationPatterns = [
  /finalRecommendationEnabled\s*:\s*true/,
  /composerReady\s*:\s*true/,
  /finalComposerReady\s*:\s*true/,
  /recommendationActivationEnabled\s*:\s*true/,
  /canActivateFinalRecommendation\s*:\s*true/,
];

const runtimeFiles = {
  liveBridge: content.liveBridge,
  finalEligibility: content.finalEligibility,
  finalReadiness: content.finalReadiness,
  scoreTool: content.scoreTool,
  responseTools: content.responseTools,
  executor: content.executor,
};

const activationLeakage = [];
for (const [name, text] of Object.entries(runtimeFiles)) {
  for (const pattern of unsafeActivationPatterns) {
    if (pattern.test(text)) {
      activationLeakage.push({ file: name, pattern: String(pattern) });
    }
  }
}

add('no-final-composer-activation-leakage', activationLeakage.length === 0, {
  activationLeakage,
});

const scatteredDiagnosticLanguagePatterns = [
  /\blet\s+verdict\s*=/,
  /\bconst\s+verdict\s*=/,
  /\bverdict\s*\+=/,
  /\blooks like a strong same-model value pick\b/i,
  /\bchoose it only if\b/i,
];

const scatteredDiagnostics = [];
for (const [name, text] of Object.entries({
  scoreTool: content.scoreTool,
  responseTools: content.responseTools,
  executor: content.executor,
})) {
  scatteredDiagnosticLanguagePatterns.forEach((pattern) => {
    if (pattern.test(text)) {
      scatteredDiagnostics.push({ file: name, pattern: String(pattern) });
    }
  });
}

if (scatteredDiagnostics.length) {
  warn('diagnostic-language-still-needs-central-composer-migration', {
    count: scatteredDiagnostics.length,
    items: scatteredDiagnostics,
    note: 'This is not final recommendation activation, but Phase 6 must migrate diagnostic/verdict wording into the central composer before final verdict activation.',
  });
}

add('diagnostic-verdict-wording-centralized', scatteredDiagnostics.length === 0, {
  scatteredDiagnostics,
});

add('final-activation-still-blocked-after-wording-centralization',
  scatteredDiagnostics.length === 0 &&
    content.finalReadiness.includes('recommendationActivationEnabled: false') &&
    content.finalReadiness.includes('canActivateFinalRecommendation: false') &&
    content.finalEligibility.includes('finalRecommendationEnabled: false') &&
    content.liveBridge.includes('finalRecommendationEnabled: false'),
  {
    diagnosticVerdictWordingCentralized: scatteredDiagnostics.length === 0,
    activationAllowed: false,
  }
);

add(
  'final-composer-readiness-status-is-not-ready',
  scatteredDiagnostics.length >= 0 &&
    content.finalReadiness.includes('finalComposerReady: false')
);

const failed = checks.filter((check) => !check.ok);

console.log(JSON.stringify({
  suite: 'ACI Final Composer Readiness Audit v1',
  ok: failed.length === 0,
  readiness: {
    centralLanguageComposerAvailable:
      content.languageComposer.includes('renderAciLanguageText') &&
      content.languageComposer.includes('renderAciTemplate'),
    diagnosticVerdictWordingCentralized: scatteredDiagnostics.length === 0,
    finalComposerReady: false,
    finalRecommendationActivationReady: false,
    activationAllowed: false,
    reason: scatteredDiagnostics.length === 0
      ? 'Diagnostic/verdict wording is centralized, but final verdict activation remains disabled until final recommendation policy, evidence thresholds, buyer-journey evals and current-market validation are explicitly activated.'
      : 'Central composer exists, but final verdict activation remains disabled until diagnostic/verdict wording is fully centralized and buyer-journey evals pass.',
  },
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((item) => item.id),
  checks,
  warnings,
}, null, 2));

if (failed.length) {
  process.exit(1);
}
