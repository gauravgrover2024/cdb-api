#!/usr/bin/env node

const assert = require('assert');
const fs = require('fs');

const read = (file) => fs.readFileSync(file, 'utf8');

const packageJson = JSON.parse(read('package.json'));
const scripts = packageJson.scripts || {};

const gateRunner = read('src/scripts/aci-decision/runDecisionGateParallelV1.cjs');
const progress = read('src/services/aciProgress/aciProgress.registry.cjs');
const buyerInputContract = read('src/services/aciCore/decisionPolicy/aciBuyerDecisionInput.contract.cjs');
const buyerContextExtractor = read('src/services/aciCore/context/aciBuyerContextExtractor.service.js');
const buyerClarification = read('src/services/aciCore/decisionPolicy/aciBuyerInputClarification.service.cjs');
const finalReadiness = read('src/services/aciCore/decisionPolicy/aciFinalRecommendationReadiness.service.cjs');
const finalEligibility = read('src/services/aciCore/decisionPolicy/aciFinalRecommendationEligibility.service.cjs');
const finalEligibilitySmoke = read('src/scripts/aci-decision/smokeFinalRecommendationEligibilityRuntimeV1.cjs');

const checks = [];

const add = (id, ok, extra = {}) => {
  checks.push({
    id,
    ok: Boolean(ok),
    ...extra,
  });
};

add('buyer-input-contract-file-present', buyerInputContract.includes('aci_buyer_decision_input_contract_v1'));
add('buyer-context-extractor-file-present', buyerContextExtractor.includes('extractBuyerContextFromMessage'));
add('buyer-clarification-file-present', buyerClarification.includes('aci_buyer_input_clarification_v1'));
add('final-readiness-file-present', finalReadiness.includes('aci_final_recommendation_policy_readiness_v1'));

add('buyer-context-extractor-learns-from-chat-signals',
  buyerContextExtractor.includes('extractBudget') &&
    buyerContextExtractor.includes('extractCity') &&
    buyerContextExtractor.includes('extractUseCase') &&
    buyerContextExtractor.includes('extractRunning') &&
    buyerContextExtractor.includes('extractFuel') &&
    buyerContextExtractor.includes('extractTransmission') &&
    buyerContextExtractor.includes('extractSafetyPriority') &&
    buyerContextExtractor.includes('extractFeaturePriority')
);

add('clarification-no-flood-policy-present',
  buyerClarification.includes("mode: 'progressive_single_question'") &&
    buyerClarification.includes('maxBuyerFacingQuestions: 1') &&
    buyerClarification.includes('revealAllMissingInputsToUser: false') &&
    buyerClarification.includes('learnFromSearchAndContext: true') &&
    buyerClarification.includes('suppressRepeatedRecommendationPrompts: true')
);

add('buyer-facing-rendering-contract-present',
  buyerClarification.includes('BUYER_FACING_RENDERING_CONTRACT') &&
    buyerClarification.includes("renderOnly: ['buyerFacingQuestions[0]', 'nextBestQuestion']") &&
    buyerClarification.includes('maxVisibleQuestions: 1') &&
    buyerClarification.includes("'internalMissingInputMap'") &&
    buyerClarification.includes("'questions'") &&
    buyerClarification.includes("'missingInputs'")
);

add('internal-map-is-explicitly-internal-only',
  buyerClarification.includes("internalOnlyPurpose: 'policy_debug_composer_only'")
);

add('final-readiness-stays-disabled',
  finalReadiness.includes('canActivateFinalRecommendation: false') &&
    finalReadiness.includes("activationMode: 'disabled_dry_run'") &&
    finalReadiness.includes('finalRecommendationPolicyReady: false') &&
    finalReadiness.includes('finalComposerReady: false') &&
    finalReadiness.includes('recommendationActivationEnabled: false') &&
    finalReadiness.includes('recommendation_activation_disabled')
);

add('final-eligibility-attaches-readiness',
  finalEligibility.includes('buildFinalRecommendationPolicyReadiness') &&
    finalEligibility.includes('finalPolicyReadiness: requestedFinalRecommendation ? finalPolicyReadiness : null')
);

add('final-eligibility-remains-dry-run',
  finalEligibility.includes('dryRun: true') &&
    finalEligibility.includes('canUseForFinalRecommendation: false') &&
    finalEligibility.includes('finalRecommendationEnabled: false') &&
    finalEligibility.includes('composerReady: false')
);

add('phase0-has-buyer-input-contract-smoke', gateRunner.includes('buyer-decision-input-contract-smoke'));
add('phase0-has-buyer-context-extraction-smoke', gateRunner.includes('buyer-context-extraction-smoke'));
add('phase0-has-buyer-clarification-smoke', gateRunner.includes('buyer-input-clarification-smoke'));
add('phase0-has-context-reuse-readiness-smoke', gateRunner.includes('buyer-context-reuse-readiness-smoke'));
const extractGateBlock = (name) => {
  const match = gateRunner.match(new RegExp(`${name}:\\s*\\[[\\s\\S]*?\\n\\s*\\],`));
  return match ? match[0] : '';
};

const similarGateBlock = extractGateBlock('similar');
const phase0GateBlock = extractGateBlock('phase0');

add(
  'similar-relation-mode-regression-remains-wired-outside-phase0',
  similarGateBlock.includes('similar-relation-mode-eval-fast') &&
    similarGateBlock.includes('aci:decision:similar-relation-mode:eval:fast') &&
    !phase0GateBlock.includes('similar-relation-mode-eval-fast')
);


add('package-has-phase5-smoke-scripts',
  scripts['aci:decision:buyer-input:smoke'] &&
    scripts['aci:decision:buyer-context:smoke'] &&
    scripts['aci:decision:buyer-clarification:smoke'] &&
    scripts['aci:decision:context-reuse-readiness:smoke']
);

add('final-eligibility-smoke-protects-no-activation',
  finalEligibilitySmoke.includes('canUseForFinalRecommendation') &&
    finalEligibilitySmoke.includes('buyerInputClarification') &&
    finalEligibilitySmoke.includes('finalRecommendationStillDisabled')
);

add('final-eligibility-smoke-accepts-safe-blocked-wording',
  finalEligibilitySmoke.includes('hasFinalBlockedReadinessWording') &&
    finalEligibilitySmoke.includes('should not recommend one yet')
);

add('progress-tracker-has-phase5a', progress.includes('decision_buyer_input_contract_v1'));
add('progress-tracker-has-phase5b', progress.includes('decision_buyer_context_extraction_v1'));
add('progress-tracker-has-phase5c', progress.includes('decision_buyer_input_clarification_v1'));
add('progress-tracker-has-phase5d', progress.includes('decision_context_reuse_final_readiness_v1'));

const unsafeRuntimePatterns = [
  /canActivateFinalRecommendation:\s*true/,
  /finalRecommendationEnabled:\s*true/,
  /composerReady:\s*true/,
  /recommendationActivationEnabled:\s*true/,
  /finalRecommendationPolicyReady:\s*true/,
  /finalComposerReady:\s*true/,
];

const runtimeFiles = {
  finalReadiness,
  finalEligibility,
};

const leakage = [];
for (const [name, content] of Object.entries(runtimeFiles)) {
  for (const pattern of unsafeRuntimePatterns) {
    if (pattern.test(content)) {
      leakage.push({ name, pattern: String(pattern) });
    }
  }
}

add('no-final-recommendation-activation-leakage', leakage.length === 0, { leakage });

const failed = checks.filter((check) => !check.ok);

console.log(JSON.stringify({
  suite: 'ACI Decision Phase 5 Closure Gate Smoke v1',
  ok: failed.length === 0,
  total: checks.length,
  passed: checks.length - failed.length,
  failed: failed.length,
  failedIds: failed.map((item) => item.id),
  checks,
}, null, 2));

if (failed.length) {
  process.exit(1);
}
