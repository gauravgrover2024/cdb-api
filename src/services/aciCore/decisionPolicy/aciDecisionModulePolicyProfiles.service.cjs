const {
  CLAIM_TYPES,
  ALLOWED_ANSWER_TYPES,
  DEGRADED_MODES,
  BLOCKED_REASONS,
  DECISION_MODULES,
} = require('./aciDecisionPolicy.constants.cjs');

const {
  createBaseDecisionPolicy,
} = require('./aciDecisionOutput.contract.cjs');

const {
  evaluateDecisionPolicy,
} = require('./aciDecisionPolicy.service.cjs');

const MODULE_POLICY_PROFILES = Object.freeze({
  [DECISION_MODULES.SCORE_INSIGHT]: Object.freeze({
    module: DECISION_MODULES.SCORE_INSIGHT,
    defaultClaimType: CLAIM_TYPES.DIAGNOSTIC,
    defaultAllowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
    canEverUseForFinalRecommendation: false,
  }),
  [DECISION_MODULES.SIMILAR_CARS]: Object.freeze({
    module: DECISION_MODULES.SIMILAR_CARS,
    defaultClaimType: CLAIM_TYPES.DIAGNOSTIC,
    defaultAllowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
    canEverUseForFinalRecommendation: false,
  }),
  [DECISION_MODULES.UPGRADE_LADDER]: Object.freeze({
    module: DECISION_MODULES.UPGRADE_LADDER,
    defaultClaimType: CLAIM_TYPES.DIAGNOSTIC,
    defaultAllowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
    canEverUseForFinalRecommendation: false,
  }),
  [DECISION_MODULES.COMPARISON]: Object.freeze({
    module: DECISION_MODULES.COMPARISON,
    defaultClaimType: CLAIM_TYPES.DIAGNOSTIC,
    defaultAllowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
    canEverUseForFinalRecommendation: false,
  }),
  [DECISION_MODULES.RECOMMENDATION]: Object.freeze({
    module: DECISION_MODULES.RECOMMENDATION,
    defaultClaimType: CLAIM_TYPES.DIAGNOSTIC,
    defaultAllowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
    canEverUseForFinalRecommendation: true,
  }),
});

const DEFAULT_MODULE_POLICY_PROFILE = Object.freeze({
  module: '',
  defaultClaimType: CLAIM_TYPES.DIAGNOSTIC,
  defaultAllowedAnswerType: ALLOWED_ANSWER_TYPES.DIAGNOSTIC_ONLY,
  canEverUseForFinalRecommendation: false,
});

const uniq = (values) => [...new Set((values || []).filter(Boolean))];

function getDecisionModulePolicyProfile(moduleName = '') {
  return MODULE_POLICY_PROFILES[moduleName] || {
    ...DEFAULT_MODULE_POLICY_PROFILE,
    module: moduleName || '',
  };
}

function applyModulePolicyProfile(input = {}) {
  const basePolicy = createBaseDecisionPolicy(input.decisionPolicy || evaluateDecisionPolicy(input));
  const moduleName = input.module || '';
  const profile = getDecisionModulePolicyProfile(moduleName);

  const blockedReasons = [...(basePolicy.blockedReasons || [])];
  let allowedAnswerType = basePolicy.allowedAnswerType || profile.defaultAllowedAnswerType;
  let claimType = basePolicy.claimType || profile.defaultClaimType;
  let canUseForFinalRecommendation = basePolicy.canUseForFinalRecommendation === true;
  let degradedMode = basePolicy.degradedMode || null;

  if (!profile.canEverUseForFinalRecommendation) {
    if (input.requestedFinalRecommendation === true || canUseForFinalRecommendation) {
      blockedReasons.push(BLOCKED_REASONS.MODULE_NOT_FINAL_RECOMMENDATION_ELIGIBLE);
      blockedReasons.push(BLOCKED_REASONS.FINAL_RECOMMENDATION_POLICY_NOT_READY);
      degradedMode = degradedMode || DEGRADED_MODES.FINAL_RECOMMENDATION_BLOCKED;
    }

    canUseForFinalRecommendation = false;

    if (allowedAnswerType === ALLOWED_ANSWER_TYPES.FINAL_RECOMMENDATION_ALLOWED) {
      allowedAnswerType = profile.defaultAllowedAnswerType;
    }

    if (claimType === CLAIM_TYPES.OPINION) {
      claimType = profile.defaultClaimType;
    }
  }

  return createBaseDecisionPolicy({
    ...basePolicy,
    canUseForFinalRecommendation,
    allowedAnswerType,
    blockedReasons: uniq(blockedReasons),
    degradedMode,
    claimType,
  });
}

function applyDecisionPolicyWithModuleProfile(input = {}) {
  const decisionPolicy = applyModulePolicyProfile({
    ...input,
    decisionPolicy: evaluateDecisionPolicy(input),
  });

  return {
    ...input,
    claimType: decisionPolicy.claimType,
    degradedMode: decisionPolicy.degradedMode,
    decisionPolicy,
  };
}

module.exports = {
  MODULE_POLICY_PROFILES,
  getDecisionModulePolicyProfile,
  applyModulePolicyProfile,
  applyDecisionPolicyWithModuleProfile,
};
