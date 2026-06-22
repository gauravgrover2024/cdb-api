const READINESS_VERSION = 'aci_candidate_evidence_readiness_contract_v1';

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const asObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) ? value : {};
const textOf = (value) => String(value ?? '').trim();
const lower = (value) => textOf(value).toLowerCase();

const unique = (items = []) => {
  const seen = new Set();
  const out = [];
  for (const item of items.map(textOf).filter(Boolean)) {
    const key = lower(item);
    if (!seen.has(key)) {
      seen.add(key);
      out.push(item);
    }
  }
  return out;
};

const valuePresent = (value) => {
  if (Array.isArray(value)) return value.filter(Boolean).length > 0;
  if (typeof value === 'number') return Number.isFinite(value) && value > 0;
  if (value && typeof value === 'object') return Object.keys(value).length > 0;
  const text = textOf(value);
  return Boolean(text) && text !== '0';
};

const hasBuyerInput = (buyerContext = {}, keys = []) =>
  keys.some((key) => valuePresent(buyerContext[key]));

const getMissingFinalBuyerInputs = (buyerContext = {}) => {
  const missing = [];

  if (!hasBuyerInput(buyerContext, ['city', 'citySlug'])) {
    missing.push('city');
  }

  if (!hasBuyerInput(buyerContext, ['budgetOrPriceCeiling', 'maxBudget', 'budget', 'priceCeiling'])) {
    missing.push('budgetOrPriceCeiling');
  }

  if (!hasBuyerInput(buyerContext, ['bodyPreferenceOrPrimaryUseCase', 'primaryUseCase', 'useCase', 'bodyPreference'])) {
    missing.push('bodyPreferenceOrPrimaryUseCase');
  }

  if (!hasBuyerInput(buyerContext, ['familySizeOrOccupancyUse', 'familySize', 'occupancy', 'seatingNeed'])) {
    missing.push('familySizeOrOccupancyUse');
  }

  if (!hasBuyerInput(buyerContext, ['fuelPreferenceOrMonthlyRunning', 'fuelPreference', 'fuel', 'monthlyRunning', 'running'])) {
    missing.push('fuelPreferenceOrMonthlyRunning');
  }

  if (!hasBuyerInput(buyerContext, ['transmissionPreference', 'transmission', 'transmissionType'])) {
    missing.push('transmissionPreference');
  }

  if (!hasBuyerInput(buyerContext, ['safetyPriority'])) {
    missing.push('safetyPriority');
  }

  if (!hasBuyerInput(buyerContext, ['featurePriority', 'mustHaveFeatures'])) {
    missing.push('featurePriority');
  }

  if (!hasBuyerInput(buyerContext, ['shortlistedModelsOrDiscoveryScope', 'discoveryScope', 'shortlistedModels'])) {
    missing.push('shortlistedModelsOrDiscoveryScope');
  }

  return missing;
};

const detectRequestedFinalRecommendation = ({ buyerContext = {}, bridge = {}, response = {} } = {}) => {
  if (buyerContext?.inferredBuyerContext?.finalChoiceIntent === true) return true;

  const text = [
    bridge.effectiveMessage,
    bridge.originalMessage,
    response.effectiveMessage,
    response.originalMessage,
    response.query,
  ].map(lower).filter(Boolean).join(' ');

  return (
    /\bwhich\s+(car|one|model|variant)\s+should\s+i\s+(buy|choose|pick|go\s+for)\b/i.test(text) ||
    /\bshould\s+i\s+(buy|choose|pick|go\s+for)\b/i.test(text) ||
    /\bfinal\s+(recommendation|verdict|answer|decision)\b/i.test(text) ||
    /\bdecide\s+for\s+me\b/i.test(text)
  );
};

const getRowEvidenceStatus = (row = {}) => {
  const decisionStatus = lower(row.decisionCandidate?.evidenceStatus);
  const scoreStatus = lower(row.scoreSignals?.status);
  const featureStatus = lower(row.featureSignals?.status);

  const hasScore = scoreStatus === 'available' || scoreStatus === 'partial';
  const hasFeature = featureStatus === 'available' || featureStatus === 'partial';
  const hasCandidateEvidence = decisionStatus === 'partial' || decisionStatus === 'complete';

  if (hasCandidateEvidence && hasScore && hasFeature) return 'partial';
  if (hasCandidateEvidence || hasScore || hasFeature) return 'limited';
  return 'missing';
};

const buildCandidateRowReadiness = ({ row = {}, buyerContext = {} } = {}) => {
  const evidenceStatus = getRowEvidenceStatus(row);
  const scoreStatus = lower(row.scoreSignals?.status);
  const featureStatus = lower(row.featureSignals?.status);
  const missingEvidence = [];

  if (!(scoreStatus === 'available' || scoreStatus === 'partial')) {
    missingEvidence.push('score_profile_evidence');
  }

  if (!(featureStatus === 'available' || featureStatus === 'partial')) {
    missingEvidence.push('feature_summary_evidence');
  }

  const canUseForDiagnosticShortlist = ['partial', 'limited'].includes(evidenceStatus);
  const missingBuyerInputsForFinalRecommendation = getMissingFinalBuyerInputs(buyerContext);

  return {
    version: READINESS_VERSION,
    status: canUseForDiagnosticShortlist ? 'diagnostic_ready_final_blocked' : 'diagnostic_limited_final_blocked',
    evidenceStatus,
    canUseForDiagnosticShortlist,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    missingEvidence,
    missingBuyerInputsForFinalRecommendation,
    blockedReasons: unique([
      'final_recommendation_policy_not_ready',
      'final_composer_not_ready',
      'recommendation_activation_disabled',
      ...missingEvidence.map((item) => `missing_${item}`),
      ...(missingBuyerInputsForFinalRecommendation.length ? ['buyer_context_incomplete'] : []),
    ]),
    nextAllowedUse: canUseForDiagnosticShortlist
      ? 'diagnostic_shortlist_and_followup_questions'
      : 'show_candidate_with_evidence_gap_warning',
  };
};

const summarizeCandidateEvidenceReadiness = (contract = {}) => ({
  version: contract.version || READINESS_VERSION,
  status: contract.status || '',
  requestedFinalRecommendation: contract.requestedFinalRecommendation === true,
  canUseForDiagnosticShortlist: contract.canUseForDiagnosticShortlist === true,
  canUseForFinalRecommendation: false,
  finalRecommendationEnabled: false,
  allowedAnswerType: contract.allowedAnswerType || 'diagnostic_only',
  evidenceStatus: contract.evidenceStatus || 'missing',
  usableEvidenceCount: Number(contract.usableEvidenceCount || 0),
  requiredEvidenceCount: Number(contract.requiredEvidenceCount || 0),
  missingScoreEvidenceCount: Number(contract.missingScoreEvidenceCount || 0),
  missingFeatureEvidenceCount: Number(contract.missingFeatureEvidenceCount || 0),
  missingBuyerInputsForFinalRecommendation: asArray(contract.missingBuyerInputsForFinalRecommendation),
  blockedReasons: asArray(contract.blockedReasons),
  nextAllowedStep: contract.nextAllowedStep || '',
  guardrail: contract.guardrail || '',
});

function buildCandidateEvidenceReadinessContract({
  rows = [],
  buyerContext = {},
  bridge = {},
  response = {},
} = {}) {
  const candidateRows = asArray(rows);
  const requestedFinalRecommendation = detectRequestedFinalRecommendation({ buyerContext, bridge, response });

  const rowsWithReadiness = candidateRows.map((row) => {
    const readiness = buildCandidateRowReadiness({ row, buyerContext });

    return {
      ...row,
      decisionCandidate: {
        ...asObject(row.decisionCandidate),
        evidenceReadiness: readiness,
        canUseForDiagnosticShortlist: readiness.canUseForDiagnosticShortlist,
        canUseForFinalRecommendation: false,
        finalRecommendationEnabled: false,
      },
      evidenceSummary: {
        ...asObject(row.evidenceSummary),
        readinessStatus: readiness.status,
        missingEvidence: readiness.missingEvidence,
        missingBuyerInputsForFinalRecommendation: readiness.missingBuyerInputsForFinalRecommendation,
      },
      candidateEvidenceReadiness: readiness,
    };
  });

  const rowReadiness = rowsWithReadiness.map((row) => row.candidateEvidenceReadiness).filter(Boolean);
  const usableEvidenceCount = rowReadiness.filter((item) => item.canUseForDiagnosticShortlist).length;
  const missingScoreEvidenceCount = rowReadiness.filter((item) => item.missingEvidence.includes('score_profile_evidence')).length;
  const missingFeatureEvidenceCount = rowReadiness.filter((item) => item.missingEvidence.includes('feature_summary_evidence')).length;
  const missingBuyerInputsForFinalRecommendation = getMissingFinalBuyerInputs(buyerContext);

  const canUseForDiagnosticShortlist = candidateRows.length > 0 && usableEvidenceCount > 0;
  const evidenceStatus =
    usableEvidenceCount === candidateRows.length && candidateRows.length > 0
      ? 'partial'
      : usableEvidenceCount > 0
        ? 'limited'
        : 'missing';

  const blockedReasons = unique([
    'final_recommendation_policy_not_ready',
    'final_composer_not_ready',
    'recommendation_activation_disabled',
    ...(missingBuyerInputsForFinalRecommendation.length ? ['buyer_context_incomplete'] : []),
    ...(missingScoreEvidenceCount ? ['some_candidates_missing_score_profile_evidence'] : []),
    ...(missingFeatureEvidenceCount ? ['some_candidates_missing_feature_summary_evidence'] : []),
  ]);

  const contract = {
    version: READINESS_VERSION,
    status: canUseForDiagnosticShortlist
      ? 'diagnostic_shortlist_ready_final_blocked'
      : 'diagnostic_shortlist_limited_final_blocked',
    requestedFinalRecommendation,
    canUseForDiagnosticShortlist,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    allowedAnswerType: 'diagnostic_only',
    evidenceStatus,
    usableEvidenceCount,
    requiredEvidenceCount: candidateRows.length,
    missingScoreEvidenceCount,
    missingFeatureEvidenceCount,
    missingBuyerInputsForFinalRecommendation,
    blockedReasons,
    nextAllowedStep: canUseForDiagnosticShortlist
      ? 'show_diagnostic_shortlist_and_ask_next_best_missing_input'
      : 'show_evidence_gap_or_recovery_options',
    guardrail: 'Candidate evidence can support diagnostic shortlisting only. Final recommendation is disabled until policy, composer, evidence, and buyer-input gates are complete.',
    rows: rowsWithReadiness,
  };

  return {
    ...contract,
    summary: summarizeCandidateEvidenceReadiness(contract),
  };
}

export {
  READINESS_VERSION,
  buildCandidateEvidenceReadinessContract,
  summarizeCandidateEvidenceReadiness,
};

export default buildCandidateEvidenceReadinessContract;
