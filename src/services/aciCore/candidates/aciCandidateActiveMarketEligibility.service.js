const ACTIVE_MARKET_ELIGIBILITY_VERSION = 'aci_candidate_active_market_eligibility_v1';

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

const normalizeModelKey = (value = '') =>
  lower(value)
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const rowModelKey = (row = {}) =>
  normalizeModelKey(
    row.modelKey ||
      row.shortModelKey ||
      row.decisionCandidate?.modelKey ||
      row.candidateMarketConfidence?.evidence?.modelKey ||
      row.rawModelKey ||
      row.slug ||
      row.fullModel ||
      row.displayName ||
      row.model ||
      row.name ||
      '',
  );

const getMarketConfidence = (row = {}) =>
  asObject(row.candidateMarketConfidence || row.decisionCandidate?.marketConfidence);

const getMarketEvidence = (row = {}) => asObject(getMarketConfidence(row).evidence);

const confidenceRank = (band = '') => {
  switch (lower(band)) {
    case 'strong':
      return 4;
    case 'good':
      return 3;
    case 'limited':
      return 2;
    case 'weak':
      return 1;
    default:
      return 0;
  }
};

const getRequestedCity = ({ row = {}, response = {}, buyerContext = {}, bridge = {} } = {}) => {
  const market = getMarketConfidence(row);
  const candidates = [
    market.requestedCity,
    buyerContext.city,
    buyerContext.citySlug,
    response.filters?.city,
    response.citySlug,
    response.city,
    bridge.citySlug,
    bridge.city,
  ];

  const found = candidates.map(textOf).find(Boolean);
  const normalized = lower(found).replace(/\s+/g, '-');

  if (['delhi', 'new-delhi', 'newdelhi', 'ncr'].includes(normalized)) return 'new-delhi';
  if (['gurgaon', 'gurugram'].includes(normalized)) return 'gurgaon';
  if (normalized === 'noida') return 'noida';

  return normalized || 'new-delhi';
};

function evaluateRowActiveMarketEligibility({ row = {}, buyerContext = {}, bridge = {}, response = {} } = {}) {
  const market = getMarketConfidence(row);
  const evidence = getMarketEvidence(row);
  const confidenceBand = market.confidenceBand || 'unknown';
  const requestedCity = getRequestedCity({ row, response, buyerContext, bridge });

  const requestedCityPriceRows = Number(evidence.requestedCityPriceRows || 0);
  const requestedCityVariantCount = Number(evidence.requestedCityVariantCount || 0);
  const scoreProfileRows = Number(evidence.scoreProfileRows || 0);
  const featureSummaryRows = Number(evidence.featureSummaryRows || 0);
  const summaryRows = Number(evidence.summaryRows || 0);
  const requestedCitySummaryRows = Number(evidence.requestedCitySummaryRows || 0);
  const stalenessDays =
    evidence.stalenessDays === null || evidence.stalenessDays === undefined
      ? null
      : Number(evidence.stalenessDays);

  const risks = [];
  const strengths = [];
  const requirementsForFinal = [
    'official_or_current_market_source_validation',
    'current_model_sale_status_validation',
    'current_city_price_or_availability_validation',
  ];

  if (confidenceRank(confidenceBand) >= 3) {
    strengths.push(`DB market-confidence band is ${confidenceBand}`);
  } else {
    risks.push(`DB market-confidence band is ${confidenceBand || 'unknown'}`);
  }

  if (requestedCitySummaryRows > 0) {
    strengths.push(`model summary exists for ${requestedCity}`);
  } else {
    risks.push(`model summary not confirmed for ${requestedCity}`);
  }

  if (requestedCityPriceRows >= 8) {
    strengths.push(`healthy ${requestedCity} price-row coverage`);
  } else if (requestedCityPriceRows >= 3) {
    strengths.push(`usable ${requestedCity} price-row coverage`);
  } else if (requestedCityPriceRows > 0) {
    risks.push(`thin ${requestedCity} price-row coverage`);
  } else {
    risks.push(`no ${requestedCity} price-row coverage`);
  }

  if (requestedCityVariantCount >= 8) {
    strengths.push('healthy requested-city variant depth');
  } else if (requestedCityVariantCount >= 3) {
    strengths.push('usable requested-city variant depth');
  } else {
    risks.push('thin requested-city variant depth');
  }

  if (scoreProfileRows > 0) {
    strengths.push('score-profile evidence exists');
  } else {
    risks.push('score-profile evidence missing');
  }

  if (featureSummaryRows > 0 || row.featureSignals?.status === 'available') {
    strengths.push('feature-summary evidence exists');
  } else {
    risks.push('feature-summary evidence missing');
  }

  if (summaryRows <= 0) risks.push('model-summary evidence missing');

  if (stalenessDays !== null) {
    if (stalenessDays <= 45) {
      strengths.push('evidence is recently updated');
    } else if (stalenessDays <= 120) {
      risks.push('evidence freshness is moderate');
    } else {
      risks.push('evidence appears stale');
    }
  } else {
    risks.push('evidence freshness unavailable');
  }

  let diagnosticUseAllowed = true;
  let status = 'db_evidence_eligible_for_diagnostic_external_validation_required';
  let activeMarketConfidenceBand = 'good';

  if (
    confidenceRank(confidenceBand) >= 4 &&
    requestedCityPriceRows >= 8 &&
    requestedCityVariantCount >= 8 &&
    scoreProfileRows > 0 &&
    (featureSummaryRows > 0 || row.featureSignals?.status === 'available')
  ) {
    status = 'db_evidence_strong_external_validation_required';
    activeMarketConfidenceBand = 'strong';
  } else if (
    confidenceRank(confidenceBand) >= 3 &&
    requestedCityPriceRows >= 3 &&
    requestedCityVariantCount >= 3 &&
    scoreProfileRows > 0
  ) {
    status = 'db_evidence_usable_external_validation_required';
    activeMarketConfidenceBand = 'good';
  } else if (requestedCityPriceRows > 0 || confidenceRank(confidenceBand) >= 2) {
    status = 'db_evidence_limited_external_validation_required';
    activeMarketConfidenceBand = 'limited';
  } else {
    status = 'db_evidence_weak_not_eligible_for_diagnostic_ranking';
    activeMarketConfidenceBand = 'weak';
    diagnosticUseAllowed = false;
  }

  const eligibility = {
    version: ACTIVE_MARKET_ELIGIBILITY_VERSION,
    status,
    requestedCity,
    modelKey: rowModelKey(row),
    activeMarketConfidenceBand,
    diagnosticUseAllowed,
    canUseForDiagnosticShortlist: diagnosticUseAllowed,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    currentMarketValidationStatus: 'external_current_market_validation_required_for_final',
    finalBlockedReasons: [
      'external_current_market_validation_required',
      'final_recommendation_policy_not_ready',
      'final_composer_not_ready',
      'recommendation_activation_disabled',
    ],
    requirementsForFinal,
    evidence: {
      requestedCityPriceRows,
      requestedCityVariantCount,
      scoreProfileRows,
      featureSummaryRows,
      summaryRows,
      requestedCitySummaryRows,
      stalenessDays,
      sourceCollections: unique(row.decisionCandidate?.sourceCollections || []),
      marketConfidenceBand: confidenceBand,
    },
    strengths: unique(strengths),
    risks: unique(risks),
    guardrail:
      'Active-market eligibility is a diagnostic guard only. Final current-market availability requires external/current-source validation.',
  };

  return eligibility;
}

function buildCandidateActiveMarketEligibility({ rows = [], buyerContext = {}, bridge = {}, response = {} } = {}) {
  const inputRows = asArray(rows);

  const rowsWithEligibility = inputRows.map((row) => {
    const eligibility = evaluateRowActiveMarketEligibility({ row, buyerContext, bridge, response });

    return {
      ...row,
      candidateActiveMarketEligibility: eligibility,
      decisionCandidate: {
        ...asObject(row.decisionCandidate),
        activeMarketEligibility: eligibility,
        canUseForFinalRecommendation: false,
        finalRecommendationEnabled: false,
        diagnosticOnly: true,
      },
      evidenceSummary: {
        ...asObject(row.evidenceSummary),
        activeMarketEligibility: eligibility,
        activeMarketRisks: eligibility.risks,
        finalActiveMarketRequirements: eligibility.requirementsForFinal,
      },
    };
  });

  const bands = rowsWithEligibility.map(
    (row) => row.candidateActiveMarketEligibility?.activeMarketConfidenceBand || 'weak',
  );

  const diagnosticAllowedCount = rowsWithEligibility.filter(
    (row) => row.candidateActiveMarketEligibility?.diagnosticUseAllowed === true,
  ).length;

  return {
    version: ACTIVE_MARKET_ELIGIBILITY_VERSION,
    status: 'evaluated',
    candidateCount: rowsWithEligibility.length,
    diagnosticAllowedCount,
    finalEligibleCount: 0,
    summary: {
      strongCount: bands.filter((band) => band === 'strong').length,
      goodCount: bands.filter((band) => band === 'good').length,
      limitedCount: bands.filter((band) => band === 'limited').length,
      weakCount: bands.filter((band) => band === 'weak').length,
    },
    canUseForDiagnosticShortlist: diagnosticAllowedCount > 0,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    currentMarketValidationStatus: 'external_current_market_validation_required_for_final',
    blockedReasons: [
      'external_current_market_validation_required',
      'final_recommendation_policy_not_ready',
      'final_composer_not_ready',
      'recommendation_activation_disabled',
    ],
    rows: rowsWithEligibility,
    guardrail:
      'Active-market eligibility allows diagnostic use only. Final current-market recommendation remains disabled.',
  };
}

const summarizeCandidateActiveMarketEligibility = (contract = {}) => ({
  version: contract.version || ACTIVE_MARKET_ELIGIBILITY_VERSION,
  status: contract.status || '',
  candidateCount: Number(contract.candidateCount || 0),
  diagnosticAllowedCount: Number(contract.diagnosticAllowedCount || 0),
  finalEligibleCount: 0,
  summary: asObject(contract.summary),
  canUseForDiagnosticShortlist: contract.canUseForDiagnosticShortlist === true,
  canUseForFinalRecommendation: false,
  finalRecommendationEnabled: false,
  diagnosticOnly: true,
  currentMarketValidationStatus:
    contract.currentMarketValidationStatus || 'external_current_market_validation_required_for_final',
  blockedReasons: unique(contract.blockedReasons || [
    'external_current_market_validation_required',
    'final_recommendation_policy_not_ready',
    'final_composer_not_ready',
    'recommendation_activation_disabled',
  ]),
  guardrail:
    contract.guardrail ||
    'Active-market eligibility allows diagnostic use only. Final current-market recommendation remains disabled.',
});

export {
  ACTIVE_MARKET_ELIGIBILITY_VERSION,
  buildCandidateActiveMarketEligibility,
  summarizeCandidateActiveMarketEligibility,
};

export default buildCandidateActiveMarketEligibility;
