const RANKING_VERSION = 'aci_candidate_diagnostic_ranking_v1';

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

const bandValue = (band = '') => {
  switch (lower(band)) {
    case 'strong':
      return 4;
    case 'good':
      return 3;
    case 'moderate':
      return 2;
    case 'limited':
      return 0.5;
    default:
      return 0;
  }
};

const rankBand = (score = 0) => {
  if (score >= 22) return 'strong';
  if (score >= 16) return 'good';
  if (score >= 9) return 'moderate';
  return 'limited';
};

const hasText = (value = '', pattern) => pattern.test(lower(value));
const joinText = (...parts) =>
  parts
    .flat(Infinity)
    .map((item) => {
      if (item && typeof item === 'object') return JSON.stringify(item);
      return textOf(item);
    })
    .filter(Boolean)
    .join(' ');

const getSignal = (row = {}, key = '') => {
  const signals = [
    ...asArray(row.scoreSignals?.prioritySignals),
    ...asArray(row.scoreSignals?.signals),
  ];

  return signals.find((signal) => lower(signal.key) === lower(key)) || null;
};

const getSignalBand = (row = {}, key = '') => getSignal(row, key)?.band || '';

const getSignalValue = (row = {}, key = '') => bandValue(getSignalBand(row, key));

const marketConfidenceValue = (band = '') => {
  switch (lower(band)) {
    case 'strong':
      return 8;
    case 'good':
      return 4;
    case 'limited':
      return -6;
    case 'weak':
      return -18;
    default:
      return -8;
  }
};

const getMarketConfidence = (row = {}) =>
  asObject(row.candidateMarketConfidence || row.decisionCandidate?.marketConfidence);

const activeMarketEligibilityValue = (eligibility = {}) => {
  const band = lower(eligibility.activeMarketConfidenceBand || '');
  if (eligibility.diagnosticUseAllowed === false) return -22;

  switch (band) {
    case 'strong':
      return 3;
    case 'good':
      return 1;
    case 'limited':
      return -8;
    case 'weak':
      return -20;
    default:
      return -4;
  }
};

const getActiveMarketEligibility = (row = {}) =>
  asObject(row.candidateActiveMarketEligibility || row.decisionCandidate?.activeMarketEligibility);


const highlightsText = (row = {}) =>
  joinText(
    row.featureSignals?.highlights,
    row.featureSignals?.safetyHighlights,
    row.featureSignals?.adasHighlights,
    row.featureSignals?.comfortHighlights,
    row.featureSignals?.infotainmentHighlights,
    row.featureSignals?.premiumHighlights,
    row.featureSignals?.summary,
  );

const fitText = (row = {}) =>
  joinText(
    row.candidateRankReason,
    row.evidenceSummary?.fitSignals,
    row.evidenceSummary?.watchouts,
    row.scoreSignals?.summary,
    row.scoreSignals?.watchouts,
    row.featureSignals?.summary,
    row.bodyType,
    row.segment,
    row.category,
  );

const hasUsableEvidence = (row = {}) =>
  row.candidateEvidenceReadiness?.canUseForDiagnosticShortlist === true ||
  row.decisionCandidate?.canUseForDiagnosticShortlist === true ||
  ['partial', 'complete', 'limited'].includes(lower(row.decisionCandidate?.evidenceStatus));

const getBuyerPriorityFrame = ({ buyerContext = {}, bridge = {}, response = {} } = {}) => {
  const text = lower(
    joinText(
      buyerContext,
      buyerContext?.inferredBuyerContext,
      bridge.originalMessage,
      bridge.effectiveMessage,
      response.title,
      response.answer,
      response.query,
    ),
  );

  const requested = {
    budget: hasText(text, /\b(budget|under|below|less than|lakh|lac|price|on road|on-road)\b/i),
    automatic: hasText(text, /\b(automatic|auto|amt|cvt|dct|at)\b/i),
    family: hasText(text, /\b(family|kids?|parents?|occupants?|seater|family of|child|children)\b/i),
    city: hasText(text, /\b(city|traffic|daily|urban|office|mostly in city|mainly in city)\b/i),
    safety: hasText(text, /\b(safe|safest|safety|airbags?|ncap|crash|adas|child safety)\b/i),
    features: hasText(text, /\b(features?|sunroof|adas|airbags?|camera|ventilated|wireless|cruise|infotainment|must have)\b/i),
    runningCost: hasText(text, /\b(mileage|running cost|fuel efficiency|monthly running|cng|diesel|petrol|hybrid|ev|electric)\b/i),
    suv: hasText(text, /\b(suv|compact suv|subcompact suv|sport utilities|body style|body-type)\b/i),
  };

  const weight = {
    budget: requested.budget ? 2.5 : 1,
    automatic: requested.automatic ? 3 : 0.5,
    family: requested.family ? 3.5 : 1,
    city: requested.city ? 3 : 1,
    safety: requested.safety ? 4 : 1.5,
    features: requested.features ? 3 : 1.5,
    runningCost: requested.runningCost ? 3 : 1,
    suv: requested.suv ? 2.5 : 0.75,
  };

  return { text, requested, weight };
};

const addContribution = (contributions, key, label, points, evidence = '') => {
  if (!points) return;
  contributions.push({
    key,
    label,
    points,
    evidence: textOf(evidence),
  });
};

const hasPositiveFit = (row = {}, pattern) => pattern.test(lower(fitText(row)));

const countHighlights = (row = {}, key = '') => {
  const feature = row.featureSignals || {};
  if (key === 'safety') return asArray(feature.safetyHighlights).length;
  if (key === 'adas') return asArray(feature.adasHighlights).length;
  if (key === 'comfort') return asArray(feature.comfortHighlights).length;
  if (key === 'tech') return asArray(feature.infotainmentHighlights).length;
  return asArray(feature.highlights).length;
};

const scoreCandidate = ({ row = {}, index = 0, priorityFrame = {} } = {}) => {
  const { requested = {}, weight = {} } = priorityFrame;
  const contributions = [];
  const tradeoffs = [];
  let score = 0;

  const marketConfidence = getMarketConfidence(row);
  const marketBand = marketConfidence.confidenceBand || '';
  const marketPoints = marketConfidenceValue(marketBand);

  score += marketPoints;
  if (marketPoints > 0) {
    addContribution(contributions, 'marketConfidence', `market confidence ${marketBand}`, marketPoints, marketConfidence.strengths?.join('; '));
  } else if (marketPoints < 0) {
    tradeoffs.push(...asArray(marketConfidence.risks));
  }

  const activeMarketEligibility = getActiveMarketEligibility(row);
  const activeMarketPoints = activeMarketEligibilityValue(activeMarketEligibility);
  score += activeMarketPoints;

  if (activeMarketPoints > 0) {
    addContribution(
      contributions,
      'activeMarketEligibility',
      `active-market diagnostic evidence ${activeMarketEligibility.activeMarketConfidenceBand}`,
      activeMarketPoints,
      activeMarketEligibility.status,
    );
  } else if (activeMarketPoints < 0) {
    tradeoffs.push(...asArray(activeMarketEligibility.risks));
  }

  if (hasUsableEvidence(row)) {
    score += 2;
    addContribution(contributions, 'evidence', 'usable diagnostic evidence', 2, row.decisionCandidate?.evidenceStatus);
  }

  if (hasPositiveFit(row, /\bfits the stated budget scope\b/i)) {
    const points = weight.budget;
    score += points;
    addContribution(contributions, 'budget', 'budget scope match', points, 'Fits stated budget scope');
  }

  if (requested.automatic && hasPositiveFit(row, /\bautomatic availability matches\b/i)) {
    const points = weight.automatic;
    score += points;
    addContribution(contributions, 'automatic', 'automatic preference match', points, 'Automatic availability matches preference');
  }

  if (requested.suv && hasPositiveFit(row, /\bsuv\/body-style scope match\b|sport utilit|suv/i)) {
    const points = weight.suv;
    score += points;
    addContribution(contributions, 'bodyStyle', 'SUV/body-style match', points, 'SUV/body-style scope match');
  }

  const safetyValue = getSignalValue(row, 'safety');
  const featureValue = getSignalValue(row, 'features');
  const practicalityValue = getSignalValue(row, 'practicality');
  const cityValue = getSignalValue(row, 'cityUse');
  const runningCostValue = getSignalValue(row, 'runningCost');
  const comfortValue = getSignalValue(row, 'premiumComfort');

  if (requested.safety || safetyValue >= 2) {
    const safetyHighlightBonus = Math.min(2, countHighlights(row, 'safety') * 0.35);
    const adasBonus = requested.safety ? Math.min(1.5, countHighlights(row, 'adas') * 0.3) : 0;
    const points = safetyValue * weight.safety + safetyHighlightBonus + adasBonus;
    score += points;
    addContribution(contributions, 'safety', `safety evidence ${getSignalBand(row, 'safety') || 'available'}`, points, highlightsText(row));
  }

  if (requested.features || featureValue >= 2) {
    const featureHighlightBonus = Math.min(2, countHighlights(row) * 0.08);
    const points = featureValue * weight.features + featureHighlightBonus;
    score += points;
    addContribution(contributions, 'features', `feature richness ${getSignalBand(row, 'features') || 'available'}`, points, row.scoreSignals?.summary);
  }

  if (requested.family || practicalityValue >= 2) {
    const points = practicalityValue * weight.family;
    score += points;
    addContribution(contributions, 'family', `family practicality ${getSignalBand(row, 'practicality') || 'available'}`, points, row.scoreSignals?.summary);
  }

  if (requested.city || cityValue >= 2) {
    const points = cityValue * weight.city;
    score += points;
    addContribution(contributions, 'city', `city-use suitability ${getSignalBand(row, 'cityUse') || 'available'}`, points, row.scoreSignals?.summary);
  }

  if (requested.runningCost || runningCostValue >= 3) {
    const points = runningCostValue * weight.runningCost;
    score += points;
    addContribution(contributions, 'runningCost', `running-cost signal ${getSignalBand(row, 'runningCost') || 'available'}`, points, row.scoreSignals?.summary);
  }

  if (comfortValue >= 3 && (requested.family || requested.features)) {
    const points = comfortValue * 0.75;
    score += points;
    addContribution(contributions, 'comfort', `comfort signal ${getSignalBand(row, 'premiumComfort') || 'available'}`, points, row.scoreSignals?.summary);
  }

  const watchoutText = lower(joinText(row.evidenceSummary?.watchouts, row.scoreSignals?.watchouts));
  const familyEvidenceGap = /\bfamily practicality evidence needs review\b/i.test(watchoutText);
  const safetyEvidenceGap = /\bsafety evidence needs review\b/i.test(watchoutText);
  const featureEvidenceGap = /\bfeature richness evidence needs review\b/i.test(watchoutText);

  if (requested.family && familyEvidenceGap) {
    score -= 4;
    tradeoffs.push('family practicality evidence needs review');
  }

  if (requested.safety && safetyEvidenceGap) {
    score -= 4;
    tradeoffs.push('safety evidence needs review');
  }

  if (requested.features && featureEvidenceGap) {
    score -= 3;
    tradeoffs.push('feature richness evidence needs review');
  }

  const missingEvidence = asArray(row.candidateEvidenceReadiness?.missingEvidence);
  if (missingEvidence.length) {
    score -= Math.min(4, missingEvidence.length * 1.25);
    tradeoffs.push(...missingEvidence.map((item) => `missing ${item}`));
  }

  // Stable deterministic tie-break only; not exposed as a score.
  score -= index * 0.001;

  const positiveSignals = unique(
    contributions
      .sort((a, b) => b.points - a.points)
      .slice(0, 5)
      .map((item) => item.label),
  );

  const matchedPriorities = unique(contributions.map((item) => item.key));

  return {
    score,
    rankBand: rankBand(score),
    positiveSignals,
    matchedPriorities,
    tradeoffs: unique(tradeoffs),
  };
};

function buildCandidateDiagnosticRanking({ rows = [], buyerContext = {}, bridge = {}, response = {} } = {}) {
  const inputRows = asArray(rows);
  if (!inputRows.length) {
    return {
      version: RANKING_VERSION,
      ok: false,
      status: 'no_candidates',
      rows: [],
      finalRecommendationEnabled: false,
      canUseForFinalRecommendation: false,
    };
  }

  const priorityFrame = getBuyerPriorityFrame({ buyerContext, bridge, response });

  const scored = inputRows.map((row, index) => {
    const ranking = scoreCandidate({ row, index, priorityFrame });
    return {
      row,
      index,
      previousRank: index + 1,
      ...ranking,
    };
  });

  scored.sort((left, right) => {
    if (right.score !== left.score) return right.score - left.score;
    return left.index - right.index;
  });

  const rowsWithRanking = scored.map((item, sortedIndex) => {
    const rank = sortedIndex + 1;
    const rankChanged = rank !== item.previousRank;

    const ranking = {
      version: RANKING_VERSION,
      status: 'diagnostic_ranking_applied',
      rank,
      previousRank: item.previousRank,
      rankChanged,
      rankBand: item.rankBand,
      matchedPriorities: item.matchedPriorities,
      positiveSignals: item.positiveSignals,
      tradeoffs: item.tradeoffs,
      canUseForDiagnosticShortlist: true,
      canUseForFinalRecommendation: false,
      finalRecommendationEnabled: false,
      diagnosticOnly: true,
      guardrail: 'Diagnostic ranking only. This is not a final purchase recommendation.',
    };

    const reasonParts = item.positiveSignals.length
      ? item.positiveSignals.slice(0, 3)
      : asArray(item.row.evidenceSummary?.fitSignals).slice(0, 3);

    const previousReason = textOf(item.row.candidateRankReason);
    const candidateRankReason = reasonParts.length
      ? `Ranked for this query because of ${reasonParts.join(', ')}.`
      : previousReason;

    return {
      ...item.row,
      candidateRankReason,
      diagnosticRanking: ranking,
      candidateDiagnosticRanking: ranking,
      decisionCandidate: {
        ...asObject(item.row.decisionCandidate),
        diagnosticRanking: ranking,
        canUseForFinalRecommendation: false,
        finalRecommendationEnabled: false,
      },
      evidenceSummary: {
        ...asObject(item.row.evidenceSummary),
        diagnosticRanking: ranking,
        rankingSignals: item.positiveSignals,
        rankingTradeoffs: item.tradeoffs,
      },
    };
  });

  const topSignals = unique(
    rowsWithRanking
      .slice(0, Math.min(3, rowsWithRanking.length))
      .flatMap((row) => asArray(row.diagnosticRanking?.positiveSignals)),
  );

  const changedCount = rowsWithRanking.filter((row) => row.diagnosticRanking?.rankChanged).length;

  return {
    version: RANKING_VERSION,
    ok: true,
    status: 'diagnostic_ranking_applied',
    candidateCount: rowsWithRanking.length,
    changedCount,
    topSignals,
    requestedPriorities: Object.entries(priorityFrame.requested || {})
      .filter(([, value]) => value === true)
      .map(([key]) => key),
    rows: rowsWithRanking,
    canUseForDiagnosticShortlist: true,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    guardrail: 'Diagnostic ranking only. Final recommendation remains disabled.',
  };
}

const summarizeCandidateDiagnosticRanking = (ranking = {}) => ({
  version: ranking.version || RANKING_VERSION,
  status: ranking.status || '',
  candidateCount: Number(ranking.candidateCount || 0),
  changedCount: Number(ranking.changedCount || 0),
  topSignals: asArray(ranking.topSignals),
  requestedPriorities: asArray(ranking.requestedPriorities),
  canUseForDiagnosticShortlist: ranking.canUseForDiagnosticShortlist === true,
  canUseForFinalRecommendation: false,
  finalRecommendationEnabled: false,
  diagnosticOnly: true,
  guardrail: ranking.guardrail || 'Diagnostic ranking only. Final recommendation remains disabled.',
});

export {
  RANKING_VERSION,
  buildCandidateDiagnosticRanking,
  summarizeCandidateDiagnosticRanking,
};

export default buildCandidateDiagnosticRanking;
