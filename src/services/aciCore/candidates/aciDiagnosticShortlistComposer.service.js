const DIAGNOSTIC_SHORTLIST_COMPOSER_VERSION = 'aci_diagnostic_shortlist_composer_v1';

const asArray = (value) => (Array.isArray(value) ? value.filter(Boolean) : []);
const asObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) ? value : {};
const textOf = (value = '') => String(value ?? '').replace(/\s+/g, ' ').trim();

const unique = (items = []) => {
  const seen = new Set();
  const out = [];
  for (const item of asArray(items).map(textOf).filter(Boolean)) {
    const key = item.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(item);
  }
  return out;
};

const moneyLabel = (value) => {
  const number = Number(value || 0);
  if (!Number.isFinite(number) || number <= 0) return '';
  if (number >= 100000) {
    const lakh = number / 100000;
    return `₹${Number.isInteger(lakh) ? lakh : lakh.toFixed(1)}L`;
  }
  return `₹${number.toLocaleString('en-IN')}`;
};

const modelLabel = (row = {}) =>
  textOf(
    row.fullModel ||
      row.displayName ||
      [row.make || row.brand, row.model].filter(Boolean).join(' ') ||
      row.model ||
      row.name ||
      '',
  );

const cleanSignal = (value = '') =>
  textOf(value)
    .replace(/\bmarket confidence\s+(strong|good|limited|weak)\b/gi, 'good evidence coverage')
    .replace(/\bactive-market diagnostic evidence\s+(strong|good|limited|weak)\b/gi, 'current data coverage checked')
    .replace(/\bfeature richness\b/gi, 'features')
    .replace(/\bcity-use suitability\b/gi, 'city-use fit')
    .replace(/\bfamily practicality\b/gi, 'family practicality')
    .replace(/\bsafety evidence\b/gi, 'safety evidence');

const topSignalsForRow = (row = {}) =>
  unique([
    ...asArray(row.diagnosticRanking?.positiveSignals).map(cleanSignal),
    ...asArray(row.evidenceSummary?.fitSignals).map(cleanSignal),
  ]).slice(0, 3);

const getRows = ({ rows, response = {} } = {}) => {
  if (Array.isArray(rows)) return rows.filter(Boolean);

  if (Array.isArray(response?.rows)) return response.rows.filter(Boolean);
  if (Array.isArray(response?.data?.rows)) return response.data.rows.filter(Boolean);

  return [];
};

const getNextBestQuestion = ({ finalEligibility = {}, buyerInputClarification = {}, response = {} } = {}) => {
  const candidates = [
    buyerInputClarification?.buyerFacingQuestions?.[0],
    buyerInputClarification?.nextBestQuestion,
    finalEligibility?.buyerInputClarification?.buyerFacingQuestions?.[0],
    finalEligibility?.buyerInputClarification?.nextBestQuestion,
    response?.buyerInputClarification?.buyerFacingQuestions?.[0],
    response?.buyerInputClarification?.nextBestQuestion,
    response?.meta?.buyerInputClarification?.buyerFacingQuestions?.[0],
    response?.meta?.buyerInputClarification?.nextBestQuestion,
    response?.data?.buyerInputClarification?.buyerFacingQuestions?.[0],
    response?.data?.buyerInputClarification?.nextBestQuestion,
  ];

  return candidates.find((item) => item && typeof item === 'object' && textOf(item.question)) || null;
};

const buyerContextSummary = ({ buyerContext = {}, finalEligibility = {}, response = {} } = {}) => {
  const normalized =
    finalEligibility?.buyerDecisionInput?.normalizedBuyerInputs ||
    response?.finalRecommendationEligibility?.buyerDecisionInput?.normalizedBuyerInputs ||
    response?.meta?.finalRecommendationEligibility?.buyerDecisionInput?.normalizedBuyerInputs ||
    {};

  const city = textOf(buyerContext.city || buyerContext.citySlug || normalized.city || response?.filters?.city || 'New Delhi');
  const budget = moneyLabel(
    buyerContext.budgetOrPriceCeiling ||
      buyerContext.budget ||
      normalized.budgetOrPriceCeiling ||
      response?.filters?.budgetMax ||
      response?.filters?.maxPrice,
  );
  const transmission = textOf(buyerContext.transmissionPreference || normalized.transmissionPreference);
  const useCase = textOf(
    buyerContext.bodyPreferenceOrPrimaryUseCase ||
      buyerContext.useCase ||
      normalized.bodyPreferenceOrPrimaryUseCase,
  );
  const family = textOf(buyerContext.familySizeOrOccupancyUse || normalized.familySizeOrOccupancyUse);

  return [city, budget, transmission, useCase, family].filter(Boolean).join(' · ');
};

const unsafeBuyerLanguage = /\b(final recommendation|you should buy|buy this car|buy it|must buy|clear winner|best final choice|final verdict|my verdict|winner)\b/i;
const internalLeakLanguage = /\b(candidateMarketConfidence|candidateActiveMarketEligibility|candidateEvidenceReadiness|candidateDiagnosticRanking|finalRecommendationEnabled|canUseForFinalRecommendation|allowedAnswerType|blockedReasons|diagnosticOnly|currentMarketValidationStatus|db_evidence_|external_current_market_validation_required_for_final)\b/i;

const sanitizeAnswer = (value = '') =>
  textOf(value)
    .replace(internalLeakLanguage, '')
    .replace(/\s+/g, ' ')
    .trim();

function buildDiagnosticShortlistComposer({
  response = {},
  rows,
  buyerContext = {},
  finalEligibility = null,
  buyerInputClarification = null,
  candidateDiagnosticRanking = null,
  candidateEvidenceReadiness = null,
  candidateActiveMarketEligibility = null,
} = {}) {
  const inputRows = getRows({ rows, response });
  const topRows = inputRows.slice(0, 3);
  const nextQuestion = getNextBestQuestion({
    finalEligibility: asObject(finalEligibility),
    buyerInputClarification: asObject(buyerInputClarification),
    response,
  });

  const requestedFinalRecommendation =
    finalEligibility?.requestedFinalRecommendation === true ||
    candidateEvidenceReadiness?.requestedFinalRecommendation === true ||
    /\b(which car should i buy|what should i buy|final|verdict|choose one|pick one)\b/i.test(
      textOf(response?.originalMessage || response?.message || ''),
    );

  const contextLine = buyerContextSummary({ buyerContext, finalEligibility: asObject(finalEligibility), response });

  const topNames = topRows.map(modelLabel).filter(Boolean);
  const topline = topNames.length
    ? `Based on your current inputs, the strongest diagnostic shortlist starts with ${topNames.join(', ')}.`
    : sanitizeAnswer(response.answer || response.data?.answer || 'I found a few matching options to shortlist.');

  const topReasons = topRows
    .map((row, index) => {
      const label = modelLabel(row);
      if (!label) return '';
      const signals = topSignalsForRow(row);
      const reason = signals.length ? ` — ${signals.join(', ')}` : '';
      return `${index + 1}. ${label}${reason}`;
    })
    .filter(Boolean);

  const limitations = [];
  const activeMarket = asObject(candidateActiveMarketEligibility);
  const readiness = asObject(candidateEvidenceReadiness);

  if (activeMarket.currentMarketValidationStatus === 'external_current_market_validation_required_for_final') {
    limitations.push('current availability / latest market status still needs validation before a final purchase call');
  }

  const missingInputs = unique([
    ...asArray(finalEligibility?.missingMandatoryInputs),
    ...asArray(readiness?.missingBuyerInputsForFinalRecommendation),
    ...asArray(finalEligibility?.buyerInputClarification?.missingInputs),
    ...asArray(buyerInputClarification?.missingInputs),
  ]);

  if (missingInputs.length) {
    limitations.push('a few buyer inputs are still missing');
  }

  const questionLine = nextQuestion?.question
    ? `Next best question: ${nextQuestion.question}`
    : '';

  const title = requestedFinalRecommendation
    ? 'Diagnostic shortlist'
    : response.title && !unsafeBuyerLanguage.test(response.title)
      ? response.title
      : 'Diagnostic shortlist';

  const answerParts = [
    requestedFinalRecommendation
      ? 'I can give a diagnostic shortlist from the current data, but not a final buy verdict yet.'
      : 'Here is a diagnostic shortlist from the current data.',
    contextLine ? `Scope: ${contextLine}.` : '',
    topline,
    topReasons.length ? `Why these are appearing first: ${topReasons.join(' | ')}` : '',
    limitations.length ? `Before finalising, ${limitations.join('; ')}.` : '',
    questionLine,
  ].filter(Boolean);

  let answer = answerParts.join(' ');

  if (unsafeBuyerLanguage.test(answer)) {
    answer = answer.replace(unsafeBuyerLanguage, 'diagnostic shortlist');
  }

  answer = sanitizeAnswer(answer);

  return {
    version: DIAGNOSTIC_SHORTLIST_COMPOSER_VERSION,
    status: 'composed',
    title,
    answer,
    requestedFinalRecommendation,
    buyerFacingQuestion: nextQuestion,
    topModels: topNames,
    topReasons,
    limitations,
    canUseForDiagnosticShortlist: true,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    renderingContract: {
      maxBuyerFacingQuestions: 1,
      renderOnly: ['answer', 'rows', 'buyerFacingQuestion'],
      doNotRenderToBuyer: [
        'candidateMarketConfidence',
        'candidateActiveMarketEligibility',
        'candidateDiagnosticRanking',
        'candidateEvidenceReadiness',
        'blockedReasons',
        'allowedAnswerType',
      ],
    },
    safety: {
      hasUnsafeFinalLanguage: unsafeBuyerLanguage.test(answer),
      hasInternalLeakLanguage: internalLeakLanguage.test(answer),
    },
    guardrail: 'Buyer-facing diagnostic shortlist only. It must not claim a final purchase recommendation.',
  };
}

export {
  DIAGNOSTIC_SHORTLIST_COMPOSER_VERSION,
  buildDiagnosticShortlistComposer,
};

export default buildDiagnosticShortlistComposer;
