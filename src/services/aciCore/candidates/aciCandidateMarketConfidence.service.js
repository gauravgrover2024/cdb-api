import mongoose from 'mongoose';

const MARKET_CONFIDENCE_VERSION = 'aci_candidate_market_confidence_v1';

const MODEL_SUMMARY_COLLECTION = 'aci_vehicle_model_summary';
const PRICE_ROWS_COLLECTION = 'aci_vehicle_price_rows';
const SCORE_PROFILE_COLLECTION = 'aci_vehicle_variant_score_profile';
const FEATURE_SUMMARY_COLLECTION = 'aci_vehicle_model_feature_summary_v1';

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
      row.rawModelKey ||
      row.slug ||
      row.fullModel ||
      row.displayName ||
      row.model ||
      row.name ||
      '',
  );

const cityFromContext = ({ buyerContext = {}, bridge = {}, response = {} } = {}) => {
  const candidates = [
    buyerContext.city,
    buyerContext.citySlug,
    buyerContext.location,
    buyerContext.selectedCity,
    response.filters?.city,
    response.citySlug,
    response.city,
    response.selectedVehicle?.citySlug,
    response.selectedVehicle?.city,
    bridge.citySlug,
    bridge.city,
  ];

  const found = candidates.map(textOf).find(Boolean);
  const normalized = lower(found).replace(/\s+/g, '-');

  if (['delhi', 'new-delhi', 'newdelhi', 'ncr'].includes(normalized)) return 'new-delhi';
  if (['gurgaon', 'gurugram'].includes(normalized)) return 'gurgaon';
  if (['noida'].includes(normalized)) return 'noida';

  return normalized || 'new-delhi';
};

const requestedBodyTokens = ({ buyerContext = {}, bridge = {}, response = {} } = {}) => {
  const text = lower(
    [
      JSON.stringify(buyerContext || {}),
      bridge.originalMessage,
      bridge.effectiveMessage,
      response.title,
      response.answer,
    ]
      .filter(Boolean)
      .join(' '),
  );

  const tokens = [];
  if (/\bsuv|sport utilit|compact suv|subcompact suv\b/i.test(text)) tokens.push('suv', 'sport utilities');
  if (/\bmuv|mpv|7 seater|seven seater|people mover\b/i.test(text)) tokens.push('muv', 'mpv');
  if (/\bsedan\b/i.test(text)) tokens.push('sedan', 'sedans');
  if (/\bhatch|hatchback\b/i.test(text)) tokens.push('hatchback');
  return unique(tokens);
};

const bodyMatches = (bodyType = '', requestedTokens = []) => {
  if (!requestedTokens.length) return true;
  const body = lower(bodyType).replace(/[^a-z0-9]+/g, ' ').trim();
  if (!body) return false;

  return requestedTokens.some((token) => {
    const normalized = lower(token).replace(/[^a-z0-9]+/g, ' ').trim();
    if (!normalized) return false;
    if (normalized === 'suv') return /\bsuv\b|sport utilit/.test(body);
    if (normalized === 'sedan') return /\bsedan/.test(body);
    if (normalized === 'hatchback') return /\bhatch/.test(body);
    if (normalized === 'muv' || normalized === 'mpv') return /\bmuv\b|\bmpv\b/.test(body);
    return body.includes(normalized);
  });
};

const collectionExists = async (db, name) => {
  if (!db || !name) return false;
  return db.listCollections({ name }).hasNext();
};

const getLatestDate = (docs = []) => {
  const dates = docs
    .flatMap((doc) => [doc.updatedAt, doc.createdAt, doc.builtAt])
    .map((value) => {
      const date = value ? new Date(value) : null;
      return date && !Number.isNaN(date.getTime()) ? date : null;
    })
    .filter(Boolean)
    .sort((a, b) => b.getTime() - a.getTime());

  return dates[0] || null;
};

const ageDays = (date) => {
  if (!date) return null;
  return Math.max(0, Math.round((Date.now() - date.getTime()) / 86400000));
};

const confidenceBand = (score) => {
  if (score >= 78) return 'strong';
  if (score >= 58) return 'good';
  if (score >= 38) return 'limited';
  return 'weak';
};

async function buildCandidateMarketConfidence({ rows = [], buyerContext = {}, bridge = {}, response = {}, db } = {}) {
  const inputRows = asArray(rows);
  const requestedCity = cityFromContext({ buyerContext, bridge, response });
  const bodyTokens = requestedBodyTokens({ buyerContext, bridge, response });
  const activeDb = db || mongoose.connection?.db || null;

  if (!activeDb || !inputRows.length) {
    const rowsWithNotEvaluatedConfidence = inputRows.map((row) => {
      const confidence = {
        version: MARKET_CONFIDENCE_VERSION,
        status: 'not_evaluated',
        requestedCity,
        confidenceBand: 'unknown',
        canUseForDiagnosticRanking: true,
        canUseForFinalRecommendation: false,
        finalRecommendationEnabled: false,
        diagnosticOnly: true,
        evidence: {
          modelKey: rowModelKey(row),
          reason: !activeDb ? 'db_not_available' : 'no_rows',
        },
        strengths: [],
        risks: [!activeDb ? 'market confidence database connection unavailable' : 'candidate rows unavailable'],
        guardrail: 'Market confidence is an evidence-quality guard for diagnostic ranking only. It is not a final availability promise.',
      };

      return {
        ...row,
        candidateMarketConfidence: confidence,
        decisionCandidate: {
          ...asObject(row.decisionCandidate),
          marketConfidence: confidence,
          canUseForFinalRecommendation: false,
          finalRecommendationEnabled: false,
        },
        evidenceSummary: {
          ...asObject(row.evidenceSummary),
          marketConfidence: confidence,
          marketConfidenceRisks: confidence.risks,
        },
      };
    });

    return {
      version: MARKET_CONFIDENCE_VERSION,
      status: 'not_evaluated',
      requestedCity,
      candidateCount: rowsWithNotEvaluatedConfidence.length,
      rows: rowsWithNotEvaluatedConfidence,
      summary: {
        strongCount: 0,
        goodCount: 0,
        limitedCount: 0,
        weakCount: 0,
        unknownCount: rowsWithNotEvaluatedConfidence.length,
      },
      canUseForDiagnosticShortlist: true,
      canUseForFinalRecommendation: false,
      finalRecommendationEnabled: false,
      diagnosticOnly: true,
      guardrail: 'Market confidence not evaluated because DB was unavailable; final recommendation remains disabled.',
    };
  }

  const modelKeys = unique(inputRows.map(rowModelKey).filter(Boolean));

  const [
    hasModelSummary,
    hasPriceRows,
    hasScoreProfiles,
    hasFeatureSummary,
  ] = await Promise.all([
    collectionExists(activeDb, MODEL_SUMMARY_COLLECTION),
    collectionExists(activeDb, PRICE_ROWS_COLLECTION),
    collectionExists(activeDb, SCORE_PROFILE_COLLECTION),
    collectionExists(activeDb, FEATURE_SUMMARY_COLLECTION),
  ]);

  const modelSummaryDocs = hasModelSummary
    ? await activeDb
        .collection(MODEL_SUMMARY_COLLECTION)
        .find({ modelKey: { $in: modelKeys } })
        .project({
          make: 1,
          brand: 1,
          model: 1,
          fullModel: 1,
          displayName: 1,
          modelKey: 1,
          citySlug: 1,
          bodyType: 1,
          segment: 1,
          category: 1,
          source: 1,
          createdAt: 1,
          updatedAt: 1,
        })
        .toArray()
    : [];

  const priceDocs = hasPriceRows
    ? await activeDb
        .collection(PRICE_ROWS_COLLECTION)
        .find({ modelKey: { $in: modelKeys } })
        .project({
          make: 1,
          brand: 1,
          model: 1,
          fullModel: 1,
          modelKey: 1,
          citySlug: 1,
          variant: 1,
          variantName: 1,
          bodyType: 1,
          segment: 1,
          category: 1,
          exShowroomPrice: 1,
          onRoadPrice: 1,
          source: 1,
          createdAt: 1,
          updatedAt: 1,
        })
        .toArray()
    : [];

  const scoreDocs = hasScoreProfiles
    ? await activeDb
        .collection(SCORE_PROFILE_COLLECTION)
        .find({ modelKey: { $in: modelKeys } })
        .project({ modelKey: 1, buildVersion: 1, formulaVersion: 1, builtAt: 1, createdAt: 1, updatedAt: 1 })
        .toArray()
    : [];

  const featureDocs = hasFeatureSummary
    ? await activeDb
        .collection(FEATURE_SUMMARY_COLLECTION)
        .find({ modelKey: { $in: modelKeys } })
        .project({ make: 1, brand: 1, model: 1, fullModel: 1, modelKey: 1, createdAt: 1, updatedAt: 1 })
        .toArray()
    : [];

  const groupByModelKey = (docs = []) => {
    const map = new Map();
    for (const doc of docs) {
      const key = normalizeModelKey(doc.modelKey || doc.model || doc.fullModel || doc.displayName || '');
      if (!key) continue;
      if (!map.has(key)) map.set(key, []);
      map.get(key).push(doc);
    }
    return map;
  };

  const summaryByKey = groupByModelKey(modelSummaryDocs);
  const priceByKey = groupByModelKey(priceDocs);
  const scoreByKey = groupByModelKey(scoreDocs);
  const featureByKey = groupByModelKey(featureDocs);

  const rowsWithConfidence = inputRows.map((row) => {
    const modelKey = rowModelKey(row);
    const summaries = summaryByKey.get(modelKey) || [];
    const prices = priceByKey.get(modelKey) || [];
    const scores = scoreByKey.get(modelKey) || [];
    const features = featureByKey.get(modelKey) || [];

    const requestedCitySummaryRows = summaries.filter((doc) => lower(doc.citySlug) === requestedCity);
    const requestedCityPriceRows = prices.filter((doc) => lower(doc.citySlug) === requestedCity);

    const cityCoverage = unique(prices.map((doc) => doc.citySlug));
    const summaryCityCoverage = unique(summaries.map((doc) => doc.citySlug));
    const variantsInRequestedCity = unique(
      requestedCityPriceRows.map((doc) => doc.variant || doc.variantName || ''),
    );
    const variantsAnyCity = unique(prices.map((doc) => doc.variant || doc.variantName || ''));

    const bodyEvidence = unique(
      [
        row.bodyType,
        row.segment,
        row.category,
        ...summaries.map((doc) => doc.bodyType || doc.segment || doc.category),
        ...requestedCityPriceRows.map((doc) => doc.bodyType || doc.segment || doc.category),
      ].filter(Boolean),
    );

    const latest = getLatestDate([...summaries, ...prices, ...scores, ...features]);
    const stalenessDays = ageDays(latest);

    let score = 0;
    const strengths = [];
    const risks = [];

    if (requestedCitySummaryRows.length) {
      score += 15;
      strengths.push(`model summary present in ${requestedCity}`);
    } else if (summaries.length) {
      score += 6;
      risks.push(`model summary not confirmed in ${requestedCity}`);
    } else {
      risks.push('model summary missing');
    }

    if (requestedCityPriceRows.length >= 8) {
      score += 30;
      strengths.push(`strong ${requestedCity} price-row coverage`);
    } else if (requestedCityPriceRows.length >= 3) {
      score += 20;
      strengths.push(`usable ${requestedCity} price-row coverage`);
    } else if (requestedCityPriceRows.length > 0) {
      score += 10;
      risks.push(`thin ${requestedCity} price-row coverage`);
    } else if (prices.length > 0) {
      score += 4;
      risks.push(`price rows exist, but not for ${requestedCity}`);
    } else {
      risks.push('price rows missing');
    }

    if (variantsInRequestedCity.length >= 8) {
      score += 12;
      strengths.push('healthy requested-city variant depth');
    } else if (variantsInRequestedCity.length >= 3) {
      score += 8;
      strengths.push('usable requested-city variant depth');
    } else if (variantsAnyCity.length >= 5) {
      score += 3;
      risks.push('variant depth exists only outside requested city or is thin');
    } else {
      risks.push('variant depth is thin');
    }

    if (scores.length >= 8) {
      score += 14;
      strengths.push('score profile evidence available');
    } else if (scores.length >= 3) {
      score += 9;
      strengths.push('partial score profile evidence available');
    } else if (scores.length > 0) {
      score += 4;
      risks.push('thin score profile evidence');
    } else {
      risks.push('score profile evidence missing');
    }

    if (features.length > 0 || row.featureSignals?.status === 'available') {
      score += 14;
      strengths.push('feature summary evidence available');
    } else {
      risks.push('feature summary evidence missing');
    }

    if (bodyTokens.length) {
      const match = bodyEvidence.some((body) => bodyMatches(body, bodyTokens));
      if (match) {
        score += 10;
        strengths.push('body style matches request');
      } else {
        score -= 14;
        risks.push('body style does not match requested scope');
      }
    }

    if (stalenessDays !== null) {
      if (stalenessDays <= 45) {
        score += 5;
        strengths.push('recent evidence build/update');
      } else if (stalenessDays <= 120) {
        score += 2;
        risks.push('evidence is not fresh');
      } else {
        score -= 5;
        risks.push('evidence appears stale');
      }
    } else {
      risks.push('evidence freshness unavailable');
    }

    const band = confidenceBand(score);
    const canUseForDiagnosticRanking = ['strong', 'good', 'limited'].includes(band);

    const confidence = {
      version: MARKET_CONFIDENCE_VERSION,
      status: 'evaluated',
      requestedCity,
      confidenceBand: band,
      canUseForDiagnosticRanking,
      canUseForFinalRecommendation: false,
      finalRecommendationEnabled: false,
      diagnosticOnly: true,
      evidence: {
        modelKey,
        summaryRows: summaries.length,
        requestedCitySummaryRows: requestedCitySummaryRows.length,
        priceRows: prices.length,
        requestedCityPriceRows: requestedCityPriceRows.length,
        requestedCityVariantCount: variantsInRequestedCity.length,
        anyCityVariantCount: variantsAnyCity.length,
        scoreProfileRows: scores.length,
        featureSummaryRows: features.length,
        cityCoverage,
        summaryCityCoverage,
        bodyEvidence,
        latestEvidenceAt: latest ? latest.toISOString() : '',
        stalenessDays,
      },
      strengths: unique(strengths),
      risks: unique(risks),
      guardrail: 'Market confidence is an evidence-quality guard for diagnostic ranking only. It is not a final availability promise.',
    };

    return {
      ...row,
      candidateMarketConfidence: confidence,
      decisionCandidate: {
        ...asObject(row.decisionCandidate),
        marketConfidence: confidence,
        canUseForFinalRecommendation: false,
        finalRecommendationEnabled: false,
      },
      evidenceSummary: {
        ...asObject(row.evidenceSummary),
        marketConfidence: confidence,
        marketConfidenceRisks: confidence.risks,
      },
    };
  });

  const bands = rowsWithConfidence.map((row) => row.candidateMarketConfidence?.confidenceBand || 'weak');
  const summary = {
    strongCount: bands.filter((band) => band === 'strong').length,
    goodCount: bands.filter((band) => band === 'good').length,
    limitedCount: bands.filter((band) => band === 'limited').length,
    weakCount: bands.filter((band) => band === 'weak').length,
  };

  return {
    version: MARKET_CONFIDENCE_VERSION,
    status: 'evaluated',
    requestedCity,
    candidateCount: rowsWithConfidence.length,
    summary,
    rows: rowsWithConfidence,
    canUseForDiagnosticShortlist: true,
    canUseForFinalRecommendation: false,
    finalRecommendationEnabled: false,
    diagnosticOnly: true,
    guardrail: 'Market confidence is diagnostic evidence quality only. Final recommendation remains disabled.',
  };
}

const summarizeCandidateMarketConfidence = (contract = {}) => ({
  version: contract.version || MARKET_CONFIDENCE_VERSION,
  status: contract.status || '',
  requestedCity: contract.requestedCity || '',
  candidateCount: Number(contract.candidateCount || 0),
  summary: asObject(contract.summary),
  canUseForDiagnosticShortlist: contract.canUseForDiagnosticShortlist === true,
  canUseForFinalRecommendation: false,
  finalRecommendationEnabled: false,
  diagnosticOnly: true,
  guardrail: contract.guardrail || 'Market confidence is diagnostic evidence quality only. Final recommendation remains disabled.',
});

export {
  MARKET_CONFIDENCE_VERSION,
  buildCandidateMarketConfidence,
  summarizeCandidateMarketConfidence,
};

export default buildCandidateMarketConfidence;
