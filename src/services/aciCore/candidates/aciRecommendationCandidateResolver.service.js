import mongoose from 'mongoose';

const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const MODEL_FEATURE_SUMMARY_COLLECTION =
  process.env.ACI_MODEL_FEATURE_SUMMARY_COLLECTION || 'aci_vehicle_model_feature_summary_v1';

const RESOLVER_VERSION = 'aci_recommendation_candidate_resolver_v1';

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

const normalizeKey = (value = '') =>
  lower(value)
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const hyphenKey = (value = '') =>
  lower(value)
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');

const compactKey = (value = '') =>
  lower(value).replace(/[^a-z0-9]+/g, '');

const buildModelKeyAliases = (value = '') => {
  const raw = lower(value);
  const aliases = [
    raw,
    normalizeKey(raw),
    hyphenKey(raw),
    compactKey(raw),
    raw.replace(/_/g, '-'),
    raw.replace(/-/g, '_'),
  ].filter(Boolean);

  return [...new Set(aliases)];
};

const getPath = (obj = {}, path = '') => {
  let current = obj;
  for (const part of String(path || '').split('.')) {
    if (!part) continue;
    current = current?.[part];
    if (current === undefined || current === null) return null;
  }
  return current;
};

const hasNumber = (value) => Number.isFinite(Number(value));

const average = (values = []) => {
  const nums = values.map(Number).filter(Number.isFinite);
  if (!nums.length) return null;
  return nums.reduce((sum, value) => sum + value, 0) / nums.length;
};

const bandForScore = (score, { lowerIsBetter = false } = {}) => {
  if (!hasNumber(score)) return 'unknown';
  const n = Number(score);

  if (lowerIsBetter) {
    if (n <= 20) return 'strong';
    if (n <= 35) return 'good';
    if (n <= 50) return 'moderate';
    return 'watchout';
  }

  if (n >= 75) return 'strong';
  if (n >= 65) return 'good';
  if (n >= 50) return 'moderate';
  return 'limited';
};

const getItemLabel = (item) => {
  if (!item) return '';
  if (typeof item === 'string') return textOf(item);
  if (typeof item === 'object') {
    return textOf(
      item.displayName ||
        item.featureName ||
        item.label ||
        item.name ||
        item.title ||
        item.key ||
        item.canonicalKey
    );
  }
  return '';
};

const takeLabels = (items = [], limit = 4) =>
  unique(asArray(items).map(getItemLabel)).slice(0, limit);

const containsAny = (value = '', terms = []) => {
  const text = lower(value);
  return terms.some((term) => text.includes(lower(term)));
};

const buyerWants = (buyerContext = {}) => {
  const useCase = lower(
    [
      buyerContext.bodyPreferenceOrPrimaryUseCase,
      buyerContext.primaryUseCase,
      buyerContext.shortlistedModelsOrDiscoveryScope,
    ].filter(Boolean).join(' ')
  );

  const safety = lower(buyerContext.safetyPriority);
  const transmission = lower(buyerContext.transmissionPreference);
  const fuel = lower(
    [
      buyerContext.fuelPreferenceOrMonthlyRunning,
      buyerContext.fuelPreference,
      buyerContext.monthlyRunning,
    ].filter(Boolean).join(' ')
  );

  return {
    family: containsAny(useCase, ['family', 'occupancy', 'rear seat']),
    city: containsAny(useCase, ['city', 'traffic', 'daily']),
    highway: containsAny(useCase, ['highway', 'long']),
    safety: Boolean(safety) || containsAny(useCase, ['safety', 'safest']),
    automatic: containsAny(transmission, ['automatic', 'auto', 'amt', 'cvt', 'dct', 'ivt', 'at']),
    cng: containsAny(fuel, ['cng']),
    petrol: containsAny(fuel, ['petrol']),
    diesel: containsAny(fuel, ['diesel']),
    electric: containsAny(fuel, ['electric', 'ev']),
    highRunning: containsAny(fuel, ['high running', 'monthly', 'daily', 'km']),
  };
};

const rowModelKey = (row = {}) => textOf(row.modelKey || row.model_key || row.model || row.fullModel || row.displayName);
const rowMakeKey = (row = {}) => textOf(row.makeKey || row.make_key || row.make || row.brand);
const rowLabel = (row = {}) => textOf(row.fullModel || row.displayName || [row.make || row.brand, row.model].filter(Boolean).join(' '));

const indexDocsByModelAlias = (docs = []) => {
  const map = new Map();

  for (const doc of docs) {
    for (const alias of buildModelKeyAliases(doc.modelKey || doc.model || doc.fullModel)) {
      if (!alias || map.has(alias)) continue;
      map.set(alias, doc);
    }
  }

  return map;
};

const findDocForRow = (map = new Map(), row = {}) => {
  for (const alias of buildModelKeyAliases(rowModelKey(row))) {
    const doc = map.get(alias);
    if (doc) return doc;
  }
  return null;
};

const groupScoreProfilesByModel = (profiles = []) => {
  const grouped = new Map();

  for (const profile of profiles) {
    for (const alias of buildModelKeyAliases(profile.modelKey)) {
      if (!grouped.has(alias)) grouped.set(alias, []);
      grouped.get(alias).push(profile);
    }
  }

  return grouped;
};

const findProfilesForRow = (grouped = new Map(), row = {}) => {
  for (const alias of buildModelKeyAliases(rowModelKey(row))) {
    const profiles = grouped.get(alias);
    if (profiles?.length) return profiles;
  }
  return [];
};

const buildScoreSignals = ({ profiles = [], buyerContext = {} } = {}) => {
  if (!profiles.length) {
    return {
      status: 'missing',
      summary: 'Score-profile evidence is not available for this candidate yet.',
      signals: [],
      watchouts: ['Score profile evidence is missing for this candidate.'],
      source: SCORE_PROFILE_COLLECTION,
    };
  }

  const wants = buyerWants(buyerContext);

  const modules = [
    {
      key: 'safety',
      label: 'safety',
      score: average(profiles.map((profile) => getPath(profile, 'safetyScore.score'))),
    },
    {
      key: 'features',
      label: 'feature richness',
      score: average(profiles.map((profile) => getPath(profile, 'featureScore.score'))),
    },
    {
      key: 'practicality',
      label: 'family practicality',
      score: average(profiles.map((profile) => getPath(profile, 'practicalityScore.score'))),
    },
    {
      key: 'cityUse',
      label: 'city-use suitability',
      score: average(profiles.map((profile) => getPath(profile, 'cityUseScore.score'))),
    },
    {
      key: 'highwayUse',
      label: 'highway-use suitability',
      score: average(profiles.map((profile) => getPath(profile, 'highwayUseScore.score'))),
    },
    {
      key: 'runningCost',
      label: 'mileage/running-cost signal',
      score: average(profiles.map((profile) => getPath(profile, 'mileageRunningCostScore.score'))),
    },
    {
      key: 'premiumComfort',
      label: 'comfort feature signal',
      score: average(profiles.map((profile) => getPath(profile, 'premiumComfortScore.score'))),
    },
    {
      key: 'regretRisk',
      label: 'trade-off risk',
      score: average(profiles.map((profile) => getPath(profile, 'regretRisk.riskScore'))),
      lowerIsBetter: true,
    },
  ].map((module) => ({
    key: module.key,
    label: module.label,
    band: bandForScore(module.score, { lowerIsBetter: module.lowerIsBetter }),
    status: hasNumber(module.score) ? 'available' : 'missing',
  }));

  const priorityKeys = new Set([
    wants.safety ? 'safety' : '',
    wants.family ? 'practicality' : '',
    wants.city ? 'cityUse' : '',
    wants.highway ? 'highwayUse' : '',
    wants.highRunning || wants.cng || wants.diesel || wants.electric ? 'runningCost' : '',
    'features',
    'regretRisk',
  ].filter(Boolean));

  const prioritySignals = modules
    .filter((module) => priorityKeys.has(module.key) && ['strong', 'good', 'moderate'].includes(module.band))
    .slice(0, 5);

  const watchouts = modules
    .filter((module) => priorityKeys.has(module.key) && ['limited', 'watchout', 'missing'].includes(module.band))
    .map((module) => `${module.label} evidence needs review`)
    .slice(0, 4);

  return {
    status: 'available',
    profileCount: profiles.length,
    signals: modules,
    prioritySignals,
    summary: prioritySignals.length
      ? `Useful diagnostic evidence: ${prioritySignals.map((item) => item.label).join(', ')}.`
      : 'Diagnostic score evidence is available, but no strong priority signal stands out yet.',
    watchouts,
    source: SCORE_PROFILE_COLLECTION,
    guardrail: 'Diagnostic module evidence only; not a final purchase verdict.',
  };
};

const buildFeatureSignals = ({ featureSummary = null } = {}) => {
  if (!featureSummary) {
    return {
      status: 'missing',
      summary: 'Model-level feature summary is not available yet.',
      highlights: [],
      source: MODEL_FEATURE_SUMMARY_COLLECTION,
    };
  }

  const safetyHighlights = takeLabels(featureSummary.safetyHighlights, 4);
  const adasHighlights = takeLabels(featureSummary.adasHighlights, 4);
  const comfortHighlights = takeLabels(featureSummary.comfortHighlights, 4);
  const infotainmentHighlights = takeLabels(featureSummary.infotainmentHighlights, 3);
  const premiumHighlights = takeLabels(featureSummary.premiumHighlights, 3);

  const highlights = unique([
    ...safetyHighlights,
    ...adasHighlights,
    ...comfortHighlights,
    ...infotainmentHighlights,
    ...premiumHighlights,
  ]).slice(0, 8);

  const summaryParts = [];
  if (safetyHighlights.length) summaryParts.push(`safety: ${safetyHighlights.slice(0, 3).join(', ')}`);
  if (adasHighlights.length) summaryParts.push(`ADAS: ${adasHighlights.slice(0, 3).join(', ')}`);
  if (comfortHighlights.length) summaryParts.push(`comfort: ${comfortHighlights.slice(0, 3).join(', ')}`);
  if (infotainmentHighlights.length) summaryParts.push(`tech: ${infotainmentHighlights.slice(0, 2).join(', ')}`);

  return {
    status: highlights.length ? 'available' : 'partial',
    summary: summaryParts.length
      ? `Feature evidence includes ${summaryParts.join('; ')}.`
      : 'Feature evidence exists but has limited highlighted equipment.',
    highlights,
    safetyHighlights,
    adasHighlights,
    comfortHighlights,
    infotainmentHighlights,
    premiumHighlights,
    source: MODEL_FEATURE_SUMMARY_COLLECTION,
  };
};

const transmissionMatches = (row = {}, buyerContext = {}) => {
  const wants = buyerWants(buyerContext);
  if (!wants.automatic) return false;
  return asArray(row.transmissions || row.gearboxes || row.transmissionText)
    .map(lower)
    .some((item) => item.includes('automatic') || item.includes('auto'));
};

const fuelMatches = (row = {}, buyerContext = {}) => {
  const wants = buyerWants(buyerContext);
  const fuels = asArray(row.fuelTypes || row.fuels || row.fuelText).map(lower).join(' ');
  if (wants.cng) return fuels.includes('cng');
  if (wants.diesel) return fuels.includes('diesel');
  if (wants.petrol) return fuels.includes('petrol');
  if (wants.electric) return fuels.includes('electric') || fuels.includes('ev');
  return false;
};

const buildFitSignals = ({ row = {}, buyerContext = {}, scoreSignals = {}, featureSignals = {} } = {}) => {
  const wants = buyerWants(buyerContext);
  const signals = [];

  if (Number(buyerContext.budgetOrPriceCeiling || buyerContext.maxBudget || 0) > 0) {
    signals.push('Fits the stated budget scope');
  }

  if (transmissionMatches(row, buyerContext)) {
    signals.push('Automatic availability matches the preference');
  }

  if (fuelMatches(row, buyerContext)) {
    signals.push('Fuel preference has matching variants');
  }

  const bodyText = lower([row.bodyType, row.bodyTypeKey, row.bodyTypeGroup, row.segment].filter(Boolean).join(' '));
  if (wants.family) signals.push('Relevant to family-use discovery');
  if (wants.city) signals.push('Relevant to city-use discovery');
  if (bodyText.includes('suv') || bodyText.includes('sport')) signals.push('SUV/body-style scope match');

  for (const signal of asArray(scoreSignals.prioritySignals).slice(0, 3)) {
    signals.push(`${signal.label} diagnostic signal is ${signal.band}`);
  }

  if (featureSignals.safetyHighlights?.length) {
    signals.push(`Safety equipment evidence: ${featureSignals.safetyHighlights.slice(0, 2).join(', ')}`);
  }

  if (featureSignals.adasHighlights?.length) {
    signals.push(`ADAS evidence: ${featureSignals.adasHighlights.slice(0, 2).join(', ')}`);
  }

  return unique(signals).slice(0, 8);
};

const buildCandidateWatchouts = ({ buyerContext = {}, scoreSignals = {}, featureSignals = {} } = {}) => {
  const watchouts = [
    ...asArray(scoreSignals.watchouts),
  ];

  if (!featureSignals || featureSignals.status === 'missing') {
    watchouts.push('Feature-summary evidence is missing for this candidate');
  }

  if (!buyerContext.fuelPreferenceOrMonthlyRunning && !buyerContext.fuelPreference && !buyerContext.monthlyRunning) {
    watchouts.push('Fuel/running preference is still needed before a final verdict');
  }

  if (!buyerContext.safetyPriority) {
    watchouts.push('Safety priority is still needed before final ranking');
  }

  if (!buyerContext.featurePriority || !asArray(buyerContext.featurePriority).length) {
    watchouts.push('Must-have feature priorities are still needed before final ranking');
  }

  return unique(watchouts).slice(0, 6);
};

const buildEvidenceSummary = ({ row = {}, buyerContext = {}, scoreSignals = {}, featureSignals = {} } = {}) => {
  const fitSignals = buildFitSignals({ row, buyerContext, scoreSignals, featureSignals });
  const watchouts = buildCandidateWatchouts({ buyerContext, scoreSignals, featureSignals });

  const candidateRankReason = fitSignals.length
    ? fitSignals.slice(0, 3).join('; ')
    : 'Candidate is included because it matches the broad discovery filters.';

  return {
    version: 'aci_candidate_evidence_summary_v1',
    fitSignals,
    watchouts,
    candidateRankReason,
    diagnosticOnly: true,
    finalRecommendationEnabled: false,
    guardrail: 'Use this as candidate evidence only, not as a final purchase recommendation.',
  };
};

const getDb = () => {
  const db = mongoose.connection?.db;
  if (!db) throw new Error('MongoDB connection is required for recommendation candidate resolver.');
  return db;
};

async function buildRecommendationCandidateResolver({
  rows = [],
  buyerContext = {},
  bridge = {},
  limit = 12,
} = {}) {
  const cleanRows = asArray(rows).slice(0, Math.max(1, Math.min(Number(limit) || 12, 25)));
  if (!cleanRows.length) {
    return {
      version: RESOLVER_VERSION,
      ok: true,
      rows: [],
      sourceCollections: [],
      evidenceStatus: 'missing',
      enrichedCount: 0,
    };
  }

  const db = getDb();
  const modelAliases = [...new Set(cleanRows.flatMap((row) => buildModelKeyAliases(rowModelKey(row))).filter(Boolean))];

  const [scoreProfiles, featureSummaries] = await Promise.all([
    db.collection(SCORE_PROFILE_COLLECTION)
      .find(
        { modelKey: { $in: modelAliases } },
        {
          projection: {
            _id: 0,
            scoreProfileKey: 1,
            variantProfileKey: 1,
            variantFullName: 1,
            makeKey: 1,
            modelKey: 1,
            variantKey: 1,
            fuelKey: 1,
            transmissionKey: 1,
            referenceExShowroomPrice: 1,
            'safetyScore.score': 1,
            'featureScore.score': 1,
            'performanceScore.score': 1,
            'mileageRunningCostScore.score': 1,
            'practicalityScore.score': 1,
            'cityUseScore.score': 1,
            'highwayUseScore.score': 1,
            'premiumComfortScore.score': 1,
            'regretRisk.riskScore': 1,
            'scoreReadiness.finalOverallScoreReady': 1,
            'scoreReadiness.finalOverallScoreReason': 1,
            buildVersion: 1,
            builtAt: 1,
          },
        },
      )
      .limit(1200)
      .toArray(),
    db.collection(MODEL_FEATURE_SUMMARY_COLLECTION)
      .find(
        { modelKey: { $in: modelAliases } },
        {
          projection: {
            _id: 0,
            make: 1,
            brand: 1,
            model: 1,
            modelKey: 1,
            fullModel: 1,
            premiumHighlights: 1,
            safetyHighlights: 1,
            adasHighlights: 1,
            comfortHighlights: 1,
            infotainmentHighlights: 1,
            activeVariantCount: 1,
            totalIndexedFeatureCount: 1,
            sourceCollection: 1,
            sourceBuildIds: 1,
            builtAt: 1,
            updatedAt: 1,
          },
        },
      )
      .limit(500)
      .toArray(),
  ]);

  const scoreByModel = groupScoreProfilesByModel(scoreProfiles);
  const featureByModel = indexDocsByModelAlias(featureSummaries);
  const sourceCollections = [
    'aci_vehicle_model_summary',
    'aci_vehicle_price_rows',
    SCORE_PROFILE_COLLECTION,
    MODEL_FEATURE_SUMMARY_COLLECTION,
  ];

  const enrichedRows = cleanRows.map((row, index) => {
    const profiles = findProfilesForRow(scoreByModel, row);
    const featureSummary = findDocForRow(featureByModel, row);
    const scoreSignals = buildScoreSignals({ profiles, buyerContext });
    const featureSignals = buildFeatureSignals({ featureSummary });
    const evidenceSummary = buildEvidenceSummary({ row, buyerContext, scoreSignals, featureSignals });

    return {
      ...row,
      decisionCandidate: {
        version: RESOLVER_VERSION,
        candidateType: 'model',
        rank: index + 1,
        label: rowLabel(row),
        makeKey: rowMakeKey(row),
        modelKey: rowModelKey(row),
        evidenceStatus:
          scoreSignals.status === 'available' || featureSignals.status === 'available'
            ? 'partial'
            : 'missing',
        canUseForFinalRecommendation: false,
        finalRecommendationEnabled: false,
        diagnosticOnly: true,
        sourceCollections,
      },
      evidenceSummary,
      scoreSignals,
      featureSignals,
      candidateRankReason: evidenceSummary.candidateRankReason,
    };
  });

  const enrichedCount = enrichedRows.filter((row) =>
    row.decisionCandidate?.evidenceStatus === 'partial'
  ).length;

  return {
    version: RESOLVER_VERSION,
    ok: true,
    rows: enrichedRows,
    sourceCollections,
    evidenceStatus: enrichedCount ? 'partial' : 'missing',
    enrichedCount,
    totalCandidates: enrichedRows.length,
    scoreProfileCount: scoreProfiles.length,
    featureSummaryCount: featureSummaries.length,
    guardrail: 'Recommendation candidates are diagnostic evidence only; final recommendations remain disabled.',
    bridge: {
      tool: bridge.tool || '',
      primaryTask: bridge.primaryTask || '',
      routingReason: bridge.routingReason || '',
    },
  };
}

export {
  RESOLVER_VERSION,
  buildRecommendationCandidateResolver,
};

export default buildRecommendationCandidateResolver;
