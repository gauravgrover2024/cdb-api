const {
  getVariantScoreProfile,
  getModelScoreProfiles,
  getSameFamilyValueProfiles,
  getTopScoreProfiles,
  getScoreProfileCoverage,
} = require('./aciVariantScoreProfile.reader.cjs');

const SCORE_MODULES = [
  { key: 'safety', label: 'Safety', field: 'safetyScore', scoreField: 'score', higherIsBetter: true },
  { key: 'features', label: 'Features', field: 'featureScore', scoreField: 'score', higherIsBetter: true },
  { key: 'performance', label: 'Performance', field: 'performanceScore', scoreField: 'score', higherIsBetter: true },
  { key: 'mileageRunningCost', label: 'Mileage / running cost', field: 'mileageRunningCostScore', scoreField: 'score', higherIsBetter: true },
  { key: 'practicality', label: 'Practicality', field: 'practicalityScore', scoreField: 'score', higherIsBetter: true },
  { key: 'cityUse', label: 'City use', field: 'cityUseScore', scoreField: 'score', higherIsBetter: true },
  { key: 'highwayUse', label: 'Highway use', field: 'highwayUseScore', scoreField: 'score', higherIsBetter: true },
  { key: 'premiumComfort', label: 'Premium comfort', field: 'premiumComfortScore', scoreField: 'score', higherIsBetter: true },
  { key: 'value', label: 'Same-model value', field: 'valueScore', scoreField: 'score', higherIsBetter: true },
  { key: 'regretRisk', label: 'Regret risk', field: 'regretRisk', scoreField: 'riskScore', higherIsBetter: false },
];

const hasNumber = (value) =>
  value !== null &&
  value !== undefined &&
  value !== '' &&
  Number.isFinite(Number(value));

const round = (value, digits = 1) =>
  hasNumber(value) ? Number(Number(value).toFixed(digits)) : null;

const compactArray = (items, limit = 8) =>
  [...new Set((items || []).filter(Boolean))].slice(0, limit);

const bandForScore = (score, higherIsBetter = true) => {
  if (!hasNumber(score)) return 'unknown';

  const n = Number(score);

  if (!higherIsBetter) {
    if (n <= 20) return 'low';
    if (n <= 40) return 'moderate';
    if (n <= 65) return 'high';
    return 'very_high';
  }

  if (n >= 80) return 'strong';
  if (n >= 65) return 'good';
  if (n >= 45) return 'average';
  if (n >= 25) return 'weak';
  return 'very_weak';
};

const normalizeModuleScore = (profile, moduleDef) => {
  const row = profile?.[moduleDef.field] || {};
  const score = hasNumber(row[moduleDef.scoreField]) ? round(row[moduleDef.scoreField]) : null;

  return {
    key: moduleDef.key,
    label: moduleDef.label,
    score,
    band: bandForScore(score, moduleDef.higherIsBetter),
    status: row.status || null,
    confidence: row.confidence || null,
    scoreType: row.scoreType || row.riskType || null,
    caveats: compactArray(row.caveats, 6),
    evidence: row.evidence || null,
    reasons: compactArray(row.reasons, 6),
  };
};

const buildStrengths = (modules) => {
  const strengths = [];

  const pushIf = (condition, text) => {
    if (condition) strengths.push(text);
  };

  pushIf(modules.safety?.score >= 75, 'Strong safety signal');
  pushIf(modules.features?.score >= 75, 'Feature-rich for its scoring context');
  pushIf(modules.cityUse?.score >= 80, 'Strong city-use suitability');
  pushIf(modules.highwayUse?.score >= 75, 'Good highway-use signal');
  pushIf(modules.mileageRunningCost?.score >= 80, 'Strong mileage/running-cost signal');
  pushIf(modules.practicality?.score >= 70, 'Practical family-use signal');
  pushIf(modules.value?.score >= 75, 'Strong same-model value signal');

  return compactArray(strengths, 5);
};

const buildWatchouts = (modules, profile) => {
  const watchouts = [];

  const pushIf = (condition, text) => {
    if (condition) watchouts.push(text);
  };

  pushIf(modules.safety?.confidence === 'medium' || modules.safety?.confidence === 'low',
    'Safety/crash applicability needs verified-source caution');
  pushIf(modules.mileageRunningCost?.score === null || modules.mileageRunningCost?.status !== 'scored',
    'Mileage score is not available or not fully scored');
  pushIf(modules.value?.score !== null && modules.value.score <= 30,
    'Same-model value score is weak');
  pushIf(modules.premiumComfort?.score !== null && modules.premiumComfort.score <= 30,
    'Premium comfort score is limited');
  pushIf(modules.regretRisk?.score !== null && modules.regretRisk.score >= 40,
    'Regret-risk signal needs attention');

  for (const caveat of profile?.scoreSummary?.caveats || []) {
    if (watchouts.length >= 6) break;
    watchouts.push(caveat);
  }

  return compactArray(watchouts, 6);
};

const toScoreInsight = (profile) => {
  if (!profile) return null;

  const modules = Object.fromEntries(
    SCORE_MODULES.map((moduleDef) => [
      moduleDef.key,
      normalizeModuleScore(profile, moduleDef),
    ])
  );

  return {
    scoreProfileKey: profile.scoreProfileKey,
    variantProfileKey: profile.variantProfileKey,
    variantFullName: profile.variantFullName,
    makeKey: profile.makeKey,
    modelKey: profile.modelKey,
    variantKey: profile.variantKey,
    fuelKey: profile.fuelKey,
    transmissionKey: profile.transmissionKey,
    transmissionSubtype: profile.transmissionSubtype,
    fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,
    referenceExShowroomPrice: profile.referenceExShowroomPrice,
    priceSegment: profile.priceSegment,
    buildVersion: profile.buildVersion,
    formulaVersion: profile.formulaVersion,
    featureScoreTaxonomyVersion: profile.featureScoreTaxonomyVersion,
    modules,
    strengths: buildStrengths(modules),
    watchouts: buildWatchouts(modules, profile),
    scoreReadiness: profile.scoreReadiness,
    scoreSummary: profile.scoreSummary,
    usageGuardrail: {
      canUseForFinalRecommendation: false,
      reason:
        'These are diagnostic module scores only. Final recommendation needs buyer-context weighting, similar-cars graph, upgrade ladder, service/resale evidence and recommendation policy.',
    },
  };
};

const getVariantScoreInsight = async ({ db, scoreProfileKey, variantProfileKey }) => {
  const profile = await getVariantScoreProfile({ db, scoreProfileKey, variantProfileKey });
  return toScoreInsight(profile);
};

const getModelScoreInsights = async ({
  db,
  makeKey,
  modelKey,
  fuelKey,
  transmissionKey,
  fuelTransmissionFamilyKey,
  limit = 80,
} = {}) => {
  const profiles = await getModelScoreProfiles({
    db,
    makeKey,
    modelKey,
    fuelKey,
    transmissionKey,
    fuelTransmissionFamilyKey,
    limit,
  });

  return {
    usageGuardrail: {
      canUseForFinalRecommendation: false,
      reason:
        'Model score insights expose diagnostic module scores for variants. They are not final buyer recommendations.',
    },
    count: profiles.length,
    variants: profiles.map(toScoreInsight),
  };
};

const getSameFamilyValueInsights = async ({
  db,
  makeKey,
  modelKey,
  fuelKey,
  transmissionKey,
  fuelTransmissionFamilyKey,
  limit = 20,
} = {}) => {
  const result = await getSameFamilyValueProfiles({
    db,
    makeKey,
    modelKey,
    fuelKey,
    transmissionKey,
    fuelTransmissionFamilyKey,
    limit,
  });

  return {
    usageGuardrail: result.usageGuardrail,
    count: result.profiles.length,
    variants: result.profiles.map(toScoreInsight),
  };
};

const getTopScoreInsights = async ({ db, scorePath, direction = 'desc', limit = 20, filters = {} } = {}) => {
  const result = await getTopScoreProfiles({
    db,
    scorePath,
    direction,
    limit,
    filters,
  });

  return {
    usageGuardrail: result.usageGuardrail,
    count: result.profiles.length,
    variants: result.profiles.map(toScoreInsight),
  };
};

module.exports = {
  SCORE_MODULES,
  toScoreInsight,
  getVariantScoreInsight,
  getModelScoreInsights,
  getSameFamilyValueInsights,
  getTopScoreInsights,
  getScoreProfileCoverage,
};
