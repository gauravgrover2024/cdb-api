const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const MODULES = Object.freeze([
  { key: 'safety', label: 'Safety', path: 'safetyScore.score', higherIsBetter: true },
  { key: 'features', label: 'Features', path: 'featureScore.score', higherIsBetter: true },
  { key: 'performance', label: 'Performance', path: 'performanceScore.score', higherIsBetter: true },
  { key: 'mileageRunningCost', label: 'Mileage / running cost', path: 'mileageRunningCostScore.score', higherIsBetter: true },
  { key: 'practicality', label: 'Practicality', path: 'practicalityScore.score', higherIsBetter: true },
  { key: 'cityUse', label: 'City use', path: 'cityUseScore.score', higherIsBetter: true },
  { key: 'highwayUse', label: 'Highway use', path: 'highwayUseScore.score', higherIsBetter: true },
  { key: 'premiumComfort', label: 'Premium comfort', path: 'premiumComfortScore.score', higherIsBetter: true },
  { key: 'value', label: 'Same-family value', path: 'valueScore.score', higherIsBetter: true },
  { key: 'regretRisk', label: 'Regret risk', path: 'regretRisk.riskScore', higherIsBetter: false },
]);

const normalizeKey = (value = '') =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const compactArray = (items = []) => items.filter(Boolean);

const round1 = (value) => {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 10) / 10;
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

const getScore = (profile = {}, module = {}) => {
  const value = getPath(profile, module.path);
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
};

const humanizeKey = (value = '') =>
  String(value || '')
    .replace(/[_-]+/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase())
    .trim();

const getCollection = (db) => {
  if (!db?.collection) throw new Error('Mongo db handle is required.');
  return db.collection(SCORE_PROFILE_COLLECTION);
};

const buildTargetQuery = ({ makeKey, modelKey, fuelKey, transmissionKey } = {}) => {
  const query = {};
  const cleanMake = normalizeKey(makeKey);
  const cleanModel = normalizeKey(modelKey);
  const cleanFuel = normalizeKey(fuelKey);
  const cleanTransmission = normalizeKey(transmissionKey);

  if (cleanMake) query.makeKey = cleanMake;
  if (cleanModel) query.modelKey = cleanModel;
  if (cleanFuel) query.fuelKey = cleanFuel;
  if (cleanTransmission) query.transmissionKey = cleanTransmission;

  return query;
};

const getTargetProfiles = async ({ db, target = {}, limit = 80 } = {}) => {
  const query = buildTargetQuery(target);
  if (!query.modelKey) {
    throw new Error('modelKey is required for cross-model score diagnostics.');
  }

  return getCollection(db)
    .find(query, {
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
        fuelTransmissionFamilyKey: 1,
        referenceExShowroomPrice: 1,
        safetyScore: 1,
        featureScore: 1,
        performanceScore: 1,
        mileageRunningCostScore: 1,
        practicalityScore: 1,
        cityUseScore: 1,
        highwayUseScore: 1,
        premiumComfortScore: 1,
        valueScore: 1,
        regretRisk: 1,
        scoreReadiness: 1,
        buildVersion: 1,
        formulaVersion: 1,
        builtAt: 1,
      },
    })
    .sort({ referenceExShowroomPrice: 1, variantKey: 1, variantProfileKey: 1 })
    .limit(Math.max(1, Math.min(Number(limit) || 80, 120)))
    .toArray();
};

const summarizeTarget = ({ target = {}, profiles = [] } = {}) => {
  const first = profiles[0] || {};
  const modelKey = normalizeKey(target.modelKey || first.modelKey);
  const makeKey = normalizeKey(target.makeKey || first.makeKey);
  const fuelKey = normalizeKey(target.fuelKey || first.fuelKey);
  const transmissionKey = normalizeKey(target.transmissionKey || first.transmissionKey);

  const modules = MODULES.map((module) => {
    const scored = profiles
      .map((profile) => ({
        score: getScore(profile, module),
        variantFullName: profile.variantFullName,
        variantProfileKey: profile.variantProfileKey,
        scoreProfileKey: profile.scoreProfileKey,
      }))
      .filter((entry) => Number.isFinite(entry.score));

    const sorted = [...scored].sort((a, b) =>
      module.higherIsBetter ? b.score - a.score : a.score - b.score
    );

    const average =
      scored.length > 0
        ? round1(scored.reduce((sum, entry) => sum + entry.score, 0) / scored.length)
        : null;

    return {
      key: module.key,
      label: module.label,
      higherIsBetter: module.higherIsBetter,
      average,
      sampleSize: scored.length,
      missingCount: Math.max(0, profiles.length - scored.length),
      strongestVariant: sorted[0]
        ? {
            variantFullName: sorted[0].variantFullName,
            variantProfileKey: sorted[0].variantProfileKey,
            scoreProfileKey: sorted[0].scoreProfileKey,
            score: round1(sorted[0].score),
          }
        : null,
    };
  });

  return {
    makeKey,
    modelKey,
    fuelKey,
    transmissionKey,
    label: compactArray([humanizeKey(makeKey), humanizeKey(modelKey)]).join(' '),
    profileCount: profiles.length,
    variantCount: profiles.length,
    variants: profiles.map((profile) => ({
      variantProfileKey: profile.variantProfileKey,
      scoreProfileKey: profile.scoreProfileKey,
      variantFullName: profile.variantFullName,
      variantKey: profile.variantKey,
      referenceExShowroomPrice: profile.referenceExShowroomPrice ?? null,
    })),
    modules,
    sourceMeta: {
      buildVersions: [...new Set(profiles.map((profile) => profile.buildVersion).filter(Boolean))],
      formulaVersions: [...new Set(profiles.map((profile) => profile.formulaVersion).filter(Boolean))],
      builtAtValues: [...new Set(profiles.map((profile) => profile.builtAt).filter(Boolean))],
    },
    readiness: {
      finalOverallScoreReady: profiles.every(
        (profile) => profile?.scoreReadiness?.finalOverallScoreReady === true
      ),
      reasons: [
        ...new Set(
          profiles
            .map((profile) => profile?.scoreReadiness?.finalOverallScoreReason)
            .filter(Boolean)
        ),
      ],
    },
  };
};

const compareModule = ({ module = {}, targets = [] } = {}) => {
  const entries = targets
    .map((target) => {
      const moduleSummary = target.modules.find((item) => item.key === module.key);
      return {
        modelKey: target.modelKey,
        makeKey: target.makeKey,
        label: target.label,
        average: moduleSummary?.average ?? null,
        sampleSize: moduleSummary?.sampleSize ?? 0,
        strongestVariant: moduleSummary?.strongestVariant || null,
      };
    })
    .filter((entry) => Number.isFinite(Number(entry.average)));

  const sorted = [...entries].sort((a, b) =>
    module.higherIsBetter ? b.average - a.average : a.average - b.average
  );

  const leader = sorted[0] || null;
  const runnerUp = sorted[1] || null;
  const delta =
    leader && runnerUp
      ? round1(Math.abs(Number(leader.average) - Number(runnerUp.average)))
      : null;

  return {
    key: module.key,
    label: module.label,
    higherIsBetter: module.higherIsBetter,
    comparedCount: entries.length,
    leader,
    runnerUp,
    delta,
    entries,
    caveat:
      module.key === 'value'
        ? 'Value score is same-model/fuel-transmission relative value, not full-market value.'
        : module.key === 'regretRisk'
          ? 'Regret risk is evidence/trade-off risk, not buyer-contextual regret prediction.'
          : null,
  };
};

const buildCrossModelScoreDiagnostic = async ({
  db,
  targets = [],
  fuelKey = '',
  transmissionKey = '',
  limitPerModel = 80,
} = {}) => {
  const cleanTargets = targets
    .map((target) => ({
      makeKey: normalizeKey(target.makeKey || target.make_key),
      modelKey: normalizeKey(target.modelKey || target.model_key),
      fuelKey: normalizeKey(target.fuelKey || target.fuel_key || fuelKey),
      transmissionKey: normalizeKey(
        target.transmissionKey || target.transmission_key || transmissionKey
      ),
    }))
    .filter((target) => target.modelKey);

  const uniqueTargets = [];
  const seen = new Set();
  for (const target of cleanTargets) {
    const key = [target.makeKey, target.modelKey, target.fuelKey, target.transmissionKey].join('|');
    if (seen.has(key)) continue;
    seen.add(key);
    uniqueTargets.push(target);
  }

  if (uniqueTargets.length < 2) {
    return {
      ok: false,
      code: 'insufficient_cross_model_targets',
      message: 'At least two model targets are required for cross-model score diagnostics.',
      targets: uniqueTargets,
      rows: [],
      usageGuardrail: {
        canUseForFinalRecommendation: false,
        reasons: ['Cross-model score diagnostics are not final recommendations.'],
      },
    };
  }

  const targetProfiles = [];
  for (const target of uniqueTargets) {
    const profiles = await getTargetProfiles({ db, target, limit: limitPerModel });
    targetProfiles.push({ target, profiles });
  }

  const summaries = targetProfiles.map(({ target, profiles }) =>
    summarizeTarget({ target, profiles })
  );

  const missingTargets = summaries.filter((summary) => summary.profileCount <= 0);
  const moduleComparisons = MODULES.map((module) => compareModule({ module, targets: summaries }));

  return {
    ok: missingTargets.length === 0,
    diagnosticType: 'cross_model_score_diagnostic',
    intent: 'cross_model_score_diagnostic',
    displayMode: 'canvas',
    canvasType: 'score_insight_canvas',
    inlineType: 'score_insight_summary',
    scope: {
      fuelKey: normalizeKey(fuelKey || uniqueTargets[0]?.fuelKey),
      transmissionKey: normalizeKey(transmissionKey || uniqueTargets[0]?.transmissionKey),
      targetCount: summaries.length,
    },
    models: summaries,
    rows: summaries,
    moduleComparisons,
    missingTargets,
    usageGuardrail: {
      canUseForFinalRecommendation: false,
      reasons: [
        'This is a diagnostic module-score comparison only.',
        'Final recommendation needs buyer context, evidence thresholds, similar-cars graph, upgrade ladder, service/resale evidence and recommendation policy.',
      ],
    },
    caveats: [
      'No final overall winner is computed in this diagnostic.',
      'Module averages compare available variant score profiles within the requested fuel/transmission scope.',
      'Same-family value scores are relative inside each model family and should not be treated as full-market value scores.',
    ],
    sourceTransparency: {
      modulesChecked: [SCORE_PROFILE_COLLECTION],
      dataSource: SCORE_PROFILE_COLLECTION,
      recordCount: summaries.reduce((sum, summary) => sum + summary.profileCount, 0),
    },
  };
};

module.exports = {
  MODULES,
  buildCrossModelScoreDiagnostic,
};
