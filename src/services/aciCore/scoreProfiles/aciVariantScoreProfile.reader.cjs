const mongoose = require('mongoose');

const SCORE_PROFILE_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const DEFAULT_LIMIT = 20;
const MAX_LIMIT = 100;

const SCORE_PROFILE_PROJECTION = {
  _id: 0,
  scoreProfileKey: 1,
  variantProfileKey: 1,
  variantFullName: 1,
  makeKey: 1,
  modelKey: 1,
  variantKey: 1,
  fuelKey: 1,
  transmissionKey: 1,
  transmissionSubtype: 1,
  fuelTransmissionFamilyKey: 1,
  referenceExShowroomPrice: 1,
  priceSegment: 1,
  buildVersion: 1,
  formulaVersion: 1,
  builtAt: 1,

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
};

const VALID_SCORE_SORTS = new Set([
  'safetyScore.score',
  'featureScore.score',
  'performanceScore.score',
  'mileageRunningCostScore.score',
  'practicalityScore.score',
  'cityUseScore.score',
  'highwayUseScore.score',
  'premiumComfortScore.score',
  'valueScore.score',
  'regretRisk.riskScore',
]);

const normalizeKey = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const clampLimit = (value, fallback = DEFAULT_LIMIT) => {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), MAX_LIMIT);
};

const getDb = (db) => {
  if (db) return db;
  if (mongoose.connection?.db) return mongoose.connection.db;
  throw new Error('MongoDB connection is not available for score profile reader.');
};

const getCollection = (db) => getDb(db).collection(SCORE_PROFILE_COLLECTION);

const collectCaveats = (doc = {}) => {
  const modules = [
    doc.safetyScore,
    doc.featureScore,
    doc.performanceScore,
    doc.mileageRunningCostScore,
    doc.practicalityScore,
    doc.cityUseScore,
    doc.highwayUseScore,
    doc.premiumComfortScore,
    doc.valueScore,
    doc.regretRisk,
  ];

  const caveats = [];

  for (const module of modules) {
    if (Array.isArray(module?.caveats)) {
      caveats.push(...module.caveats);
    }
  }

  return [...new Set(caveats.filter(Boolean))];
};

const normalizeScoreProfile = (doc) => {
  if (!doc) return null;

  return {
    ...doc,
    scoreSummary: {
      finalOverallScoreReady: doc.scoreReadiness?.finalOverallScoreReady === true,
      finalOverallScoreReason: doc.scoreReadiness?.finalOverallScoreReason || null,
      knownSourceLimitations: doc.scoreReadiness?.knownSourceLimitations || [],
      caveatCount: doc.scoreReadiness?.caveatCount || 0,
      caveats: collectCaveats(doc),
    },
    usageGuardrail: {
      canUseForFinalRecommendation: false,
      reason:
        'Score profiles expose module scores only. Buyer-context weighting, similar-cars graph, upgrade ladder, resale/service evidence and recommendation policy must be applied before final recommendations.',
    },
  };
};

const buildBaseFilter = ({
  makeKey,
  modelKey,
  variantKey,
  fuelKey,
  transmissionKey,
  fuelTransmissionFamilyKey,
  priceSegment,
  minPrice,
  maxPrice,
  includeFinalOverallReady = true,
} = {}) => {
  const filter = {};

  if (makeKey) filter.makeKey = normalizeKey(makeKey);
  if (modelKey) filter.modelKey = normalizeKey(modelKey);
  if (variantKey) filter.variantKey = normalizeKey(variantKey);
  if (fuelKey) filter.fuelKey = normalizeKey(fuelKey);
  if (transmissionKey) filter.transmissionKey = normalizeKey(transmissionKey);
  if (fuelTransmissionFamilyKey) filter.fuelTransmissionFamilyKey = normalizeKey(fuelTransmissionFamilyKey);
  if (priceSegment) filter.priceSegment = normalizeKey(priceSegment);

  if (minPrice || maxPrice) {
    filter.referenceExShowroomPrice = {};
    if (minPrice) filter.referenceExShowroomPrice.$gte = Number(minPrice);
    if (maxPrice) filter.referenceExShowroomPrice.$lte = Number(maxPrice);
  }

  if (!includeFinalOverallReady) {
    filter['scoreReadiness.finalOverallScoreReady'] = false;
  }

  return filter;
};

const getVariantScoreProfile = async ({ db, scoreProfileKey, variantProfileKey }) => {
  const key = scoreProfileKey || variantProfileKey;
  if (!key) throw new Error('scoreProfileKey or variantProfileKey is required.');

  const filter = scoreProfileKey
    ? { scoreProfileKey: String(scoreProfileKey) }
    : { variantProfileKey: String(variantProfileKey) };

  const doc = await getCollection(db).findOne(filter, { projection: SCORE_PROFILE_PROJECTION });
  return normalizeScoreProfile(doc);
};

const getModelScoreProfiles = async ({
  db,
  makeKey,
  modelKey,
  fuelKey,
  transmissionKey,
  fuelTransmissionFamilyKey,
  limit = 80,
} = {}) => {
  if (!modelKey) throw new Error('modelKey is required.');

  const filter = buildBaseFilter({
    makeKey,
    modelKey,
    fuelKey,
    transmissionKey,
    fuelTransmissionFamilyKey,
  });

  const docs = await getCollection(db)
    .find(filter, { projection: SCORE_PROFILE_PROJECTION })
    .sort({
      fuelTransmissionFamilyKey: 1,
      referenceExShowroomPrice: 1,
      variantFullName: 1,
    })
    .limit(clampLimit(limit, 80))
    .toArray();

  return docs.map(normalizeScoreProfile);
};

const getSameFamilyValueProfiles = async ({
  db,
  makeKey,
  modelKey,
  fuelKey,
  transmissionKey,
  fuelTransmissionFamilyKey,
  limit = 20,
} = {}) => {
  if (!modelKey) throw new Error('modelKey is required.');

  const filter = buildBaseFilter({
    makeKey,
    modelKey,
    fuelKey,
    transmissionKey,
    fuelTransmissionFamilyKey,
  });

  const docs = await getCollection(db)
    .find(filter, { projection: SCORE_PROFILE_PROJECTION })
    .sort({
      'valueScore.score': -1,
      referenceExShowroomPrice: 1,
    })
    .limit(clampLimit(limit))
    .toArray();

  return {
    usageGuardrail: {
      canUseForFinalRecommendation: false,
      reason:
        'Value profiles are same-model/fuel-transmission relative. They are useful for variant value review, not full-market recommendations.',
    },
    profiles: docs.map(normalizeScoreProfile),
  };
};

const getTopScoreProfiles = async ({
  db,
  scorePath,
  direction = 'desc',
  limit = 20,
  filters = {},
} = {}) => {
  if (!VALID_SCORE_SORTS.has(scorePath)) {
    throw new Error(`Invalid scorePath. Allowed: ${[...VALID_SCORE_SORTS].join(', ')}`);
  }

  const filter = buildBaseFilter(filters);
  const sortDirection = direction === 'asc' ? 1 : -1;

  // Avoid treating null scores as top/bottom useful results.
  filter[scorePath] = { $ne: null };

  const docs = await getCollection(db)
    .find(filter, { projection: SCORE_PROFILE_PROJECTION })
    .sort({ [scorePath]: sortDirection, referenceExShowroomPrice: 1 })
    .limit(clampLimit(limit))
    .toArray();

  return {
    usageGuardrail: {
      canUseForFinalRecommendation: false,
      reason:
        'Sorted module scores are diagnostic signals. Final recommendations require buyer context and recommendation policy.',
    },
    profiles: docs.map(normalizeScoreProfile),
  };
};

const getScoreProfileCoverage = async ({ db } = {}) => {
  const col = getCollection(db);

  return {
    collection: SCORE_PROFILE_COLLECTION,
    totalScoreProfiles: await col.countDocuments(),
    byBuildVersion: await col
      .aggregate([
        { $group: { _id: '$buildVersion', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ])
      .toArray(),
    byFormulaVersion: await col
      .aggregate([
        { $group: { _id: '$formulaVersion', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ])
      .toArray(),
    finalOverallScoreReadyCount: await col.countDocuments({
      'scoreReadiness.finalOverallScoreReady': true,
    }),
    featureJoinMissing: await col.countDocuments({
      'scoreReadiness.hasFeatureMatrixJoin': false,
    }),
    featureAliasDiagnostic: await col.countDocuments({
      'scoreReadiness.knownSourceLimitations': 'feature_alias_diagnostic',
    }),
    mileageNotScored: await col.countDocuments({
      'mileageRunningCostScore.status': { $ne: 'scored' },
    }),
    regretRiskByLevel: await col
      .aggregate([
        { $group: { _id: '$regretRisk.riskLevel', count: { $sum: 1 } } },
        { $sort: { count: -1 } },
      ])
      .toArray(),
  };
};

module.exports = {
  SCORE_PROFILE_COLLECTION,
  VALID_SCORE_SORTS,
  normalizeKey,
  getVariantScoreProfile,
  getModelScoreProfiles,
  getSameFamilyValueProfiles,
  getTopScoreProfiles,
  getScoreProfileCoverage,
};
