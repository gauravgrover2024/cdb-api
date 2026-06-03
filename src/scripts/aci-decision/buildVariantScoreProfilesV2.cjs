#!/usr/bin/env node
require('dotenv').config();

const mongoose = require('mongoose');
const { loadFeatureScoreTaxonomy } = require('../../services/aciCore/scoreProfiles/featureScoreTaxonomy.loader.cjs');

const PROFILE_COLLECTION =
  process.env.ACI_VARIANT_DECISION_PROFILE_COLLECTION || 'aci_vehicle_variant_decision_profile';
const FEATURE_MATRIX_COLLECTION =
  process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2';
const FEATURE_SCORE_MATRIX_PROJECTION_COLLECTION =
  process.env.ACI_FEATURE_SCORE_MATRIX_PROJECTION_COLLECTION || 'aci_feature_score_matrix_projection_v1';
const TARGET_COLLECTION =
  process.env.ACI_VARIANT_SCORE_PROFILE_COLLECTION || 'aci_vehicle_variant_score_profile';

const args = process.argv.slice(2);
const WRITE = args.includes('--write');
const RESET = args.includes('--reset');
const limitArgIndex = args.indexOf('--limit');
const LIMIT = limitArgIndex >= 0 ? Number(args[limitArgIndex + 1]) : 0;

const BUILD_VERSION = 'variant_score_profile_v2_2_2026_06_03';
const FORMULA_VERSION = 'trust_first_module_scores_v2_2_taxonomy_driven_feature_score';

const FEATURE_SCORE_TAXONOMY = loadFeatureScoreTaxonomy();
const FEATURE_DEFS = FEATURE_SCORE_TAXONOMY.features;
const FEATURE_LAYER_WEIGHTS = FEATURE_SCORE_TAXONOMY.layerWeights;

const hasNumber = (value) =>
  value !== null &&
  value !== undefined &&
  value !== '' &&
  Number.isFinite(Number(value));

const toNumber = (value) => (hasNumber(value) ? Number(value) : null);

const clamp = (value, min = 0, max = 100) =>
  Math.max(min, Math.min(max, Number(value) || 0));

const round = (value, digits = 1) =>
  hasNumber(value) ? Number(Number(value).toFixed(digits)) : null;

const normKey = (value) =>
  String(value || '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const compact = (arr) => arr.filter((x) => x !== null && x !== undefined && x !== '');

const safeArray = (value) => (Array.isArray(value) ? value : []);

const firstNumber = (...values) => {
  for (const value of values) {
    if (hasNumber(value)) return Number(value);
  }
  return null;
};

const percentileScore = (value, sortedValues, { higherIsBetter = true } = {}) => {
  if (!hasNumber(value) || !sortedValues.length) return null;
  if (sortedValues.length === 1) return 50;

  const v = Number(value);
  let lowerOrEqual = 0;

  for (const n of sortedValues) {
    if (n <= v) lowerOrEqual += 1;
    else break;
  }

  const pct = ((lowerOrEqual - 1) / (sortedValues.length - 1)) * 100;
  return round(higherIsBetter ? pct : 100 - pct);
};

const priceSegmentFor = (price) => {
  const p = Number(price || 0);
  if (!p) return 'unknown';
  if (p < 700000) return 'budget';
  if (p < 1200000) return 'value';
  if (p < 2000000) return 'mid';
  if (p < 3500000) return 'premium';
  return 'luxury';
};

const detectTransmissionSubtype = (profile = {}) => {
  const text = normKey([
    profile.variantKey,
    profile.variantFullName,
    profile.performanceBasis?.transmissionType,
    profile.transmissionKey,
  ].filter(Boolean).join(' '));

  const hasToken = (token) => new RegExp(`(^|_)${token}(_|$)`).test(text);

  if (hasToken('ivt')) return 'ivt';
  if (hasToken('cvt')) return 'cvt';
  if (hasToken('dct')) return 'dct';
  if (hasToken('dsg')) return 'dsg';
  if (hasToken('amt')) return 'amt';
  if (hasToken('imt')) return 'imt';
  if (hasToken('tc') || hasToken('torque_converter')) return 'tc';
  if (hasToken('at') || text.includes('automatic')) return 'at';
  if (hasToken('mt') || text.includes('manual')) return 'mt';

  return profile.transmissionKey === 'automatic' ? 'at' : profile.transmissionKey === 'manual' ? 'mt' : 'unknown';
};

const featureValueLooksPresent = (value) => {
  if (value === true) return true;
  if (value === false || value === null || value === undefined) return false;

  if (typeof value === 'number') return Number.isFinite(value) && value > 0;

  if (typeof value === 'string') {
    const text = value.trim().toLowerCase();
    if (!text) return false;
    if (['no', 'false', 'not available', 'na', 'n/a', '-', '0'].includes(text)) return false;
    if (text.includes('not available')) return false;
    return true;
  }

  if (typeof value === 'object') {
    if (value.available === true) return true;
    if (value.present === true) return true;
    if (value.status && /available|yes|present/i.test(String(value.status))) return true;
    if (value.value !== undefined) return featureValueLooksPresent(value.value);
  }

  return false;
};

const getNormalizedFeatureIndex = (matrixDoc = {}) => {
  if (!matrixDoc) return { keys: new Set(), values: new Map() };

  if (matrixDoc.__normalizedFeatureIndex) return matrixDoc.__normalizedFeatureIndex;

  const values = new Map();
  const keys = new Set();

  // Important:
  // featuresByKey carries availability metadata.
  // featureKeys may include catalog keys even when the feature is "Not Available",
  // so featureKeys must not be used as proof of availability.
  for (const [key, value] of Object.entries(matrixDoc.featuresByKey || {})) {
    const nk = normKey(key);
    if (nk) {
      keys.add(nk);
      values.set(nk, value);
    }
  }

  matrixDoc.__normalizedFeatureIndex = { keys, values };
  return matrixDoc.__normalizedFeatureIndex;
};

const getFeatureRaw = (matrixDoc, aliases) => {
  if (!matrixDoc) return null;

  const { keys, values } = getNormalizedFeatureIndex(matrixDoc);
  const normalizedAliases = aliases.map(normKey).filter(Boolean);

  // Exact normalized key lookup only.
  for (const alias of normalizedAliases) {
    if (values.has(alias)) return values.get(alias);
  }

  // Conservative one-way fuzzy lookup:
  // Allow a source key to be more specific than alias.
  // Do NOT allow alias.includes(key), because generic keys like "parking_sensors"
  // can falsely satisfy "front parking sensors".
  for (const alias of normalizedAliases) {
    for (const key of keys) {
      if (key.includes(alias) && values.has(key)) {
        return values.get(key);
      }
    }
  }

  return null;
};

const hasFeature = (matrixDoc, aliases) => featureValueLooksPresent(getFeatureRaw(matrixDoc, aliases));

const featureValueText = (value) =>
  String(
    typeof value === 'object' && value !== null
      ? [value.value, value.displayName, value.availabilityStatus].filter(Boolean).join(' ')
      : value || ''
  ).toLowerCase();

const hasParkingSensorPosition = (matrixDoc, position) => {
  const raw = getFeatureRaw(matrixDoc, ['parking_sensors', 'parking sensors']);
  if (!featureValueLooksPresent(raw)) return false;

  const text = featureValueText(raw);
  if (position === 'rear') return text.includes('rear');
  if (position === 'front') return text.includes('front');

  return false;
};

const featureSpecificFallback = (matrixDoc, featureKey) => {
  if (featureKey === 'parkingSensorsRear') return hasParkingSensorPosition(matrixDoc, 'rear');
  if (featureKey === 'parkingSensorsFront') return hasParkingSensorPosition(matrixDoc, 'front');
  return false;
};

const featureKeysCount = (matrixDoc) => {
  if (!matrixDoc) return 0;
  return safeArray(matrixDoc.featureKeys).length || Object.keys(matrixDoc.featuresByKey || {}).length || 0;
};


const featureDirectFallback = (profile, featureKey) => {
  const safety = profile.safetyBasis || {};
  const practicality = profile.practicalityBasis || {};

  const direct = {
    rearAcVents: practicality.rearAcVents === true,
    rearCamera: safety.hasRearCamera === true,
    camera360: safety.hasCamera360 === true,
    adas: safety.hasAdas === true,
  };

  return direct[featureKey] === true;
};


const scoreFeatureRichness = (profile, matrixDoc) => {
  if (!matrixDoc) {
    return {
      score: null,
      scoreType: 'taxonomy_driven_layered_equipment_richness_v2_2',
      status: 'not_scored_missing_feature_matrix',
      confidence: 'low',
      subScores: {},
      evidence: { presentWeighted: 0, possibleWeighted: 0, presentKeys: [] },
      featureDetectionDiagnostic: {
        featureKeysInMatrix: 0,
        aliasesChecked: FEATURE_DEFS.length,
        suspiciouslyLowScore: false
      },
      caveats: ['Feature matrix could not be joined for this variant.']
    };
  }

  const featureKeysInMatrix =
    Number(matrixDoc?.featureKeysInMatrixCount || 0) ||
    safeArray(matrixDoc?.featureKeys).length ||
    Object.keys(matrixDoc?.featuresByKey || {}).length ||
    0;

  const layers = {
    essential: { presentWeighted: 0, possibleWeighted: 0, presentKeys: [] },
    useful: { presentWeighted: 0, possibleWeighted: 0, presentKeys: [] },
    premium: { presentWeighted: 0, possibleWeighted: 0, presentKeys: [] }
  };

  const directFallback = (key) => {
    const safety = profile.safetyBasis || {};
    const practicality = profile.practicalityBasis || {};

    if (key === 'rearAcVents') return practicality.rearAcVents === true;
    if (key === 'rearCamera') return safety.hasRearCamera === true;
    if (key === 'camera360') return safety.hasCamera360 === true;
    if (key === 'adas') return safety.hasAdas === true;

    return false;
  };

  let presentWeighted = 0;
  let possibleWeighted = 0;
  const presentKeys = [];

  for (const def of FEATURE_DEFS) {
    const layer = layers[def.category] ? def.category : 'useful';
    possibleWeighted += def.weight;
    layers[layer].possibleWeighted += def.weight;

    const present =
      hasFeature(matrixDoc, def.aliases) ||
      featureSpecificFallback(matrixDoc, def.key) ||
      directFallback(def.key);

    if (present) {
      presentWeighted += def.weight;
      presentKeys.push(def.key);
      layers[layer].presentWeighted += def.weight;
      layers[layer].presentKeys.push(def.key);
    }
  }

  const subScores = Object.fromEntries(
    Object.entries(layers).map(([key, row]) => [
      key,
      {
        score: row.possibleWeighted ? round((row.presentWeighted / row.possibleWeighted) * 100) : null,
        presentWeighted: row.presentWeighted,
        possibleWeighted: row.possibleWeighted,
        presentKeys: row.presentKeys
      }
    ])
  );

  const weightedLayerParts = Object.entries(FEATURE_LAYER_WEIGHTS)
    .map(([key, weight]) => ({
      key,
      weight,
      score: subScores[key]?.score
    }))
    .filter((row) => hasNumber(row.score));

  const layerWeightTotal = weightedLayerParts.reduce((sum, row) => sum + row.weight, 0);

  const layeredScore = layerWeightTotal
    ? weightedLayerParts.reduce((sum, row) => sum + row.score * row.weight, 0) / layerWeightTotal
    : null;

  const rawCoverageScore = possibleWeighted ? (presentWeighted / possibleWeighted) * 100 : null;

  const suspiciouslyLowScore =
    hasNumber(layeredScore) &&
    layeredScore < 15 &&
    featureKeysInMatrix > 35 &&
    presentKeys.length <= 2;

  const caveats = [
    'Feature score is taxonomy-driven and layered. Safety-critical equipment is handled mainly by safetyScore.'
  ];

  if (suspiciouslyLowScore) {
    caveats.push('Feature score is suspiciously low despite many feature keys in matrix; alias coverage should be reviewed before using this score strongly.');
  }

  return {
    score: round(layeredScore),
    rawCoverageScore: round(rawCoverageScore),
    scoreType: 'taxonomy_driven_layered_equipment_richness_v2_2',
    status: 'scored',
    confidence: suspiciouslyLowScore ? 'medium_low' : 'medium',
    subScores,
    evidence: {
      presentWeighted,
      possibleWeighted,
      presentKeys,
      taxonomyVersion: FEATURE_SCORE_TAXONOMY.taxonomyVersion,
      taxonomySourcePath: FEATURE_SCORE_TAXONOMY.sourcePath,
      layerWeights: FEATURE_LAYER_WEIGHTS,
      joinKey: matrixDoc.__joinKey || null,
      featureMatrixBuildId: matrixDoc.buildId || matrixDoc.sourceBuildId || null
    },
    featureDetectionDiagnostic: {
      featureKeysInMatrix,
      aliasesChecked: FEATURE_DEFS.length,
      suspiciouslyLowScore
    },
    caveats
  };
};


const crashStarsFromSafety = (safety) => ({
  adult: firstNumber(safety?.bharatNcapAdult?.stars, safety?.globalNcapAdult?.stars),
  child: firstNumber(safety?.bharatNcapChild?.stars, safety?.globalNcapChild?.stars)
});

const crashConfidenceMultiplierFor = (status) => {
  if (status === 'verified_official') return 1.0;
  if (status === 'internal_variant_feature_matrix_needs_official_verification') return 0.85;
  if (status === 'inherited_model_level_consistent_needs_official_verification') return 0.80;
  if (status === 'unknown_or_not_publicly_verified') return 0;
  if (status === 'blocked_mixed_internal_ratings') return 0;
  return 0.80;
};

const scoreSafety = (profile) => {
  const safety = profile.safetyBasis || {};
  const status = safety.crashRatingStatus || 'missing_crash_status';
  const caveats = [];

  const stars = crashStarsFromSafety(safety);
  let rawCrashScore = null;

  if (hasNumber(stars.adult) || hasNumber(stars.child)) {
    const adultScore = hasNumber(stars.adult) ? (stars.adult / 5) * 100 : null;
    const childScore = hasNumber(stars.child) ? (stars.child / 5) * 100 : null;
    rawCrashScore =
      hasNumber(adultScore) && hasNumber(childScore)
        ? adultScore * 0.65 + childScore * 0.35
        : firstNumber(adultScore, childScore);
  }

  const multiplier = crashConfidenceMultiplierFor(status);
  const effectiveCrashScore = hasNumber(rawCrashScore) ? rawCrashScore * multiplier : null;

  const featurePoints =
    (safety.hasSixAirbags === true ? 16 : 0) +
    (safety.hasEsc === true ? 14 : 0) +
    (safety.hasAbs === true ? 8 : 0) +
    (safety.hasEbd === true ? 5 : 0) +
    (safety.hasBrakeAssist === true ? 4 : 0) +
    (safety.hasIsofix === true ? 8 : 0) +
    (safety.hasTpms === true ? 4 : 0) +
    (safety.hasHillAssist === true ? 3 : 0) +
    (safety.hasAdas === true ? 8 : 0) +
    (safety.hasRearCamera === true ? 3 : 0) +
    (safety.hasCamera360 === true ? 4 : 0);

  const safetyFeatureScore = clamp((featurePoints / 77) * 100);

  let confidence = 'medium';

  if (status === 'internal_variant_feature_matrix_needs_official_verification') {
    confidence = 'medium';
    caveats.push('Crash rating comes from internal feature matrix and needs official tested-variant applicability verification.');
  } else if (status === 'inherited_model_level_consistent_needs_official_verification') {
    confidence = 'medium_low';
    caveats.push('Crash rating is inherited from consistent same-model internal evidence and needs official verification.');
  } else if (status === 'unknown_or_not_publicly_verified') {
    confidence = 'low';
    caveats.push('Crash rating is unknown or not publicly verified; safety score relies mainly on safety-feature package and is capped.');
  } else if (status === 'blocked_mixed_internal_ratings') {
    confidence = 'low';
    caveats.push('Mixed internal crash-rating evidence blocked confident crash scoring.');
  }

  let score;

  if (hasNumber(effectiveCrashScore) && multiplier > 0) {
    score = effectiveCrashScore * 0.70 + safetyFeatureScore * 0.30;
  } else {
    score = Math.min(safetyFeatureScore * 0.85, status === 'blocked_mixed_internal_ratings' ? 55 : 60);
  }

  return {
    score: round(score),
    scoreType: 'crash_weighted_with_confidence_v2',
    crashScore: round(rawCrashScore),
    effectiveCrashScore: round(effectiveCrashScore),
    crashConfidenceMultiplier: multiplier,
    safetyFeatureScore: round(safetyFeatureScore),
    status,
    confidence,
    evidence: {
      adultStars: stars.adult,
      childStars: stars.child,
      airbagsCount: safety.airbagsCount || null,
      hasSixAirbags: safety.hasSixAirbags === true,
      hasEsc: safety.hasEsc === true,
      hasAbs: safety.hasAbs === true,
      hasIsofix: safety.hasIsofix === true,
      hasAdas: safety.hasAdas === true,
      crashRatingSource: safety.crashRatingSource || null,
      crashRatingConfidence: safety.crashRatingConfidence || null,
      crashRatingReviewStatus: safety.crashRatingReviewStatus || null,
    },
    caveats
  };
};

const getMileageValue = (profile) =>
  firstNumber(
    profile.mileageBasis?.primaryMileageValue,
    profile.mileageBasis?.claimedMileageKmpl,
    profile.mileageBasis?.cngMileageKmPerKg,
    profile.mileageBasis?.araiMileage,
    profile.mileageBasis?.araiMileageKmpl,
    profile.mileageBasis?.evClaimedRange,
    profile.mileageBasis?.evClaimedRangeKm
  );

const scoreMileage = (profile, distributions) => {
  const value = getMileageValue(profile);
  const fuelKey = profile.fuelKey || 'unknown';
  const status = profile.dataQuality?.mileageCompletenessStatus || null;

  if (!hasNumber(value)) {
    return {
      score: null,
      scoreType: 'fuel_relative_mileage_percentile_v2',
      status: String(status || '').startsWith('known_source_limitation')
        ? 'not_scored_known_source_limitation'
        : 'not_scored_missing_mileage',
      confidence: 'low',
      evidence: { value: null, fuelKey, basis: status },
      caveats: ['No reliable mileage/range value available for scoring.']
    };
  }

  const score = percentileScore(value, distributions.mileageByFuel[fuelKey] || []);
  const caveats = [];
  let confidence = 'medium';

  if (status === 'manual_google_review_complete') {
    confidence = 'medium';
    caveats.push('Mileage value comes from manual Google review evidence and is stored as claimed/reviewed mileage, not automatically official ARAI.');
  }

  return {
    score,
    scoreType: 'fuel_relative_mileage_percentile_v2',
    status: 'scored',
    confidence,
    evidence: {
      value,
      fuelKey,
      unit: fuelKey === 'cng' ? 'km/kg' : fuelKey === 'electric' ? 'km range' : 'kmpl',
      sourceStatus: status || null
    },
    caveats
  };
};

const scorePerformance = (profile, distributions) => {
  const perf = profile.performanceBasis || {};
  const caveats = ['Performance score v2 is global-percentile based; segment-relative performance will be added later.'];

  const powerScore = percentileScore(perf.powerBhp, distributions.powerBhp);
  const torqueScore = percentileScore(perf.torqueNm, distributions.torqueNm);
  const ptwScore = percentileScore(perf.powerToWeight, distributions.powerToWeight);

  const parts = compact([
    hasNumber(ptwScore) ? { value: ptwScore, weight: 0.45 } : null,
    hasNumber(powerScore) ? { value: powerScore, weight: 0.30 } : null,
    hasNumber(torqueScore) ? { value: torqueScore, weight: 0.25 } : null,
  ]);

  if (!parts.length) {
    return {
      score: null,
      scoreType: 'global_performance_percentile_v2',
      status: 'not_scored_missing_performance_data',
      confidence: 'low',
      evidence: {},
      caveats: ['Power/torque inputs missing.']
    };
  }

  const totalWeight = parts.reduce((s, p) => s + p.weight, 0);
  const score = parts.reduce((s, p) => s + p.value * p.weight, 0) / totalWeight;

  if (!hasNumber(perf.powerToWeight)) {
    caveats.push('Power-to-weight unavailable; performance score uses power/torque only.');
  }

  return {
    score: round(score),
    scoreType: 'global_performance_percentile_v2',
    status: 'scored',
    confidence: hasNumber(perf.powerToWeight) ? 'high' : 'medium',
    evidence: {
      powerBhp: toNumber(perf.powerBhp),
      torqueNm: toNumber(perf.torqueNm),
      powerToWeight: toNumber(perf.powerToWeight),
      turbo: typeof perf.turbo === 'boolean' ? perf.turbo : null,
      transmissionType: perf.transmissionType || null
    },
    caveats
  };
};

const scorePracticality = (profile, distributions) => {
  const p = profile.practicalityBasis || {};
  const fuelKey = profile.fuelKey || 'unknown';
  const caveats = [];
  const components = [];

  let seatingScore = null;
  if (hasNumber(p.seatingCapacity)) {
    const seats = Number(p.seatingCapacity);
    seatingScore = seats >= 8 ? 90 : seats >= 7 ? 85 : seats >= 6 ? 75 : seats >= 5 ? 60 : 35;
    components.push({ key: 'seating', score: seatingScore, weight: 0.25 });
  }

  const bootScore = percentileScore(p.bootSpaceLitres, distributions.bootSpaceLitres);
  if (hasNumber(bootScore)) {
    components.push({ key: 'boot', score: bootScore, weight: 0.25 });
  } else if (fuelKey === 'cng') {
    caveats.push('Boot space data missing or reduced due to CNG tank placement; score excludes boot normalization.');
  } else {
    caveats.push('Boot-space value unavailable; practicality score excludes boot normalization.');
  }

  const familyKitScore =
    (p.rearAcVents === true ? 50 : 0) +
    (p.isofix === true ? 50 : 0);
  components.push({ key: 'familyKit', score: familyKitScore, weight: 0.25 });

  const groundClearanceScore = percentileScore(p.groundClearanceMm, distributions.groundClearanceMm);
  if (hasNumber(groundClearanceScore)) {
    components.push({ key: 'groundClearance', score: groundClearanceScore, weight: 0.15 });
  } else {
    caveats.push('Ground-clearance value unavailable; practicality score excludes ground-clearance normalization.');
  }

  const cabinWidthScore = percentileScore(p.widthMm, distributions.widthMm);
  if (hasNumber(cabinWidthScore)) {
    components.push({ key: 'cabinWidth', score: cabinWidthScore, weight: 0.10 });
  }

  const totalWeight = components.reduce((sum, row) => sum + row.weight, 0);
  const score = totalWeight
    ? components.reduce((sum, row) => sum + row.score * row.weight, 0) / totalWeight
    : null;

  return {
    score: round(score),
    scoreType: 'component_weighted_practicality_v2',
    status: hasNumber(score) ? 'scored' : 'not_scored_missing_practicality_data',
    confidence: caveats.length ? 'medium' : 'high',
    evidence: {
      seatingCapacity: toNumber(p.seatingCapacity),
      bootSpaceLitres: toNumber(p.bootSpaceLitres),
      lengthMm: toNumber(p.lengthMm),
      widthMm: toNumber(p.widthMm),
      groundClearanceMm: toNumber(p.groundClearanceMm),
      rearAcVents: p.rearAcVents === true,
      isofix: p.isofix === true,
      components
    },
    caveats
  };
};

const cityTransmissionBonus = (subtype) => {
  const map = {
    ivt: 14,
    cvt: 14,
    tc: 12,
    at: 12,
    dct: 10,
    dsg: 10,
    amt: 5,
    imt: 3,
    mt: 0,
    unknown: 0
  };
  return map[subtype] ?? 0;
};

const scoreCityUse = (profile, matrixDoc, distributions) => {
  const p = profile.practicalityBasis || {};
  const safety = profile.safetyBasis || {};
  const subtype = detectTransmissionSubtype(profile);
  const caveats = [];

  const lengthScore = percentileScore(p.lengthMm, distributions.lengthMm, { higherIsBetter: false });
  const widthScore = percentileScore(p.widthMm, distributions.widthMm, { higherIsBetter: false });

  const hasRearCam =
    hasFeature(matrixDoc, ['rearCamera', 'rear parking camera', 'parking camera']) ||
    safety.hasRearCamera === true;

  const has360Cam =
    hasFeature(matrixDoc, ['camera360', '360 camera', '360 degree camera']) ||
    safety.hasCamera360 === true;

  const frontSensors =
    hasFeature(matrixDoc, ['parkingSensorsFront', 'front parking sensors']) ||
    hasParkingSensorPosition(matrixDoc, 'front');

  const rearSensors =
    hasFeature(matrixDoc, ['parkingSensorsRear', 'rear parking sensors']) ||
    hasParkingSensorPosition(matrixDoc, 'rear');

  let score = 42;
  if (hasNumber(lengthScore)) score += lengthScore * 0.20;
  if (hasNumber(widthScore)) score += widthScore * 0.12;

  score += cityTransmissionBonus(subtype);

  if (hasRearCam) score += 7;
  if (has360Cam) score += 10;
  if (frontSensors) score += 5;
  if (rearSensors) score += 3;

  if (subtype === 'amt') caveats.push('AMT is convenient versus manual but may feel less smooth in stop-go city traffic than CVT/IVT or torque converter.');
  if (subtype === 'dct' || subtype === 'dsg') caveats.push('DCT/DSG can be quick but may need a traffic-use caveat depending on model/heat management evidence.');

  return {
    score: round(clamp(score)),
    scoreType: 'city_daily_use_v2',
    status: 'scored',
    confidence: matrixDoc ? 'medium' : 'medium_low',
    evidence: {
      lengthMm: toNumber(p.lengthMm),
      widthMm: toNumber(p.widthMm),
      transmissionKey: profile.transmissionKey,
      transmissionSubtype: subtype,
      transmissionCityBonus: cityTransmissionBonus(subtype),
      rearCamera: hasRearCam,
      camera360: has360Cam,
      parkingSensorsFront: frontSensors,
      parkingSensorsRear: rearSensors
    },
    caveats
  };
};

const scoreHighwayUse = (profile, matrixDoc, performanceScore, safetyScore, mileageScore) => {
  const safety = profile.safetyBasis || {};
  const caveats = [
    'Highway score v2 uses performance, safety, mileage and highway-assist features; ride comfort, NVH, tyre quality and braking feel are not yet scored.'
  ];

  if (safeArray(safetyScore.caveats).length) caveats.push(...safetyScore.caveats);

  const cruise = hasFeature(matrixDoc, ['cruiseControl', 'cruise control']);
  const adaptiveCruise = hasFeature(matrixDoc, ['adaptiveCruiseControl', 'adaptive cruise control']);
  const adas = hasFeature(matrixDoc, ['adas']) || safety.hasAdas === true;

  let score = 30;
  if (hasNumber(performanceScore.score)) score += performanceScore.score * 0.25;
  if (hasNumber(safetyScore.score)) score += safetyScore.score * 0.30;
  if (hasNumber(mileageScore.score)) score += mileageScore.score * 0.10;

  if (cruise) score += 6;
  if (adaptiveCruise) score += 5;
  if (adas) score += 5;

  return {
    score: round(clamp(score)),
    scoreType: 'highway_use_v2',
    status: 'scored',
    confidence: matrixDoc ? 'medium' : 'medium_low',
    evidence: {
      performanceScore: performanceScore.score,
      safetyScore: safetyScore.score,
      mileageScore: mileageScore.score,
      cruiseControl: cruise,
      adaptiveCruiseControl: adaptiveCruise,
      adas
    },
    caveats
  };
};

const scorePremiumComfort = (matrixDoc, practicalityScore, featureScore) => {
  if (!matrixDoc) {
    return {
      score: null,
      scoreType: 'premium_comfort_v2',
      status: 'not_scored_missing_feature_matrix',
      confidence: 'low',
      evidence: {},
      caveats: ['Feature matrix could not be joined.']
    };
  }

  const premiumSubScore = featureScore.subScores?.premium?.score;
  let score = hasNumber(premiumSubScore) ? premiumSubScore * 0.75 : 20;
  if (hasNumber(practicalityScore.score)) score += practicalityScore.score * 0.20;

  const evidence = {
    ventilatedSeats: hasFeature(matrixDoc, ['ventilatedSeats', 'ventilated front seats']),
    panoramicSunroof: hasFeature(matrixDoc, ['panoramicSunroof', 'panoramic sunroof']),
    sunroof: hasFeature(matrixDoc, ['sunroof']),
    leatheretteSeats: hasFeature(matrixDoc, ['leatheretteSeats', 'leatherette upholstery']),
    automaticClimateControl: hasFeature(matrixDoc, ['automaticClimateControl', 'automatic climate control']),
    poweredDriverSeat: hasFeature(matrixDoc, ['poweredDriverSeat', 'power driver seat']),
    wirelessCharging: hasFeature(matrixDoc, ['wirelessCharging']),
    premiumSound: hasFeature(matrixDoc, ['premiumSound', 'branded speakers'])
  };

  return {
    score: round(clamp(score)),
    scoreType: 'premium_comfort_v2',
    status: 'scored',
    confidence: 'medium',
    evidence,
    caveats: ['Premium comfort score v2 is feature-richness based; material quality, seat comfort, ride comfort and cabin quietness need editorial/test evidence later.']
  };
};

const blendedUtility = ({ safetyScore, featureScore, performanceScore, mileageScore, practicalityScore }) => {
  const parts = compact([
    hasNumber(safetyScore.score) ? { key: 'safetyScore', value: safetyScore.score, weight: 0.15 } : null,
    hasNumber(featureScore.score) ? { key: 'featureScore', value: featureScore.score, weight: 0.30 } : null,
    hasNumber(performanceScore.score) ? { key: 'performanceScore', value: performanceScore.score, weight: 0.15 } : null,
    hasNumber(mileageScore.score) ? { key: 'mileageRunningCostScore', value: mileageScore.score, weight: 0.20 } : null,
    hasNumber(practicalityScore.score) ? { key: 'practicalityScore', value: practicalityScore.score, weight: 0.20 } : null,
  ]);

  if (!parts.length) return { value: null, parts: [] };

  const totalWeight = parts.reduce((sum, part) => sum + part.weight, 0);
  return {
    value: parts.reduce((sum, part) => sum + part.value * part.weight, 0) / totalWeight,
    parts
  };
};

const familyKeyFor = (profile) =>
  `${profile.modelKey || 'unknown'}__${profile.fuelTransmissionFamilyKey || profile.fuelKey || 'unknown'}`;

const scoreValue = (profile, modules, familyDistributions) => {
  const price = toNumber(profile.referenceExShowroomPrice);
  const familyKey = familyKeyFor(profile);

  if (!hasNumber(price)) {
    return {
      score: null,
      scoreType: 'same_model_family_value_v2',
      status: 'not_scored_missing_price',
      confidence: 'low',
      evidence: {},
      caveats: ['Reference ex-showroom price missing.']
    };
  }

  const utility = blendedUtility(modules);

  if (!hasNumber(utility.value)) {
    return {
      score: null,
      scoreType: 'same_model_family_value_v2',
      status: 'not_scored_missing_utility_inputs',
      confidence: 'low',
      evidence: { price, comparisonScope: familyKey },
      caveats: ['No utility score inputs available.']
    };
  }

  const valuePerLakh = utility.value / (price / 100000);
  const distribution = familyDistributions.valuePerLakhByFamily[familyKey] || [];
  const rawScore = percentileScore(valuePerLakh, distribution);
  const sampleSize = distribution.length;

  let finalScore = rawScore;
  const caveats = ['Value score v2 is same-model/fuel-transmission relative value, not full-market value.'];
  let softened = false;

  if (hasNumber(finalScore) && sampleSize <= 5) {
    finalScore = clamp(finalScore, 20, 80);
    softened = true;
    caveats.push('Small family (≤5 variants); score clamped to 20–80 to avoid misleading 0/100 extremes.');
  }

  if (sampleSize === 1) {
    caveats.push('Only one variant in this comparison family; value score is a weak signal.');
  }

  return {
    score: round(finalScore),
    rawScore: round(rawScore),
    scoreType: 'same_model_family_value_v2',
    status: 'scored_same_model_family_v2',
    confidence: sampleSize <= 5 ? 'medium_low' : 'medium',
    evidence: {
      price,
      blendedUtility: round(utility.value),
      utilityParts: utility.parts,
      valuePerLakh: round(valuePerLakh, 3),
      comparisonScope: familyKey,
      sampleSize,
      scoreSoftened: softened,
      utilityVarianceDriver: 'feature_score_and_price_within_same_family_v2'
    },
    caveats
  };
};

const scoreRegretRisk = ({ profile, safetyScore, mileageScore, featureScore, valueScore, cityUseScore, practicalityScore }) => {
  let risk = 15;
  const reasons = [];
  const caveats = ['Regret risk v2 is evidence/trade-off based; buyer-contextual regret logic comes later.'];

  if (safetyScore.confidence === 'low') {
    risk += 18;
    reasons.push('Safety confidence is low.');
  }

  if (safetyScore.status === 'blocked_mixed_internal_ratings') {
    risk += 18;
    reasons.push('Crash-rating evidence is mixed/blocked.');
  }

  if (mileageScore.status && mileageScore.status.startsWith('not_scored')) {
    risk += 12;
    reasons.push('Mileage/running-cost evidence is incomplete or source-limited.');
  }

  if (
    hasNumber(featureScore.score) &&
    featureScore.score < 20 &&
    featureScore.featureDetectionDiagnostic?.suspiciouslyLowScore !== true
  ) {
    risk += 8;
    reasons.push('Feature richness is very low on selected high-value buyer-facing features.');
  }

  if (featureScore.featureDetectionDiagnostic?.suspiciouslyLowScore === true) {
    caveats.push('Feature score may be under-detected due to alias mismatch; regret risk avoids penalising low feature score strongly.');
  }

  if (hasNumber(valueScore.score) && valueScore.score < 25) {
    risk += valueScore.evidence?.sampleSize <= 5 ? 4 : 8;
    reasons.push('Same-model value score is weak.');
  }

  if (profile.fuelKey === 'cng' && !hasNumber(profile.practicalityBasis?.bootSpaceLitres)) {
    risk += 5;
    reasons.push('CNG boot-space impact is not quantified.');
  }

  const subtype = cityUseScore.evidence?.transmissionSubtype;
  if (subtype === 'amt') {
    risk += 3;
    reasons.push('AMT may feel less smooth in heavy stop-go city traffic.');
  }

  const riskScore = round(clamp(risk));
  const riskLevel = riskScore >= 70 ? 'high' : riskScore >= 40 ? 'medium' : 'low';

  return {
    riskScore,
    riskLevel,
    riskType: 'evidence_and_tradeoff_risk_v2',
    status: 'scored_v2',
    confidence: 'medium',
    reasons,
    caveats
  };
};

const buildDistributions = (profiles) => {
  const dist = {
    powerBhp: [],
    torqueNm: [],
    powerToWeight: [],
    mileageByFuel: {},
    bootSpaceLitres: [],
    widthMm: [],
    lengthMm: [],
    groundClearanceMm: [],
  };

  for (const profile of profiles) {
    if (hasNumber(profile.performanceBasis?.powerBhp)) dist.powerBhp.push(Number(profile.performanceBasis.powerBhp));
    if (hasNumber(profile.performanceBasis?.torqueNm)) dist.torqueNm.push(Number(profile.performanceBasis.torqueNm));
    if (hasNumber(profile.performanceBasis?.powerToWeight)) dist.powerToWeight.push(Number(profile.performanceBasis.powerToWeight));

    const mileage = getMileageValue(profile);
    if (hasNumber(mileage)) {
      const fuelKey = profile.fuelKey || 'unknown';
      if (!dist.mileageByFuel[fuelKey]) dist.mileageByFuel[fuelKey] = [];
      dist.mileageByFuel[fuelKey].push(Number(mileage));
    }

    if (hasNumber(profile.practicalityBasis?.bootSpaceLitres)) dist.bootSpaceLitres.push(Number(profile.practicalityBasis.bootSpaceLitres));
    if (hasNumber(profile.practicalityBasis?.widthMm)) dist.widthMm.push(Number(profile.practicalityBasis.widthMm));
    if (hasNumber(profile.practicalityBasis?.lengthMm)) dist.lengthMm.push(Number(profile.practicalityBasis.lengthMm));
    if (hasNumber(profile.practicalityBasis?.groundClearanceMm)) dist.groundClearanceMm.push(Number(profile.practicalityBasis.groundClearanceMm));
  }

  for (const key of ['powerBhp', 'torqueNm', 'powerToWeight', 'bootSpaceLitres', 'widthMm', 'lengthMm', 'groundClearanceMm']) {
    dist[key].sort((a, b) => a - b);
  }

  for (const key of Object.keys(dist.mileageByFuel)) {
    dist.mileageByFuel[key].sort((a, b) => a - b);
  }

  return dist;
};


const getFeatureMatrixProjection = () => {
  const projection = {
    _id: 0,
    modelKey: 1,
    variantKey: 1,
    activePricelistMatched: 1,
    discontinuedPricelistMatched: 1,
    featureKeys: 1,
    buildId: 1,
    updatedAt: 1,
  };

  const featureKeys = new Set();

  for (const feature of FEATURE_DEFS) {
    for (const alias of feature.aliases || []) {
      const key = normKey(alias);
      if (key) featureKeys.add(key);
    }

    for (const sourceKey of feature.sourceKeys || []) {
      const key = normKey(sourceKey);
      if (key) featureKeys.add(key);
    }
  }

  // Generic value used by positional parking-sensor detection.
  featureKeys.add('parking_sensors');

  for (const key of featureKeys) {
    projection[`featuresByKey.${key}`] = 1;
  }

  return projection;
};

const buildFeatureMatrixIndex = (featureDocs) => {
  const index = new Map();

  const preference = (doc) =>
    (doc.activePricelistMatched === true ? 100 : 0) +
    (doc.discontinuedPricelistMatched === true ? -50 : 0) +
    (safeArray(doc.featureKeys).length || 0) +
    Object.keys(doc.featuresByKey || {}).length;

  for (const doc of featureDocs) {
    const key = `${normKey(doc.modelKey)}__${normKey(doc.variantKey)}`;
    if (!key || key === '__') continue;

    const enriched = { ...doc, __joinKey: key };
    const existing = index.get(key);

    if (!existing || preference(enriched) > preference(existing)) {
      index.set(key, enriched);
    }
  }

  return index;
};

const buildBaseModules = ({ profile, matrixDoc, distributions }) => {
  const safetyScore = scoreSafety(profile);
  const featureScore = scoreFeatureRichness(profile, matrixDoc);
  const performanceScore = scorePerformance(profile, distributions);
  const mileageRunningCostScore = scoreMileage(profile, distributions);
  const practicalityScore = scorePracticality(profile, distributions);
  const cityUseScore = scoreCityUse(profile, matrixDoc, distributions);
  const highwayUseScore = scoreHighwayUse(profile, matrixDoc, performanceScore, safetyScore, mileageRunningCostScore);
  const premiumComfortScore = scorePremiumComfort(matrixDoc, practicalityScore, featureScore);

  return {
    safetyScore,
    featureScore,
    performanceScore,
    mileageRunningCostScore,
    practicalityScore,
    cityUseScore,
    highwayUseScore,
    premiumComfortScore
  };
};

const buildScoreDoc = ({ profile, matrixDoc, modules, valueScore, regretRisk }) => {
  const allModules = {
    ...modules,
    valueScore,
    regretRisk
  };

  const caveatCount = Object.values(allModules).reduce(
    (sum, entry) => sum + safeArray(entry?.caveats).length,
    0
  );

  const knownSourceLimitations = compact([
    modules.mileageRunningCostScore.status?.startsWith('not_scored') ? 'mileage' : null,
    modules.safetyScore.confidence === 'low' ? 'safety_confidence' : null,
    modules.featureScore.featureDetectionDiagnostic?.suspiciouslyLowScore ? 'feature_alias_diagnostic' : null,
    profile.fuelKey === 'cng' && !hasNumber(profile.practicalityBasis?.bootSpaceLitres) ? 'cng_boot_space' : null,
  ]);

  return {
    scoreProfileKey: profile.variantProfileKey,
    variantProfileKey: profile.variantProfileKey,
    variantFullName: profile.variantFullName,
    makeKey: profile.makeKey,
    modelKey: profile.modelKey,
    variantKey: profile.variantKey,
    fuelKey: profile.fuelKey,
    transmissionKey: profile.transmissionKey,
    transmissionSubtype: detectTransmissionSubtype(profile),
    fuelTransmissionFamilyKey: profile.fuelTransmissionFamilyKey,
    referenceExShowroomPrice: profile.referenceExShowroomPrice,
    priceSegment: priceSegmentFor(profile.referenceExShowroomPrice),

    buildVersion: BUILD_VERSION,
    formulaVersion: FORMULA_VERSION,
    featureScoreTaxonomyVersion: FEATURE_SCORE_TAXONOMY.taxonomyVersion,
    featureScoreTaxonomySourcePath: FEATURE_SCORE_TAXONOMY.sourcePath,
    builtAt: new Date(),

    ...allModules,

    scoreReadiness: {
      hasFeatureMatrixJoin: Boolean(matrixDoc),
      caveatCount,
      knownSourceLimitations,
      finalOverallScoreReady: false,
      finalOverallScoreReason:
        'Module scores are built first. Buyer-context weighting, similar-cars graph, resale/service evidence and upgrade ladder are required before final overall ranking.'
    }
  };
};

(async () => {
  const mongoUri = process.env.MONGODB_URI || process.env.MONGO_URI || process.env.DATABASE_URL;
  if (!mongoUri) throw new Error('Missing Mongo URI');

  await mongoose.connect(mongoUri, { serverSelectionTimeoutMS: 60000 });

  const db = mongoose.connection.db;
  const profilesCol = db.collection(PROFILE_COLLECTION);
  const matrixCol = db.collection(FEATURE_MATRIX_COLLECTION);
  const compactFeatureScoreCol = db.collection(FEATURE_SCORE_MATRIX_PROJECTION_COLLECTION);
  const targetCol = db.collection(TARGET_COLLECTION);

  console.error('[load] Loading decision profiles...');
  const allProfiles = await profilesCol.find({}, {
    projection: {
      _id: 0,
      variantProfileKey: 1,
      variantFullName: 1,
      makeKey: 1,
      modelKey: 1,
      variantKey: 1,
      fuelKey: 1,
      transmissionKey: 1,
      fuelTransmissionFamilyKey: 1,
      referenceExShowroomPrice: 1,
      safetyBasis: 1,
      performanceBasis: 1,
      mileageBasis: 1,
      practicalityBasis: 1,
      dataQuality: 1
    }
  }).toArray();

  const scoringProfiles = LIMIT > 0 ? allProfiles.slice(0, LIMIT) : allProfiles;
  console.error(`[load] Decision profiles=${allProfiles.length}; scoring=${scoringProfiles.length}`);

  console.error('[load] Loading feature score matrix projection docs...');

  let featureMatrixSource = FEATURE_SCORE_MATRIX_PROJECTION_COLLECTION;
  console.time('[load] feature_score_projection_find_toArray');

  let featureDocs = await compactFeatureScoreCol.find(
    { taxonomyVersion: FEATURE_SCORE_TAXONOMY.taxonomyVersion },
    {
      projection: {
        _id: 0,
        modelKey: 1,
        variantKey: 1,
        activePricelistMatched: 1,
        discontinuedPricelistMatched: 1,
        featureKeys: 1,
        featureKeysInMatrixCount: 1,
        featuresByKey: 1,
        buildId: 1,
        sourceBuildId: 1,
        taxonomyVersion: 1
      }
    }
  ).maxTimeMS(120000).toArray();

  console.timeEnd('[load] feature_score_projection_find_toArray');

  if (!featureDocs.length) {
    featureMatrixSource = FEATURE_MATRIX_COLLECTION;
    console.error(`[load] No compact projection docs found for taxonomy=${FEATURE_SCORE_TAXONOMY.taxonomyVersion}; falling back to ${FEATURE_MATRIX_COLLECTION}`);
    console.time('[load] feature_matrix_find_toArray');

    featureDocs = await matrixCol.find({}, {
      projection: getFeatureMatrixProjection()
    }).maxTimeMS(120000).toArray();

    console.timeEnd('[load] feature_matrix_find_toArray');
  }

  console.error(`[load] Feature matrix source=${featureMatrixSource}; docs=${featureDocs.length}`);

  console.time('[load] build_feature_matrix_index');
  const featureIndex = buildFeatureMatrixIndex(featureDocs);
  console.timeEnd('[load] build_feature_matrix_index');
  console.error(`[load] Feature matrix join index=${featureIndex.size}`);

  const distributions = buildDistributions(allProfiles);

  const baseRows = allProfiles.map((profile) => {
    const joinKey = `${normKey(profile.modelKey)}__${normKey(profile.variantKey)}`;
    const matrixDoc = featureIndex.get(joinKey) || null;
    const modules = buildBaseModules({ profile, matrixDoc, distributions });
    return { profile, matrixDoc, modules };
  });

  const familyDistributions = { valuePerLakhByFamily: {} };

  for (const row of baseRows) {
    const price = toNumber(row.profile.referenceExShowroomPrice);
    const utility = blendedUtility({
      safetyScore: row.modules.safetyScore,
      featureScore: row.modules.featureScore,
      performanceScore: row.modules.performanceScore,
      mileageScore: row.modules.mileageRunningCostScore,
      practicalityScore: row.modules.practicalityScore,
    });

    if (!hasNumber(price) || !hasNumber(utility.value)) continue;

    const familyKey = familyKeyFor(row.profile);
    const valuePerLakh = utility.value / (price / 100000);

    if (!familyDistributions.valuePerLakhByFamily[familyKey]) {
      familyDistributions.valuePerLakhByFamily[familyKey] = [];
    }

    familyDistributions.valuePerLakhByFamily[familyKey].push(valuePerLakh);
  }

  for (const key of Object.keys(familyDistributions.valuePerLakhByFamily)) {
    familyDistributions.valuePerLakhByFamily[key].sort((a, b) => a - b);
  }

  const baseByKey = new Map(baseRows.map((row) => [row.profile.variantProfileKey, row]));

  const docs = scoringProfiles.map((profile) => {
    const row = baseByKey.get(profile.variantProfileKey);
    const valueScore = scoreValue(profile, {
      safetyScore: row.modules.safetyScore,
      featureScore: row.modules.featureScore,
      performanceScore: row.modules.performanceScore,
      mileageScore: row.modules.mileageRunningCostScore,
      practicalityScore: row.modules.practicalityScore,
    }, familyDistributions);

    const regretRisk = scoreRegretRisk({
      profile,
      safetyScore: row.modules.safetyScore,
      mileageScore: row.modules.mileageRunningCostScore,
      featureScore: row.modules.featureScore,
      valueScore,
      cityUseScore: row.modules.cityUseScore,
      practicalityScore: row.modules.practicalityScore,
    });

    return buildScoreDoc({
      profile,
      matrixDoc: row.matrixDoc,
      modules: row.modules,
      valueScore,
      regretRisk
    });
  });

  const summary = {
    mode: WRITE ? 'WRITE' : 'DRY_RUN',
    reset: RESET,
    sourceProfiles: allProfiles.length,
    scoreDocs: docs.length,
    featureMatrixJoinCount: docs.filter(d => d.scoreReadiness.hasFeatureMatrixJoin).length,
    suspiciousFeatureScores: docs.filter(d => d.featureScore.featureDetectionDiagnostic?.suspiciouslyLowScore).length,
    safetyScoreScored: docs.filter(d => d.safetyScore.status).length,
    performanceScoreScored: docs.filter(d => d.performanceScore.status === 'scored').length,
    mileageScoreScored: docs.filter(d => d.mileageRunningCostScore.status === 'scored').length,
    practicalityScoreScored: docs.filter(d => d.practicalityScore.status === 'scored').length,
    valueScoreScored: docs.filter(d => d.valueScore.status === 'scored_same_model_family_v2').length,
    regretRiskScored: docs.filter(d => d.regretRisk.status === 'scored_v2').length,
    byPriceSegment: docs.reduce((acc, d) => {
      acc[d.priceSegment] = (acc[d.priceSegment] || 0) + 1;
      return acc;
    }, {}),
    byRegretRiskLevel: docs.reduce((acc, d) => {
      acc[d.regretRisk.riskLevel] = (acc[d.regretRisk.riskLevel] || 0) + 1;
      return acc;
    }, {}),
    notFinalOverallScore: true,
    featureMatrixSource,
    targetCollection: TARGET_COLLECTION
  };

  let writeResult = null;

  if (WRITE) {
    if (RESET) await targetCol.deleteMany({});

    if (docs.length) {
      const ops = docs.map((doc) => ({
        replaceOne: {
          filter: { scoreProfileKey: doc.scoreProfileKey },
          replacement: doc,
          upsert: true
        }
      }));

      const result = await targetCol.bulkWrite(ops, { ordered: false });
      writeResult = {
        matched: result.matchedCount || 0,
        modified: result.modifiedCount || 0,
        upserted: result.upsertedCount || 0
      };

      await targetCol.createIndex({ scoreProfileKey: 1 }, { unique: true, name: 'score_profile_key_unique' });
      await targetCol.createIndex({ variantProfileKey: 1 }, { name: 'variant_profile_key_idx' });
      await targetCol.createIndex({ makeKey: 1, modelKey: 1 }, { name: 'make_model_score_idx' });
      await targetCol.createIndex({ priceSegment: 1 }, { name: 'price_segment_idx' });
      await targetCol.createIndex({ fuelKey: 1, transmissionKey: 1 }, { name: 'fuel_transmission_score_idx' });
      await targetCol.createIndex({ 'safetyScore.score': -1 }, { name: 'safety_score_idx' });
      await targetCol.createIndex({ 'featureScore.score': -1 }, { name: 'feature_score_idx' });
      await targetCol.createIndex({ 'valueScore.score': -1 }, { name: 'value_score_idx' });
      await targetCol.createIndex({ 'regretRisk.riskScore': 1 }, { name: 'regret_risk_score_idx' });
    }
  }

  console.log(JSON.stringify({
    summary,
    writeResult,
    samples: docs
      .filter((d) =>
        ['baleno alpha', 'nexon', 'harrier', 'innova crysta', 'punch']
          .some((term) => String(d.variantFullName || '').toLowerCase().includes(term))
      )
      .slice(0, 30)
      .concat(docs.slice(0, 8))
      .map((d) => ({
      variantFullName: d.variantFullName,
      scoreProfileKey: d.scoreProfileKey,
      priceSegment: d.priceSegment,
      transmissionSubtype: d.transmissionSubtype,
      safetyScore: d.safetyScore,
      featureScore: d.featureScore,
      performanceScore: d.performanceScore,
      mileageRunningCostScore: d.mileageRunningCostScore,
      practicalityScore: d.practicalityScore,
      cityUseScore: d.cityUseScore,
      highwayUseScore: d.highwayUseScore,
      premiumComfortScore: d.premiumComfortScore,
      valueScore: d.valueScore,
      regretRisk: d.regretRisk,
      scoreReadiness: d.scoreReadiness
    }))
  }, null, 2));

  await mongoose.disconnect();
})().catch(async (error) => {
  console.error(error);
  try { await mongoose.disconnect(); } catch (_) {}
  process.exit(1);
});
