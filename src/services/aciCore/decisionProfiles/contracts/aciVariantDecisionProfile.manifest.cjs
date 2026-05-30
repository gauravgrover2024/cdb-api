const MANIFEST_VERSION = 'aci_variant_decision_profile_manifest_v1_2026_05_31';

const SOURCES = {
  INTERNAL_PRICE_ROWS: 'aci_vehicle_price_rows',
  INTERNAL_FEATURE_MATRIX: 'vehicle_variant_feature_matrix_v2',
  INTERNAL_MODEL_SUMMARY: 'aci_vehicle_model_summary',
  INTERNAL_VEHICLES: 'vehicles',
  INTERNAL_COLORS: 'vehicle_colors_v2',

  OFFICIAL_BRAND: 'official_brand_site_or_brochure',
  BHARAT_NCAP: 'bharat_ncap',
  GLOBAL_NCAP: 'global_ncap',
  CARDEKHO: 'cardekho',
  CARWALE: 'carwale',
  CARTRADE: 'cartrade',
  TRUSTED_REVIEWS: 'trusted_reviews',
  MANUAL_EDITORIAL: 'manual_editorial',
};

const PRIORITY = {
  CORE_V1: 'core_v1',
  GAP_V1: 'gap_v1',
  SCORING_V1: 'scoring_v1',
  UPGRADE_V1: 'upgrade_v1',
  MODEL_V1: 'model_v1',
  LATER: 'later',
};

const USAGE = {
  IDENTITY: 'identity',
  DISCOVERY: 'discovery',
  PRICING: 'pricing',
  FEATURE_ANSWER: 'feature_answer',
  SAFETY_SCORE: 'safety_score',
  PERFORMANCE_SCORE: 'performance_score',
  MILEAGE_SCORE: 'mileage_score',
  PRACTICALITY_SCORE: 'practicality_score',
  COMFORT_SCORE: 'comfort_score',
  VALUE_SCORE: 'value_score',
  REGRET_SCORE: 'regret_score',
  RECOMMENDATION_SCORE: 'recommendation_score',
  UPGRADE_LADDER: 'upgrade_ladder',
  BUYER_FIT: 'buyer_fit',
};

const FIELD_MANIFEST = [
  // Identity
  { path: 'variantProfileKey', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'make', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.IDENTITY, USAGE.DISCOVERY] },
  { path: 'makeKey', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'model', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.IDENTITY, USAGE.DISCOVERY] },
  { path: 'modelKey', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'variant', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.IDENTITY] },
  { path: 'variantKey', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'variantFullName', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'brandModelKey', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'lifecycleStatus', group: 'identity', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.DISCOVERY] },

  // Segment/body
  { path: 'bodyType', group: 'segmentBody', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_MODEL_SUMMARY, usage: [USAGE.DISCOVERY, USAGE.RECOMMENDATION_SCORE] },
  { path: 'bodyTypeKey', group: 'segmentBody', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.DISCOVERY] },
  { path: 'segmentKey', group: 'segmentBody', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.INTERNAL_MODEL_SUMMARY, usage: [USAGE.VALUE_SCORE, USAGE.RECOMMENDATION_SCORE] },
  { path: 'sizeClass', group: 'segmentBody', priority: PRIORITY.SCORING_V1, required: false, source: 'derived_or_external', usage: [USAGE.PRACTICALITY_SCORE, USAGE.BUYER_FIT] },
  { path: 'buyerSegment', group: 'segmentBody', priority: PRIORITY.SCORING_V1, required: false, source: 'derived_or_editorial', usage: [USAGE.RECOMMENDATION_SCORE] },

  // Variant normalisation
  { path: 'variantFamilyKey', group: 'variantNormalisation', priority: PRIORITY.CORE_V1, required: false, source: 'derived', usage: [USAGE.UPGRADE_LADDER] },
  { path: 'fuelTransmissionFamilyKey', group: 'variantNormalisation', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.UPGRADE_LADDER, USAGE.VALUE_SCORE] },
  { path: 'priceRank', group: 'variantNormalisation', priority: PRIORITY.UPGRADE_V1, required: false, source: 'derived', usage: [USAGE.UPGRADE_LADDER, USAGE.VALUE_SCORE] },
  { path: 'equipmentRank', group: 'variantNormalisation', priority: PRIORITY.UPGRADE_V1, required: false, source: 'derived', usage: [USAGE.UPGRADE_LADDER] },
  { path: 'isBaseVariant', group: 'variantNormalisation', priority: PRIORITY.UPGRADE_V1, required: false, source: 'derived', usage: [USAGE.REGRET_SCORE] },
  { path: 'isMidVariant', group: 'variantNormalisation', priority: PRIORITY.UPGRADE_V1, required: false, source: 'derived', usage: [USAGE.RECOMMENDATION_SCORE] },
  { path: 'isTopVariant', group: 'variantNormalisation', priority: PRIORITY.UPGRADE_V1, required: false, source: 'derived', usage: [USAGE.VALUE_SCORE] },
  { path: 'isCosmeticOnly', group: 'variantNormalisation', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.UPGRADE_LADDER] },
  { path: 'shouldSkipInUpgradeLadder', group: 'variantNormalisation', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.UPGRADE_LADDER] },

  // Price reference
  { path: 'referenceExShowroomPrice', group: 'priceReference', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.PRICING, USAGE.VALUE_SCORE] },
  { path: 'referenceOnRoadPrice', group: 'priceReference', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.PRICING, USAGE.VALUE_SCORE] },
  { path: 'referencePriceCitySlug', group: 'priceReference', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.PRICING] },
  { path: 'priceBandKey', group: 'priceReference', priority: PRIORITY.CORE_V1, required: false, source: 'derived', usage: [USAGE.DISCOVERY] },

  // Powertrain
  { path: 'fuel', group: 'powertrain', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.DISCOVERY, USAGE.MILEAGE_SCORE] },
  { path: 'fuelKey', group: 'powertrain', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.DISCOVERY, USAGE.MILEAGE_SCORE] },
  { path: 'transmission', group: 'powertrain', priority: PRIORITY.CORE_V1, required: true, source: SOURCES.INTERNAL_PRICE_ROWS, usage: [USAGE.DISCOVERY, USAGE.BUYER_FIT] },
  { path: 'transmissionKey', group: 'powertrain', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.DISCOVERY, USAGE.BUYER_FIT] },
  { path: 'gearbox', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'engineType', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'engineCc', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'turbo', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'drivetrain', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE, USAGE.BUYER_FIT] },
  { path: 'powerBhp', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'torqueNm', group: 'powertrain', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'batteryCapacityKwh', group: 'powertrain', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.OFFICIAL_BRAND, usage: [USAGE.MILEAGE_SCORE] },
  { path: 'claimedRangeKm', group: 'powertrain', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.OFFICIAL_BRAND, usage: [USAGE.MILEAGE_SCORE] },

  // Safety
  { path: 'safetyBasis.airbagsCount', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasSixAirbags', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasAbs', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasEbd', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasEsc', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasIsofix', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasTpms', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.hasAdas', group: 'safetyBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.globalNcapAdult', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.GLOBAL_NCAP, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.globalNcapChild', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.GLOBAL_NCAP, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.bharatNcapAdult', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.BHARAT_NCAP, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.bharatNcapChild', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.BHARAT_NCAP, usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.crashRatingSource', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: 'bncap_or_gncap_or_official', usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.crashRatingTestedVariant', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: 'bncap_or_gncap_or_official', usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.crashRatingAppliesToVariant', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: 'derived_with_evidence', usage: [USAGE.SAFETY_SCORE] },
  { path: 'safetyBasis.crashRatingApplicabilityCaveat', group: 'safetyBasis', priority: PRIORITY.GAP_V1, required: false, source: 'derived_with_evidence', usage: [USAGE.SAFETY_SCORE] },

  // Performance
  { path: 'performanceBasis.powerBhp', group: 'performanceBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'performanceBasis.torqueNm', group: 'performanceBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'performanceBasis.kerbWeightKg', group: 'performanceBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'performanceBasis.powerToWeight', group: 'performanceBasis', priority: PRIORITY.CORE_V1, required: false, source: 'derived', usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'performanceBasis.zeroToHundredClaimedSec', group: 'performanceBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.OFFICIAL_BRAND, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'performanceBasis.zeroToHundredTestedSec', group: 'performanceBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.TRUSTED_REVIEWS, usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'performanceBasis.cityDriveability20To80Sec', group: 'performanceBasis', priority: PRIORITY.LATER, required: false, source: SOURCES.TRUSTED_REVIEWS, usage: [USAGE.PERFORMANCE_SCORE] },

  // Mileage
  { path: 'mileageBasis.araiMileage', group: 'mileageBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.MILEAGE_SCORE] },
  { path: 'mileageBasis.fuelTankCapacity', group: 'mileageBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.MILEAGE_SCORE] },
  { path: 'mileageBasis.cityMileage', group: 'mileageBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.TRUSTED_REVIEWS, usage: [USAGE.MILEAGE_SCORE] },
  { path: 'mileageBasis.highwayMileage', group: 'mileageBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.TRUSTED_REVIEWS, usage: [USAGE.MILEAGE_SCORE] },
  { path: 'mileageBasis.estimatedRunningCostPerKm', group: 'mileageBasis', priority: PRIORITY.SCORING_V1, required: false, source: 'derived', usage: [USAGE.MILEAGE_SCORE, USAGE.VALUE_SCORE] },

  // Practicality
  { path: 'practicalityBasis.seatingCapacity', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'practicalityBasis.bootSpaceLitres', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'practicalityBasis.lengthMm', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'practicalityBasis.widthMm', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'practicalityBasis.heightMm', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'practicalityBasis.wheelbaseMm', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'practicalityBasis.groundClearanceMm', group: 'practicalityBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.PRACTICALITY_SCORE] },

  // Comfort
  { path: 'comfortBasis.automaticClimateControl', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.rearAcVents', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.ventilatedSeats', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.leatheretteSeats', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.sunroof', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.panoramicSunroof', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.wirelessCharging', group: 'comfortBasis', priority: PRIORITY.CORE_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.premiumAudio', group: 'comfortBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },
  { path: 'comfortBasis.ambientLighting', group: 'comfortBasis', priority: PRIORITY.GAP_V1, required: false, source: SOURCES.INTERNAL_FEATURE_MATRIX, usage: [USAGE.COMFORT_SCORE] },

  // Score fields: should remain null until scoring config is locked
  { path: 'scores.safetyScore', group: 'scores', priority: PRIORITY.SCORING_V1, required: false, source: 'score_config', usage: [USAGE.SAFETY_SCORE] },
  { path: 'scores.featureScore', group: 'scores', priority: PRIORITY.SCORING_V1, required: false, source: 'score_config', usage: [USAGE.FEATURE_ANSWER] },
  { path: 'scores.valueScore', group: 'scores', priority: PRIORITY.SCORING_V1, required: false, source: 'score_config', usage: [USAGE.VALUE_SCORE] },
  { path: 'scores.recommendationScore', group: 'scores', priority: PRIORITY.SCORING_V1, required: false, source: 'score_config', usage: [USAGE.RECOMMENDATION_SCORE] },
  { path: 'scores.regretRiskScore', group: 'scores', priority: PRIORITY.SCORING_V1, required: false, source: 'score_config', usage: [USAGE.REGRET_SCORE] },

  // Data quality
  { path: 'dataQuality.hasPrice', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.IDENTITY] },
  { path: 'dataQuality.hasFeatureMatrix', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.FEATURE_ANSWER] },
  { path: 'dataQuality.hasSafetyData', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.SAFETY_SCORE] },
  { path: 'dataQuality.hasPerformanceData', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.PERFORMANCE_SCORE] },
  { path: 'dataQuality.hasMileageData', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.MILEAGE_SCORE] },
  { path: 'dataQuality.hasDimensionsData', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.PRACTICALITY_SCORE] },
  { path: 'dataQuality.confidenceTier', group: 'dataQuality', priority: PRIORITY.CORE_V1, required: true, source: 'derived', usage: [USAGE.RECOMMENDATION_SCORE] },
];

const SCORE_REQUIREMENTS = {
  safetyScore: [
    'safetyBasis.hasSixAirbags',
    'safetyBasis.hasAbs',
    'safetyBasis.hasEsc',
    'safetyBasis.hasIsofix',
    'safetyBasis.globalNcapAdult|safetyBasis.bharatNcapAdult'
  ],
  performanceScore: [
    'performanceBasis.powerBhp',
    'performanceBasis.torqueNm',
    'performanceBasis.kerbWeightKg',
    'performanceBasis.powerToWeight'
  ],
  mileageScore: [
    'mileageBasis.araiMileage|mileageBasis.evClaimedRange',
    'mileageBasis.fuelTankCapacity|powertrain.batteryCapacityKwh'
  ],
  practicalityScore: [
    'practicalityBasis.seatingCapacity',
    'practicalityBasis.bootSpaceLitres',
    'practicalityBasis.lengthMm',
    'practicalityBasis.widthMm',
    'practicalityBasis.groundClearanceMm'
  ],
  comfortScore: [
    'comfortBasis.automaticClimateControl',
    'comfortBasis.rearAcVents',
    'comfortBasis.ventilatedSeats',
    'comfortBasis.sunroof',
    'comfortBasis.wirelessCharging'
  ],
  valueScore: [
    'referenceExShowroomPrice',
    'featureFlags',
    'priceRank',
    'equipmentRank'
  ],
  regretRiskScore: [
    'dataQuality.confidenceTier',
    'safetyBasis.hasEsc',
    'safetyBasis.hasSixAirbags',
    'variantRole.role',
    'featureFlags'
  ],
};

const EXTERNAL_SOURCE_QUEUE_RULES = [
  {
    gapKey: 'crash_rating',
    fields: [
      'safetyBasis.globalNcapAdult',
      'safetyBasis.globalNcapChild',
      'safetyBasis.bharatNcapAdult',
      'safetyBasis.bharatNcapChild',
      'safetyBasis.crashRatingSource',
      'safetyBasis.crashRatingTestedVariant',
      'safetyBasis.crashRatingAppliesToVariant',
      'safetyBasis.crashRatingApplicabilityCaveat',
    ],
    sourcePriority: [SOURCES.BHARAT_NCAP, SOURCES.GLOBAL_NCAP, SOURCES.OFFICIAL_BRAND],
    neededBeforeScoring: true,
  },
  {
    gapKey: 'feature_matrix_missing',
    fields: ['featureFlags', 'featureEvidence', 'powertrain', 'performanceBasis', 'mileageBasis', 'practicalityBasis'],
    sourcePriority: [SOURCES.OFFICIAL_BRAND, SOURCES.CARDEKHO, SOURCES.CARWALE, SOURCES.CARTRADE],
    neededBeforeScoring: true,
  },
  {
    gapKey: 'real_world_mileage',
    fields: ['mileageBasis.cityMileage', 'mileageBasis.highwayMileage'],
    sourcePriority: [SOURCES.TRUSTED_REVIEWS, SOURCES.CARDEKHO, SOURCES.CARWALE],
    neededBeforeScoring: false,
  },
  {
    gapKey: 'tested_performance',
    fields: ['performanceBasis.zeroToHundredClaimedSec', 'performanceBasis.zeroToHundredTestedSec'],
    sourcePriority: [SOURCES.OFFICIAL_BRAND, SOURCES.TRUSTED_REVIEWS],
    neededBeforeScoring: false,
  },
  {
    gapKey: 'ownership_tco',
    fields: ['ownershipBasis.warrantyYears', 'ownershipBasis.serviceCostEstimate', 'ownershipBasis.serviceIntervalKm'],
    sourcePriority: [SOURCES.OFFICIAL_BRAND, SOURCES.CARDEKHO, SOURCES.CARWALE],
    neededBeforeScoring: false,
  },
];

module.exports = {
  MANIFEST_VERSION,
  SOURCES,
  PRIORITY,
  USAGE,
  FIELD_MANIFEST,
  SCORE_REQUIREMENTS,
  EXTERNAL_SOURCE_QUEUE_RULES,
};
