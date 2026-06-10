const {
  slugKey,
  getFirst,
  toNumber,
  normalizeFuelKey,
  normalizeTransmissionKey,
  buildFuelTransmissionFamilyKey,
  makeBrandModelKey,
  makeVariantLookupKey,
  makeVariantLooseLookupKey,
} = require('./aciDecisionProfileKeys.cjs');

const SOURCE_VERSION = 'variant_decision_profile_v1_2026_05_30';

const SOURCE_COLLECTIONS = Object.freeze([
  process.env.ACI_PRICE_ROWS_COLLECTION || 'aci_vehicle_price_rows',
  process.env.ACI_FEATURE_MATRIX_COLLECTION || 'vehicle_variant_feature_matrix_v2',
  process.env.ACI_MODEL_SUMMARY_COLLECTION || 'aci_vehicle_model_summary',
  process.env.ACI_SOURCE_VEHICLE_COLLECTION || 'vehicles',
]);

const FEATURE_RULES = {
  sunroof: ['sunroof'],
  panoramicSunroof: ['panoramic_sunroof', 'panorama_sunroof'],
  adasPackage: ['adas_package', 'adas', 'advanced_driver_assistance_systems'],

  sixAirbags: ['six_airbags'],
  abs: ['anti_lock_braking_system_abs', 'abs'],
  ebd: ['electronic_brakeforce_distribution_ebd', 'ebd'],
  esc: ['electronic_stability_control_esc', 'esc', 'esp'],
  brakeAssist: ['brake_assist'],
  tractionControl: ['traction_control'],
  tpms: ['tyre_pressure_monitoring_system_tpms', 'tpms'],
  hillAssist: ['hill_assist', 'hill_hold'],
  hillDescentControl: ['hill_descent_control'],
  isofix: ['isofix_child_seat_mounts', 'isofix'],

  rearCamera: ['rear_camera'],
  camera360: ['camera_360', '360_degree_camera', 'surround_view_camera'],
  parkingSensorsFront: ['front_parking_sensors', 'front_parking_sensor', 'parking_sensors'],
  parkingSensorsRear: ['rear_parking_sensors', 'rear_parking_sensor', 'parking_sensors'],

  ventilatedSeats: ['ventilated_seats'],
  poweredDriverSeat: ['powered_driver_seat', 'electric_driver_seat', 'driver_electric_adjustable_seat'],
  poweredPassengerSeat: ['powered_passenger_seat', 'electric_passenger_seat', 'passenger_electric_adjustable_seat'],
  leatheretteSeats: ['leatherette_seats', 'leatherette_upholstery', 'upholstery'],
  automaticClimateControl: ['automatic_climate_control'],
  rearAcVents: ['rear_ac_vents'],

  cruiseControl: ['cruise_control'],
  wirelessCharging: ['wireless_charging', 'wireless_phone_charging'],
  touchscreen: ['touchscreen'],
  androidAuto: ['android_auto'],
  appleCarPlay: ['apple_carplay'],
  connectedCar: ['connected_car_features', 'connected_car'],
  digitalCluster: ['digital_cluster'],
  ledHeadlamps: ['led_headlamps', 'led_headlights'],
  alloyWheels: ['alloy_wheels'],
  paddleShifters: ['paddle_shifters'],
  driveModes: ['drive_modes', 'drive_mode']
};

const SCORE_FIELDS = [
  'safetyScore',
  'featureScore',
  'valueScore',
  'familyScore',
  'cityUseScore',
  'highwayScore',
  'performanceScore',
  'mileageScore',
  'comfortScore',
  'premiumScore',
  'featureToPriceRatio',
  'runningCostScore',
  'regretRiskScore',
  'recommendationScore',
];

const isObject = (value) =>
  value && typeof value === 'object' && !Array.isArray(value) && !(value instanceof Date);

const flattenRecord = (value, prefix = '', output = []) => {
  if (Array.isArray(value)) {
    value.forEach((item, index) => flattenRecord(item, `${prefix}.${index}`, output));
    return output;
  }

  if (isObject(value)) {
    Object.entries(value).forEach(([key, nestedValue]) => {
      flattenRecord(nestedValue, prefix ? `${prefix}.${key}` : key, output);
    });
    return output;
  }

  if (value !== undefined && value !== null && String(value).trim() !== '') {
    output.push({
      path: prefix,
      value,
      line: `${prefix}: ${String(value)}`,
    });
  }

  return output;
};

const normalizeSearchText = (value) =>
  String(value ?? '')
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();

const normalizeFeatureKey = (value) =>
  String(value ?? '')
    .trim()
    .toLowerCase()
    .replace(/&/g, ' and ')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '');

const deriveAvailabilityFromValue = (value, availabilityStatus) => {
  const status = normalizeFeatureKey(availabilityStatus);
  if (status === 'available' || status === 'standard' || status === 'yes') return true;
  if (status === 'not_available' || status === 'unavailable' || status === 'no') return false;

  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value > 0;

  const text = normalizeSearchText(value);
  if (!text) return null;

  if (
    text === 'yes' ||
    text === 'available' ||
    text === 'standard' ||
    text === 'front' ||
    text === 'rear' ||
    text === 'front and rear' ||
    text === 'front rear' ||
    text.includes('front') ||
    text.includes('rear')
  ) {
    return true;
  }

  if (
    text === 'no' ||
    text === 'false' ||
    text.includes('not available') ||
    text.includes('not offered') ||
    text.includes('unavailable')
  ) {
    return false;
  }

  return true;
};

const adjustDirectionalParkingAvailability = (featureKey, entry, available) => {
  if (!entry || !String(entry.displayName || '').toLowerCase().includes('parking')) return available;

  const valueText = normalizeSearchText(entry.value);
  if (featureKey === 'parkingSensorsFront') {
    if (valueText.includes('front')) return available;
    if (valueText.includes('rear') && !valueText.includes('front')) return false;
  }

  if (featureKey === 'parkingSensorsRear') {
    if (valueText.includes('rear')) return available;
    if (valueText.includes('front') && !valueText.includes('rear')) return false;
  }

  return available;
};

const adjustUpholsteryAvailability = (featureKey, entry, available) => {
  if (featureKey !== 'leatheretteSeats' || !entry) return available;

  const valueText = normalizeSearchText(entry.value);
  if (!valueText) return null;

  if (
    valueText.includes('leatherette') ||
    valueText.includes('leather') ||
    valueText.includes('art leather') ||
    valueText.includes('synthetic leather')
  ) {
    return true;
  }

  if (
    valueText.includes('fabric') ||
    valueText.includes('cloth') ||
    valueText.includes('woven') ||
    valueText.includes('not available')
  ) {
    return false;
  }

  return null;
};

const getFeatureEntryByKey = (featureDoc, key) => {
  const featuresByKey = featureDoc && featureDoc.featuresByKey;
  if (!featuresByKey || typeof featuresByKey !== 'object') return null;
  return featuresByKey[normalizeFeatureKey(key)] || null;
};

const getFeatureValue = (featureDoc, keys) => {
  for (const key of keys) {
    const entry = getFeatureEntryByKey(featureDoc, key);
    if (!entry) continue;
    if (entry.value !== undefined && entry.value !== null && String(entry.value).trim() !== '') return entry.value;
  }
  return null;
};

const getFeatureNumber = (featureDoc, keys) => {
  const value = getFeatureValue(featureDoc, keys);
  return toNumber(value);
};

const getFeatureAvailability = (featureDoc, keys) => {
  for (const key of keys) {
    const entry = getFeatureEntryByKey(featureDoc, key);
    if (!entry) continue;
    return typeof entry.available === 'boolean'
      ? entry.available
      : deriveAvailabilityFromValue(entry.value, entry.availabilityStatus);
  }
  return null;
};

const getFeatureEntry = (featureDoc, aliases) => {
  const featuresByKey = featureDoc && featureDoc.featuresByKey;
  if (!featuresByKey || typeof featuresByKey !== 'object') return { key: null, entry: null };

  for (const alias of aliases) {
    const normalizedAlias = normalizeFeatureKey(alias);
    if (Object.prototype.hasOwnProperty.call(featuresByKey, normalizedAlias)) {
      return { key: normalizedAlias, entry: featuresByKey[normalizedAlias] };
    }
  }

  return { key: null, entry: null };
};

const detectFeature = (featureDoc, featureKey, aliases) => {
  const { key, entry } = getFeatureEntry(featureDoc, aliases);

  if (!entry) {
    return {
      available: null,
      value: null,
      displayName: null,
      sourceKey: null,
      groupKey: null,
      confidence: 'low',
      conflictStatus: 'missing',
    };
  }

  let available =
    typeof entry.available === 'boolean'
      ? entry.available
      : deriveAvailabilityFromValue(entry.value, entry.availabilityStatus);

  available = adjustDirectionalParkingAvailability(featureKey, entry, available);
  available = adjustUpholsteryAvailability(featureKey, entry, available);

  const conflictStatus = entry.conflictStatus || 'clean';
  const isConflict = conflictStatus && conflictStatus !== 'clean' && conflictStatus !== 'none';

  return {
    available,
    value: entry.value ?? null,
    displayName: entry.displayName || key,
    sourceKey: `featuresByKey.${key}`,
    groupKey: entry.groupKey || null,
    confidence: typeof entry.available === 'boolean' ? 'high' : 'medium',
    conflictStatus: isConflict ? conflictStatus : 'none',
  };
};

const buildFeatureEvidence = (featureDoc) => {
  const featureEvidence = {};
  const featureFlags = {};
  const missingFeatureKeys = [];
  const conflictedFeatureKeys = [];

  Object.entries(FEATURE_RULES).forEach(([featureKey, aliases]) => {
    const evidence = detectFeature(featureDoc, featureKey, aliases);
    featureEvidence[featureKey] = evidence;
    featureFlags[featureKey] = evidence.available;

    if (evidence.available === null) missingFeatureKeys.push(featureKey);
    if (evidence.conflictStatus && evidence.conflictStatus !== 'none' && evidence.conflictStatus !== 'missing') {
      conflictedFeatureKeys.push(featureKey);
    }
  });

  return {
    featureFlags,
    featureEvidence,
    missingFeatureKeys,
    conflictedFeatureKeys,
  };
};

const buildNullScores = () =>
  SCORE_FIELDS.reduce((acc, key) => {
    acc[key] = null;
    return acc;
  }, {});

const priceBandKey = (price) => {
  if (!price) return null;
  if (price < 500000) return 'under_5l';
  if (price < 1000000) return '5_10l';
  if (price < 1500000) return '10_15l';
  if (price < 2000000) return '15_20l';
  if (price < 3000000) return '20_30l';
  if (price < 5000000) return '30_50l';
  return 'above_50l';
};

const inferVariantFamily = (variant) => {
  const clean = String(variant || '').trim();
  if (!clean) return { variantFamilyKey: null, variantFamilyLabel: null };
  const firstMeaningful = clean
    .replace(/\bdual tone\b/gi, '')
    .replace(/\bdt\b/gi, '')
    .trim()
    .split(/\s|\/|\+|\-/)[0];

  return {
    variantFamilyKey: slugKey(firstMeaningful),
    variantFamilyLabel: firstMeaningful || null,
  };
};

const buildVariantDecisionProfileFromSources = ({ priceRow = {}, featureDoc = null, modelSummary = null }) => {
  const now = new Date();

  const make = getFirst(priceRow, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']) ||
    getFirst(featureDoc, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']) ||
    getFirst(modelSummary, ['make', 'brand', 'makeName', 'brandName', 'manufacturer']);

  const makeKey = getFirst(priceRow, ['makeKey', 'brandKey']) || slugKey(make);

  const model = getFirst(priceRow, ['model', 'modelName', 'fullModel', 'nameplate']) ||
    getFirst(featureDoc, ['model', 'modelName', 'fullModel', 'nameplate']) ||
    getFirst(modelSummary, ['model', 'modelName', 'fullModel', 'nameplate']);

  const modelKey = getFirst(priceRow, ['modelKey']) || slugKey(model);

  const variant = getFirst(priceRow, ['variant', 'variantName', 'variantLabel', 'trim', 'version']) ||
    getFirst(featureDoc, ['variant', 'variantName', 'variantLabel', 'trim', 'version']);

  const variantKey = getFirst(priceRow, ['variantKey']) ||
    getFirst(featureDoc, ['variantKey']) ||
    slugKey(variant);

  const fullModel = getFirst(priceRow, ['fullModel']) || [make, model].filter(Boolean).join(' ');
  const variantFullName =
    getFirst(priceRow, ['variantFullName', 'fullVariantName']) ||
    [make, model, variant].filter(Boolean).join(' ');

  const fuel = getFirst(priceRow, ['fuel', 'fuelType', 'fuel_type']);
  const transmission = getFirst(priceRow, ['transmission', 'transmissionType']);
  const gearbox = getFirst(priceRow, ['gearbox']);

  const fuelKey = getFirst(priceRow, ['fuelKey']) || normalizeFuelKey(fuel);
  const transmissionKey = getFirst(priceRow, ['transmissionKey']) || normalizeTransmissionKey(transmission || gearbox);

  const fuelTransmissionFamilyKey = buildFuelTransmissionFamilyKey({
    fuel,
    fuelKey,
    transmission,
    transmissionKey,
    gearbox,
  });

  const brandModelKey = makeBrandModelKey({ make, makeKey, model, modelKey });
  const variantProfileKey = `${brandModelKey}__${variantKey}__${fuelTransmissionFamilyKey || 'unknown_powertrain'}`;

  const referenceExShowroomPrice = toNumber(
    getFirst(priceRow, ['exShowroomPrice', 'ex_showroom_price', 'price', 'priceValue'])
  );

  const referenceOnRoadPrice = toNumber(
    getFirst(priceRow, ['onRoadPrice', 'on_road_price', 'onRoadPriceWithoutOptional'])
  );

  const featureBuild = buildFeatureEvidence(featureDoc);
  const { featureFlags } = featureBuild;

  const powerBhp =
    toNumber(getFirst(priceRow, ['powerBhp', 'maxPowerBhp', 'power', 'engine.powerBhp'])) ||
    getFeatureNumber(featureDoc, ['max_power', 'power']);

  const torqueNm =
    toNumber(getFirst(priceRow, ['torqueNm', 'maxTorqueNm', 'torque', 'engine.torqueNm'])) ||
    getFeatureNumber(featureDoc, ['max_torque', 'torque']);

  const kerbWeightKg =
    toNumber(getFirst(priceRow, ['kerbWeightKg', 'kerbWeight', 'weight.kerbWeightKg'])) ||
    getFeatureNumber(featureDoc, ['kerb_weight']);

  const araiMileage =
    toNumber(getFirst(priceRow, ['araiMileage', 'mileage', 'claimedMileage'])) ||
    getFeatureNumber(featureDoc, ['arai_mileage', 'petrol_mileage_arai', 'diesel_mileage_arai', 'cng_mileage_arai']);

  const batteryCapacityKwh =
    toNumber(getFirst(priceRow, ['batteryCapacityKwh', 'battery.capacityKwh'])) ||
    getFeatureNumber(featureDoc, ['battery_capacity']);

  const claimedRangeKm =
    toNumber(getFirst(priceRow, ['claimedRangeKm', 'rangeKm', 'evClaimedRange'])) ||
    getFeatureNumber(featureDoc, ['range', 'claimed_range']);

  const variantFamily = inferVariantFamily(variant);
  const variantText = String(variant || '');
  const isDualToneOnly = /\bdual tone\b|\bdt\b/i.test(variantText);
  const isDarkEdition = /\bdark\b|black edition/i.test(variantText);
  const isAdventureEdition = /\badventure\b/i.test(variantText);
  const isPerformanceEdition = /\bn line\b|\brs\b|\bgt\b|\bperformance\b/i.test(variantText);
  const isCosmeticOnly = isDualToneOnly || isDarkEdition;

  const hasFeatureMatrix = Boolean(featureDoc);
  const hasPrice = Boolean(referenceExShowroomPrice || referenceOnRoadPrice);
  const hasPerformanceData = Boolean(powerBhp || torqueNm);
  const hasMileageData = Boolean(araiMileage || claimedRangeKm);
  const hasDimensionsData = Boolean(
    getFirst(modelSummary, ['lengthMm', 'widthMm', 'heightMm', 'wheelbaseMm', 'groundClearanceMm']) ||
      getFeatureNumber(featureDoc, ['length']) ||
      getFeatureNumber(featureDoc, ['width']) ||
      getFeatureNumber(featureDoc, ['height']) ||
      getFeatureNumber(featureDoc, ['wheel_base']) ||
      getFeatureNumber(featureDoc, ['ground_clearance_unladen', 'ground_clearance'])
  );

  const missingCriticalFields = [];
  if (!make) missingCriticalFields.push('make');
  if (!model) missingCriticalFields.push('model');
  if (!variant) missingCriticalFields.push('variant');
  if (!hasPrice) missingCriticalFields.push('price');
  if (!hasFeatureMatrix) missingCriticalFields.push('feature_matrix');

  const confidenceTier =
    missingCriticalFields.length === 0 && hasPerformanceData && hasMileageData
      ? 'high'
      : missingCriticalFields.length <= 1
        ? 'medium'
        : 'low';

  return {
    variantProfileKey,

    make,
    makeKey,
    model,
    modelKey,
    fullModel,
    variant,
    variantKey,
    variantFullName,
    brandModelKey,
    sourceVehicleId: getFirst(priceRow, ['sourceVehicleId', 'vehicleId', '_id']),
    lifecycleStatus: getFirst(priceRow, ['lifecycleStatus', 'status']) || 'active_new_car',
    dataStatus: confidenceTier === 'low' ? 'needs_review' : 'partial',
    sourceVersion: SOURCE_VERSION,
    buildVersion: SOURCE_VERSION,
    builtAt: new Date().toISOString(),
    sourceCollections: [...SOURCE_COLLECTIONS],
    createdAt: now,
    updatedAt: now,

    bodyType: getFirst(modelSummary, ['bodyType', 'body_style']) || getFirst(priceRow, ['bodyType']),
    bodyTypeKey: slugKey(getFirst(modelSummary, ['bodyType', 'body_style']) || getFirst(priceRow, ['bodyType'])),
    segmentKey: getFirst(modelSummary, ['segmentKey']) || slugKey(getFirst(modelSummary, ['segment', 'segmentLabel'])),
    segmentLabel: getFirst(modelSummary, ['segmentLabel', 'segment']),
    sizeClass: getFirst(modelSummary, ['sizeClass']),
    buyerSegment: getFirst(modelSummary, ['buyerSegment']),
    retailCategory: getFirst(modelSummary, ['retailCategory']) || 'private_car',
    isRetailMainstream: getFirst(modelSummary, ['isRetailMainstream']) ?? null,
    isCommercialOrFleetFocused: getFirst(modelSummary, ['isCommercialOrFleetFocused']) ?? null,

    ...variantFamily,
    equipmentRank: null,
    priceRank: null,
    normalizedVariantRank: null,
    fuelTransmissionFamilyKey,
    isBaseVariant: null,
    isMidVariant: null,
    isTopVariant: null,
    isDualToneOnly,
    isCosmeticOnly,
    isSpecialEdition: /\bspecial\b|edition/i.test(variantText),
    isDarkEdition,
    isAdventureEdition,
    isPerformanceEdition,
    parentEquipmentVariantKey: null,
    equivalentVariantKeys: [],
    shouldSkipInUpgradeLadder: isCosmeticOnly,

    fuel,
    fuelKey,
    transmission,
    transmissionKey,
    gearbox,
    engineType: getFirst(priceRow, ['engineType', 'engine.type']) || getFeatureValue(featureDoc, ['engine_type']),
    engineCc:
      toNumber(getFirst(priceRow, ['engineCc', 'engine.cc'])) ||
      getFeatureNumber(featureDoc, ['displacement', 'engine_displacement']),
    cylinders:
      toNumber(getFirst(priceRow, ['cylinders', 'engine.cylinders'])) ||
      getFeatureNumber(featureDoc, ['number_of_cylinders']),
    turbo: getFirst(priceRow, ['turbo', 'engine.turbo']) ?? getFeatureAvailability(featureDoc, ['turbo_charger']),
    supercharged: getFirst(priceRow, ['supercharged', 'engine.supercharged']) ?? getFeatureAvailability(featureDoc, ['super_charger']),
    hybridType: getFirst(priceRow, ['hybridType']),
    drivetrain: getFirst(priceRow, ['drivetrain', 'driveTrain']) || getFeatureValue(featureDoc, ['drive_type']),
    powerBhp,
    torqueNm,
    batteryCapacityKwh,
    motorPowerBhp: toNumber(getFirst(priceRow, ['motorPowerBhp'])),
    motorTorqueNm: toNumber(getFirst(priceRow, ['motorTorqueNm'])),
    claimedRangeKm,

    referenceExShowroomPrice,
    referenceOnRoadPrice,
    referencePriceCitySlug: getFirst(priceRow, ['citySlug']) || null,
    referencePriceUpdatedAt: getFirst(priceRow, ['priceUpdatedAt', 'updatedAt']) || null,
    priceBandKey: priceBandKey(referenceExShowroomPrice),
    pricePositionInModel: null,
    pricePositionInSegment: null,

    ...featureBuild,

    safetyBasis: {
      airbagsCount: featureFlags.sixAirbags === true ? 6 : null,
      hasSixAirbags: featureFlags.sixAirbags,
      hasAbs: featureFlags.abs,
      hasEbd: featureFlags.ebd,
      hasEsc: featureFlags.esc,
      hasBrakeAssist: featureFlags.brakeAssist,
      hasTractionControl: featureFlags.tractionControl,
      hasIsofix: featureFlags.isofix,
      hasTpms: featureFlags.tpms,
      hasHillAssist: featureFlags.hillAssist,
      hasHillDescentControl: featureFlags.hillDescentControl,
      hasAdas: featureFlags.adasPackage,
      hasRearCamera: featureFlags.rearCamera,
      hasCamera360: featureFlags.camera360,
      globalNcapAdult: getFirst(modelSummary, ['globalNcapAdult']),
      globalNcapChild: getFirst(modelSummary, ['globalNcapChild']),
      bharatNcapAdult: getFirst(modelSummary, ['bharatNcapAdult']),
      bharatNcapChild: getFirst(modelSummary, ['bharatNcapChild']),
      crashRatingSource: getFirst(modelSummary, ['crashRatingSource']),
      crashRatingTestedVariant: getFirst(modelSummary, ['crashRatingTestedVariant']),
      crashRatingAppliesToVariant: null,
      crashRatingAppliesToAllVariants: null,
      crashRatingApplicabilityCaveat: null,
    },
    safetyTier: null,
    safetyStrengths: [],
    safetyMissingCriticals: [],
    safetyCaveats: [],

    performanceBasis: {
      powerBhp,
      torqueNm,
      kerbWeightKg,
      powerToWeight: powerBhp && kerbWeightKg ? Number((powerBhp / (kerbWeightKg / 1000)).toFixed(2)) : null,
      torqueToWeight: torqueNm && kerbWeightKg ? Number((torqueNm / (kerbWeightKg / 1000)).toFixed(2)) : null,
      zeroToHundredClaimedSec: toNumber(getFirst(priceRow, ['zeroToHundredClaimedSec'])),
      zeroToHundredTestedSec: toNumber(getFirst(priceRow, ['zeroToHundredTestedSec'])),
      cityDriveability20To80Sec: toNumber(getFirst(priceRow, ['cityDriveability20To80Sec'])),
      topSpeedKmph: toNumber(getFirst(priceRow, ['topSpeedKmph'])),
      turbo: getFirst(priceRow, ['turbo', 'engine.turbo']) ?? getFeatureAvailability(featureDoc, ['turbo_charger']),
      drivetrain: getFirst(priceRow, ['drivetrain', 'driveTrain']) || getFeatureValue(featureDoc, ['drive_type']),
      transmissionType: transmission,
    },
    performanceTier: null,
    performanceStrengths: [],
    performanceCaveats: [],

    mileageBasis: {
      araiMileage,
      cityMileage: toNumber(getFirst(priceRow, ['cityMileage'])),
      highwayMileage: toNumber(getFirst(priceRow, ['highwayMileage'])),
      fuelTankCapacity:
        toNumber(getFirst(priceRow, ['fuelTankCapacity'])) ||
        getFeatureNumber(featureDoc, ['fuel_tank_capacity', 'petrol_fuel_tank_capacity', 'diesel_fuel_tank_capacity']),
      evClaimedRange: claimedRangeKm,
      batteryCapacityKwh,
      estimatedRunningCostPerKm: null,
      runningCostConfidence: null,
    },
    mileageTier: null,
    runningCostTier: null,
    mileageCaveats: [],

    practicalityBasis: {
      seatingCapacity:
        toNumber(getFirst(modelSummary, ['seatingCapacity']) || getFirst(priceRow, ['seatingCapacity'])) ||
        getFeatureNumber(featureDoc, ['seating_capacity']),
      bootSpaceLitres:
        toNumber(getFirst(modelSummary, ['bootSpaceLitres']) || getFirst(priceRow, ['bootSpaceLitres'])) ||
        getFeatureNumber(featureDoc, ['boot_space']),
      lengthMm: toNumber(getFirst(modelSummary, ['lengthMm'])) || getFeatureNumber(featureDoc, ['length']),
      widthMm: toNumber(getFirst(modelSummary, ['widthMm'])) || getFeatureNumber(featureDoc, ['width']),
      heightMm: toNumber(getFirst(modelSummary, ['heightMm'])) || getFeatureNumber(featureDoc, ['height']),
      wheelbaseMm: toNumber(getFirst(modelSummary, ['wheelbaseMm'])) || getFeatureNumber(featureDoc, ['wheel_base']),
      groundClearanceMm:
        toNumber(getFirst(modelSummary, ['groundClearanceMm'])) ||
        getFeatureNumber(featureDoc, ['ground_clearance_unladen', 'ground_clearance']),
      rearAcVents: featureFlags.rearAcVents,
      isofix: featureFlags.isofix,
      foldableRearSeat: null,
      parkingEaseSignal: null,
      ingressEgressSignal: null,
      rearSeatComfortSignal: null,
    },
    practicalityTier: null,
    practicalityStrengths: [],
    practicalityCaveats: [],

    comfortBasis: {
      automaticClimateControl: featureFlags.automaticClimateControl,
      rearAcVents: featureFlags.rearAcVents,
      ventilatedSeats: featureFlags.ventilatedSeats,
      poweredSeats: featureFlags.poweredDriverSeat || featureFlags.poweredPassengerSeat,
      leatheretteSeats: featureFlags.leatheretteSeats,
      sunroof: featureFlags.sunroof,
      panoramicSunroof: featureFlags.panoramicSunroof,
      cruiseControl: featureFlags.cruiseControl,
      wirelessCharging: featureFlags.wirelessCharging,
      connectedCar: featureFlags.connectedCar,
      premiumAudio: null,
      ambientLighting: null,
    },
    comfortTier: null,
    premiumFeelTier: null,
    comfortStrengths: [],
    comfortCaveats: [],

    scores: buildNullScores(),
    scoreVersion: null,
    scoreComputedAt: null,
    scoreConfidence: {
      safety: 'not_scored',
      features: 'not_scored',
      performance: 'not_scored',
      mileage: 'not_scored',
      practicality: 'not_scored',
      value: 'not_scored',
    },

    scoreEvidence: {
      safetyReasons: [],
      featureReasons: [],
      valueReasons: [],
      familyReasons: [],
      cityUseReasons: [],
      highwayReasons: [],
      performanceReasons: [],
      mileageReasons: [],
      comfortReasons: [],
      caveats: [],
      missingDataWarnings: missingCriticalFields.map((field) => `Missing ${field}`),
    },

    variantRole: {
      role: null,
      idealFor: [],
      skipIf: [],
      upgradeVerdict: null,
      regretWarning: null,
      isRecommendedVariant: null,
      isBestValueVariant: null,
      isSafetyPickVariant: null,
      isFeatureLoadedVariant: null,
      isPerformancePickVariant: null,
    },

    dataQuality: {
      hasPrice,
      hasFeatureMatrix,
      hasSafetyData: hasFeatureMatrix && [
        featureFlags.sixAirbags,
        featureFlags.abs,
        featureFlags.esc,
        featureFlags.isofix,
      ].some((value) => value !== null),
      hasPerformanceData,
      hasMileageData,
      hasDimensionsData,
      conflictCount: featureBuild.conflictedFeatureKeys.length,
      missingCriticalFields,
      confidenceTier,
      needsReview: confidenceTier === 'low',
    },

    lookupKeys: {
      exact: makeVariantLookupKey({ make, makeKey, model, modelKey, variant, variantKey, fuel, fuelKey, transmission, transmissionKey }),
      loose: makeVariantLooseLookupKey({ make, makeKey, model, modelKey, variant, variantKey }),
    },
  };
};

module.exports = {
  SOURCE_VERSION,
  SOURCE_COLLECTIONS,
  buildVariantDecisionProfileFromSources,
  makeVariantLookupKey,
  makeVariantLooseLookupKey,
};
