import { firstMeaningful } from "./aiAgent.normalizers.js";

const asText = (value) =>
  String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();

const normalized = (value) => asText(value).toLowerCase();

const toNumber = (value) => {
  const text = asText(value);
  if (!text) return null;
  const match = text.match(/-?\d+(?:\.\d+)?/);
  return match ? Number(match[0]) : null;
};

const isNegative = (value) => {
  const text = normalized(value);
  return !text || ["not available", "no", "0", "na", "n/a", "false", "none"].includes(text);
};

const isTruthyFeature = (value) => {
  const text = normalized(value);
  if (!text) return false;
  if (isNegative(text)) return false;
  if (["yes", "true", "available", "standard", "present", "1"].includes(text)) return true;
  if (text.includes("not available") || text.includes("not offered") || text.includes("unavailable")) {
    return false;
  }
  return true;
};

const findFeatureValue = (features = {}, patterns = []) => {
  const entries = Object.entries(features || {});
  for (const [key, value] of entries) {
    const keyNorm = normalized(key);
    if (patterns.some((pattern) => keyNorm.includes(pattern))) {
      return value;
    }
  }
  return "";
};

const hasAnyAdasFeature = (features = {}) => {
  const adasNeedles = [
    "automatic emergency braking",
    "lane departure warning",
    "lane keep assist",
    "driver attention warning",
    "adaptive cruise control",
    "traffic sign recognition",
    "adaptive high beam assist",
  ];
  return adasNeedles.some((needle) =>
    isTruthyFeature(findFeatureValue(features, [needle])),
  );
};

const automaticFromContext = ({ transmissionType, gearbox, variant }) => {
  const transmissionText = normalized(transmissionType);
  const gearboxText = normalized(gearbox);
  const variantText = normalized(variant);

  if (transmissionText.includes("automatic")) return true;
  if (/\b(amt|cvt|dct|ivt|torque converter|automatic|at)\b/i.test(gearboxText)) {
    return true;
  }

  // Only parse AT tokens in variant text when tokenized, avoid "at 9 percent" pattern.
  if (/\b(amt|cvt|dct|ivt|automatic)\b/i.test(variantText)) return true;
  if (/\bat\b/i.test(variantText) && /\b(mt|manual|petrol|diesel|turbo|dct|ivt|cvt|amt|at)\b/i.test(variantText)) {
    return true;
  }

  return false;
};

export const buildFeatureProfile = (featureDoc = {}) => {
  const features = featureDoc.features || {};

  const bodyType = firstMeaningful(
    featureDoc.body_type_bucket,
    findFeatureValue(features, ["body type"]),
    featureDoc.segment,
    featureDoc.category,
    "",
  );

  const seatingCapacity = toNumber(
    firstMeaningful(
      featureDoc.seating_capacity,
      findFeatureValue(features, ["seating capacity"]),
      "",
    ),
  );

  const transmissionType = asText(
    findFeatureValue(features, ["transmission type", "transmission"]),
  );
  const gearbox = asText(findFeatureValue(features, ["gearbox"]));
  const variant = asText(featureDoc.variant);

  const sunroofValue = asText(
    findFeatureValue(features, ["sunroof", "voice assisted sunroof"]),
  );

  const profile = {
    brand: asText(firstMeaningful(featureDoc.brand, featureDoc.make)),
    model: asText(featureDoc.model),
    variant,
    bodyType: asText(bodyType),
    seatingCapacity,
    fuelType: asText(findFeatureValue(features, ["fuel type"])),
    transmissionType,
    gearbox,
    isAutomatic: automaticFromContext({ transmissionType, gearbox, variant }),
    automaticType: asText(
      firstMeaningful(
        /\b(amt|cvt|dct|ivt|at|automatic)\b/i.exec(`${gearbox} ${transmissionType} ${variant}`)?.[1],
        "",
      ),
    ).toUpperCase(),
    mileage: toNumber(
      firstMeaningful(
        findFeatureValue(features, ["petrol mileage arai", "diesel mileage arai", "mileage"]),
      ),
    ),
    powerBhP: toNumber(findFeatureValue(features, ["max power", "power"])),
    torqueNm: toNumber(findFeatureValue(features, ["max torque", "torque"])),
    displacementCc: toNumber(
      findFeatureValue(features, ["displacement", "engine cc", "engine"]),
    ),
    groundClearanceMm: toNumber(
      findFeatureValue(features, ["ground clearance unladen", "ground clearance"]),
    ),
    bootSpaceLitres: toNumber(findFeatureValue(features, ["boot space"])),
    lengthMm: toNumber(findFeatureValue(features, ["length"])),
    widthMm: toNumber(findFeatureValue(features, ["width"])),
    heightMm: toNumber(findFeatureValue(features, ["height"])),
    wheelbaseMm: toNumber(findFeatureValue(features, ["wheel base", "wheelbase"])),
    airbags: toNumber(findFeatureValue(features, ["no. of airbags", "number of airbags", "airbags"])),
    hasAbs: isTruthyFeature(findFeatureValue(features, ["abs"])),
    hasEbd: isTruthyFeature(findFeatureValue(features, ["ebd"])),
    hasEsc: isTruthyFeature(findFeatureValue(features, ["esc"])),
    hasTpms: isTruthyFeature(findFeatureValue(features, ["tpms", "tyre pressure monitor"])),
    hasIsofix: isTruthyFeature(findFeatureValue(features, ["isofix"])),
    hasHillAssist: isTruthyFeature(findFeatureValue(features, ["hill assist"])),
    hasHillDescent: isTruthyFeature(findFeatureValue(features, ["hill descent"])),
    hasRearCamera: isTruthyFeature(findFeatureValue(features, ["rear camera"])),
    has360Camera: isTruthyFeature(findFeatureValue(features, ["360 view camera", "360 degree camera", "around view"])),
    hasAdas:
      isTruthyFeature(findFeatureValue(features, ["adas"])) ||
      hasAnyAdasFeature(features),
    adasFeatures: Object.entries(features)
      .filter(([key, value]) => /adas|lane|adaptive cruise|collision|driver attention|traffic sign|blind/i.test(key) && isTruthyFeature(value))
      .map(([key, value]) => `${key.split("|").pop()?.trim() || key}: ${asText(value)}`)
      .slice(0, 12),
    hasSunroof: isTruthyFeature(sunroofValue),
    sunroofType: sunroofValue,
    hasVentilatedSeats: isTruthyFeature(findFeatureValue(features, ["ventilated seats"])),
    hasRearAcVents: isTruthyFeature(findFeatureValue(features, ["rear ac vents"])),
    hasCruiseControl: isTruthyFeature(findFeatureValue(features, ["cruise control"])),
    hasWirelessCharging: isTruthyFeature(findFeatureValue(features, ["wireless charger", "wireless charging"])),
    hasAndroidAuto: isTruthyFeature(findFeatureValue(features, ["android auto"])),
    hasAppleCarPlay: isTruthyFeature(findFeatureValue(features, ["apple carplay", "apple car play"])),
    hasAlloyWheels: isTruthyFeature(findFeatureValue(features, ["alloy wheels"])),
    frontBrakeType: asText(findFeatureValue(features, ["front brake type"])),
    rearBrakeType: asText(findFeatureValue(features, ["rear brake type"])),
    bharatNcapAdult: toNumber(findFeatureValue(features, ["bharat ncap safety rating", "bharat ncap adult"])),
    bharatNcapChild: toNumber(findFeatureValue(features, ["bharat ncap child"])),
    globalNcapAdult: toNumber(findFeatureValue(features, ["global ncap adult"])),
    globalNcapChild: toNumber(findFeatureValue(features, ["global ncap child"])),
    missingData: [],
    rawFeatureMap: features,
  };

  if (!profile.bharatNcapAdult && !profile.globalNcapAdult) {
    profile.missingData.push("safety_rating");
  }
  if (!profile.mileage) profile.missingData.push("mileage");
  if (!profile.groundClearanceMm) profile.missingData.push("ground_clearance");
  if (!profile.bootSpaceLitres) profile.missingData.push("boot_space");
  if (!profile.powerBhP) profile.missingData.push("power");
  if (!profile.torqueNm) profile.missingData.push("torque");

  return profile;
};
