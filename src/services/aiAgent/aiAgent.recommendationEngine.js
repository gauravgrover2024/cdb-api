const asNumber = (value, fallback = 0) => {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
};

const unique = (items = []) => [...new Set(items.filter(Boolean))];

const bodyBucket = (value = "") => {
  const text = String(value || "").toLowerCase();
  if (/compact\s*suv/.test(text)) return "compact_suv";
  if (/\bsuv|crossover/.test(text)) return "suv";
  if (/\bsedan/.test(text)) return "sedan";
  if (/\bhatchback/.test(text)) return "hatchback";
  if (/\bmpv|muv|van/.test(text)) return "mpv";
  return "unknown";
};

const boolScore = (value, weight, reasons, label) => {
  if (value) {
    reasons.push(label);
    return weight;
  }
  return 0;
};

export const scoreSafety = (profile = {}) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [];

  const adult = asNumber(profile.bharatNcapAdult || profile.globalNcapAdult, 0);
  const child = asNumber(profile.bharatNcapChild || profile.globalNcapChild, 0);
  const airbags = asNumber(profile.airbags, 0);

  if (adult > 0) {
    score += adult * 12;
    reasons.push(`${adult} star adult safety rating`);
  } else {
    missingData.push("crash_rating_adult");
  }

  if (child > 0) {
    score += child * 8;
    reasons.push(`${child} star child safety rating`);
  } else {
    missingData.push("crash_rating_child");
  }

  if (airbags > 0) {
    score += Math.min(airbags, 8) * 4;
    reasons.push(`${airbags} airbags`);
  } else {
    missingData.push("airbags");
  }

  score += boolScore(profile.hasEsc, 8, reasons, "ESC available");
  score += boolScore(profile.hasEbd, 4, reasons, "EBD available");
  score += boolScore(profile.hasAbs, 4, reasons, "ABS available");
  score += boolScore(profile.hasTpms, 3, reasons, "TPMS available");
  score += boolScore(profile.hasIsofix, 5, reasons, "ISOFIX available");
  score += boolScore(profile.hasHillAssist, 2, reasons, "Hill assist available");
  score += boolScore(profile.hasRearCamera, 2, reasons, "Rear camera available");
  score += boolScore(profile.has360Camera, 3, reasons, "360 camera available");
  score += boolScore(profile.hasAdas, 10, reasons, "ADAS support available");

  if (!reasons.length) penalties.push("limited_safety_data");

  return { score, reasons, missingData: unique(missingData), penalties };
};

export const scoreSimilarity = (
  anchorProfile = {},
  candidateProfile = {},
  anchorPrice = 0,
  candidatePrice = 0,
) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [];

  const anchorBody = bodyBucket(anchorProfile.bodyType);
  const candidateBody = bodyBucket(candidateProfile.bodyType);

  const incompatible = {
    sedan: new Set(["suv", "compact_suv", "hatchback", "mpv"]),
    suv: new Set(["sedan", "hatchback", "mpv"]),
    compact_suv: new Set(["sedan", "hatchback", "mpv"]),
    hatchback: new Set(["sedan", "suv", "compact_suv", "mpv"]),
    mpv: new Set(["sedan", "suv", "compact_suv", "hatchback"]),
  };

  if (
    anchorBody !== "unknown" &&
    candidateBody !== "unknown" &&
    incompatible[anchorBody]?.has(candidateBody)
  ) {
    return {
      score: -1000,
      reasons: ["incompatible body type"],
      missingData,
      penalties: ["body_type_mismatch"],
    };
  }

  if (anchorBody !== "unknown" && anchorBody === candidateBody) {
    score += 50;
    reasons.push("same body type");
  } else if (anchorBody === "suv" && candidateBody === "compact_suv") {
    score += 35;
    reasons.push("compatible SUV segment");
  } else if (anchorBody === "compact_suv" && candidateBody === "suv") {
    score += 35;
    reasons.push("compatible SUV segment");
  } else if (candidateBody === "unknown") {
    score -= 40;
    missingData.push("candidate_body_type");
  }

  if (anchorPrice > 0 && candidatePrice > 0) {
    const diffRatio = Math.abs(candidatePrice - anchorPrice) / Math.max(anchorPrice, 1);
    score += Math.max(0, 30 - diffRatio * 60);
    reasons.push("price-band proximity");
  } else {
    missingData.push("price_band");
  }

  if (asNumber(anchorProfile.seatingCapacity) > 0 && asNumber(candidateProfile.seatingCapacity) > 0) {
    if (asNumber(anchorProfile.seatingCapacity) === asNumber(candidateProfile.seatingCapacity)) {
      score += 8;
      reasons.push("same seating capacity");
    }
  }

  if (anchorProfile.fuelType && candidateProfile.fuelType) {
    if (String(anchorProfile.fuelType).toLowerCase() === String(candidateProfile.fuelType).toLowerCase()) {
      score += 5;
      reasons.push("same fuel type");
    }
  }

  if (anchorProfile.isAutomatic === candidateProfile.isAutomatic) {
    score += 5;
    reasons.push("transmission match");
  }

  if (anchorProfile.powerBhP && candidateProfile.powerBhP) {
    const diff = Math.abs(candidateProfile.powerBhP - anchorProfile.powerBhP);
    if (diff <= 20) {
      score += 8;
      reasons.push("similar power band");
    }
  }

  return { score, reasons, missingData: unique(missingData), penalties };
};

export const scoreAutomaticValue = (row = {}, profile = {}, budget = 0) => {
  const reasons = [];
  const penalties = [];
  const missingData = [];

  if (!profile.isAutomatic) {
    return { score: -500, reasons: ["not an automatic variant"], missingData, penalties: ["not_automatic"] };
  }

  let score = 60;
  const price = asNumber(row.canonicalOnRoadPrice || row.onRoadPrice || row.price, 0);

  if (budget > 0 && price > 0) {
    if (price <= budget) {
      score += 20;
      reasons.push("within budget");
    } else {
      score -= 30;
      penalties.push("over_budget");
    }
  }

  if (profile.automaticType) {
    score += 8;
    reasons.push(`${profile.automaticType} automatic`);
  }

  const safety = scoreSafety(profile);
  score += Math.min(safety.score, 40) * 0.3;
  reasons.push(...safety.reasons.slice(0, 2));

  if (profile.mileage) {
    score += Math.min(profile.mileage, 25);
    reasons.push(`mileage ${profile.mileage} kmpl`);
  } else {
    missingData.push("mileage");
  }

  score += boolScore(profile.hasRearCamera, 3, reasons, "rear camera");
  score += boolScore(profile.hasWirelessCharging, 2, reasons, "wireless charging");
  score += boolScore(profile.hasSunroof, 2, reasons, "sunroof");

  return { score, reasons: unique(reasons), missingData: unique(missingData), penalties };
};

const FEATURE_MATCHERS = {
  automatic: (p) => p.isAutomatic,
  sunroof: (p) => p.hasSunroof,
  "panoramic sunroof": (p) => p.hasSunroof && /panoramic/i.test(String(p.sunroofType || "")),
  "6 airbags": (p) => asNumber(p.airbags) >= 6,
  adas: (p) => p.hasAdas,
  "360 camera": (p) => p.has360Camera,
  "ventilated seats": (p) => p.hasVentilatedSeats,
  "rear camera": (p) => p.hasRearCamera,
  tpms: (p) => p.hasTpms,
  esc: (p) => p.hasEsc,
  isofix: (p) => p.hasIsofix,
  "cruise control": (p) => p.hasCruiseControl,
  "rear ac vents": (p) => p.hasRearAcVents,
  "alloy wheels": (p) => p.hasAlloyWheels,
  "wireless charging": (p) => p.hasWirelessCharging,
  "android auto": (p) => p.hasAndroidAuto,
  "apple carplay": (p) => p.hasAppleCarPlay,
};

export const scoreFeatureMatch = (profile = {}, requestedFeatures = []) => {
  const normalizedRequested = unique(
    requestedFeatures
      .map((item) => String(item || "").toLowerCase().trim())
      .filter(Boolean),
  );

  let score = 0;
  const matchedFeatures = [];
  const missingFeatures = [];
  const partialMatches = [];
  const reasons = [];

  for (const feature of normalizedRequested) {
    const matcher = FEATURE_MATCHERS[feature];
    if (matcher && matcher(profile)) {
      matchedFeatures.push(feature);
      score += 18;
      reasons.push(`matches ${feature}`);
      continue;
    }

    if (feature === "airbags" && asNumber(profile.airbags) >= 4) {
      partialMatches.push(`airbags (${profile.airbags})`);
      score += 6;
      continue;
    }

    missingFeatures.push(feature);
    score -= 5;
  }

  return { score, matchedFeatures, missingFeatures, partialMatches, reasons };
};

export const scoreFamily = (profile = {}) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [];

  const safety = scoreSafety(profile);
  score += safety.score * 0.45;
  reasons.push(...safety.reasons.slice(0, 3));

  const seats = asNumber(profile.seatingCapacity, 0);
  if (seats >= 5) {
    score += 12;
    reasons.push(`${seats}-seater practicality`);
  } else {
    missingData.push("seating_capacity");
  }

  if (asNumber(profile.bootSpaceLitres, 0) > 350) {
    score += 10;
    reasons.push("good boot space");
  }

  score += boolScore(profile.hasRearAcVents, 4, reasons, "rear AC vents");
  score += boolScore(profile.hasIsofix, 6, reasons, "ISOFIX support");
  score += boolScore(profile.hasRearCamera, 4, reasons, "rear camera");

  if (asNumber(profile.groundClearanceMm, 0) >= 180) {
    score += 5;
    reasons.push("good ground clearance");
  }

  if (profile.isAutomatic) {
    score += 4;
    reasons.push("automatic convenience");
  }

  return { score, reasons: unique(reasons), missingData: unique([...missingData, ...safety.missingData]), penalties };
};

export const scoreSeniorFriendly = (profile = {}) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [];

  if (profile.isAutomatic) {
    score += 18;
    reasons.push("automatic transmission");
  }

  const body = bodyBucket(profile.bodyType);
  if (body === "suv" || body === "compact_suv") {
    score += 14;
    reasons.push("higher seating posture");
  }

  const gc = asNumber(profile.groundClearanceMm, 0);
  if (gc >= 170 && gc <= 210) {
    score += 10;
    reasons.push("easy ingress ground clearance");
  } else if (gc > 220) {
    score += 2;
    penalties.push("too_tall_for_some_users");
  }

  score += boolScore(profile.hasRearCamera, 6, reasons, "rear camera");
  score += boolScore(profile.hasHillAssist, 3, reasons, "hill assist");

  const safety = scoreSafety(profile);
  score += safety.score * 0.3;
  reasons.push(...safety.reasons.slice(0, 2));

  if (!gc) missingData.push("ground_clearance");

  return { score, reasons: unique(reasons), missingData: unique([...missingData, ...safety.missingData]), penalties };
};

export const scoreSpace = (profile = {}) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [];

  if (asNumber(profile.bootSpaceLitres, 0) > 0) {
    score += Math.min(asNumber(profile.bootSpaceLitres, 0) / 10, 40);
    reasons.push(`boot space ${profile.bootSpaceLitres}L`);
  } else {
    missingData.push("boot_space");
  }

  if (asNumber(profile.wheelbaseMm, 0) > 0) {
    score += Math.min((profile.wheelbaseMm - 2200) / 20, 22);
    reasons.push(`wheelbase ${profile.wheelbaseMm}mm`);
  }

  if (asNumber(profile.widthMm, 0) > 0) {
    score += Math.min((profile.widthMm - 1600) / 10, 18);
    reasons.push(`width ${profile.widthMm}mm`);
  }

  if (asNumber(profile.seatingCapacity, 0) >= 5) {
    score += 8;
    reasons.push(`${profile.seatingCapacity}-seater`);
  }

  score += boolScore(profile.hasRearAcVents, 4, reasons, "rear AC vents");

  return { score, reasons: unique(reasons), missingData: unique(missingData), penalties };
};

export const scorePerformance = (profile = {}, price = 0) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [];

  if (asNumber(profile.powerBhP, 0) > 0) {
    score += Math.min(profile.powerBhP * 0.6, 80);
    reasons.push(`${profile.powerBhP} bhp`);
  } else {
    missingData.push("power");
  }

  if (asNumber(profile.torqueNm, 0) > 0) {
    score += Math.min(profile.torqueNm * 0.12, 50);
    reasons.push(`${profile.torqueNm} Nm torque`);
  } else {
    missingData.push("torque");
  }

  if (/turbo/i.test(String(profile.rawFeatureMap?.["Engine & Transmission | Turbo Charger"] || ""))) {
    score += 8;
    reasons.push("turbo support");
  }

  if (profile.isAutomatic) {
    score += 4;
    reasons.push("automatic transmission");
  }

  const effectivePrice = asNumber(price, 0);
  if (effectivePrice > 0 && asNumber(profile.powerBhP, 0) > 0) {
    score += Math.min((profile.powerBhP / effectivePrice) * 1000000, 20);
    reasons.push("power-to-price value");
  }

  return { score, reasons: unique(reasons), missingData: unique(missingData), penalties };
};

export const scoreTcoEstimate = (row = {}, profile = {}) => {
  let score = 0;
  const reasons = [];
  const penalties = [];
  const missingData = [
    "verified_service_cost",
    "resale_value",
    "spare_parts_cost",
  ];

  const price = asNumber(row.canonicalOnRoadPrice || row.onRoadPrice || row.price, 0);
  if (price > 0) {
    score += Math.max(0, 70 - price / 250000);
    reasons.push("lower upfront on-road price");
  }

  if (asNumber(profile.mileage, 0) > 0) {
    score += Math.min(profile.mileage * 1.8, 40);
    reasons.push(`mileage ${profile.mileage} kmpl`);
  }

  const fuelType = String(profile.fuelType || row.fuel || "").toLowerCase();
  if (/cng|electric|hybrid/.test(fuelType)) {
    score += 10;
    reasons.push("potentially lower running cost fuel type");
  }

  return { score, reasons, missingData, penalties };
};

export const scoreVariantUpgrade = (
  baseProfile = {},
  nextProfile = {},
  basePrice = 0,
  nextPrice = 0,
) => {
  const priceDifference = asNumber(nextPrice, 0) - asNumber(basePrice, 0);

  const boolTransitions = [
    ["ADAS", baseProfile.hasAdas, nextProfile.hasAdas, "safety"],
    ["360 camera", baseProfile.has360Camera, nextProfile.has360Camera, "safety"],
    ["ESC", baseProfile.hasEsc, nextProfile.hasEsc, "safety"],
    ["Sunroof", baseProfile.hasSunroof, nextProfile.hasSunroof, "comfort"],
    ["Ventilated seats", baseProfile.hasVentilatedSeats, nextProfile.hasVentilatedSeats, "comfort"],
    ["Wireless charging", baseProfile.hasWirelessCharging, nextProfile.hasWirelessCharging, "comfort"],
  ];

  const addedFeatures = [];
  const removedFeatures = [];
  const safetyAdditions = [];
  const comfortAdditions = [];
  const performanceAdditions = [];

  for (const [label, before, after, bucket] of boolTransitions) {
    if (!before && after) {
      addedFeatures.push(label);
      if (bucket === "safety") safetyAdditions.push(label);
      if (bucket === "comfort") comfortAdditions.push(label);
    }
    if (before && !after) removedFeatures.push(label);
  }

  if (asNumber(nextProfile.powerBhP, 0) > asNumber(baseProfile.powerBhP, 0)) {
    performanceAdditions.push("higher power output");
    addedFeatures.push("higher power output");
  }

  if (asNumber(nextProfile.torqueNm, 0) > asNumber(baseProfile.torqueNm, 0)) {
    performanceAdditions.push("higher torque output");
    addedFeatures.push("higher torque output");
  }

  let verdict = "depends";
  const reasons = [];

  if (addedFeatures.length >= 3 && priceDifference <= 200000) {
    verdict = "worth_it";
    reasons.push("multiple meaningful upgrades for the price jump");
  } else if (addedFeatures.length <= 1 && priceDifference > 200000) {
    verdict = "skip";
    reasons.push("limited gains for a high upgrade premium");
  } else {
    reasons.push("upgrade value depends on your must-have features");
  }

  return {
    priceDifference,
    addedFeatures: unique(addedFeatures),
    removedFeatures: unique(removedFeatures),
    safetyAdditions: unique(safetyAdditions),
    comfortAdditions: unique(comfortAdditions),
    performanceAdditions: unique(performanceAdditions),
    verdict,
    reasons,
  };
};
