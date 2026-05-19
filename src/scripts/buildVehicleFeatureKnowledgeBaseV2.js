import dotenv from "dotenv";
import fs from "fs";
import path from "path";
import mongoose from "mongoose";

dotenv.config();

const SOURCE_FEATURES = "vehicle_features";
const SOURCE_VEHICLES = "vehicles";

const OUT_CATALOG = "vehicle_feature_catalog_v2";
const OUT_MATRIX = "vehicle_variant_feature_matrix_v2";
const OUT_ROWS = "vehicle_feature_rows_v2";
const OUT_BUILDS = "vehicle_feature_kb_builds";

const FEATURE_SCHEMA_VERSION = "feature-kb-v2.0";

const args = new Set(process.argv.slice(2));
const SHOULD_WRITE = args.has("--write");
const SHOULD_FULL_WRITE = args.has("--full-write");
const SHOULD_SLIM_WRITE = SHOULD_WRITE && !SHOULD_FULL_WRITE;
const SHOULD_REPLACE = args.has("--replace");
const SHOULD_WRITE_ROWS = args.has("--write-rows");
const PROBE = args.has("--probe");

const OUT_DIR = "/tmp/aci_feature_kb_v2";

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const nowIso = () => new Date().toISOString();

const clean = (value = "") =>
  String(value ?? "")
    .replace(/&amp;/g, "&")
    .replace(/<br\s*\/?>/gi, " ")
    .replace(/<[^>]*>/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const normalizeText = (value = "") =>
  clean(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[’']/g, "")
    .replace(/no\./g, "number")
    .replace(/\bno\b/g, "number")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compactText = (value = "") => normalizeText(value).replace(/\s+/g, "");

const slug = (value = "") => normalizeText(value).replace(/\s+/g, "_");

const uniq = (items = []) => [...new Set(items.filter(Boolean))];

const numberOrZero = (value) => {
  const n = Number(String(value ?? "").replace(/[^\d.]/g, ""));
  return Number.isFinite(n) ? n : 0;
};

const safeJsonWrite = (file, data) => {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, JSON.stringify(data, null, 2));
};

const chunk = (arr, size = 1000) => {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
};

const titleCase = (value = "") =>
  clean(value).replace(/\w\S*/g, (word) =>
    word.charAt(0).toUpperCase() + word.slice(1).toLowerCase(),
  );

const parseFeatureKey = (key = "") => {
  const parts = clean(key)
    .split("|")
    .map((item) => clean(item))
    .filter(Boolean);

  if (parts.length >= 2) {
    return {
      rawSection: parts[0],
      rawFeatureName: parts.slice(1).join(" | "),
    };
  }

  return {
    rawSection: "",
    rawFeatureName: clean(key),
  };
};

const stripVariantPrefix = ({ brand = "", model = "", variant = "" } = {}) => {
  let value = clean(variant);

  const prefixes = [
    clean(`${brand} ${model}`),
    clean(model),
    clean(brand),
  ].filter(Boolean);

  for (const prefix of prefixes) {
    const escaped = prefix.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
    value = value.replace(new RegExp(`^${escaped}\\s+`, "i"), "").trim();
  }

  return value || clean(variant);
};

const canonicalFeatureName = (featureName = "") => {
  let value = clean(featureName);

  value = value
    .replace(/^No\.?\s+of\s+/i, "Number of ")
    .replace(/^No\s+of\s+/i, "Number of ")
    .replace(/\s+/g, " ")
    .trim();

  return value;
};

const parseAvailability = (value = "") => {
  const raw = clean(value);
  const rawLower = raw
    .toLowerCase()
    .replace(/&amp;/g, "&")
    .replace(/[’']/g, "")
    .replace(/\s+/g, " ")
    .trim();

  const negativeValues = new Set([
    "",
    "no",
    "no.",
    "false",
    "na",
    "n/a",
    "n a",
    "not available",
    "not applicable",
    "-",
    "--",
  ]);

  if (negativeValues.has(rawLower)) {
    return {
      available: false,
      displayValue: "Not Available",
      rawValue: raw,
    };
  }

  return {
    available: true,
    displayValue: raw === "Yes" ? "Yes" : raw,
    rawValue: raw,
  };
};

const sectionGroup = (section = "") => {
  const n = normalizeText(section);

  if (!n) return { groupKey: "other", groupLabel: "Other", sectionLabel: "" };

  if (n.includes("charging")) {
    return { groupKey: "charging", groupLabel: "Charging", sectionLabel: "Charging" };
  }

  if (n.includes("comfort") || n.includes("convenience")) {
    return { groupKey: "comfort", groupLabel: "Comfort & Convenience", sectionLabel: "Comfort & Convenience" };
  }

  if (n.includes("interior")) {
    return { groupKey: "interior", groupLabel: "Interior", sectionLabel: "Interior" };
  }

  if (n.includes("exterior")) {
    return { groupKey: "exterior", groupLabel: "Exterior", sectionLabel: "Exterior" };
  }

  if (n.includes("safety")) {
    return { groupKey: "safety", groupLabel: "Safety", sectionLabel: "Safety" };
  }

  if (n.includes("adas")) {
    return { groupKey: "adas", groupLabel: "ADAS", sectionLabel: "ADAS Feature" };
  }

  if (
    n.includes("entertainment") ||
    n.includes("communication") ||
    n.includes("infotainment") ||
    n.includes("audio")
  ) {
    return { groupKey: "infotainment", groupLabel: "Infotainment", sectionLabel: "Entertainment & Communication" };
  }

  if (n.includes("internet") || n.includes("connected") || n.includes("remote")) {
    return { groupKey: "connected", groupLabel: "Connected Car", sectionLabel: "Connected Car Features" };
  }

  if (n.includes("engine") || n.includes("transmission")) {
    return { groupKey: "engine", groupLabel: "Engine & Transmission", sectionLabel: "Engine & Transmission" };
  }

  if (n.includes("fuel") || n.includes("performance") || n.includes("mileage")) {
    return { groupKey: "performance", groupLabel: "Fuel & Performance", sectionLabel: "Fuel & Performance" };
  }

  if (n.includes("suspension") || n.includes("steering") || n.includes("brake")) {
    return { groupKey: "chassis", groupLabel: "Suspension, Steering & Brakes", sectionLabel: "Suspension, Steering & Brakes" };
  }

  if (n.includes("dimension") || n.includes("capacity") || n.includes("boot") || n.includes("ground clearance")) {
    return { groupKey: "dimensions", groupLabel: "Dimensions & Capacity", sectionLabel: "Dimensions & Capacity" };
  }

  if (n.includes("key specification")) {
    return { groupKey: "key_specs", groupLabel: "Key Specifications", sectionLabel: "Key Specifications" };
  }

  if (n.includes("key feature")) {
    return { groupKey: "key_features", groupLabel: "Key Features", sectionLabel: "Key Features" };
  }

  return {
    groupKey: "other",
    groupLabel: "Other",
    sectionLabel: clean(section),
  };
};

const groupRank = (groupKey = "") => {
  const ranks = {
    safety: 100,
    adas: 100,
    comfort: 95,
    exterior: 95,
    interior: 90,
    infotainment: 90,
    connected: 90,
    charging: 90,
    engine: 90,
    performance: 85,
    dimensions: 85,
    chassis: 80,
    key_features: 45,
    key_specs: 40,
    other: 30,
  };

  return ranks[groupKey] || 20;
};

const featurePriority = (feature = {}) => {
  let score = groupRank(feature.groupKey);

  if (feature.available) score += 5;
  if (feature.value && feature.value !== "Yes" && feature.value !== "Not Available") score += 4;
  if (feature.groupKey === "key_features" || feature.groupKey === "key_specs") score -= 10;

  return score;
};

const safeAliasesForFeature = (displayName = "", groupKey = "") => {
  const base = canonicalFeatureName(displayName);
  const n = normalizeText(base);
  const aliases = new Set();

  const add = (...items) => {
    for (const item of items) {
      const normalized = normalizeText(item);
      if (normalized) aliases.add(normalized);
    }
  };

  add(base, n);

  const singular = n
    .replace(/\bseats\b/g, "seat")
    .replace(/\bwheels\b/g, "wheel")
    .replace(/\bheadlamps\b/g, "headlamp")
    .replace(/\bheadlights\b/g, "headlight")
    .replace(/\bairbags\b/g, "airbag")
    .trim();

  add(singular);

  const weakRemoved = n
    .replace(/\b(system|systems|feature|features|control|controls|front|rear|the|and|with)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();

  if (weakRemoved.length >= 4) add(weakRemoved);

  if (n.includes("sunroof")) {
    add("sunroof", "sun roof", "moonroof");
  }

  if (n.includes("rear camera") || n.includes("parking camera")) {
    add("rear camera", "reverse camera", "parking camera");
  }

  if (n.includes("360") || n.includes("surround view") || n.includes("around view")) {
    add("360 camera", "360 degree camera", "surround camera", "around view camera");
  }

  if (n.includes("ventilated")) {
    add("ventilated seats", "ventilated seat", "cooled seats", "seat ventilation");
  }

  if (n.includes("wireless") && (n.includes("charging") || n.includes("charger"))) {
    add("wireless charger", "wireless charging", "wireless phone charger", "phone charger");
  }

  if (n.includes("alloy")) {
    add("alloy wheels", "alloys", "alloy");
  }

  if ((n.includes("headlamp") || n.includes("headlight")) && n.includes("led")) {
    add("led headlamps", "led headlights");
  }

  if (n.includes("rear ac")) {
    add("rear ac vents", "rear vents", "back ac vents");
  }

  if (n.includes("automatic climate")) {
    add("automatic climate control", "climate control", "auto ac");
  }

  if (n.includes("hill hold") || n.includes("hill assist") || n.includes("hill start")) {
    add("hill hold", "hill assist", "hill start assist");
  }

  if (n.includes("tyre pressure") || n.includes("tire pressure") || n.includes("tpms")) {
    add("tpms", "tyre pressure monitor", "tyre pressure monitoring", "tire pressure monitoring");
  }

  if (n.includes("cruise control")) {
    add("cruise control", "cruise");
  }

  if (n.includes("speaker") || n.includes("sound") || n.includes("audio")) {
    add("speakers", "sound system", "audio system");
    if (n.includes("bose")) add("bose speakers", "bose audio");
    if (n.includes("jbl")) add("jbl speakers", "jbl audio");
    if (n.includes("harman")) add("harman speakers", "harman audio");
    if (n.includes("premium") || n.includes("branded")) add("premium audio", "branded audio");
  }

  if (groupKey === "adas" || n.includes("adas")) {
    add("adas", "advanced driver assistance", "driver assistance");
  }

  return [...aliases];
};

const parseNumericValue = (value = "") => {
  const m = clean(value).match(/\d+(\.\d+)?/);
  return m ? Number(m[0]) : 0;
};

const normalizeValueForMerge = (value = "") =>
  clean(value)
    .toLowerCase()
    .replace(/&amp;/g, "&")
    .replace(/[’']/g, "")
    .replace(/\s+/g, " ")
    .trim();

const isGenericYesValue = (value = "") =>
  normalizeValueForMerge(value) === "yes";

const isNegativeValue = (value = "") => {
  const v = normalizeValueForMerge(value);
  return ["", "no", "not available", "n/a", "na", "-", "--", "false"].includes(v);
};

const sourceSectionRank = (section = "") => {
  const n = normalizeText(section);

  if (!n) return 10;
  if (n.includes("key specification")) return 20;
  if (n.includes("key feature")) return 20;
  if (n.includes("derived")) return 35;
  if (n.includes("charging")) return 100;
  if (n.includes("engine") || n.includes("transmission")) return 95;
  if (n.includes("fuel") || n.includes("performance")) return 95;
  if (n.includes("safety")) return 95;
  if (n.includes("comfort") || n.includes("convenience")) return 92;
  if (n.includes("exterior")) return 92;
  if (n.includes("interior")) return 92;
  if (n.includes("entertainment") || n.includes("communication")) return 92;
  if (n.includes("internet") || n.includes("connected")) return 90;

  return 50;
};

const valueSpecificityScore = (value = "") => {
  const v = clean(value);
  const n = normalizeValueForMerge(v);

  if (!v) return 0;
  if (isNegativeValue(v)) return 1;
  if (n === "yes") return 5;

  let score = 20;
  score += Math.min(v.length / 4, 30);
  if (/\d/.test(v)) score += 8;
  if (/[|,;/]/.test(v)) score += 8;
  if (/front|rear|with|without|split|inch|kw|cc|litre|speaker|jio|harman|bose|jbl/i.test(v)) {
    score += 10;
  }

  return score;
};

const comparableNumeric = (value = "") => {
  const raw = clean(value);
  const num = raw.match(/\d+(\.\d+)?/);
  if (!num) return "";

  return String(Number(num[0]));
};

const valuesAreEquivalent = (a = "", b = "", canonicalKey = "") => {
  const av = normalizeValueForMerge(a);
  const bv = normalizeValueForMerge(b);

  if (av === bv) return true;

  const numericKeys = new Set([
    "displacement",
    "touchscreen_size",
    "digital_cluster_size",
    "number_of_speakers",
    "fuel_tank_capacity",
    "boot_space",
    "ground_clearance_unladen",
    "length",
    "width",
    "height",
    "wheel_base",
  ]);

  if (numericKeys.has(canonicalKey)) {
    const an = comparableNumeric(a);
    const bn = comparableNumeric(b);
    if (an && bn && an === bn) return true;
  }

  if (
    canonicalKey === "fuel_type" &&
    ((av === "electric" && bv === "electric battery") ||
      (bv === "electric" && av === "electric battery"))
  ) {
    return true;
  }

  return false;
};

const valueContainsOther = (a = "", b = "") => {
  const av = normalizeValueForMerge(a);
  const bv = normalizeValueForMerge(b);

  if (!av || !bv) return false;
  if (av === bv) return true;

  return av.includes(bv) || bv.includes(av);
};

const pickBetterFeatureValue = (a, b) => {
  const score = (item) =>
    sourceSectionRank(item.rawSection || item.section) +
    valueSpecificityScore(item.value) +
    (item.synthetic ? -10 : 0);

  return score(b) > score(a) ? b : a;
};

const detectChargingTimeType = (value = "", section = "") => {
  const raw = clean(value);
  const v = normalizeText(raw);
  const s = normalizeText(section);

  if (/\bdc\b/i.test(raw) || v.includes(" dc ") || v.includes("fast charging") || v.includes("rapid charging")) {
    return "dc";
  }

  if (/\bac\b/i.test(raw) || v.includes(" ac ") || v.includes("home charging") || v.includes("wallbox")) {
    return "ac";
  }

  const kwMatch = raw.match(/(\d+(?:\.\d+)?)\s*kW/i);
  const kw = kwMatch ? Number(kwMatch[1]) : 0;

  if (kw >= 50) return "dc";
  if (kw > 0 && kw <= 22) return "ac";

  if (/\d+\s*(min|mins|minute|minutes)/i.test(raw) && /(10\s*-\s*80|0\s*-\s*80|5\s*-\s*80)/i.test(raw)) {
    return "dc";
  }

  if (/\d+\s*(h|hr|hrs|hour|hours)/i.test(raw)) {
    return "ac";
  }

  if (s.includes("charging")) return "general";

  return "general";
};

const conflictObject = (existing, incoming, resolution = "unresolved") => ({
  resolution,
  existing: {
    value: existing.value,
    available: existing.available,
    rawSection: existing.rawSection,
    rawFeatureName: existing.rawFeatureName,
  },
  incoming: {
    value: incoming.value,
    available: incoming.available,
    rawSection: incoming.rawSection,
    rawFeatureName: incoming.rawFeatureName,
  },
});

const normalizeChargingTimeFeature = ({
  displayName = "",
  value = "",
  section = "",
} = {}) => {
  const rawName = clean(displayName);
  const rawValue = clean(value);
  const rawSection = clean(section);
  const name = normalizeText(rawName);
  const rawAll = clean([displayName, value, section].join(" "));
  const all = normalizeText(rawAll);

  if (!name.includes("charging time")) return null;

  const explicitDc =
    /\bd\.?c\.?\b/i.test(rawName) ||
    /\bd\.?c\.?\b/i.test(rawValue) ||
    /\bd\.?c\.?\b/i.test(rawSection) ||
    all.includes(" dc ");

  const explicitAc =
    /\ba\.?c\.?\b/i.test(rawName) ||
    /\ba\.?c\.?\b/i.test(rawValue) ||
    /\ba\.?c\.?\b/i.test(rawSection) ||
    all.includes(" ac ") ||
    all.includes("plug point") ||
    all.includes("wallbox") ||
    all.includes("home charging");

  const kwMatch = rawAll.match(/(\d+(?:\.\d+)?)\s*kW/i);
  const kw = kwMatch ? Number(kwMatch[1]) : 0;

  if (all.includes("15 a") || all.includes("15a") || all.includes("plug point")) {
    return {
      canonicalKey: "charging_time_ac_15a_plug_point",
      displayName: "AC Charging Time (15A Plug Point)",
    };
  }

  if (kw > 0) {
    const kwSlug = String(kw).replace(".", "_");

    // Explicit AC must win over words like "fast charger".
    if (explicitAc && !explicitDc) {
      return {
        canonicalKey: `charging_time_ac_${kwSlug}kw`,
        displayName: `AC Charging Time (${kw} kW)`,
      };
    }

    if (explicitDc || kw >= 25) {
      return {
        canonicalKey: `charging_time_dc_${kwSlug}kw`,
        displayName: `DC Charging Time (${kw} kW)`,
      };
    }

    return {
      canonicalKey: `charging_time_ac_${kwSlug}kw`,
      displayName: `AC Charging Time (${kw} kW)`,
    };
  }

  if (explicitDc) {
    return {
      canonicalKey: "charging_time_dc",
      displayName: "DC Charging Time",
    };
  }

  if (explicitAc) {
    return {
      canonicalKey: "charging_time_ac",
      displayName: "AC Charging Time",
    };
  }

  const inferredType = detectChargingTimeType(value, section);

  if (inferredType === "dc") {
    return {
      canonicalKey: "charging_time_dc",
      displayName: "DC Charging Time",
    };
  }

  if (inferredType === "ac") {
    return {
      canonicalKey: "charging_time_ac",
      displayName: "AC Charging Time",
    };
  }

  return {
    canonicalKey: "charging_time_general",
    displayName: "Charging Time",
  };
};

const MULTI_VALUE_CANONICAL_KEYS = new Set([
  "inbuilt_apps",
  "drive_mode_types",
  "connected_car_apps",
  "smartphone_connectivity",
]);

const canMergeAsMultiValue = (canonicalKey = "") => {
  const key = String(canonicalKey || "");
  return (
    MULTI_VALUE_CANONICAL_KEYS.has(key) ||
    key === "charging_time" ||
    key.startsWith("charging_time_")
  );
};

const splitMultiValues = (value = "") =>
  clean(value)
    .split(/[,;/|]+|\band\b/gi)
    .map((item) => clean(item))
    .filter((item) => item && !isGenericYesValue(item) && !isNegativeValue(item));

const mergeMultiValueText = (...values) => {
  const parts = uniq(values.flatMap(splitMultiValues));

  if (!parts.length) {
    return values.find((value) => !isGenericYesValue(value) && !isNegativeValue(value)) || "Yes";
  }

  return parts.join(", ");
};



const makeFeatureObject = ({
  canonicalKey,
  displayName,
  groupKey,
  groupLabel,
  section,
  rawSection,
  rawFeatureName,
  sourceKey,
  value,
  rawValue,
  available,
  sourceDocId,
  synthetic = false,
  sourceFeatureKeys = [],
  rawDisplayName = "",
  excludeFromDefaultCompare = false,
  isNarrativeFeature = false,
}) => ({
  canonicalKey,
  displayName,
  rawDisplayName,
  excludeFromDefaultCompare,
  isNarrativeFeature,
  groupKey,
  groupLabel,
  section,
  rawSection,
  rawFeatureName,
  sourceKey,
  value,
  rawValue,
  available,
  synthetic,
  sourceDocId,
  sourceFeatureKeys,
  quality: {
    source: synthetic ? "derived_from_db_rows" : "vehicle_features",
    confidence: synthetic ? "derived_high" : "exact_db_feature",
  },
});

const mergeFeature = (existing, incoming) => {
  if (!existing) {
    return {
      ...incoming,
      alternatives: [],
      conflicts: [],
      conflictStatus: "clean",
      availabilityStatus:
        incoming.available === true
          ? "available"
          : incoming.available === false
            ? "not_available"
            : "unknown",
      sourceFeatureKeys: uniq(incoming.sourceFeatureKeys || [incoming.sourceKey]),
    };
  }

  const existingValue = existing.value;
  const incomingValue = incoming.value;
  const sameAvailability = existing.available === incoming.available;
  const equivalentValue = valuesAreEquivalent(
    existingValue,
    incomingValue,
    existing.canonicalKey,
  );

  if (sameAvailability && equivalentValue) {
    const chosen = pickBetterFeatureValue(existing, incoming);
    const other = chosen === existing ? incoming : existing;

    return {
      ...chosen,
      alternatives: uniq([
        ...(existing.alternatives || []),
        ...(incoming.alternatives || []),
        other.value,
      ]).filter((value) => normalizeValueForMerge(value) !== normalizeValueForMerge(chosen.value)),
      conflicts: [...(existing.conflicts || []), ...(incoming.conflicts || [])],
      conflictStatus: existing.conflictStatus === "conflicted" ? "conflicted" : "clean",
      availabilityStatus:
        chosen.available === true
          ? "available"
          : chosen.available === false
            ? "not_available"
            : "unknown",
      sourceFeatureKeys: uniq([
        ...(existing.sourceFeatureKeys || []),
        ...(incoming.sourceFeatureKeys || []),
        existing.sourceKey,
        incoming.sourceKey,
      ]),
    };
  }

  // If one value is just "Yes" and the other is specific, keep the specific value.
  if (
    sameAvailability &&
    existing.available === true &&
    (isGenericYesValue(existingValue) || isGenericYesValue(incomingValue))
  ) {
    const chosen = isGenericYesValue(existingValue) ? incoming : existing;
    const other = chosen === existing ? incoming : existing;

    return {
      ...chosen,
      alternatives: uniq([
        ...(existing.alternatives || []),
        ...(incoming.alternatives || []),
        other.value,
      ]).filter((value) => normalizeValueForMerge(value) !== normalizeValueForMerge(chosen.value)),
      conflicts: [...(existing.conflicts || []), ...(incoming.conflicts || [])],
      conflictStatus: "resolved",
      conflictResolution: "specific_value_over_generic_yes",
      availabilityStatus: "available",
      sourceFeatureKeys: uniq([
        ...(existing.sourceFeatureKeys || []),
        ...(incoming.sourceFeatureKeys || []),
        existing.sourceKey,
        incoming.sourceKey,
      ]),
    };
  }

  // If both are available and one value contains the other, keep the more specific/superset value.
  if (sameAvailability && existing.available === true && valueContainsOther(existingValue, incomingValue)) {
    const chosen = valueSpecificityScore(incomingValue) > valueSpecificityScore(existingValue)
      ? incoming
      : existing;
    const other = chosen === existing ? incoming : existing;

    return {
      ...chosen,
      alternatives: uniq([
        ...(existing.alternatives || []),
        ...(incoming.alternatives || []),
        other.value,
      ]).filter((value) => normalizeValueForMerge(value) !== normalizeValueForMerge(chosen.value)),
      conflicts: [...(existing.conflicts || []), ...(incoming.conflicts || [])],
      conflictStatus: "resolved",
      conflictResolution: "more_specific_value_selected",
      availabilityStatus: "available",
      sourceFeatureKeys: uniq([
        ...(existing.sourceFeatureKeys || []),
        ...(incoming.sourceFeatureKeys || []),
        existing.sourceKey,
        incoming.sourceKey,
      ]),
    };
  }

  // Multi-value features such as Inbuilt Apps should become a combined list,
  // not unresolved conflicts.
  if (
    canMergeAsMultiValue(existing.canonicalKey) &&
    existing.available !== false &&
    incoming.available !== false
  ) {
    const mergedValue = mergeMultiValueText(
      ...(existing.alternatives || []),
      ...(incoming.alternatives || []),
      existing.value,
      incoming.value,
    );

    return {
      ...existing,
      value: mergedValue,
      displayValue: mergedValue,
      rawValue: mergedValue,
      available: true,
      alternatives: uniq([
        ...(existing.alternatives || []),
        ...(incoming.alternatives || []),
        existing.value,
        incoming.value,
      ]).filter(Boolean),
      conflicts: [...(existing.conflicts || []), ...(incoming.conflicts || [])],
      conflictStatus: "resolved",
      conflictResolution: "multi_value_union",
      availabilityStatus: "available",
      sourceFeatureKeys: uniq([
        ...(existing.sourceFeatureKeys || []),
        ...(incoming.sourceFeatureKeys || []),
        existing.sourceKey,
        incoming.sourceKey,
      ]),
    };
  }

  // If a synthetic derived row conflicts with an exact DB row, trust the exact DB row.
  if (existing.synthetic !== incoming.synthetic) {
    const chosen = existing.synthetic ? incoming : existing;
    const other = chosen === existing ? incoming : existing;

    return {
      ...chosen,
      alternatives: uniq([
        ...(existing.alternatives || []),
        ...(incoming.alternatives || []),
        other.value,
      ]),
      conflicts: [
        ...(existing.conflicts || []),
        ...(incoming.conflicts || []),
        conflictObject(existing, incoming, "exact_db_row_over_synthetic"),
      ],
      conflictStatus: "resolved",
      conflictResolution: "exact_db_row_over_synthetic",
      availabilityStatus:
        chosen.available === true
          ? "available"
          : chosen.available === false
            ? "not_available"
            : "unknown",
      sourceFeatureKeys: uniq([
        ...(existing.sourceFeatureKeys || []),
        ...(incoming.sourceFeatureKeys || []),
        existing.sourceKey,
        incoming.sourceKey,
      ]),
    };
  }

  const chosen = pickBetterFeatureValue(existing, incoming);

  return {
    ...chosen,
    value: "Conflicting data",
    displayValue: "Conflicting data",
    available: null,
    alternatives: uniq([
      ...(existing.alternatives || []),
      ...(incoming.alternatives || []),
      existing.value,
      incoming.value,
    ]),
    conflicts: [
      ...(existing.conflicts || []),
      ...(incoming.conflicts || []),
      conflictObject(existing, incoming, "conflicted_needs_review"),
    ],
    conflictStatus: "conflicted",
    conflictResolution: "conflicted_needs_review",
    availabilityStatus: "conflicted",
    sourceFeatureKeys: uniq([
      ...(existing.sourceFeatureKeys || []),
      ...(incoming.sourceFeatureKeys || []),
      existing.sourceKey,
      incoming.sourceKey,
    ]),
  };
};

const getVehiclePrice = (row = {}) =>
  numberOrZero(
    row.exShowroomPrice ||
      row.ex_showroom_price ||
      row.exShowroom ||
      row.ex_showroom ||
      row.price ||
      row.onRoadPrice ||
      row.on_road_price ||
      0,
  );

const isActiveVehicleRow = (row = {}) => {
  if (row.is_discontinued === true) return false;
  if (row.discontinued === true) return false;
  if (row.active === false) return false;
  if (row.is_active === false) return false;
  if (/discontinued/i.test(clean(row.status || row.activeStatus))) return false;
  return true;
};

const loadVehiclePriceIndex = async (db) => {
  const rows = await db
    .collection(SOURCE_VEHICLES)
    .find(
      {},
      {
        projection: {
          brand: 1,
          make: 1,
          model: 1,
          model_normalized: 1,
          variant: 1,
          variant_short: 1,
          variant_normalized: 1,
          version: 1,
          fuel: 1,
          transmission: 1,
          exShowroomPrice: 1,
          ex_showroom_price: 1,
          exShowroom: 1,
          ex_showroom: 1,
          price: 1,
          onRoadPrice: 1,
          on_road_price: 1,
          imageUrl: 1,
          displayImageUrl: 1,
          normalizedImageUrl: 1,
          heroImageUrl: 1,
          active: 1,
          is_active: 1,
          is_discontinued: 1,
          discontinued: 1,
          status: 1,
          activeStatus: 1,
        },
      },
    )
    .toArray();

  const byModelVariant = new Map();

  for (const row of rows) {
    const brand = clean(row.brand || row.make);

    // Use normalized display fields first. In vehicles, row.model can include brand
    // e.g. "Hyundai Creta", while feature matrix modelKey is "creta".
    const model = clean(
      row.model_normalized ||
        row.modelName ||
        row.model_name ||
        row.model ||
        ""
    ).replace(new RegExp(`^${clean(row.brand || row.make).replace(/[.*+?^${}()|[\\]\\]/g, "\\$&")}\\s+`, "i"), "").trim();

    // Prefer normalized/short variant fields. Full variant often contains brand+model.
    const variantRaw = clean(
      row.variant_normalized ||
        row.variant_short ||
        row.version ||
        row.variant ||
        ""
    );

    const variant = stripVariantPrefix({ brand, model, variant: variantRaw });

    if (!model || !variant) continue;

    const key = `${slug(model)}__${slug(variant)}`;
    const price = getVehiclePrice(row);

    if (!byModelVariant.has(key)) {
      byModelVariant.set(key, []);
    }

    byModelVariant.get(key).push({
      brand,
      model,
      variant,
      fuel: clean(row.fuel),
      transmission: clean(row.transmission),
      price,
      onRoadPrice: numberOrZero(row.onRoadPrice || row.on_road_price),
      imageUrl:
        row.displayImageUrl ||
        row.normalizedImageUrl ||
        row.heroImageUrl ||
        row.imageUrl ||
        "",
      active: isActiveVehicleRow(row),
    });
  }

  return byModelVariant;
};

const priceSummaryForVariant = (priceRows = []) => {
  const activeRows = priceRows.filter((row) => row.active);
  const inactiveRows = priceRows.filter((row) => !row.active);
  const source = activeRows.length ? activeRows : priceRows;

  const prices = source.map((row) => row.price).filter(Boolean);

  const activePricelistMatched = activeRows.length > 0;
  const priceRowsMatched = priceRows.length;
  const discontinuedPricelistMatched = priceRows.length > 0 && activeRows.length === 0;

  const lifecycleStatus = activePricelistMatched
    ? "active_new_car"
    : discontinuedPricelistMatched
      ? "discontinued_or_inactive"
      : "feature_only_unmatched";

  return {
    activePricelistMatched,
    activeForFeatureExplorer: activePricelistMatched,
    lifecycleStatus,
    priceRowsMatched,
    activePricelistRows: activeRows.length,
    inactivePricelistRows: inactiveRows.length,
    discontinuedPricelistMatched,
    priceMin: prices.length ? Math.min(...prices) : 0,
    priceMax: prices.length ? Math.max(...prices) : 0,
    fuels: uniq(source.map((row) => row.fuel)),
    transmissions: uniq(source.map((row) => row.transmission)),
    imageUrl: source.find((row) => row.imageUrl)?.imageUrl || "",
  };
};

const extractRowsFromDoc = (doc = {}) => {
  const brand = clean(doc.brand || doc.make || "");
  const model = clean(doc.model || "");
  const variantFull = clean(doc.variant || doc.variantName || doc.version || "");
  const variant = stripVariantPrefix({ brand, model, variant: variantFull });

  const rows = [];
  let index = 0;

  if (doc.features && typeof doc.features === "object" && !Array.isArray(doc.features)) {
    for (const [sourceKey, sourceValue] of Object.entries(doc.features)) {
      const { rawSection, rawFeatureName } = parseFeatureKey(sourceKey);
      const displayName = canonicalFeatureName(rawFeatureName);

      if (!displayName) continue;

      const sectionMeta = sectionGroup(rawSection);
      const availability = parseAvailability(sourceValue);

      const isNarrativeFeature = normalizeText(displayName) === "additional features";

      let canonicalKey = isNarrativeFeature
        ? `additional_features_${sectionMeta.groupKey}`
        : slug(displayName);

      let publicDisplayName = isNarrativeFeature
        ? `${sectionMeta.groupLabel} Highlights`
        : displayName;

      if (!isNarrativeFeature && normalizeText(displayName).includes("charging time")) {
        const chargingFeature = normalizeChargingTimeFeature({
          displayName,
          value: sourceValue,
          section: rawSection,
        });

        if (chargingFeature) {
          canonicalKey = chargingFeature.canonicalKey;
          publicDisplayName = chargingFeature.displayName;
          sectionMeta.groupKey = "charging";
          sectionMeta.groupLabel = "Charging";
          sectionMeta.sectionLabel = "Charging";
        }
      }

      rows.push({
        id: `${String(doc._id)}-${index}`,
        sourceDocId: String(doc._id || ""),
        brand,
        make: brand,
        model,
        variant,
        variantFull,
        modelKey: slug(model),
        variantKey: slug(variant),
        brandModelKey: slug(`${brand} ${model}`),
        canonicalKey,
        displayName: publicDisplayName,
        rawDisplayName: displayName,
        isNarrativeFeature,
        excludeFromDefaultCompare: isNarrativeFeature,
        groupKey: sectionMeta.groupKey,
        groupLabel: sectionMeta.groupLabel,
        section: sectionMeta.sectionLabel,
        rawSection,
        rawFeatureName,
        sourceKey,
        value: availability.displayValue,
        displayValue: availability.displayValue,
        rawValue: availability.rawValue,
        available: availability.available,
      });

      index += 1;
    }
  }

  return rows;
};

const addCatalogFeature = (catalog, feature) => {
  const key = feature.canonicalKey;

  if (!catalog.has(key)) {
    catalog.set(key, {
      canonicalKey: key,
      displayName: feature.displayName,
      groupKey: feature.groupKey,
      groupLabel: feature.groupLabel,
      aliases: new Set(),
      rawFeatureNames: new Set(),
      rawSections: new Set(),
      sections: new Set(),
      models: new Set(),
      variants: new Set(),
      rows: 0,
      availableRows: 0,
      synthetic: Boolean(feature.synthetic),
      sourceFeatureKeys: new Set(),
      examples: [],
    });
  }

  const entry = catalog.get(key);

  entry.aliases = new Set([
    ...entry.aliases,
    ...safeAliasesForFeature(feature.displayName, feature.groupKey),
  ]);

  entry.rawFeatureNames.add(feature.rawFeatureName || feature.displayName);
  entry.rawSections.add(feature.rawSection || feature.section);
  entry.sections.add(feature.section);
  if (feature.model) entry.models.add(feature.model);
  if (feature.variant) entry.variants.add(feature.variant);
  if (feature.sourceKey) entry.sourceFeatureKeys.add(feature.sourceKey);

  entry.rows += 1;
  if (feature.available) entry.availableRows += 1;

  if (entry.examples.length < 8) {
    entry.examples.push({
      model: feature.model,
      variant: feature.variant,
      value: feature.value,
      section: feature.section,
      rawFeatureName: feature.rawFeatureName,
    });
  }
};

const addFeatureToMatrix = (matrixDoc, feature, catalog) => {
  const existing = matrixDoc.featuresByKey[feature.canonicalKey];

  matrixDoc.featuresByKey[feature.canonicalKey] = mergeFeature(existing, feature);

  matrixDoc.featureKeys = uniq([...matrixDoc.featureKeys, feature.canonicalKey]);

  addCatalogFeature(catalog, {
    ...feature,
    model: matrixDoc.model,
    variant: matrixDoc.variant,
  });
};

const addSyntheticFeatures = (matrixDoc, catalog) => {
  const features = Object.values(matrixDoc.featuresByKey);

  const available = (feature) => feature?.available === true;

  const sourceFeaturesContaining = (...phrases) =>
    features.filter((feature) => {
      const text = normalizeText(
        [
          feature.displayName,
          feature.rawFeatureName,
          feature.value,
          feature.rawValue,
          feature.section,
          feature.rawSection,
        ].join(" "),
      );

      return phrases.some((phrase) => text.includes(normalizeText(phrase)));
    });

  const adasRows = features.filter(
    (feature) => feature.groupKey === "adas" && feature.available,
  );

  if (features.some((feature) => feature.groupKey === "adas")) {
    addFeatureToMatrix(
      matrixDoc,
      makeFeatureObject({
        canonicalKey: "adas_package",
        displayName: "ADAS Package",
        groupKey: "adas",
        groupLabel: "ADAS",
        section: "ADAS Feature",
        rawSection: "Derived from ADAS Feature",
        rawFeatureName: "ADAS Package",
        sourceKey: "derived:adas_package",
        value: adasRows.length ? `${adasRows.length} ADAS features` : "Not Available",
        rawValue: adasRows.length ? `${adasRows.length} ADAS features` : "Not Available",
        available: adasRows.length > 0,
        sourceDocId: matrixDoc.sourceDocId,
        synthetic: true,
        sourceFeatureKeys: adasRows.map((row) => row.canonicalKey),
      }),
      catalog,
    );
  }

  const connectedRows = features.filter(
    (feature) =>
      feature.groupKey === "connected" ||
      /internet|connected|remote|blue link|bluelink|kia connect/i.test(
        [
          feature.displayName,
          feature.rawFeatureName,
          feature.rawSection,
          feature.value,
        ].join(" "),
      ),
  );

  const connectedAvailable = connectedRows.filter(available);

  if (connectedRows.length) {
    addFeatureToMatrix(
      matrixDoc,
      makeFeatureObject({
        canonicalKey: "connected_car_features",
        displayName: "Connected Car Features",
        groupKey: "connected",
        groupLabel: "Connected Car",
        section: "Connected Car Features",
        rawSection: "Derived from connected feature rows",
        rawFeatureName: "Connected Car Features",
        sourceKey: "derived:connected_car_features",
        value: connectedAvailable.length ? `${connectedAvailable.length} connected features` : "Not Available",
        rawValue: connectedAvailable.length ? `${connectedAvailable.length} connected features` : "Not Available",
        available: connectedAvailable.length > 0,
        sourceDocId: matrixDoc.sourceDocId,
        synthetic: true,
        sourceFeatureKeys: connectedRows.map((row) => row.canonicalKey),
      }),
      catalog,
    );
  }

  const airbagFeature =
    matrixDoc.featuresByKey.number_of_airbags ||
    matrixDoc.featuresByKey.no_of_airbags ||
    features.find((feature) => normalizeText(feature.displayName).includes("airbag"));

  if (airbagFeature) {
    const airbags = parseNumericValue(airbagFeature.value);

    addFeatureToMatrix(
      matrixDoc,
      makeFeatureObject({
        canonicalKey: "six_airbags",
        displayName: "6 Airbags",
        groupKey: "safety",
        groupLabel: "Safety",
        section: "Safety",
        rawSection: airbagFeature.rawSection,
        rawFeatureName: "6 Airbags",
        sourceKey: "derived:six_airbags",
        value: airbags >= 6 ? `${airbags} airbags` : "Not Available",
        rawValue: airbagFeature.value,
        available: airbags >= 6,
        sourceDocId: matrixDoc.sourceDocId,
        synthetic: true,
        sourceFeatureKeys: [airbagFeature.canonicalKey],
      }),
      catalog,
    );
  }

  const camera360Rows = sourceFeaturesContaining("360", "surround view", "around view", "bird view");
  const camera360Available = camera360Rows.filter(available);

  if (camera360Rows.length) {
    addFeatureToMatrix(
      matrixDoc,
      makeFeatureObject({
        canonicalKey: "camera_360",
        displayName: "360 Camera",
        groupKey: "safety",
        groupLabel: "Safety",
        section: "Safety",
        rawSection: "Derived from camera feature rows",
        rawFeatureName: "360 Camera",
        sourceKey: "derived:camera_360",
        value: camera360Available[0]?.value || "Not Available",
        rawValue: camera360Available[0]?.rawValue || "Not Available",
        available: camera360Available.length > 0,
        sourceDocId: matrixDoc.sourceDocId,
        synthetic: true,
        sourceFeatureKeys: camera360Rows.map((row) => row.canonicalKey),
      }),
      catalog,
    );
  }

  const ledHeadlampRows = features.filter((feature) => {
    if (feature.excludeFromDefaultCompare) return false;
    if (String(feature.canonicalKey || "").startsWith("additional_features_")) return false;

    const nameText = normalizeText(
      [feature.displayName, feature.rawFeatureName, feature.sourceKey].join(" "),
    );
    const valueText = normalizeText([feature.value, feature.rawValue].join(" "));

    const isHeadlampFeature =
      nameText.includes("headlamp") ||
      nameText.includes("headlight") ||
      nameText.includes("head lamp") ||
      nameText.includes("head light");

    const saysLed = nameText.includes("led") || valueText.includes("led");

    return isHeadlampFeature && saysLed;
  });

  const ledAvailable = ledHeadlampRows.filter(available);

  if (ledHeadlampRows.length && !matrixDoc.featuresByKey.led_headlamps) {
    addFeatureToMatrix(
      matrixDoc,
      makeFeatureObject({
        canonicalKey: "led_headlamps",
        displayName: "LED Headlamps",
        groupKey: "exterior",
        groupLabel: "Exterior",
        section: "Exterior",
        rawSection: "Derived from lighting feature rows",
        rawFeatureName: "LED Headlamps",
        sourceKey: "derived:led_headlamps",
        value: ledAvailable[0]?.value || "Not Available",
        rawValue: ledAvailable[0]?.rawValue || "Not Available",
        available: ledAvailable.length > 0,
        sourceDocId: matrixDoc.sourceDocId,
        synthetic: true,
        sourceFeatureKeys: ledHeadlampRows.map((row) => row.canonicalKey),
      }),
      catalog,
    );
  }

  const boseRows = features.filter((feature) => {
    const text = normalizeText(
      [feature.displayName, feature.rawFeatureName, feature.value, feature.rawValue].join(" "),
    );

    return text.includes("bose");
  });

  if (boseRows.length) {
    addFeatureToMatrix(
      matrixDoc,
      makeFeatureObject({
        canonicalKey: "bose_audio",
        displayName: "Bose Audio",
        groupKey: "infotainment",
        groupLabel: "Infotainment",
        section: "Entertainment & Communication",
        rawSection: "Derived from audio feature rows",
        rawFeatureName: "Bose Audio",
        sourceKey: "derived:bose_audio",
        value: boseRows.find(available)?.value || "Not Available",
        rawValue: boseRows.find(available)?.rawValue || "Not Available",
        available: boseRows.some(available),
        sourceDocId: matrixDoc.sourceDocId,
        synthetic: true,
        sourceFeatureKeys: boseRows.map((row) => row.canonicalKey),
      }),
      catalog,
    );
  }
};

const buildComparisonPreview = ({ matrixDocs, model, variants }) => {
  const modelKey = slug(model);
  const variantKeys = variants.map(slug);

  const selected = matrixDocs.filter(
    (doc) => doc.modelKey === modelKey && variantKeys.includes(doc.variantKey),
  );

  const allFeatureKeys = uniq(
    selected.flatMap((doc) =>
      Object.entries(doc.featuresByKey || {})
        .filter(([key, feature]) =>
          !feature.excludeFromDefaultCompare &&
          !String(key || "").startsWith("additional_features_")
        )
        .map(([key]) => key),
    ),
  );

  return allFeatureKeys.map((featureKey) => {
    const firstFeature = selected
      .map((doc) => doc.featuresByKey[featureKey])
      .find(Boolean);

    return {
      featureKey,
      displayName: firstFeature?.displayName || titleCase(featureKey.replace(/_/g, " ")),
      groupKey: firstFeature?.groupKey || "other",
      values: Object.fromEntries(
        selected.map((doc) => [
          doc.variant,
          doc.featuresByKey[featureKey]
            ? {
                value: doc.featuresByKey[featureKey].value,
                available: doc.featuresByKey[featureKey].available,
              }
            : {
                value: "Not Available",
                available: false,
              },
        ]),
      ),
    };
  });
};


const toSlimCatalogDocForMongo = (doc = {}) => ({
  buildId: doc.buildId,
  schemaVersion: doc.schemaVersion,
  canonicalKey: doc.canonicalKey,
  displayName: doc.displayName,
  groupKey: doc.groupKey,
  groupLabel: doc.groupLabel,
  aliases: doc.aliases || [],
  sections: doc.sections || [],
  rows: doc.rows || 0,
  availableRows: doc.availableRows || 0,
  modelsCount: doc.modelsCount || 0,
  variantsCount: doc.variantsCount || 0,
  synthetic: Boolean(doc.synthetic),
  updatedAt: doc.updatedAt || new Date(),
  createdAt: doc.createdAt || new Date(),
});

const toSlimFeatureForMongo = (feature = {}) => {
  const slim = {
    displayName: feature.displayName || "",
    groupKey: feature.groupKey || "other",
    groupLabel: feature.groupLabel || "Other",
    section: feature.section || "",
    value: feature.value || feature.displayValue || "",
    available: feature.available === null ? null : Boolean(feature.available),
    availabilityStatus:
      feature.availabilityStatus ||
      (feature.available === true
        ? "available"
        : feature.available === false
          ? "not_available"
          : "unknown"),
    conflictStatus: feature.conflictStatus || "clean",
    synthetic: Boolean(feature.synthetic),
  };

  if (feature.conflictResolution) {
    slim.conflictResolution = feature.conflictResolution;
  }

  if (feature.alternatives?.length) {
    slim.alternatives = feature.alternatives.slice(0, 8);
  }

  return slim;
};

const toSlimMatrixDocForMongo = (doc = {}, buildId = "") => {
  const featuresByKey = {};

  for (const [key, feature] of Object.entries(doc.featuresByKey || {})) {
    if (!feature) continue;

    // Runtime feature explorer/comparison should not load narrative highlight rows by default.
    if (
      feature.excludeFromDefaultCompare ||
      String(key || "").startsWith("additional_features_")
    ) {
      continue;
    }

    featuresByKey[key] = toSlimFeatureForMongo(feature);
  }

  const featureKeys = Object.keys(featuresByKey);
  const featureGroups = uniq(
    Object.values(featuresByKey).map((feature) => feature.groupKey).filter(Boolean),
  );

  return {
    buildId,
    schemaVersion: doc.schemaVersion,
    brand: doc.brand,
    make: doc.make || doc.brand,
    model: doc.model,
    variant: doc.variant,
    variantFull: doc.variantFull,
    modelKey: doc.modelKey,
    variantKey: doc.variantKey,
    brandModelKey: doc.brandModelKey,
    activePricelistMatched: Boolean(doc.activePricelistMatched),
    activeForFeatureExplorer: Boolean(doc.activeForFeatureExplorer),
    lifecycleStatus: doc.lifecycleStatus || "feature_only_unmatched",
    priceRowsMatched: doc.priceRowsMatched || 0,
    activePricelistRows: doc.activePricelistRows || 0,
    inactivePricelistRows: doc.inactivePricelistRows || 0,
    discontinuedPricelistMatched: Boolean(doc.discontinuedPricelistMatched),
    priceMin: doc.priceMin || 0,
    priceMax: doc.priceMax || 0,
    fuels: doc.fuels || [],
    transmissions: doc.transmissions || [],
    imageUrl: doc.imageUrl || "",
    featureKeys,
    featureGroups,
    featuresByKey,
    quality: {
      rawFeatureRows: doc.quality?.rawFeatureRows || 0,
      conflicts: doc.quality?.conflicts || 0,
      generatedAt: doc.quality?.generatedAt || nowIso(),
    },
    createdAt: doc.createdAt || new Date(),
    updatedAt: new Date(),
  };
};


const main = async () => {
  if (!mongoUri) {
    throw new Error("Mongo URI missing. Check .env.");
  }

  fs.mkdirSync(OUT_DIR, { recursive: true });

  await mongoose.connect(mongoUri);
  const db = mongoose.connection.db;

  const buildId = `feature-kb-v2-${Date.now()}`;

  console.log("============================================================");
  console.log("ACI ASSIST FEATURE KNOWLEDGE BASE V2");
  console.log("============================================================");
  console.log("Mode:", SHOULD_WRITE ? "WRITE" : "DRY RUN");
  console.log("Replace:", SHOULD_REPLACE);
  console.log("Write normalized rows:", SHOULD_WRITE_ROWS);
  console.log("Build ID:", buildId);

  console.log("\n[1/6] Loading vehicle price/current index...");
  const priceIndex = await loadVehiclePriceIndex(db);
  console.log("Vehicle price index keys:", priceIndex.size);

  console.log("\n[2/6] Loading vehicle_features docs...");
  const docs = await db.collection(SOURCE_FEATURES).find({}).toArray();
  console.log("Feature docs:", docs.length);

  console.log("\n[3/6] Normalizing feature rows and building matrix...");
  const allRows = [];
  const matrixByKey = new Map();
  const catalog = new Map();
  const unknownSections = new Map();

  for (const doc of docs) {
    const rows = extractRowsFromDoc(doc);
    allRows.push(...rows);

    for (const row of rows) {
      if (row.groupKey === "other") {
        const key = clean(row.rawSection || "blank");
        unknownSections.set(key, (unknownSections.get(key) || 0) + 1);
      }

      const matrixKey = `${row.modelKey}__${row.variantKey}`;

      if (!matrixByKey.has(matrixKey)) {
        const priceKey = `${row.modelKey}__${row.variantKey}`;
        const priceRows = priceIndex.get(priceKey) || [];
        const price = priceSummaryForVariant(priceRows);

        matrixByKey.set(matrixKey, {
          buildId,
          schemaVersion: FEATURE_SCHEMA_VERSION,
          sourceDocId: row.sourceDocId,
          brand: row.brand,
          make: row.brand,
          model: row.model,
          variant: row.variant,
          variantFull: row.variantFull,
          modelKey: row.modelKey,
          variantKey: row.variantKey,
          brandModelKey: row.brandModelKey,
          activePricelistMatched: price.activePricelistMatched,
          activeForFeatureExplorer: price.activeForFeatureExplorer,
          lifecycleStatus: price.lifecycleStatus,
          priceRowsMatched: price.priceRowsMatched,
          activePricelistRows: price.activePricelistRows,
          inactivePricelistRows: price.inactivePricelistRows,
          discontinuedPricelistMatched: price.discontinuedPricelistMatched,
          priceMin: price.priceMin,
          priceMax: price.priceMax,
          fuels: price.fuels,
          transmissions: price.transmissions,
          imageUrl: price.imageUrl,
          featureKeys: [],
          featuresByKey: {},
          quality: {
            rawFeatureRows: 0,
            conflicts: 0,
            generatedAt: nowIso(),
          },
          createdAt: new Date(),
          updatedAt: new Date(),
        });
      }

      const matrixDoc = matrixByKey.get(matrixKey);

      const feature = makeFeatureObject({
        canonicalKey: row.canonicalKey,
        displayName: row.displayName,
        groupKey: row.groupKey,
        groupLabel: row.groupLabel,
        section: row.section,
        rawSection: row.rawSection,
        rawFeatureName: row.rawFeatureName,
        sourceKey: row.sourceKey,
        value: row.value,
        rawValue: row.rawValue,
        available: row.available,
        sourceDocId: row.sourceDocId,
        synthetic: false,
        sourceFeatureKeys: [row.canonicalKey],
        rawDisplayName: row.rawDisplayName || "",
        excludeFromDefaultCompare: Boolean(row.excludeFromDefaultCompare),
        isNarrativeFeature: Boolean(row.isNarrativeFeature),
      });

      addFeatureToMatrix(matrixDoc, feature, catalog);
      matrixDoc.quality.rawFeatureRows += 1;
    }
  }

  console.log("Normalized rows:", allRows.length);
  console.log("Matrix variants:", matrixByKey.size);
  console.log("Catalog before synthetic:", catalog.size);

  console.log("\n[4/6] Adding safe DB-derived synthetic features...");
  const matrixDocs = [...matrixByKey.values()];

  for (const doc of matrixDocs) {
    addSyntheticFeatures(doc, catalog);

    doc.quality.conflicts = Object.values(doc.featuresByKey).reduce(
      (sum, feature) => sum + (feature.conflicts?.length || 0),
      0,
    );
  }

  console.log("Catalog after synthetic:", catalog.size);

  const catalogDocs = [...catalog.values()].map((entry) => ({
    buildId,
    schemaVersion: FEATURE_SCHEMA_VERSION,
    canonicalKey: entry.canonicalKey,
    displayName: entry.displayName,
    groupKey: entry.groupKey,
    groupLabel: entry.groupLabel,
    aliases: [...entry.aliases].sort(),
    rawFeatureNames: [...entry.rawFeatureNames].sort(),
    rawSections: [...entry.rawSections].sort(),
    sections: [...entry.sections].sort(),
    modelsCount: entry.models.size,
    variantsCount: entry.variants.size,
    rows: entry.rows,
    availableRows: entry.availableRows,
    synthetic: entry.synthetic,
    sourceFeatureKeys: [...entry.sourceFeatureKeys].slice(0, 100),
    examples: entry.examples,
    createdAt: new Date(),
    updatedAt: new Date(),
  }));

  const allConflictDocs = matrixDocs
    .flatMap((doc) =>
      Object.values(doc.featuresByKey)
        .filter((feature) =>
          feature.conflicts?.length ||
          feature.conflictStatus === "conflicted" ||
          feature.conflictStatus === "resolved"
        )
        .map((feature) => ({
          model: doc.model,
          variant: doc.variant,
          canonicalKey: feature.canonicalKey,
          displayName: feature.displayName,
          conflictStatus: feature.conflictStatus || "clean",
          conflictResolution: feature.conflictResolution || "",
          availabilityStatus: feature.availabilityStatus || "",
          conflicts: feature.conflicts || [],
          alternatives: feature.alternatives || [],
        })),
    );

  const conflictDocs = allConflictDocs
    .filter((row) => row.conflictStatus === "conflicted" || row.availabilityStatus === "conflicted")
    .slice(0, 5000);

  const resolvedConflictDocs = allConflictDocs
    .filter((row) => row.conflictStatus === "resolved")
    .slice(0, 5000);

  const unknownSectionDocs = [...unknownSections.entries()]
    .sort((a, b) => b[1] - a[1])
    .map(([section, count]) => ({ section, count }));

  const summary = {
    buildId,
    schemaVersion: FEATURE_SCHEMA_VERSION,
    generatedAt: nowIso(),
    source: {
      featureDocs: docs.length,
      normalizedRows: allRows.length,
      matrixVariants: matrixDocs.length,
      catalogFeatures: catalogDocs.length,
      unknownSections: unknownSectionDocs.length,
      conflicts: conflictDocs.length,
    },
    groups: catalogDocs.reduce((acc, item) => {
      acc[item.groupKey] = (acc[item.groupKey] || 0) + 1;
      return acc;
    }, {}),
    topCatalogFeatures: catalogDocs
      .sort((a, b) => b.rows - a.rows)
      .slice(0, 40)
      .map((item) => ({
        canonicalKey: item.canonicalKey,
        displayName: item.displayName,
        groupKey: item.groupKey,
        rows: item.rows,
        availableRows: item.availableRows,
        aliases: item.aliases.slice(0, 10),
      })),
  };

  console.log("\n[5/6] Writing audit files...");
  safeJsonWrite(path.join(OUT_DIR, "summary.json"), summary);
  safeJsonWrite(path.join(OUT_DIR, "unknown_sections.json"), unknownSectionDocs);
  safeJsonWrite(path.join(OUT_DIR, "feature_conflicts_sample.json"), conflictDocs);
  safeJsonWrite(path.join(OUT_DIR, "resolved_feature_conflicts_sample.json"), resolvedConflictDocs);
  safeJsonWrite(path.join(OUT_DIR, "catalog_sample.json"), catalogDocs.slice(0, 200));
  safeJsonWrite(path.join(OUT_DIR, "catalog_full.json"), catalogDocs);

  if (PROBE) {
    const preview = buildComparisonPreview({
      matrixDocs,
      model: "Creta",
      variants: ["E", "EX Diesel"],
    });

    safeJsonWrite(path.join(OUT_DIR, "comparison_preview_creta_e_vs_ex_diesel.json"), preview);
  }

  console.log("Audit dir:", OUT_DIR);
  console.log("Summary:", JSON.stringify(summary.source, null, 2));

  if (SHOULD_WRITE) {
    console.log("\n[6/6] Writing Mongo collections...");

    if (SHOULD_REPLACE) {
      console.log("Replacing previous v2 collections...");
      await db.collection(OUT_CATALOG).deleteMany({});
      await db.collection(OUT_MATRIX).deleteMany({});
      if (SHOULD_WRITE_ROWS) await db.collection(OUT_ROWS).deleteMany({});
    }

    const mongoCatalogDocs = SHOULD_SLIM_WRITE
      ? catalogDocs.map(toSlimCatalogDocForMongo)
      : catalogDocs;

    const mongoMatrixDocs = SHOULD_SLIM_WRITE
      ? matrixDocs.map((doc) => toSlimMatrixDocForMongo(doc, buildId))
      : matrixDocs;

    console.log("Mongo payload mode:", SHOULD_SLIM_WRITE ? "SLIM_RUNTIME" : "FULL_AUDIT");
    console.log("Writing catalog:", mongoCatalogDocs.length);
    for (const part of chunk(mongoCatalogDocs, 500)) {
      await db.collection(OUT_CATALOG).bulkWrite(
        part.map((doc) => ({
          updateOne: {
            filter: { canonicalKey: doc.canonicalKey },
            update: { $set: doc },
            upsert: true,
          },
        })),
        { ordered: false },
      );
    }

    console.log("Writing variant matrix:", mongoMatrixDocs.length);
    for (const part of chunk(mongoMatrixDocs, 300)) {
      await db.collection(OUT_MATRIX).bulkWrite(
        part.map((doc) => ({
          updateOne: {
            filter: {
              modelKey: doc.modelKey,
              variantKey: doc.variantKey,
            },
            update: { $set: doc },
            upsert: true,
          },
        })),
        { ordered: false },
      );
    }

    if (SHOULD_WRITE_ROWS) {
      console.log("Writing normalized rows:", allRows.length);
      const rowDocs = allRows.map((row) => ({
        ...row,
        buildId,
        schemaVersion: FEATURE_SCHEMA_VERSION,
        createdAt: new Date(),
        updatedAt: new Date(),
      }));

      for (const part of chunk(rowDocs, 1000)) {
        await db.collection(OUT_ROWS).insertMany(part, { ordered: false });
      }
    }

    console.log("Creating indexes...");
    await db.collection(OUT_CATALOG).createIndex({ canonicalKey: 1 }, { unique: true });
    await db.collection(OUT_CATALOG).createIndex({ aliases: 1 });
    await db.collection(OUT_CATALOG).createIndex({ groupKey: 1 });

    await db.collection(OUT_MATRIX).createIndex({ modelKey: 1, variantKey: 1 }, { unique: true });
    await db.collection(OUT_MATRIX).createIndex({ modelKey: 1 });
    await db.collection(OUT_MATRIX).createIndex({ "featureKeys": 1 });
    await db.collection(OUT_MATRIX).createIndex({ activePricelistMatched: 1 });

    if (SHOULD_WRITE_ROWS) {
      await db.collection(OUT_ROWS).createIndex({ modelKey: 1, variantKey: 1 });
      await db.collection(OUT_ROWS).createIndex({ canonicalKey: 1 });
      await db.collection(OUT_ROWS).createIndex({ groupKey: 1 });
    }

    await db.collection(OUT_BUILDS).insertOne({
      ...summary,
      createdAt: new Date(),
      collections: {
        catalog: OUT_CATALOG,
        matrix: OUT_MATRIX,
        rows: SHOULD_WRITE_ROWS ? OUT_ROWS : null,
      },
    });

    console.log("Mongo write complete.");
  } else {
    console.log("\n[6/6] Dry run complete. Add --write --replace to write Mongo collections.");
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
  process.exit(1);
});
