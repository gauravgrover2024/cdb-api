import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const SOURCE_COLLECTION = "vehicle_variant_feature_matrix_v2";
const OUT_COLLECTION = "aci_vehicle_model_feature_summary_v1";

const cleanText = (value = "") => String(value || "").replace(/\s+/g, " ").trim();

const normalizeText = (value = "") =>
  cleanText(value)
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const groupOf = (feature = {}) => normalizeText(feature.groupKey || feature.groupLabel || feature.section);

const isAvailable = (feature = {}) => {
  if (!feature || typeof feature !== "object") return false;
  if (feature.available === true || feature.isAvailable === true || feature.present === true) return true;

  const status = normalizeText([
    feature.availabilityStatus,
    feature.status,
    feature.value,
    feature.answer,
  ].filter(Boolean).join(" "));

  if (!status) return false;
  if (/\b(no|not available|unavailable|absent|false)\b/.test(status)) return false;

  return /\b(yes|available|standard|present|included|offered)\b/.test(status);
};

const displayNameFor = (key, feature = {}) =>
  cleanText(
    feature.displayName ||
    feature.display_name ||
    feature.label ||
    feature.name ||
    key.replace(/[_-]+/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
  );

const BORING_KEYS = new Set([
  "power_steering",
  "air_conditioning",
  "heater",
  "accessory_power_outlet",
  "tachometer",
  "displacement",
  "engine_displacement",
  "fuel_tank_capacity",
  "seating_capacity",
  "fuel_type",
  "transmission_type",
  "engine_type",
  "max_power",
  "max_torque",
  "number_of_cylinders",
  "valves_per_cylinder",
  "reported_boot_space",
  "boot_space",
  "tyre_size",
  "tyre_type",
  "usb_ports",
  "radio",
  "speakers",
  "antenna",
  "integrated_antenna",
]);

const PREMIUM_KEY_WEIGHTS = [
  [/adas|adaptive_cruise|lane|collision|blind_spot|driver_attention|high_beam|autonomous|emergency_brak/i, 100],
  [/airbags|airbag|esc|stability|traction|tpms|isofix|abs|brake_assist|hill|parking_sensors|camera|speed_alert/i, 85],
  [/sunroof|ventilated|powered_seat|electric_adjustable|climate|cruise_control|wireless|paddle|rear_ac|cooled_glovebox/i, 75],
  [/touchscreen|android_auto|apple_carplay|connected|bose|speaker|infotainment|bluetooth|navigation/i, 65],
  [/led|alloy|roof_rail|headlamp|tail_lamp|rear_spoiler/i, 45],
];

const scoreFeature = ({ key, label, groupKey, count, variantCount }) => {
  let score = 0;

  for (const [rx, weight] of PREMIUM_KEY_WEIGHTS) {
    if (rx.test(key) || rx.test(label)) score += weight;
  }

  const coverageRatio = variantCount ? count / variantCount : 0;

  if (groupKey.includes("adas")) score += 80;
  if (groupKey.includes("safety")) score += 55;
  if (groupKey.includes("comfort")) score += 35;
  if (groupKey.includes("infotainment")) score += 30;
  if (groupKey.includes("connected")) score += 30;
  if (groupKey.includes("exterior")) score += 15;

  score += Math.round(coverageRatio * 20);

  if (BORING_KEYS.has(key)) score -= 120;
  if (/capacity|displacement|torque|power|fuel|cylinder|valve|tank|dimension|length|width|height/i.test(key)) {
    score -= 40;
  }

  return score;
};

const topByGroup = (features, groupMatcher, limit = 8) =>
  features
    .filter((f) => groupMatcher(f))
    .sort((a, b) => b.score - a.score || b.count - a.count || a.label.localeCompare(b.label))
    .slice(0, limit);

const buildSummaryForModel = ({ rows }) => {
  const first = rows[0] || {};
  const variantCount = rows.length;
  const featureMap = new Map();

  for (const row of rows) {
    const byKey = row.featuresByKey || {};
    for (const [key, feature] of Object.entries(byKey)) {
      if (!isAvailable(feature)) continue;

      const label = displayNameFor(key, feature);
      if (!label) continue;

      const existing = featureMap.get(key) || {
        key,
        label,
        groupKey: groupOf(feature),
        groupLabel: cleanText(feature.groupLabel || feature.section || ""),
        count: 0,
        variants: [],
      };

      existing.count += 1;

      const variant = cleanText(row.variant || row.variantFull || row.variantName);
      if (variant && existing.variants.length < 8) existing.variants.push(variant);

      featureMap.set(key, existing);
    }
  }

  const features = [...featureMap.values()].map((feature) => ({
    ...feature,
    coverageRatio: variantCount ? Number((feature.count / variantCount).toFixed(3)) : 0,
    score: scoreFeature({ ...feature, variantCount }),
  }));

  const premiumHighlights = features
    .filter((f) => f.score > 0 && !BORING_KEYS.has(f.key))
    .sort((a, b) => b.score - a.score || b.count - a.count || a.label.localeCompare(b.label))
    .slice(0, 14);

  const safetyHighlights = topByGroup(
    features,
    (f) =>
      f.groupKey.includes("safety") ||
      /airbag|abs|esc|traction|tpms|isofix|hill|camera|parking|brake|speed_alert/i.test(f.key),
    10
  );

  const adasHighlights = topByGroup(
    features,
    (f) =>
      f.groupKey.includes("adas") ||
      /adas|adaptive|lane|collision|blind_spot|driver_attention|high_beam/i.test(f.key),
    10
  );

  const comfortHighlights = topByGroup(
    features,
    (f) =>
      f.groupKey.includes("comfort") ||
      /sunroof|ventilated|climate|cruise|wireless|rear_ac|powered|seat/i.test(f.key),
    10
  );

  const infotainmentHighlights = topByGroup(
    features,
    (f) =>
      f.groupKey.includes("infotainment") ||
      f.groupKey.includes("connected") ||
      /touchscreen|android|apple|bluetooth|speaker|connected|navigation|infotainment/i.test(f.key),
    10
  );

  return {
    make: cleanText(first.make || first.brand),
    brand: cleanText(first.brand || first.make),
    model: cleanText(first.model),
    modelKey: cleanText(first.modelKey),
    fullModel: [cleanText(first.make || first.brand), cleanText(first.model)].filter(Boolean).join(" "),
    activeVariantCount: variantCount,
    totalIndexedFeatureCount: features.length,
    premiumHighlights,
    safetyHighlights,
    adasHighlights,
    comfortHighlights,
    infotainmentHighlights,
    allFeatures: features
      .sort((a, b) => b.score - a.score || b.count - a.count || a.label.localeCompare(b.label))
      .slice(0, 80),
    sourceCollection: SOURCE_COLLECTION,
    sourceBuildIds: [...new Set(rows.map((r) => r.buildId).filter(Boolean))],
    builtAt: new Date(),
    updatedAt: new Date(),
  };
};

await connectDB();

const db = mongoose.connection.db;
const source = db.collection(SOURCE_COLLECTION);
const out = db.collection(OUT_COLLECTION);

await source.createIndex(
  { modelKey: 1, activeForFeatureExplorer: 1, variantKey: 1 },
  { name: "aci_feature_matrix_model_explorer_variant", background: true }
);

const modelKeys = await source.distinct("modelKey", {
  activeForFeatureExplorer: true,
  modelKey: { $exists: true, $ne: "" },
});

let written = 0;

for (const modelKey of modelKeys.sort()) {
  const rows = await source
    .find(
      { modelKey, activeForFeatureExplorer: true },
      {
        projection: {
          _id: 0,
          make: 1,
          brand: 1,
          model: 1,
          modelKey: 1,
          variant: 1,
          variantFull: 1,
          variantKey: 1,
          featuresByKey: 1,
          buildId: 1,
        },
      }
    )
    .toArray();

  if (!rows.length) continue;

  const summary = buildSummaryForModel({ rows });

  await out.updateOne(
    { modelKey },
    { $set: summary, $setOnInsert: { createdAt: new Date() } },
    { upsert: true }
  );

  written += 1;
}

await out.createIndex({ modelKey: 1 }, { name: "model_feature_summary_model_key", unique: true });
await out.createIndex({ make: 1, model: 1 }, { name: "model_feature_summary_make_model" });
await out.createIndex(
  { "allFeatures.key": 1, "allFeatures.count": 1, modelKey: 1 },
  { name: "model_feature_summary_feature_availability" },
);
await out.createIndex({ updatedAt: -1 }, { name: "model_feature_summary_updated" });

console.log(JSON.stringify({
  ok: true,
  collection: OUT_COLLECTION,
  sourceCollection: SOURCE_COLLECTION,
  sourceModels: modelKeys.length,
  written,
  indexes: await out.indexes(),
}, null, 2));

for (const modelKey of ["creta", "seltos", "scorpio-n", "scorpio_n", "baleno", "city"]) {
  const doc = await out.findOne({ modelKey }, { projection: { _id: 0 } });
  if (!doc) continue;

  console.log("\n--- SUMMARY", modelKey, "---");
  console.log(JSON.stringify({
    fullModel: doc.fullModel,
    activeVariantCount: doc.activeVariantCount,
    totalIndexedFeatureCount: doc.totalIndexedFeatureCount,
    premiumHighlights: doc.premiumHighlights?.slice(0, 8)?.map((f) => f.label),
    safetyHighlights: doc.safetyHighlights?.slice(0, 8)?.map((f) => f.label),
    adasHighlights: doc.adasHighlights?.slice(0, 8)?.map((f) => f.label),
    comfortHighlights: doc.comfortHighlights?.slice(0, 8)?.map((f) => f.label),
    infotainmentHighlights: doc.infotainmentHighlights?.slice(0, 8)?.map((f) => f.label),
  }, null, 2));
}

await mongoose.disconnect();
