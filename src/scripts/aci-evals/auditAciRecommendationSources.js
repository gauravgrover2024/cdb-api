import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const collections = [
  "aci_vehicle_model_summary",
  "aci_vehicle_price_rows",
  "vehicle_feature_catalog_v2",
  "vehicle_variant_feature_matrix_v2",
  "vehicle_colors_v2",
];

const pick = (doc = {}, keys = []) =>
  Object.fromEntries(keys.map((key) => [key, doc?.[key]]).filter(([, value]) => value !== undefined));

const fieldCounts = (docs = []) => {
  const counts = {};
  for (const doc of docs) {
    for (const key of Object.keys(doc || {})) {
      counts[key] = (counts[key] || 0) + 1;
    }
  }
  return Object.entries(counts)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 80)
    .map(([field, count]) => ({ field, count }));
};

await connectDB();
const db = mongoose.connection.db;

for (const name of collections) {
  const exists = await db.listCollections({ name }).hasNext();
  console.log("\n==============================");
  console.log(name, exists ? "FOUND" : "MISSING");

  if (!exists) continue;

  const total = await db.collection(name).estimatedDocumentCount();
  const sample = await db.collection(name).find({}).limit(8).toArray();
  const indexes = await db.collection(name).indexes();

  console.log(JSON.stringify({
    total,
    indexes: indexes.map((idx) => ({
      name: idx.name,
      key: idx.key,
    })),
    fields: fieldCounts(sample),
    sample: sample.slice(0, 3),
  }, null, 2));
}

console.log("\n==============================");
console.log("Recommendation-specific probes");

const modelSummaryProbe = await db.collection("aci_vehicle_model_summary")
  .find({
    $or: [
      { model: /creta/i },
      { model: /seltos/i },
      { model: /swift/i },
      { model: /baleno/i },
      { model: /city/i },
      { model: /verna/i },
    ],
  })
  .limit(20)
  .toArray();

console.log("MODEL SUMMARY PROBE");
console.log(JSON.stringify(modelSummaryProbe.map((doc) => pick(doc, [
  "make", "brand", "model", "fullModel", "modelKey", "brandModelKey",
  "citySlug", "bodyType", "bodyTypeBucket", "segment", "minExShowroomPrice",
  "minOnRoadPrice", "maxOnRoadPrice", "variantCount", "fuelTypes",
  "transmissions", "features", "safetyRating", "popularityScore",
])), null, 2));

const featureProbe = await db.collection("vehicle_feature_catalog_v2")
  .find({
    $or: [
      { featureKey: { $in: ["sunroof", "adas_package", "six_airbags", "ventilated_seats", "anti_lock_braking_system_abs"] } },
      { key: { $in: ["sunroof", "adas_package", "six_airbags", "ventilated_seats", "anti_lock_braking_system_abs"] } },
      { displayName: /sunroof|ADAS|airbag|ventilated|ABS/i },
      { featureName: /sunroof|ADAS|airbag|ventilated|ABS/i },
    ],
  })
  .limit(40)
  .toArray();

console.log("FEATURE PROBE");
console.log(JSON.stringify(featureProbe.map((doc) => pick(doc, [
  "featureKey", "key", "displayName", "featureName", "category", "aliases", "canonicalKey"
])), null, 2));

const matrixProbe = await db.collection("vehicle_variant_feature_matrix_v2")
  .find({
    $or: [
      { model: /creta/i },
      { model: /seltos/i },
      { model: /swift/i },
      { model: /baleno/i },
      { featureKey: /adas|sunroof|airbag|ventilated|abs/i },
    ],
  })
  .limit(20)
  .toArray();

console.log("MATRIX PROBE");
console.log(JSON.stringify(matrixProbe.map((doc) => pick(doc, [
  "make", "brand", "model", "fullModel", "modelKey", "brandModelKey",
  "variant", "variantName", "variantKey", "fuel", "fuelType",
  "transmission", "featureKey", "key", "featureName", "displayName",
  "availability", "value", "citySlug"
])), null, 2));

await mongoose.disconnect();
