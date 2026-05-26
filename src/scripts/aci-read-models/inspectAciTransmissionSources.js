import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

const clean = (v = "") => String(v || "").trim();

const show = (title, value) => {
  console.log("\n==============================");
  console.log(title);
  console.log("==============================");
  console.log(JSON.stringify(value, null, 2));
};

await connectDB();

const db = mongoose.connection.db;

const collections = [
  "vehicles",
  "vehicle_features",
  "vehicle_variant_feature_matrix_v2",
  "vehicle_feature_catalog_v2",
];

for (const collectionName of collections) {
  const exists = await db.listCollections({ name: collectionName }, { nameOnly: true }).hasNext();
  if (!exists) {
    show(collectionName, { exists: false });
    continue;
  }

  const rows = await db.collection(collectionName)
    .find({
      $or: [
        { model: /verna/i },
        { modelName: /verna/i },
        { model_name: /verna/i },
        { fullModel: /verna/i },
        { variant: /verna/i },
        { variantName: /verna/i },
        { variant_name: /verna/i },
        { "raw.modelName": /verna/i },
        { "raw_price_json": /verna/i },
      ],
    })
    .limit(30)
    .toArray();

  const samples = rows.map((row) => ({
    _id: String(row._id),
    model: row.model,
    modelName: row.modelName,
    variant: row.variant,
    variantName: row.variantName,
    variant_name: row.variant_name,

    transmission: row.transmission,
    transmissionType: row.transmissionType,
    transmission_type: row.transmission_type,
    gearbox: row.gearbox,

    fuel: row.fuel,
    fuelType: row.fuelType,
    fuel_type: row.fuel_type,

    keys: Object.keys(row).slice(0, 80),

    rawKeys: row.raw && typeof row.raw === "object" ? Object.keys(row.raw).slice(0, 80) : [],
    rawTransmission: row.raw?.transmission || row.raw?.transmissionType || row.raw?.gearbox,

    featuresKeys:
      row.features && typeof row.features === "object"
        ? Object.keys(row.features).slice(0, 80)
        : [],

    featuresByKeyTransmission:
      row.featuresByKey?.transmission ||
      row.featuresByKey?.gearbox ||
      row.featuresByKey?.automatic_transmission ||
      row.featuresByKey?.manual_transmission ||
      null,

    raw_price_json:
      typeof row.raw_price_json === "string"
        ? row.raw_price_json.slice(0, 1200)
        : row.raw_price_json || null,
  }));

  show(collectionName, {
    countSampled: rows.length,
    samples,
  });
}

await mongoose.disconnect();
