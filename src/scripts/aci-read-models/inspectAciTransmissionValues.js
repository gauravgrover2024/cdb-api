import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

await connectDB();

const db = mongoose.connection.db;

const rows = await db.collection("vehicle_variant_feature_matrix_v2")
  .find(
    {
      modelKey: "verna",
      activeForFeatureExplorer: true,
    },
    {
      projection: {
        modelKey: 1,
        variantKey: 1,
        model: 1,
        variant: 1,
        variantFull: 1,
        transmissions: 1,
        fuels: 1,
        "featuresByKey.transmission": 1,
        "featuresByKey.gearbox": 1,
        "featuresByKey.automatic_transmission": 1,
        "featuresByKey.manual_transmission": 1,
      },
    },
  )
  .sort({ variant: 1 })
  .toArray();

console.log(JSON.stringify({
  count: rows.length,
  rows: rows.map((row) => ({
    _id: String(row._id),
    modelKey: row.modelKey,
    variantKey: row.variantKey,
    model: row.model,
    variant: row.variant,
    variantFull: row.variantFull,
    transmissions: row.transmissions || [],
    fuels: row.fuels || [],
    transmissionFeature: row.featuresByKey?.transmission || null,
    gearboxFeature: row.featuresByKey?.gearbox || null,
    automaticTransmissionFeature: row.featuresByKey?.automatic_transmission || null,
    manualTransmissionFeature: row.featuresByKey?.manual_transmission || null,
  })),
}, null, 2));

await mongoose.disconnect();
