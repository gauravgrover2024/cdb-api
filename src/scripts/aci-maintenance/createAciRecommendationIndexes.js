import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

await connectDB();

const db = mongoose.connection.db;

const stableKey = (value = {}) => JSON.stringify(value);

const ensure = async (collection, keys, options = {}) => {
  const col = db.collection(collection);
  const requestedName =
    options.name || Object.entries(keys).map(([key, value]) => `${key}_${value}`).join("_");

  console.log(`Creating index on ${collection}:`, keys);

  try {
    await col.createIndex(keys, {
      background: true,
      ...options,
    });
  } catch (error) {
    if (error?.code !== 85 && error?.codeName !== "IndexOptionsConflict") {
      throw error;
    }

    const indexes = await col.indexes();
    const existing = indexes.find((index) => stableKey(index.key) === stableKey(keys));

    if (existing) {
      console.log(
        `Index already covered on ${collection}: requested=${requestedName}, existing=${existing.name}`,
      );
      return;
    }

    throw error;
  }
};

await ensure("aci_vehicle_price_rows", {
  citySlug: 1,
  fuelKey: 1,
  transmissionKey: 1,
  exShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "aci_price_rows_city_fuel_trans_price_model",
});

await ensure("aci_vehicle_price_rows", {
  citySlug: 1,
  bodyTypeKey: 1,
  exShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "aci_price_rows_city_body_price_model",
});

await ensure("aci_vehicle_price_rows", {
  citySlug: 1,
  fuelKey: 1,
  exShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "aci_price_rows_city_fuel_price_model",
});

await ensure("aci_vehicle_model_summary", {
  citySlug: 1,
  minExShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "aci_model_summary_city_price_model",
});

await ensure("aci_vehicle_model_summary", {
  citySlug: 1,
  bodyTypeKey: 1,
  minExShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "aci_model_summary_city_body_price_model",
});

await ensure("vehicle_variant_feature_matrix_v2", {
  activePricelistMatched: 1,
  modelKey: 1,
  priceMin: 1,
}, {
  name: "matrix_active_model_price",
});

await ensure("vehicle_variant_feature_matrix_v2", {
  activePricelistMatched: 1,
  "featuresByKey.global_ncap_safety_rating.available": 1,
  priceMin: 1,
  modelKey: 1,
}, {
  name: "matrix_active_gncap_price_model",
});

await ensure("vehicle_variant_feature_matrix_v2", {
  activePricelistMatched: 1,
  "featuresByKey.six_airbags.available": 1,
  priceMin: 1,
  modelKey: 1,
}, {
  name: "matrix_active_six_airbags_price_model",
});

await ensure("vehicle_variant_feature_matrix_v2", {
  activePricelistMatched: 1,
  "featuresByKey.electronic_stability_control_esc.available": 1,
  priceMin: 1,
  modelKey: 1,
}, {
  name: "matrix_active_esc_price_model",
});

await ensure("vehicle_variant_feature_matrix_v2", {
  activePricelistMatched: 1,
  "featuresByKey.isofix_child_seat_mounts.available": 1,
  priceMin: 1,
  modelKey: 1,
}, {
  name: "matrix_active_isofix_price_model",
});

await ensure("aci_vehicle_variant_decision_profile", {
  variantProfileKey: 1,
}, {
  name: "decision_profile_variant_key",
  unique: true,
});

await ensure("aci_vehicle_variant_decision_profile", {
  referencePriceCitySlug: 1,
  referenceExShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "decision_profile_reference_city_price_model",
});

await ensure("aci_vehicle_variant_decision_profile", {
  referencePriceCitySlug: 1,
  bodyTypeKey: 1,
  referenceExShowroomPrice: 1,
}, {
  name: "decision_profile_reference_city_body_price",
});

await ensure("aci_vehicle_variant_decision_profile", {
  makeKey: 1,
  modelKey: 1,
  fuelTransmissionFamilyKey: 1,
  priceRank: 1,
}, {
  name: "decision_profile_model_family_rank",
});

await ensure("aci_vehicle_variant_decision_profile", {
  modelKey: 1,
  fuelKey: 1,
  transmissionKey: 1,
  referenceExShowroomPrice: 1,
}, {
  name: "decision_profile_model_powertrain_price",
});

await ensure("aci_vehicle_variant_city_price_profile", {
  citySlug: 1,
  exShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "city_price_profile_city_price_model",
});

await ensure("aci_vehicle_variant_city_price_profile", {
  variantProfileKey: 1,
  citySlug: 1,
}, {
  name: "city_price_profile_variant_city",
  unique: true,
});

console.log("ACI recommendation indexes created.");
await mongoose.disconnect();
