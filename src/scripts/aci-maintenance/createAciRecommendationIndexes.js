import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";

await connectDB();

const db = mongoose.connection.db;

const ensure = async (collection, keys, options = {}) => {
  console.log(`Creating index on ${collection}:`, keys);
  await db.collection(collection).createIndex(keys, {
    background: true,
    ...options,
  });
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

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  minExShowroomPrice: 1,
  modelKey: 1,
}, {
  name: "decision_profile_city_price_model",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  safetyScore: -1,
  minExShowroomPrice: 1,
}, {
  name: "decision_profile_city_safety_price",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  familyScore: -1,
  minExShowroomPrice: 1,
}, {
  name: "decision_profile_city_family_price",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  valueScore: -1,
  minExShowroomPrice: 1,
}, {
  name: "decision_profile_city_value_price",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  bodyTypeKey: 1,
  minExShowroomPrice: 1,
}, {
  name: "decision_profile_city_body_price",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  fuels: 1,
  minExShowroomPrice: 1,
}, {
  name: "decision_profile_city_fuels_price",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  transmissions: 1,
  minExShowroomPrice: 1,
}, {
  name: "decision_profile_city_transmissions_price",
});

await ensure("aci_vehicle_decision_profile", {
  citySlug: 1,
  similarModelKeys: 1,
}, {
  name: "decision_profile_city_similar_models",
});

console.log("ACI recommendation indexes created.");
await mongoose.disconnect();
