// src/models/VehicleFeature.js
import mongoose from "mongoose";

const vehicleFeatureSchema = mongoose.Schema(
  {
    brand: { type: String, required: true },
    model: { type: String, required: true },
    variant: { type: String, required: true },
    body_type_bucket: { type: String, default: "" },
    seating_capacity: { type: Number, default: null },
    features: {
      type: mongoose.Schema.Types.Mixed,
      default: {},
    },
  },
  {
    timestamps: true,
    strict: false,
    collection: "vehicle_features",
  },
);

// Indexes for fast brand-scoped queries (getVariantsWithPriceAndFeatures filters by brand first)
vehicleFeatureSchema.index({ brand: 1 });
vehicleFeatureSchema.index({ brand: 1, model: 1 });
vehicleFeatureSchema.index({ brand: 1, model: 1, variant: 1 }, { unique: true, name: "brand_model_variant_unique" });
vehicleFeatureSchema.index({ body_type_bucket: 1, seating_capacity: 1, brand: 1, model: 1 });
vehicleFeatureSchema.index({ body_type_bucket: 1, seating_capacity: 1 });
vehicleFeatureSchema.index(
  { brand: "text", model: "text", variant: "text" },
  { name: "vehicle_feature_text" },
);

const VehicleFeature = mongoose.model("VehicleFeature", vehicleFeatureSchema);
export default VehicleFeature;
