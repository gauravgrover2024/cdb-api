// src/models/VehicleFeature.js
import mongoose from "mongoose";

const vehicleFeatureSchema = mongoose.Schema(
  {
    brand: { type: String, required: true }, // "Audi"
    model: { type: String, required: true }, // "A6"
    variant: { type: String, required: true }, // "Audi A6 45 TFSI Premium Plus"
    features: {
      type: mongoose.Schema.Types.Mixed, // { "Comfort & Convenience | Power Steering": "Yes", ... }
      default: {},
    },
  },
  {
    timestamps: true,
    strict: false,
    collection: "vehicle_features", // important: uses existing collection
  },
);

const VehicleFeature = mongoose.model("VehicleFeature", vehicleFeatureSchema);
export default VehicleFeature;
