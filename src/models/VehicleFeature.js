// src/models/VehicleFeature.js
import mongoose from "mongoose";

const vehicleFeatureSchema = mongoose.Schema(
  {
    brand: { type: String, required: true },
    model: { type: String, required: true },
    variant: { type: String, required: true },
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

const VehicleFeature = mongoose.model("VehicleFeature", vehicleFeatureSchema);
export default VehicleFeature;
