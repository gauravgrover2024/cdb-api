// src/models/VehicleSuggestionTerm.js
//
// Holds user-typed Make/Model/Variant terms that aren't in the scraper-owned
// `vehicles` collection yet. Kept in a separate collection so a manually
// typed term never gets silently wiped/overwritten by the next scrape.
import mongoose from "mongoose";

const vehicleSuggestionTermSchema = mongoose.Schema(
  {
    level: { type: String, enum: ["make", "model", "variant"], required: true },
    make: { type: String, default: "" },
    model: { type: String, default: "" },
    variant: { type: String, default: "" },
    makeNormalized: { type: String, default: "" },
    modelNormalized: { type: String, default: "" },
    variantNormalized: { type: String, default: "" },
    normalizedKey: { type: String, required: true },
    city: { type: String, default: "" },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User", default: null },
  },
  {
    timestamps: true,
    collection: "vehicle_suggestion_terms",
  },
);

vehicleSuggestionTermSchema.index({ normalizedKey: 1 }, { unique: true });
vehicleSuggestionTermSchema.index({ level: 1, makeNormalized: 1, modelNormalized: 1 });

const VehicleSuggestionTerm = mongoose.model(
  "VehicleSuggestionTerm",
  vehicleSuggestionTermSchema,
);
export default VehicleSuggestionTerm;
