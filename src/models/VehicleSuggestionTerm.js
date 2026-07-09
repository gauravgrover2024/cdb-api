import mongoose from "mongoose";

// Manual "Type & Save" entries for the Make/Model/Variant autosuggest.
// Kept separate from the `vehicles` collection so the scraper (which
// discontinues anything it doesn't see in a run) never touches these.
const vehicleSuggestionTermSchema = mongoose.Schema(
  {
    level: { type: String, enum: ["make", "model", "variant"], required: true },

    // Full context, mirroring Vehicle's own fields, so this term can be
    // matched against scraped rows with the same query builder used everywhere
    // else. Only the fields up to `level` are populated:
    //   level=make    -> make
    //   level=model   -> make, model
    //   level=variant -> make, model, variant
    make: { type: String, required: true },
    model: { type: String },
    variant: { type: String },

    canonicalKey: { type: String, required: true }, // normalized dedupe key for this level's own value
    // "" for make; normalized make key for model; "make|model" normalized key for variant
    scopeKey: { type: String, default: "", required: true },

    source: { type: String, enum: ["manual"], default: "manual" },
    status: {
      type: String,
      enum: ["active", "merged", "hidden"],
      default: "active",
    },

    mergedIntoValue: { type: String },
    mergedAt: { type: Date },

    city: { type: String },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  },
  { timestamps: true },
);

vehicleSuggestionTermSchema.index(
  { level: 1, scopeKey: 1, canonicalKey: 1 },
  { unique: true },
);
vehicleSuggestionTermSchema.index({ level: 1, scopeKey: 1, status: 1 });

const VehicleSuggestionTerm = mongoose.model(
  "VehicleSuggestionTerm",
  vehicleSuggestionTermSchema,
);
export default VehicleSuggestionTerm;
