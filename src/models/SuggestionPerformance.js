import mongoose from "mongoose";

const suggestionPerformanceSchema = new mongoose.Schema(
  {
    intent: { type: String, required: true, index: true },
    suggestionId: { type: String, required: true, index: true },
    impressions: { type: Number, default: 0 },
    clicks: { type: Number, default: 0 },
    globalCtr: { type: Number, default: 0 },
  },
  {
    timestamps: true,
  },
);

suggestionPerformanceSchema.index(
  { intent: 1, suggestionId: 1 },
  { unique: true, name: "intent_suggestion_unique" },
);

const SuggestionPerformance = mongoose.model(
  "SuggestionPerformance",
  suggestionPerformanceSchema,
);

export default SuggestionPerformance;
