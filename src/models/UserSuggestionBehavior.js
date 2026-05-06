import mongoose from "mongoose";

const userSuggestionBehaviorSchema = new mongoose.Schema(
  {
    userId: { type: String, required: true, index: true },
    intent: { type: String, required: true, index: true },
    suggestionId: { type: String, required: true, index: true },
    impressions: { type: Number, default: 0 },
    clicks: { type: Number, default: 0 },
    userCtr: { type: Number, default: 0 },
    lastUsed: { type: Date, default: Date.now },
  },
  {
    timestamps: true,
  },
);

userSuggestionBehaviorSchema.index(
  { userId: 1, intent: 1, suggestionId: 1 },
  { unique: true, name: "user_intent_suggestion_unique" },
);

const UserSuggestionBehavior = mongoose.model(
  "UserSuggestionBehavior",
  userSuggestionBehaviorSchema,
);

export default UserSuggestionBehavior;
