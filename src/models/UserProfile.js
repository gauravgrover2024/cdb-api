import mongoose from "mongoose";

const userProfileSchema = new mongoose.Schema(
  {
    userId: { type: String, required: true, unique: true, index: true },
    preferredBudget: { type: Number, default: null },
    preferredBodyType: { type: String, default: null },
    preferredFuel: { type: String, default: null },
    preferredTransmission: { type: String, default: null },
    buyingPriority: { type: String, default: null },
    intentAffinity: { type: mongoose.Schema.Types.Mixed, default: {} },
    intentAffinity: {
      type: Map,
      of: Number,
      default: {},
    },
    lastActive: { type: Date, default: Date.now },
  },
  {
    timestamps: true,
  },
);

const UserProfile = mongoose.model("UserProfile", userProfileSchema);

export default UserProfile;
