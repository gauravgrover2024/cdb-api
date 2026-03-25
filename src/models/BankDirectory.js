import mongoose from "mongoose";

const bankDirectorySchema = new mongoose.Schema(
  {
    ifsc: {
      type: String,
      trim: true,
      uppercase: true,
      index: true,
      sparse: true,
      unique: true,
    },
    micr: {
      type: String,
      trim: true,
      index: true,
    },
    bankName: {
      type: String,
      trim: true,
      index: true,
    },
    branch: {
      type: String,
      trim: true,
    },
    address: {
      type: String,
      trim: true,
    },
    city: {
      type: String,
      trim: true,
    },
    state: {
      type: String,
      trim: true,
    },
    district: {
      type: String,
      trim: true,
    },
    contact: {
      type: String,
      trim: true,
    },
    active: {
      type: Boolean,
      default: true,
      index: true,
    },
    source: {
      type: String,
      trim: true,
      default: "cache",
    },
    lastVerifiedAt: {
      type: Date,
      default: Date.now,
      index: true,
    },
    raw: {
      type: mongoose.Schema.Types.Mixed,
      default: null,
    },
  },
  {
    timestamps: true,
  },
);

bankDirectorySchema.index({ micr: 1, bankName: 1 });
// Compound index that exactly matches the sort used in getAllBanks
// { bankName:1, ifsc:1 } — lets MongoDB satisfy the sort via the index
// so it never needs an in-memory sort (avoids the 32 MB limit).
bankDirectorySchema.index({ bankName: 1, ifsc: 1 });

const BankDirectory = mongoose.model("BankDirectory", bankDirectorySchema);
export default BankDirectory;
