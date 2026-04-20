import mongoose from "mongoose";

const insurancePayoutRateSchema = new mongoose.Schema(
  {
    companyName: {
      type: String,
      required: true,
      trim: true,
      index: true,
    },
    payoutPercentage: {
      type: Number,
      required: true,
      min: 0,
      max: 100,
      default: 10,
    },
    effectiveFrom: {
      type: Date,
      required: true,
      default: Date.now,
      index: true,
    },
    active: {
      type: Boolean,
      default: true,
      index: true,
    },
    notes: {
      type: String,
      trim: true,
      default: "",
    },
  },
  { timestamps: true },
);

insurancePayoutRateSchema.index({ companyName: 1, effectiveFrom: -1, active: 1 });

const InsurancePayoutRate = mongoose.model(
  "InsurancePayoutRate",
  insurancePayoutRateSchema,
);

export default InsurancePayoutRate;
