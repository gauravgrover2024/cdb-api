import mongoose from "mongoose";

const loanBreakupFieldSchema = new mongoose.Schema(
  {
    key: {
      type: String,
      required: true,
      trim: true,
      lowercase: true,
      unique: true,
    },
    label: {
      type: String,
      required: true,
      trim: true,
    },
    order: {
      type: Number,
      default: 1000,
    },
    active: {
      type: Boolean,
      default: true,
    },
    createdBy: {
      type: String,
      default: "system",
    },
  },
  { timestamps: true },
);

loanBreakupFieldSchema.index({ key: 1 }, { unique: true, name: "loan_breakup_field_key_1" });
loanBreakupFieldSchema.index({ active: 1, order: 1, createdAt: 1 });

const LoanBreakupField = mongoose.model("LoanBreakupField", loanBreakupFieldSchema);

export default LoanBreakupField;

