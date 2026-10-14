import mongoose from "mongoose";

// Custom loan-amount breakup components for the Home Loan module. Kept in its
// own collection so home-loan components don't show up in the car-loan
// breakup list (LoanBreakupField) and vice versa.
const homeLoanBreakupFieldSchema = new mongoose.Schema(
  {
    key: {
      type: String,
      required: true,
      trim: true,
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
  { timestamps: true, collection: "home_loan_breakup_fields" },
);

homeLoanBreakupFieldSchema.index({ active: 1, order: 1, createdAt: 1 });

const HomeLoanBreakupField =
  mongoose.models.HomeLoanBreakupField ||
  mongoose.model("HomeLoanBreakupField", homeLoanBreakupFieldSchema);

export default HomeLoanBreakupField;
