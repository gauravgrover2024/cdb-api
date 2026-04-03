import mongoose from "mongoose";

const receivableSchema = new mongoose.Schema(
  {
    receivableKind: {
      type: String,
      trim: true,
      default: "loan",
      index: true,
    }, // Loan | Commission | Insurance | ...
    sourceModule: {
      type: String,
      trim: true,
      default: "loan",
      index: true,
    }, // LoanForm | Payments | DeliveryOrder | ...
    loanId: { type: String, required: true, trim: true, index: true },
    loanMongoId: { type: mongoose.Schema.Types.ObjectId, ref: "Loan" },
    customerName: { type: String, trim: true, default: "" },
    payoutId: { type: String, required: true, trim: true },
    sourceArrayKey: { type: String, trim: true, default: "loan_receivables" },
    payload: { type: mongoose.Schema.Types.Mixed, default: {} },

    payout_type: { type: String, trim: true, default: "" },
    payout_party_name: { type: String, trim: true, default: "" },
    payout_direction: { type: String, trim: true, default: "" },
    payout_status: { type: String, trim: true, default: "" },
    payout_percentage: { type: String, trim: true, default: "" },
    payout_amount: { type: Number, default: 0 },
    net_payout_amount: { type: Number, default: 0 },
    tds_amount: { type: Number, default: 0 },
    tds_percentage: { type: Number, default: 0 },
    payout_received_date: { type: mongoose.Schema.Types.Mixed, default: null },
    created_date: { type: mongoose.Schema.Types.Mixed, default: null },
    payout_createdAt: { type: mongoose.Schema.Types.Mixed, default: null },
    payment_history: { type: [mongoose.Schema.Types.Mixed], default: [] },
    activity_log: { type: [mongoose.Schema.Types.Mixed], default: [] },
    meta_source: { type: String, trim: true, default: "" },
  },
  {
    timestamps: true,
    collection: "receivables",
  },
);

receivableSchema.index(
  { loanId: 1, payoutId: 1 },
  { unique: true, name: "loanId_payoutId_unique" },
);
receivableSchema.index({ payoutId: 1 }, { name: "payoutId_1" });
receivableSchema.index(
  { payout_status: 1, updatedAt: -1 },
  { name: "payout_status_updatedAt" },
);
receivableSchema.index(
  { payout_party_name: 1, payout_status: 1, updatedAt: -1 },
  { name: "party_status_updatedAt" },
);
receivableSchema.index(
  { payout_type: 1, payout_status: 1, updatedAt: -1 },
  { name: "type_status_updatedAt" },
);
receivableSchema.index(
  { meta_source: 1, updatedAt: -1 },
  { name: "meta_source_updatedAt" },
);
receivableSchema.index(
  { payout_received_date: -1, updatedAt: -1 },
  { name: "payout_received_date_updatedAt" },
);
receivableSchema.index(
  { created_date: -1, updatedAt: -1 },
  { name: "created_date_updatedAt" },
);
receivableSchema.index(
  { receivableKind: 1, sourceModule: 1, payout_status: 1, updatedAt: -1 },
  { name: "kind_source_status_updatedAt" },
);

const Receivable = mongoose.model("Receivable", receivableSchema);

export default Receivable;
