import mongoose from "mongoose";

const aciLeadSchema = new mongoose.Schema(
  {
    leadId: { type: String, required: true, unique: true, index: true },
    source: { type: String, default: "ACI Assist" },
    sourceChannel: { type: String, default: "customer_portal" },
    leadType: {
      type: String,
      enum: [
        "quotation",
        "test_drive",
        "callback",
        "finance_callback",
        "offer_enquiry",
      ],
      required: true,
      index: true,
    },
    customer: {
      name: String,
      mobile: String,
      email: String,
      city: String,
      pincode: String,
    },
    vehicle: {
      brand: String,
      model: String,
      variant: String,
      fuel: String,
      transmission: String,
      color: String,
      city: String,
    },
    priceContext: {
      onRoadPrice: Number,
      exShowroom: Number,
      rto: Number,
      insurance: Number,
      optionalItems: { type: Array, default: [] },
      optionalTotal: Number,
      otherItems: { type: Array, default: [] },
      otherTotal: Number,
      priceBreakupLines: { type: Array, default: [] },
      priceIntegrity: {
        isValid: Boolean,
        difference: Number,
        warnings: [String],
      },
    },
    financeContext: {
      interested: Boolean,
      downPayment: Number,
      loanAmount: Number,
      tenureMonths: Number,
      roi: Number,
      estimatedEmi: Number,
      preferredBank: String,
    },
    tradeInContext: {
      hasExchange: Boolean,
      make: String,
      model: String,
      year: String,
      registrationNumber: String,
      expectedPrice: Number,
    },
    offerContext: {
      offerConfirmationRequired: Boolean,
      requestedOfferTypes: [String],
    },
    conversationContext: {
      originalQuery: String,
      lastIntent: String,
      stage: String,
      buyingSignals: [String],
      userProfile: mongoose.Schema.Types.Mixed,
      selectedSuggestions: { type: Array, default: [] },
    },
    assignment: {
      assignedTo: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
      assignedToName: String,
      showroomId: { type: mongoose.Schema.Types.ObjectId, ref: "Showroom" },
      showroomName: String,
    },
    status: {
      type: String,
      enum: [
        "new",
        "contacted",
        "quotation_requested",
        "quotation_sent",
        "test_drive_requested",
        "test_drive_booked",
        "converted",
        "lost",
      ],
      default: "new",
      index: true,
    },
    priority: {
      type: String,
      enum: ["cold", "warm", "hot"],
      default: "warm",
      index: true,
    },
    notes: { type: Array, default: [] },
    activities: { type: Array, default: [] },
    linkedCustomerId: { type: mongoose.Schema.Types.ObjectId, ref: "Customer" },
    linkedLoanId: { type: mongoose.Schema.Types.ObjectId, ref: "Loan" },
    linkedQuotationId: { type: mongoose.Schema.Types.ObjectId, ref: "Quotation" },
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  },
  {
    timestamps: true,
    collection: "aci_leads",
  },
);

aciLeadSchema.index({ "customer.mobile": 1 });
aciLeadSchema.index({ "vehicle.model": 1 });
aciLeadSchema.index({ createdAt: -1 });

const AciLead = mongoose.models.AciLead || mongoose.model("AciLead", aciLeadSchema);

export default AciLead;
