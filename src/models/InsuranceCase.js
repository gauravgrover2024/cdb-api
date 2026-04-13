import mongoose from "mongoose";

const insuranceQuoteSchema = new mongoose.Schema(
  {
    id: { type: Number, required: true },
    insuranceCompany: { type: String, default: "" },
    coverageType: { type: String, default: "" },
    vehicleIdv: { type: Number, default: 0 },
    cngIdv: { type: Number, default: 0 },
    accessoriesIdv: { type: Number, default: 0 },
    policyDuration: { type: String, default: "" },
    ncbDiscount: { type: Number, default: 0 },
    odAmount: { type: Number, default: 0 },
    thirdPartyAmount: { type: Number, default: 0 },
    addOnsAmount: { type: Number, default: 0 },
    addOns: { type: Object, default: {} },
    totalIdv: { type: Number, default: 0 },
    addOnsTotal: { type: Number, default: 0 },
    taxableAmount: { type: Number, default: 0 },
    gstAmount: { type: Number, default: 0 },
    totalPremium: { type: Number, default: 0 },
    isAccepted: { type: Boolean, default: false },
  },
  { _id: false },
);

const insuranceDocumentSchema = new mongoose.Schema(
  {
    id: { type: String, required: true },
    name: { type: String, default: "" },
    size: { type: Number, default: 0 },
    type: { type: String, default: "" },
    tag: { type: String, default: "" },
  },
  { _id: false },
);

const paymentHistorySchema = new mongoose.Schema(
  {
    amount: { type: Number, required: true },
    date: { type: Date, required: true },
    paymentType: { 
      type: String, 
      enum: ["customer", "inhouse"], 
      required: true 
    },
    paymentMode: { 
      type: String, 
      enum: ["Cash", "Cheque", "NEFT", "RTGS", "UPI", "Card", "Other"],
      default: "Cash"
    },
    bankName: { type: String, default: "" },
    transactionRef: { type: String, default: "" },
    remarks: { type: String, default: "" },
    receivableId: { type: mongoose.Schema.Types.ObjectId, ref: "Receivable" },
    recordedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    recordedAt: { type: Date, default: Date.now },
  },
  { _id: true, timestamps: true },
);

const customerSnapshotSchema = new mongoose.Schema(
  {
    customerName: { type: String, default: "" },
    primaryMobile: { type: String, default: "" },
    email: { type: String, default: "" },
    panNumber: { type: String, default: "" },
    residenceAddress: { type: String, default: "" },
    pincode: { type: String, default: "" },
    city: { type: String, default: "" },
  },
  { _id: false },
);

const insuranceCaseSchema = new mongoose.Schema(
  {
    caseId: { type: String, required: true, unique: true, index: true },

    // Optional linkage to Customers module
    customerId: { type: mongoose.Schema.Types.ObjectId, ref: "Customer" },
    customerSnapshot: { type: customerSnapshotSchema, default: {} },

    // Workflow
    status: {
      type: String,
      enum: ["draft", "submitted", "issued", "cancelled"],
      default: "draft",
    },
    currentStep: { type: Number, default: 1 },

    // Step 1: meta + customer info
    buyerType: { type: String, default: "Individual" },
    vehicleType: { type: String, default: "New Car" },
    policyDoneBy: { type: String, default: "Autocredits India LLP" },
    brokerName: { type: String, default: "" },
    sourceOrigin: { type: String, default: "" },
    employeeName: { type: String, default: "" },

    customerName: { type: String, default: "" },
    companyName: { type: String, default: "" },
    contactPersonName: { type: String, default: "" },
    mobile: { type: String, default: "" },
    alternatePhone: { type: String, default: "" },
    email: { type: String, default: "" },
    gender: { type: String, default: "" },
    panNumber: { type: String, default: "" },
    aadhaarNumber: { type: String, default: "" },
    gstNumber: { type: String, default: "" },
    residenceAddress: { type: String, default: "" },
    pincode: { type: String, default: "" },
    city: { type: String, default: "" },

    nomineeName: { type: String, default: "" },
    nomineeRelationship: { type: String, default: "" },
    nomineeAge: { type: String, default: "" },
    referenceName: { type: String, default: "" },
    referencePhone: { type: String, default: "" },

    // Step 2: vehicle details
    registrationNumber: { type: String, default: "" },
    vehicleMake: { type: String, default: "" },
    vehicleModel: { type: String, default: "" },
    vehicleVariant: { type: String, default: "" },
    cubicCapacity: { type: String, default: "" },
    engineNumber: { type: String, default: "" },
    chassisNumber: { type: String, default: "" },
    typesOfVehicle: { type: String, default: "Four Wheeler" },
    manufactureMonth: { type: String, default: "" },
    manufactureYear: { type: String, default: "" },

    // Step 3: previous policy (renewals)
    previousInsuranceCompany: { type: String, default: "" },
    previousPolicyNumber: { type: String, default: "" },
    previousPolicyType: { type: String, default: "" },
    previousPolicyStartDate: { type: String, default: "" },
    previousPolicyDuration: { type: String, default: "" },
    previousOdExpiryDate: { type: String, default: "" },
    previousTpExpiryDate: { type: String, default: "" },
    claimTakenLastYear: { type: String, default: "" },
    previousNcbDiscount: { type: Number, default: 0 },
    previousHypothecation: { type: String, default: "" },
    previousRemarks: { type: String, default: "" },

    // Step 4: quotes
    quotes: { type: [insuranceQuoteSchema], default: [] },
    acceptedQuoteId: { type: Number, default: null },

    // Step 5: new policy details
    newInsuranceCompany: { type: String, default: "" },
    newPolicyType: { type: String, default: "" },
    newPolicyNumber: { type: String, default: "" },
    newIssueDate: { type: String, default: "" },
    newPolicyStartDate: { type: String, default: "" },
    newInsuranceDuration: { type: String, default: "" },
    newOdExpiryDate: { type: String, default: "" },
    newTpExpiryDate: { type: String, default: "" },
    newNcbDiscount: { type: Number, default: 0 },
    newIdvAmount: { type: Number, default: 0 },
    newTotalPremium: { type: Number, default: 0 },
    newHypothecation: { type: String, default: "" },
    newRemarks: { type: String, default: "" },

    // Step 6: documents
    documents: { type: [insuranceDocumentSchema], default: [] },

    // Payment tracking (for #66 - Fully Paid vs Payment Due logic)
    customerPaymentExpected: { type: Number, default: 0 },
    customerPaymentReceived: { type: Number, default: 0 },
    inhousePaymentExpected: { type: Number, default: 0 },
    inhousePaymentReceived: { type: Number, default: 0 },
    paymentHistory: { type: [paymentHistorySchema], default: [] },

    // Renewal tracking
    isRenewal: { type: Boolean, default: false },
    renewedFromCaseId: { type: mongoose.Schema.Types.ObjectId, ref: "InsuranceCase" },
    renewedToCaseId: { type: mongoose.Schema.Types.ObjectId, ref: "InsuranceCase" },
    renewalFollowUpStatus: { 
      type: String, 
      enum: ["pending", "contacted", "interested", "renewed", "lost", "not_applicable"],
      default: "not_applicable"
    },
    renewalFollowUpNotes: { type: String, default: "" },
    renewalLastContactedAt: { type: Date },
    renewalNextFollowUpDate: { type: Date },

    // Audit
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
    updatedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  },
  {
    timestamps: true,
    strict: false,
  },
);

insuranceCaseSchema.index({ caseId: 1 }, { unique: true });
insuranceCaseSchema.index({ customerId: 1, createdAt: -1 });

const InsuranceCase = mongoose.model("InsuranceCase", insuranceCaseSchema);

export default InsuranceCase;

