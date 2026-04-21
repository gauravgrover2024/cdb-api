import mongoose from "mongoose";

const insuranceQuoteSchema = new mongoose.Schema(
  {
    id: { type: mongoose.Schema.Types.Mixed, required: true },
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
  { _id: false, strict: false },
);

const insuranceDocumentSchema = new mongoose.Schema(
  {
    id: { type: String, required: true },
    name: { type: String, default: "" },
    size: { type: Number, default: 0 },
    type: { type: String, default: "" },
    tag: { type: String, default: "" },
    url: { type: String, default: "" },
    previewUrl: { type: String, default: "" },
    downloadUrl: { type: String, default: "" },
    storageKey: { type: String, default: "" },
    uploadedAt: { type: Date, default: null },
  },
  { _id: false, strict: false },
);

const paymentHistorySchema = new mongoose.Schema(
  {
    amount: { type: Number, required: true },
    date: { type: Date, default: null },
    entryType: {
      type: String,
      enum: [
        "INSURER_PAYMENT",
        "CUSTOMER_RECEIPT",
        "SUBVENTION",
        "SUBVENTION_NON_RECOVERABLE",
        "SUBVENTION_REFUND",
      ],
      default: undefined,
    },
    paidBy: { type: String, default: "" },
    paymentType: {
      type: String,
      enum: ["customer", "inhouse", "adjustment", "subvention_nr"],
      default: "inhouse",
    },
    paymentMode: {
      type: String,
      enum: [
        "",
        "Online Transfer/UPI",
        "Cash",
        "Cheque",
        "DD",
        "Credit Card",
        "NEFT",
        "RTGS",
        "UPI",
        "Card",
        "Other",
      ],
      default: "",
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
    caseId: { type: String, required: true, unique: true },

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
    policyCategory: { type: String, default: "Insurance Policy" },
    policyTypeSelector: { type: String, default: "Insurance Policy" },
    policyDoneBy: { type: String, default: "Autocredits India LLP" },
    brokerName: { type: String, default: "" },
    showroomName: { type: String, default: "" },
    source: { type: String, default: "Direct" },
    sourceName: { type: String, default: "" },
    dealerChannelName: { type: String, default: "" },
    dealerChannelAddress: { type: String, default: "" },
    payoutApplicable: { type: String, default: "No" },
    payoutPercent: { type: Number, default: 0 },
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
    nomineeDob: { type: String, default: "" },
    nomineeAge: { type: String, default: "" },
    referenceName: { type: String, default: "" },
    referencePhone: { type: String, default: "" },

    // Step 2: vehicle details
    registrationNumber: { type: String, default: "" },
    registrationAllotted: { type: String, default: "Yes" },
    vehicleMake: { type: String, default: "" },
    vehicleModel: { type: String, default: "" },
    vehicleVariant: { type: String, default: "" },
    cubicCapacity: { type: String, default: "" },
    engineNumber: { type: String, default: "" },
    chassisNumber: { type: String, default: "" },
    typesOfVehicle: { type: String, default: "Four Wheeler" },
    manufactureMonth: { type: String, default: "" },
    manufactureYear: { type: String, default: "" },
    manufactureDate: { type: String, default: "" },
    regAuthority: { type: String, default: "" },
    dateOfReg: { type: String, default: "" },
    fuelType: { type: String, default: "" },
    batteryNumber: { type: String, default: "" },
    chargerNumber: { type: String, default: "" },
    hypothecation: { type: String, default: "" },

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
    previousIdvAmount: { type: Number, default: 0 },
    previousOwnDamageAmount: { type: Number, default: 0 },
    previousBasicOwnDamageAmount: { type: Number, default: 0 },
    previousThirdPartyAmount: { type: Number, default: 0 },
    previousBasicThirdPartyAmount: { type: Number, default: 0 },
    previousAddOnsTotal: { type: Number, default: 0 },
    previousTotalPremium: { type: Number, default: 0 },
    previousSelectedAddOns: { type: [String], default: [] },
    previousHypothecation: { type: String, default: "" },
    previousRemarks: { type: String, default: "" },

    // Step 4: quotes
    quotes: { type: [insuranceQuoteSchema], default: [] },
    acceptedQuoteId: { type: mongoose.Schema.Types.Mixed, default: null },

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

    // Policy details upgrade
    exShowroomPrice: { type: Number, default: 0 },
    dateOfSale: { type: String, default: "" },
    dateOfPurchase: { type: String, default: "" },
    odometerReading: { type: Number, default: 0 },
    policyPurchaseDate: { type: String, default: "" },

    // Extended Warranty
    ewCommencementDate: { type: String, default: "" },
    ewExpiryDate: { type: String, default: "" },
    kmsCoverage: { type: Number, default: 0 },

    // Step 6: documents
    documents: { type: [insuranceDocumentSchema], default: [] },

    // Payment tracking (for #66 - Fully Paid vs Payment Due logic)
    customerPaymentExpected: { type: Number, default: 0 },
    customerPaymentReceived: { type: Number, default: 0 },
    inhousePaymentExpected: { type: Number, default: 0 },
    inhousePaymentReceived: { type: Number, default: 0 },
    paymentHistory: { type: [paymentHistorySchema], default: [] },

    // Payout tracking (same as loans but for insurance)
    insurance_receivables: { type: [mongoose.Schema.Types.Mixed], default: [] },
    insurance_payables: { type: [mongoose.Schema.Types.Mixed], default: [] },

    // Renewal tracking
    isRenewal: { type: Boolean, default: false },
    renewedFromCaseId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "InsuranceCase",
    },
    renewedToCaseId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "InsuranceCase",
    },
    renewalFollowUpStatus: {
      type: String,
      enum: [
        "pending",
        "contacted",
        "interested",
        "renewed",
        "lost",
        "not_applicable",
      ],
      default: "not_applicable",
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

insuranceCaseSchema.index({ customerId: 1, createdAt: -1 });
insuranceCaseSchema.index({ status: 1, updatedAt: -1 });
insuranceCaseSchema.index({ createdAt: -1 });

const InsuranceCase = mongoose.model("InsuranceCase", insuranceCaseSchema);

export default InsuranceCase;
