import mongoose from "mongoose";

const { Schema } = mongoose;

// ─── Reusable sub-schemas ────────────────────────────────────────────────────

const addressSchema = new Schema(
  {
    line1: { type: String },
    pincode: { type: String },
    city: { type: String },
    state: { type: String },
    district: { type: String },
    area: { type: String },
  },
  { _id: false },
);

const bankDetailSchema = new Schema(
  {
    bankName: { type: String },
    accountNumber: { type: String },
    ifsc: { type: String },
    branch: { type: String },
    accountType: { type: String },
    accountSinceYears: { type: Number },
    openedIn: { type: Number },
  },
  { _id: false },
);

// Replaces cheque_N_* flat fields (20 cheques × 9 fields = 180 → 1 array)
const instrumentSchema = new Schema(
  {
    tag: { type: String },           // PDC, Security, Advance, etc.
    number: { type: String },
    bankName: { type: String },
    accountNumber: { type: String },
    date: { type: Date },
    amount: { type: Number },
    signedBy: { type: String },
    favouring: { type: String },
    imageUrl: { type: String },
  },
  { _id: false },
);

const personSchema = new Schema(
  {
    customerId: { type: Schema.Types.ObjectId, ref: "Customer" },
    customerName: { type: String },
    motherName: { type: String },
    fatherName: { type: String },
    dob: { type: Date },
    gender: { type: String },
    maritalStatus: { type: String },
    dependents: { type: Number },
    education: { type: String },
    houseType: { type: String },
    pan: { type: String },
    aadhaar: { type: String },
    passport: { type: String },
    dlNumber: { type: String },
    identityProofType: { type: String },
    identityProofNumber: { type: String },
    identityProofExpiry: { type: Date },
    addressProofType: { type: String },
    addressProofNumber: { type: String },
    primaryMobile: { type: String },
    extraMobiles: [{ type: String }],
    email: { type: String },
    officialEmail: { type: String },
    addressType: { type: String },
    address: { type: addressSchema },
    permanentAddress: { type: addressSchema },
    sameAsCurrentAddress: { type: Boolean, default: true },
    yearsInCurrentCity: { type: Number },
    yearsInCurrentHouse: { type: Number },
    // Occupational
    occupationType: { type: String },
    isMSME: { type: String },
    professionalType: { type: String },
    companyType: { type: String },
    businessNature: [{ type: String }],
    experienceCurrent: { type: String },
    totalExperience: { type: String },
    designation: { type: String },
    companyName: { type: String },
    employmentAddress: { type: String },
    employmentPincode: { type: String },
    employmentCity: { type: String },
    employmentPhone: { type: String },
    companyPhone: { type: String },
    companyAddress: { type: String },
    companyPincode: { type: String },
    companyCity: { type: String },
    // Income
    grossSalary: { type: Number },
    netSalary: { type: Number },
    totalIncome: { type: Number },
    totalTurnoverGST: { type: Number },
    // Banking
    banking: { type: bankDetailSchema },
    additionalBankDetails: [bankDetailSchema],
    hasAdditionalBankDetails: { type: Boolean },
    // Company-specific
    contactPersonName: { type: String },
    contactPersonMobile: { type: String },
    companyPartners: [
      {
        name: String,
        panNumber: String,
        contactNumber: String,
        dateOfBirth: String,
        _id: false,
      },
    ],
    // Financial
    applicantCategory: { type: String },
    relationship: { type: String },
    yearsAtCurrentResidence: { type: String },
  },
  { _id: false },
);

const bankApprovalSchema = new Schema(
  {
    bankId: { type: String },
    bankName: { type: String },
    status: { type: String },  // Pending | Applied | Approved | Rejected | Disbursed
    roi: { type: Number },
    tenureMonths: { type: Number },
    loanAmountApproved: { type: Number },
    loanAmountDisbursed: { type: Number },
    processingFees: { type: Number },
    approvalDate: { type: Date },
    disbursedDate: { type: Date },
    breakup: {
      netLoanApproved: { type: Number },
      insuranceFinance: { type: Number },
      ewFinance: { type: Number },
      creditAssured: { type: Number },
      _id: false,
    },
    // Record details per bank
    receivingDate: { type: Date },
    receivingTime: { type: String },
    recordSource: { type: String },
    sourceName: { type: String },
    dealerMobile: { type: String },
    dealerAddress: { type: String },
    payoutApplicable: { type: String },
    payoutPercentage: { type: Number },
    referenceDetails: { type: String },
    dealtBy: { type: String },
    docsPreparedBy: { type: String },
    statusHistory: [
      {
        status: String,
        updatedAt: Date,
        updatedBy: String,
        remarks: String,
        _id: false,
      },
    ],
  },
  { _id: false },
);

const pendencySchema = new Schema(
  {
    type: { type: String },
    description: { type: String },
    dueDate: { type: Date },
    status: { type: String, default: "open" },
    resolvedAt: { type: Date },
    resolvedBy: { type: String },
    remarks: { type: String },
  },
  { timestamps: true },
);

const documentSchema = new Schema(
  {
    docType: { type: String },
    applicantType: { type: String },    // primary | co | guarantor
    fileUrl: { type: String },
    fileName: { type: String },
    verificationStatus: { type: String, default: "pending" },
    uploadedAt: { type: Date, default: Date.now },
    remarks: { type: String },
  },
  { _id: false },
);

const workflowEventSchema = new Schema(
  {
    stage: { type: String },
    fromStatus: { type: String },
    toStatus: { type: String },
    action: { type: String },
    actionBy: { type: String },
    actionAt: { type: Date, default: Date.now },
    remarks: { type: String },
  },
  { _id: false },
);

const auditEntrySchema = new Schema(
  {
    action: { type: String },
    section: { type: String },
    changedBy: { type: String },
    changedAt: { type: Date, default: Date.now },
    oldValues: { type: Schema.Types.Mixed },
    newValues: { type: Schema.Types.Mixed },
  },
  { _id: false },
);

// ─── Main HomeLoan schema ─────────────────────────────────────────────────────

const homeLoanSchema = new Schema(
  {
    applicationNumber: { type: String, unique: true, sparse: true },
    loanId: { type: String, unique: true, sparse: true },

    // References
    customerId: { type: Schema.Types.ObjectId, ref: "Customer" },
    branchId: { type: String },
    createdBy: { type: Schema.Types.ObjectId, ref: "User" },
    updatedBy: { type: Schema.Types.ObjectId, ref: "User" },

    // Workflow state
    status: {
      type: String,
      enum: ["draft", "submitted", "under_review", "approved", "rejected", "disbursed", "closed"],
      default: "draft",
    },
    currentStep: {
      type: String,
      enum: ["profile", "prefile", "approval", "postfile", "payout", "delivery"],
      default: "profile",
    },
    completedSteps: [{ type: String }],

    // ── Sourcing / Lead ──────────────────────────────────────────────────────
    lead: {
      sourcingChannel: { type: String },
      dsaCode: { type: String },
      leadId: { type: String },
      dsaId: { type: String },
      salesExecutive: { type: String },
      leadDate: { type: Date },
      leadTime: { type: Date },
      source: { type: String },           // Direct | Indirect
      recordSource: { type: String },
      sourceName: { type: String },
      sourceDetails: { type: String },
      dealerName: { type: String },
      dealerAddress: { type: String },
      dealerMobile: { type: String },
      dealtBy: { type: String },
      payoutApplicable: { type: String },
      payoutPercentage: { type: Number },
      referenceDetails: { type: String },
      docsPreparedBy: { type: String },
      _id: false,
    },

    // Top-level denormalised for quick search/display
    applicantType: { type: String, default: "Individual" },
    caseType: { type: String },
    customerType: { type: String },
    customerName: { type: String },
    typeOfLoan: { type: String },

    // ── Applicant ────────────────────────────────────────────────────────────
    applicant: { type: personSchema },

    // ── Co-Applicant (optional) ──────────────────────────────────────────────
    coApplicant: {
      type: personSchema,
      default: undefined,
    },

    // ── Guarantor (optional) ─────────────────────────────────────────────────
    guarantor: {
      type: personSchema,
      default: undefined,
    },

    // ── Authorised Signatory (Company cases) ─────────────────────────────────
    authorisedSignatory: {
      type: personSchema,
      default: undefined,
    },

    // ── Vehicle ──────────────────────────────────────────────────────────────
    vehicle: {
      make: { type: String },
      model: { type: String },
      variant: { type: String },
      fuelType: { type: String },
      regNo: { type: String },
      boughtInYear: { type: String },
      usage: { type: String },
      valuation: { type: Number },
      hypothecation: { type: String },
      hypothecationBank: { type: String },
      purposeOfLoan: { type: String },
      registrationCity: { type: String },
      registrationAddress: { type: String },
      registrationPincode: { type: String },
      registerSameAsAadhaar: { type: String },
      registerSameAsPermanent: { type: String },
      _id: false,
    },

    // ── Pricing (New Car) ─────────────────────────────────────────────────────
    pricing: {
      exShowroomPrice: { type: Number },
      insuranceCost: { type: Number },
      roadTax: { type: Number },
      accessoriesAmount: { type: Number },
      accessories: { type: String },
      additionsOthers: { type: String },
      dealerDiscount: { type: Number },
      manufacturerDiscount: { type: Number },
      marginMoney: { type: Number },
      advanceEmi: { type: Number },
      tradeInValue: { type: Number },
      otherDiscounts: { type: Number },
      // Calculated (stored for quick access)
      onRoadPrice: { type: Number },
      grossLoan: { type: Number },
      netLoan: { type: Number },
      _id: false,
    },

    // ── Dealer ────────────────────────────────────────────────────────────────
    dealer: {
      name: { type: String },
      contactPerson: { type: String },
      contactNumber: { type: String },
      address: { type: String },
      _id: false,
    },

    // ── Approval Stage ────────────────────────────────────────────────────────
    approval: {
      status: { type: String },
      bankId: { type: String },
      bankName: { type: String },
      roi: { type: Number },
      tenureMonths: { type: Number },
      loanAmountApproved: { type: Number },
      loanAmountDisbursed: { type: Number },
      processingFees: { type: Number },
      approvalDate: { type: Date },
      disbursedDate: { type: Date },
      breakup: {
        netLoanApproved: { type: Number },
        insuranceFinance: { type: Number },
        ewFinance: { type: Number },
        creditAssured: { type: Number },
        _id: false,
      },
      // All bank submissions for this loan
      banks: [bankApprovalSchema],
      statusHistory: [
        {
          status: String,
          updatedAt: Date,
          updatedBy: String,
          remarks: String,
          _id: false,
        },
      ],
      _id: false,
    },

    // ── Post-File ─────────────────────────────────────────────────────────────
    postFile: {
      locked: { type: Boolean, default: false },
      // Instruments: replaces cheque_1..20 flat fields (180 fields → 1 array)
      instruments: [instrumentSchema],
      // Document tracking
      aadhaarCardDocUrl: { type: String },
      aadhaarCardBackDocUrl: { type: String },
      panCardDocUrl: { type: String },
      passportDocUrl: { type: String },
      dlDocUrl: { type: String },
      addressProofDocUrl: { type: String },
      gstDocUrl: { type: String },
      gstDocUrlPage2: { type: String },
      gstDocUrlPage3: { type: String },
      incomeDocUrl: { type: String },
      itrDocUrl: { type: String },
      bankStatementDocUrl: { type: String },
      // Approval details (mirrored from approval for post-file view)
      approvalDate: { type: Date },
      disbursedDate: { type: Date },
      loanAmountApproved: { type: Number },
      roi: { type: Number },
      tenureMonths: { type: Number },
      processingFees: { type: Number },
      // EMI
      firstEmiDate: { type: Date },
      emiAmount: { type: Number },
      emiStartDate: { type: Date },
      // RC / Invoice
      invoiceDate: { type: Date },
      invoiceReceivedDate: { type: Date },
      invoiceNumber: { type: String },
      rcNumber: { type: String },
      rcRegdDate: { type: Date },
      rcReceivedDate: { type: Date },
      rcInvStorageNumber: { type: String },
      // Dispatch
      dispatchDate: { type: Date },
      dispatchTime: { type: String },
      dispatchCourier: { type: String },
      dispatchTracking: { type: String },
      dispatchRemarks: { type: String },
      // Principal
      principalOutstandingAmount: { type: Number },
      principalOutstandingDate: { type: Date },
      // Vehicle verification
      vehicleVerified: { type: Boolean },
      verificationRemarks: { type: String },
      _id: false,
    },

    // ── Disbursement ──────────────────────────────────────────────────────────
    disbursement: {
      date: { type: Date },
      time: { type: String },
      amount: { type: Number },
      mode: { type: String },
      referenceNumber: { type: String },
      remarks: { type: String },
      _id: false,
    },

    // ── Vehicle Delivery ──────────────────────────────────────────────────────
    delivery: {
      dealerName: { type: String },
      dealerContactPerson: { type: String },
      dealerContactNumber: { type: String },
      dealerAddress: { type: String },
      deliveryDate: { type: Date },
      notes: { type: String },
      initialized: { type: Boolean, default: false },
      _id: false,
    },

    // ── Payout ────────────────────────────────────────────────────────────────
    payout: {
      billNumber: { type: String },
      billDate: { type: Date },
      receivables: [{ type: Schema.Types.Mixed }],
      payables: [{ type: Schema.Types.Mixed }],
      _id: false,
    },

    // ── Misc / Notes ──────────────────────────────────────────────────────────
    notes: [
      {
        content: { type: String },
        addedBy: { type: String },
        addedAt: { type: Date, default: Date.now },
        _id: false,
      },
    ],
    approxClosureDate: { type: Date },

    // ── Pendencies ────────────────────────────────────────────────────────────
    pendencies: [pendencySchema],

    // ── Documents ─────────────────────────────────────────────────────────────
    documents: [documentSchema],

    // ── Workflow history ──────────────────────────────────────────────────────
    workflowHistory: [workflowEventSchema],

    // ── Audit log ─────────────────────────────────────────────────────────────
    auditLog: [auditEntrySchema],

    // ── Soft delete ───────────────────────────────────────────────────────────
    deletedAt: { type: Date, default: null },
    deletedBy: { type: String },
  },
  {
    timestamps: true,
    collection: "home_loans",
  },
);

// ─── Indexes ──────────────────────────────────────────────────────────────────

homeLoanSchema.index({ applicationNumber: 1 });
homeLoanSchema.index({ loanId: 1 });
homeLoanSchema.index({ customerId: 1 });
homeLoanSchema.index({ status: 1 });
homeLoanSchema.index({ currentStep: 1 });
homeLoanSchema.index({ deletedAt: 1 });
homeLoanSchema.index({ createdAt: -1 });
homeLoanSchema.index({ customerName: "text" });
homeLoanSchema.index({ "applicant.pan": 1 });
homeLoanSchema.index({ "vehicle.regNo": 1 });
homeLoanSchema.index({ "approval.bankName": 1 });
homeLoanSchema.index({ typeOfLoan: 1, status: 1 });

// ─── Application number generator ────────────────────────────────────────────

homeLoanSchema.statics.generateApplicationNumber = async function () {
  const year = new Date().getFullYear();
  const prefix = `HL-${year}-`;
  const last = await this.findOne(
    { applicationNumber: { $regex: `^${prefix}` } },
    { applicationNumber: 1 },
    { sort: { applicationNumber: -1 } },
  );
  let seq = 1;
  if (last?.applicationNumber) {
    const parts = last.applicationNumber.split("-");
    seq = (parseInt(parts[parts.length - 1], 10) || 0) + 1;
  }
  return `${prefix}${String(seq).padStart(4, "0")}`;
};

const HomeLoan = mongoose.model("HomeLoan", homeLoanSchema);
export default HomeLoan;
