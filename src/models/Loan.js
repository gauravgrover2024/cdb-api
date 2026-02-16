import mongoose from 'mongoose';

const loanSchema = mongoose.Schema(
  {
    loanId: { type: String, required: true, unique: true }, // Custom ID e.g. "LN-2024-001"
    customerId: { type: mongoose.Schema.Types.ObjectId, ref: 'Customer' },
    
    // Denormalized customer info for quicker access (common in non-relational)
    customerName: { type: String }, 
    
    // --- Sourcing ---
    // --- Sourcing & Lead ---
    sourcingChannel: { type: String },
    leadId: { type: String },
    dsaId: { type: String },
    salesExecutive: { type: String },
    leadDate: { type: Date },
    leadTime: { type: Date },
    source: { type: String }, // Direct / Indirect (from LeadDetails)
    recordSource: { type: String }, // Direct / Indirect (from Record Details - should match source)
    sourceName: { type: String }, // Dealer Name or Source Name
    sourceDetails: { type: String }, // Specific details if Direct
    dealerName: { type: String }, // Dealer name if Indirect
    dealerAddress: { type: String }, // Dealer address if Indirect
    dealerMobile: { type: String }, // Dealer mobile if Indirect
    dealtBy: { type: String }, // Assigned Employee
    
    // --- Payout Details (only applicable when Indirect) ---
    payoutApplicable: { type: String }, // Yes / No (from Record Details)
    prefile_sourcePayoutPercentage: { type: Number }, // Payout % for indirect source

    // --- Applicant Type ---
    applicantType: { type: String, default: 'Individual' },
    isMSME: { type: String },

    // --- Personal Details ---
    dob: { type: Date },
    gender: { type: String },
    maritalStatus: { type: String },
    dependents: { type: Number },
    education: { type: String },
    houseType: { type: String },
    addressType: { type: String },
    
    identityProofType: { type: String },
    identityProofNumber: { type: String },
    identityProofExpiry: { type: Date },
    
    addressProofType: { type: String },
    addressProofNumber: { type: String },
    
    residenceAddress: { type: String },
    pincode: { type: String },
    city: { type: String },
    yearsInCurrentCity: { type: Number },
    yearsInCurrentHouse: { type: Number },
    
    primaryMobile: { type: String },
    email: { type: String },
    extraMobiles: { type: [String] }, // Array of strings
    
    permanentAddress: { type: String },
    permanentPincode: { type: String },
    permanentCity: { type: String },

    // --- Co-Applicant (Flat Fields matching Frontend) ---
    hasCoApplicant: { type: Boolean },
    co_name: { type: String },
    co_motherName: { type: String },
    co_fatherName: { type: String },
    co_dob: { type: Date }, // Frontend sends date object or string
    co_gender: { type: String },
    co_maritalStatus: { type: String },
    co_dependents: { type: Number },
    co_education: { type: String },
    co_house: { type: String },
    co_mobile: { type: String },
    co_address: { type: String },
    co_pincode: { type: String },
    co_city: { type: String },
    co_pan: { type: String },
    co_aadhaar: { type: String },
    co_aadhar: { type: String }, // Alias
    co_occupation: { type: String },
    co_occupationType: { type: String },
    co_professionalType: { type: String },
    co_companyType: { type: String },
    co_businessNature: { type: [String] }, // Multiple select
    co_designation: { type: String },
    co_currentExp: { type: Number }, // In years - numeric
    co_totalExp: { type: Number }, // In years - numeric
    co_companyName: { type: String },
    co_companyAddress: { type: String },
    co_companyPincode: { type: String },
    co_companyCity: { type: String },
    co_companyPhone: { type: String },
    co_salaryMonthly: { type: Number },
    co_monthlySalary: { type: Number },

    // --- Guarantor (Flat Fields matching Frontend) ---
    hasGuarantor: { type: Boolean },
    gu_name: { type: String },
    gu_motherName: { type: String },
    gu_fatherName: { type: String },
    gu_dob: { type: Date },
    gu_gender: { type: String },
    gu_maritalStatus: { type: String },
    gu_dependents: { type: Number },
    gu_education: { type: String },
    gu_house: { type: String },
    gu_mobile: { type: String },
    gu_address: { type: String },
    gu_pincode: { type: String },
    gu_city: { type: String },
    gu_pan: { type: String },
    gu_aadhaar: { type: String },
    gu_aadhar: { type: String }, // Alias
    gu_occupation: { type: String },
    gu_occupationType: { type: String },
    gu_professionalType: { type: String },
    gu_companyType: { type: String },
    gu_businessNature: { type: [String] },
    gu_designation: { type: String },
    gu_currentExp: { type: Number }, // In years - numeric
    gu_totalExp: { type: Number }, // In years - numeric
    gu_companyName: { type: String },
    gu_companyAddress: { type: String },
    gu_companyPincode: { type: String },
    gu_companyCity: { type: String },
    gu_companyPhone: { type: String },

    // --- Vehicle Details & Pricing (Frontend naming convention) ---
    loanType: { type: String }, // New Car, Used Car, etc.
    usage: { type: String }, // Private, Commercial
    vehicleMake: { type: String },
    vehicleModel: { type: String },
    vehicleVariant: { type: String },
    
    // --- Pricing Breakdown
    exShowroomPrice: { type: Number },
    insuranceCost: { type: Number },
    roadTax: { type: Number },
    accessoriesAmount: { type: Number },
    dealerDiscount: { type: Number },
    manufacturerDiscount: { type: Number },
    marginMoney: { type: Number },
    advanceEmi: { type: Number },
    tradeInValue: { type: Number },
    otherDiscounts: { type: Number },
    onRoadPrice: { type: Number }, // Calculated
    
    // Dealer
    dealerName: { type: String },
    dealerContactPerson: { type: String },
    dealerContactNumber: { type: String },
    dealerAddress: { type: String },

    // Registration & Hypothecation
    hypothecation: { type: String }, // Yes/No
    hypothecationBank: { type: String },
    registerSameAsAadhaar: { type: String }, // Yes/No
    registrationAddress: { type: String },
    registrationCity: { type: String },
    
    // Buying Year (Used Car)
    boughtInYear: { type: String },
    purposeOfLoan: { type: String },

    // --- Extended Vehicle Technicals (Optional) ---
    vehicleFuel: { type: String },
    vehicleTransmission: { type: String },
    vehicleColor: { type: String },
    manufacturingYear: { type: String },
    registrationNumber: { type: String },
    chassisNumber: { type: String },
    engineNumber: { type: String },
    policyType: { type: String },
    insuranceExpiry: { type: Date },

    // --- Income & Employment (Applicant) ---
    occupationType: { type: String }, // Salaried, Self-Employed, Professional
    employmentType: { type: String },
    monthlyIncome: { type: Number }, // Self Employed
    monthlySalary: { type: Number }, // Salaried
    salaryMonthly: { type: Number }, // Alias for monthlySalary
    annualIncome: { type: Number },
    totalIncomeITR: { type: Number }, // Total Income as per ITR
    annualTurnover: { type: Number }, // For Self Employed
    netProfit: { type: Number }, // For Self Employed
    otherIncome: { type: Number },
    otherIncomeSource: { type: String },
    
    // Office Address
    employmentAddress: { type: String },
    employmentPincode: { type: String },
    employmentCity: { type: String },
    employmentPhone: { type: String },
    officialEmail: { type: String },
    
    // State & Other Personal
    state: { type: String },
    fatherName: { type: String },
    motherName: { type: String },
    sdwOf: { type: String }, // Son/Daughter/Wife of

    // --- Loan Parameters ---
    isFinanced: { type: String, default: 'Yes' },
    loanAmount: { type: Number },
    requiredLoanAmount: { type: Number },
    tenure: { type: Number },
    interestRate: { type: Number },

    // Approval / Sanction / Disbursement
    currentStage: { type: String, default: 'profile' }, 
    status: { type: String, default: 'Pending' },

    // ===== APPROVAL STAGE (Only approval data, NO payout yet) =====
    approval_bankId: { type: String },
    approval_bankName: { type: String },
    approval_loanAmountApproved: { type: Number },
    approval_roi: { type: Number },
    approval_tenureMonths: { type: Number },
    approval_processingFees: { type: Number },
    approval_status: { type: String }, // "Approved", "Rejected", "Pending"
    approval_approvalDate: { type: Date },
    approval_remarks: { type: String },
    
    // Multi-Bank Data
    approval_banksData: { type: Array, default: [] }, 

    // ===== DISBURSEMENT STAGE (NEW - Separate from Approval) =====
    disburse_status: { type: String }, // "Pending", "Disbursed", "Cancelled"
    disburse_bankName: { type: String }, // Bank that actually disbursed
    disburse_amount: { type: Number }, // Actual disbursed amount
    disburse_date: { type: Date },
    disburse_remarks: { type: String }, // MANDATORY: Disbursement remarks/reason (required from frontend)
    disbursementRemarks: { type: String }, // Alias for disburse_remarks (stored in banksData array)
    
    // DEPRECATED (Legacy - kept for backward compatibility)
    approval_loanAmountDisbursed: { type: Number },
    approval_disbursedDate: { type: Date },

    // ===== PAYOUT DATA (Generated ONLY after disbursement) =====
    payout_percentage: { type: Number }, // Filled ONLY at disbursement
    payout_amount: { type: Number }, // Calculated at disbursement
    payout_calculatedAt: { type: Date },
    payout_applicableFor: { type: String }, // "Bank", "Dealer", "Both"
    
    // Receivables & Payables (Created after disbursement)
    loan_receivables: [mongoose.Schema.Types.Mixed], // Array of receivable records from bank payout
    loan_payables: [mongoose.Schema.Types.Mixed], // Array of payable records for dealer payout
    
    // Bill Printing (Payout)
    bill_number: { type: String }, // Auto-generated bill number (BILL-YYYYMMDD-XXXX)
    bill_date: { type: Date }, // Date when bill was generated
    
    // DEPRECATED (Legacy - moved to disbursement stage)
    payoutPercentage: { type: Number },
    payoutAmount: { type: Number },
    
    do_number: { type: String },
    do_date: { type: Date },

    // --- Delivery & Insurance (Frontend Form Fields) ---
    delivery_date: { type: Date },
    delivery_dealerName: { type: String },
    delivery_dealerContactPerson: { type: String },
    delivery_dealerContactNumber: { type: String },
    delivery_dealerAddress: { type: String },
    delivery_by: { type: String },

    insurance_by: { type: String },
    insurance_company_name: { type: String },
    insurance_policy_number: { type: String },

    invoice_number: { type: String },
    invoice_date: { type: Date },
    invoice_received_as: { type: String },
    invoice_received_from: { type: String },
    invoice_received_date: { type: Date },

    rc_redg_no: { type: String },
    rc_chassis_no: { type: String },
    rc_engine_no: { type: String },
    rc_redg_date: { type: Date },
    rc_received_as: { type: String },
    rc_received_from: { type: String },
    rc_received_date: { type: Date },

    // --- Breakup Fields ---
    approval_breakup_netLoanApproved: { type: Number },
    approval_breakup_creditAssured: { type: Number },
    approval_breakup_insuranceFinance: { type: Number },
    approval_breakup_ewFinance: { type: Number },

    // --- Document Uploads (All Files & Images) ---
    // Identity & Address Proofs
    aadhaarCardDocUrl: { type: String },
    panCardDocUrl: { type: String },
    passportDocUrl: { type: String },
    dlDocUrl: { type: String }, // Driver License
    gstDocUrl: { type: String },
    addressProofDocUrl: { type: String },
    
    // Co-Applicant Documents
    co_aadhaarCardDocUrl: { type: String },
    co_panCardDocUrl: { type: String },
    co_passportDocUrl: { type: String },
    co_dlDocUrl: { type: String },
    co_addressProofDocUrl: { type: String },
    
    // Guarantor Documents
    gu_aadhaarCardDocUrl: { type: String },
    gu_panCardDocUrl: { type: String },
    gu_passportDocUrl: { type: String },
    gu_dlDocUrl: { type: String },
    gu_addressProofDocUrl: { type: String },
    
    // Vehicle Documents
    vehiclePhotoUrl: { type: String },
    vehicleRCUrl: { type: String },
    insurancePolicyUrl: { type: String },
    hypothecationDocUrl: { type: String },
    
    // Delivery Order & Invoices
    delivery_invoiceFile: { type: String },
    delivery_rcFile: { type: String },
    
    // PostFile Documents
    postfile_documents: [mongoose.Schema.Types.Mixed], // Array of document objects
    
    // Additional KYC Documents
    aadhaarNumber: { type: String },
    panNumber: { type: String },
    passportNumber: { type: String },
    dlNumber: { type: String },
    gstNumber: { type: String },
    
    // Co-Applicant & Guarantor ID Numbers
    co_aadhaarNumber: { type: String },
    co_panNumber: { type: String },
    co_passportNumber: { type: String },
    co_dlNumber: { type: String },
    co_gstNumber: { type: String },
    
    gu_aadhaarNumber: { type: String },
    gu_panNumber: { type: String },
    gu_passportNumber: { type: String },
    gu_dlNumber: { type: String },
    gu_gstNumber: { type: String },
    
    // PostFile Specific Fields
    postfile_bankName: { type: String },
    postfile_regd_city: { type: String },
    postfile_loanAmountApproved: { type: Number },
    postfile_loanAmountDisbursed: { type: Number },
    postfile_roi: { type: Number },
    postfile_tenureMonths: { type: Number },
    postfile_processingFees: { type: Number },
    postfile_emiAmount: { type: Number },
    postfile_firstEmiDate: { type: Date },
    postfile_roiType: { type: String }, // Fixed / Floating
    postfile_sameAsApproved: { type: String }, // Yes / No
    
    // PostFile Disbursal Breakup (Net Loan Amount for Disbursal)
    postfile_disbursedLoan: { type: Number },
    postfile_disbursedCreditAssured: { type: Number },
    postfile_disbursedInsurance: { type: Number },
    postfile_disbursedEw: { type: Number },
    
    // Dispatch & Disbursement
    dispatch_date: { type: Date },
    dispatch_time: { type: String },
    dispatch_through: { type: String },
    disbursement_date: { type: Date },
    disbursement_time: { type: String },
    loan_number: { type: String },
    
    // Record Details / Section 7
    receivingDate: { type: Date },
    receivingTime: { type: String },
    referenceName: { type: String },
    referenceNumber: { type: String },
    docsPreparedBy: { type: String },
    
    // Finance Details
    typeOfLoan: { type: String },
    financeExpectation: { type: Number }, // Expected Funding
    loanTenureMonths: { type: Number }, // Requested Tenure in Months
    isFinanced: { type: String }, // Yes / No
    customLoanAmount: { type: Number },
    customTenure: { type: Number },
    customRate: { type: Number },
    
    // Bulk Loan Creation
    numberOfCars: { type: Number },
    
    // Lead Details
    leadType: { type: String },
    leadSource: { type: String },
    
    // General Extras
    nomineeName: { type: String },
    nomineeDob: { type: Date },
    nomineeRelation: { type: String },
    
    // References
    reference1_name: { type: String },
    reference1_mobile: { type: String },
    reference1_address: { type: String },
    reference1_pincode: { type: String },
    reference1_city: { type: String },
    reference1_relation: { type: String },
    reference2_name: { type: String },
    reference2_mobile: { type: String },
    reference2_address: { type: String },
    reference2_pincode: { type: String },
    reference2_city: { type: String },
    reference2_relation: { type: String },
    
    // Company Details
    companyName: { type: String },
    companyAddress: { type: String },
    companyPincode: { type: String },
    companyCity: { type: String },
    companyPhone: { type: String },
    companyType: { type: String },
    businessNature: { type: [String] },
    
    // Professional Details
    professionalType: { type: String },
    designation: { type: String },
    currentExp: { type: Number },
    totalExp: { type: Number },
    incorporationYear: { type: Number },
    
    // Extra Fields
    educationOther: { type: String },
    yearsInCurrentHouse: { type: Number },
    yearsInCurrentCity: { type: Number },
    yearOfReg: { type: String },
    whatsappNumber: { type: String },
    ifscCode: { type: String },
    ifsc: { type: String }, // Alias
    accountNumber: { type: String },
    accountType: { type: String },
    bankName: { type: String },
    branch: { type: String },
    accountSinceYears: { type: Number },
    openedIn: { type: Number },
    maritalStatus: { type: String },
    dependents: { type: Number },
    education: { type: String },
    
    // Co-Applicant & Guarantor Banking
    co_accountNumber: { type: String },
    co_accountType: { type: String },
    co_bankName: { type: String },
    co_branch: { type: String },
    co_ifscCode: { type: String },
    co_salaryMonthly: { type: Number },
    co_monthlySalary: { type: Number },
    co_monthlyIncome: { type: Number },
    co_annualIncome: { type: Number },
    
    gu_accountNumber: { type: String },
    gu_accountType: { type: String },
    gu_bankName: { type: String },
    gu_branch: { type: String },
    gu_ifscCode: { type: String },
    gu_salaryMonthly: { type: Number },
    gu_monthlySalary: { type: Number },
    gu_monthlyIncome: { type: Number },
    gu_annualIncome: { type: Number },
    
    // Internal Flags
    __postfileSeeded: { type: Boolean, default: false },
    __postfileLocked: { type: Boolean, default: false },
    
    // --- Bulk ---
    isBulk: { type: Boolean, default: false },
    bulkCount: { type: Number },

    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  {
    timestamps: true,
    strict: false, // Allow any additional fields from form
  }
);

// --- Indexes ---
// Text index for global search
loanSchema.index({ 
  customerName: 'text', 
  primaryMobile: 'text', 
  loanId: 'text', 
  registrationNumber: 'text',
  chassisNumber: 'text',
  engineNumber: 'text'
});

// Single field indexes for performance
loanSchema.index({ customerId: 1 });
loanSchema.index({ status: 1 });
loanSchema.index({ currentStage: 1 });
loanSchema.index({ loanType: 1 });
loanSchema.index({ createdAt: -1 });
loanSchema.index({ primaryMobile: 1 });


const Loan = mongoose.model('Loan', loanSchema);

export default Loan;
