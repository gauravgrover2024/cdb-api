import mongoose from 'mongoose';

const loanSchema = mongoose.Schema(
  {
    loanId: { type: String, required: true, index: true }, // Custom ID e.g. "LN-2024-001"
    customerId: { type: mongoose.Schema.Types.ObjectId, ref: 'Customer' },
    
    // Denormalized customer info for quicker access (common in non-relational)
    customerName: { type: String }, 
    
    // --- Sourcing ---
    // --- Sourcing & Lead ---
    sourcingChannel: { type: String },
    dsaCode: { type: String }, // Alias used in UI/migration
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
    caseType: { type: String },
    customerType: { type: String },
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
    customerMobile: { type: String }, // Alias in pre-file/profile helpers
    mobileNo: { type: String }, // Alias used by profile/header snapshots
    email: { type: String },
    customerEmail: { type: String }, // Header alias
    customerAddress: { type: String }, // Header alias
    customerPan: { type: String }, // Header alias
    customerAadhar: { type: String }, // Header alias
    contactPersonName: { type: String },
    contactPersonMobile: { type: String },
    extraMobiles: { type: [String] }, // Array of strings
    
    permanentAddress: { type: String },
    permanentPincode: { type: String },
    permanentCity: { type: String },
    sameAsCurrentAddress: { type: Boolean },

    // --- Co-Applicant (Flat Fields matching Frontend) ---
    hasCoApplicant: { type: Boolean },
    coApplicant_name: { type: String }, // Sticky-header alias
    co_name: { type: String },
    co_customerName: { type: String }, // Alias
    co_id: { type: String },
    co_primaryMobile: { type: String },
    co_motherName: { type: String },
    co_fatherName: { type: String },
    co_dob: { type: Date }, // Frontend sends date object or string
    co_gender: { type: String },
    co_maritalStatus: { type: String },
    co_dependents: { type: Number },
    co_education: { type: String },
    co_house: { type: String },
    co_houseType: { type: String },
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
    co_currentExperience: { type: Number },
    co_totalExp: { type: Number }, // In years - numeric
    co_totalExperience: { type: Number },
    co_companyName: { type: String },
    co_companyAddress: { type: String },
    co_companyPincode: { type: String },
    co_companyCity: { type: String },
    co_companyPhone: { type: String },
    co_salaryMonthly: { type: Number },
    co_monthlySalary: { type: Number },

    // --- Guarantor (Flat Fields matching Frontend) ---
    hasGuarantor: { type: Boolean },
    guarantor_name: { type: String }, // Sticky-header alias
    gu_name: { type: String },
    gu_customerName: { type: String }, // Alias
    gu_id: { type: String },
    gu_primaryMobile: { type: String },
    gu_motherName: { type: String },
    gu_fatherName: { type: String },
    gu_dob: { type: Date },
    gu_gender: { type: String },
    gu_maritalStatus: { type: String },
    gu_dependents: { type: Number },
    gu_education: { type: String },
    gu_house: { type: String },
    gu_houseType: { type: String },
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
    gu_currentExperience: { type: Number }, // Alias
    gu_totalExp: { type: Number }, // In years - numeric
    gu_totalExperience: { type: Number }, // Alias
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
    vehicleFuelType: { type: String },
    
    // --- Pricing Breakdown
    exShowroom: { type: Number }, // Vehicle pricing popup alias
    insurance: { type: Number }, // Vehicle pricing popup alias
    tcs: { type: Number }, // Vehicle pricing popup alias
    accessories: { type: Number }, // Vehicle pricing popup alias
    epc: { type: Number }, // Vehicle pricing popup alias
    fastag: { type: Number }, // Vehicle pricing popup alias
    extendedWarranty: { type: Number }, // Vehicle pricing popup alias
    exchange: { type: Number }, // Vehicle pricing popup alias
    additionsOthers: { type: Number }, // Dynamic additions bucket
    discountsOthers: { type: Number }, // Dynamic discounts bucket
    schemeDiscount: { type: Number },
    corporate: { type: Number },
    loyalty: { type: Number },
    insuranceCashback: { type: Number },
    exShowroomPrice: { type: Number },
    insuranceCost: { type: Number },
    roadTax: { type: Number },
    accessoriesAmount: { type: Number },
    valuation: { type: Number }, // Used Car / Cash-in / Refinance valuation
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
    showroomDealerName: { type: String },
    showroomDealerContactPerson: { type: String },
    showroomDealerContactNumber: { type: String },
    showroomDealerAddress: { type: String },

    // Registration & Hypothecation
    hypothecation: { type: String }, // Yes/No
    hypothecationBank: { type: String },
    registerSameAsAadhaar: { type: String }, // Yes/No
    registerSameAsAadhar: { type: String }, // Spelling alias used in post-file
    registerSameAsPermanent: { type: String },
    registrationAddress: { type: String },
    registrationPincode: { type: String },
    registrationCity: { type: String },
    
    // Buying Year (Used Car)
    boughtInYear: { type: String },
    purposeOfLoan: { type: String },

    // --- Extended Vehicle Technicals (Optional) ---
    vehicleFuel: { type: String },
    vehicleTransmission: { type: String },
    vehicleColor: { type: String },
    manufacturingYear: { type: String },
    yearOfManufacture: { type: String }, // Alias used in delivery RC section
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
    experienceCurrent: { type: Number }, // Alias used in UI
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
    postFileStatus: { type: String },
    completedDate: { type: Date },

    // ===== APPROVAL STAGE (Only approval data, NO payout yet) =====
    approval_bankId: { type: String },
    approval_bankName: { type: String },
    approval_loanAmountApproved: { type: Number },
    approval_roi: { type: Number },
    approval_tenureMonths: { type: Number },
    approval_processingFees: { type: Number },
    approval_status: { type: String }, // "Approved", "Rejected", "Pending"
    approval_statusHistory: { type: Array, default: [] },
    approval_approvalDate: { type: Date },
    approval_remarks: { type: String },
    
    // Multi-Bank Data
    approval_banksData: { type: Array, default: [] }, 

    // ===== DISBURSEMENT STAGE (NEW - Separate from Approval) =====
    disburse_status: { type: String }, // "Pending", "Disbursed", "Cancelled"
    disbursementStatus: { type: String }, // Alias
    disbursement_status: { type: String }, // Alias
    disburse_bankName: { type: String }, // Bank that actually disbursed
    disburse_amount: { type: Number }, // Actual disbursed amount
    disburseAmount: { type: Number }, // Alias
    disburse_date: { type: Date },
    disbursedDate: { type: Date }, // Alias
    disbursementDate: { type: Date }, // Alias
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
    billNumber: { type: String }, // Alias
    billDate: { type: Date }, // Alias
    
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
    insurance_policy_start_date: { type: Date },
    insurance_policy_duration_od: { type: String },
    insurance_policy_end_date_od: { type: Date },

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
    postfile_documents_ledger: [mongoose.Schema.Types.Mixed],
    
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
    
    // Authorised Signatory
    signatorySameAsCoApplicant: { type: Boolean },
    signatory_id: { type: String },
    signatory_customerName: { type: String },
    signatory_primaryMobile: { type: String },
    signatory_address: { type: String },
    signatory_pincode: { type: String },
    signatory_city: { type: String },
    signatory_dob: { type: Date },
    signatory_gender: { type: String },
    signatory_designation: { type: String },
    signatory_pan: { type: String },
    signatory_aadhaar: { type: String },
    
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
    postfile_emiPlan: { type: String },
    postfile_emiMode: { type: String },
    emiPlan: { type: String },
    emiMode: { type: String },
    postfile_roiType: { type: String }, // Fixed / Floating
    postfile_sameAsApproved: { type: String }, // Yes / No
    postfile_approvalDate: { type: Date }, // Alias
    postfile_maturityDate: { type: Date },
    
    // PostFile Disbursal Breakup (Net Loan Amount for Disbursal)
    postfile_disbursedLoan: { type: Number },
    postfile_disbursedCreditAssured: { type: Number },
    postfile_disbursedInsurance: { type: Number },
    postfile_disbursedEw: { type: Number },
    postfile_disbursedLoanTotal: { type: Number }, // Alias
    postfile_tags: { type: [String], default: [] },
    
    // Dispatch & Disbursement
    dispatch_date: { type: Date },
    dispatch_time: { type: String },
    dispatch_through: { type: String },
    disbursement_date: { type: Date },
    disbursement_time: { type: String },
    loan_number: { type: String },
    rc_inv_storage_number: { type: String },
    
    // Instrument Details
    instrumentType: { type: String },
    nach_accountNumber: { type: String },
    nach_signedBy: { type: String },
    nach_image: { type: String },
    si_accountNumber: { type: String },
    si_signedBy: { type: String },
    si_image: { type: String },
    ecs_micrCode: { type: String },
    ecs_bankName: { type: String },
    ecs_accountNumber: { type: String },
    ecs_date: { type: Date },
    ecs_amount: { type: Number },
    ecs_tag: { type: String },
    ecs_favouring: { type: String },
    ecs_signedBy: { type: String },
    ecs_image: { type: String },
    cheque_1_number: { type: String },
    cheque_1_bankName: { type: String },
    cheque_1_accountNumber: { type: String },
    cheque_1_date: { type: Date },
    cheque_1_amount: { type: Number },
    cheque_1_tag: { type: String },
    cheque_1_favouring: { type: String },
    cheque_1_signedBy: { type: String },
    cheque_1_image: { type: String },
    cheque_2_number: { type: String },
    cheque_2_bankName: { type: String },
    cheque_2_accountNumber: { type: String },
    cheque_2_date: { type: Date },
    cheque_2_amount: { type: Number },
    cheque_2_tag: { type: String },
    cheque_2_favouring: { type: String },
    cheque_2_signedBy: { type: String },
    cheque_2_image: { type: String },
    cheque_3_number: { type: String },
    cheque_3_bankName: { type: String },
    cheque_3_accountNumber: { type: String },
    cheque_3_date: { type: Date },
    cheque_3_amount: { type: Number },
    cheque_3_tag: { type: String },
    cheque_3_favouring: { type: String },
    cheque_3_signedBy: { type: String },
    cheque_3_image: { type: String },
    cheque_4_number: { type: String },
    cheque_4_bankName: { type: String },
    cheque_4_accountNumber: { type: String },
    cheque_4_date: { type: Date },
    cheque_4_amount: { type: Number },
    cheque_4_tag: { type: String },
    cheque_4_favouring: { type: String },
    cheque_4_signedBy: { type: String },
    cheque_4_image: { type: String },
    cheque_5_number: { type: String },
    cheque_5_bankName: { type: String },
    cheque_5_accountNumber: { type: String },
    cheque_5_date: { type: Date },
    cheque_5_amount: { type: Number },
    cheque_5_tag: { type: String },
    cheque_5_favouring: { type: String },
    cheque_5_signedBy: { type: String },
    cheque_5_image: { type: String },
    cheque_6_number: { type: String },
    cheque_6_bankName: { type: String },
    cheque_6_accountNumber: { type: String },
    cheque_6_date: { type: Date },
    cheque_6_amount: { type: Number },
    cheque_6_tag: { type: String },
    cheque_6_favouring: { type: String },
    cheque_6_signedBy: { type: String },
    cheque_6_image: { type: String },
    cheque_7_number: { type: String },
    cheque_7_bankName: { type: String },
    cheque_7_accountNumber: { type: String },
    cheque_7_date: { type: Date },
    cheque_7_amount: { type: Number },
    cheque_7_tag: { type: String },
    cheque_7_favouring: { type: String },
    cheque_7_signedBy: { type: String },
    cheque_7_image: { type: String },
    cheque_8_number: { type: String },
    cheque_8_bankName: { type: String },
    cheque_8_accountNumber: { type: String },
    cheque_8_date: { type: Date },
    cheque_8_amount: { type: Number },
    cheque_8_tag: { type: String },
    cheque_8_favouring: { type: String },
    cheque_8_signedBy: { type: String },
    cheque_8_image: { type: String },
    cheque_9_number: { type: String },
    cheque_9_bankName: { type: String },
    cheque_9_accountNumber: { type: String },
    cheque_9_date: { type: Date },
    cheque_9_amount: { type: Number },
    cheque_9_tag: { type: String },
    cheque_9_favouring: { type: String },
    cheque_9_signedBy: { type: String },
    cheque_9_image: { type: String },
    cheque_10_number: { type: String },
    cheque_10_bankName: { type: String },
    cheque_10_accountNumber: { type: String },
    cheque_10_date: { type: Date },
    cheque_10_amount: { type: Number },
    cheque_10_tag: { type: String },
    cheque_10_favouring: { type: String },
    cheque_10_signedBy: { type: String },
    cheque_10_image: { type: String },
    cheque_11_number: { type: String },
    cheque_11_bankName: { type: String },
    cheque_11_accountNumber: { type: String },
    cheque_11_date: { type: Date },
    cheque_11_amount: { type: Number },
    cheque_11_tag: { type: String },
    cheque_11_favouring: { type: String },
    cheque_11_signedBy: { type: String },
    cheque_11_image: { type: String },
    cheque_12_number: { type: String },
    cheque_12_bankName: { type: String },
    cheque_12_accountNumber: { type: String },
    cheque_12_date: { type: Date },
    cheque_12_amount: { type: Number },
    cheque_12_tag: { type: String },
    cheque_12_favouring: { type: String },
    cheque_12_signedBy: { type: String },
    cheque_12_image: { type: String },
    cheque_13_number: { type: String },
    cheque_13_bankName: { type: String },
    cheque_13_accountNumber: { type: String },
    cheque_13_date: { type: Date },
    cheque_13_amount: { type: Number },
    cheque_13_tag: { type: String },
    cheque_13_favouring: { type: String },
    cheque_13_signedBy: { type: String },
    cheque_13_image: { type: String },
    cheque_14_number: { type: String },
    cheque_14_bankName: { type: String },
    cheque_14_accountNumber: { type: String },
    cheque_14_date: { type: Date },
    cheque_14_amount: { type: Number },
    cheque_14_tag: { type: String },
    cheque_14_favouring: { type: String },
    cheque_14_signedBy: { type: String },
    cheque_14_image: { type: String },
    cheque_15_number: { type: String },
    cheque_15_bankName: { type: String },
    cheque_15_accountNumber: { type: String },
    cheque_15_date: { type: Date },
    cheque_15_amount: { type: Number },
    cheque_15_tag: { type: String },
    cheque_15_favouring: { type: String },
    cheque_15_signedBy: { type: String },
    cheque_15_image: { type: String },
    cheque_16_number: { type: String },
    cheque_16_bankName: { type: String },
    cheque_16_accountNumber: { type: String },
    cheque_16_date: { type: Date },
    cheque_16_amount: { type: Number },
    cheque_16_tag: { type: String },
    cheque_16_favouring: { type: String },
    cheque_16_signedBy: { type: String },
    cheque_16_image: { type: String },
    cheque_17_number: { type: String },
    cheque_17_bankName: { type: String },
    cheque_17_accountNumber: { type: String },
    cheque_17_date: { type: Date },
    cheque_17_amount: { type: Number },
    cheque_17_tag: { type: String },
    cheque_17_favouring: { type: String },
    cheque_17_signedBy: { type: String },
    cheque_17_image: { type: String },
    cheque_18_number: { type: String },
    cheque_18_bankName: { type: String },
    cheque_18_accountNumber: { type: String },
    cheque_18_date: { type: Date },
    cheque_18_amount: { type: Number },
    cheque_18_tag: { type: String },
    cheque_18_favouring: { type: String },
    cheque_18_signedBy: { type: String },
    cheque_18_image: { type: String },
    cheque_19_number: { type: String },
    cheque_19_bankName: { type: String },
    cheque_19_accountNumber: { type: String },
    cheque_19_date: { type: Date },
    cheque_19_amount: { type: Number },
    cheque_19_tag: { type: String },
    cheque_19_favouring: { type: String },
    cheque_19_signedBy: { type: String },
    cheque_19_image: { type: String },
    cheque_20_number: { type: String },
    cheque_20_bankName: { type: String },
    cheque_20_accountNumber: { type: String },
    cheque_20_date: { type: Date },
    cheque_20_amount: { type: Number },
    cheque_20_tag: { type: String },
    cheque_20_favouring: { type: String },
    cheque_20_signedBy: { type: String },
    cheque_20_image: { type: String },
    
    // Record Details / Section 7
    receivingDate: { type: Date },
    receivingTime: { type: String },
    referenceName: { type: String },
    referenceNumber: { type: String },
    docsPreparedBy: { type: String },
    docs_prepared_by: { type: String },
    remarks: { type: String },
    paid_date: { type: Date },
    payment_date: { type: Date },
    payment_amount: { type: Number },
    payment_remarks: { type: String },
    received_date: { type: Date },
    
    // Finance Details
    typeOfLoan: { type: String },
    financeExpectation: { type: Number }, // Expected Funding
    downPayment: { type: Number },
    loanTenureMonths: { type: Number }, // Requested Tenure in Months
    isFinanced: { type: String }, // Yes / No
    customLoanAmount: { type: Number },
    customTenure: { type: Number },
    customRate: { type: Number },
    
    // Bulk Loan Creation
    numberOfCars: { type: Number },
    isMultipleCars: { type: Boolean },
    isSameVehicle: { type: Boolean },
    
    // Lead Details
    leadType: { type: String },
    leadSource: { type: String },
    
    // General Extras
    nomineeName: { type: String },
    nomineeDob: { type: Date },
    nomineeRelation: { type: String },
    loan_notes: { type: String },
    payoutStatus: { type: String },
    deliveryStatus: { type: String },
    photoUrl: { type: String },
    signatureUrl: { type: String },
    
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
    businessName: { type: String }, // Alias used in vehicle verification flow
    cinNumber: { type: String },
    companyName: { type: String },
    companyAddress: { type: String },
    companyPincode: { type: String },
    companyCity: { type: String },
    companyPhone: { type: String },
    companyType: { type: String },
    businessNature: { type: [String] },
    companyPartners: { type: Array, default: [] },
    
    // Professional Details
    professionalType: { type: String },
    designation: { type: String },
    currentExp: { type: Number },
    totalExp: { type: Number },
    totalExperience: { type: Number }, // Alias used by profile/pre-file UI
    incorporationYear: { type: Number },
    
    // Extra Fields
    customerIdDisplay: { type: String },
    educationOther: { type: String },
    yearsInCurrentHouse: { type: Number },
    yearsInCurrentCity: { type: Number },
    yearOfReg: { type: String },
    vehicleRegNo: { type: String }, // Header alias
    vehicleChassisNo: { type: String }, // Header alias
    vehicleEngineNo: { type: String }, // Header alias
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
    __deliveryInitialized: { type: Boolean, default: false },
    __dispatchInitialized: { type: Boolean, default: false },
    bulk_next_action: { type: String },
    bulk_priority: { type: String },
    bulk_remarks: { type: String },
    collection_next_action: { type: String },
    collection_last_followup: { type: Date },
    collection_remarks: { type: String },
    
    // --- Bulk ---
    isBulk: { type: Boolean, default: false },
    bulkCount: { type: Number },
    isCashCase: { type: Boolean, default: false },
    latestBusinessDate: { type: Date },

    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
  },
  {
    timestamps: true,
    strict: false, // Allow any additional fields from form
  }
);

const inferIsCashCase = (doc) => {
  const isFinancedText = String(doc?.isFinanced ?? doc?.isFinanceRequired ?? "")
    .trim()
    .toLowerCase();
  if (isFinancedText === "no" || isFinancedText === "false") return true;
  if (isFinancedText === "yes" || isFinancedText === "true") return false;

  const loanTypeText = String(
    doc?.typeOfLoan || doc?.loanType || doc?.caseType || doc?.loan_type || "",
  )
    .trim()
    .toLowerCase();
  return loanTypeText.includes("cash");
};

const pickLatestBusinessDate = (doc, isCashCase) => {
  if (isCashCase) {
    return (
      doc?.delivery_date ||
      doc?.deliveryDate ||
      doc?.delivery_done_at ||
      doc?.vehicleDeliveryDate ||
      null
    );
  }
  return (
    doc?.disbursement_date ||
    doc?.approval_disbursedDate ||
    doc?.disbursedDate ||
    null
  );
};

loanSchema.pre("save", function computeBusinessFields() {
  try {
    const cash = inferIsCashCase(this);
    this.isCashCase = cash;
    this.latestBusinessDate = pickLatestBusinessDate(this, cash) || null;
  } catch (_) {
    // Best-effort derived fields only
  }
});

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
loanSchema.index({ updatedAt: -1, _id: -1 });
loanSchema.index({ disbursement_date: -1, _id: -1 });
loanSchema.index({ delivery_date: -1, _id: -1 });
loanSchema.index({ latestBusinessDate: -1, _id: -1 });
loanSchema.index({ isCashCase: 1, latestBusinessDate: -1 });
loanSchema.index({ loan_number: 1 });
loanSchema.index({ registrationNumber: 1 });
loanSchema.index({ rc_redg_no: 1 });
loanSchema.index({ approval_bankName: 1 });
loanSchema.index({ postfile_bankName: 1 });
loanSchema.index({ currentStage: 1, status: 1 });
loanSchema.index({ status: 1, updatedAt: -1 });
loanSchema.index({ approval_approvalDate: -1 });


const Loan = mongoose.model('Loan', loanSchema);

export default Loan;
