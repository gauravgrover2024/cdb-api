import mongoose from 'mongoose';
import { buildSearchTokens } from "../utils/searchTokens.js";

const bankDetailSchema = new mongoose.Schema(
  {
    bankName: { type: String },
    accountNumber: { type: String },
    ifscCode: { type: String },
    ifsc: { type: String },
    branch: { type: String },
    accountType: { type: String },
    accountSinceYears: { type: Number },
    openedIn: { type: Number },
  },
  { _id: false },
);

const customerSchema = mongoose.Schema(
  {
    customerId: { type: String, unique: true },
    customerName: { type: String, required: true },

    // --- Contact ---
    primaryMobile: { type: String, required: true },
    extraMobiles: { type: [String], default: [] }, // Alt numbers
    email: { type: String },
    contactPersonName: { type: String },
    contactPersonMobile: { type: String },
    searchTokens: { type: [String], default: [] },

    // --- Personal ---
    sdwOf: { type: String }, // Son/Daughter/Wife of
    fatherName: { type: String },
    dob: { type: Date }, // ISO date format
    gender: { type: String },
    motherName: { type: String },
    maritalStatus: { type: String },
    dependents: { type: Number },
    education: { type: String },
    educationOther: { type: String },

    // --- Housing ---
    yearsInCurrentHouse: { type: Number },
    yearsInCurrentCity: { type: Number },
    houseType: { type: String }, // Owned, Rented, etc.
    residenceAddress: { type: String },
    pincode: { type: String },
    city: { type: String },
    state: { type: String },
    yearsInCurrentCity: { type: Number },

    // --- References ---
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

    // --- Nomination ---
    nomineeName: { type: String },
    nomineeDob: { type: Date },
    nomineeRelation: { type: String },

    // --- ID Proofs (KYC) ---
    panNumber: { type: String },
    aadharNumber: { type: String }, // Primary spelling
    aadhaarNumber: { type: String }, // Alias for compatibility
    voterId: { type: String },
    dlNumber: { type: String },
    passportNumber: { type: String },
    gstNumber: { type: String },

    // Additional proof fields for loan integration
    identityProofType: { type: String },
    identityProofNumber: { type: String },
    identityProofExpiry: { type: Date },
    addressProofType: { type: String },
    addressProofNumber: { type: String },

    // Docs URLs
    panCardDocUrl: { type: String },
    aadhaarCardDocUrl: { type: String },
    passportDocUrl: { type: String },
    passportBackDocUrl: { type: String },
    gstDocUrl: { type: String },
    dlDocUrl: { type: String },
    addressProofDocUrl: { type: String },

    // --- Professional / Occupation ---
    customerType: { type: String, default: "New" }, // New, Repeat
    applicantType: { type: String, default: "Individual" },
    isMSME: { type: String },
    occupationType: { type: String }, // Salaried, Self-Employed, Professional
    employmentType: { type: String }, // Additional employment type
    professionalType: { type: String }, // Doctor, CA, etc.
    companyType: { type: String }, // Pvt Ltd, Propreitorship
    businessNature: { type: Array },

    companyName: { type: String },
    designation: { type: String },
    currentExp: { type: String }, // In years - numeric
    totalExp: { type: String }, // In years - numeric
    experienceCurrent: { type: String },
    totalExperience: { type: String },

    // Employment Address (Aliases for Loan compatibility)
    employmentAddress: { type: String }, // Alias for companyAddress
    employmentPincode: { type: String },
    employmentCity: { type: String },
    employmentPhone: { type: String },

    // Office Address
    companyAddress: { type: String },
    companyPincode: { type: String },
    companyCity: { type: String },
    companyPhone: { type: String }, // Landline
    officialEmail: { type: String },
    companyPartners: { type: Array, default: [] },

    // Income
    monthlyIncome: { type: Number }, // Self Employed
    salaryMonthly: { type: Number }, // Salaried (primary)
    monthlySalary: { type: Number }, // Alias
    annualIncome: { type: Number },
    totalIncomeITR: { type: Number }, // Total Income as per ITR
    annualTurnover: { type: Number }, // For Self Employed
    netProfit: { type: Number }, // For Self Employed
    otherIncome: { type: Number },
    otherIncomeSource: { type: String },

    // Loan Request Details
    typeOfLoan: { type: String }, // Type of loan requested
    loanTenureMonths: { type: Number }, // Requested Tenure in Months

    // --- Banking ---
    bankName: { type: String },
    accountNumber: { type: String },
    ifscCode: { type: String },
    ifsc: { type: String }, // Alias
    branch: { type: String },
    accountType: { type: String },
    accountSinceYears: { type: Number },
    openedIn: { type: Number },
    bankDetails: { type: [bankDetailSchema], default: [] },

    // Additional fields for compatibility
    currentAddress: { type: String },
    officeAddress: { type: String },
    incorporationYear: { type: String },
    docsPreparedBy: { type: String },

    // Registration snapshot aliases (used in cash/profile flows)
    registerSameAsAadhaar: { type: String },
    registerSameAsPermanent: { type: String },
    registrationAddress: { type: String },
    registrationPincode: { type: String },
    registrationCity: { type: String },

    // Embedded co-applicant quick snapshot (customer module reuse)
    co_customerName: { type: String },
    co_primaryMobile: { type: String },
    co_dob: { type: Date },
    co_pan: { type: String },
    co_address: { type: String },

    // Embedded signatory snapshot (company flows)
    signatory_customerName: { type: String },
    signatory_primaryMobile: { type: String },
    signatory_address: { type: String },
    signatory_pincode: { type: String },
    signatory_city: { type: String },
    signatory_dob: { type: Date },
    signatory_designation: { type: String },
    signatory_pan: { type: String },
    signatory_aadhaar: { type: String },

    // KYC Workflow
    kycStatus: {
      type: String,
      enum: ["Pending Docs", "In Progress", "Completed", "Rejected"],
      default: "Pending Docs",
    },

    // Additional loan-related fields
    permanentAddress: { type: String },
    permanentPincode: { type: String },
    permanentCity: { type: String },
    sameAsCurrentAddress: { type: Boolean },
    addressType: { type: String },

    employmentAddress: { type: String },
    employmentPincode: { type: String },
    employmentCity: { type: String },
    employmentPhone: { type: String },

    whatsappNumber: { type: String },
    emailAddress: { type: String }, // Alias for email

    // Notes
    loan_notes: { type: String }, // Internal notes for this customer

    // Metadata
    createdOn: { type: String }, // Format: "DD-MM-YYYY" often used in frontend
    createdBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  },
  {
    timestamps: true,
    strict: false,
  },
);

// Index for search
customerSchema.index({ "bankDetails.ifscCode": 1 });
customerSchema.index({ "bankDetails.accountNumber": 1 });
customerSchema.index({ searchTokens: 1 });

customerSchema.pre("save", function () {
  this.searchTokens = buildSearchTokens([
    this.customerId,
    this.customerName,
    this.companyName,
    this.contactPersonName,
    this.primaryMobile,
    this.extraMobiles,
    this.whatsappNumber,
    this.panNumber,
    this.aadharNumber,
    this.aadhaarNumber,
    this.gstNumber,
    this.city,
    this.state,
    this.companyCity,
    this.registrationCity,
  ]);
});

const Customer = mongoose.model('Customer', customerSchema);

export default Customer;
