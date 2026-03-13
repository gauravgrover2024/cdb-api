import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import BankDirectory from "../models/BankDirectory.js";
import Loan from "../models/Loan.js";
import Customer from "../models/Customer.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";
import {
  calculatePayoutsOnDisbursement,
  validateDisbursementData,
} from "../services/payoutService.js";
import { upsertVehicleRecordFromLoan } from "../services/vehicleRecordService.js";

// Fields to sync from Loan -> Customer (comprehensive list)
const CUSTOMER_SYNC_FIELDS = [
  "customerName",
  "primaryMobile",
  "extraMobiles",
  "whatsappNumber",
  "emailAddress",
  "email",
  "sdwOf",
  "fatherName",
  "motherName",
  "dob",
  "gender",
  "maritalStatus",
  "dependents",
  "residenceAddress",
  "pincode",
  "city",
  "state",
  "yearsInCurrentHouse",
  "yearsInCurrentCity",
  "houseType",
  "education",
  "educationOther",
  "addressType",
  "panNumber",
  "aadhaarNumber",
  "aadharNumber",
  "voterId",
  "dlNumber",
  "passportNumber",
  "gstNumber",
  "identityProofType",
  "identityProofNumber",
  "identityProofExpiry",
  "addressProofType",
  "addressProofNumber",
  "panCardDocUrl",
  "aadhaarCardDocUrl",
  "passportDocUrl",
  "gstDocUrl",
  "dlDocUrl",
  "addressProofDocUrl",
  "currentAddress",
  "permanentAddress",
  "permanentPincode",
  "permanentCity",
  "officeAddress",
  "employmentType",
  "occupationType",
  "professionalType",
  "monthlyIncome",
  "salaryMonthly",
  "monthlySalary",
  "annualIncome",
  "totalIncomeITR",
  "annualTurnover",
  "netProfit",
  "otherIncome",
  "otherIncomeSource",
  "companyName",
  "designation",
  "companyType",
  "businessNature",
  "incorporationYear",
  "currentExp",
  "totalExp",
  "companyAddress",
  "companyPincode",
  "companyCity",
  "companyPhone",
  "employmentAddress",
  "employmentPincode",
  "employmentCity",
  "employmentPhone",
  "officialEmail",
  "typeOfLoan",
  "financeExpectation",
  "loanTenureMonths",
  "nomineeName",
  "nomineeDob",
  "nomineeRelation",
  "reference1_name",
  "reference1_mobile",
  "reference1_address",
  "reference1_pincode",
  "reference1_city",
  "reference1_relation",
  "reference2_name",
  "reference2_mobile",
  "reference2_address",
  "reference2_pincode",
  "reference2_city",
  "reference2_relation",
  "bankName",
  "accountNumber",
  "ifscCode",
  "ifsc",
  "branch",
  "accountType",
  "loan_notes",
  "kycStatus",
  "referenceName",
  "referenceNumber",
  "customerType",
  "createdOn",
  "createdBy",
  "contactPersonName",
  "contactPersonMobile",
  "sameAsCurrentAddress",
  "isMSME",
  "experienceCurrent",
  "totalExperience",
  "companyPartners",
];

const IFSC_CODE_BANK_MAP = {
  HDFC: "HDFC Bank",
  ICIC: "ICICI Bank",
  SBIN: "State Bank of India",
  UTIB: "Axis Bank",
  KKBK: "Kotak Mahindra Bank",
  FDRL: "Federal Bank",
  PUNB: "Punjab National Bank",
  CNRB: "Canara Bank",
  IDIB: "Indian Bank",
  BARB: "Bank of Baroda",
  BKID: "Bank of India",
  UBIN: "Union Bank of India",
  INDB: "IndusInd Bank",
  YESB: "Yes Bank",
  IDFB: "IDFC First Bank",
  MAHB: "Bank of Maharashtra",
};

const MICR_BANK_CODE_MAP = {
  "002": "State Bank of India",
  "012": "Bank of Baroda",
  "013": "Bank of India",
  "015": "Canara Bank",
  "019": "Indian Bank",
  "026": "Union Bank of India",
  "176": "Punjab National Bank",
  "211": "Axis Bank",
  "229": "ICICI Bank",
  "237": "IndusInd Bank",
  "240": "HDFC Bank",
  "425": "Federal Bank",
  "485": "Kotak Mahindra Bank",
  "532": "Yes Bank",
  "760": "IDFC First Bank",
};

const normalizeIfsc = (value) =>
  String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 11);

const normalizeMicr = (value) =>
  String(value || "")
    .replace(/\D/g, "")
    .slice(0, 9);

const inferBankNameFromIfsc = (value) => {
  const ifsc = normalizeIfsc(value);
  if (!/^[A-Z]{4}0[A-Z0-9]{6}$/.test(ifsc)) return "";
  return IFSC_CODE_BANK_MAP[ifsc.slice(0, 4)] || "";
};

const inferBankNameFromMicr = (value) => {
  const micr = normalizeMicr(value);
  if (micr.length !== 9) return "";
  return MICR_BANK_CODE_MAP[micr.slice(3, 6)] || "";
};

const fetchIfscFromRazorpay = async (ifsc) => {
  const normalized = normalizeIfsc(ifsc);
  if (!/^[A-Z]{4}0[A-Z0-9]{6}$/.test(normalized)) return null;

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 4000);
  try {
    const res = await fetch(`https://ifsc.razorpay.com/${normalized}`, {
      method: "GET",
      signal: controller.signal,
    });

    if (!res.ok) {
      return { notFound: res.status === 404, status: res.status };
    }

    const data = await res.json();
    return {
      ifsc: normalized,
      micr: normalizeMicr(data?.MICR),
      bankName: String(data?.BANK || "").trim(),
      branch: String(data?.BRANCH || "").trim(),
      address: String(data?.ADDRESS || "").trim(),
      city: String(data?.CITY || "").trim(),
      state: String(data?.STATE || "").trim(),
      district: String(data?.DISTRICT || "").trim(),
      contact: String(data?.CONTACT || "").trim(),
      active: true,
      source: "razorpay-ifsc",
      lastVerifiedAt: new Date(),
      raw: data,
    };
  } catch (error) {
    return { error: error?.name === "AbortError" ? "timeout" : "fetch_failed" };
  } finally {
    clearTimeout(timeout);
  }
};

// Helper function to save document with retry logic and reload on version conflicts
const saveWithRetry = async (doc, maxRetries = 3) => {
  let retries = maxRetries;
  let lastError;
  let docToSave = doc;

  while (retries > 0) {
    try {
      return await docToSave.save();
    } catch (error) {
      lastError = error;
      if (error.name === "VersionError" && retries > 1) {
        console.warn(
          `⚠️ VersionError on save: Document was modified elsewhere. Retrying with fresh copy... (${retries - 1} attempts left)`,
        );

        // Reload the document to get latest version
        const id = docToSave._id;
        docToSave = await doc.constructor.findById(id);

        if (!docToSave) {
          throw new Error("Document was deleted during update operation");
        }

        // Re-apply the changes from the original document object
        // We do this by copying non-system fields from the original
        const originalFields = doc.toObject();
        Object.keys(originalFields).forEach((key) => {
          if (!["_id", "__v", "createdAt", "updatedAt"].includes(key)) {
            docToSave[key] = originalFields[key];
          }
        });

        retries--;
        // Wait a bit before retrying to avoid thundering herd
        await new Promise((resolve) => setTimeout(resolve, 100));
      } else {
        throw error;
      }
    }
  }

  throw lastError;
};

// Normalize common aliases sent by frontend
const normalizeCustomerFields = (payload) => {
  const normalized = { ...payload };

  // Standardize dates to ISO format or handle consistently - COMPREHENSIVE LIST
  const dateFields = [
    // Personal & Co-Applicant & Guarantor
    "dob",
    "co_dob",
    "gu_dob",
    "nomineeDob",
    "leadDate",
    "leadTime",
    // Identity & Proofs
    "identityProofExpiry",
    "insuranceExpiry",
    // Registration & Approval
    "rc_redg_date",
    "approval_approvalDate",
    "approval_disbursedDate",
    "postfile_firstEmiDate",
    "postfile_approvalDate",
    "dispatch_date",
    "disbursement_date",

    "ecs_date",
    "signatory_dob",
    // Delivery & Invoice
    "do_date",
    "delivery_date",
    "invoice_date",
    "invoice_received_date",
    // RC
    "rc_received_date",
  ];

  const chequeDateFields = Array.from({ length: 20 }, (_, i) => `cheque_${i + 1}_date`);
  dateFields.push(...chequeDateFields);

  dateFields.forEach((field) => {
    if (normalized[field] && typeof normalized[field] === "string") {
      // If it's DD-MM-YYYY format, convert to ISO
      const match = normalized[field].match(/^(\d{2})-(\d{2})-(\d{4})$/);
      if (match) {
        normalized[field] = new Date(`${match[3]}-${match[2]}-${match[1]}`);
      } else {
        // Try parsing as ISO
        const parsed = new Date(normalized[field]);
        if (!isNaN(parsed.getTime())) {
          normalized[field] = parsed;
        }
      }
    }
  });

  // Ensure numeric fields - COMPREHENSIVE LIST including all loan & pricing fields
  const numericFields = [
    // Personal Details
    "yearsInCurrentHouse",
    "yearsInCurrentCity",
    "dependents",
    // Co-Applicant
    "co_dependents",
    "co_currentExp",
    "co_currentExperience",
    "co_totalExp",
    "co_totalExperience",
    "co_salaryMonthly",
    "co_monthlySalary",
    "co_monthlyIncome",
    "co_annualIncome",
    // Guarantor
    "gu_dependents",
    "gu_currentExp",
    "gu_totalExp",
    "gu_salaryMonthly",
    "gu_monthlySalary",
    "gu_monthlyIncome",
    "gu_annualIncome",
    // Employment & Income - CRITICAL ADDITIONS
    "monthlyIncome",
    "monthlySalary",
    "salaryMonthly",
    "annualIncome",
    "currentExp",
    "totalExp",
    "totalIncomeITR",
    "annualTurnover",
    "netProfit",
    "otherIncome",
    // Vehicle Pricing - CRITICAL
    "exShowroomPrice",
    "insuranceCost",
    "roadTax",
    "accessoriesAmount",
    "dealerDiscount",
    "manufacturerDiscount",
    "marginMoney",
    "advanceEmi",
    "tradeInValue",
    "otherDiscounts",
    "onRoadPrice",
    // Loan Parameters - CRITICAL
    "loanAmount",
    "requiredLoanAmount",
    "tenure",
    "interestRate",
    "loanTenureMonths",
    "financeExpectation",
    // Approval Details
    "approval_loanAmountApproved",
    "approval_loanAmountDisbursed",
    "approval_roi",
    "approval_tenureMonths",
    "approval_processingFees",
    // Payout
    "payoutPercentage",
    "payoutAmount",
    "prefile_sourcePayoutPercentage",
    // Breakup Fields
    "approval_breakup_netLoanApproved",
    "approval_breakup_creditAssured",
    "approval_breakup_insuranceFinance",
    "approval_breakup_ewFinance",
    // Additional Experience & Years
    "incorporationYear",
    "accountSinceYears",
    "openedIn",
    "experienceCurrent",
    "totalExperience", // Frontend Aliases
    "ecs_amount",

  ];

  const chequeAmountFields = Array.from({ length: 20 }, (_, i) => `cheque_${i + 1}_amount`);
  numericFields.push(...chequeAmountFields);

  numericFields.forEach((field) => {
    if (
      normalized[field] !== undefined &&
      normalized[field] !== null &&
      normalized[field] !== ""
    ) {
      if (
        typeof normalized[field] === "string" ||
        typeof normalized[field] === "boolean"
      ) {
        const num = Number(normalized[field]);
        if (!isNaN(num)) {
          normalized[field] = num;
        }
      }
    }
  });

  // Common Applicant Aliases
  if (normalized.aadhaarNumber && !normalized.aadharNumber)
    normalized.aadharNumber = normalized.aadhaarNumber;
  if (normalized.aadharNumber && !normalized.aadhaarNumber)
    normalized.aadhaarNumber = normalized.aadharNumber;
  if (normalized.emailAddress && !normalized.email)
    normalized.email = normalized.emailAddress;
  if (normalized.email && !normalized.emailAddress)
    normalized.emailAddress = normalized.email;
  if (normalized.ifsc && !normalized.ifscCode) normalized.ifscCode = normalized.ifsc;
  if (normalized.ifscCode && !normalized.ifsc) normalized.ifsc = normalized.ifscCode;
  if (normalized.ifsc) normalized.ifsc = normalizeIfsc(normalized.ifsc);
  if (normalized.ifscCode) normalized.ifscCode = normalizeIfsc(normalized.ifscCode);
  if (normalized.co_ifsc) normalized.co_ifsc = normalizeIfsc(normalized.co_ifsc);
  if (normalized.co_ifscCode) normalized.co_ifscCode = normalizeIfsc(normalized.co_ifscCode);
  if (normalized.gu_ifsc) normalized.gu_ifsc = normalizeIfsc(normalized.gu_ifsc);
  if (normalized.gu_ifscCode) normalized.gu_ifscCode = normalizeIfsc(normalized.gu_ifscCode);
  if (normalized.ecs_micrCode) normalized.ecs_micrCode = normalizeMicr(normalized.ecs_micrCode);
  const inferredMainBank = inferBankNameFromIfsc(normalized.ifscCode || normalized.ifsc);
  if (inferredMainBank) {
    if (!normalized.bankName) normalized.bankName = inferredMainBank;
    if (!normalized.approval_bankName) normalized.approval_bankName = inferredMainBank;
    if (!normalized.postfile_bankName) normalized.postfile_bankName = inferredMainBank;
    if (!normalized.disburse_bankName) normalized.disburse_bankName = inferredMainBank;
  }
  const inferredEcsBank = inferBankNameFromMicr(normalized.ecs_micrCode);
  if (inferredEcsBank && !normalized.ecs_bankName) normalized.ecs_bankName = inferredEcsBank;
  const inferredCoBank = inferBankNameFromIfsc(normalized.co_ifscCode || normalized.co_ifsc);
  if (inferredCoBank && !normalized.co_bankName) normalized.co_bankName = inferredCoBank;
  const inferredGuBank = inferBankNameFromIfsc(normalized.gu_ifscCode || normalized.gu_ifsc);
  if (inferredGuBank && !normalized.gu_bankName) normalized.gu_bankName = inferredGuBank;
  if (normalized.typeOfLoan && !normalized.loanType)
    normalized.loanType = normalized.typeOfLoan;
  if (normalized.loanType && !normalized.typeOfLoan)
    normalized.typeOfLoan = normalized.loanType;
  if (normalized.fatherName && !normalized.sdwOf)
    normalized.sdwOf = normalized.fatherName;
  if (normalized.businessNature && typeof normalized.businessNature === "string")
    normalized.businessNature = normalized.businessNature
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
  if (normalized.co_businessNature && typeof normalized.co_businessNature === "string")
    normalized.co_businessNature = normalized.co_businessNature
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);
  if (normalized.gu_businessNature && typeof normalized.gu_businessNature === "string")
    normalized.gu_businessNature = normalized.gu_businessNature
      .split(",")
      .map((value) => value.trim())
      .filter(Boolean);

  // Income Aliases - Ensure all variants are captured
  if (normalized.salaryMonthly && !normalized.monthlySalary)
    normalized.monthlySalary =
      parseInt(normalized.salaryMonthly, 10) || normalized.salaryMonthly;
  if (normalized.monthlySalary && !normalized.salaryMonthly)
    normalized.salaryMonthly =
      parseInt(normalized.monthlySalary, 10) || normalized.monthlySalary;
  if (normalized.monthlyIncome)
    normalized.monthlyIncome =
      parseInt(normalized.monthlyIncome, 10) || normalized.monthlyIncome;
  if (normalized.annualIncome)
    normalized.annualIncome =
      parseInt(normalized.annualIncome, 10) || normalized.annualIncome;

  // Experience Aliases
  if (normalized.experienceCurrent && !normalized.currentExp)
    normalized.currentExp = normalized.experienceCurrent;
  if (normalized.currentExp && !normalized.experienceCurrent)
    normalized.experienceCurrent = normalized.currentExp;
  if (normalized.totalExperience && !normalized.totalExp)
    normalized.totalExp = normalized.totalExperience;
  if (normalized.totalExp && !normalized.totalExperience)
    normalized.totalExperience = normalized.totalExp;

  // Co-Applicant Aliases
  if (normalized.co_aadhar && !normalized.co_aadhaar)
    normalized.co_aadhaar = normalized.co_aadhar;
  if (normalized.co_aadhaar && !normalized.co_aadhar)
    normalized.co_aadhar = normalized.co_aadhaar;
  if (normalized.co_currentExperience && !normalized.co_currentExp)
    normalized.co_currentExp = normalized.co_currentExperience;
  if (normalized.co_currentExp && !normalized.co_currentExperience)
    normalized.co_currentExperience = normalized.co_currentExp;
  if (normalized.co_totalExperience && !normalized.co_totalExp)
    normalized.co_totalExp = normalized.co_totalExperience;
  if (normalized.co_totalExp && !normalized.co_totalExperience)
    normalized.co_totalExperience = normalized.co_totalExp;
  if (normalized.co_occupationType && !normalized.co_occupation)
    normalized.co_occupation = normalized.co_occupationType;
  if (normalized.co_occupation && !normalized.co_occupationType)
    normalized.co_occupationType = normalized.co_occupation;
  if (normalized.co_salaryMonthly && !normalized.co_monthlySalary)
    normalized.co_monthlySalary =
      parseInt(normalized.co_salaryMonthly, 10) || normalized.co_salaryMonthly;
  if (normalized.co_monthlySalary && !normalized.co_salaryMonthly)
    normalized.co_salaryMonthly =
      parseInt(normalized.co_monthlySalary, 10) || normalized.co_monthlySalary;

  // Guarantor Aliases
  if (normalized.gu_aadhar && !normalized.gu_aadhaar)
    normalized.gu_aadhaar = normalized.gu_aadhar;
  if (normalized.gu_aadhaar && !normalized.gu_aadhar)
    normalized.gu_aadhar = normalized.gu_aadhaar;
  if (normalized.gu_occupationType && !normalized.gu_occupation)
    normalized.gu_occupation = normalized.gu_occupationType;
  if (normalized.gu_occupation && !normalized.gu_occupationType)
    normalized.gu_occupationType = normalized.gu_occupation;
  if (normalized.gu_salaryMonthly && !normalized.gu_monthlySalary)
    normalized.gu_monthlySalary =
      parseInt(normalized.gu_salaryMonthly, 10) || normalized.gu_salaryMonthly;
  if (normalized.gu_monthlySalary && !normalized.gu_salaryMonthly)
    normalized.gu_salaryMonthly =
      parseInt(normalized.gu_monthlySalary, 10) || normalized.gu_monthlySalary;
  // Flatten Reference Objects to Flat Fields
  if (normalized.reference1 && typeof normalized.reference1 === "object") {
    normalized.reference1_name = normalized.reference1.name;
    normalized.reference1_mobile = normalized.reference1.mobile;
    normalized.reference1_address = normalized.reference1.address;
    normalized.reference1_pincode = normalized.reference1.pincode;
    normalized.reference1_city = normalized.reference1.city;
    normalized.reference1_relation = normalized.reference1.relation;
    delete normalized.reference1;
  }
  if (normalized.reference2 && typeof normalized.reference2 === "object") {
    normalized.reference2_name = normalized.reference2.name;
    normalized.reference2_mobile = normalized.reference2.mobile;
    normalized.reference2_address = normalized.reference2.address;
    normalized.reference2_pincode = normalized.reference2.pincode;
    normalized.reference2_city = normalized.reference2.city;
    normalized.reference2_relation = normalized.reference2.relation;
    delete normalized.reference2;
  }

  // Pincode -> city fallback for cases where UI/import did not resolve city.
  const inferCityFromPin = (rawPin) => {
    const pin = String(rawPin || "").replace(/\D/g, "").slice(0, 6);
    if (!pin) return "";
    if (pin.startsWith("110")) return "Delhi";
    if (pin.startsWith("122")) return "Gurgaon";
    if (pin.startsWith("121")) return "Faridabad";
    if (pin.startsWith("2013")) return "Noida";
    if (pin.startsWith("2010")) return "Ghaziabad";
    return "";
  };
  [
    ["city", "pincode"],
    ["permanentCity", "permanentPincode"],
    ["employmentCity", "employmentPincode"],
    ["co_city", "co_pincode"],
    ["co_companyCity", "co_companyPincode"],
    ["gu_city", "gu_pincode"],
    ["gu_companyCity", "gu_companyPincode"],
    ["signatory_city", "signatory_pincode"],
    ["registrationCity", "registrationPincode"],
    ["reference1_city", "reference1_pincode"],
    ["reference2_city", "reference2_pincode"],
  ].forEach(([cityKey, pinKey]) => {
    if (normalized[cityKey]) return;
    const inferred = inferCityFromPin(normalized[pinKey]);
    if (inferred) normalized[cityKey] = inferred;
  });

  return normalized;
};

const normalizeInstrumentType = (value) => {
  const t = String(value || "").trim().toUpperCase();
  if (!t) return "";
  if (t.includes("CHEQUE") || t.includes("CHQ") || t.includes("PDC")) return "CHEQUE";
  if (t.includes("ECS")) return "ECS";
  if (t === "SI" || t.includes("STANDING INSTRUCTION")) return "SI";
  if (t.includes("NACH") || t.includes("MANDATE")) return "NACH";
  return "";
};

const sanitizeInstrumentForPersistence = (payload) => {
  if (!payload || typeof payload !== "object") return payload;
  const cleaned = { ...payload };
  const type = normalizeInstrumentType(cleaned.instrumentType);
  const chequeSuffixes = [
    "number",
    "bankName",
    "accountNumber",
    "date",
    "amount",
    "tag",
    "favouring",
    "signedBy",
    "image",
  ];

  const isFilled = (v) => {
    if (v === undefined || v === null) return false;
    if (typeof v === "number") return Number.isFinite(v) && v !== 0;
    const s = String(v).trim();
    if (!s) return false;
    if (s === "0" || s === "0.0" || s === "0.00") return false;
    return true;
  };

  for (let i = 1; i <= 20; i += 1) {
    const keys = chequeSuffixes.map((suffix) => `cheque_${i}_${suffix}`);
    const hasValue = keys.some((k) => isFilled(cleaned[k]));
    if (!hasValue) {
      keys.forEach((k) => {
        cleaned[k] = undefined;
      });
    }
  }

  const unsetEcs = () => {
    [
      "ecs_micrCode",
      "ecs_bankName",
      "ecs_accountNumber",
      "ecs_date",
      "ecs_amount",
      "ecs_tag",
      "ecs_favouring",
      "ecs_signedBy",
      "ecs_image",
    ].forEach((k) => {
      cleaned[k] = undefined;
    });
  };

  const unsetSi = () => {
    ["si_accountNumber", "si_signedBy", "si_image"].forEach((k) => {
      cleaned[k] = undefined;
    });
  };

  const unsetCheques = () => {
    for (let i = 1; i <= 20; i += 1) {
      chequeSuffixes.forEach((suffix) => {
        cleaned[`cheque_${i}_${suffix}`] = undefined;
      });
    }
  };

  if (type === "SI" || type === "NACH") {
    unsetEcs();
    unsetCheques();
  } else if (type === "ECS") {
    unsetSi();
  } else if (type === "CHEQUE") {
    unsetSi();
    unsetEcs();
  }

  return cleaned;
};

const splitUndefinedForMongo = (payload) => {
  const setPayload = {};
  const unsetPayload = {};
  Object.entries(payload || {}).forEach(([k, v]) => {
    if (v === undefined) unsetPayload[k] = 1;
    else setPayload[k] = v;
  });
  return { setPayload, unsetPayload };
};

const applyUndefinedOnDoc = (doc, payload) => {
  Object.entries(payload || {}).forEach(([k, v]) => {
    if (v === undefined) doc.set(k, undefined);
  });
};

const upsertBankDirectoryEntry = async ({
  ifsc,
  micr,
  bankName,
  branch,
  address,
  city,
  state,
  district,
  contact,
  source = "internal",
  active = true,
  raw = null,
}) => {
  const normalizedIfsc = normalizeIfsc(ifsc);
  const normalizedMicr = normalizeMicr(micr);
  if (!normalizedIfsc && !normalizedMicr) return null;

  const base = {
    ifsc: normalizedIfsc || undefined,
    micr: normalizedMicr || undefined,
    bankName: String(bankName || "").trim() || undefined,
    branch: String(branch || "").trim() || undefined,
    address: String(address || "").trim() || undefined,
    city: String(city || "").trim() || undefined,
    state: String(state || "").trim() || undefined,
    district: String(district || "").trim() || undefined,
    contact: String(contact || "").trim() || undefined,
    active: Boolean(active),
    source,
    lastVerifiedAt: new Date(),
    raw,
  };

  const query = normalizedIfsc
    ? { ifsc: normalizedIfsc }
    : { micr: normalizedMicr, bankName: base.bankName || inferBankNameFromMicr(normalizedMicr) };

  return await BankDirectory.findOneAndUpdate(
    query,
    { $set: base },
    { upsert: true, new: true, setDefaultsOnInsert: true },
  );
};

// Resolve customer by ObjectId or custom customerId
// Helper to sync bank details with Bank collection
const syncBankCollection = async (payload) => {
  const banksToSync = [
    {
      name: payload.bankName,
      ifsc: payload.ifscCode || payload.ifsc,
      address: payload.branch,
      micr: undefined,
    },
    {
      name: payload.co_bankName,
      ifsc: payload.co_ifscCode || payload.co_ifsc,
      address: payload.co_branch,
      micr: undefined,
    },
    {
      name: payload.gu_bankName,
      ifsc: payload.gu_ifscCode || payload.gu_ifsc,
      address: payload.gu_branch,
      micr: undefined,
    },
    {
      name: payload.ecs_bankName,
      ifsc: undefined,
      address: "",
      micr: payload.ecs_micrCode,
    },
  ];

  for (const bank of banksToSync) {
    if (bank.name && (bank.ifsc || bank.micr)) {
      try {
        await upsertBankDirectoryEntry({
          ifsc: bank.ifsc,
          micr: bank.micr,
          bankName: bank.name,
          branch: bank.address || "",
          address: bank.address || "",
          source: "loan-save",
          active: true,
        });
      } catch (err) {
        console.error("Error syncing bank collection:", err.message);
      }
    }
  }
};

const resolveCustomerById = async (customerIdValue) => {
  if (!customerIdValue) return null;

  if (String(customerIdValue).match(/^[0-9a-fA-F]{24}$/)) {
    return await Customer.findById(customerIdValue);
  }

  return await Customer.findOne({ customerId: customerIdValue });
};

// Helper: Get Next ID
const getNextId = async (Model, prefix, fieldName = "loanId") => {
  const today = new Date();
  const year = today.getFullYear();

  // Find last created document for THIS year
  const regex = new RegExp(`^${prefix}-${year}-\\d{4}$`);
  const query = {};
  query[fieldName] = { $regex: regex };

  const lastDoc = await Model.findOne(query).sort({ [fieldName]: -1 });

  let nextNum = 1;
  if (lastDoc && lastDoc[fieldName]) {
    const parts = lastDoc[fieldName].split("-");
    if (parts.length === 3) {
      const numPart = parseInt(parts[2], 10);
      if (!isNaN(numPart)) {
        nextNum = numPart + 1;
      }
    }
  }
  return `${prefix}-${year}-${String(nextNum).padStart(4, "0")}`;
};

// Determine if loan is for New Car
const isNewCarLoan = (loanDoc) => {
  const raw =
    loanDoc?.vehicleType || loanDoc?.loanType || loanDoc?.typeOfLoan || "";
  const normalized = String(raw)
    .trim()
    .toUpperCase()
    .replace(/[-_\s]+/g, " ");
  return (
    normalized === "NEW CAR" ||
    normalized === "NEWCAR" ||
    normalized === "NEW CAR LOAN" ||
    normalized === "NEW"
  );
};

// Ensure DO + Payment exist for a loan with proper data population
const ensureLinkedRecords = async (loanDoc) => {
  if (!loanDoc?.loanId) return;

  if (!isNewCarLoan(loanDoc)) {
    return;
  }
  try {
    // Create/Update DeliveryOrder with loan snapshot data.
    // Avoid transaction/session dependency so this stays safe in serverless/shared Mongo tiers.
    const doPayload = {
      loanId: loanDoc.loanId,
      do_loanId: loanDoc.loanId, // Maintain both field names for compatibility
      dealerName: loanDoc.dealerName,
      dealerAddress: loanDoc.dealerAddress,
      vehicleModel: loanDoc.vehicleModel,
      vehicleColor: loanDoc.vehicleColor,
      chassisNumber: loanDoc.chassisNumber,
      engineNumber: loanDoc.engineNumber,
      createdBy: loanDoc.createdBy || undefined,
    };

    await DeliveryOrder.findOneAndUpdate(
      { loanId: loanDoc.loanId },
      { $setOnInsert: doPayload },
      { upsert: true, new: true },
    );

    // Create Payment skeleton (idempotent upsert)
    const paymentPayload = {
      loanId: loanDoc.loanId,
      showroomRows: [],
      entryTotals: {},
      isVerified: false,
      autocreditsRows: [],
      autocreditsTotals: {},
      isAutocreditsVerified: false,
      createdBy: loanDoc.createdBy || undefined,
    };

    await Payment.findOneAndUpdate(
      { loanId: loanDoc.loanId },
      { $setOnInsert: paymentPayload },
      { upsert: true, new: true },
    );
  } catch (err) {
    // Linked records are supplementary, never block loan save/update flow.
    console.error("ensureLinkedRecords warning:", err?.message || err);
  }
};

const syncVehicleMasterRecord = async (loanDoc) => {
  try {
    await upsertVehicleRecordFromLoan(loanDoc);
  } catch (err) {
    // Vehicle master record is supplementary; loan save should never fail because of this.
    console.error("syncVehicleMasterRecord warning:", err?.message || err);
  }
};

const parseAmount = (value) => {
  if (value == null || value === "") return 0;
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  const parsed = Number(String(value).replace(/[^\d.-]/g, ""));
  return Number.isFinite(parsed) ? parsed : 0;
};

const escapeRegex = (value = "") =>
  String(value).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");

const LIST_COUNT_CACHE_TTL_MS = 45 * 1000;
const DASHBOARD_STATS_CACHE_TTL_MS = 60 * 1000;
const listCountCache = new Map();
let dashboardStatsCache = { ts: 0, data: null };
const clearLoanCaches = () => {
  listCountCache.clear();
  dashboardStatsCache = { ts: 0, data: null };
};

// @desc    Get loans with search + pagination
// @route   GET /api/loans
// @access  Public
const getLoans = asyncHandler(async (req, res) => {
  const startedAt = Date.now();
  const {
    search = "",
    q = "",
    skip = 0,
    page = 1,
    limit = 200,
    view = "",
    sortBy = "",
    sortDir = "desc",
  } = req.query;

  const safeLimit = Math.min(Math.max(Number(limit) || 200, 1), 1000);
  const safePage = Math.max(Number(page) || 1, 1);
  const safeSkip =
    Number.isFinite(Number(skip)) && Number(skip) >= 0
      ? Number(skip)
      : (safePage - 1) * safeLimit;
  const safeSearch = String(search || q || "").trim();
  const viewMode = String(view || "").toLowerCase();
  const direction = String(sortDir).toLowerCase() === "asc" ? 1 : -1;
  const sortField =
    String(sortBy || "").trim() || (viewMode === "dashboard" ? "latestBusiness" : "updatedAt");

  let query = {};
  if (safeSearch) {
    const escaped = escapeRegex(safeSearch);
    const isMostlyNumeric = /^[\d-]+$/.test(safeSearch);

    if (isMostlyNumeric) {
      // Prefix/exact style searches hit B-tree indexes and are much faster than global /i regex.
      query = {
        $or: [
          { loanId: new RegExp(`^${escaped}`) },
          { loan_number: new RegExp(`^${escaped}`) },
          { primaryMobile: new RegExp(`^${escaped}`) },
          { registrationNumber: new RegExp(`^${escaped}`) },
          { rc_redg_no: new RegExp(`^${escaped}`) },
        ],
      };
    } else {
      // Use text index for fast free-text lookups.
      query = { $text: { $search: safeSearch } };
    }
  }

  const dashboardFields = [
    "_id",
    "loanId",
    "loan_number",
    "loanType",
    "typeOfLoan",
    "caseType",
    "loan_type",
    "isFinanced",
    "isFinanceRequired",
    "customerName",
    "primaryMobile",
    "city",
    "permanentCity",
    "residenceAddress",
    "permanentAddress",
    "address",
    "source",
    "recordSource",
    "sourceName",
    "dealerName",
    "showroomName",
    "showroom",
    "showroom_name",
    "approval_bankName",
    "postfile_bankName",
    "approval_banksData",
    "approval_loanAmountDisbursed",
    "approval_loanAmountApproved",
    "approval_roi",
    "approval_tenureMonths",
    "postfile_emiAmount",
    "emiAmount",
    "financeExpectation",
    "loanTenureMonths",
    "disbursement_date",
    "approval_disbursedDate",
    "delivery_date",
    "dispatch_date",
    "postfile_firstEmiDate",
    "postfile_maturityDate",
    "postfile_currentOutstanding",
    "postfile_current_outstanding",
    "livePrincipalOutstanding",
    "principalOutstanding",
    "registrationNumber",
    "vehicleRegNo",
    "rc_redg_no",
    "postfile_regd_city",
    "registrationCity",
    "vehicleMake",
    "vehicleModel",
    "vehicleVariant",
    "status",
    "loanStatus",
    "isCashCase",
    "latestBusinessDate",
    "currentStage",
    "createdAt",
    "updatedAt",
  ];
  const dashboardSelect = dashboardFields.join(" ");
  const countKey = JSON.stringify({ viewMode, search: safeSearch });
  const cachedCount = listCountCache.get(countKey);
  const useCachedCount =
    cachedCount && Date.now() - cachedCount.ts < LIST_COUNT_CACHE_TTL_MS;
  const totalPromise = useCachedCount
    ? Promise.resolve(cachedCount.total)
    : !safeSearch && viewMode === "dashboard"
      ? Loan.estimatedDocumentCount()
      : Loan.countDocuments(query);
  let dataPromise;

  if (sortField === "latestBusiness") {
    dataPromise = Loan.find(query)
      .sort({ latestBusinessDate: direction, _id: -1 })
      .skip(safeSkip)
      .limit(safeLimit)
      .select(viewMode === "dashboard" ? dashboardSelect : "")
      .lean();
  } else {
    const allowedSort = new Set([
      "createdAt",
      "updatedAt",
      "loanId",
      "loan_number",
      "customerName",
      "approval_loanAmountDisbursed",
      "approval_loanAmountApproved",
      "postfile_emiAmount",
      "aging",
      "disbursement_date",
      "delivery_date",
      "vehicleModel",
    ]);
    const safeSortField = allowedSort.has(sortField) ? sortField : "updatedAt";
    const sort = { [safeSortField]: direction, _id: -1 };
    dataPromise = Loan.find(query)
      .sort(sort)
      .skip(safeSkip)
      .limit(safeLimit)
      .select(viewMode === "dashboard" ? dashboardSelect : "")
      .lean();
  }

  const [data, total] = await Promise.all([dataPromise, totalPromise]);
  if (!useCachedCount) {
    listCountCache.set(countKey, { total, ts: Date.now() });
  }
  const queryMs = Date.now() - startedAt;
  const responseMeta = {
    queryMs,
    rows: Array.isArray(data) ? data.length : 0,
    total,
    page: Math.floor(safeSkip / safeLimit) + 1,
    limit: safeLimit,
    skip: safeSkip,
    view: viewMode || "default",
    sortBy: sortField,
    sortDir: direction === 1 ? "asc" : "desc",
    searched: Boolean(safeSearch),
    searchLength: safeSearch.length,
  };

  if (process.env.NODE_ENV !== "production") {
    console.info("[LoansAPI:getLoans]", responseMeta);
  }

  res.json({
    data,
    total,
    page: Math.floor(safeSkip / safeLimit) + 1,
    limit: safeLimit,
    skip: safeSkip,
    hasMore: safeSkip + data.length < total,
    meta: responseMeta,
  });
});

// @desc    Dashboard aggregate stats for all loans
// @route   GET /api/loans/dashboard/stats
// @access  Public
const getLoanDashboardStats = asyncHandler(async (req, res) => {
  const cached =
    dashboardStatsCache?.data &&
    Date.now() - Number(dashboardStatsCache.ts || 0) < DASHBOARD_STATS_CACHE_TTL_MS;
  if (cached) {
    return res.json({
      ...dashboardStatsCache.data,
      meta: {
        ...(dashboardStatsCache.data?.meta || {}),
        fromCache: true,
      },
    });
  }

  const startedAt = Date.now();
  const today = new Date();
  today.setHours(0, 0, 0, 0);
  const nextDay = new Date(today);
  nextDay.setDate(nextDay.getDate() + 1);
  const agg = await Loan.aggregate([
    {
      $addFields: {
        __statusLower: { $toLower: { $ifNull: ["$status", ""] } },
        __stageLower: { $toLower: { $ifNull: ["$currentStage", ""] } },
        __bookValue: {
          $ifNull: [
            "$approval_loanAmountDisbursed",
            {
              $ifNull: [
                "$approval_loanAmountApproved",
                { $ifNull: ["$loanAmount", { $ifNull: ["$financeExpectation", 0] }] },
              ],
            },
          ],
        },
        __emiValue: {
          $ifNull: [
            { $arrayElemAt: ["$approval_banksData.emiAmount", 0] },
            {
              $ifNull: [
                { $arrayElemAt: ["$approval_banksData.emi", 0] },
                { $ifNull: ["$postfile_emiAmount", { $ifNull: ["$emiAmount", 0] }] },
              ],
            },
          ],
        },
      },
    },
    {
      $group: {
        _id: null,
        total: { $sum: 1 },
        pending: {
          $sum: {
            $cond: [
              {
                $and: [
                  { $eq: ["$__stageLower", "approval"] },
                  {
                    $or: [
                      { $regexMatch: { input: "$__statusLower", regex: "pending" } },
                      { $regexMatch: { input: "$__statusLower", regex: "progress" } },
                    ],
                  },
                ],
              },
              1,
              0,
            ],
          },
        },
        disbursed: {
          $sum: {
            $cond: [{ $regexMatch: { input: "$__statusLower", regex: "disburs" } }, 1, 0],
          },
        },
        approvedToday: {
          $sum: {
            $cond: [
              {
                $or: [
                  {
                    $and: [
                      { $gte: ["$approval_approvalDate", today] },
                      { $lt: ["$approval_approvalDate", nextDay] },
                    ],
                  },
                  {
                    $and: [
                      { $gte: ["$updatedAt", today] },
                      { $lt: ["$updatedAt", nextDay] },
                      { $regexMatch: { input: "$__statusLower", regex: "approved" } },
                    ],
                  },
                ],
              },
              1,
              0,
            ],
          },
        },
        totalBookValue: { $sum: { $ifNull: ["$__bookValue", 0] } },
        emiCapturedCount: {
          $sum: {
            $cond: [{ $gt: [{ $ifNull: ["$__emiValue", 0] }, 0] }, 1, 0],
          },
        },
        regNoCapturedCount: {
          $sum: {
            $cond: [
              {
                $or: [
                  { $ne: [{ $ifNull: ["$rc_redg_no", null] }, null] },
                  { $ne: [{ $ifNull: ["$vehicleRegNo", null] }, null] },
                  { $ne: [{ $ifNull: ["$registrationNumber", null] }, null] },
                  { $ne: [{ $ifNull: ["$vehicleNumber", null] }, null] },
                ],
              },
              1,
              0,
            ],
          },
        },
      },
    },
  ]);

  const row = agg[0] || {};

  const queryMs = Date.now() - startedAt;
  const response = {
    total: Number(row.total) || 0,
    pending: Number(row.pending) || 0,
    approvedToday: Number(row.approvedToday) || 0,
    disbursed: Number(row.disbursed) || 0,
    totalBookValue: Number(row.totalBookValue) || 0,
    emiCapturedCount: Number(row.emiCapturedCount) || 0,
    regNoCapturedCount: Number(row.regNoCapturedCount) || 0,
    meta: {
      queryMs,
      rowsScanned: Number(row.total) || 0,
      fromCache: false,
    },
  };

  if (process.env.NODE_ENV !== "production") {
    console.info("[LoansAPI:getLoanDashboardStats]", response.meta);
  }

  dashboardStatsCache = { ts: Date.now(), data: response };
  res.json(response);
});

// @desc    Get loan by ID
// @route   GET /api/loans/:id
// @access  Public
const getLoanById = asyncHandler(async (req, res) => {
  let loan;
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    loan = await Loan.findById(req.params.id).populate("customerId");
  } else {
    loan = await Loan.findOne({ loanId: req.params.id }).populate("customerId");
  }

  if (loan) {
    // Verify customerId reference integrity
    if (!loan.customerId) {
      console.warn(
        "⚠️ Orphaned loan detected:",
        loan.loanId,
        "- No customer linked",
      );
    } else if (
      typeof loan.customerId === "string" ||
      loan.customerId instanceof mongoose.Types.ObjectId
    ) {
      console.error(
        "❌ Broken reference in loan:",
        loan.loanId,
        "- Customer may not exist",
      );

      // Try to fetch customer directly
      const customerExists = await Customer.findById(loan.customerId);
      if (!customerExists) {
        console.error("❌ CRITICAL: Customer not found for loan:", loan.loanId);
      } else {
        loan = await Loan.findById(loan._id).populate("customerId");
      }
    }

    // Merge customer data into the loan object for frontend compatibility
    const loanObj = loan.toObject();
    if (loanObj.customerId && typeof loanObj.customerId === "object") {
      const customerData = loanObj.customerId;
      const customerId = customerData._id;

      // Merge customer fields into loan ONLY if loan field is empty/null
      // Priority: Loan fields take precedence (don't overwrite existing loan data)
      // This ensures the frontend form sees a complete flat object
      Object.keys(customerData).forEach((key) => {
        // Skip internal MongoDB fields and customerId
        if (
          key === "_id" ||
          key === "__v" ||
          key === "createdAt" ||
          key === "updatedAt" ||
          key === "customerId"
        ) {
          return;
        }
        // Only fill if loan doesn't have this field or it's empty
        // Use strict checks to avoid overwriting falsy values like 0 or false
        if (
          loanObj[key] === undefined ||
          loanObj[key] === null ||
          loanObj[key] === ""
        ) {
          loanObj[key] = customerData[key];
        }
      });

      // Set customerId as scalar string for frontend
      loanObj.customerId = customerId.toString();
    }

    // Fallback: If approval_banksData is missing or empty, create one from top-level fields
    if (
      !loanObj.approval_banksData ||
      loanObj.approval_banksData.length === 0
    ) {
      loanObj.approval_banksData = [
        {
          bankName: loanObj.approval_bankName,
          loanAmount: loanObj.approval_loanAmountApproved,
          interestRate: loanObj.approval_roi,
          tenure: loanObj.approval_tenureMonths,
          status: loanObj.approval_status,
          processingFees: loanObj.approval_processingFees,
          approvalDate: loanObj.approval_approvalDate,
          remarks: loanObj.approval_remarks,
          // Add more fields as needed
        },
      ];
    }

    // Fallback: If caseType is missing, infer from typeOfLoan or loanType
    if (!loanObj.caseType) {
      loanObj.caseType = loanObj.typeOfLoan || loanObj.loanType || null;
    }
    res.json({ success: true, data: loanObj });
  } else {
    res.status(404);
    throw new Error("Loan not found");
  }
});

// @desc    Create a loan
// @route   POST /api/loans
// @access  Public
const createLoan = asyncHandler(async (req, res) => {
  const { numberOfCars, ...loanData } = req.body;
  const normalizedLoanData = normalizeCustomerFields(loanData);
  const instrumentSanitizedLoanData = sanitizeInstrumentForPersistence(normalizedLoanData);

  // ---------------------------------------------------------
  // 1️⃣ VALIDATE / LINK CUSTOMER (AUTO-CREATE IF NEEDED)
  // ---------------------------------------------------------
  let linkedCustomerId = null;
  let linkedCustomer = null;
  const requestedCustomerId = normalizedLoanData.customerId;

  // AUTO-CREATE: Try to find or create customer from loan form data
  if (requestedCustomerId) {
    // Validate provided customerId
    const customer = await resolveCustomerById(requestedCustomerId);
    if (!customer) {
      res.status(400);
      throw new Error("Invalid customerId: " + requestedCustomerId);
    }
    linkedCustomerId = customer._id;
    linkedCustomer = customer;
  } else {
    // Try to find customer by primaryMobile
    const { primaryMobile, customerName } = normalizedLoanData;
    let existingCustomer = null;

    if (primaryMobile) {
      existingCustomer = await Customer.findOne({ primaryMobile });
    }

    if (existingCustomer) {
      linkedCustomerId = existingCustomer._id;
      linkedCustomer = existingCustomer;
    } else {
      // AUTO-CREATE: Create new customer from loan form data

      // Validate required fields for customer creation
      if (!customerName || !primaryMobile) {
        res.status(400);
        throw new Error(
          "❌ Customer name and mobile number are required to create a loan.\n" +
            "Please provide customerName and primaryMobile in the form.",
        );
      }

      const nextCustomerId = await getNextId(Customer, "ACILLP", "customerId");

      const customerPayload = {
        customerId: nextCustomerId,
        ...normalizedLoanData, // All loan form fields become customer fields
        createdFrom: "LOAN_FORM",
        createdBy: normalizedLoanData.createdBy || "System",
      };

      try {
        linkedCustomer = await Customer.create(customerPayload);
        linkedCustomerId = linkedCustomer._id;
        console.log(`✅ Customer created: ${linkedCustomer.customerId}`);
      } catch (err) {
        console.error(`❌ Customer creation failed: ${err.message}`);
        // Fallback: Use temporary linking without customer creation
      }
    }
  }

  // Prepare loan payload with ALL fields (let Mongoose schema handle it)
  const loanPayload = {
    ...instrumentSanitizedLoanData,
    customerId: linkedCustomerId,
  };

  // Ensure customer display fields are present for dashboards
  if (!loanPayload.customerName && linkedCustomer) {
    loanPayload.customerName = linkedCustomer.customerName;
  }
  if (!loanPayload.primaryMobile && linkedCustomer) {
    loanPayload.primaryMobile = linkedCustomer.primaryMobile;
  }

  // ---------------------------------------------------------
  // 2️⃣ VALIDATE INDIRECT SOURCE PAYOUT REQUIREMENTS
  // ---------------------------------------------------------
  if (
    normalizedLoanData.recordSource === "Indirect" ||
    normalizedLoanData.source === "Indirect"
  ) {
    const hasPayoutDetails =
      normalizedLoanData.payoutApplicable === "Yes" ||
      normalizedLoanData.prefile_sourcePayoutPercentage;

    if (!hasPayoutDetails) {
      console.warn(
        "⚠️ Indirect source without payout details - filling defaults",
      );
      // Allow proceeding but mark as requiring payout later
      // This is a soft validation - user can add payout before approval
    }

    // Validate required indirect source fields
    const requiredIndirectFields = {
      sourceName: normalizedLoanData.sourceName,
      dealerMobile: normalizedLoanData.dealerMobile,
      dealerAddress: normalizedLoanData.dealerAddress,
    };

    const missingFields = Object.entries(requiredIndirectFields)
      .filter(([_, value]) => !value)
      .map(([key]) => key);

    if (missingFields.length > 0) {
      console.warn(
        `⚠️ Indirect source missing fields: ${missingFields.join(", ")}. Form will show required fields.`,
      );
      // Don't throw - just warn, as form validation should handle this
    }
  }

  // ---------------------------------------------------------
  // 2️⃣ BULK CREATION
  // ---------------------------------------------------------
  if (numberOfCars && Number(numberOfCars) > 1) {
    const count = Number(numberOfCars);
    const createdLoans = [];

    // Get base ID
    let currentLoanIdStr = await getNextId(Loan, "LN", "loanId");
    // Parse the number back out to increment locally loop
    let currentBase = parseInt(currentLoanIdStr.split("-")[2], 10);

    for (let i = 0; i < count; i++) {
      const nextNum = currentBase + i;
      const uniqueLoanId = `LN-${new Date().getFullYear()}-${String(nextNum).padStart(4, "0")}`;

      try {
        const loan = await Loan.create({
          ...loanPayload,
          loanId: uniqueLoanId,
          isBulk: true,
          bulkCount: count,
        });
        await ensureLinkedRecords(loan);
        await syncVehicleMasterRecord(loan);
        createdLoans.push(loan);
      } catch (err) {
        // Collision fallback: try one with offset
        const fallbackNum = currentBase + count + i + 10;
        const fallbackId = `LN-${new Date().getFullYear()}-${String(fallbackNum).padStart(4, "0")}`;
        try {
          const loan = await Loan.create({
            ...loanPayload,
            loanId: fallbackId,
            isBulk: true,
            bulkCount: count,
          });
          await ensureLinkedRecords(loan);
          await syncVehicleMasterRecord(loan);
          createdLoans.push(loan);
        } catch (e) {
          console.error("Failed to create bulk loan item", e);
        }
      }
    }

    clearLoanCaches();
    res.status(201).json({
      success: true,
      count: createdLoans.length,
      data: createdLoans,
      message: `Successfully created ${createdLoans.length} loan applications.`,
    });
    return;
  }

  // ---------------------------------------------------------
  // 3️⃣ SINGLE CREATION
  // ---------------------------------------------------------
  let { loanId } = loanPayload;

  if (!loanId) {
    loanId = await getNextId(Loan, "LN", "loanId");
  }

  let loan;
  try {
    // ==========================================
    // 🔍 PRE-SAVE VALIDATION & LOGGING
    // ==========================================
    const finalPayload = {
      ...loanPayload,
      loanId,
    };

    // ✅ SAVE ALL FIELDS - No filtering, just use the entire payload
    loan = await Loan.create(finalPayload);

    // Auto-sync bank details to global Bank collection for future auto-fill
    await syncBankCollection(finalPayload);
  } catch (error) {
    if (error.code === 11000) {
      // Duplicate key error - Loan ID collision
      const newId = await getNextId(Loan, "LN", "loanId");
      const parts = newId.split("-");
      const incId = `LN-${parts[1]}-${String(Number(parts[2]) + 1).padStart(4, "0")}`;

      loan = await Loan.create({
        ...loanPayload,
        loanId: incId,
      });
    } else if (error.name === "ValidationError") {
      console.error(
        "❌ Validation Error:",
        Object.keys(error.errors).join(", "),
      );
      throw error;
    } else {
      throw error;
    }
  }

  if (loan) {
    await ensureLinkedRecords(loan);
    await syncVehicleMasterRecord(loan);
    clearLoanCaches();

    // Return comprehensive response confirming all details were saved
    res.status(201).json({
      success: true,
      loanId: loan.loanId,
      data: loan,
      customerLinked: linkedCustomer
        ? {
            customerId: linkedCustomer.customerId,
            customerName: linkedCustomer.customerName,
            primaryMobile: linkedCustomer.primaryMobile,
            createdNew: linkedCustomer.createdFrom === "LOAN_FORM",
          }
        : null,
      message:
        linkedCustomer?.createdFrom === "LOAN_FORM"
          ? `✅ Loan created with auto-generated customer! All ${Object.keys(loan.toObject()).length} details saved in both.`
          : "✅ Loan created with all details saved",
      savedDetails: {
        loanId: loan.loanId,
        customerId: linkedCustomerId,
        customerName: loan.customerName,
        primaryMobile: loan.primaryMobile,
        vehicleModel: loan.vehicleModel,
        loanAmount: loan.loanAmount,
        hasCoApplicant: loan.hasCoApplicant,
        hasGuarantor: loan.hasGuarantor,
        status: loan.status,
        currentStage: loan.currentStage,
        totalFieldsSaved: Object.keys(loan.toObject()).length,
      },
      // dbVerification removed: was not defined, causing ReferenceError
    });
  } else {
    res.status(400);
    throw new Error("Invalid loan data");
  }
});

// @desc    Update a loan
// @route   PUT /api/loans/:id
// @access  Public
const updateLoan = asyncHandler(async (req, res) => {
  let loan;
  let step = "resolve-loan";
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    loan = await Loan.findById(req.params.id);
  } else {
    loan = await Loan.findOne({ loanId: req.params.id });
  }

  if (loan) {
    try {
      step = "normalize-body";
      // 1. Update Loan (store full payload, including customer fields)
      const normalizedBody = normalizeCustomerFields(req.body || {});
      const instrumentSanitizedBody = sanitizeInstrumentForPersistence(normalizedBody);

      step = "clean-body";
      // Remove immutable/system fields
      const cleanedBody = { ...instrumentSanitizedBody };
      delete cleanedBody._id;
      delete cleanedBody.__v;
      delete cleanedBody.createdAt;
      delete cleanedBody.updatedAt;
      delete cleanedBody.loanId;

      step = "validate-customer";
      // Validate customer reference if provided or missing
      if (normalizedBody?.customerId) {
        const customer = await resolveCustomerById(normalizedBody.customerId);
        if (!customer) {
          res.status(400);
          throw new Error(
            "Invalid customerId. Please select or create a valid customer first.",
          );
        }
        loan.customerId = customer._id;
        delete cleanedBody.customerId;
      } else if (!loan.customerId) {
        res.status(400);
        throw new Error(
          "Customer is required. Please link a customer before updating this loan.",
        );
      }

      step = "assign-fields";
      // ASSIGN ALL FIELDS - ensure nothing is missed
      Object.assign(loan, cleanedBody);
      applyUndefinedOnDoc(loan, cleanedBody);

      step = "save-loan-primary";
      // Save with retry for version conflicts.
      // Fallback to direct atomic update if mongoose save middleware throws runtime issues
      // (e.g. "next is not a function" from legacy hook paths on deployed envs).
      let updatedLoan;
      try {
        updatedLoan = await saveWithRetry(loan);
      } catch (saveErr) {
        step = "save-loan-fallback";
        console.error(
          "Loan saveWithRetry failed, using fallback update path:",
          saveErr?.message || saveErr,
        );
        const filter = loan?._id
          ? { _id: loan._id }
          : { loanId: req.params.id };
        const { setPayload, unsetPayload } = splitUndefinedForMongo(cleanedBody);

        updatedLoan = await Loan.findOneAndUpdate(
          filter,
          { $set: setPayload, ...(Object.keys(unsetPayload).length ? { $unset: unsetPayload } : {}) },
          { new: true, runValidators: false },
        );

        if (!updatedLoan) {
          throw saveErr;
        }
      }

      step = "sync-bank-collection";
      // Auto-sync bank details to global Bank collection for future auto-fill
      await syncBankCollection(normalizedBody);

      step = "sync-customer";
      // 2. Sync with Customer (Bidirectional)
      if (loan.customerId) {
        try {
          const customer = await Customer.findById(loan.customerId);
          if (customer) {
            let hasCustomerUpdate = false;
            CUSTOMER_SYNC_FIELDS.forEach((field) => {
              if (normalizedBody[field] !== undefined) {
                customer[field] = normalizedBody[field];
                hasCustomerUpdate = true;
              }
            });

            if (hasCustomerUpdate) {
              await customer.save();
            }
          }
        } catch (err) {
          console.error("Error syncing customer profile:", err);
        }
      }

      step = "ensure-linked-records";
      // 3. Ensure linked records (DO and Payment)
      await ensureLinkedRecords(updatedLoan);

      step = "sync-vehicle-record";
      // 4. Upsert vehicle master record
      await syncVehicleMasterRecord(updatedLoan);

      step = "respond-success";
      res.json({
        success: true,
        data: updatedLoan,
        message: "✅ Loan updated with all details saved",
        updatedFields: Object.keys(cleanedBody).length,
      });
    } catch (err) {
      if (res.statusCode < 400) res.status(500);
      throw new Error(`[updateLoan:${step}] ${err.message}`);
    }
  } else {
    res.status(404);
    throw new Error("Loan not found");
  }
});

// @desc    Delete a loan
// @route   DELETE /api/loans/:id
// @access  Public
const deleteLoan = asyncHandler(async (req, res) => {
  let loan;
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    loan = await Loan.findById(req.params.id);
  } else {
    loan = await Loan.findOne({ loanId: req.params.id });
  }

  if (loan) {
    await loan.deleteOne();
    clearLoanCaches();
    res.json({ success: true, message: "Loan removed" });
  } else {
    res.status(404);
    throw new Error("Loan not found");
  }
});

// @desc    Disburse a loan and generate payouts
// @route   POST /api/loans/:id/disburse
// @access  Public
// @purpose Separate endpoint for disbursement with payout calculation
// KEY PRINCIPLE: Payout percentage and receivable creation happen HERE, not at approval
const disburseLoan = asyncHandler(async (req, res) => {
  // Fetch loan
  let loan;
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    loan = await Loan.findById(req.params.id);
  } else {
    loan = await Loan.findOne({ loanId: req.params.id });
  }

  if (!loan) {
    res.status(404);
    throw new Error("Loan not found");
  }

  // Extract disbursement data from request
  const {
    disburseAmount,
    disbursedBankName,
    payoutPercentage,
    disbursedDate,
    remarks,
  } = req.body;

  // =====================================
  // 1. VALIDATE REQUEST DATA
  // =====================================
  const validation = validateDisbursementData({
    disburseAmount,
    disbursedBankName,
    payoutPercentage,
  });

  if (!validation.isValid) {
    res.status(400);
    throw new Error(
      `Disbursement validation failed: ${validation.errors.join(", ")}`,
    );
  }

  // =====================================
  // 2. VERIFY LOAN IS APPROVED
  // =====================================
  if (loan.approval_status !== "Approved") {
    res.status(400);
    throw new Error(
      `Loan must be "Approved" before disbursement. Current status: ${loan.approval_status}`,
    );
  }

  // =====================================
  // 3. CALCULATE PAYOUTS
  // =====================================
  let payoutData;
  try {
    payoutData = await calculatePayoutsOnDisbursement(loan, {
      disburseAmount,
      disbursedBankName,
      payoutPercentage,
      disbursedDate,
      remarks,
    });

    console.log(
      `✅ Disbursement processed for ${loan.loanId}: ${payoutData.receivables.length} receivables created`,
    );
  } catch (err) {
    console.error(`❌ Disbursement failed for ${loan.loanId}: ${err.message}`);
    res.status(400);
    throw new Error(`Payout calculation failed: ${err.message}`);
  }

  // =====================================
  // 4. UPDATE LOAN WITH DISBURSEMENT DATA
  // =====================================
  loan.disburse_status = "Disbursed";
  loan.disburse_bankName = disbursedBankName;
  loan.disburse_amount = parseFloat(disburseAmount);
  loan.disburse_date = disbursedDate ? new Date(disbursedDate) : new Date();
  loan.disburse_remarks = remarks || "";

  // Store payout data in loan
  loan.payout_percentage = parseFloat(payoutPercentage);
  loan.payout_amount = parseFloat(
    payoutData.summary.totalReceivable.toFixed(2),
  );
  loan.payout_calculatedAt = new Date();
  loan.payout_applicableFor =
    payoutData.receivables.length > 0 ? "Bank" : "None";

  // Also set legacy fields for backward compatibility
  loan.approval_loanAmountDisbursed = parseFloat(disburseAmount);
  loan.approval_disbursedDate = loan.disburse_date;

  // ✅ Calculate and store EMI for post-file management
  const calculateEMI = (principal, annualRate, tenureMonths) => {
    const P = Number(principal) || 0;
    const N = Number(tenureMonths) || 0;
    const R = (Number(annualRate) || 0) / 12 / 100;
    if (!P || !N || !R) return 0;
    const pow = Math.pow(1 + R, N);
    return Math.round((P * R * pow) / (pow - 1));
  };

  const roi = loan.approval_roi || loan.postfile_roi || 0;
  const tenure = loan.approval_tenureMonths || loan.postfile_tenureMonths || 0;
  const emiAmount = calculateEMI(parseFloat(disburseAmount), roi, tenure);

  // Auto-populate post-file fields for seamless workflow
  loan.postfile_bankName = disbursedBankName;
  loan.postfile_loanAmountApproved = parseFloat(disburseAmount);
  loan.postfile_loanAmountDisbursed = parseFloat(disburseAmount);
  loan.postfile_roi = roi;
  loan.postfile_tenureMonths = tenure;
  loan.postfile_emiAmount = emiAmount;

  // Auto-populate post-file disbursal breakdown from approval breakup
  loan.postfile_disbursedLoan = loan.approval_breakup_netLoanApproved || 0;
  loan.postfile_disbursedCreditAssured =
    loan.approval_breakup_creditAssured || 0;
  loan.postfile_disbursedInsurance =
    loan.approval_breakup_insuranceFinance || 0;
  loan.postfile_disbursedEw = loan.approval_breakup_ewFinance || 0;

  // Store receivables and payables in loan
  loan.loan_receivables = payoutData.receivables;
  loan.loan_payables = payoutData.payables;

  // Save updated loan with retry for version conflicts
  await saveWithRetry(loan);
  clearLoanCaches();

  // =====================================
  // 5. CREATE/UPDATE PAYMENT RECORD
  // =====================================
  let payment;
  try {
    payment = await Payment.findOneAndUpdate(
      { loanId: loan.loanId },
      {
        loanId: loan.loanId,
        payoutRecords: {
          receivables: payoutData.receivables,
          payables: payoutData.payables,
        },
        disbursementDetails: {
          status: "Disbursed",
          bankName: disbursedBankName,
          amount: parseFloat(disburseAmount),
          date: loan.disburse_date,
          payoutCalculatedAt: new Date(),
        },
        createdBy: req.user?.id || null,
      },
      { upsert: true, new: true },
    );
  } catch (err) {
    // Payment record is supplementary, disbursement still successful
  }

  // =====================================
  // 6. RESPONSE
  // =====================================
  res.json({
    success: true,
    message: "✅ Loan disbursed successfully with payouts calculated",
    loan: {
      loanId: loan.loanId,
      disburse_status: loan.disburse_status,
      disburse_bankName: loan.disburse_bankName,
      disburse_amount: loan.disburse_amount,
      disburse_date: loan.disburse_date,
      disburse_remarks: loan.disburse_remarks,
      payout_percentage: loan.payout_percentage,
      payout_amount: loan.payout_amount,
      loan_receivables: loan.loan_receivables || [],
      loan_payables: loan.loan_payables || [],
      // ✅ Include post-file EMI data
      postfile_emiAmount: loan.postfile_emiAmount,
      postfile_roi: loan.postfile_roi,
      postfile_tenureMonths: loan.postfile_tenureMonths,
      postfile_loanAmountDisbursed: loan.postfile_loanAmountDisbursed,
    },
    payouts: {
      receivables: payoutData.receivables,
      payables: payoutData.payables,
      summary: payoutData.summary,
    },
  });
});

// Get banks data for a specific loan
const getBanksData = asyncHandler(async (req, res) => {
  const { id } = req.params;

  const loan = await Loan.findById(id);
  if (!loan) {
    res.status(404);
    throw new Error("Loan not found");
  }

  res.json({
    success: true,
    banks: loan.approval_banksData || [],
  });
});

// Save/update banks data for a specific loan
const saveBanksData = asyncHandler(async (req, res) => {
  const { id } = req.params;
  const { banks } = req.body;

  if (!Array.isArray(banks)) {
    res.status(400);
    throw new Error("Banks data must be an array");
  }

  const loan = await Loan.findById(id);
  if (!loan) {
    res.status(404);
    throw new Error("Loan not found");
  }

  loan.approval_banksData = banks;

  // Save with retry for version conflicts
  await saveWithRetry(loan);
  clearLoanCaches();

  res.json({
    success: true,
    message: "Banks data saved successfully",
    banks: loan.approval_banksData,
  });
});

// Get all banks
const getAllBanks = asyncHandler(async (req, res) => {
  const docs = await BankDirectory.find({})
    .sort({ bankName: 1, ifsc: 1 })
    .select("bankName ifsc branch address micr active updatedAt createdAt")
    .lean();

  // Keep API shape backward-compatible with previous `Bank` model.
  const banks = docs.map((row) => ({
    _id: row._id,
    name: row.bankName || inferBankNameFromIfsc(row.ifsc) || inferBankNameFromMicr(row.micr) || "",
    ifsc: row.ifsc || "",
    branch: row.branch || row.address || "",
    address: row.address || row.branch || "",
    micr: row.micr || "",
    active: row.active !== false,
    updatedAt: row.updatedAt,
    createdAt: row.createdAt,
  }));

  res.json(banks);
});

const resolveBankLookup = asyncHandler(async (req, res) => {
  const ifsc = normalizeIfsc(req.query.ifsc);
  const micr = normalizeMicr(req.query.micr);

  if (!ifsc && !micr) {
    res.status(400);
    throw new Error("Provide ifsc or micr query parameter");
  }

  // 1) IFSC path (cache-first + remote refresh/fetch)
  if (ifsc) {
    const cached = await BankDirectory.findOne({ ifsc });
    if (cached) {
      const fallbackBranch = cached.branch || cached.address || "";
      const fallbackAddress = cached.address || cached.branch || "";
      return res.json({
        success: true,
        data: {
          ifsc: cached.ifsc || ifsc,
          micr: cached.micr || "",
          bankName: cached.bankName || inferBankNameFromIfsc(ifsc),
          branch: fallbackBranch,
          address: fallbackAddress,
          city: cached.city || "",
          state: cached.state || "",
          district: cached.district || "",
          contact: cached.contact || "",
          active: cached.active !== false,
          source: "cache",
          lastVerifiedAt: cached.lastVerifiedAt || cached.updatedAt || null,
        },
      });
    }

    const fetched = await fetchIfscFromRazorpay(ifsc);
    if (fetched && !fetched.error && !fetched.notFound) {
      const saved = await upsertBankDirectoryEntry({
        ...fetched,
        source: "razorpay-ifsc",
        active: true,
      });
      return res.json({
        success: true,
        data: {
          ifsc: saved?.ifsc || fetched.ifsc,
          micr: saved?.micr || fetched.micr || "",
          bankName: saved?.bankName || fetched.bankName || inferBankNameFromIfsc(ifsc),
          branch: saved?.branch || fetched.branch || saved?.address || fetched.address || bankMaster?.address || "",
          address: saved?.address || fetched.address || saved?.branch || fetched.branch || bankMaster?.address || "",
          city: saved?.city || fetched.city || "",
          state: saved?.state || fetched.state || "",
          district: saved?.district || fetched.district || "",
          contact: saved?.contact || fetched.contact || "",
          active: true,
          source: "razorpay-ifsc",
          lastVerifiedAt: saved?.lastVerifiedAt || new Date(),
        },
      });
    }

    const inferred = inferBankNameFromIfsc(ifsc);
    if (inferred) {
      // Soft cache even if remote is unavailable.
      await upsertBankDirectoryEntry({
        ifsc,
        bankName: inferred,
        source: fetched?.notFound ? "ifsc-code-fallback-notfound" : "ifsc-code-fallback",
        active: fetched?.notFound ? false : true,
        raw: fetched || null,
      });
      return res.json({
        success: true,
        data: {
          ifsc,
          micr: "",
          bankName: inferred,
          branch: "",
          address: "",
          city: "",
          state: "",
          district: "",
          contact: "",
          active: fetched?.notFound ? false : true,
          source: fetched?.notFound ? "ifsc-code-fallback-notfound" : "ifsc-code-fallback",
          lastVerifiedAt: new Date(),
        },
      });
    }

    res.status(404);
    throw new Error("IFSC not found");
  }

  // 2) MICR path (cache-first + bank-code fallback)
  const cachedByMicr = await BankDirectory.findOne({ micr }).sort({ updatedAt: -1 });
  if (cachedByMicr) {
    const fallbackBranch = cachedByMicr.branch || cachedByMicr.address || "";
    const fallbackAddress = cachedByMicr.address || cachedByMicr.branch || "";
    return res.json({
      success: true,
      data: {
        ifsc: cachedByMicr.ifsc || "",
        micr: cachedByMicr.micr || micr,
        bankName: cachedByMicr.bankName || inferBankNameFromMicr(micr),
        branch: fallbackBranch,
        address: fallbackAddress,
        city: cachedByMicr.city || "",
        state: cachedByMicr.state || "",
        district: cachedByMicr.district || "",
        contact: cachedByMicr.contact || "",
        active: cachedByMicr.active !== false,
        source: "cache-micr",
        lastVerifiedAt: cachedByMicr.lastVerifiedAt || cachedByMicr.updatedAt || null,
      },
    });
  }

  const inferredMicrBank = inferBankNameFromMicr(micr);
  if (inferredMicrBank) {
    await upsertBankDirectoryEntry({
      micr,
      bankName: inferredMicrBank,
      source: "micr-code-fallback",
      active: true,
    });
    return res.json({
      success: true,
      data: {
        ifsc: "",
        micr,
        bankName: inferredMicrBank,
        branch: "",
        address: "",
        city: "",
        state: "",
        district: "",
        contact: "",
        active: true,
        source: "micr-code-fallback",
        lastVerifiedAt: new Date(),
      },
    });
  }

  res.status(404);
  throw new Error("MICR not found");
});

const createBank = asyncHandler(async (req, res) => {
  const { name, ifsc, address, branch, micr } = req.body;

  const normalizedIfsc = normalizeIfsc(ifsc);
  const normalizedMicr = normalizeMicr(micr);
  const existing = normalizedIfsc
    ? await BankDirectory.findOne({ ifsc: normalizedIfsc })
    : normalizedMicr
      ? await BankDirectory.findOne({ micr: normalizedMicr, bankName: String(name || "").trim() })
      : null;
  if (existing) {
    res.status(400);
    throw new Error("Bank directory entry already exists");
  }

  const bank = await upsertBankDirectoryEntry({
    ifsc: normalizedIfsc,
    micr: normalizedMicr,
    bankName: name,
    branch: branch || address || "",
    address: address || branch || "",
    source: "manual-bank-create",
    active: true,
  });

  if (bank) {
    res.status(201).json({
      _id: bank._id,
      name: bank.bankName || "",
      ifsc: bank.ifsc || "",
      branch: bank.branch || bank.address || "",
      address: bank.address || bank.branch || "",
      micr: bank.micr || "",
      active: bank.active !== false,
      source: bank.source || "manual-bank-create",
    });
  } else {
    res.status(400);
    throw new Error("Invalid bank data");
  }
});

export {
  getLoans,
  getLoanDashboardStats,
  getLoanById,
  createLoan,
  updateLoan,
  deleteLoan,
  disburseLoan,
  getBanksData,
  saveBanksData,
  getAllBanks,
  resolveBankLookup,
  createBank,
};
