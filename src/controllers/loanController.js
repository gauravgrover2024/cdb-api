import asyncHandler from "express-async-handler";
import mongoose from "mongoose";
import Bank from "../models/Bank.js";
import Loan from "../models/Loan.js";
import Customer from "../models/Customer.js";
import DeliveryOrder from "../models/DeliveryOrder.js";
import Payment from "../models/Payment.js";
import {
  calculatePayoutsOnDisbursement,
  validateDisbursementData,
} from "../services/payoutService.js";

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
];

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
    // Delivery & Invoice
    "do_date",
    "delivery_date",
    "invoice_date",
    "invoice_received_date",
    // RC
    "rc_received_date",
  ];

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
    "co_totalExp",
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
  ];

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
  if (normalized.ifsc && !normalized.ifscCode)
    normalized.ifscCode = normalized.ifsc;
  if (normalized.ifscCode && !normalized.ifsc)
    normalized.ifsc = normalized.ifscCode;
  if (normalized.typeOfLoan && !normalized.loanType)
    normalized.loanType = normalized.typeOfLoan;
  if (normalized.loanType && !normalized.typeOfLoan)
    normalized.typeOfLoan = normalized.loanType;
  if (normalized.fatherName && !normalized.sdwOf)
    normalized.sdwOf = normalized.fatherName;

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
  return normalized;
};

// Resolve customer by ObjectId or custom customerId
// Helper to sync bank details with Bank collection
const syncBankCollection = async (payload) => {
  const banksToSync = [
    {
      name: payload.bankName,
      ifsc: payload.ifscCode || payload.ifsc,
      address: payload.branch,
    },
    {
      name: payload.co_bankName,
      ifsc: payload.co_ifscCode,
      address: payload.co_branch,
    },
    {
      name: payload.gu_bankName,
      ifsc: payload.gu_ifscCode,
      address: payload.gu_branch,
    },
  ];

  for (const bank of banksToSync) {
    if (bank.name && bank.ifsc) {
      try {
        const normalizedIfsc = bank.ifsc.toUpperCase();
        const existingBank = await Bank.findOne({ ifsc: normalizedIfsc });
        if (!existingBank) {
          await Bank.create({
            name: bank.name,
            ifsc: normalizedIfsc,
            address: bank.address || "",
          });
          console.log(
            `✅ New bank added to database: ${bank.name} (${normalizedIfsc})`,
          );
        } else {
          // Update address if it's provided and different
          let updated = false;
          if (bank.address && existingBank.address !== bank.address) {
            existingBank.address = bank.address;
            updated = true;
          }
          if (existingBank.name !== bank.name) {
            existingBank.name = bank.name;
            updated = true;
          }
          if (updated) {
            await existingBank.save();
          }
        }
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

  const session = await mongoose.startSession();

  try {
    await session.withTransaction(async () => {
      // Create/Update DeliveryOrder with loan snapshot data
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

      const deliveryOrder = await DeliveryOrder.findOneAndUpdate(
        { loanId: loanDoc.loanId },
        { $setOnInsert: doPayload },
        { upsert: true, new: true, session },
      );

      if (!deliveryOrder) {
        throw new Error("DeliveryOrder creation failed");
      }

      // Create Payment only if DO exists
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

      const payment = await Payment.findOneAndUpdate(
        { loanId: loanDoc.loanId },
        { $setOnInsert: paymentPayload },
        { upsert: true, new: true, session },
      );

      if (!payment) {
        throw new Error("Payment creation failed");
      }
    });
  } catch (err) {
    // Linked records are supplementary, don't block loan creation
  } finally {
    session.endSession();
  }
};

// @desc    Get loans with search + pagination
// @route   GET /api/loans
// @access  Public
const getLoans = asyncHandler(async (req, res) => {
  console.log("getLoans req.query:", req.query);

  const { search = "", skip = 0, limit = 50 } = req.query;

  const safeLimit = Math.min(Number(limit) || 50, 200); // cap at 200
  const safeSkip = Number(skip) || 0;

  let query = {};

  if (search && String(search).trim()) {
    const s = String(search).trim();
    const regex = new RegExp(s, "i");

    query = {
      $or: [
        { loanId: regex },
        { customerName: regex },
        { recordSource: regex },
        { sourceName: regex },
        { vehicleMake: regex },
        { vehicleModel: regex },
        { vehicleVariant: regex },
      ],
    };
  }

  const [loans, total] = await Promise.all([
    Loan.find(query)
      .populate("customerId") // keep your existing populate
      .sort({ createdAt: -1 })
      .skip(safeSkip)
      .limit(safeLimit),
    Loan.countDocuments(query),
  ]);

  console.log("getLoans result:", { count: data.length, total });

  res.json({
    success: true,
    total, // total matching loans (for pagination)
    count: loans.length, // count in this page
    data: loans,
  });
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
    ...normalizedLoanData,
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
          createdLoans.push(loan);
        } catch (e) {
          console.error("Failed to create bulk loan item", e);
        }
      }
    }

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
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
    loan = await Loan.findById(req.params.id);
  } else {
    loan = await Loan.findOne({ loanId: req.params.id });
  }

  if (loan) {
    // 1. Update Loan (store full payload, including customer fields)
    const normalizedBody = normalizeCustomerFields(req.body || {});

    // Remove immutable/system fields
    const cleanedBody = { ...normalizedBody };
    delete cleanedBody._id;
    delete cleanedBody.__v;
    delete cleanedBody.createdAt;
    delete cleanedBody.updatedAt;
    delete cleanedBody.loanId;

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

    // ASSIGN ALL FIELDS - ensure nothing is missed
    Object.assign(loan, cleanedBody);

    // Save with retry for version conflicts
    const updatedLoan = await saveWithRetry(loan);

    // Auto-sync bank details to global Bank collection for future auto-fill
    await syncBankCollection(normalizedBody);

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

    // 3. Ensure linked records (DO and Payment)
    await ensureLinkedRecords(updatedLoan);

    res.json({
      success: true,
      data: updatedLoan,
      message: "✅ Loan updated with all details saved",
      updatedFields: Object.keys(cleanedBody).length,
    });
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

  res.json({
    success: true,
    message: "Banks data saved successfully",
    banks: loan.approval_banksData,
  });
});

// Get all banks
const getAllBanks = asyncHandler(async (req, res) => {
  const banks = await Bank.find({}).sort({ name: 1 });
  res.json(banks);
});

const createBank = asyncHandler(async (req, res) => {
  const { name, ifsc, address } = req.body;

  const bankExists = await Bank.findOne({ ifsc });
  if (bankExists) {
    res.status(400);
    throw new Error("Bank with this IFSC already exists");
  }

  const bank = await Bank.create({
    name,
    ifsc,
    address,
  });

  if (bank) {
    res.status(201).json(bank);
  } else {
    res.status(400);
    throw new Error("Invalid bank data");
  }
});

export {
  getLoans,
  getLoanById,
  createLoan,
  updateLoan,
  deleteLoan,
  disburseLoan,
  getBanksData,
  saveBanksData,
  getAllBanks,
  createBank,
};
