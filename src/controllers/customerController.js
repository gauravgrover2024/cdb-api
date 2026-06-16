import asyncHandler from 'express-async-handler';
import Customer from '../models/Customer.js';
import Loan from '../models/Loan.js';
import InsuranceCase from '../models/InsuranceCase.js';
import {
  buildSearchTokenFilter,
  escapeSearchRegex,
} from "../utils/searchTokens.js";

const buildInsuranceCustomerSnapshot = (customer) => {
  if (!customer) return {};
  return {
    customerName: String(customer.customerName || '').trim(),
    companyName: String(customer.companyName || '').trim(),
    contactPersonName: String(customer.contactPersonName || '').trim(),
    primaryMobile: String(customer.primaryMobile || '').trim(),
    email: String(customer.email || customer.emailAddress || '').trim(),
    panNumber: String(customer.panNumber || '').trim(),
    residenceAddress: String(customer.residenceAddress || '').trim(),
    pincode: String(customer.pincode || '').trim(),
    city: String(customer.city || '').trim(),
  };
};

const normalizeIfsc = (value) =>
  String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "")
    .slice(0, 11);

const hasBankEntryValue = (entry = {}) =>
  Boolean(
    String(entry?.bankName || "").trim() ||
      String(entry?.accountNumber || "").trim() ||
      normalizeIfsc(entry?.ifscCode || entry?.ifsc) ||
      String(entry?.branch || "").trim() ||
      String(entry?.accountType || "").trim(),
  );

const normalizeBankEntry = (entry = {}) => {
  const ifscCode = normalizeIfsc(entry?.ifscCode || entry?.ifsc);
  const accountSinceYears = Number(entry?.accountSinceYears);
  const openedIn = Number(entry?.openedIn);
  return {
    bankName: String(entry?.bankName || "").trim(),
    accountNumber: String(entry?.accountNumber || "").trim(),
    ifscCode,
    ifsc: ifscCode,
    branch: String(entry?.branch || "").trim(),
    accountType: String(entry?.accountType || "").trim(),
    accountSinceYears: Number.isFinite(accountSinceYears)
      ? accountSinceYears
      : undefined,
    openedIn: Number.isFinite(openedIn) ? openedIn : undefined,
  };
};

const applyNormalizedBankDetails = (normalized = {}) => {
  const stored = Array.isArray(normalized?.bankDetails)
    ? normalized.bankDetails
    : [];
  const additional = Array.isArray(normalized?.additionalBankDetails)
    ? normalized.additionalBankDetails
    : [];

  const normalizedStored = [...stored, ...additional]
    .map((entry) => normalizeBankEntry(entry))
    .filter((entry) => hasBankEntryValue(entry))
    .slice(0, 3);

  const primaryFlat = normalizeBankEntry({
    bankName: normalized?.bankName,
    accountNumber: normalized?.accountNumber,
    ifscCode: normalized?.ifscCode,
    ifsc: normalized?.ifsc,
    branch: normalized?.branch,
    accountType: normalized?.accountType,
    accountSinceYears: normalized?.accountSinceYears,
    openedIn: normalized?.openedIn,
  });

  const firstStored = normalizedStored[0];
  const primary = hasBankEntryValue(primaryFlat)
    ? {
        ...firstStored,
        ...Object.fromEntries(
          Object.entries(primaryFlat).filter(([, value]) => value !== undefined && value !== ""),
        ),
      }
    : firstStored || primaryFlat;

  const additionalBanks = normalizedStored.slice(firstStored ? 1 : 0);
  const bankDetails = [primary, ...additionalBanks]
    .filter((entry) => hasBankEntryValue(entry))
    .slice(0, 3);

  normalized.bankDetails = bankDetails;

  const top = bankDetails[0];
  if (top && hasBankEntryValue(top)) {
    normalized.bankName = top.bankName || normalized.bankName || "";
    normalized.accountNumber = top.accountNumber || normalized.accountNumber || "";
    normalized.ifscCode = top.ifscCode || normalized.ifscCode || normalized.ifsc || "";
    normalized.ifsc = top.ifsc || normalized.ifscCode || normalized.ifsc || "";
    normalized.branch = top.branch || normalized.branch || "";
    normalized.accountType = top.accountType || normalized.accountType || "";
    normalized.accountSinceYears =
      top.accountSinceYears !== undefined
        ? top.accountSinceYears
        : normalized.accountSinceYears;
    normalized.openedIn =
      top.openedIn !== undefined ? top.openedIn : normalized.openedIn;
  }

  if (normalized.ifsc && !normalized.ifscCode) normalized.ifscCode = normalized.ifsc;
  if (normalized.ifscCode && !normalized.ifsc) normalized.ifsc = normalized.ifscCode;

  delete normalized.additionalBankDetails;
  delete normalized.hasAdditionalBankDetails;
  return normalized;
};

// Normalize customer data - same as loan controller for consistency
const normalizeCustomerData = (payload) => {
  const normalized = { ...payload };

  // Standardize dates
  const dateFields = [
    'dob', 'nomineeDob', 'identityProofExpiry'
  ];
  
  dateFields.forEach(field => {
    if (normalized[field] && typeof normalized[field] === 'string') {
      // DD-MM-YYYY format
      const match = normalized[field].match(/^(\d{2})-(\d{2})-(\d{4})$/);
      if (match) {
        normalized[field] = new Date(`${match[3]}-${match[2]}-${match[1]}`);
      } else {
        const parsed = new Date(normalized[field]);
        if (!isNaN(parsed.getTime())) {
          normalized[field] = parsed;
        }
      }
    }
  });

  // Ensure numeric fields
  const numericFields = [
    'yearsInCurrentHouse', 'yearsInCurrentCity', 'dependents',
    'currentExp', 'totalExp', 'monthlyIncome', 'salaryMonthly',
    'monthlySalary', 'annualIncome',
    'totalIncomeITR', 'accountSinceYears', 'openedIn'
  ];

  numericFields.forEach(field => {
    if (normalized[field] !== undefined && normalized[field] !== null && normalized[field] !== '') {
      if (typeof normalized[field] === 'string' || typeof normalized[field] === 'boolean') {
        const num = Number(normalized[field]);
        if (!isNaN(num)) {
          normalized[field] = num;
        }
      }
    }
  });

  // Handle aliases
  if (normalized.aadhaarNumber && !normalized.aadharNumber) normalized.aadharNumber = normalized.aadhaarNumber;
  if (normalized.aadharNumber && !normalized.aadhaarNumber) normalized.aadhaarNumber = normalized.aadharNumber;
  if (normalized.emailAddress && !normalized.email) normalized.email = normalized.emailAddress;
  if (normalized.email && !normalized.emailAddress) normalized.emailAddress = normalized.email;
  if (normalized.ifsc && !normalized.ifscCode) normalized.ifscCode = normalized.ifsc;
  if (normalized.ifscCode && !normalized.ifsc) normalized.ifsc = normalized.ifscCode;
  if (normalized.salaryMonthly && !normalized.monthlySalary) normalized.monthlySalary = parseInt(normalized.salaryMonthly, 10) || normalized.salaryMonthly;
  if (normalized.monthlySalary && !normalized.salaryMonthly) normalized.salaryMonthly = parseInt(normalized.monthlySalary, 10) || normalized.monthlySalary;
  if (normalized.fatherName && !normalized.sdwOf) normalized.sdwOf = normalized.fatherName;
  if (normalized.itrYears !== undefined && normalized.totalIncomeITR === undefined) normalized.totalIncomeITR = normalized.itrYears;
  if (normalized.experienceCurrent !== undefined && normalized.currentExp === undefined) normalized.currentExp = normalized.experienceCurrent;
  if (normalized.currentExp !== undefined && normalized.experienceCurrent === undefined) normalized.experienceCurrent = normalized.currentExp;
  if (normalized.totalExperience !== undefined && normalized.totalExp === undefined) normalized.totalExp = normalized.totalExperience;
  if (normalized.totalExp !== undefined && normalized.totalExperience === undefined) normalized.totalExperience = normalized.totalExp;
  applyNormalizedBankDetails(normalized);

  // Flatten reference1/reference2 objects to flat fields (DB schema uses reference1_name, etc.)
  if (normalized.reference1 && typeof normalized.reference1 === 'object') {
    normalized.reference1_name = normalized.reference1.name;
    normalized.reference1_mobile = normalized.reference1.mobile;
    normalized.reference1_address = normalized.reference1.address;
    normalized.reference1_pincode = normalized.reference1.pincode;
    normalized.reference1_city = normalized.reference1.city;
    normalized.reference1_relation = normalized.reference1.relation;
    delete normalized.reference1;
  }
  if (normalized.reference2 && typeof normalized.reference2 === 'object') {
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

// Helper: Get Next Customer ID (format: ACILLP-year-sequence, e.g. ACILLP-2025-0001)
const getNextCustomerId = async () => {
  const prefix = 'ACILLP';
  const year = new Date().getFullYear();
  const regex = new RegExp(`^${prefix}-${year}-\\d{4}$`);
  
  const lastDoc = await Customer.findOne({ customerId: { $regex: regex } }).sort({ customerId: -1 });
  
  let nextNum = 1;
  if (lastDoc && lastDoc.customerId) {
      const parts = lastDoc.customerId.split('-');
      if (parts.length === 3) {
          const numPart = parseInt(parts[2], 10);
          if (!isNaN(numPart)) {
              nextNum = numPart + 1;
          }
      }
  }
  return `${prefix}-${year}-${String(nextNum).padStart(4, '0')}`;
};

// @desc    Search customers
// @route   GET /api/customers/search
// @access  Public
const searchCustomers = asyncHandler(async (req, res) => {
  const q = req.query.q || '';
  if (!q) {
    return res.json({ success: true, data: [] });
  }

  const tokenFilter = buildSearchTokenFilter(q);

  if (tokenFilter) {
    const indexedCustomers = await Customer.find(tokenFilter).limit(20);
    if (indexedCustomers.length) {
      return res.json({
        success: true,
        data: indexedCustomers,
      });
    }
  }

  // Fallback keeps middle-of-field substring matches working.
  const regex = new RegExp(escapeSearchRegex(q), 'i');
  
  const customers = await Customer.find({
    $or: [
      { customerName: regex },
      { companyName: regex },
      { contactPersonName: regex },
      { primaryMobile: regex },
      { panNumber: regex },
      { city: regex },
      { customerId: regex },
    ],
  }).limit(20);

  res.json({
    success: true,
    data: customers,
  });
});

// @desc    Get all customers
// @route   GET /api/customers
// @access  Public
const getCustomers = asyncHandler(async (req, res) => {
  const pageSize = Number(req.query.limit) || 50;
  const skip = Number(req.query.skip) || 0;

  const count = await Customer.countDocuments({});
  const customers = await Customer.find({})
    .sort({ createdAt: -1 })
    .limit(pageSize)
    .skip(skip);

  res.json({
    success: true,
    count,
    data: customers,
  });
});

// @desc    Get single customer by ID
// @route   GET /api/customers/:id
// @access  Public
const getCustomerById = asyncHandler(async (req, res) => {
  let customer;
  // Support both _id and string customerId
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
      customer = await Customer.findById(req.params.id);
  } else {
      customer = await Customer.findOne({ customerId: req.params.id });
  }

  if (customer) {
    // 🔗 FETCH LINKED LOANS - so customer updates are visible
    const linkedLoans = await Loan.find({ customerId: customer._id })
      .select(
        '_id loanId loan_number typeOfLoan loanType caseType isFinanced status currentStage ' +
        'customerName primaryMobile email panNumber source sourceName dealerName ' +
        'vehicleMake vehicleModel vehicleVariant vehicleRegNo registrationNumber rc_redg_no registrationCity postfile_regd_city ' +
        'bankName approval_bankName approval_loanAmountApproved approval_loanAmountDisbursed postfile_emiAmount ' +
        'disbursement_date approval_disbursedDate delivery_date createdAt updatedAt bankDetails',
      )
      .sort({ createdAt: -1 })
      .lean();

    const responseData = {
      ...customer.toObject(),
      linkedLoans: linkedLoans || []
    };

    res.json({ success: true, data: responseData });
  } else {
    res.status(404);
    throw new Error('Customer not found');
  }
});

// @desc    Get customer details with linked loans (for dashboard)
// @route   GET /api/customers/:id/dashboard
// @access  Public
const getCustomerDashboard = asyncHandler(async (req, res) => {
  let customer;
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
      customer = await Customer.findById(req.params.id);
  } else {
      customer = await Customer.findOne({ customerId: req.params.id });
  }

  if (!customer) {
    res.status(404);
    throw new Error('Customer not found');
  }

  // Fetch all loans linked to this customer
  const loans = await Loan.find({ customerId: customer._id })
    .select('loanId status currentStage vehicleModel loanAmount tenure approval_status approval_loanAmountApproved createdAt')
    .sort({ createdAt: -1 });

  res.json({
    success: true,
    data: {
      customer: {
        _id: customer._id,
        customerId: customer.customerId,
        customerName: customer.customerName,
        primaryMobile: customer.primaryMobile,
        email: customer.email,
        gender: customer.gender,
        dob: customer.dob,
        maritalStatus: customer.maritalStatus,
        dependents: customer.dependents,
        education: customer.education,
        // Contact
        extraMobiles: customer.extraMobiles,
        whatsappNumber: customer.whatsappNumber,
        // Address
        residenceAddress: customer.residenceAddress,
        pincode: customer.pincode,
        city: customer.city,
        state: customer.state,
        permanentAddress: customer.permanentAddress,
        permanentPincode: customer.permanentPincode,
        permanentCity: customer.permanentCity,
        sameAsCurrentAddress: customer.sameAsCurrentAddress,
        // Employment
        occupationType: customer.occupationType,
        employmentType: customer.employmentType,
        companyName: customer.companyName,
        designation: customer.designation,
        contactPersonName: customer.contactPersonName,
        contactPersonMobile: customer.contactPersonMobile,
        currentExp: customer.currentExp,
        totalExp: customer.totalExp,
        experienceCurrent: customer.experienceCurrent,
        totalExperience: customer.totalExperience,
        isMSME: customer.isMSME,
        companyType: customer.companyType,
        businessNature: customer.businessNature,
        companyAddress: customer.companyAddress,
        companyCity: customer.companyCity,
        companyPincode: customer.companyPincode,
        companyPhone: customer.companyPhone,
        officialEmail: customer.officialEmail,
        companyPartners: customer.companyPartners,
        // Income
        monthlyIncome: customer.monthlyIncome,
        salaryMonthly: customer.salaryMonthly,
        monthlySalary: customer.monthlySalary,
        annualIncome: customer.annualIncome,
        // Identity Proofs
        panNumber: customer.panNumber,
        aadharNumber: customer.aadharNumber,
        aadhaarNumber: customer.aadhaarNumber,
        dlNumber: customer.dlNumber,
        passportNumber: customer.passportNumber,
        gstNumber: customer.gstNumber,
        identityProofType: customer.identityProofType,
        identityProofNumber: customer.identityProofNumber,
        addressProofType: customer.addressProofType,
        addressProofNumber: customer.addressProofNumber,
        // Banking
        bankName: customer.bankName,
        accountNumber: customer.accountNumber,
        ifscCode: customer.ifscCode,
        ifsc: customer.ifsc,
        branch: customer.branch,
        accountType: customer.accountType,
        accountSinceYears: customer.accountSinceYears,
        openedIn: customer.openedIn,
        bankDetails: customer.bankDetails,
        // References
        reference1_name: customer.reference1_name,
        reference1_mobile: customer.reference1_mobile,
        reference2_name: customer.reference2_name,
        reference2_mobile: customer.reference2_mobile,
        // Nominee
        nomineeName: customer.nomineeName,
        nomineeDob: customer.nomineeDob,
        nomineeRelation: customer.nomineeRelation,
        // Metadata
        customerType: customer.customerType,
        kycStatus: customer.kycStatus,
        createdOn: customer.createdOn,
        createdAt: customer.createdAt,
        updatedAt: customer.updatedAt
      },
      loans: loans.map(loan => ({
        loanId: loan.loanId,
        status: loan.status,
        currentStage: loan.currentStage,
        vehicleModel: loan.vehicleModel,
        loanAmount: loan.loanAmount,
        tenure: loan.tenure,
        approvalStatus: loan.approval_status,
        approvalAmount: loan.approval_loanAmountApproved,
        createdAt: loan.createdAt
      })),
      summary: {
        totalLoans: loans.length,
        profileCompletion: calculateProfileCompletion(customer),
        kycStatus: customer.kycStatus,
        activeLoans: loans.filter(l => l.status === 'Pending' || l.status === 'Approved').length
      }
    },
    message: '✅ Customer dashboard data retrieved with all linked loans'
  });
});

// Helper: Calculate profile completion percentage
const calculateProfileCompletion = (customer) => {
  const fields = [
    'customerName', 'primaryMobile', 'email', 'dob', 'gender',
    'occupationType', 'companyName', 'monthlyIncome', 'salaryMonthly',
    'panNumber', 'aadharNumber', 'residenceAddress', 'city', 'pincode'
  ];
  
  const filled = fields.filter(field => customer[field]).length;
  return Math.round((filled / fields.length) * 100);
};

// @desc    Create a customer
// @route   POST /api/customers
// @access  Public
const createCustomer = asyncHandler(async (req, res) => {
  const normalizedData = normalizeCustomerData(req.body);

  // companyType is single-select in UI; businessNature remains multi-select
  if (Array.isArray(normalizedData.companyType)) {
    normalizedData.companyType = normalizedData.companyType[0] || "";
  }
  if (!Array.isArray(normalizedData.businessNature)) {
    normalizedData.businessNature = normalizedData.businessNature
      ? [normalizedData.businessNature]
      : [];
  }

  const { 
    customerName, primaryMobile 
  } = normalizedData;

  if (!customerName || !primaryMobile) {
    res.status(400);
    throw new Error('Please include Customer Name and Mobile Number');
  }

  const today = new Date();
  const createdOn = `${String(today.getDate()).padStart(2, '0')}-${String(today.getMonth() + 1).padStart(2, '0')}-${today.getFullYear()}`;

  // Retry logic for ID collision
  let customerId = await getNextCustomerId();
  let customer;
  
  try {
      // ENSURE ALL FIELDS ARE SAVED
      const customerPayload = {
        ...normalizedData,
        customerId,
        createdOn,
        customerType: normalizedData.customerType || 'New'
      };

      customer = await Customer.create(customerPayload);

  } catch (error) {
     if (error.code === 11000 && error.keyPattern?.customerId) {
        console.warn("Customer ID collision, retrying with new ID...");
        const retryId = await getNextCustomerId();
        if (retryId === customerId) {
             const parts = retryId.split('-');
             customerId = `${parts[0]}-${parts[1]}-${String(Number(parts[2]) + 1).padStart(4, '0')}`;
        } else {
             customerId = retryId;
        }

        customer = await Customer.create({
            ...normalizedData,
            customerId,
            createdOn,
            customerType: normalizedData.customerType || 'New'
        });

     } else {
        throw error;
     }
  }

  if (customer) {
    res.status(201).json({
      success: true,
      data: customer,
      message: '✅ Customer profile created with all details saved',
      savedDetails: {
        customerId: customer.customerId,
        customerName: customer.customerName,
        primaryMobile: customer.primaryMobile,
        email: customer.email,
        occupationType: customer.occupationType,
        monthlyIncome: customer.monthlyIncome || customer.salaryMonthly,
        panNumber: customer.panNumber,
        aadharNumber: customer.aadharNumber,
        kycStatus: customer.kycStatus
      }
    });
  } else {
    res.status(400);
    throw new Error('Invalid customer data');
  }
});

// @desc    Update a customer
// @route   PUT /api/customers/:id
// @access  Public
const updateCustomer = asyncHandler(async (req, res) => {
  let customer;
  if (req.params.id.match(/^[0-9a-fA-F]{24}$/)) {
      customer = await Customer.findById(req.params.id);
  } else {
      customer = await Customer.findOne({ customerId: req.params.id });
  }

  if (customer) {
    // Normalize incoming data
    const normalizedData = normalizeCustomerData(req.body);

    // companyType is single-select in UI; businessNature remains multi-select
    if (Array.isArray(normalizedData.companyType)) {
      normalizedData.companyType = normalizedData.companyType[0] || "";
    }
    if (!Array.isArray(normalizedData.businessNature)) {
      normalizedData.businessNature = normalizedData.businessNature
        ? [normalizedData.businessNature]
        : [];
    }

    // Clean body - remove system fields
    const cleanedData = { ...normalizedData };
    delete cleanedData._id;
    delete cleanedData.__v;
    delete cleanedData.customerId;
    delete cleanedData.createdAt;
    delete cleanedData.updatedAt;
    delete cleanedData.createdOn;
    delete cleanedData.createdBy;

    // Update all fields
    Object.assign(customer, cleanedData);

    // Explicitly update critical fields
    if (normalizedData?.customerName) customer.customerName = normalizedData.customerName;
    if (normalizedData?.primaryMobile) customer.primaryMobile = normalizedData.primaryMobile;
    if (normalizedData?.email) customer.email = normalizedData.email;
    if (normalizedData?.dob) customer.dob = normalizedData.dob;
    if (normalizedData?.occupationType) customer.occupationType = normalizedData.occupationType;
    if (normalizedData?.monthlyIncome !== undefined) customer.monthlyIncome = normalizedData.monthlyIncome;
    if (normalizedData?.salaryMonthly !== undefined) customer.salaryMonthly = normalizedData.salaryMonthly;
    if (normalizedData?.monthlySalary !== undefined) customer.monthlySalary = normalizedData.monthlySalary;
    if (normalizedData?.panNumber) customer.panNumber = normalizedData.panNumber;
    if (normalizedData?.aadharNumber) customer.aadharNumber = normalizedData.aadharNumber;
    
    const updatedCustomer = await customer.save();

    // 🔁 Sync updated customer fields into linked loans (denormalized snapshot)
    try {
      const loanUpdate = {};
      Object.keys(cleanedData).forEach((key) => {
        if (cleanedData[key] !== undefined) loanUpdate[key] = cleanedData[key];
      });

      // Ensure common aliases are also updated on loans
      if (normalizedData?.email !== undefined) loanUpdate.email = normalizedData.email;
      if (normalizedData?.emailAddress !== undefined) loanUpdate.emailAddress = normalizedData.emailAddress;
      if (normalizedData?.aadharNumber !== undefined) loanUpdate.aadharNumber = normalizedData.aadharNumber;
      if (normalizedData?.aadhaarNumber !== undefined) loanUpdate.aadhaarNumber = normalizedData.aadhaarNumber;
      if (normalizedData?.primaryMobile !== undefined) loanUpdate.primaryMobile = normalizedData.primaryMobile;
      if (normalizedData?.customerName !== undefined) loanUpdate.customerName = normalizedData.customerName;

      if (Object.keys(loanUpdate).length > 0) {
        const loanSyncResult = await Loan.updateMany(
          { customerId: customer._id },
          { $set: { ...loanUpdate, updatedAt: new Date() } }
        );
      }
    } catch (err) {
      console.error('⚠️ Failed to sync loans with customer update:', err.message);
    }

    // Sync linked insurance cases (denormalized customer fields + snapshot)
    try {
      const insuranceUpdate = {};
      if (normalizedData?.customerName !== undefined) {
        insuranceUpdate.customerName = normalizedData.customerName;
      }
      if (normalizedData?.companyName !== undefined) {
        insuranceUpdate.companyName = normalizedData.companyName;
      }
      if (normalizedData?.contactPersonName !== undefined) {
        insuranceUpdate.contactPersonName = normalizedData.contactPersonName;
      }
      if (normalizedData?.primaryMobile !== undefined) {
        insuranceUpdate.mobile = normalizedData.primaryMobile;
      }
      if (normalizedData?.email !== undefined) {
        insuranceUpdate.email = normalizedData.email;
      }
      if (Object.keys(insuranceUpdate).length > 0) {
        const snap = buildInsuranceCustomerSnapshot(updatedCustomer);
        for (const [k, v] of Object.entries(snap)) {
          insuranceUpdate[`customerSnapshot.${k}`] = v;
        }
        const insuranceSyncResult = await InsuranceCase.updateMany(
          { customerId: customer._id },
          { $set: { ...insuranceUpdate, updatedAt: new Date() } },
        );
      }
    } catch (err) {
      console.error('⚠️ Failed to sync insurance cases with customer update:', err.message);
    }

    res.json({
      success: true, 
      data: updatedCustomer,
      message: '✅ Customer profile updated with all details saved',
      updatedFields: Object.keys(cleanedData).length
    });
  } else {
    res.status(404);
    throw new Error('Customer not found');
  }
});

// @desc    Reassign all loans from one customer to another (so the first customer can be deleted)
// @route   POST /api/customers/:id/reassign-loans
// @access  Public
const reassignLoans = asyncHandler(async (req, res) => {
  const fromCustomerId = req.params.id;
  const { targetCustomerId } = req.body || {};

  if (!targetCustomerId) {
    res.status(400);
    throw new Error('targetCustomerId is required in request body');
  }

  const fromCustomer = await Customer.findById(fromCustomerId);
  if (!fromCustomer) {
    res.status(404);
    throw new Error('Customer not found');
  }

  const targetCustomer = await Customer.findById(targetCustomerId);
  if (!targetCustomer) {
    res.status(404);
    throw new Error('Target customer not found');
  }

  if (String(fromCustomerId) === String(targetCustomerId)) {
    res.status(400);
    throw new Error('Source and target customer must be different');
  }

  const result = await Loan.updateMany(
    { customerId: fromCustomer._id },
    { $set: { customerId: targetCustomer._id } }
  );

  const reassignedCount = result.modifiedCount || 0;

  res.json({
    success: true,
    message: `${reassignedCount} loan(s) reassigned from "${fromCustomer.customerName}" to "${targetCustomer.customerName}"`,
    reassignedCount,
  });
});

// @desc    Delete a customer
// @route   DELETE /api/customers/:id
// @access  Public
const deleteCustomer = asyncHandler(async (req, res) => {
  const customer = await Customer.findById(req.params.id);

  if (customer) {
    // 🔒 CASCADE DELETE PROTECTION - Check for linked loans
    const linkedLoansCount = await Loan.countDocuments({ customerId: customer._id });
    
    if (linkedLoansCount > 0) {
      console.error(`❌ Cannot delete customer ${customer.customerId} - ${linkedLoansCount} loans linked`);
      res.status(400);
      throw new Error(
        `Cannot delete customer "${customer.customerName}" (${customer.customerId}). ` +
        `${linkedLoansCount} loan(s) are linked to this customer. ` +
        `Please delete or reassign the loans first.`
      );
    }
    
    await customer.deleteOne();
    
    res.json({ 
      success: true, 
      message: `Customer "${customer.customerName}" removed successfully`,
      deletedCustomerId: customer.customerId
    });
  } else {
    res.status(404);
    throw new Error('Customer not found');
  }
});

export {
  getCustomers,
  getCustomerById,
  getCustomerDashboard,
  createCustomer,
  updateCustomer,
  deleteCustomer,
  searchCustomers,
  reassignLoans,
};
