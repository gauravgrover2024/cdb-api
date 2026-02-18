// scripts/migrateLoansToCustomers.js
import mongoose from "mongoose";
import dotenv from "dotenv";
import Loan from "../models/Loan.js";
import Customer from "../models/Customer.js";
import { fileURLToPath } from "url";
import path from "path";

// Load .env
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
dotenv.config({ path: path.join(__dirname, "..", "..", ".env") });

const connectDB = async () => {
  await mongoose.connect(process.env.MONGODB_URI);
  console.log(
    "Connected to Mongo:",
    mongoose.connection.host,
    "db:",
    mongoose.connection.name,
  );
};

// Simple helper to generate customerId (same logic as in controller)
const getNextCustomerId = async () => {
  const prefix = "ACILLP";
  const fieldName = "customerId";
  const year = new Date().getFullYear();
  const regex = new RegExp(`^${prefix}-${year}-\\d{4}$`);
  const query = {};
  query[fieldName] = { $regex: regex };

  const lastDoc = await Customer.findOne(query).sort({ [fieldName]: -1 });

  let nextNum = 1;
  if (lastDoc && lastDoc[fieldName]) {
    const parts = lastDoc[fieldName].split("-");
    if (parts.length === 3) {
      const numPart = parseInt(parts[2], 10);
      if (!isNaN(numPart)) nextNum = numPart + 1;
    }
  }
  return `${prefix}-${year}-${String(nextNum).padStart(4, "0")}`;
};

const run = async () => {
  await connectDB();

  // Pick loans that do NOT have customerId set
  const loans = await Loan.find({
    $or: [{ customerId: { $exists: false } }, { customerId: null }],
  });

  console.log("Loans without customerId:", loans.length);

  let created = 0;
  let linkedExisting = 0;

  for (const loan of loans) {
    const customerName = loan.customerName || loan.applicant?.name;
    const primaryMobile =
      loan.primaryMobile || loan.applicant?.primaryMobile || null;

    if (!customerName || !primaryMobile) {
      console.log(
        `Skipping loan ${loan.loanId || loan._id} – missing customerName or primaryMobile`,
      );
      continue;
    }

    // Check if a customer already exists with this mobile
    let customer = await Customer.findOne({ primaryMobile });

    if (customer) {
      // Link existing customer
      loan.customerId = customer._id;
      await loan.save();
      linkedExisting++;
      continue;
    }

    // Create new customer from loan snapshot
    const customerId = await getNextCustomerId();

    const payload = {
      customerId,
      customerName,
      primaryMobile,

      // Basic contact & KYC
      email: loan.email || loan.applicant?.email || "",
      whatsappNumber: loan.whatsappNumber || "",
      panNumber: loan.kyc?.panNumber || loan.panNumber || "",
      aadhaarNumber: loan.kyc?.aadhaarNumber || loan.aadhaarNumber || "",
      aadharNumber: loan.kyc?.aadhaarNumber || loan.aadhaarNumber || "",

      // Personal
      dob: loan.applicant?.dob || loan.dob || null,
      gender: loan.applicant?.gender || loan.gender || "",
      fatherName: loan.applicant?.fatherName || loan.fatherName || "",
      motherName: loan.applicant?.motherName || loan.motherName || "",
      maritalStatus: loan.applicant?.maritalStatus || loan.maritalStatus || "",
      dependents:
        loan.applicant?.dependents != null
          ? loan.applicant.dependents
          : loan.dependents,
      education: loan.applicant?.education || loan.education || "",

      // Address
      residenceAddress:
        loan.residenceAddress || loan.applicant?.residenceAddress || "",
      pincode: loan.pincode || loan.applicant?.pincode || "",
      city: loan.city || loan.applicant?.city || "",
      state: loan.state || loan.applicant?.state || "",
      yearsInCurrentHouse:
        loan.applicant?.yearsInCurrentHouse || loan.yearsInCurrentHouse || null,
      yearsInCurrentCity:
        loan.applicant?.yearsInCurrentCity || loan.yearsInCurrentCity || null,

      // Employment / business
      occupationType:
        loan.applicantEmployment?.occupationType || loan.occupationType || "",
      companyName:
        loan.applicantEmployment?.companyName || loan.companyName || "",
      companyType:
        loan.applicantEmployment?.companyType || loan.companyType || "",
      businessNature:
        loan.applicantEmployment?.businessNature || loan.businessNature || [],
      employmentAddress:
        loan.applicantEmployment?.employmentAddress ||
        loan.companyAddress ||
        "",
      employmentPincode:
        loan.applicantEmployment?.employmentPincode ||
        loan.companyPincode ||
        "",
      employmentCity:
        loan.applicantEmployment?.employmentCity || loan.companyCity || "",
      employmentPhone:
        loan.applicantEmployment?.employmentPhone || loan.companyPhone || "",

      // Banking
      bankName: loan.applicantBank?.bankName || loan.bankName || "",
      accountNumber:
        loan.applicantBank?.accountNumber || loan.accountNumber || "",
      ifscCode: loan.applicantBank?.ifscCode || loan.ifscCode || "",
      branch: loan.applicantBank?.branch || loan.branch || "",

      // Loan intent (optional)
      typeOfLoan: loan.typeOfLoan || loan.finance?.typeOfLoan || "",
      financeExpectation:
        loan.finance?.financeExpectation || loan.financeExpectation || null,
      loanTenureMonths:
        loan.finance?.loanTenureMonths || loan.loanTenureMonths || null,

      createdFrom: "MIGRATION",
    };

    customer = await Customer.create(payload);
    loan.customerId = customer._id;
    await loan.save();

    created++;
    console.log(
      `Created customer ${customer.customerId} for loan ${
        loan.loanId || loan._id
      }`,
    );
  }

  console.log("Migration complete.");
  console.log("Existing customers linked:", linkedExisting);
  console.log("New customers created:", created);

  await mongoose.disconnect();
  process.exit(0);
};

run().catch((err) => {
  console.error(err);
  mongoose.disconnect().then(() => process.exit(1));
});
