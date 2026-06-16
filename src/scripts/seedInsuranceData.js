import fs from "fs";
import path from "path";
import JSONStream from "JSONStream";
import mongoose from "mongoose";
import dotenv from "dotenv";

// Load environment variables
dotenv.config();

// Mongoose Models
import InsuranceCase from "../models/InsuranceCase.js";
import Customer from "../models/Customer.js";
import VehicleRecord from "../models/VehicleRecord.js";

const DRY_RUN = process.argv.includes("--dry-run");

const STATS = {
  totalRecords: 0,
  mappedRecords: 0,
  failedRecords: 0,
  missingMandatoryFields: 0,
  unmappedFields: new Set(),
  newCustomersToCreate: 0,
  existingCustomersFound: 0,
};

// In-memory cache for Customer and Vehicle
const customerPhoneMap = new Map();

// Helper: phone validation
function isValidPhone(val) {
  if (!val) return false;
  // allow digits, spaces, plus, hyphens
  const str = String(val).trim();
  if (/[a-zA-Z]/.test(str)) return false; // Contains letters
  return /\d{8,}/.test(str); // at least 8 digits
}

// Helper: Safe number extraction
function safeNum(val) {
  if (val === null || val === undefined || val === "") return 0;
  const parsed = Number(val);
  return isNaN(parsed) ? 0 : parsed;
}

// Helper: Normalize reg number
function normalizeReg(reg) {
  if (!reg) return "";
  return String(reg).replace(/[^a-zA-Z0-9]/g, "").toUpperCase();
}

async function loadCaches() {
  console.log("Loading Customer cache...");
  const customers = await Customer.find({}, { primaryMobile: 1, _id: 1 }).lean();
  for (const c of customers) {
    if (c.primaryMobile) {
      customerPhoneMap.set(c.primaryMobile.trim(), c._id);
    }
  }
  console.log(`Loaded ${customers.length} customers into cache.`);
}

function processMAKE_MODEL(makeModel) {
  if (!makeModel) return { make: "", model: "", variant: "" };
  const parts = String(makeModel).trim().split(" ");
  const make = parts[0] || "";
  const model = parts[1] || "";
  const variant = parts.slice(2).join(" ");
  return { make, model, variant };
}

function processRow(row) {
  STATS.totalRecords++;

  if (!row.INSURANCE_ID) {
    STATS.missingMandatoryFields++;
    STATS.failedRecords++;
    return null;
  }

  // Determine Customer
  const customerName = String(row.CUSTOMER_NAME || "").trim();
  const primaryPhone = String(row.PHONE_NUMBER_1 || "").trim();
  let validPhone1 = isValidPhone(primaryPhone) ? primaryPhone : "";
  let validPhone2 = isValidPhone(row.PHONE_NUMBER_2) ? String(row.PHONE_NUMBER_2).trim() : "";
  let validPhone3 = isValidPhone(row.PHONE_NUMBER3) ? String(row.PHONE_NUMBER3).trim() : "";

  if (!validPhone1 && validPhone2) validPhone1 = validPhone2;
  if (!validPhone1 && validPhone3) validPhone1 = validPhone3;

  const email = String(row.E_MAIL_ADDRESS || "").trim().toLowerCase();
  const address = String(row.CUSTOMER_ADDRESS || "").trim();

  let customerId = null;
  let isNewCustomer = false;

  if (validPhone1) {
    if (customerPhoneMap.has(validPhone1)) {
      customerId = customerPhoneMap.get(validPhone1);
      STATS.existingCustomersFound++;
    } else {
      isNewCustomer = true;
      customerId = new mongoose.Types.ObjectId();
      customerPhoneMap.set(validPhone1, customerId);
      STATS.newCustomersToCreate++;
    }
  }

  // Payment Auto Gen
  const amountPaid = safeNum(row.AMOUNT_PAID);
  const paymentHistory = [];
  if (amountPaid > 0) {
    paymentHistory.push({
      amount: amountPaid,
      entryType: "CUSTOMER_RECEIPT",
      paymentType: "customer",
      date: row.INS_EFFECTIVE_FROM_DATE ? new Date(row.INS_EFFECTIVE_FROM_DATE) : new Date(),
    });
  }

  // Vehicle Split
  const { make, model, variant } = processMAKE_MODEL(row.MAKE_MODEL);
  const regNo = String(row.REGISTRATION_NUMBER || "").trim();
  const engNo = String(row.ENGINE_NUMBER || "").trim();
  const chasNo = String(row.CHASSIS_NUMBER || "").trim();

  // Policy Classification
  let policyCategory = String(row.INSURANCE_CATEGORY || "").trim();
  if (policyCategory.includes("Renewal")) policyCategory = "Renewal";
  if (policyCategory.includes("New")) policyCategory = "New";

  const isRenewal = row.IS_RENEWAL === "Y";
  const usedCarFlowType = isRenewal ? "Renewal" : "New Business";

  // Build InsuranceCase
  const insuranceCase = {
    caseId: String(row.INSURANCE_ID).trim(),
    customerId: customerId,
    customerSnapshot: {
      customerName: customerName,
      primaryMobile: validPhone1,
      email: email,
      residenceAddress: address,
    },
    // Defaults
    status: "issued",
    currentStep: 6,
    registrationAllotted: regNo ? "Yes" : "No",
    policyDoneBy: "Autocredits India LLP",
    source: "Migration",
    typesOfVehicle: "Four Wheeler",

    // Tracking
    sourceOrigin: String(row.LEAD_ID || "").trim(),
    employeeName: String(row.EXE1 || "").trim(),
    assignedTo: String(row.SAVED_BY || "").trim(),
    dealerChannelName: String(row.DEALER_NAME || "").trim(),
    sourceName: String(row.CHANNEL || String(row.BRANCH || "")).trim(),
    channelDealerNo: String(row.CHANNEL || "").trim(),
    showroomName: String(row.BRANCH || "").trim(),
    referenceName: String(row.REFERENCE_NAME || "").trim(),
    referencePhone: isValidPhone(row.REFERENCE_MOBILE_NUMBER) ? String(row.REFERENCE_MOBILE_NUMBER).trim() : "",

    // Vehicle
    vehicleMake: make,
    vehicleModel: model,
    vehicleVariant: variant,
    manufactureYear: String(row.MFG_YEAR || "").trim(),
    cubicCapacity: String(row.CUBIC_CAPACITY || "").trim(),
    registrationNumber: regNo,
    engineNumber: engNo,
    chassisNumber: chasNo,
    vehicleType: row.USEDCAR_NEWCAR === "NEW" ? "New Car" : "Used Car",

    // Policy
    isRenewal: isRenewal,
    usedCarFlowType: usedCarFlowType,
    policyCategory: policyCategory,
    previousInsuranceCompany: String(row.COMPANY_NAME || "").trim(),
    newInsuranceCompany: String(row.COMPANY_NAME || "").trim(),
    previousPolicyNumber: String(row.COVER_NOTE_NUMBER || "").trim(),
    newPolicyNumber: String(row.COVER_NOTE_NUMBER || "").trim(),
    newPolicyType: String(row.POLICY_TYPE || "").trim(),
    newIssueDate: row.DATE_OF_ISSUE ? new Date(row.DATE_OF_ISSUE) : null,
    newPolicyStartDate: row.INS_EFFECTIVE_FROM_DATE ? new Date(row.INS_EFFECTIVE_FROM_DATE) : null,
    newOdExpiryDate: row.INS_EXPIRY_ON_DATE ? new Date(row.INS_EXPIRY_ON_DATE) : null,
    newTpExpiryDate: row.INS_EXPIRY_ON_DATE ? new Date(row.INS_EXPIRY_ON_DATE) : null,
    
    newNcbDiscount: safeNum(row.NCB),
    newAccessoriesIdv: safeNum(row.ACCESSORIES),
    newIdvAmount: safeNum(row.SUM_INSURED),
    newTotalPremium: safeNum(row.PREMIUM_AMOUNT),
    customerPaymentReceived: amountPaid,
    paymentHistory: paymentHistory,
    newHypothecation: String(row.ADD_RISK_AND_SPL_CONDITION || "").trim(),

    nomineeName: String(row.NOMINEE_NAME || "").trim(),
    nomineeRelationship: String(row.NOMINEE_RELATION || "").trim(),
    nomineeAge: String(row.NOMINEE_AGE_DOB || "").trim(),

    // Quotes breakdown (synthetic)
    quotes: [{
      id: "MIG-" + row.INSURANCE_ID,
      insuranceCompany: String(row.COMPANY_NAME || "").trim(),
      odAmount: safeNum(row.OD_AMOUNT),
      totalPremium: safeNum(row.PREMIUM_AMOUNT),
      totalIdv: safeNum(row.SUM_INSURED),
      isAccepted: true
    }],

    // Audits
    createdAt: row.SAVED_ON_DATE ? new Date(row.SAVED_ON_DATE) : new Date(),
    updatedAt: row.MODIFIED_ON_DATE ? new Date(row.MODIFIED_ON_DATE) : new Date(),
  };

  const newCustomerData = isNewCustomer ? {
    _id: customerId,
    customerName: customerName,
    primaryMobile: validPhone1,
    email: email,
    residenceAddress: address,
    customerId: "MIG-" + validPhone1,
    customerType: "New",
  } : null;

  const vehicleRecordData = {
    registrationNumber: regNo,
    registrationNumberNormalized: normalizeReg(regNo),
    make: make,
    model: model,
    variant: variant,
    engineNumber: engNo,
    chassisNumber: chasNo,
    yearOfManufacture: String(row.MFG_YEAR || "").trim(),
    cubicCapacityCc: safeNum(row.CUBIC_CAPACITY),
    sourceCaseType: "Insurance",
    customerId: customerId,
    customerName: customerName,
    primaryMobile: validPhone1,
    typesOfVehicle: "Four Wheeler"
  };

  STATS.mappedRecords++;

  return { insuranceCase, newCustomerData, vehicleRecordData };
}

async function syncDatabase() {
  console.log(`Starting ${DRY_RUN ? 'DRY RUN' : 'PRODUCTION'} sync...`);

  if (!DRY_RUN) {
    if (!process.env.MONGO_URI) {
      throw new Error("MONGO_URI is missing in .env");
    }
    await mongoose.connect(process.env.MONGO_URI);
    console.log("📦 Connected to MongoDB");
    await loadCaches();
  } else {
    console.log("No DB connection made in DRY RUN. Caches skipped.");
  }

  const jsonFilePath = path.join(process.cwd(), "INS_COVER_NOTE_DETAIL.json");
  if (!fs.existsSync(jsonFilePath)) {
    throw new Error(`File not found: ${jsonFilePath}`);
  }

  const stream = fs.createReadStream(jsonFilePath, { encoding: "utf8" });
  const parser = JSONStream.parse("*"); // Parse each object in the array

  let customerOperations = [];
  let vehicleOperations = [];
  let insuranceOperations = [];

  const BATCH_SIZE = 1000;

  async function flushBatches() {
    if (DRY_RUN) {
      customerOperations = [];
      vehicleOperations = [];
      insuranceOperations = [];
      return;
    }

    if (customerOperations.length > 0) {
      await Customer.bulkWrite(customerOperations, { ordered: false }).catch(e => console.error("Customer bulkWrite error:", e.message));
      customerOperations = [];
    }
    if (vehicleOperations.length > 0) {
      await VehicleRecord.bulkWrite(vehicleOperations, { ordered: false }).catch(e => console.error("Vehicle bulkWrite error:", e.message));
      vehicleOperations = [];
    }
    if (insuranceOperations.length > 0) {
      await InsuranceCase.bulkWrite(insuranceOperations, { ordered: false }).catch(e => console.error("Insurance bulkWrite error:", e.message));
      insuranceOperations = [];
    }
  }

  stream.pipe(parser);

  parser.on("data", async (row) => {
    parser.pause();

    try {
      const processed = processRow(row);
      if (processed) {
        const { insuranceCase, newCustomerData, vehicleRecordData } = processed;

        if (newCustomerData) {
          customerOperations.push({
            insertOne: {
              document: newCustomerData
            }
          });
        }

        if (vehicleRecordData.registrationNumberNormalized) {
          vehicleOperations.push({
            updateOne: {
              filter: { registrationNumberNormalized: vehicleRecordData.registrationNumberNormalized },
              update: { $set: vehicleRecordData },
              upsert: true
            }
          });
        }

        insuranceOperations.push({
          updateOne: {
            filter: { caseId: insuranceCase.caseId },
            update: { $set: insuranceCase },
            upsert: true
          }
        });

        if (insuranceOperations.length >= BATCH_SIZE) {
          process.stdout.write(`✅ Processed ${STATS.totalRecords} rows...\n`);
          await flushBatches();
        }
      }
    } catch (err) {
      console.error("Error processing row:", err);
      STATS.failedRecords++;
    }

    parser.resume();
  });

  parser.on("end", async () => {
    // Flush remaining
    await flushBatches();

    console.log("\n📊 Execution Report");
    console.log("-------------------");
    console.log(`Total Records: ${STATS.totalRecords}`);
    console.log(`Mapped Records: ${STATS.mappedRecords}`);
    console.log(`Failed Records: ${STATS.failedRecords}`);
    console.log(`Missing Mandatory Fields: ${STATS.missingMandatoryFields}`);
    console.log(`New Customers To Create: ${STATS.newCustomersToCreate}`);
    console.log(`Existing Customers Found: ${STATS.existingCustomersFound}`);
    console.log("-------------------");

    if (!DRY_RUN) {
      await mongoose.disconnect();
      console.log("🔌 Disconnected from MongoDB");
    }
  });

  parser.on("error", (err) => {
    console.error("JSON Parsing Error:", err);
  });
}

syncDatabase().catch(console.error);
