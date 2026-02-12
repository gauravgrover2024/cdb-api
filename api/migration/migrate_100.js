process.env.MONGODB_URI =
  "mongodb+srv://Vercel-Admin-cdb:m7UR55exPG0pkXpe@cdb.h7adfv5.mongodb.net/?retryWrites=true&w=majority";

import dotenv from "dotenv";
dotenv.config();

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getDb } from "../_db.js";

// Resolve folder path
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// File paths
const CPV_FILE = path.join(__dirname, "cpv_data.json");
const BANK_FILE = path.join(__dirname, "bank_data.json");
const INS_FILE = path.join(__dirname, "insurance_data.json");

function normalizeString(str) {
  if (!str) return "";
  return str
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function normalizeMobile(mobile) {
  if (!mobile) return null;
  const digits = mobile.toString().replace(/\D/g, "");
  return digits.length >= 10 ? digits.slice(-10) : null;
}

function formatDob(dateStr) {
  if (!dateStr) return null;
  return dateStr.split(" ")[0]; // YYYY-MM-DD
}

function formatCreatedOn(dateStr) {
  const date = new Date(dateStr);
  return date.toLocaleDateString("en-GB", {
    day: "2-digit",
    month: "short",
    year: "numeric",
  });
}

function similarity(a, b) {
  a = normalizeString(a);
  b = normalizeString(b);
  if (!a || !b) return 0;
  let matches = 0;
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] === b[i]) matches++;
  }
  return (matches / Math.max(a.length, b.length)) * 100;
}

function extractNominee(customer, insuranceData) {
  const mobiles = [
    normalizeMobile(customer.RESI_PHONE1),
    normalizeMobile(customer.RESI_PHONE2),
    normalizeMobile(customer.RESI_PHONE3),
    normalizeMobile(customer.MOBILE),
  ].filter(Boolean);

  const name = customer.CUSTOMER_NAME;

  const matches = insuranceData.filter((ins) => {
    const insMobile = normalizeMobile(
      ins.PHONE_NUMBER_1 || ins.PHONE_NUMBER_2 || ins.PHONE_NUMBER3,
    );

    if (!mobiles.includes(insMobile)) return false;

    return similarity(name, ins.CUSTOMER_NAME) >= 90;
  });

  if (matches.length === 1) {
    return {
      nomineeName: matches[0].NOMINEE_NAME || null,
      nomineeRelation: matches[0].NOMINEE_RELATION || null,
      nomineeDob: matches[0].NOMINEE_AGE_DOB
        ? formatDob(matches[0].NOMINEE_AGE_DOB)
        : null,
    };
  }

  if (matches.length > 1) {
    matches.sort(
      (a, b) => new Date(b.DATE_OF_ISSUE) - new Date(a.DATE_OF_ISSUE),
    );
    const latest = matches[0];
    return {
      nomineeName: latest.NOMINEE_NAME || null,
      nomineeRelation: latest.NOMINEE_RELATION || null,
      nomineeDob: latest.NOMINEE_AGE_DOB
        ? formatDob(latest.NOMINEE_AGE_DOB)
        : null,
    };
  }

  return { nomineeName: null, nomineeRelation: null, nomineeDob: null };
}

async function run() {
  const cpvData = JSON.parse(fs.readFileSync(CPV_FILE));
  const bankData = JSON.parse(fs.readFileSync(BANK_FILE));
  const insData = JSON.parse(fs.readFileSync(INS_FILE));

  const db = await getDb();
  console.log("Writing into DB:", db.databaseName);

  const customersCol = db.collection("customers");

  let inserted = 0;
  let nullLog = {};
  let preview = [];

  const first100 = cpvData.slice(0, 100);

  for (const customer of first100) {
    const bank = bankData.find(
      (b) => b.CPV_ACCOUNT_NO === customer.CPV_ACCOUNT_NO,
    );

    const nominee = extractNominee(customer, insData);

    const createdDate = new Date(customer.CPV_DATE);

    const doc = {
      customerType: "Migrated",
      kycStatus: "Complete",
      createdOn: formatCreatedOn(customer.CPV_DATE),
      createdAt: createdDate.toISOString(),
      updatedAt: new Date().toISOString(),

      customerName: customer.CUSTOMER_NAME,
      sdwOf: [
        customer.FATHERS_NAME_FIRST,
        customer.FATHERS_NAME_MIDDLE,
        customer.FATHERS_NAME_LAST,
      ]
        .filter(Boolean)
        .join(" "),
      gender: customer.SEX === "F" ? "female" : "male",
      dob: formatDob(customer.DATE_OF_BIRTH),
      motherName: customer.MOTHERS_MAIDEN_NAME || null,
      residenceAddress: [customer.RESI_ADD1, customer.RESI_ADD2]
        .filter(Boolean)
        .join(" "),
      pincode: customer.RESI_PIN || null,
      city: customer.RESI_CITY || null,
      yearsInCurrentHouse: customer.YEARS_AT_RESIDENCE || null,
      houseType: customer.RESIDENCE_TYPE === "Your own" ? "owned" : "rented",
      education: customer.EDUCATION ? customer.EDUCATION.toLowerCase() : null,
      maritalStatus: customer.MARITAL_STATUS === "M" ? "married" : "unmarried",
      dependents:
        customer.NO_OF_DEPENDANTS !== null
          ? String(customer.NO_OF_DEPENDANTS)
          : null,
      primaryMobile: normalizeMobile(customer.RESI_PHONE1),
      extraMobiles: [
        normalizeMobile(customer.RESI_PHONE2),
        normalizeMobile(customer.RESI_PHONE3),
        normalizeMobile(customer.MOBILE),
      ].filter(Boolean),
      email:
        customer.EMAIL_ADDRESS && customer.EMAIL_ADDRESS.toLowerCase() !== "n a"
          ? customer.EMAIL_ADDRESS
          : null,

      nomineeName: nominee.nomineeName,
      nomineeDob: nominee.nomineeDob,
      nomineeRelation: nominee.nomineeRelation,

      occupationType: customer.PROFESSION_TYPE,
      companyName: customer.OFF_NAME,
      companyType: customer.CATEGORY,
      businessNature: customer.INDUSTRY_DETAIL
        ? [customer.INDUSTRY_DETAIL]
        : [],
      incorporationYear: customer.YEAR_AT_PROFESSION
        ? String(
            new Date().getFullYear() - parseInt(customer.YEAR_AT_PROFESSION),
          )
        : null,
      designation: null,
      employmentAddress: [customer.OFF_ADD1, customer.OFF_ADD2]
        .filter(Boolean)
        .join(" "),
      employmentPincode: customer.OFF_PIN,
      employmentCity: customer.OFF_CITY,
      employmentPhone: normalizeMobile(customer.OFF_PHONE1),

      panNumber: customer.PAN_NUMBER,
      itrYears: customer.ANNUAL_INCOME || 0,

      bankName: bank ? bank.BANK_NAME : null,
      accountNumber: bank ? bank.SB_ACCOUNT_NO || bank.CA_ACCOUNT_NO : null,
      ifsc: null,
      branch: bank ? bank.BANK_ADDRESS : null,
      accountSinceYears: null,
      accountType:
        bank && bank.SB_ACCOUNT_NO
          ? "Savings"
          : bank && bank.CA_ACCOUNT_NO
            ? "Current"
            : null,

      aadhaarNumber: customer.AADHAAR_NUMBER,
      passportNumber: customer.PASSPORT_NUMBER,
      gstNumber: null,
      dlNumber: customer.DRIVING_LICENSE,

      reference1: {
        name: customer.REF1_NAME,
        mobile: normalizeMobile(customer.REF1_PHONE),
        address: customer.REF1_ADD,
        pincode: null,
        city: null,
      },
      reference2: {
        name: customer.REF2_NAME,
        mobile: normalizeMobile(customer.REF2_PHONE),
        address: customer.REF2_ADD,
        pincode: null,
        city: null,
      },
    };

    Object.entries(doc).forEach(([key, value]) => {
      if (value === null) {
        nullLog[key] = (nullLog[key] || 0) + 1;
      }
    });

    preview.push(doc);

    await customersCol.insertOne(doc);
    inserted++;
  }

  fs.writeFileSync(
    path.join(__dirname, "preview_output.json"),
    JSON.stringify(preview, null, 2),
  );

  fs.writeFileSync(
    path.join(__dirname, "migration_report.txt"),
    `Inserted: ${inserted}`,
  );

  fs.writeFileSync(
    path.join(__dirname, "null_log.txt"),
    JSON.stringify(nullLog, null, 2),
  );

  console.log("Migration completed.");
}

run();
