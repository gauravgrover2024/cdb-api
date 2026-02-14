process.env.MONGODB_URI =
  "mongodb+srv://Vercel-Admin-cdb:m7UR55exPG0pkXpe@cdb.h7adfv5.mongodb.net/?retryWrites=true&w=majority";

// migration/migrate_v2.js
import dotenv from "dotenv";
dotenv.config();

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { getDb } from "../_db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const CPV_FILE = path.join(__dirname, "cpv_data.json");
const BANK_FILE = path.join(__dirname, "bank_data.json");
const INS_FILE = path.join(__dirname, "insurance_data.json");

// ---------- helpers: normalization & similarity ----------

function normalizeString(str) {
  if (!str) return "";
  return String(str)
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "")
    .trim();
}

function normalizeMobile(mobile) {
  if (!mobile) return null;
  const digits = String(mobile).replace(/\D/g, "");
  if (!digits) return null;
  return digits.length >= 10 ? digits.slice(-10) : null;
}

function formatDob(dateStr) {
  if (!dateStr) return null;
  const s = String(dateStr).trim();
  // Accept ISO-like or "YYYY-MM-DD hh:mm:ss"
  const part = s.split(" ")[0];
  if (/^\d{4}-\d{2}-\d{2}$/.test(part)) return part;
  return null;
}

function cleanEmail(email) {
  if (!email) return null;
  const s = String(email).trim().toLowerCase();
  if (!s || s === "na" || s === "n/a") return null;
  return s;
}

function cleanNomineeDob(raw) {
  if (!raw) return { dob: null, age: null };
  const s = String(raw).trim().toUpperCase();
  if (!s || s === "NA" || s === "N/A") return { dob: null, age: null };
  // Age patterns: "40", "40YR", "40YRS"
  const ageMatch = s.match(/^(\d+)(?:\s*YRS?)?$/);
  if (ageMatch) return { dob: null, age: parseInt(ageMatch[1], 10) };
  if (/^\d{4}-\d{2}-\d{2}(?:\s+\d{2}:\d{2}:\d{2})?$/.test(s)) {
    return { dob: s.split(" ")[0], age: null };
  }
  return { dob: null, age: null };
}

// prefix-based similarity
function similarity(a, b) {
  a = normalizeString(a);
  b = normalizeString(b);
  if (!a || !b) return 0;
  let matches = 0;
  for (let i = 0; i < Math.min(a.length, b.length); i++) {
    if (a[i] === b[i]) matches++;
    else break;
  }
  return (matches / Math.max(a.length, b.length)) * 100;
}

// ---------- pre-index insurance by mobile ----------

function indexInsuranceByMobile(insuranceData) {
  const map = new Map();
  for (const ins of insuranceData) {
    const mob = normalizeMobile(
      ins.PHONE_NUMBER_1 || ins.PHONE_NUMBER_2 || ins.PHONE_NUMBER3,
    );
    if (!mob) continue;
    if (!map.has(mob)) map.set(mob, []);
    map.get(mob).push(ins);
  }
  // sort each list by DATE_OF_ISSUE desc
  for (const [mob, list] of map.entries()) {
    list.sort((a, b) => new Date(b.DATE_OF_ISSUE) - new Date(a.DATE_OF_ISSUE));
  }
  return map;
}

function extractNomineeFromIndexed(customer, insByMobile) {
  const mobiles = [
    normalizeMobile(customer.RESI_PHONE1),
    normalizeMobile(customer.RESI_PHONE2),
    normalizeMobile(customer.RESI_PHONE3),
    normalizeMobile(customer.MOBILE),
  ].filter(Boolean);

  const name = customer.CUSTOMER_NAME || "";
  let candidates = [];

  for (const m of mobiles) {
    const list = insByMobile.get(m);
    if (!list) continue;
    candidates.push(...list);
  }

  if (!candidates.length) {
    return {
      nomineeName: null,
      nomineeRelation: null,
      nomineeDob: null,
      nomineeAge: null,
      matchedCount: 0,
    };
  }

  // Filter by name similarity (but a bit relaxed, e.g. 75)
  candidates = candidates.filter((ins) => {
    if (!ins.CUSTOMER_NAME) return false;
    return similarity(name, ins.CUSTOMER_NAME) >= 75;
  });

  if (!candidates.length) {
    return {
      nomineeName: null,
      nomineeRelation: null,
      nomineeDob: null,
      nomineeAge: null,
      matchedCount: 0,
    };
  }

  // Already sorted by DATE_OF_ISSUE desc in index
  const latest = candidates[0];
  const { dob, age } = cleanNomineeDob(latest.NOMINEE_AGE_DOB);

  return {
    nomineeName: latest.NOMINEE_NAME || null,
    nomineeRelation: latest.NOMINEE_RELATION || null,
    nomineeDob: dob,
    nomineeAge: age,
    matchedCount: candidates.length,
  };
}

// ---------- group CPV by customer (PAN/mobile) ----------

function groupCpvByCustomer(cpvData) {
  const groups = new Map(); // key -> { rows: [] }

  for (const row of cpvData) {
    const pan = row.PAN_NUMBER && String(row.PAN_NUMBER).trim();
    const mobile = normalizeMobile(row.RESI_PHONE1 || row.MOBILE);
    let key;

    if (pan) key = `PAN:${pan}`;
    else if (mobile) key = `MOB:${mobile}`;
    else key = `ROW:${row.CPV_ACCOUNT_NO}`; // fallback, unique per row

    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(row);
  }

  return groups;
}

function pickLatestByCpvDate(rows) {
  return rows.reduce((best, cur) => {
    if (!best) return cur;
    const dBest = new Date(best.CPV_DATE || 0);
    const dCur = new Date(cur.CPV_DATE || 0);
    return dCur > dBest ? cur : best;
  }, null);
}

// ---------- build final doc from master row + bank + nominee ----------

function buildDoc(master, groupRows, bankRow, nominee) {
  const createdDate = master.CPV_DATE ? new Date(master.CPV_DATE) : new Date();

  const primaryMobile =
    normalizeMobile(master.RESI_PHONE1) ||
    normalizeMobile(master.MOBILE) ||
    null;

  const extraSet = new Set(
    [
      normalizeMobile(master.RESI_PHONE2),
      normalizeMobile(master.RESI_PHONE3),
      normalizeMobile(master.MOBILE),
    ].filter(Boolean),
  );
  extraSet.delete(primaryMobile);

  const email = cleanEmail(master.EMAIL_ADDRESS || master.E_MAIL) || null;

  // Business nature
  const businessNature = master.INDUSTRY_DETAIL
    ? [String(master.INDUSTRY_DETAIL).trim()]
    : [];

  // Incorporation: derive start year if YEAR_AT_PROFESSION is years
  let incorporationYear = null;
  if (master.YEAR_AT_PROFESSION) {
    const yrs = parseInt(master.YEAR_AT_PROFESSION, 10);
    if (!Number.isNaN(yrs) && yrs >= 0 && yrs <= 80) {
      incorporationYear = String(new Date().getFullYear() - yrs);
    }
  }

  const houseType =
    master.RESIDENCE_TYPE && master.RESIDENCE_TYPE.toLowerCase().includes("own")
      ? "owned"
      : master.RESIDENCE_TYPE
        ? "rented"
        : null;

  const maritalStatus =
    master.MARITAL_STATUS === "M"
      ? "married"
      : master.MARITAL_STATUS
        ? "unmarried"
        : null;

  const nomineeFields = {
    nomineeName: nominee.nomineeName || null,
    nomineeDob: nominee.nomineeDob || null,
    nomineeRelation: nominee.nomineeRelation || null,
    nomineeAge: nominee.nomineeAge || null,
  };

  const doc = {
    customerType: "Migrated",
    kycStatus: "Complete",
    createdOn: createdDate.toLocaleDateString("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
    }),
    createdAt: createdDate.toISOString(),
    updatedAt: new Date().toISOString(),

    customerName: master.CUSTOMER_NAME || null,
    sdwOf:
      [
        master.FATHERS_NAME_FIRST,
        master.FATHERS_NAME_MIDDLE,
        master.FATHERS_NAME_LAST,
      ]
        .filter(Boolean)
        .join(" ") || null,
    gender: master.SEX === "F" ? "female" : master.SEX === "M" ? "male" : null,
    dob: formatDob(master.DATE_OF_BIRTH),
    motherName: master.MOTHERS_MAIDEN_NAME || master.MOTHER_NAME || null,

    residenceAddress:
      [master.RESI_ADD1, master.RESI_ADD2].filter(Boolean).join(" ") || null,
    pincode: master.RESI_PIN || null,
    city: master.RESI_CITY || null,
    yearsInCurrentHouse: master.YEARS_AT_RESIDENCE || null,
    houseType,

    education: master.EDUCATION ? String(master.EDUCATION).toLowerCase() : null,
    maritalStatus,
    dependents:
      master.NO_OF_DEPENDANTS !== undefined && master.NO_OF_DEPENDANTS !== null
        ? String(master.NO_OF_DEPENDANTS)
        : null,

    primaryMobile,
    extraMobiles: [...extraSet],
    email,

    ...nomineeFields,

    occupationType: master.PROFESSION_TYPE || null,
    companyName: master.OFF_NAME || null,
    companyType: master.CATEGORY || null,
    businessNature,
    incorporationYear,
    designation: null,
    employmentAddress:
      [master.OFF_ADD1, master.OFF_ADD2].filter(Boolean).join(" ") || null,
    employmentPincode: master.OFF_PIN || null,
    employmentCity: master.OFF_CITY || null,
    employmentPhone: normalizeMobile(master.OFF_PHONE1) || null,

    panNumber: master.PAN_NUMBER || null,
    itrYears: master.ANNUAL_INCOME || null,

    bankName: bankRow ? bankRow.BANK_NAME : null,
    accountNumber: bankRow
      ? bankRow.SB_ACCOUNT_NO || bankRow.CA_ACCOUNT_NO || null
      : null,
    ifsc: bankRow ? bankRow.IFSC || null : null,
    branch: bankRow ? bankRow.BANK_ADDRESS || bankRow.BRANCH || null : null,
    accountSinceYears: null,
    accountType: bankRow
      ? bankRow.SB_ACCOUNT_NO
        ? "Savings"
        : bankRow.CA_ACCOUNT_NO
          ? "Current"
          : null
      : null,

    aadhaarNumber: master.AADHAAR_NUMBER || null,
    passportNumber: master.PASSPORT_NUMBER || null,
    gstNumber: master.GST_NUMBER || master.GSTNumber || null,
    dlNumber: master.DRIVING_LICENSE || null,

    reference1: {
      name: master.REF1_NAME || null,
      mobile: normalizeMobile(master.REF1_PHONE) || null,
      address: master.REF1_ADD || null,
      pincode: null,
      city: null,
    },
    reference2: {
      name: master.REF2_NAME || null,
      mobile: normalizeMobile(master.REF2_PHONE) || null,
      address: master.REF2_ADD || null,
      pincode: null,
      city: null,
    },

    _migration: {
      sourceFile: "cpv_data.json",
      cpvAccountNo: master.CPV_ACCOUNT_NO || null,
      originalCpvDate: master.CPV_DATE || null,
      cpvHistory: groupRows.map((r) => ({
        cpvAccountNo: r.CPV_ACCOUNT_NO,
        cpvDate: r.CPV_DATE,
      })),
      nomineeMatches: nominee.matchedCount || 0,
      migratedAt: new Date().toISOString(),
    },
  };

  return doc;
}

// ---------- main run ----------

async function run() {
  if (!fs.existsSync(CPV_FILE))
    throw new Error(`Missing CPV file: ${CPV_FILE}`);
  if (!fs.existsSync(BANK_FILE))
    throw new Error(`Missing BANK file: ${BANK_FILE}`);
  if (!fs.existsSync(INS_FILE))
    throw new Error(`Missing INS file: ${INS_FILE}`);

  const cpvData = JSON.parse(fs.readFileSync(CPV_FILE, "utf8"));
  const bankData = JSON.parse(fs.readFileSync(BANK_FILE, "utf8"));
  const insData = JSON.parse(fs.readFileSync(INS_FILE, "utf8"));

  const bankByCpv = new Map();
  for (const b of bankData) {
    bankByCpv.set(b.CPV_ACCOUNT_NO, b);
  }

  const insByMobile = indexInsuranceByMobile(insData);

  const groups = groupCpvByCustomer(cpvData);
  const db = await getDb();
  const customersCol = db.collection("customers");

  let inserted = 0;
  let updated = 0;

  for (const [, rows] of groups.entries()) {
    const master = pickLatestByCpvDate(rows);
    const bankRow = bankByCpv.get(master.CPV_ACCOUNT_NO) || null;
    const nominee = extractNomineeFromIndexed(master, insByMobile);

    const doc = buildDoc(master, rows, bankRow, nominee);

    const filter = doc.panNumber
      ? { panNumber: doc.panNumber }
      : doc.primaryMobile
        ? { primaryMobile: doc.primaryMobile }
        : { customerName: doc.customerName, pincode: doc.pincode };

    const { createdAt, createdOn, ...rest } = doc;
    const update = {
      $set: { ...rest, updatedAt: new Date().toISOString() },
      $setOnInsert: { createdAt, createdOn },
    };

    const res = await customersCol.findOneAndUpdate(filter, update, {
      upsert: true,
      returnDocument: "after",
    });

    if (res.lastErrorObject && res.lastErrorObject.upserted) inserted++;
    else updated++;
  }

  console.log({ inserted, updated });
}

run().catch((err) => {
  console.error("Fatal migration error:", err);
  process.exit(1);
});
