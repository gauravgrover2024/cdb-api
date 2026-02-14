// migration/migrate_loans_v2_test.js
process.env.MONGODB_URI =
  "mongodb+srv://Vercel-Admin-cdb:m7UR55exPG0pkXpe@cdb.h7adfv5.mongodb.net/?retryWrites=true&w=majority";

import dotenv from "dotenv";
dotenv.config();

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import readline from "readline";
import { getDb } from "../_db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---- FILE PATHS ----
const CPV_FILE = path.join(__dirname, "cpv_data.json");
const BANK_FILE = path.join(__dirname, "bank_data.json");
const CUSTOMER_ACCOUNT_FILE = path.join(
  __dirname,
  "MIL2.RC_CUSTOMER_ACCOUNT.json",
);
const INSTRUMENT_FILE = path.join(__dirname, "MIL2.RC_INSTRUMENT_DETAIL.json");
const RC_INV_STATUS_FILE = path.join(__dirname, "MIL2.RC_RC_INV_STATUS.json");
const AUTH_SIGNATORY_FILE = path.join(__dirname, "MIL2.AUTH_SIGNATORY.json");
const GURANTOR_FILE = path.join(__dirname, "MIL2.GURANTOR.json");

// ---- TEST CASES (only these 5) ----
const TEST_CASES = [
  { cpvAccountNo: 3208, cdbAccountNo: "3000004015" },
  { cpvAccountNo: 3219, cdbAccountNo: "3000004026" },
  { cpvAccountNo: 3557, cdbAccountNo: "3000004372" },
  { cpvAccountNo: 3585, cdbAccountNo: "3000004400" },
  { cpvAccountNo: 3573, cdbAccountNo: "3000004388" },
];

// ----------------- helpers -----------------

function normalizeString(str) {
  if (!str) return "";
  return String(str).trim();
}

function toInt(x) {
  const n = parseInt(x, 10);
  return Number.isNaN(n) ? null : n;
}

function toFloat(x) {
  const n = parseFloat(x);
  return Number.isNaN(n) ? null : n;
}

function safeDate(str) {
  if (!str) return null;
  const s = String(str).trim();
  if (!s) return null;
  const part = s.slice(0, 19); // "YYYY-MM-DD HH:MM:SS"
  const d = new Date(part);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function mapGender(code) {
  const c = (code || "").trim().toUpperCase();
  if (c === "M") return "Male";
  if (c === "F") return "Female";
  return null;
}

function mapMarital(code) {
  const c = (code || "").trim().toUpperCase();
  if (c === "M") return "Married";
  if (c === "U") return "Unmarried";
  return null;
}

function yearsAtResidence(val) {
  const s = (val == null ? "" : String(val)).trim().toUpperCase();
  if (!s) return null;
  if (s === "BB" || s.includes("BIRTH")) return 99;
  const n = parseInt(s, 10);
  return Number.isNaN(n) ? null : n;
}

function deriveStateFromPincode(pin) {
  // stub – OK to return null, we decided state is auto from pin later
  return null;
}

function deriveRegdCity(regNo, resiCity) {
  if (regNo) return resiCity || null;
  return resiCity || null;
}

function parseMakeModel(makeModel) {
  const mm = (makeModel || "").trim();
  if (!mm) return { make: null, model: null, variant: null };
  const parts = mm.split(/\s+/);
  const make = parts[0] || null;
  const model = parts[1] || null;
  const variant = parts.length > 2 ? parts.slice(2).join(" ") : null;
  return { make, model, variant };
}

function showProgress(current, total) {
  const width = 30;
  const ratio = total ? current / total : 1;
  const filled = Math.round(ratio * width);
  const bar =
    "[" +
    "#".repeat(filled) +
    "-".repeat(width - filled) +
    `] ${current}/${total}`;
  readline.clearLine(process.stdout, 0);
  readline.cursorTo(process.stdout, 0);
  process.stdout.write(bar);
}

// ----------------- build loan doc -----------------

function buildLoanDoc({
  cpvNo,
  cdbNo,
  cpv,
  cust,
  bank,
  instrList,
  rcList,
  auth,
  gur,
}) {
  const nowIso = new Date().toISOString();

  // ---- lead ----
  const leadId = cdbNo || String(cpvNo);
  const cpvDate = safeDate(cpv?.CPVDATE || cpv?.CPV_DATE);
  const caseMonth = normalizeString(cust?.CASE_FOR_YEAR_MONTH);
  const leadDate =
    cpvDate ||
    (caseMonth ? new Date(caseMonth + "01T00:00:00").toISOString() : nowIso);

  const source = normalizeString(cust?.SOURCE);
  const dealtBy = normalizeString(cust?.DEALT_BY);
  const dealerName = normalizeString(cust?.PAYMENT_FAVOURING_AT_BOOKING);

  const lead = {
    leadId,
    leadDate,
    leadType: "Reference",
    leadSource: "Reference",
    source,
    dealtBy,
    salesExecutive: dealtBy,
    dealerName,
    sourcingChannel: normalizeString(cust?.CHANNEL_CODE),
    dsaId: normalizeString(cust?.DSA_CODE),
  };

  // ---- vehicle ----
  const makeModel =
    normalizeString(cust?.MAKE_MODEL) ||
    normalizeString(cust?.DELIVERED_MAKE_MODEL);
  const { make, model, variant } = parseMakeModel(makeModel);
  const financer =
    normalizeString(cust?.HP_TO) || normalizeString(cpv?.FINANCER);
  const isFinanced = financer === "CASH SALE" ? "No" : "Yes";

  const vehicle = {
    make,
    model,
    variant,
    isFinanced,
    fuel: null,
    transmission: null,
    color: null,
    manufacturingYear:
      normalizeString(cust?.MFG_YEAR) ||
      normalizeString(cust?.DELIVERED_MFG_YEAR),
    registrationNumber: normalizeString(cust?.REGISTRATION_NUMBER),
    chassisNumber: normalizeString(cust?.CHASIS_NUMBER),
    engineNumber: normalizeString(cust?.ENGINE_NUMBER),
    policyType: null,
    insuranceExpiry: null,
  };

  // ---- finance ----
  const typeOfLoan =
    normalizeString(cust?.CASE_TYPE) || normalizeString(cpv?.LOANTYPE);
  const finance = {
    typeOfLoan,
    financeExpectation: toFloat(cpv?.LOAN_EXPECTED),
    loanTenureMonths: toInt(cust?.TENOR),
    loanAmount: toFloat(cust?.LOAN_AMOUNT),
    interestRate: toFloat(cust?.ROI),
  };

  // ---- applicant ----
  const residencePincode = normalizeString(cpv?.RESI_PIN);
  const residenceCity = normalizeString(cpv?.RESI_CITY);
  const state = deriveStateFromPincode(residencePincode);

  const father =
    [
      normalizeString(cpv?.FATHERS_NAME_FIRST),
      normalizeString(cpv?.FATHERS_NAME_MIDDLE),
      normalizeString(cpv?.FATHERS_NAME_LAST),
    ]
      .filter(Boolean)
      .join(" ")
      .trim() || null;

  const applicant = {
    // name: CPV first, fallback to CUSTOMER_ACCOUNT
    name:
      normalizeString(cpv?.CUSTOMER_NAME) || normalizeString(cust?.CUST_NAME),

    // phones: CPV first, then CUSTOMER_ACCOUNT
    primaryMobile:
      normalizeString(cpv?.RESI_PHONE1) ||
      normalizeString(cpv?.MOBILE) ||
      normalizeString(cpv?.OFF_PHONE1) ||
      normalizeString(cust?.MOBILE_NUMBER) ||
      normalizeString(cust?.PHONE_NUMBERS_RESI) ||
      normalizeString(cust?.PHONE_NUMBERS_OFFICE) ||
      "",

    // email from CPV
    email: normalizeString(cpv?.EMAIL_ADDRESS) || normalizeString(cpv?.E_MAIL),

    fatherName: father,
    motherName: normalizeString(cpv?.MOTHERS_MAIDEN_NAME),
    dob: safeDate(cpv?.DATE_OF_BIRTH),
    gender: mapGender(cpv?.SEX),
    maritalStatus: mapMarital(cpv?.MARITAL_STATUS),
    dependents: toInt(cpv?.NO_OF_DEPENDANTS),
    education: normalizeString(cpv?.EDUCATION),

    residenceAddress:
      [normalizeString(cpv?.RESI_ADD1), normalizeString(cpv?.RESI_ADD2)]
        .filter(Boolean)
        .join(" ") || null,

    pincode: residencePincode,
    city: residenceCity,
    state,
    yearsInCurrentHouse: yearsAtResidence(cpv?.YEARS_AT_RESIDENCE),

    permanentAddress: normalizeString(cpv?.PERMANENT_ADDRESS) || null,
    permanentPincode: null,
    permanentCity: null,
  };

  // ---- applicant employment ----
  const applicantEmployment = {
    occupationType: normalizeString(cpv?.PROFESSION_TYPE),
    companyName: normalizeString(cpv?.OFF_NAME),
    companyType:
      normalizeString(cpv?.ORGANISATION_TYPE) || normalizeString(cpv?.CATEGORY),
    businessNature:
      normalizeString(cpv?.INDUSTRY_DETAIL) || normalizeString(cpv?.CATEGORY),
    currentExp: toInt(cpv?.YEAR_AT_PROFESSION),
    employmentAddress:
      [normalizeString(cpv?.OFF_ADD1), normalizeString(cpv?.OFF_ADD2)]
        .filter(Boolean)
        .join(" ") || null,
    employmentPincode: normalizeString(cpv?.OFF_PIN),
    employmentCity: normalizeString(cpv?.OFF_CITY),
    employmentPhone: normalizeString(cpv?.OFF_PHONE1),
  };

  // ---- applicant income ----
  const annualIncome = toFloat(cpv?.ANNUAL_INCOME);
  const applicantIncome = {
    annualIncome,
    monthlyIncome: annualIncome ? annualIncome / 12 : null,
  };

  // ---- applicant bank ----
  const sb = normalizeString(bank?.SB_ACCOUNT_NO);
  const ca = normalizeString(bank?.CA_ACCOUNT_NO);
  const accountNumber = sb || ca || null;
  let accountType = null;
  if (sb) accountType = "Savings";
  else if (ca) accountType = "Current";

  const applicantBank = {
    bankName: normalizeString(bank?.BANK_NAME),
    accountNumber,
    accountType,
    branch: normalizeString(bank?.BANK_ADDRESS),
    ifscCode: null,
    openedIn: null,
  };

  // ---- references ----
  const ref1 = {
    name: normalizeString(cpv?.REF1_NAME),
    mobile: normalizeString(cpv?.REF1_PHONE),
    address: normalizeString(cpv?.REF1_ADD),
    relation: normalizeString(cpv?.REF1_RELATION),
  };
  const ref2 = {
    name: normalizeString(cpv?.REF2_NAME),
    mobile: normalizeString(cpv?.REF2_PHONE),
    address: normalizeString(cpv?.REF2_ADD),
    relation: normalizeString(cpv?.REF2_RELATION),
  };
  const references = [];
  if (Object.values(ref1).some(Boolean)) references.push(ref1);
  if (Object.values(ref2).some(Boolean)) references.push(ref2);

  // ---- kyc ----
  const kyc = {
    aadhaarNumber: normalizeString(cpv?.AADHAAR_NUMBER),
    panNumber: normalizeString(cpv?.PAN_NUMBER),
    passportNumber: normalizeString(cpv?.PASSPORT_NUMBER),
    dlNumber: normalizeString(cpv?.DRIVING_LICENSE),
    gstNumber: null,
    voterId: null,
  };

  // ---- co-applicant (from GURANTOR) ----
  let coApplicant;
  if (gur) {
    const coAnnual = toFloat(gur.ANNUAL_INCOME);
    coApplicant = {
      hasCoApplicant: true,
      name: normalizeString(gur.NAME),
      dob: safeDate(gur.DATE_OF_BIRTH),
      gender: mapGender(gur.SEX),
      maritalStatus: mapMarital(gur.MARITAL_STATUS),
      dependents: toInt(gur.NO_OF_DEPEND),
      education: normalizeString(gur.EDUCATION),
      house: normalizeString(gur.RESIDENCE_TYPE),
      mobile: normalizeString(gur.MOBILE) || normalizeString(gur.RESI_PHONE),
      address:
        [normalizeString(gur.RESI_ADD1), normalizeString(gur.RESI_ADD2)]
          .filter(Boolean)
          .join(" ") || null,
      pincode: normalizeString(gur.RESI_PIN),
      city: normalizeString(gur.RESI_CITY),
      occupationType: normalizeString(gur.PROFESSION_TYPE),
      companyName: normalizeString(gur.OFF_NAME),
      companyAddress:
        [normalizeString(gur.OFF_ADD1), normalizeString(gur.OFF_ADD2)]
          .filter(Boolean)
          .join(" ") || null,
      companyPincode: normalizeString(gur.OFF_PIN),
      companyCity: normalizeString(gur.OFF_CITY),
      companyPhone: normalizeString(gur.OFF_PHONE),
      annualIncome: coAnnual,
      aadhaarNumber: normalizeString(gur.G_AADHAAR_NUMBER),
    };
  } else {
    coApplicant = { hasCoApplicant: false };
  }

  // ---- authorised signatory ----
  let authorisedSignatory = null;
  if (auth) {
    authorisedSignatory = {
      name: normalizeString(auth.NAME),
      address:
        [normalizeString(auth.ADD1), normalizeString(auth.ADD2)]
          .filter(Boolean)
          .join(" ") || null,
      city: normalizeString(auth.CITY),
      pincode: normalizeString(auth.PIN),
      mobile: normalizeString(auth.MOBILE) || normalizeString(auth.PHONE),
      dob: safeDate(auth.DATE_OF_BIRTH),
      designation: normalizeString(auth.DESIGNATION),
      aadhaarNumber: normalizeString(auth.AUTH_AADHAAR_NUMBER),
    };
  }

  // ---- approval ----
  const appliedLoan =
    toFloat(cust?.APPLIED_LOAN_AMOUNT) || toFloat(cust?.LOAN_AMOUNT);
  const appliedRoi = toFloat(cust?.APPLIED_ROI) || toFloat(cust?.ROI);
  const appliedTenor = toInt(cust?.APPLIED_TENOR) || toInt(cust?.TENOR);

  const approval = {
    bank: {
      name: financer,
      status: normalizeString(cust?.STATUS) || normalizeString(cust?.FILE_AT),
      loanAmountApproved: appliedLoan,
      roi: appliedRoi,
      tenureMonths: appliedTenor,
      processingFees: 0,
      approvalDate: safeDate(cust?.DATE_OF_APPROVAL_STATUS_TAKEN),
    },
    breakup: {
      netLoanApproved: toFloat(cust?.LOAN_AMOUNT),
      creditAssured: toFloat(cust?.ICICI_CREDIT_ASSURED),
      insuranceFinance: toFloat(cust?.INSURANCE_FINANCED),
      ewFinance: 0,
    },
  };

  // ---- disbursement ----
  const disbAmount =
    toFloat(cust?.LOAN_RECEIVED_AMOUNT) || toFloat(cust?.LOAN_AMOUNT);
  const disbDate = safeDate(cust?.DATE_OF_DISBURSE);
  const disbStatus = disbDate ? "Disbursed" : "Pending";

  const disbursement = {
    status: disbStatus,
    bankName: financer,
    amount: disbAmount,
    date: disbDate,
    remarks: normalizeString(cust?.REMARKS) || "Migrated from legacy",
    payoutApplicable: "No",
    payoutPercentage: 0,
    payoutAmount: 0,
  };

  // ---- post-file instruments ----
  const cheques = [];
  let ecsObj = null;
  let siObj = null;
  for (const inst of instrList) {
    const itype = normalizeString(inst.INSTRMNT_TYPE);
    if (itype === "Cheque") {
      cheques.push({
        type: "Cheque",
        number: normalizeString(inst.INSTRMNT_NO),
        drawnOn: normalizeString(inst.DRAWN_ON),
        accountNumber: normalizeString(inst.ACCOUNT_NUMBER),
        date: safeDate(inst.INSTRMNT_DATE),
        amount: toFloat(inst.INSTRMNT_AMOUNT),
        favouring: normalizeString(inst.INSTRMNT_FAVOURING),
        status: normalizeString(inst.STATUS),
      });
    } else if (itype === "ECS") {
      ecsObj = {
        bank: normalizeString(inst.DRAWN_ON),
        accountNumber: normalizeString(inst.ACCOUNT_NUMBER),
        micr: normalizeString(inst.MICR_CODE),
        amount: toFloat(inst.INSTRMNT_AMOUNT),
      };
    } else if (itype === "SI") {
      siObj = {
        bank: normalizeString(inst.DRAWN_ON),
        accountNumber: normalizeString(inst.ACCOUNT_NUMBER),
        amount: toFloat(inst.INSTRMNT_AMOUNT),
      };
    }
  }

  const regdCity = deriveRegdCity(
    normalizeString(cust?.REGISTRATION_NUMBER),
    residenceCity,
  );

  const postFile = {
    sameAsApproved: true,
    bankName: financer,
    regdCity,
    loanAmountApproved: appliedLoan,
    loanAmountDisbursed: disbAmount,
    roi: appliedRoi,
    roiType: normalizeString(cust?.ROI_TYPE),
    tenureMonths: appliedTenor,
    processingFees: 0,
    emiAmount: toFloat(cust?.APPLIED_EMI),
    firstEmiDate: safeDate(cust?.EMI_DUE_DATE),
    disbursedBreakup: {
      loan: appliedLoan,
      creditAssured: toFloat(cust?.ICICI_CREDIT_ASSURED),
      insurance: toFloat(cust?.INSURANCE_FINANCED),
      ew: 0,
    },
    instruments: {
      cheques,
      ecs: ecsObj,
      si: siObj,
    },
    dispatch: {
      date: safeDate(
        cust?.DATE_WHEN_FILE_DESPATCH || cust?.DATE_OF_DESPATCH_FOR_APPROVAL,
      ),
      time:
        normalizeString(
          cust?.TIME_WHEN_FILE_DESPATCH || cust?.TIME_OF_DESPATCH_FOR_APPROVAL,
        ) || null,
    },
    loanNumber: normalizeString(cust?.LOAN_NUMBER_SUFFIX),
  };

  // ---- delivery ----
  const delivery = {
    date: safeDate(cust?.DATE_OF_DELIVERY),
    dealerName,
    dealerContactPerson: normalizeString(cust?.PERSON_WHO_DELIVERED_THE_CAR),
    insuranceBy: normalizeString(cust?.INSURANCE_BY),
    insuranceCompany: normalizeString(cust?.INSURANCE_COMPANY),
    insurancePolicyNumber: normalizeString(cust?.INSURANCE_COVERNOTE_NUMBER),
    invoice: {
      number: normalizeString(cust?.INVOICE_NUMBER),
      date: safeDate(cust?.INVOICE_DATE),
      receivedAs: null,
      receivedFrom: null,
      receivedDate: null,
    },
    rc: {
      registrationNumber: normalizeString(cust?.REGISTRATION_NUMBER),
      chassisNumber: normalizeString(cust?.CHASIS_NUMBER),
      engineNumber: normalizeString(cust?.ENGINE_NUMBER),
      registrationDate: null,
      receivedAs: null,
      receivedFrom: null,
      receivedDate: null,
    },
  };

  // ---- payout ----
  const payout = {
    payoutApplicable: "No",
    payoutPercentage: 0,
    payoutAmount: 0,
    loanReceivables: [],
    loanPayables: [],
    billNumber: null,
    billDate: null,
  };

  return {
    cpvAccountNo: cpvNo,
    tempCustCode: cdbNo,
    lead,
    vehicle,
    finance,
    applicant,
    applicantEmployment,
    applicantIncome,
    applicantBank,
    references,
    kyc,
    coApplicant,
    authorisedSignatory,
    approval,
    disbursement,
    postFile,
    delivery,
    payout,
    migratedAt: nowIso,
    _migrationSource: "loan_migrate_v2_test",
  };
}

// ----------------- main -----------------

async function run() {
  // Check files
  for (const f of [
    CPV_FILE,
    BANK_FILE,
    CUSTOMER_ACCOUNT_FILE,
    INSTRUMENT_FILE,
    RC_INV_STATUS_FILE,
    AUTH_SIGNATORY_FILE,
    GURANTOR_FILE,
  ]) {
    if (!fs.existsSync(f)) {
      throw new Error(`Missing JSON file: ${f}`);
    }
  }

  console.log("Loading JSON files...");
  const cpvData = JSON.parse(fs.readFileSync(CPV_FILE, "utf8"));
  const bankData = JSON.parse(fs.readFileSync(BANK_FILE, "utf8"));
  const custData = JSON.parse(fs.readFileSync(CUSTOMER_ACCOUNT_FILE, "utf8"));
  const instrData = JSON.parse(fs.readFileSync(INSTRUMENT_FILE, "utf8"));
  const rcInvData = JSON.parse(fs.readFileSync(RC_INV_STATUS_FILE, "utf8"));
  const authData = JSON.parse(fs.readFileSync(AUTH_SIGNATORY_FILE, "utf8"));
  const gurData = JSON.parse(fs.readFileSync(GURANTOR_FILE, "utf8"));

  // Index CPV by CPV_ACCOUNT_NO / CDB_ACCOUNT_NO
  const cpvByCpv = new Map();
  const cpvByCdb = new Map();
  for (const row of cpvData) {
    const cpvNo = toInt(row.CPV_ACCOUNT_NO);
    const cdb = normalizeString(row.CDB_ACCOUNT_NO);
    if (cpvNo) cpvByCpv.set(cpvNo, row);
    if (cdb) cpvByCdb.set(cdb, row);
  }

  // Bank by CPV_ACCOUNT_NO
  const bankByCpv = new Map();
  for (const row of bankData) {
    const cpvNo = toInt(row.CPV_ACCOUNT_NO);
    if (cpvNo && !bankByCpv.has(cpvNo)) {
      bankByCpv.set(cpvNo, row);
    }
  }

  // Customer account by CPV / CDB
  const custByCpv = new Map();
  const custByCdb = new Map();
  for (const row of custData) {
    const cpvNo = toInt(row.CPV_ACCOUNT_NO);
    const cdb = normalizeString(row.TEMP_CUST_CODE);
    if (cpvNo && !custByCpv.has(cpvNo)) custByCpv.set(cpvNo, row);
    if (cdb && !custByCdb.has(cdb)) custByCdb.set(cdb, row);
  }

  // Instruments by TEMP_CUST_CODE
  const instrByTemp = new Map();
  for (const row of instrData) {
    const temp = normalizeString(row.TEMP_CUST_CODE);
    if (!temp) continue;
    if (!instrByTemp.has(temp)) instrByTemp.set(temp, []);
    instrByTemp.get(temp).push(row);
  }

  // RC/Invoice by TEMP_CUST_CODE
  const rcInvByTemp = new Map();
  for (const row of rcInvData) {
    const temp = normalizeString(row.TEMP_CUST_CODE);
    if (!temp) continue;
    if (!rcInvByTemp.has(temp)) rcInvByTemp.set(temp, []);
    rcInvByTemp.get(temp).push(row);
  }

  // Auth signatory by CPV
  const authByCpv = new Map();
  for (const row of authData) {
    const cpvNo = toInt(row.CPV_ACCOUNT_NO);
    if (cpvNo && !authByCpv.has(cpvNo)) authByCpv.set(cpvNo, row);
  }

  // Guarantor by CPV
  const gurByCpv = new Map();
  for (const row of gurData) {
    const cpvNo = toInt(row.CPV_ACCOUNT_NO);
    if (cpvNo && !gurByCpv.has(cpvNo)) gurByCpv.set(cpvNo, row);
  }

  console.log("Connecting to MongoDB...");
  const db = await getDb();
  const loansCol = db.collection("loans");

  let processed = 0;
  let inserted = 0;
  let failed = 0;
  const total = TEST_CASES.length;

  console.log("Migrating 5 test loans...");
  for (const test of TEST_CASES) {
    processed += 1;
    showProgress(processed, total);

    const { cpvAccountNo: cpvNo, cdbAccountNo: cdbNo } = test;

    try {
      const cpv = cpvByCpv.get(cpvNo) || cpvByCdb.get(cdbNo) || null;
      const cust = custByCpv.get(cpvNo) || custByCdb.get(cdbNo) || null;
      const bank = bankByCpv.get(cpvNo) || null;
      const instrList = instrByTemp.get(cdbNo) || [];
      const rcList = rcInvByTemp.get(cdbNo) || [];
      const auth = authByCpv.get(cpvNo) || null;
      const gur = gurByCpv.get(cpvNo) || null;

      if (!cpv || !cust) {
        console.warn(
          `\n[WARN] Missing CPV or CUSTOMER_ACCOUNT for CPV ${cpvNo}, CDB ${cdbNo}`,
        );
        failed += 1;
        continue;
      }

      const doc = buildLoanDoc({
        cpvNo,
        cdbNo,
        cpv,
        cust,
        bank,
        instrList,
        rcList,
        auth,
        gur,
      });

      // Upsert by cpvAccountNo + tempCustCode
      const filter = {
        cpvAccountNo: cpvNo,
        tempCustCode: cdbNo,
      };

      const res = await loansCol.findOneAndUpdate(
        filter,
        {
          $set: doc,
        },
        { upsert: true, returnDocument: "after" },
      );

      if (res.lastErrorObject?.upserted) {
        inserted += 1;
        console.log(`\n[OK] Inserted loan CPV ${cpvNo}, CDB ${cdbNo}`);
      } else {
        console.log(`\n[OK] Updated loan CPV ${cpvNo}, CDB ${cdbNo}`);
      }
    } catch (err) {
      failed += 1;
      console.error(
        `\n[ERROR] Failed CPV ${cpvNo}, CDB ${cdbNo}:`,
        err.message,
      );
    }
  }

  showProgress(total, total);
  console.log(
    `\nDone. Processed=${processed}, Inserted=${inserted}, Failed=${failed}`,
  );
}

run().catch((err) => {
  console.error("Fatal migration error:", err);
  process.exit(1);
});
