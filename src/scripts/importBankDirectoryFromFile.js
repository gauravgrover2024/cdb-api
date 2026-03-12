import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import xlsx from "xlsx";
import connectDB from "../config/db.js";
import BankDirectory from "../models/BankDirectory.js";

dotenv.config();

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

const normalizeHeader = (value) =>
  String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");

const cleanText = (value) => {
  const text = String(value ?? "").trim();
  if (!text) return "";
  if (text.toLowerCase() === "nan" || text.toLowerCase() === "null") return "";
  return text;
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

const inferBankName = ({ ifsc, micr, bankName }) => {
  const explicit = cleanText(bankName);
  if (explicit) return explicit;
  const normalizedIfsc = normalizeIfsc(ifsc);
  if (/^[A-Z]{4}0[A-Z0-9]{6}$/.test(normalizedIfsc)) {
    const byIfsc = IFSC_CODE_BANK_MAP[normalizedIfsc.slice(0, 4)];
    if (byIfsc) return byIfsc;
  }
  const normalizedMicr = normalizeMicr(micr);
  if (normalizedMicr.length === 9) {
    const byMicr = MICR_BANK_CODE_MAP[normalizedMicr.slice(3, 6)];
    if (byMicr) return byMicr;
  }
  return "";
};

const pickByAliases = (row, aliases) => {
  const keys = Object.keys(row || {});
  const aliasSet = new Set(aliases.map(normalizeHeader));
  for (const key of keys) {
    if (aliasSet.has(normalizeHeader(key))) {
      return row[key];
    }
  }
  return "";
};

const pruneUndefined = (obj) =>
  Object.fromEntries(
    Object.entries(obj).filter(([, value]) => value !== undefined && value !== null && value !== ""),
  );

const parseArgs = (argv) => {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const a = argv[i];
    if (a === "--file" && argv[i + 1]) out.file = argv[++i];
    else if (a === "--sheet" && argv[i + 1]) out.sheet = argv[++i];
    else if (a === "--source" && argv[i + 1]) out.source = argv[++i];
  }
  return out;
};

async function run() {
  const { file, sheet, source } = parseArgs(process.argv.slice(2));
  if (!file) {
    throw new Error("Usage: node src/scripts/importBankDirectoryFromFile.js --file /abs/path/file.csv [--sheet Sheet1] [--source latest-bank-csv]");
  }

  const abs = path.resolve(file);
  if (!fs.existsSync(abs)) {
    throw new Error(`File not found: ${abs}`);
  }

  await connectDB();
  console.log(`Reading file: ${abs}`);

  const workbook = xlsx.readFile(abs, { cellDates: false, raw: false });
  const chosenSheet = sheet || workbook.SheetNames[0];
  if (!chosenSheet || !workbook.SheetNames.includes(chosenSheet)) {
    throw new Error(`Sheet not found. Available: ${workbook.SheetNames.join(", ")}`);
  }

  const ws = workbook.Sheets[chosenSheet];
  const rows = xlsx.utils.sheet_to_json(ws, {
    defval: "",
    raw: false,
    blankrows: false,
  });

  console.log(`Rows read: ${rows.length}`);

  const aliases = {
    ifsc: ["ifsc", "ifsc_code", "ifsccode", "ifsc code"],
    micr: ["micr", "micr_code", "micrcode", "micr code"],
    bankName: ["bankname", "bank_name", "bank", "bank name", "banknamefull"],
    branch: ["branch", "branchname", "branch_name", "branch name"],
  };

  const deduped = new Map();
  let skipped = 0;

  rows.forEach((row, index) => {
    const rawIfsc = pickByAliases(row, aliases.ifsc);
    const rawMicr = pickByAliases(row, aliases.micr);
    const rawBankName = pickByAliases(row, aliases.bankName);
    const rawBranch = pickByAliases(row, aliases.branch);

    const ifsc = normalizeIfsc(rawIfsc);
    const micr = normalizeMicr(rawMicr);
    const bankName = inferBankName({ ifsc, micr, bankName: rawBankName });
    const branchName = cleanText(rawBranch);

    if (!ifsc && !micr) {
      skipped += 1;
      return;
    }

    const key = ifsc ? `ifsc:${ifsc}` : `micr:${micr}:${normalizeHeader(bankName)}`;
    deduped.set(key, {
      ifsc: ifsc || undefined,
      micr: micr || undefined,
      bankName: bankName || undefined,
      branch: branchName || undefined,
      active: true,
      source: cleanText(source) || "latest-bank-file",
      lastVerifiedAt: new Date(),
      raw: pruneUndefined({
        rowIndex: index + 2,
      }),
    });
  });

  const records = [...deduped.values()];
  console.log(`Valid records (deduped): ${records.length} | skipped: ${skipped}`);

  const dirOps = records.map((r) => ({
    updateOne: {
      filter: r.ifsc
        ? { ifsc: r.ifsc }
        : { micr: r.micr, bankName: r.bankName || inferBankName({ micr: r.micr }) || "Unknown Bank" },
      update: { $set: pruneUndefined(r) },
      upsert: true,
    },
  }));

  let dirResult = null;
  if (dirOps.length) {
    dirResult = await BankDirectory.bulkWrite(dirOps, { ordered: false });
  }

  console.log("Import complete.");
  console.log({
    bankDirectory: dirResult
      ? {
          matched: dirResult.matchedCount || 0,
          modified: dirResult.modifiedCount || 0,
          upserted: dirResult.upsertedCount || 0,
        }
      : { matched: 0, modified: 0, upserted: 0 },
  });

  process.exit(0);
}

run().catch((error) => {
  console.error("Import failed:", error.message);
  process.exit(1);
});
