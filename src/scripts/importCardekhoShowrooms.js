import fs from "fs";
import path from "path";
import crypto from "crypto";
import dotenv from "dotenv";
import xlsx from "xlsx";
import connectDB from "../config/db.js";
import Showroom from "../models/Showroom.js";

dotenv.config();

const cleanText = (value) =>
  String(value ?? "")
    .replace(/\s+/g, " ")
    .trim();

const stripHtml = (value) => cleanText(String(value ?? "").replace(/<[^>]*>/g, " "));

const normalizeName = (name) =>
  cleanText(name)
    .replace(/\s*-\s*showroom$/i, "")
    .trim();

const canonicalBrandKey = (value) => {
  const raw = cleanText(value).toLowerCase().replace(/[_-]+/g, " ");
  if (!raw) return "";
  const compact = raw.replace(/[^a-z0-9]/g, "");

  if (["maruti", "marutisuzuki", "suzuki", "msil"].includes(compact)) return "maruti";
  if (["mercedes", "mercedesbenz", "benz", "mercedesbenzcars"].includes(compact)) return "mercedes-benz";
  if (["bmw", "bmwindia"].includes(compact)) return "bmw";
  if (["landrover", "jaguarlandrover", "jlr"].includes(compact)) return "land-rover";
  if (["volkswagen", "vw"].includes(compact)) return "volkswagen";
  if (["mahindra", "mahindramahindra"].includes(compact)) return "mahindra";
  if (["mg", "morrisgarages", "morrisgarage"].includes(compact)) return "mg";
  if (["tata", "tatamotors"].includes(compact)) return "tata";
  if (["hyundai"].includes(compact)) return "hyundai";
  if (["kia"].includes(compact)) return "kia";
  if (["honda"].includes(compact)) return "honda";
  if (["toyota", "toyotakirloskar"].includes(compact)) return "toyota";
  if (["renault"].includes(compact)) return "renault";
  if (["nissan"].includes(compact)) return "nissan";
  if (["skoda"].includes(compact)) return "skoda";
  if (["audi"].includes(compact)) return "audi";
  if (["jeep"].includes(compact)) return "jeep";
  if (["isuzu"].includes(compact)) return "isuzu";
  if (["citroen"].includes(compact)) return "citroen";
  if (["byd"].includes(compact)) return "byd";
  if (["force", "forcemotors"].includes(compact)) return "force";
  if (["jaguar"].includes(compact)) return "jaguar";
  if (["astonmartin"].includes(compact)) return "aston-martin";
  if (["bentley"].includes(compact)) return "bentley";

  return raw;
};

const extractCity = (address) => {
  const input = stripHtml(address);
  if (!input) return "Unknown";

  const beforeDash = input.split(" - ")[0].trim();
  const parts = beforeDash.split(",").map((p) => cleanText(p)).filter(Boolean);
  const candidate = parts.length ? parts[parts.length - 1] : "";
  if (!candidate) return "Unknown";
  if (/\d/.test(candidate)) return "Unknown";
  if (!/^[a-zA-Z .&'-]{2,}$/.test(candidate)) return "Unknown";
  return candidate
    .toLowerCase()
    .replace(/\b\w/g, (ch) => ch.toUpperCase());
};

const extractMobile = (text) => {
  const input = String(text || "");
  const match = input.match(/(?:\+91[\s-]?)?([6-9]\d{9})/);
  return match ? match[1] : "";
};

const syntheticMobile = (seed) => {
  const hash = crypto.createHash("md5").update(seed).digest("hex");
  const n = BigInt(`0x${hash.slice(0, 12)}`) % 1000000000n;
  return `9${String(n).padStart(9, "0")}`;
};

const showroomIdFor = (seed) => {
  const hash = crypto.createHash("sha1").update(seed).digest("hex").slice(0, 10).toUpperCase();
  return `SH-IND-${hash}`;
};

const parseArgs = (argv) => {
  const out = {};
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--file" && argv[i + 1]) out.file = argv[++i];
    else if (arg === "--sheet" && argv[i + 1]) out.sheet = argv[++i];
  }
  return out;
};

async function run() {
  const { file, sheet } = parseArgs(process.argv.slice(2));
  if (!file) {
    throw new Error(
      "Usage: node src/scripts/importCardekhoShowrooms.js --file /abs/path/showrooms.csv [--sheet Sheet1]",
    );
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

  let skipped = 0;
  const dedup = new Map();

  rows.forEach((row) => {
    const brand = cleanText(row["Brand Name"] || row.brand || row.Brand || "");
    const brandKey = canonicalBrandKey(brand);
    const rawName = row["Showroom Name"] || row.showroomName || row.name || "";
    const name = normalizeName(rawName);
    const rawAddress = row.Address || row.address || "";
    const address = stripHtml(rawAddress);
    const city = extractCity(address);

    if (!name || name.length < 3) {
      skipped += 1;
      return;
    }

    if (/st\.json|meta property|<meta/i.test(String(rawAddress || ""))) {
      skipped += 1;
      return;
    }

    const key = `${name.toLowerCase()}|${city.toLowerCase()}`;
    const mobile = extractMobile(rawAddress) || syntheticMobile(`${key}|${address}`);
    const showroomId = showroomIdFor(`${key}|${address}`);

    if (!dedup.has(key)) {
      dedup.set(key, {
        showroomId,
        name,
        businessName: name,
        mobile,
        address: address || "Unknown",
        city: city || "Unknown",
        status: "Active",
        businessType: "Dealership",
        brands: [],
        brandKeys: [],
      });
    }

    if (brand) {
      const item = dedup.get(key);
      if (!item.brands.find((b) => b.toLowerCase() === brand.toLowerCase())) {
        item.brands.push(brand);
      }
      if (brandKey && !item.brandKeys.includes(brandKey)) {
        item.brandKeys.push(brandKey);
      }
    }
  });

  const docs = [...dedup.values()];
  console.log(`Rows read: ${rows.length}`);
  console.log(`Prepared unique showroom records: ${docs.length} | skipped rows: ${skipped}`);

  if (!docs.length) {
    console.log("No showroom records to import.");
    process.exit(0);
  }

  const operations = docs.map((doc) => ({
    updateOne: {
      filter: { name: doc.name, city: doc.city },
      update: {
        $set: {
          businessName: doc.businessName,
          mobile: doc.mobile,
          address: doc.address,
          city: doc.city,
          status: "Active",
          businessType: doc.businessType,
        },
        $setOnInsert: {
          showroomId: doc.showroomId,
        },
        $addToSet: {
          brands: { $each: doc.brands || [] },
          brandKeys: { $each: doc.brandKeys || [] },
        },
      },
      upsert: true,
    },
  }));

  const result = await Showroom.bulkWrite(operations, { ordered: false });
  console.log("Import complete.");
  console.log({
    matched: result.matchedCount || 0,
    modified: result.modifiedCount || 0,
    upserted: result.upsertedCount || 0,
  });
  process.exit(0);
}

run().catch((error) => {
  console.error("Showroom import failed:", error.message);
  process.exit(1);
});
