import dotenv from "dotenv";
import fs from "fs";
import connectDB from "../config/db.js";
import Vehicle from "../models/Vehicle.js";
import VehicleFeature from "../models/VehicleFeature.js";

dotenv.config();

const normalizeBrandForJoin = (rawBrand) => {
  if (!rawBrand) return "";
  let b = String(rawBrand).trim().toLowerCase();

  // "aston-martin" -> "aston martin", "land-rover" -> "land rover"
  b = b.replace(/[-_]+/g, " ");

  // collapse spaces
  b = b.replace(/\s+/g, " ").trim();

  return b;
};

const normalizeModelForJoin = (brand, rawModel) => {
  if (!rawModel) return "";
  let m = String(rawModel).trim().toLowerCase();
  const b = normalizeBrandForJoin(brand);

  // strip brand prefix: "aston martin db12" -> "db12"
  const prefix = b + " ";
  if (m.startsWith(prefix)) {
    m = m.slice(prefix.length);
  }

  // unify separators: "grand-vitara" -> "grand vitara"
  m = m.replace(/[-_]+/g, " ");

  // collapse spaces
  m = m.replace(/\s+/g, " ").trim();

  return m;
};

const normalizeVariantForJoin = (rawVariant) => {
  if (!rawVariant) return "";
  let v = String(rawVariant).trim().toLowerCase();

  // just normalize whitespace so long names line up
  v = v.replace(/\s+/g, " ").trim();

  return v;
};

const run = async () => {
  await connectDB();
  console.log("MongoDB Connected");

  const featureDocs = await VehicleFeature.find({});
  const vehicles = await Vehicle.find({});

  console.log("Total feature docs:", featureDocs.length);
  console.log("Total vehicle docs (all cities):", vehicles.length);

  // build key set from vehicles
  const vehicleKeys = new Set(
    vehicles.map((v) => {
      const brand = normalizeBrandForJoin(v.brand || v.make);
      const model = normalizeModelForJoin(brand, v.model);
      const variant = normalizeVariantForJoin(v.variant);
      return `${brand}|${model}|${variant}`;
    }),
  );

  let matchedCount = 0;
  const missingList = [];

  for (const f of featureDocs) {
    const brand = normalizeBrandForJoin(f.brand);
    const model = normalizeModelForJoin(brand, f.model);
    const variant = normalizeVariantForJoin(f.variant);
    const key = `${brand}|${model}|${variant}`;

    if (vehicleKeys.has(key)) {
      matchedCount += 1;
    } else {
      missingList.push({
        brand: f.brand,
        model: f.model,
        variant: f.variant,
      });
    }
  }

  console.log("Features with matching vehicle:", matchedCount);
  console.log("Features without matching vehicle:", missingList.length);

  console.log("Sample missing (first 20):");
  console.log(JSON.stringify(missingList.slice(0, 20), null, 2));

  try {
    fs.writeFileSync(
      "missing_features.json",
      JSON.stringify(missingList, null, 2),
      "utf8",
    );
    console.log('Full missing list written to "missing_features.json"');
  } catch (e) {
    console.error("Error writing missing_features.json:", e);
  }

  process.exit(0);
};

run().catch((e) => {
  console.error(e);
  process.exit(1);
});
