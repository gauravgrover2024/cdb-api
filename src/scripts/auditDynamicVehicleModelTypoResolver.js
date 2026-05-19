import dotenv from "dotenv";
import mongoose from "mongoose";
import {
  loadVehicleModelIndex,
  loadVehicleVariantIndexByModelKey,
  resolveVehicleModelFromText,
  resolveVehicleVariantFromText,
} from "../services/aiAgent/aiAgent.vehicleModelResolver.js";

dotenv.config();

const uri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.DATABASE_URL;

if (!uri) {
  console.error("Missing Mongo URI in .env");
  process.exit(1);
}

await mongoose.connect(uri);

const db = mongoose.connection.db;

const modelIndex = await loadVehicleModelIndex({ db, force: true });
const variantIndex = await loadVehicleVariantIndexByModelKey({ db, force: true });

console.log("=== MAKE-AWARE DYNAMIC VEHICLE MODEL INDEX ===");
console.log({
  modelsIndexed: modelIndex.length,
  sample: modelIndex.slice(0, 12).map((row) => ({
    brand: row.brand,
    model: row.model,
    fullModel: row.fullModel,
    modelKey: row.modelKey,
    shortModelKey: row.shortModelKey,
    fullModelKey: row.fullModelKey,
    source: row.source,
  })),
});

const badFunctionRows = modelIndex.filter((row) =>
  /function|return this|constructor|modelDbSymbol/i.test(
    `${row.model} ${row.fullModel} ${row.sourceModel}`,
  ),
);

console.log("\n=== BAD FUNCTION ROWS SHOULD BE 0 ===");
console.log(badFunctionRows.length);

const modelCases = [
  ["cretaa adas", "Creta"],
  ["hyundai cretaa adas", "Creta"],
  ["cretaa me adas hai kya", "Creta"],
  ["cretaaa pricelist", "Creta"],
  ["cretta featuers", "Creta"],
  ["which hyundai cretaa variants have sunroof", "Creta"],

  ["vrna sunroof", "Verna"],
  ["hyundai vrna me adas hai kya", "Verna"],
  ["vernaa six airbags", "Verna"],
  ["which hyundai vernaa variants have 360 camera", "Verna"],

  ["thaar music system", "Thar"],
  ["mahindra thaar music system", "Thar"],

  ["seltoss price", "Seltos"],
  ["kia seltoss price", "Seltos"],
  ["sonett features", "Sonet"],
  ["kia sonett features", "Sonet"],

  ["xuv 700 adas", "XUV700"],
  ["xuv700 features", "XUV700"],
  ["mahindra xuv 700 adas", "XUV700"],
];

const failures = [];

for (const [message, expectedModel] of modelCases) {
  const resolved = await resolveVehicleModelFromText({ db, message });

  const summary = {
    message,
    expectedModel,
    resolvedModel: resolved?.model || "",
    fullModel: resolved?.fullModel || "",
    brand: resolved?.brand || "",
    confidence: resolved?.confidence || 0,
    method: resolved?.method || "",
    matchedText: resolved?.matchedText || "",
    corrected: resolved?.corrected || false,
  };

  console.log("\n===", message, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (resolved?.model !== expectedModel) {
    failures.push(summary);
  }
}

const creta = await resolveVehicleModelFromText({
  db,
  message: "hyundai creta ex diesel sunroof",
});

const variantCases = [
  ["hyundai creta ex diesel sunroof", "EX Diesel"],
  ["creta ex diesel sunroof", "EX Diesel"],
  ["ex diesel sunroof", "EX Diesel"],
  ["hyundai creta king music system", "King"],
  ["creta king ivt features", "King IVT"],
];

console.log("\n=== VARIANT INDEX SAMPLE FOR CRETA ===");
console.log(
  (variantIndex.get(creta?.shortModelKey || "creta") || []).slice(0, 12).map((row) => ({
    variant: row.variant,
    fullVariant: row.fullVariant,
    variantKey: row.variantKey,
  })),
);

for (const [message, expectedVariant] of variantCases) {
  const resolvedVariant = await resolveVehicleVariantFromText({
    db,
    modelKey: creta?.shortModelKey || "creta",
    message,
  });

  const summary = {
    message,
    expectedVariant,
    resolvedVariant: resolvedVariant?.variant || "",
    fullVariant: resolvedVariant?.fullVariant || "",
    confidence: resolvedVariant?.confidence || 0,
    method: resolvedVariant?.method || "",
    matchedText: resolvedVariant?.matchedText || "",
  };

  console.log("\n=== VARIANT:", message, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (resolvedVariant?.variant !== expectedVariant) {
    failures.push(summary);
  }
}

console.log("\n=== FINAL RESULT ===");

await mongoose.disconnect();

if (badFunctionRows.length) {
  failures.push({
    reason: "bad function rows entered model index",
    count: badFunctionRows.length,
  });
}

if (failures.length) {
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: Make-aware DB-backed dynamic model and variant typo resolver is working.");
