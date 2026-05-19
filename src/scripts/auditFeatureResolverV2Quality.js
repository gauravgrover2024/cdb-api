import dotenv from "dotenv";
import mongoose from "mongoose";
import {
  getModelFeatureExplorerV2,
  answerModelFeatureV2,
  discoverFeatureVariantsV2,
  compareVariantFeaturesV2,
} from "../services/aiAgent/aiAgent.featureResolverV2.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const failures = [];

const assert = (condition, label, details = {}) => {
  if (!condition) {
    failures.push({ label, details });
    console.error("❌", label, details);
  } else {
    console.log("✅", label);
  }
};

const variantNames = (rows = []) => rows.map((row) => row.variant);
const valueKey = (row) =>
  `${row.available}|${row.availabilityStatus}|${row.value}`;

const hasInternalCopy = (text = "") =>
  /backend|functionality|not available in current|feature records|database|resolver|tool/i.test(
    text,
  );

const main = async () => {
  await mongoose.connect(mongoUri);

  console.log("\n=== 1. ACTIVE LIFECYCLE GATES ===");
  const cretaExplorer = await getModelFeatureExplorerV2({ model: "Creta" });
  assert(cretaExplorer.ok, "Creta explorer returns ok");
  assert(
    cretaExplorer.data.variants.every((v) => v.activeForFeatureExplorer === true),
    "Creta explorer has only active variants",
    cretaExplorer.data.variants.filter((v) => !v.activeForFeatureExplorer),
  );
  assert(
    cretaExplorer.data.variants.length === 49,
    "Creta active variant count is 49",
    { count: cretaExplorer.data.variants.length },
  );

  assert(
    cretaExplorer.data.features.every((feature) => feature.availableCount > 0),
    "Default Creta explorer hides features unavailable on all current variants",
    cretaExplorer.data.features.filter((feature) => feature.availableCount === 0).slice(0, 10),
  );

  const vernaExplorer = await getModelFeatureExplorerV2({ model: "Verna" });
  assert(vernaExplorer.ok, "Verna explorer returns ok");
  assert(
    vernaExplorer.data.variants.every((v) => v.activeForFeatureExplorer === true),
    "Verna explorer has only active variants",
    vernaExplorer.data.variants.filter((v) => !v.activeForFeatureExplorer),
  );
  assert(
    !variantNames(vernaExplorer.data.variants).some((name) =>
      ["EX", "S", "SX", "SX IVT", "SX Opt"].includes(name),
    ),
    "Old Verna variants are hidden from active explorer",
    variantNames(vernaExplorer.data.variants),
  );

  console.log("\n=== 2. INACTIVE VARIANT GATES ===");
  const oldSx = await answerModelFeatureV2({
    model: "Verna",
    variant: "SX",
    feature: "sunroof",
  });
  assert(
    oldSx.data.stats.totalRows === 0,
    "Old Verna SX direct answer returns zero active rows",
    oldSx,
  );
  assert(
    /(older .*variant|current new-car option|current variant)/i.test(oldSx.answer),
    "Old Verna SX answer clearly says inactive/hidden in customer language",
    oldSx.answer,
  );

  const oldSxExplorer = await getModelFeatureExplorerV2({
    model: "Verna",
    variant: "SX",
  });

  assert(
    !/feature explorer|not an active new-car variant/i.test(oldSxExplorer.answer || ""),
    "Old Verna SX explorer copy is customer-friendly",
    oldSxExplorer.answer,
  );

  console.log("\n=== 3. FEATURE ANSWER SEMANTIC GATES ===");
  const cretaSunroof = await answerModelFeatureV2({
    model: "Creta",
    feature: "sunroof",
  });
  assert(cretaSunroof.ok, "Creta sunroof answer ok");
  assert(
    cretaSunroof.data.stats.totalRows === 49,
    "Creta sunroof checks all 49 active variants",
    cretaSunroof.data.stats,
  );
  assert(
    cretaSunroof.data.stats.availableRows === 43,
    "Creta sunroof available count is 43",
    cretaSunroof.data.stats,
  );
  assert(
    cretaSunroof.data.cheapestAvailableVariant?.variant === "EX (O)",
    "Cheapest active Creta sunroof variant is EX (O)",
    cretaSunroof.data.cheapestAvailableVariant,
  );
  assert(
    !hasInternalCopy(cretaSunroof.answer),
    "Creta sunroof answer has no internal/backend copy",
    cretaSunroof.answer,
  );

  const exoSunroof = await answerModelFeatureV2({
    model: "Creta",
    variant: "EX (O)",
    feature: "sunroof",
  });
  const exoDistinctValues = new Set(exoSunroof.data.rows.map(valueKey));
  assert(
    exoSunroof.data.rows.length > 1,
    "Creta EX(O) query is correctly recognized as variant-family match",
    exoSunroof.data.rows.map((r) => r.variant),
  );
  assert(
    exoDistinctValues.size === 1
      ? /all|matching|subvariant|family/i.test(exoSunroof.answer)
      : /varies|choose|exact/i.test(exoSunroof.answer),
    "Variant-family answer wording is safe",
    {
      answer: exoSunroof.answer,
      rows: exoSunroof.data.rows.map((r) => ({
        variant: r.variant,
        value: r.value,
        available: r.available,
      })),
    },
  );

  console.log("\n=== 4. DISCOVERY GATES ===");
  const cheapestSunroof = await discoverFeatureVariantsV2({
    model: "Creta",
    feature: "sunroof",
    cheapestOnly: true,
  });
  assert(
    cheapestSunroof.data.rows[0]?.variant === "EX (O)",
    "Cheapest sunroof discovery returns EX (O)",
    cheapestSunroof.data.rows[0],
  );

  console.log("\n=== 5. COMPARISON VARIANT INTENT GATES ===");
  const cretaAdasCopy = await answerModelFeatureV2({
    model: "Creta",
    feature: "ADAS",
  });

  assert(
    /ADAS/.test(cretaAdasCopy.answer || "") && !/aDAS/.test(cretaAdasCopy.answer || ""),
    "ADAS answer casing is customer-ready",
    cretaAdasCopy.answer,
  );

  const eVsExDiesel = await compareVariantFeaturesV2({
    model: "Creta",
    variants: ["E", "EX Diesel"],
  });
  assert(eVsExDiesel.ok, "Creta E vs EX Diesel comparison ok");
  assert(
    eVsExDiesel.data.variants.map((v) => v.variant).join(" | ") ===
      "E Diesel | EX Diesel",
    "E vs EX Diesel resolves to E Diesel vs EX Diesel",
    {
      requested: eVsExDiesel.data.requestedVariants,
      resolved: eVsExDiesel.data.variants.map((v) => v.variant),
      resolution: eVsExDiesel.data.variantResolution,
    },
  );

  const ePetrolVsExDiesel = await compareVariantFeaturesV2({
    model: "Creta",
    variants: ["E Petrol", "EX Diesel"],
  });
  assert(
    ePetrolVsExDiesel.data.variants.map((v) => v.variant).join(" | ") ===
      "E | EX Diesel",
    "Explicit E Petrol vs EX Diesel does not force diesel alignment",
    {
      requested: ePetrolVsExDiesel.data.requestedVariants,
      resolved: ePetrolVsExDiesel.data.variants.map((v) => v.variant),
      resolution: ePetrolVsExDiesel.data.variantResolution,
    },
  );

  const eVsEx = await compareVariantFeaturesV2({
    model: "Creta",
    variants: ["E", "EX"],
  });
  assert(
    eVsEx.data.variants.map((v) => v.variant).join(" | ") === "E | EX",
    "E vs EX without fuel stays E vs EX",
    {
      requested: eVsEx.data.requestedVariants,
      resolved: eVsEx.data.variants.map((v) => v.variant),
      resolution: eVsEx.data.variantResolution,
    },
  );

  assert(
    new Set(eVsExDiesel.data.rows.map((row) => row.featureKey)).size ===
      eVsExDiesel.data.rows.length,
    "Comparison has one row per featureKey",
  );

  assert(
    eVsExDiesel.data.rows.every((row) =>
      eVsExDiesel.data.variants.every((variant) => row.values[variant.variantKey]),
    ),
    "Every comparison row has values for every compared variant",
  );

  console.log("\n=== 6. CUSTOMER COPY QUALITY GATES ===");
  const copySamples = [
    oldSx,
    cretaSunroof,
    exoSunroof,
    cheapestSunroof,
    eVsExDiesel,
    ePetrolVsExDiesel,
    eVsEx,
  ];

  const badCopyPattern =
    /backend|database|feature records|resolver|tool|functionality|I found \d+ rows|not available in current ACI Assist backend/i;

  for (const sample of copySamples) {
    assert(
      !badCopyPattern.test(sample.answer || ""),
      `Customer copy is clean: ${sample.intent || sample.title || "sample"}`,
      sample.answer,
    );
  }

  console.log("\n=== FINAL RESULT ===");
  if (failures.length) {
    console.error(`FAILED: ${failures.length} semantic quality gate(s).`);
    console.dir(failures, { depth: 10 });
    await mongoose.disconnect();
    process.exit(1);
  }

  console.log("PASSED: Feature Resolver V2 semantic quality gates are clean.");
  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
