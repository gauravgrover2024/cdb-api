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

const tests = [
  {
    label: "Explorer - Creta active only",
    run: () => getModelFeatureExplorerV2({ model: "Creta" }),
  },
  {
    label: "Explorer - Creta safety active only",
    run: () => getModelFeatureExplorerV2({ model: "Creta", groupKey: "safety" }),
  },
  {
    label: "Explorer - Verna active only should not include old SX",
    run: () => getModelFeatureExplorerV2({ model: "Verna" }),
  },
  {
    label: "Explorer - Verna old SX should be hidden",
    run: () => getModelFeatureExplorerV2({ model: "Verna", variant: "SX" }),
  },
  {
    label: "Answer - Creta sunroof",
    run: () => answerModelFeatureV2({ model: "Creta", feature: "sunroof" }),
  },
  {
    label: "Answer - Creta EX(O) sunroof",
    run: () =>
      answerModelFeatureV2({
        model: "Creta",
        variant: "EX (O)",
        feature: "sunroof",
      }),
  },
  {
    label: "Discovery - Creta variants with sunroof",
    run: () => discoverFeatureVariantsV2({ model: "Creta", feature: "sunroof" }),
  },
  {
    label: "Cheapest - Creta sunroof",
    run: () =>
      discoverFeatureVariantsV2({
        model: "Creta",
        feature: "sunroof",
        cheapestOnly: true,
      }),
  },
  {
    label: "Missing - Creta sunroof",
    run: () =>
      discoverFeatureVariantsV2({
        model: "Creta",
        feature: "sunroof",
        includeMissing: true,
      }),
  },
  {
    label: "Answer - Verna old SX sunroof should be hidden",
    run: () =>
      answerModelFeatureV2({
        model: "Verna",
        variant: "SX",
        feature: "sunroof",
      }),
  },
  {
    label: "Answer - Verna HX4 sunroof",
    run: () =>
      answerModelFeatureV2({
        model: "Verna",
        variant: "HX4",
        feature: "sunroof",
      }),
  },
  {
    label: "Answer - Creta ADAS",
    run: () => answerModelFeatureV2({ model: "Creta", feature: "ADAS" }),
  },
  {
    label: "Answer - Creta 6 airbags",
    run: () => answerModelFeatureV2({ model: "Creta", feature: "6 airbags" }),
  },
  {
    label: "Compare - Creta E vs EX Diesel should align to E Diesel vs EX Diesel",
    run: () =>
      compareVariantFeaturesV2({
        model: "Creta",
        variants: ["E", "EX Diesel"],
      }),
  },
  {
    label: "Compare - Creta E Petrol vs EX Diesel should not align fuels",
    run: () =>
      compareVariantFeaturesV2({
        model: "Creta",
        variants: ["E Petrol", "EX Diesel"],
      }),
  },
  {
    label: "Compare - Creta E vs EX no fuel should stay base petrol/common trims",
    run: () =>
      compareVariantFeaturesV2({
        model: "Creta",
        variants: ["E", "EX"],
      }),
  },
];

const summarize = (result) => {
  const variants = result.data?.variants || [];
  const rows = result.data?.rows || [];
  const features = result.data?.features || [];
  const differenceRows = result.data?.differenceRows || [];

  return {
    ok: result.ok,
    intent: result.intent,
    canvasType: result.canvasType || "",
    inlineType: result.inlineType || "",
    reason: result.reason || "",
    title: result.title || "",
    answer: result.answer || "",
    model: result.data?.model,
    selectedVariant: result.data?.selectedVariant,
    variantCount: variants.length,
    featureCount: features.length,
    rowCount: rows.length || differenceRows.length,
    stats: result.data?.stats || {},
    requestedVariants: result.data?.requestedVariants || [],
    variantResolution: result.data?.variantResolution || [],
    sharedFuelContext: result.data?.sharedFuelContext || "",
    firstVariants: variants.slice(0, 8).map((v) => ({
      variant: v.variant,
      lifecycle: v.lifecycleStatus,
      active: v.activeForFeatureExplorer,
      price: v.priceLabel,
    })),
    firstRows:
      result.intent === "vehicle_feature_comparison"
        ? (differenceRows.length ? differenceRows : rows).slice(0, 8).map((r) => ({
            featureKey: r.featureKey,
            feature: r.displayName,
            group: r.groupLabel,
            values: r.values,
          }))
        : rows.length
          ? rows.slice(0, 8).map((r) => ({
              variant: r.variant,
              feature: r.feature,
              value: r.value,
              available: r.available,
              price: r.priceLabel,
            }))
          : features.slice(0, 8).map((f) => ({
              featureKey: f.featureKey,
              feature: f.displayName,
              group: f.groupLabel,
              availableCount: f.availableCount,
            })),
  };
};

const main = async () => {
  await mongoose.connect(mongoUri);

  for (const test of tests) {
    console.log(`\n================ ${test.label} ================`);
    const result = await test.run();
    console.dir(summarize(result), { depth: 8 });
  }

  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
