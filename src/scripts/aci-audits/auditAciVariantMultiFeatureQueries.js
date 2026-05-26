import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const hasText = (value = "", part = "") => clean(value).includes(clean(part));

const isAvailableFeatureValue = (feature = {}) => {
  if (!feature || typeof feature !== "object") return false;
  if (feature.available === true) return true;

  const status = clean(feature.availabilityStatus || "");
  if (["available", "yes", "standard"].includes(status)) return true;

  const value = clean(feature.value || "");
  if (!value) return false;

  return !["not available", "no", "na", "n a", "n/a"].includes(value);
};

const collectResponseText = (response = {}) =>
  [
    response.title,
    response.answer,
    response.feature,
    ...(Array.isArray(response.features) ? response.features.map((item) => JSON.stringify(item)) : []),
    ...(Array.isArray(response.rows) ? response.rows.map((row) => JSON.stringify(row)) : []),
    ...(Array.isArray(response.items) ? response.items.map((item) => JSON.stringify(item)) : []),
    response.data ? JSON.stringify(response.data) : "",
    response.widget ? JSON.stringify(response.widget) : "",
  ]
    .filter(Boolean)
    .join(" ");

const pickVariantCase = async () => {
  const db = mongoose.connection.db;

  const rows = await db
    .collection("vehicle_variant_feature_matrix_v2")
    .find(
      {
        modelKey: "punch",
        $or: [
          { activeForFeatureExplorer: { $exists: false } },
          { activeForFeatureExplorer: { $ne: false } },
        ],
        "featuresByKey.sunroof": { $exists: true },
        "featuresByKey.adas_package": { $exists: true },
      },
      {
        projection: {
          brand: 1,
          make: 1,
          model: 1,
          modelKey: 1,
          variant: 1,
          variantKey: 1,
          variantFull: 1,
          featuresByKey: 1,
          activeForFeatureExplorer: 1,
        },
        limit: 100,
      },
    )
    .toArray();

  const picked =
    rows.find(
      (row) =>
        row.variant &&
        isAvailableFeatureValue(row.featuresByKey?.sunroof) &&
        isAvailableFeatureValue(row.featuresByKey?.adas_package),
    ) || rows.find((row) => row.variant);

  if (!picked) {
    throw new Error("Could not find a DB-backed Tata Punch variant with sunroof + ADAS feature keys");
  }

  return {
    id: "punch-variant-sunroof-and-adas",
    message: `Does Tata Punch ${picked.variant} have sunroof and ADAS?`,
    expectMake: picked.make || picked.brand || "Tata",
    expectModel: picked.model || "Punch",
    expectVariant: picked.variant,
    requiredFeatureKeyParts: ["sunroof", "adas"],
    maxFeatureCount: 3,
    maxTotalVariantsPerFeature: 1,
  };
};

const runCase = async (testCase) => {
  const startedAt = Date.now();

  let response = null;
  let error = "";

  try {
    response = await chatWithAgent({
      message: testCase.message,
      context: {},
      conversationId: `audit-variant-multi-feature-${testCase.id}`,
      userId: "audit",
    });
  } catch (err) {
    error = err?.stack || err?.message || String(err);
  }

  const failures = [];
  const patch = response?.contextPatch || {};
  const selectedVehicle = patch.selectedVehicle || {};
  const responseText = collectResponseText(response);

  const rawReturnedFeatures = [
    ...(Array.isArray(response?.features) ? response.features : []),
    ...(Array.isArray(response?.rows) ? response.rows : []),
    ...(Array.isArray(response?.items) ? response.items : []),
  ];

  const returnedFeatureMap = new Map();

  for (const item of rawReturnedFeatures) {
    const key = item?.featureKey || item?.canonicalKey || item?.displayName || item?.feature || "";
    if (!key || returnedFeatureMap.has(key)) continue;
    returnedFeatureMap.set(key, item);
  }

  const returnedFeatures = [...returnedFeatureMap.values()];
  const uniqueReturnedFeatureKeys = [...returnedFeatureMap.keys()];

  if (error) failures.push(`chatWithAgent threw: ${error}`);

  if (response?.intent !== "vehicle_multi_feature_answer") {
    failures.push(`Expected vehicle_multi_feature_answer, got "${response?.intent || ""}"`);
  }

  if (clean(patch.anchorMake) !== clean(testCase.expectMake)) {
    failures.push(`Expected anchorMake "${testCase.expectMake}", got "${patch.anchorMake || ""}"`);
  }

  if (clean(patch.anchorModel) !== clean(testCase.expectModel)) {
    failures.push(`Expected anchorModel "${testCase.expectModel}", got "${patch.anchorModel || ""}"`);
  }

  const variantBag = [
    patch.anchorVariant,
    selectedVehicle.variant,
    selectedVehicle.variantName,
    selectedVehicle.selectedVariant,
  ].join(" ");

  if (!hasText(variantBag, testCase.expectVariant)) {
    failures.push(`Expected selected variant context to include "${testCase.expectVariant}", got "${variantBag}"`);
  }

  for (const requiredPart of testCase.requiredFeatureKeyParts || []) {
    if (!uniqueReturnedFeatureKeys.some((key) => clean(key).includes(clean(requiredPart)))) {
      failures.push(
        `Expected returned feature keys to include "${requiredPart}", got ${uniqueReturnedFeatureKeys.join(", ")}`,
      );
    }
  }

  if (
    Number(testCase.maxFeatureCount || 0) > 0 &&
    uniqueReturnedFeatureKeys.length > testCase.maxFeatureCount
  ) {
    failures.push(
      `Expected at most ${testCase.maxFeatureCount} returned features, got ${uniqueReturnedFeatureKeys.length}: ${uniqueReturnedFeatureKeys.join(", ")}`,
    );
  }

  for (const feature of returnedFeatures) {
    if (
      Number(testCase.maxTotalVariantsPerFeature || 0) > 0 &&
      Number(feature?.totalVariants || 0) > testCase.maxTotalVariantsPerFeature
    ) {
      failures.push(
        `Expected variant-scoped feature "${feature.featureKey || feature.displayName || ""}" to check at most ${testCase.maxTotalVariantsPerFeature} variant, got totalVariants=${feature.totalVariants}`,
      );
    }
  }

  if (!hasText(responseText, "sunroof")) failures.push("Expected response to mention sunroof");
  if (!hasText(responseText, "adas")) failures.push("Expected response to mention ADAS");

  return {
    id: testCase.id,
    message: testCase.message,
    pass: failures.length === 0,
    durationMs: Date.now() - startedAt,
    failures,
    summary: {
      intent: response?.intent || "",
      title: response?.title || "",
      answer: response?.answer || "",
      anchorMake: patch.anchorMake || "",
      anchorModel: patch.anchorModel || "",
      anchorVariant: patch.anchorVariant || "",
      selectedVehicle: {
        make: selectedVehicle.make || "",
        model: selectedVehicle.model || "",
        variant: selectedVehicle.variant || "",
        variantName: selectedVehicle.variantName || "",
        selectedVariant: selectedVehicle.selectedVariant || "",
      },
      featureKeys: uniqueReturnedFeatureKeys,
      features: returnedFeatures.map((item) => ({
        featureKey: item.featureKey || "",
        displayName: item.displayName || item.feature || "",
        status: item.status || "",
        totalVariants: item.totalVariants || 0,
        availableCount: item.availableCount || 0,
      })),
      sourceTransparency: response?.sourceTransparency || {},
      runtimeResultsMeta: response?.runtimeResultsMeta || [],
    },
  };
};

const main = async () => {
  await connectDB();

  const cases = [await pickVariantCase()];
  const results = [];

  for (const testCase of cases) {
    results.push(await runCase(testCase));
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI variant multi-feature query audit",
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failed.length) process.exit(1);
};

main().catch(async (err) => {
  console.error(err?.stack || err?.message || err);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
