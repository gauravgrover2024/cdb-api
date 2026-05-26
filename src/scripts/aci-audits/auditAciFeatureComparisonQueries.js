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

const collectResponseText = (response = {}) =>
  [
    response.title,
    response.answer,
    response.intent,
    response.canvasType,
    response.inlineType,
    ...(Array.isArray(response.features) ? response.features.map((item) => JSON.stringify(item)) : []),
    ...(Array.isArray(response.rows) ? response.rows.map((row) => JSON.stringify(row)) : []),
    ...(Array.isArray(response.items) ? response.items.map((item) => JSON.stringify(item)) : []),
    response.data ? JSON.stringify(response.data) : "",
    response.widget ? JSON.stringify(response.widget) : "",
    response.sourceTransparency ? JSON.stringify(response.sourceTransparency) : "",
  ]
    .filter(Boolean)
    .join(" ");

const cases = [
  {
    id: "punch-vs-nexon-four-feature-comparison",
    message:
      "Compare Tata Punch and Tata Nexon on sunroof, ADAS, 6 airbags and cruise control.",
    expectedModels: ["Punch", "Nexon"],
    requiredFeatureParts: ["sunroof", "adas", "airbag", "cruise"],
    forbiddenVariantParts: [
      "sunroof",
      "adas",
      "airbag",
      "cruise",
      "punch",
      "nexon",
      "tata punch",
      "tata nexon",
    ],
    minRecordCount: 1,
    minComparisonRows: 1,
  },
];

const runCase = async (testCase) => {
  const startedAt = Date.now();

  let response = null;
  let error = "";

  try {
    response = await chatWithAgent({
      message: testCase.message,
      context: {},
      conversationId: `audit-feature-comparison-${testCase.id}`,
      userId: "audit",
    });
  } catch (err) {
    error = err?.stack || err?.message || String(err);
  }

  const failures = [];
  const patch = response?.contextPatch || {};
  const selectedVehicle = patch.selectedVehicle || {};
  const responseText = collectResponseText(response);

  if (error) failures.push(`chatWithAgent threw: ${error}`);

  for (const model of testCase.expectedModels) {
    if (!hasText(responseText, model)) {
      failures.push(`Expected response to mention model "${model}"`);
    }
  }

  for (const feature of testCase.requiredFeatureParts) {
    if (!hasText(responseText, feature)) {
      failures.push(`Expected response to mention feature "${feature}"`);
    }
  }

  const variantBag = [
    patch.anchorVariant,
    selectedVehicle.variant,
    selectedVehicle.variantName,
    selectedVehicle.selectedVariant,
  ].join(" ");

  for (const forbidden of testCase.forbiddenVariantParts) {
    if (hasText(variantBag, forbidden)) {
      failures.push(`Comparison/model/feature text "${forbidden}" leaked into variant context: "${variantBag}"`);
    }
  }

  const recordCount = Number(response?.sourceTransparency?.recordCount || 0);
  const rowsCount = Array.isArray(response?.rows) ? response.rows.length : 0;
  const itemsCount = Array.isArray(response?.items) ? response.items.length : 0;
  const featuresCount = Array.isArray(response?.features) ? response.features.length : 0;
  const comparisonPayloadCount = rowsCount + itemsCount + featuresCount;

  if (Number(testCase.minRecordCount || 0) > 0 && recordCount < testCase.minRecordCount) {
    failures.push(`Expected sourceTransparency.recordCount >= ${testCase.minRecordCount}, got ${recordCount}`);
  }

  if (
    Number(testCase.minComparisonRows || 0) > 0 &&
    comparisonPayloadCount < testCase.minComparisonRows
  ) {
    failures.push(
      `Expected comparison payload rows/items/features >= ${testCase.minComparisonRows}, got rows=${rowsCount}, items=${itemsCount}, features=${featuresCount}`,
    );
  }

  if (hasText(response?.answer || "", "could not confidently")) {
    failures.push(`Response is a failure fallback, not a valid comparison: "${response.answer}"`);
  }

  const hasComparisonIntent =
    hasText(response?.intent, "compare") ||
    hasText(response?.intent, "comparison") ||
    hasText(response?.canvasType, "comparison") ||
    hasText(response?.inlineType, "comparison") ||
    hasText(responseText, " vs ");

  if (!hasComparisonIntent) {
    failures.push(
      `Expected comparison-style response, got intent="${response?.intent || ""}" canvasType="${response?.canvasType || ""}" inlineType="${response?.inlineType || ""}"`,
    );
  }

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
      displayMode: response?.displayMode || "",
      canvasType: response?.canvasType || "",
      inlineType: response?.inlineType || "",
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
      rowsCount: Array.isArray(response?.rows) ? response.rows.length : 0,
      itemsCount: Array.isArray(response?.items) ? response.items.length : 0,
      featuresCount: Array.isArray(response?.features) ? response.features.length : 0,
      sourceTransparency: response?.sourceTransparency || {},
      runtimeResultsMeta: response?.runtimeResultsMeta || [],
    },
  };
};

const main = async () => {
  await connectDB();

  const results = [];

  for (const testCase of cases) {
    results.push(await runCase(testCase));
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI feature comparison query audit",
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
