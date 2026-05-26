import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const cases = [
  {
    id: "punch-sunroof-and-adas",
    message: "Does the Tata Punch have a sunroof and ADAS?",
    expectMake: "Tata",
    expectModel: "Punch",
    forbiddenVariantParts: ["adas", "and adas", "sunroof"],
    expectedMentionParts: ["sunroof", "adas"],
    requiredFeatureKeyParts: ["sunroof", "adas"],
    forbiddenFeatureKeyParts: ["voice_assisted_sunroof"],
    maxFeatureCount: 3,
  },
];

const hasText = (value = "", part = "") => clean(value).includes(clean(part));

const collectResponseText = (response = {}) =>
  [
    response.title,
    response.answer,
    response.feature,
    ...(Array.isArray(response.features) ? response.features : []),
    ...(Array.isArray(response.rows)
      ? response.rows.map((row) => JSON.stringify(row))
      : []),
    ...(Array.isArray(response.items)
      ? response.items.map((item) => JSON.stringify(item))
      : []),
    response.data ? JSON.stringify(response.data) : "",
    response.widget ? JSON.stringify(response.widget) : "",
  ]
    .filter(Boolean)
    .join(" ");

const main = async () => {
  await connectDB();

  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();
    let response = null;
    let error = "";

    try {
      response = await chatWithAgent({
        message: testCase.message,
        context: {},
        conversationId: `audit-multi-feature-${testCase.id}`,
        userId: "audit",
      });
    } catch (err) {
      error = err?.stack || err?.message || String(err);
    }

    const failures = [];
    const patch = response?.contextPatch || {};
    const selectedVehicle = patch.selectedVehicle || {};
    const responseText = collectResponseText(response);
    const returnedFeatures = [
      ...(Array.isArray(response?.features) ? response.features : []),
      ...(Array.isArray(response?.rows) ? response.rows : []),
      ...(Array.isArray(response?.items) ? response.items : []),
    ];

    const uniqueReturnedFeatureKeys = [
      ...new Set(
        returnedFeatures
          .map((item) => item?.featureKey || item?.canonicalKey || item?.displayName || item?.feature || "")
          .filter(Boolean),
      ),
    ];

    if (
      Number(testCase.maxFeatureCount || 0) > 0 &&
      uniqueReturnedFeatureKeys.length > testCase.maxFeatureCount
    ) {
      failures.push(
        `Expected at most ${testCase.maxFeatureCount} returned features, got ${uniqueReturnedFeatureKeys.length}: ${uniqueReturnedFeatureKeys.join(", ")}`,
      );
    }

    for (const requiredPart of testCase.requiredFeatureKeyParts || []) {
      if (!uniqueReturnedFeatureKeys.some((key) => clean(key).includes(clean(requiredPart)))) {
        failures.push(
          `Expected returned feature keys to include "${requiredPart}", got ${uniqueReturnedFeatureKeys.join(", ")}`,
        );
      }
    }

    for (const forbiddenPart of testCase.forbiddenFeatureKeyParts || []) {
      if (uniqueReturnedFeatureKeys.some((key) => clean(key).includes(clean(forbiddenPart)))) {
        failures.push(
          `Forbidden returned feature key "${forbiddenPart}" found in ${uniqueReturnedFeatureKeys.join(", ")}`,
        );
      }
    }

    if (error) failures.push(`chatWithAgent threw: ${error}`);

    if (clean(patch.anchorMake) !== clean(testCase.expectMake)) {
      failures.push(`Expected anchorMake ${testCase.expectMake}, got "${patch.anchorMake || ""}"`);
    }

    if (clean(patch.anchorModel) !== clean(testCase.expectModel)) {
      failures.push(`Expected anchorModel ${testCase.expectModel}, got "${patch.anchorModel || ""}"`);
    }

    const variantBag = [
      patch.anchorVariant,
      selectedVehicle.variant,
      selectedVehicle.variantName,
      selectedVehicle.selectedVariant,
    ].join(" ");

    for (const forbidden of testCase.forbiddenVariantParts) {
      if (hasText(variantBag, forbidden)) {
        failures.push(`Feature text "${forbidden}" leaked into variant context: "${variantBag}"`);
      }
    }

    for (const expectedPart of testCase.expectedMentionParts) {
      if (!hasText(responseText, expectedPart)) {
        failures.push(`Expected response to mention "${expectedPart}"`);
      }
    }

    results.push({
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
        sourceTransparency: response?.sourceTransparency || {},
        runtimeResultsMeta: response?.runtimeResultsMeta || [],
      },
    });
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI multi-feature query audit",
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
