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

const cases = [
  {
    id: "be-6e-sunroof-model-alias-no-variant-pollution",
    message: "be 6e sunroof",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    forbiddenVariantParts: ["be 6e", "6e"],
    expectedAnswerParts: ["sunroof"],
    minRecordCount: 1,
  },
  {
    id: "mahindra-be-6e-sunroof-model-alias-no-variant-pollution",
    message: "mahindra be 6e sunroof",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    forbiddenVariantParts: ["be 6e", "mahindra be 6e", "6e"],
    expectedAnswerParts: ["sunroof"],
    minRecordCount: 1,
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
      conversationId: `audit-model-alias-feature-${testCase.id}`,
      userId: "audit",
    });
  } catch (err) {
    error = err?.stack || err?.message || String(err);
  }

  const failures = [];
  const patch = response?.contextPatch || {};
  const selectedVehicle = patch.selectedVehicle || {};
  const variantBag = [
    patch.anchorVariant,
    selectedVehicle.variant,
    selectedVehicle.variantName,
    selectedVehicle.selectedVariant,
  ].join(" ");

  const responseText = [
    response?.title,
    response?.answer,
    JSON.stringify(response?.sourceTransparency || {}),
  ]
    .filter(Boolean)
    .join(" ");

  if (error) failures.push(`chatWithAgent threw: ${error}`);

  if (clean(patch.anchorMake) !== clean(testCase.expectedMake)) {
    failures.push(`Expected anchorMake "${testCase.expectedMake}", got "${patch.anchorMake || ""}"`);
  }

  if (clean(patch.anchorModel) !== clean(testCase.expectedModel)) {
    failures.push(`Expected anchorModel "${testCase.expectedModel}", got "${patch.anchorModel || ""}"`);
  }

  for (const forbidden of testCase.forbiddenVariantParts || []) {
    if (hasText(variantBag, forbidden)) {
      failures.push(`Model alias text "${forbidden}" leaked into variant context: "${variantBag}"`);
    }
  }

  for (const expected of testCase.expectedAnswerParts || []) {
    if (!hasText(responseText, expected)) {
      failures.push(`Expected response to mention "${expected}"`);
    }
  }

  const recordCount = Number(response?.sourceTransparency?.recordCount || 0);
  if (recordCount < Number(testCase.minRecordCount || 0)) {
    failures.push(`Expected sourceTransparency.recordCount >= ${testCase.minRecordCount}, got ${recordCount}`);
  }

  if (hasText(response?.answer || "", "older") || hasText(response?.answer || "", "pick a current variant")) {
    failures.push(`Response is a false variant fallback: "${response.answer || ""}"`);
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
      anchorMake: patch.anchorMake || "",
      anchorModel: patch.anchorModel || "",
      anchorFullModel: patch.anchorFullModel || "",
      anchorVariant: patch.anchorVariant || "",
      selectedVehicle: {
        make: selectedVehicle.make || "",
        model: selectedVehicle.model || "",
        fullModel: selectedVehicle.fullModel || "",
        variant: selectedVehicle.variant || "",
        variantName: selectedVehicle.variantName || "",
      },
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
    suite: "ACI model alias feature query audit",
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
