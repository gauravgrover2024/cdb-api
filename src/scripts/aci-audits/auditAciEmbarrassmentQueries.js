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

const collectText = (response = {}) =>
  [
    response.intent,
    response.title,
    response.answer,
    response.displayMode,
    response.canvasType,
    response.inlineType,
    response.contextPatch ? JSON.stringify(response.contextPatch) : "",
    response.sourceTransparency ? JSON.stringify(response.sourceTransparency) : "",
    ...(Array.isArray(response.features) ? response.features.map((item) => JSON.stringify(item)) : []),
    ...(Array.isArray(response.rows) ? response.rows.map((item) => JSON.stringify(item)) : []),
    ...(Array.isArray(response.items) ? response.items.map((item) => JSON.stringify(item)) : []),
  ]
    .filter(Boolean)
    .join(" ");

const cases = [
  {
    id: "eqs-range-must-not-open-overview",
    message: "eqs range",
    expectedModelParts: ["eqs"],
    expectedTextParts: ["range"],
    forbiddenTextParts: [
      "opened mercedes benz eqs overview",
      "opened",
      "overview",
    ],
    forbiddenIntentParts: ["overview"],
    minRecordCount: 1,
  },
  {
    id: "mercedes-eqs-range-must-not-open-overview",
    message: "mercedes eqs range",
    expectedModelParts: ["eqs"],
    expectedTextParts: ["range"],
    forbiddenTextParts: [
      "opened mercedes benz eqs overview",
      "opened",
      "overview",
    ],
    forbiddenIntentParts: ["overview"],
    minRecordCount: 1,
  },
  {
    id: "be-6e-sunroof-model-alias-no-fake-variant",
    message: "be 6e sunroof",
    expectedModelParts: ["be 6"],
    expectedTextParts: ["sunroof"],
    forbiddenVariantParts: ["be 6e", "6e"],
    forbiddenTextParts: [
      "looks like an older",
      "pick a current variant",
    ],
    minRecordCount: 1,
  },
  {
    id: "mahindra-be-6e-sunroof-model-alias-no-fake-variant",
    message: "mahindra be 6e sunroof",
    expectedModelParts: ["be 6"],
    expectedTextParts: ["sunroof"],
    forbiddenVariantParts: ["be 6e", "mahindra be 6e", "6e"],
    forbiddenTextParts: [
      "looks like an older",
      "pick a current variant",
    ],
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
      conversationId: `audit-embarrassment-${testCase.id}`,
      userId: "audit",
    });
  } catch (err) {
    error = err?.stack || err?.message || String(err);
  }

  const failures = [];
  const patch = response?.contextPatch || {};
  const selectedVehicle = patch.selectedVehicle || {};
  const responseText = collectText(response);

  const modelBag = [
    patch.anchorModel,
    patch.anchorFullModel,
    selectedVehicle.model,
    selectedVehicle.fullModel,
    response?.title,
    response?.answer,
  ].join(" ");

  const variantBag = [
    patch.anchorVariant,
    selectedVehicle.variant,
    selectedVehicle.variantName,
    selectedVehicle.selectedVariant,
  ].join(" ");

  if (error) failures.push(`chatWithAgent threw: ${error}`);

  for (const expected of testCase.expectedModelParts || []) {
    if (!hasText(modelBag, expected)) {
      failures.push(`Expected model context/text to include "${expected}", got "${modelBag}"`);
    }
  }

  for (const expected of testCase.expectedTextParts || []) {
    if (!hasText(responseText, expected)) {
      failures.push(`Expected response text to include "${expected}"`);
    }
  }

  for (const forbidden of testCase.forbiddenTextParts || []) {
    if (hasText(responseText, forbidden)) {
      failures.push(`Forbidden fallback/overview text "${forbidden}" found`);
    }
  }

  for (const forbidden of testCase.forbiddenIntentParts || []) {
    if (
      hasText(response?.intent || "", forbidden) ||
      hasText(response?.canvasType || "", forbidden) ||
      hasText(response?.inlineType || "", forbidden)
    ) {
      failures.push(
        `Forbidden intent/canvas/inline part "${forbidden}" found in intent="${response?.intent || ""}" canvasType="${response?.canvasType || ""}" inlineType="${response?.inlineType || ""}"`,
      );
    }
  }

  for (const forbidden of testCase.forbiddenVariantParts || []) {
    if (hasText(variantBag, forbidden)) {
      failures.push(`Forbidden model/feature text "${forbidden}" leaked into variant context: "${variantBag}"`);
    }
  }

  const recordCount = Number(response?.sourceTransparency?.recordCount || 0);
  if (Number(testCase.minRecordCount || 0) > 0 && recordCount < testCase.minRecordCount) {
    failures.push(`Expected sourceTransparency.recordCount >= ${testCase.minRecordCount}, got ${recordCount}`);
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
    suite: "ACI embarrassment query audit",
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
