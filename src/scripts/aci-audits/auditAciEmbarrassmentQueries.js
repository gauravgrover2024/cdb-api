import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import { hasForbiddenContextPayload, planContextCase } from "./auditAciContextManagerV1.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const hasText = (value = "", part = "") => clean(value).includes(clean(part));

const vehicleText = (vehicle = {}) =>
  [
    vehicle.make,
    vehicle.model,
    vehicle.fullModel,
    vehicle.variant,
    vehicle.variantName,
    vehicle.selectedVariant,
  ]
    .filter(Boolean)
    .join(" ");

const cases = [
  {
    id: "eqs-range-must-not-open-overview",
    message: "eqs range",
    expectedMake: "Mercedes Benz",
    expectedModel: "Eqs",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedTextParts: ["eqs"],
    forbiddenModelParts: ["land rover", "range rover", "evoque"],
    forbiddenVariantParts: ["range"],
  },
  {
    id: "mercedes-eqs-range-must-not-open-overview",
    message: "mercedes eqs range",
    expectedMake: "Mercedes Benz",
    expectedModel: "Eqs",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedTextParts: ["eqs"],
    forbiddenModelParts: ["land rover", "range rover", "evoque"],
    forbiddenVariantParts: ["range"],
  },
  {
    id: "ix-range-must-not-resolve-land-rover",
    message: "ix range",
    expectedMake: "Bmw",
    expectedModel: "Ix",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedTextParts: ["ix"],
    forbiddenModelParts: ["land rover", "range rover", "evoque"],
    forbiddenVariantParts: ["range"],
  },
  {
    id: "bmw-ix-range-control-case",
    message: "bmw ix range",
    expectedMake: "Bmw",
    expectedModel: "Ix",
    expectedTool: "vehicle_spec_attribute_lookup",
    expectedTextParts: ["ix"],
    forbiddenModelParts: ["land rover", "range rover", "evoque"],
    forbiddenVariantParts: ["range"],
  },
  {
    id: "be-6e-sunroof-model-alias-no-fake-variant",
    message: "be 6e sunroof",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    expectedTool: "vehicle_feature_lookup",
    expectedTextParts: ["be 6"],
    forbiddenVariantParts: ["be 6e", "6e"],
  },
  {
    id: "mahindra-be-6e-sunroof-model-alias-no-fake-variant",
    message: "mahindra be 6e sunroof",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    expectedTool: "vehicle_feature_lookup",
    expectedTextParts: ["be 6"],
    forbiddenVariantParts: ["be 6e", "mahindra be 6e", "6e"],
  },
];

const runCase = async (testCase = {}) => {
  const startedAt = Date.now();
  const failures = [];

  try {
    const result = await planContextCase({ message: testCase.message, context: {} });
    const selectedVehicle = result.selectedVehicle || {};
    const modelBag = [
      selectedVehicle.make,
      selectedVehicle.model,
      selectedVehicle.fullModel,
      result.plan?.tools?.[0]?.entities?.model,
    ].join(" ");
    const variantBag = [
      selectedVehicle.variant,
      selectedVehicle.variantName,
      selectedVehicle.selectedVariant,
      result.plan?.tools?.[0]?.entities?.variant,
    ].join(" ");
    const forbiddenContextKeys = hasForbiddenContextPayload(result.contextState || {});

    if (result.tool !== testCase.expectedTool) {
      failures.push(`Expected tool ${testCase.expectedTool}, got ${result.tool}`);
    }

    if (clean(selectedVehicle.make) !== clean(testCase.expectedMake)) {
      failures.push(`Expected make ${testCase.expectedMake}, got ${selectedVehicle.make || ""}`);
    }

    if (clean(selectedVehicle.model) !== clean(testCase.expectedModel)) {
      failures.push(`Expected model ${testCase.expectedModel}, got ${selectedVehicle.model || ""}`);
    }

    for (const expected of testCase.expectedTextParts || []) {
      if (!hasText(modelBag, expected)) {
        failures.push(`Expected model context/text to include "${expected}", got "${modelBag}"`);
      }
    }

    for (const forbidden of testCase.forbiddenModelParts || []) {
      if (hasText(modelBag, forbidden)) {
        failures.push(`Forbidden wrong model text "${forbidden}" found in model context/text: "${modelBag}"`);
      }
    }

    for (const forbidden of testCase.forbiddenVariantParts || []) {
      if (hasText(variantBag, forbidden)) {
        failures.push(`Forbidden model/spec text "${forbidden}" leaked into variant context: "${variantBag}"`);
      }
    }

    if (forbiddenContextKeys.length) {
      failures.push(`Durable contextState contains forbidden payload keys: ${forbiddenContextKeys.join(", ")}`);
    }

    return {
      id: testCase.id,
      message: testCase.message,
      pass: failures.length === 0,
      durationMs: Date.now() - startedAt,
      failures,
      summary: {
        tool: result.tool,
        anchorMake: selectedVehicle.make || "",
        anchorModel: selectedVehicle.model || "",
        anchorFullModel: selectedVehicle.fullModel || "",
        selectedVehicle: {
          make: selectedVehicle.make || "",
          model: selectedVehicle.model || "",
          fullModel: selectedVehicle.fullModel || "",
          variant: selectedVehicle.variant || "",
        },
        vehicleText: vehicleText(selectedVehicle),
        forbiddenContextKeys,
      },
    };
  } catch (error) {
    return {
      id: testCase.id,
      message: testCase.message,
      pass: false,
      durationMs: Date.now() - startedAt,
      failures: [error?.stack || error?.message || String(error)],
      summary: {},
    };
  }
};

const main = async () => {
  await connectDB();

  const startedAt = Date.now();
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
    durationMs: Date.now() - startedAt,
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
