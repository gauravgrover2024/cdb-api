import "dotenv/config";
import mongoose from "mongoose";

import connectDB from "../../config/db.js";
import { planContextCase } from "./auditAciContextManagerV1.js";

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
    id: "be-6e-sunroof-model-alias-no-variant-pollution",
    message: "be 6e sunroof",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    expectedTool: "vehicle_feature_lookup",
    forbiddenVariantParts: ["be 6e", "6e"],
  },
  {
    id: "mahindra-be-6e-sunroof-model-alias-no-variant-pollution",
    message: "mahindra be 6e sunroof",
    expectedMake: "Mahindra",
    expectedModel: "Be 6",
    expectedTool: "vehicle_feature_lookup",
    forbiddenVariantParts: ["be 6e", "mahindra be 6e", "6e"],
  },
];

const runCase = async (testCase = {}) => {
  const startedAt = Date.now();
  const failures = [];

  try {
    const result = await planContextCase({ message: testCase.message, context: {} });
    const selectedVehicle = result.selectedVehicle || {};
    const variantBag = [
      selectedVehicle.variant,
      selectedVehicle.variantName,
      selectedVehicle.selectedVariant,
    ].join(" ");

    if (result.tool !== testCase.expectedTool) {
      failures.push(`Expected tool ${testCase.expectedTool}, got ${result.tool}`);
    }

    if (clean(selectedVehicle.make) !== clean(testCase.expectedMake)) {
      failures.push(`Expected make ${testCase.expectedMake}, got ${selectedVehicle.make || ""}`);
    }

    if (clean(selectedVehicle.model) !== clean(testCase.expectedModel)) {
      failures.push(`Expected model ${testCase.expectedModel}, got ${selectedVehicle.model || ""}`);
    }

    for (const forbidden of testCase.forbiddenVariantParts || []) {
      if (hasText(variantBag, forbidden)) {
        failures.push(`Model alias text "${forbidden}" leaked into variant context: "${variantBag}"`);
      }
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
    suite: "ACI model alias feature query audit",
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
