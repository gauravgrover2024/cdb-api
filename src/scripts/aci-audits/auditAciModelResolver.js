import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { resolveVehicleModelFromText } from "../../services/aiAgent/aiAgent.vehicleModelResolver.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const getResolvedMake = (resolved = {}) =>
  resolved?.make || resolved?.brand || "";

const getResolvedModel = (resolved = {}) =>
  resolved?.model || "";

const getResolvedFullModel = (resolved = {}) =>
  resolved?.fullModel || resolved?.displayName || "";

const cases = [
  {
    id: "resolver-creta-price",
    message: "Show Creta pricelist",
    expectMake: "Hyundai",
    expectModel: "Creta",
  },
  {
    id: "resolver-seltos-colors",
    message: "Show Seltos colors",
    expectMake: "Kia",
    expectModel: "Seltos",
  },
  {
    id: "resolver-honda-city-emi",
    message: "EMI for Honda City",
    expectMake: "Honda",
    expectModel: "City",
  },
  {
    id: "resolver-thar-feature",
    message: "Does Thar have sunroof?",
    expectMake: "Mahindra",
    expectModel: "Thar",
  },
  {
    id: "resolver-verna-price",
    message: "Show Verna pricelist",
    expectMake: "Hyundai",
    expectModel: "Verna",
  },
  {
    id: "resolver-internal-loan-guard",
    message: "Loan closure 7077",
    expectNull: true,
  },
  {
    id: "resolver-generic-price-guard",
    message: "Show price in Delhi",
    expectNull: true,
  },
  {
    id: "resolver-generic-color-guard",
    message: "black available?",
    expectNull: true,
  },
];

const run = async () => {
  await connectDB();

  const db = mongoose.connection.db;
  const results = [];

  for (const testCase of cases) {
    const startedAt = Date.now();
    let resolved = null;
    let error = "";

    try {
      resolved = await resolveVehicleModelFromText({
        db,
        message: testCase.message,
      });
    } catch (err) {
      error = err?.stack || err?.message || String(err);
    }

    const make = getResolvedMake(resolved);
    const model = getResolvedModel(resolved);
    const fullModel = getResolvedFullModel(resolved);

    const failures = [];

    if (error) {
      failures.push(`Resolver threw error: ${error}`);
    }

    if (testCase.expectNull) {
      if (resolved?.model || resolved?.make || resolved?.brand) {
        failures.push(
          `Expected no model, got make="${make}" model="${model}" fullModel="${fullModel}"`,
        );
      }
    } else {
      if (clean(make) !== clean(testCase.expectMake)) {
        failures.push(`Expected make "${testCase.expectMake}", got "${make}"`);
      }

      if (clean(model) !== clean(testCase.expectModel)) {
        failures.push(`Expected model "${testCase.expectModel}", got "${model}"`);
      }
    }

    results.push({
      id: testCase.id,
      message: testCase.message,
      pass: failures.length === 0,
      durationMs: Date.now() - startedAt,
      failures,
      resolved: resolved
        ? {
            make,
            model,
            fullModel,
            matchedText: resolved.matchedText || "",
            method: resolved.method || "",
            confidence: resolved.confidence || "",
            score: resolved.score || "",
          }
        : null,
    });
  }

  const failed = results.filter((item) => !item.pass);

  console.log(JSON.stringify({
    suite: "ACI model resolver audit",
    total: results.length,
    passed: results.length - failed.length,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    results,
  }, null, 2));

  await mongoose.disconnect();

  if (failed.length) {
    process.exit(1);
  }
};

run().catch(async (err) => {
  console.error(err?.stack || err?.message || err);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
