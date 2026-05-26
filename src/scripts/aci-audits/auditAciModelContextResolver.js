import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import {
  hydrateAciExplicitModelEntityFromReadModel,
  resolveAciExplicitMessageModelEntity,
} from "../../services/aiAgent/aiAgent.modelContextResolver.js";

const clean = (value = "") =>
  String(value || "")
    .replace(/\s+/g, " ")
    .trim()
    .toLowerCase();

const cases = [
  {
    id: "context-resolve-creta",
    type: "resolve",
    message: "Show Creta pricelist",
    expectMake: "Hyundai",
    expectModel: "Creta",
    expectFullModel: "Hyundai Creta",
  },
  {
    id: "context-resolve-seltos",
    type: "resolve",
    message: "Show Seltos colors",
    expectMake: "Kia",
    expectModel: "Seltos",
    expectFullModel: "Kia Seltos",
  },
  {
    id: "context-resolve-honda-city",
    type: "resolve",
    message: "EMI for Honda City",
    expectMake: "Honda",
    expectModel: "City",
    expectFullModel: "Honda City",
  },
  {
    id: "context-resolve-thar",
    type: "resolve",
    message: "Does Thar have sunroof?",
    expectMake: "Mahindra",
    expectModel: "Thar",
    expectFullModel: "Mahindra Thar",
  },
  {
    id: "context-resolve-generic-price-null",
    type: "resolve",
    message: "Show price in Delhi",
    expectNull: true,
  },
  {
    id: "context-hydrate-verna-model-only",
    type: "hydrate",
    entity: {
      model: "Verna",
    },
    expectMake: "Hyundai",
    expectModel: "Verna",
    expectFullModel: "Hyundai Verna",
    expectFromReadModelSummary: true,
  },
  {
    id: "context-hydrate-city-with-brand",
    type: "hydrate",
    entity: {
      make: "Honda",
      brand: "Honda",
      model: "City",
      fullModel: "Honda City",
    },
    expectMake: "Honda",
    expectModel: "City",
    expectFullModel: "Honda City",
    expectFromReadModelSummary: true,
  },
];

const readField = (entity = {}, key = "") => String(entity?.[key] || "");

const runCase = async (testCase) => {
  const startedAt = Date.now();
  let result = null;
  let error = "";

  try {
    if (testCase.type === "hydrate") {
      result = await hydrateAciExplicitModelEntityFromReadModel(testCase.entity || {});
    } else {
      result = await resolveAciExplicitMessageModelEntity(testCase.message || "");
    }
  } catch (err) {
    error = err?.stack || err?.message || String(err);
  }

  const failures = [];

  if (error) {
    failures.push(`Threw error: ${error}`);
  }

  if (testCase.expectNull) {
    if (result?.model || result?.make || result?.brand) {
      failures.push(
        `Expected null/blank result, got make="${readField(result, "make")}" model="${readField(result, "model")}" fullModel="${readField(result, "fullModel")}"`,
      );
    }
  } else {
    const make = result?.make || result?.brand || "";
    const model = result?.model || "";
    const fullModel = result?.fullModel || result?.displayName || "";

    if (clean(make) !== clean(testCase.expectMake)) {
      failures.push(`Expected make "${testCase.expectMake}", got "${make}"`);
    }

    if (clean(model) !== clean(testCase.expectModel)) {
      failures.push(`Expected model "${testCase.expectModel}", got "${model}"`);
    }

    if (clean(fullModel) !== clean(testCase.expectFullModel)) {
      failures.push(`Expected fullModel "${testCase.expectFullModel}", got "${fullModel}"`);
    }

    if (
      testCase.expectFromReadModelSummary &&
      result?.fromReadModelSummary !== true
    ) {
      failures.push("Expected fromReadModelSummary=true");
    }
  }

  return {
    id: testCase.id,
    type: testCase.type,
    message: testCase.message || "",
    pass: failures.length === 0,
    durationMs: Date.now() - startedAt,
    failures,
    result: result
      ? {
          make: result.make || "",
          brand: result.brand || "",
          model: result.model || "",
          fullModel: result.fullModel || "",
          displayName: result.displayName || "",
          matchedText: result.matchedText || "",
          method: result.method || "",
          confidence: result.confidence || "",
          fromMessage: result.fromMessage || false,
          fromReadModelSummary: result.fromReadModelSummary || false,
        }
      : null,
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
    suite: "ACI model context resolver audit",
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

main().catch(async (err) => {
  console.error(err?.stack || err?.message || err);
  try {
    await mongoose.disconnect();
  } catch {}
  process.exit(1);
});
