import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../../config/db.js";
import { buildAciCompoundVehiclePlan } from "../../services/aiAgent/aiAgent.compoundVehicleRequest.js";
import {
  sanitizePlannerPlan,
  validatePlannerPlan,
} from "../../services/aiAgent/aiAgent.planSchema.js";

const cases = [
  {
    id: "one-car-four-capabilities",
    message: "Show Hyundai Creta sunroof, colours, price list and EMI in Noida",
    modelCount: 1,
    requiredTools: {
      vehicle_feature_lookup: 1,
      vehicle_colors: 1,
      vehicle_pricelist: 1,
      vehicle_emi: 1,
    },
  },
  {
    id: "two-car-five-features-colors-prices",
    message:
      "Compare Hyundai Creta and Kia Seltos on sunroof, ABS, 6 airbags, ADAS and 360 camera, and show their colour options and price lists in Noida",
    modelCount: 2,
    requiredTools: {
      vehicle_feature_comparison: 1,
      vehicle_colors: 2,
      vehicle_pricelist: 2,
    },
  },
  {
    id: "three-car-spec-price-color",
    message:
      "Compare Hyundai Creta, Kia Seltos and Honda Elevate prices, mileage, boot space and colours in Noida",
    modelCount: 3,
    requiredTools: {
      vehicle_compare: 2,
      vehicle_colors: 3,
      vehicle_pricelist: 3,
      vehicle_spec_attribute_lookup: 6,
    },
  },
  {
    id: "two-car-commercial-and-alternatives",
    message:
      "Compare Mahindra Thar and Maruti Jimny price breakup, EMI, ground clearance and alternatives",
    modelCount: 2,
    requiredTools: {
      vehicle_compare: 1,
      vehicle_price_breakup: 2,
      vehicle_emi: 2,
      vehicle_spec_attribute_lookup: 2,
      vehicle_similar: 2,
    },
  },
  {
    id: "two-car-lifecycle-and-score-intents",
    message:
      "Compare Hyundai Creta and Kia Seltos variants, price history, ratings, mileage and EMI in Noida",
    modelCount: 2,
    requiredTools: {
      vehicle_compare: 1,
      vehicle_pricelist: 2,
      vehicle_price_history: 2,
      vehicle_emi: 2,
      vehicle_spec_attribute_lookup: 2,
      vehicle_score_insight: 2,
    },
  },
  {
    id: "five-car-mixed-research",
    message:
      "Compare Hyundai Creta, Kia Seltos, Honda Elevate, Tata Curvv and Maruti Grand Vitara on sunroof, and show their prices, colours, EMI, mileage and alternatives in Noida",
    modelCount: 5,
    requiredTools: {
      vehicle_feature_comparison: 4,
      vehicle_colors: 5,
      vehicle_pricelist: 5,
      vehicle_emi: 5,
      vehicle_spec_attribute_lookup: 5,
      vehicle_similar: 5,
    },
  },
  {
    id: "context-followup-three-capabilities",
    message: "Now show colours, price list and EMI",
    context: {
      selectedVehicle: {
        make: "Kia",
        model: "Seltos",
        fullModel: "Kia Seltos",
      },
      anchorMake: "Kia",
      anchorModel: "Seltos",
    },
    modelCount: 1,
    requiredTools: {
      vehicle_colors: 1,
      vehicle_pricelist: 1,
      vehicle_emi: 1,
    },
  },
];

const main = async () => {
  await connectDB();
  const results = [];

  for (const testCase of cases) {
    const failures = [];
    const rawPlan = await buildAciCompoundVehiclePlan({
      message: testCase.message,
      context: testCase.context || {},
    });

    if (!rawPlan) {
      failures.push("No compound plan returned");
      results.push({ id: testCase.id, pass: false, failures });
      continue;
    }

    const plan = sanitizePlannerPlan(rawPlan, { message: testCase.message });
    const validation = validatePlannerPlan(plan, { message: testCase.message });
    const tools = plan.tools || [];
    const counts = tools.reduce((output, tool) => {
      output[tool.tool] = Number(output[tool.tool] || 0) + 1;
      return output;
    }, {});
    const modelCount = Number(plan.contextPatch?.compoundRequest?.modelCount || 0);

    if (!validation.valid) failures.push(...(validation.errors || []));
    if (modelCount !== testCase.modelCount) {
      failures.push(`Expected ${testCase.modelCount} models, got ${modelCount}`);
    }
    for (const [tool, expectedCount] of Object.entries(testCase.requiredTools)) {
      if (Number(counts[tool] || 0) !== expectedCount) {
        failures.push(`Expected ${expectedCount} ${tool} tools, got ${counts[tool] || 0}`);
      }
    }

    results.push({
      id: testCase.id,
      pass: failures.length === 0,
      failures,
      summary: {
        models: plan.contextPatch?.compoundRequest?.models || [],
        requestedCapabilities:
          plan.contextPatch?.compoundRequest?.requestedCapabilities || [],
        toolCount: tools.length,
        toolCounts: counts,
      },
    });
  }

  const failed = results.filter((result) => !result.pass);
  console.log(
    JSON.stringify(
      {
        suite: "ACI compound query capability matrix",
        total: results.length,
        passed: results.length - failed.length,
        failed: failed.length,
        failedIds: failed.map((result) => result.id),
        results,
      },
      null,
      2,
    ),
  );

  if (failed.length) process.exitCode = 1;
};

main()
  .catch((error) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    await mongoose.disconnect();
  });
