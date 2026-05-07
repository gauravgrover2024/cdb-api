import "dotenv/config";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import { buildAiPlan } from "../services/aiAgent/aiAgent.planner.js";
import {
  validatePlannerPlan,
  normalizeSearchKey,
} from "../services/aiAgent/aiAgent.planSchema.js";

const PLANNER_TIMEOUT_BUFFER_MS = 1500;

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const asArray = (value) => {
  if (Array.isArray(value)) return value;
  if (value === undefined || value === null || value === "") return [];
  return [value];
};

const norm = (value = "") => normalizeSearchKey(String(value || ""));

const includesText = (value, needle) =>
  norm(JSON.stringify(value || "")).includes(norm(needle));

const getAllTools = (plan = {}) => asArray(plan.tools);

const getFirstTool = (plan = {}) => getAllTools(plan)[0] || null;

const getTool = (plan = {}, expectedTool = "") =>
  getAllTools(plan).find((toolPlan) => toolPlan.tool === expectedTool) || null;

const getMergedEntitiesAndFilters = (toolPlan = {}) => ({
  ...(toolPlan.entities || {}),
  ...(toolPlan.filters || {}),
});

const getLeadType = (toolPlan = {}) =>
  toolPlan?.entities?.leadType || toolPlan?.filters?.leadType || "";

const getUnavailableReason = (plan = {}, toolPlan = {}) =>
  plan.unavailableReason ||
  toolPlan?.unavailableReason ||
  toolPlan?.filters?.unavailableReason ||
  toolPlan?.entities?.unavailableReason ||
  "";

const hasTool = (plan = {}, expectedTool = "") =>
  getAllTools(plan).some((toolPlan) => toolPlan.tool === expectedTool);

const hasAnyTool = (plan = {}, expectedTools = []) =>
  getAllTools(plan).some((toolPlan) => expectedTools.includes(toolPlan.tool));

const hasNoTool = (plan = {}, disallowedTool = "") =>
  !getAllTools(plan).some((toolPlan) => toolPlan.tool === disallowedTool);

const hasNoLeadType = (plan = {}, disallowedLeadType = "") =>
  !getAllTools(plan).some(
    (toolPlan) =>
      toolPlan?.entities?.leadType === disallowedLeadType ||
      toolPlan?.filters?.leadType === disallowedLeadType,
  );

const hasModel = (toolPlan = {}, expectedModel = "") => {
  const bag = getMergedEntitiesAndFilters(toolPlan);

  return (
    includesText(bag.model, expectedModel) ||
    includesText(bag.models, expectedModel) ||
    includesText(bag.primaryModel, expectedModel) ||
    includesText(bag.comparisonModels, expectedModel)
  );
};

const hasVariant = (toolPlan = {}, expectedVariant = "") => {
  const bag = getMergedEntitiesAndFilters(toolPlan);

  return (
    includesText(bag.variant, expectedVariant) ||
    includesText(bag.variants, expectedVariant) ||
    includesText(bag.primaryVariant, expectedVariant) ||
    includesText(bag.comparisonVariants, expectedVariant)
  );
};

const hasFeature = (toolPlan = {}, expectedFeature = "") => {
  const bag = getMergedEntitiesAndFilters(toolPlan);

  return (
    includesText(bag.feature, expectedFeature) ||
    includesText(bag.features, expectedFeature) ||
    includesText(bag.mustHaveFeatures, expectedFeature) ||
    includesText(bag.compareFeatures, expectedFeature)
  );
};

const hasBudgetMax = (toolPlan = {}, expectedBudgetMax) => {
  const bag = getMergedEntitiesAndFilters(toolPlan);
  return Number(bag.budgetMax) === Number(expectedBudgetMax);
};

const hasDownPayment = (toolPlan = {}, expectedDownPayment) => {
  const bag = getMergedEntitiesAndFilters(toolPlan);
  return Number(bag.downPayment) === Number(expectedDownPayment);
};

const hasTenureMonths = (toolPlan = {}, expectedTenureMonths) => {
  const bag = getMergedEntitiesAndFilters(toolPlan);
  return Number(bag.tenureMonths) === Number(expectedTenureMonths);
};

const hasBodyType = (toolPlan = {}, expectedBodyType = "") => {
  const bag = getMergedEntitiesAndFilters(toolPlan);
  return includesText(bag.bodyType, expectedBodyType);
};

const hasTransmission = (toolPlan = {}, expectedTransmission = "") => {
  const bag = getMergedEntitiesAndFilters(toolPlan);
  return includesText(bag.transmission, expectedTransmission);
};

const hasRanking = (toolPlan = {}, expectedRanking = "") =>
  toolPlan?.ranking === expectedRanking;

const hasAnyRanking = (toolPlan = {}, expectedRankings = []) =>
  expectedRankings.includes(toolPlan?.ranking);

const hasContextModel = (plan = {}, expectedModel = "") =>
  includesText(plan?.contextPatch?.anchorModel, expectedModel) ||
  includesText(plan?.contextPatch?.selectedVehicle?.model, expectedModel);

const hasContextVariant = (plan = {}, expectedVariant = "") =>
  includesText(plan?.contextPatch?.anchorVariant, expectedVariant) ||
  includesText(plan?.contextPatch?.selectedVehicle?.variant, expectedVariant);

const hasFakeOfferAmount = (plan = {}) => {
  const text = JSON.stringify(plan || {});

  const hardOfferFields =
    /(cashDiscount|cash_discount|discountAmount|discount_amount|exchangeBonus|exchange_bonus|corporateOffer|corporate_offer|offerAmount|offer_amount|schemeAmount|scheme_amount)/i;

  if (hardOfferFields.test(text)) return true;

  return /(discount|offer|bonus|scheme)\D{0,40}(?:₹|rs\.?|inr)?\s*\d{4,}/i.test(
    text,
  );
};

const nextStepsHaveBadBudgetText = (plan = {}) =>
  asArray(plan.nextSteps).some((step) =>
    /\b\d{6,}\s*lakh\b/i.test(String(step?.query || "")),
  );

const nextStepsMentionTestDrive = (plan = {}) =>
  asArray(plan.nextSteps).some((step) =>
    /\btest\s*drive\b/i.test(`${step?.label || ""} ${step?.query || ""}`),
  );

const fail = (condition, message, failures) => {
  if (!condition) failures.push(message);
};

const validateCommon = ({ plan, validation }, failures) => {
  fail(Boolean(plan), "No planner plan returned", failures);
  fail(validation?.valid, "Plan failed validatePlannerPlan()", failures);

  if (nextStepsHaveBadBudgetText(plan)) {
    failures.push("nextSteps contain invalid text like '2000000 lakh'");
  }

  if (nextStepsMentionTestDrive(plan)) {
    failures.push("nextSteps should not mention test drive");
  }

  if (!hasNoLeadType(plan, "test_drive")) {
    failures.push("Plan should not contain leadType test_drive");
  }
};

const TESTS = [
  {
    id: "planner-001",
    query: "Verna pricelist",
    validate: ({ plan, firstTool, failures }) => {
      fail(plan.domain === "new_car", "Expected domain new_car", failures);
      fail(
        firstTool?.tool === "vehicle_pricelist",
        "Expected vehicle_pricelist",
        failures,
      );
      fail(hasModel(firstTool, "Verna"), "Expected model Verna", failures);
      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
    },
  },
  {
    id: "planner-002",
    query: "Show colors of Verna",
    validate: ({ plan, firstTool, failures }) => {
      fail(
        firstTool?.tool === "vehicle_colors",
        "Expected vehicle_colors",
        failures,
      );
      fail(hasModel(firstTool, "Verna"), "Expected model Verna", failures);
      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
    },
  },
  {
    id: "planner-003",
    query: "Does Verna SX have sunroof?",
    validate: ({ plan, firstTool, failures }) => {
      fail(
        firstTool?.tool === "vehicle_feature_lookup",
        "Expected vehicle_feature_lookup",
        failures,
      );
      fail(hasModel(firstTool, "Verna"), "Expected model Verna", failures);
      fail(hasVariant(firstTool, "SX"), "Expected variant SX", failures);
      fail(
        hasFeature(firstTool, "sunroof"),
        "Expected feature sunroof",
        failures,
      );
      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
      fail(
        hasContextVariant(plan, "SX"),
        "Expected contextPatch to preserve SX",
        failures,
      );
    },
  },
  {
    id: "planner-004",
    query: "Compare Verna and City",
    validate: ({ plan, firstTool, failures }) => {
      fail(
        firstTool?.tool === "vehicle_compare",
        "Expected vehicle_compare",
        failures,
      );

      const bag = getMergedEntitiesAndFilters(firstTool);

      fail(
        includesText(bag.models, "Verna") ||
          includesText(bag.comparisonModels, "Verna"),
        "Expected comparison models to include Verna",
        failures,
      );

      fail(
        includesText(bag.models, "City") ||
          includesText(bag.comparisonModels, "City"),
        "Expected comparison models to include City",
        failures,
      );

      fail(
        firstTool?.resolution?.variantSelectionMode ===
          "representative_default",
        "Model-only comparison should use representative_default variants",
        failures,
      );

      fail(
        plan?.ambiguity?.level === "soft_default",
        "Model-only comparison should set ambiguity.level soft_default",
        failures,
      );

      fail(
        plan?.ambiguity?.type === "comparison_variant",
        "Model-only comparison should set ambiguity.type comparison_variant",
        failures,
      );
    },
  },
  {
    id: "planner-005",
    query: "Best automatic SUV under 20 lakh with sunroof and 6 airbags",
    validate: ({ firstTool, failures }) => {
      fail(
        firstTool?.tool === "vehicle_recommend",
        "Expected vehicle_recommend",
        failures,
      );

      fail(
        hasAnyRanking(firstTool, ["feature_match", "automatic_value", "value"]),
        "Expected ranking feature_match OR automatic_value OR value",
        failures,
      );

      fail(
        hasBudgetMax(firstTool, 2000000),
        "Expected budgetMax 2000000",
        failures,
      );

      fail(hasBodyType(firstTool, "suv"), "Expected bodyType SUV", failures);

      fail(
        hasTransmission(firstTool, "automatic"),
        "Expected transmission automatic",
        failures,
      );

      fail(
        hasFeature(firstTool, "sunroof"),
        "Expected mustHaveFeatures sunroof",
        failures,
      );

      fail(
        hasFeature(firstTool, "6 airbags") || hasFeature(firstTool, "airbags"),
        "Expected mustHaveFeatures 6 airbags",
        failures,
      );
    },
  },
  {
    id: "planner-006",
    query: "Safest SUV under 20 lakh",
    validate: ({ firstTool, failures }) => {
      fail(
        firstTool?.tool === "vehicle_recommend",
        "Expected vehicle_recommend",
        failures,
      );

      fail(
        hasRanking(firstTool, "safety"),
        "Expected ranking safety",
        failures,
      );

      fail(
        hasBudgetMax(firstTool, 2000000),
        "Expected budgetMax 2000000",
        failures,
      );

      fail(hasBodyType(firstTool, "suv"), "Expected bodyType SUV", failures);
    },
  },
  {
    id: "planner-007",
    query: "EMI for Verna with 2 lakh down payment for 5 years",
    validate: ({ plan, firstTool, failures }) => {
      fail(firstTool?.tool === "vehicle_emi", "Expected vehicle_emi", failures);
      fail(hasModel(firstTool, "Verna"), "Expected model Verna", failures);
      fail(
        hasDownPayment(firstTool, 200000),
        "Expected downPayment 200000",
        failures,
      );
      fail(
        hasTenureMonths(firstTool, 60),
        "Expected tenureMonths 60",
        failures,
      );
      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
    },
  },
  {
    id: "planner-008",
    query: "Latest offers on Verna",
    validate: ({ plan, firstTool, failures }) => {
      fail(
        hasAnyTool(plan, ["aci_lead_capture", "unavailable"]),
        "Expected aci_lead_capture OR unavailable",
        failures,
      );

      if (firstTool?.tool === "aci_lead_capture") {
        fail(
          getLeadType(firstTool) === "offer_enquiry",
          "Expected leadType offer_enquiry",
          failures,
        );
      }

      const reason = getUnavailableReason(plan, firstTool);

      if (reason) {
        fail(
          reason === "offers_not_available" ||
            reason === "schemes_not_available",
          "Expected offers_not_available/schemes_not_available reason",
          failures,
        );
      }

      fail(
        !hasFakeOfferAmount(plan),
        "Planner included fake offer/discount amount",
        failures,
      );

      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
    },
  },
  {
    id: "planner-009",
    query: "Nearest Hyundai service center",
    validate: ({ plan, firstTool, failures }) => {
      fail(firstTool?.tool === "unavailable", "Expected unavailable", failures);

      const reason = getUnavailableReason(plan, firstTool);

      fail(
        reason === "service_centers_not_available",
        "Expected service_centers_not_available reason",
        failures,
      );
    },
  },
  {
    id: "planner-010",
    query: "Which bank gives best loan for Verna?",
    validate: ({ plan, firstTool, failures }) => {
      fail(
        hasAnyTool(plan, ["aci_lead_capture", "unavailable"]),
        "Expected aci_lead_capture OR unavailable",
        failures,
      );

      if (firstTool?.tool === "aci_lead_capture") {
        fail(
          getLeadType(firstTool) === "finance_callback",
          "Expected leadType finance_callback",
          failures,
        );
      }

      fail(
        !includesText(plan, "rank banks") &&
          !includesText(plan, "best bank is") &&
          !includesText(plan, "lowest roi bank"),
        "Planner appears to rank banks despite bank-wise schemes unavailable",
        failures,
      );

      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
    },
  },
  {
    id: "planner-011",
    query: "Loan closure 7077",
    validate: ({ plan, firstTool, failures }) => {
      const newCarTools = [
        "vehicle_pricelist",
        "vehicle_colors",
        "vehicle_feature_lookup",
        "vehicle_compare",
        "vehicle_recommend",
        "vehicle_price_breakup",
        "vehicle_emi",
        "vehicle_price_history",
        "vehicle_explainer",
        "aci_lead_capture",
      ];

      fail(
        plan.domain === "internal" ||
          firstTool?.tool === "internal_passthrough",
        "Expected internal domain OR internal_passthrough",
        failures,
      );

      fail(
        !getAllTools(plan).some((toolPlan) =>
          newCarTools.includes(toolPlan.tool),
        ),
        "Internal loan query should not return new-car vehicle tools",
        failures,
      );
    },
  },
  {
    id: "planner-012",
    query: "Does Verna SX get Titan Grey?",
    validate: ({ plan, firstTool, failures }) => {
      fail(
        hasAnyTool(plan, ["vehicle_colors", "aci_lead_capture", "unavailable"]),
        "Expected vehicle_colors OR aci_lead_capture OR unavailable",
        failures,
      );

      const reason = getUnavailableReason(plan, firstTool);
      const hasVariantWiseWarning =
        reason === "variant_wise_color_not_available" ||
        includesText(plan, "variant wise") ||
        includesText(plan, "variant-wise") ||
        includesText(plan, "model level") ||
        includesText(plan, "model-level") ||
        firstTool?.tool === "aci_lead_capture";

      fail(
        hasVariantWiseWarning,
        "Expected variant-wise color limitation or confirmation lead path",
        failures,
      );

      fail(
        !includesText(plan, "available in sx") &&
          !includesText(plan, "sx gets titan grey") &&
          !includesText(plan, "comes in sx"),
        "Planner appears to claim variant-wise color availability",
        failures,
      );
    },
  },
  {
    id: "planner-013",
    query:
      "Show Verna price in Delhi, compare with City, tell EMI for 5 years and check offers",
    validate: ({ plan, failures }) => {
      fail(
        plan.mode === "multi_tool",
        "Expected multi_tool for multi-intent query",
        failures,
      );

      fail(
        hasTool(plan, "vehicle_pricelist"),
        "Expected vehicle_pricelist in multi-intent plan",
        failures,
      );

      fail(
        hasTool(plan, "vehicle_compare"),
        "Expected vehicle_compare in multi-intent plan",
        failures,
      );

      fail(
        hasTool(plan, "vehicle_emi"),
        "Expected vehicle_emi in multi-intent plan",
        failures,
      );

      fail(
        hasAnyTool(plan, ["aci_lead_capture", "unavailable"]),
        "Expected offer enquiry as aci_lead_capture or unavailable",
        failures,
      );

      const priceTool = getTool(plan, "vehicle_pricelist");
      const compareTool = getTool(plan, "vehicle_compare");
      const emiTool = getTool(plan, "vehicle_emi");

      fail(
        hasModel(priceTool, "Verna"),
        "Expected Verna in price tool",
        failures,
      );
      fail(
        hasModel(compareTool, "Verna"),
        "Expected Verna in comparison tool",
        failures,
      );
      fail(
        hasModel(compareTool, "City"),
        "Expected City in comparison tool",
        failures,
      );
      fail(hasModel(emiTool, "Verna"), "Expected Verna in EMI tool", failures);
      fail(
        hasTenureMonths(emiTool, 60),
        "Expected EMI tenureMonths 60",
        failures,
      );

      fail(
        compareTool?.resolution?.variantSelectionMode ===
          "representative_default",
        "Expected representative_default comparison variants",
        failures,
      );
    },
  },
  {
    id: "planner-014",
    query: "EMI?",
    context: {
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      anchorCity: "new-delhi",
      selectedVehicle: {
        brand: "Hyundai",
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
    },
    validate: ({ plan, firstTool, failures }) => {
      fail(firstTool?.tool === "vehicle_emi", "Expected vehicle_emi", failures);
      fail(
        hasModel(firstTool, "Verna"),
        "Expected follow-up EMI to use anchor model Verna",
        failures,
      );
      fail(
        hasVariant(firstTool, "SX IVT"),
        "Expected follow-up EMI to use anchor variant SX IVT",
        failures,
      );
      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
      fail(
        hasContextVariant(plan, "SX IVT"),
        "Expected contextPatch to preserve SX IVT",
        failures,
      );
    },
  },
  {
    id: "planner-015",
    query: "Get quote",
    context: {
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      anchorCity: "new-delhi",
      selectedVehicle: {
        brand: "Hyundai",
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
    },
    validate: ({ plan, firstTool, failures }) => {
      fail(
        firstTool?.tool === "aci_lead_capture",
        "Expected aci_lead_capture",
        failures,
      );
      fail(
        getLeadType(firstTool) === "quotation",
        "Expected leadType quotation",
        failures,
      );
      fail(
        hasModel(firstTool, "Verna"),
        "Expected quote to use anchor model Verna",
        failures,
      );
      fail(
        hasVariant(firstTool, "SX IVT"),
        "Expected quote to use anchor variant SX IVT",
        failures,
      );
      fail(
        hasContextModel(plan, "Verna"),
        "Expected contextPatch to preserve Verna",
        failures,
      );
      fail(
        hasContextVariant(plan, "SX IVT"),
        "Expected contextPatch to preserve SX IVT",
        failures,
      );
    },
  },
  {
    id: "planner-016",
    query: "Compare with City",
    context: {
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      anchorCity: "new-delhi",
      selectedVehicle: {
        brand: "Hyundai",
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
    },
    validate: ({ plan, firstTool, failures }) => {
      fail(
        firstTool?.tool === "vehicle_compare",
        "Expected vehicle_compare",
        failures,
      );
      fail(
        hasModel(firstTool, "Verna"),
        "Expected comparison to include anchor model Verna",
        failures,
      );
      fail(
        hasModel(firstTool, "City"),
        "Expected comparison to include City",
        failures,
      );
      fail(
        hasVariant(firstTool, "SX IVT") ||
          includesText(firstTool?.resolution?.selectedVariants, "SX IVT"),
        "Expected comparison to preserve anchor variant SX IVT",
        failures,
      );
      fail(
        firstTool?.resolution?.variantSelectionMode ===
          "representative_default" ||
          firstTool?.resolution?.variantSelectionMode === "exact",
        "Expected exact or representative_default comparison resolution",
        failures,
      );
    },
  },
  {
    id: "planner-017",
    query: "Book test drive for Verna",
    validate: ({ plan, firstTool, failures }) => {
      fail(firstTool?.tool === "unavailable", "Expected unavailable", failures);

      const reason = getUnavailableReason(plan, firstTool);

      fail(
        reason === "outside_current_scope",
        "Expected outside_current_scope for disabled test-drive flow",
        failures,
      );

      fail(
        hasNoLeadType(plan, "test_drive"),
        "Plan should not contain test_drive leadType",
        failures,
      );

      fail(
        !nextStepsMentionTestDrive(plan),
        "Plan should not suggest test drive nextStep",
        failures,
      );
    },
  },
];

const runOne = async (test) => {
  const startedAt = Date.now();
  const failures = [];

  let result = null;
  let validation = null;
  let plan = null;
  let firstTool = null;

  try {
    result = await buildAiPlan({
      message: test.query,
      context: test.context || {},
      selectedEntity: test.selectedEntity || null,
      filters: test.filters || {},
      debug: process.env.ACI_TEST_DEBUG === "true",
      force: Boolean(test.force),
    });

    plan = result.plan;

    validation = validatePlannerPlan(plan, {
      message: test.query,
    });

    if (validation.valid && validation.plan) {
      plan = validation.plan;
    }

    firstTool = getFirstTool(plan);

    validateCommon({ plan, validation }, failures);

    test.validate({
      result,
      validation,
      plan,
      firstTool,
      failures,
    });
  } catch (error) {
    failures.push(error?.message || "Unknown planner test error");
  }

  const durationMs = result?.durationMs || Date.now() - startedAt;
  const pass = failures.length === 0;

  const row = {
    id: test.id,
    query: test.query,
    pass,
    failureReason: failures.join(" | "),
    plannerMode: result?.plannerMode || "",
    provider: result?.provider || "",
    model: result?.model || "",
    fallbackModelUsed: Boolean(result?.fallbackModelUsed),
    durationMs,
    domain: plan?.domain || "",
    mode: plan?.mode || "",
    conversationMode: plan?.conversationMode || "",
    customerStage: plan?.customerStage || "",
    confidence: plan?.confidence ?? null,
    tool: firstTool?.tool || "",
    allTools: getAllTools(plan).map((toolPlan) => toolPlan.tool),
    ranking: firstTool?.ranking || null,
    entities: firstTool?.entities || {},
    filters: firstTool?.filters || {},
    resolution: firstTool?.resolution || {},
    ambiguity: plan?.ambiguity || {},
    contextPatch: plan?.contextPatch || {},
    nextStepsCount: asArray(plan?.nextSteps).length,
    fallbackRequired: Boolean(result?.fallbackRequired),
    lowConfidence: Boolean(result?.lowConfidence),
  };

  if (process.env.ACI_TEST_SHOW_PLAN === "true") {
    row.plan = plan;
  }

  console.log(JSON.stringify(row, null, 2));

  return row;
};

const main = async () => {
  await connectDB();
  if (
    !process.env.GEMINI_API_KEY &&
    !process.env.GOOGLE_GENERATIVE_AI_API_KEY
  ) {
    console.error(
      JSON.stringify(
        {
          pass: false,
          error:
            "Missing GEMINI_API_KEY or GOOGLE_GENERATIVE_AI_API_KEY in environment.",
        },
        null,
        2,
      ),
    );
    await mongoose.connection.close();
    process.exit(1);
  }

  const results = [];

  for (const test of TESTS) {
    const result = await runOne(test);
    results.push(result);

    await sleep(150);
  }

  const failed = results.filter((item) => !item.pass);
  const passed = results.length - failed.length;

  const summary = {
    total: results.length,
    passed,
    failed: failed.length,
    failedIds: failed.map((item) => item.id),
    failedQueries: failed.map((item) => item.query),
  };

  console.log(JSON.stringify(summary, null, 2));

  if (failed.length) {
    process.exit(1);
  }

  setTimeout(() => process.exit(0), PLANNER_TIMEOUT_BUFFER_MS).unref?.();
};

main().catch((error) => {
  console.error(
    JSON.stringify(
      {
        pass: false,
        error: error?.message || "Planner test script failed",
        stack: error?.stack,
      },
      null,
      2,
    ),
  );
  process.exit(1);
});
