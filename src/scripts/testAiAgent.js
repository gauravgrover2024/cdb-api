import dotenv from "dotenv";
import fs from "fs";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import { parseAgentMessage } from "../services/aiAgent/aiAgent.intentParser.js";
import { getToolForIntent } from "../services/aiAgent/aiAgent.toolRegistry.js";
import {
  NEW_CAR_CANVAS_TYPES,
  NEW_CAR_INLINE_TYPES,
} from "../services/aiAgent/aiAgent.newCarQuestionMap.js";

dotenv.config();

const adminUser = {
  _id: "000000000000000000000000",
  role: "admin",
  name: "ACI Assist Test Harness",
};

const TEST_CASES = [
  // Core
  { query: "Verna pricelist", expectIntent: "vehicle_pricelist" },
  { query: "Verna price in Mumbai", expectIntent: "vehicle_city_price" },
  { query: "Verna SX price", expectIntent: "vehicle_variant_price" },
  { query: "Show colors of Verna", expectIntent: "vehicle_colors" },
  { query: "Show features of Verna", expectIntent: "vehicle_model_features_explorer" },
  { query: "Does Verna SX have sunroof?", expectIntent: "vehicle_feature_answer", expectInline: true },
  { query: "Which Verna variants have sunroof?", expectIntent: "vehicle_feature_discovery" },
  { query: "SUVs under 20L", expectIntent: "vehicle_budget_search", expectModelGrouped: true, expectLeading: true },
  { query: "Safest SUVs under 20L", expectIntent: "vehicle_safety_search", expectModelGrouped: true, expectLeading: true },
  { query: "Automatic cars under 15 lakh", expectIntent: "vehicle_budget_search", expectModelGrouped: true, expectLeading: true },
  { query: "Compare Verna City Slavia", expectIntent: "vehicle_comparison" },
  { query: "Cars similar to Verna", expectIntent: "vehicle_similar_cars" },
  { query: "Which Verna variant should I buy?", expectIntent: "vehicle_variant_recommendation" },
  { query: "Difference between Verna SX and SX(O)", expectIntent: "vehicle_variant_upgrade_value" },
  { query: "EMI for Verna with 90% loan for 5 years at 9 percent", expectIntent: "vehicle_emi_calculator" },
  { query: "EMI for Verna with 2 lakh down payment", expectIntent: "vehicle_emi_calculator" },
  { query: "What documents are required for car loan?", expectIntent: "new_car_finance_faq", expectInline: true },
  { query: "Latest offers on Verna", expectIntent: "vehicle_offers" },
  { query: "Get quotation for Verna SX in Delhi", expectIntent: "aci_new_car_quotation" },
  { query: "Nearest Hyundai service center in Delhi", expectIntent: "new_car_service_center_search" },
  { query: "Verna service cost", expectIntent: "new_car_service_cost" },
  { query: "Is Verna available in Delhi?", expectIntent: "vehicle_availability" },

  // Analytical
  { query: "Which car is cheapest to own for 5 years?", expectIntent: "vehicle_tco_analysis", expectLeading: true },
  { query: "Should I buy petrol or diesel for Creta?", expectIntent: "vehicle_fuel_decision_advisor", expectLeading: true },
  { query: "Which car has best resale value?", expectIntent: "vehicle_resale_value_analysis", expectLeading: true },
  { query: "Best car for my lifestyle", expectIntent: "vehicle_lifestyle_fit_score", expectLeading: true },
  { query: "Best car for parents", expectIntent: "vehicle_senior_friendly_recommendation", expectLeading: true },
  { query: "Most spacious cars under 20 lakh", expectIntent: "vehicle_space_practicality_advisor", expectLeading: true },
  { query: "Best performance car under 20 lakh", expectIntent: "vehicle_performance_advisor", expectLeading: true },
  { query: "Cars with highest ground clearance", expectIntent: "vehicle_spec_ranking", expectLeading: true },
  { query: "I want automatic, sunroof and 6 airbags under 15 lakh", expectIntent: "vehicle_must_have_feature_builder", expectLeading: true },
  { query: "My monthly budget is 30000, which car can I buy?", expectIntent: "vehicle_monthly_budget_planner", expectLeading: true },
  { query: "What extra features do I get by paying 1.5 lakh more?", expectIntent: "vehicle_variant_upgrade_value" },

  // Ambiguity
  { query: "Show Venue price", expectIntent: "vehicle_model_ambiguity", expectInline: true, allowAmbiguityFallback: true },
  { query: "Verna SX price", expectIntent: "vehicle_variant_price", allowVariantAmbiguity: true },

  // Multi-intent
  { query: "Show Verna price, colors and EMI", expectIntent: "vehicle_pricelist", expectMulti: true },
  {
    query: "Does Verna SX have sunroof and what is EMI with 2 lakh down payment?",
    expectIntent: "vehicle_emi_calculator",
    expectMulti: true,
  },
  { query: "Compare Creta and Seltos and tell me which has better mileage", expectIntent: "vehicle_comparison", expectMulti: true },
  { query: "Show offers and quotation for Safari in Delhi", expectIntent: "aci_new_car_quotation", expectMulti: true },
  { query: "Find Hyundai service center and service cost for Verna", expectIntent: "new_car_service_center_search", expectMulti: true },

  // Out of scope
  { query: "Loan closure 7077", expectIntent: "new_car_unavailable_or_out_of_scope", expectInline: true },
  { query: "Sell my used car", expectIntent: "new_car_unavailable_or_out_of_scope", expectInline: true },

  // City fallback transparency
  { query: "Verna price in Patna", expectIntent: "vehicle_city_price", expectFallbackNotice: true },
];

const rowsOfWidget = (widget = {}) => {
  if (!widget) return [];
  if (Array.isArray(widget.rows)) return widget.rows;
  if (Array.isArray(widget.records)) return widget.records;
  if (Array.isArray(widget.options)) return widget.options;
  if (Array.isArray(widget.colors)) return widget.colors;
  if (Array.isArray(widget.models)) return widget.models;
  if (Array.isArray(widget.variants)) return widget.variants;
  if (Array.isArray(widget.data?.rows)) return widget.data.rows;
  if (Array.isArray(widget.data?.records)) return widget.data.records;
  if (Array.isArray(widget.data?.options)) return widget.data.options;
  if (Array.isArray(widget.data?.groupedByModel)) return widget.data.groupedByModel;
  if (Array.isArray(widget.groupedByModel)) return widget.groupedByModel;
  return [];
};

const matchedCountFor = (response) =>
  (response.widgets || []).reduce((sum, widget) => {
    const directCount =
      widget?.summary?.total ?? widget?.data?.total ?? widget?.total ?? null;
    if (directCount !== null && directCount !== undefined) {
      return sum + (Number(directCount) || 0);
    }
    return sum + rowsOfWidget(widget).length;
  }, response?.ambiguity?.options?.length || 0);

const containsAny = (text = "", patterns = []) =>
  patterns.some((item) => text.toLowerCase().includes(String(item).toLowerCase()));

const hasGroupedModels = (response = {}) => {
  const primary = response.widgets?.[0] || {};
  if (Array.isArray(primary.modelCards) && primary.modelCards.length) return true;
  if (Array.isArray(primary.groupedByModel) && primary.groupedByModel.length)
    return true;
  if (Array.isArray(primary.data?.groupedByModel) && primary.data.groupedByModel.length)
    return true;
  const rows = rowsOfWidget(primary);
  if (!rows.length) return false;
  return rows.every((row) => row.model && !row.variant);
};

const checkFrontendRegistryCoverage = () => {
  const registryPath =
    "/Users/gauravgrover/cdb-frontend/src/components/aci-assist/canvasRegistry.js";

  const source = fs.readFileSync(registryPath, "utf8");

  const missingCanvas = NEW_CAR_CANVAS_TYPES.filter((type) =>
    !new RegExp(`\\b${type}\\s*:`).test(source),
  );
  const missingInline = NEW_CAR_INLINE_TYPES.filter((type) =>
    !new RegExp(`\\b${type}\\s*:`).test(source),
  );

  return {
    pass: missingCanvas.length === 0 && missingInline.length === 0,
    missingCanvas,
    missingInline,
  };
};

const validateLine = ({ test, parsed, tool, response }) => {
  const failures = [];

  if (test.expectIntent && response.intent !== test.expectIntent) {
    if (
      !(test.allowAmbiguityFallback && response.intent === "vehicle_pricelist") &&
      !(test.allowVariantAmbiguity && response.intent === "vehicle_variant_ambiguity")
    ) {
      failures.push(`intent expected ${test.expectIntent}, got ${response.intent}`);
    }
  }

  if (
    response.intent !== "new_car_unavailable_or_out_of_scope" &&
    !response.canvasType &&
    !response.inlineType
  ) {
    failures.push("structured intent has no canvasType/inlineType");
  }

  if ((response.widgets || []).length === 0) {
    failures.push("no widget returned for structured response");
  }

  if ((response.actions || []).length === 0) {
    failures.push("no actions returned");
  }

  if (test.expectLeading && (response.leadingQuestions || []).length === 0) {
    failures.push("leadingQuestions missing for exploratory intent");
  }

  if (test.expectModelGrouped && !hasGroupedModels(response)) {
    failures.push("broad recommendation is not grouped by model");
  }

  if (test.expectFallbackNotice) {
    const notices = (response.widgets || [])
      .flatMap((widget) => [
        ...(Array.isArray(widget.notices) ? widget.notices : []),
        ...(Array.isArray(widget.data?.notices) ? widget.data.notices : []),
        widget?.data?.message,
      ])
      .filter(Boolean)
      .join(" ");

    if (!containsAny(notices, ["showing", "fallback", "instead"])) {
      failures.push("unsupported city fallback notice missing");
    }
  }

  if (test.query.toLowerCase().includes("color")) {
    const modules = (response.sourceTransparency?.modulesChecked || []).map(
      (item) => item.module,
    );
    if (!modules.some((item) => /color/i.test(item))) {
      failures.push("colors query did not use color collection/module trace");
    }
  }

  if (test.query.toLowerCase().includes("sunroof") || test.query.toLowerCase().includes("feature")) {
    const modules = (response.sourceTransparency?.modulesChecked || []).map(
      (item) => item.module,
    );
    const hasFeatureModule = modules.some((item) => /feature/i.test(item));
    if (!hasFeatureModule) {
      failures.push("feature query did not use feature collection/module trace");
    }
  }

  if (test.expectInline && response.displayMode === "canvas") {
    failures.push("expected inline response but got canvas mode");
  }

  const fullDump = JSON.stringify(response);
  if (
    containsAny(fullDump, [
      "Himgiri Hyundai",
      "2-4 Weeks",
      "Value Retention",
      "Family Explorer",
    ])
  ) {
    failures.push("dummy data marker detected in response payload");
  }

  return failures;
};

const run = async () => {
  await connectDB();

  const registryCoverage = checkFrontendRegistryCoverage();
  if (!registryCoverage.pass) {
    console.log("[FAIL] Frontend canvas registry coverage check", registryCoverage);
  } else {
    console.log("[PASS] Frontend canvas registry coverage check");
  }

  const lines = [];

  for (const test of TEST_CASES) {
    try {
      const parsed = parseAgentMessage(test.query, {}, null, {});
      const tool = getToolForIntent(parsed.intent);

      const response = await chatWithAgent({
        message: test.query,
        context: {},
        selectedEntity: null,
        filters: {},
        user: adminUser,
      });

      const failures = validateLine({ test, parsed, tool, response });

      const line = {
        query: test.query,
        detectedIntent: response.intent || parsed.intent,
        displayMode: response.displayMode || "",
        canvasType: response.canvasType || null,
        inlineType: response.inlineType || null,
        selectedTool: tool?.intent || "generic_search",
        collectionsModulesUsed: (response.sourceTransparency?.modulesChecked || []).map(
          (item) => item.module,
        ),
        matchedCount: matchedCountFor(response),
        leadingQuestionsCount: (response.leadingQuestions || []).length,
        actionsCount: (response.actions || []).length,
        pass: failures.length === 0,
        failureReason: failures.join("; "),
      };

      lines.push(line);
      console.log(JSON.stringify(line, null, 2));
    } catch (error) {
      const line = {
        query: test.query,
        detectedIntent: "error",
        displayMode: "",
        canvasType: null,
        inlineType: null,
        selectedTool: "error",
        collectionsModulesUsed: [],
        matchedCount: 0,
        leadingQuestionsCount: 0,
        actionsCount: 0,
        pass: false,
        failureReason: error?.message || "Unhandled error",
      };
      lines.push(line);
      console.log(JSON.stringify(line, null, 2));
    }
  }

  const failed = lines.filter((line) => !line.pass).length;
  const passed = lines.length - failed;

  console.log("\n--- Summary ---");
  console.log(
    JSON.stringify(
      {
        total: lines.length,
        passed,
        failed,
        frontendRegistryCoverage: registryCoverage,
      },
      null,
      2,
    ),
  );

  await mongoose.connection.close();
  process.exit(failed > 0 || !registryCoverage.pass ? 1 : 0);
};

run().catch(async (error) => {
  console.error("ACI Assist test harness crashed:", error);
  await mongoose.connection.close().catch(() => {});
  process.exit(1);
});
