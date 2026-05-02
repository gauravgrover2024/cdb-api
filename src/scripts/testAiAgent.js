import dotenv from "dotenv";
import mongoose from "mongoose";
import connectDB from "../config/db.js";
import Loan from "../models/Loan.js";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";
import { AI_AGENT_FIELD_MAPS } from "../services/aiAgent/aiAgent.fieldMaps.js";
import { routeAiAgentIntent } from "../services/aiAgent/aiAgent.intentRouter.js";
import { getToolForIntent } from "../services/aiAgent/aiAgent.toolRegistry.js";

dotenv.config();

const adminUser = {
  _id: "000000000000000000000000",
  role: "admin",
  name: "AI Agent Test",
};

const baseTests = [
  {
    query: "Venue pricelist",
    expectedIntent: "vehicle_pricelist",
    assert: ({ response }) => {
      const isAmbiguous = response.widgets?.some(
        (w) => w.type === "model_ambiguity",
      );
      const isMixed =
        response.widgets?.[0]?.rows?.some((r) => /n line/i.test(r.model)) &&
        response.widgets?.[0]?.rows?.some((r) => !/n line/i.test(r.model));
      return isAmbiguous || !isMixed;
    },
  },
  {
    query: "Venue N Line pricelist",
    expectedIntent: "vehicle_pricelist",
    assert: ({ response }) => {
      const widget = widgetOf(response, "vehicle_pricelist");
      return (
        widget?.type === "vehicle_pricelist" &&
        widget?.rows?.every((r) =>
          /n line/i.test(r.model_normalized || r.model),
        )
      );
    },
  },
  {
    query: "Show all Venue models",
    expectedIntent: "vehicle_pricelist",
    allowedWidgets: ["vehicle_pricelist", "model_group_results"],
    assert: ({ response }) => {
      const widget =
        widgetOf(response, "vehicle_pricelist") ||
        widgetOf(response, "model_group_results");
      const rows = widget?.rows || widget?.groups || [];
      return Array.isArray(rows);
    },
  },
  {
    query: "Verna pricelist",
    expectedIntent: "vehicle_pricelist",
    assert: ({ response }) => {
      const widget = widgetOf(response, "vehicle_pricelist");
      return (
        widget?.type === "vehicle_pricelist" &&
        /delhi/i.test(widget?.city || "") &&
        widget?.rows?.length > 0
      );
    },
  },
  {
    query: "Prices in Gurgaon",
    context: { intent: "vehicle_pricelist", model: "verna", city: "delhi" },
    expectedIntent: "vehicle_city_change",
    assert: ({ response }) => {
      const widget = response.widgets?.[0];
      return (
        (widget?.type === "vehicle_pricelist" &&
          /gurgaon/i.test(widget?.city || widget?.data?.city || "")) ||
        widget?.type === "unavailable_notice"
      );
    },
  },
  {
    query: "Verna HX6 price",
    expectedIntent: "vehicle_pricelist",
    assert: ({ response }) =>
      response.widgets?.some((w) => w.type === "variant_ambiguity"),
  },
  {
    query: "Verna HX6 price breakup",
    expectedIntent: "vehicle_price_breakup",
    assert: ({ response }) =>
      response.widgets?.some(
        (w) =>
          w.type === "vehicle_price_breakup" || w.type === "variant_ambiguity",
      ),
  },
  {
    query: "Show features of Verna SX",
    expectedIntent: "vehicle_features",
    expectedWidgets: ["vehicle_features"],
  },
  {
    query: "Does Verna SX have sunroof?",
    expectedIntent: "vehicle_feature_answer",
    expectedWidgets: ["vehicle_feature_answer"],
    assert: ({ response }) => {
      const widget = widgetOf(response, "vehicle_feature_answer");
      return (
        !!widget?.answer &&
        (widget?.evidenceRows?.length > 0 ||
          widget?.data?.evidenceRows?.length > 0)
      );
    },
  },
  {
    query: "Which Verna variants have sunroof?",
    expectedIntent: "vehicle_feature_discovery",
    expectedWidgets: ["vehicle_feature_discovery"],
  },
  {
    query: "Which cars have 6 airbags under 20L?",
    expectedIntent: "vehicle_feature_discovery",
    expectedWidgets: ["vehicle_feature_discovery"],
  },
  {
    query: "Show colors of Verna",
    expectedIntent: "vehicle_colors",
    expectedWidgets: ["vehicle_colors"],
    assert: ({ response }) => {
      const widget = widgetOf(response, "vehicle_colors");
      const colors =
        widget?.colors || widget?.data?.colors || widget?.rows || [];
      return (
        colors.length > 0 &&
        colors.some((color) => color.imageUrl || color.image_url || color.hex)
      );
    },
  },
  {
    query: "Which cars are available in black?",
    expectedIntent: "vehicle_color_search",
    expectedWidgets: ["vehicle_color_search"],
  },
  {
    query: "Show similar cars to Verna",
    expectedIntent: "similar_cars",
    expectedWidgets: ["similar_cars"],
    assert: ({ response }) => {
      const widget = widgetOf(response, "similar_cars");
      return (
        (widget?.rows || []).length > 0 &&
        (widget?.rows || []).every(
          (r) => r.reason || r.score || r.matchedReason,
        )
      );
    },
  },
  {
    query: "Compare Verna City Slavia",
    expectedIntent: "vehicle_comparison",
    assert: ({ response }) => {
      const widget =
        widgetOf(response, "vehicle_model_comparison") ||
        widgetOf(response, "variant_selector");
      return (
        (widget?.models || []).length === 3 ||
        (widget?.options || []).length === 3
      );
    },
  },
  {
    query: "Compare selected variants",
    expectedIntent: "vehicle_comparison",
    allowedWidgets: [
      "vehicle_variant_comparison",
      "variant_selector",
      "unavailable_notice",
    ],
    context: { selectedVariants: ["v1", "v2"] },
    assert: ({ response }) => {
      const widgets = widgetTypesFor(response);

      // Dummy v1/v2 are not real DB IDs, so unavailable_notice is acceptable here.
      // Real selected-variant comparison should be tested separately after we fetch real variant IDs.
      return (
        widgets.includes("vehicle_variant_comparison") ||
        widgets.includes("variant_selector") ||
        widgets.includes("unavailable_notice")
      );
    },
  },
  {
    query: "SUVs under 20L",
    expectedIntent: "vehicle_budget_search",
    expectedWidgets: ["vehicle_recommendation_results"],
    expectedEntities: {
      bodyType: "suv",
      budgetMax: 2000000,
    },
    assert: ({ response, route }) => {
      const bodyType = String(route.entities?.bodyType || "").toLowerCase();
      const widget = widgetOf(response, "vehicle_recommendation_results");
      const rows = rowsOfWidget(widget);

      return bodyType === "suv" && Array.isArray(rows);
    },
  },
  {
    query: "Automatic SUVs under 20L",
    expectedIntent: "vehicle_budget_search",
    expectedWidgets: ["vehicle_recommendation_results"],
    expectedEntities: {
      bodyType: "suv",
      transmission: "automatic",
      budgetMax: 2000000,
    },
    assert: ({ response, route }) => {
      const bodyType = String(route.entities?.bodyType || "").toLowerCase();
      const transmission = String(
        route.entities?.transmission || "",
      ).toLowerCase();
      const widget = widgetOf(response, "vehicle_recommendation_results");
      const rows = rowsOfWidget(widget);

      return (
        bodyType === "suv" &&
        transmission === "automatic" &&
        Array.isArray(rows)
      );
    },
  },
  {
    query: "Cars with sunroof under 15L",
    expectedIntent: "vehicle_feature_discovery",
    expectedWidgets: ["vehicle_feature_discovery"],
  },
  {
    query: "Best family car under 20L",
    expectedIntent: "vehicle_use_case_recommendation",
    expectedWidgets: ["vehicle_recommendation_results"],
    assert: ({ response }) => {
      const widget = widgetOf(response, "vehicle_recommendation_results");
      return (
        (widget?.rows || []).length > 0 &&
        (widget?.rows || []).every(
          (r) => r.matchedReasons || r.reason || r.matchedReason,
        )
      );
    },
  },
  {
    query:
      "EMI for Verna HX6 with 2 lakh down payment for 5 years at 9 percent",
    expectedIntent: "vehicle_emi_calculator",
    allowedWidgets: ["vehicle_emi_calculator", "variant_ambiguity"],
    forbiddenEntities: ["last4"],
    expectedEntities: {
      model: "verna",
      variant: "HX6",
      downPayment: 200000,
      tenureMonths: 60,
      annualRate: 9,
    },
    assert: ({ response, route }) => {
      const widgets = widgetTypesFor(response);

      if (route.entities?.last4) return false;
      if (route.entities?.transmission === "at") return false;

      return (
        widgets.includes("vehicle_emi_calculator") ||
        widgets.includes("variant_ambiguity")
      );
    },
  },
  {
    query: "Cars with EMI under 25000",
    expectedIntent: "vehicle_emi_budget_search",
  },
  {
    query: "Show Verna price history",
    expectedIntent: "vehicle_price_history",
  },
  {
    query: "Safest SUVs under 20L",
    expectedIntent: "vehicle_safety_expert",
    allowedWidgets: [
      "vehicle_safety_results",
      "vehicle_recommendation_results",
      "unavailable_notice",
    ],
    expectedEntities: {
      budgetMax: 2000000,
    },
    assert: ({ response, route }) => {
      const bodyType = String(route.entities?.bodyType || "").toLowerCase();
      const widgets = widgetTypesFor(response);

      return (
        ["suv", "suvs"].includes(bodyType) &&
        (widgets.includes("vehicle_safety_results") ||
          widgets.includes("vehicle_recommendation_results") ||
          widgets.includes("unavailable_notice"))
      );
    },
  },
  {
    query: "Which Verna variant should I buy?",
    expectedIntent: "vehicle_best_variant_recommendation",
  },
  {
    query: "Difference between Verna HX6 and HX8",
    expectedIntent: "vehicle_variant_difference",
  },
];

const physicalCollectionsFor = (tool) =>
  (tool?.collectionsUsed || []).map(
    (key) => AI_AGENT_FIELD_MAPS[key]?.collectionName || key,
  );

const widgetTypesFor = (response) => {
  if (response.ambiguity)
    return [
      "ambiguity",
      ...(response.widgets || []).map((widget) => widget.type).filter(Boolean),
    ];
  return (response.widgets || []).map((widget) => widget.type).filter(Boolean);
};

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
  if (Array.isArray(widget.data?.groupedByModel))
    return widget.data.groupedByModel;
  if (Array.isArray(widget.groupedByModel)) return widget.groupedByModel;
  if (Array.isArray(widget.groups)) return widget.groups;

  return [];
};

const widgetOf = (response, type) =>
  (response.widgets || []).find((widget) => widget.type === type);

const matchedCountFor = (response) => {
  const topLevelAmbiguityCount = response.ambiguity?.options?.length || 0;

  const widgetCount = (response.widgets || []).reduce((sum, widget) => {
    const directCount =
      widget?.summary?.total ?? widget?.data?.total ?? widget?.total ?? null;

    if (directCount !== null && directCount !== undefined) {
      return sum + (Number(directCount) || 0);
    }

    return sum + rowsOfWidget(widget).length;
  }, 0);

  return topLevelAmbiguityCount + widgetCount;
};

const hasRecords = (response) =>
  (response.widgets || []).some((widget) => rowsOfWidget(widget).length > 0);

const buildLine = ({ test, route, tool, response }) => {
  const widgetTypes = widgetTypesFor(response);
  const physicalCollections = physicalCollectionsFor(tool);
  const failures = [];

  if (test.expectedEntities) {
    for (const [key, expectedValue] of Object.entries(test.expectedEntities)) {
      const actualValue = route.entities?.[key];

      if (Array.isArray(expectedValue)) {
        if (!expectedValue.includes(actualValue)) {
          failures.push(
            `entity ${key} expected one of ${expectedValue.join(", ")}, got ${actualValue}`,
          );
        }
      } else if (actualValue !== expectedValue) {
        failures.push(
          `entity ${key} expected ${expectedValue}, got ${actualValue}`,
        );
      }
    }
  }

  if (test.forbiddenEntities?.length) {
    for (const key of test.forbiddenEntities) {
      if (route.entities?.[key]) {
        failures.push(
          `forbidden entity ${key} was extracted as ${route.entities[key]}`,
        );
      }
    }
  }

  if (test.expectedIntent && route.intent !== test.expectedIntent)
    failures.push(
      `intent expected ${test.expectedIntent}, got ${route.intent}`,
    );
  if (test.forbiddenIntent && route.intent === test.forbiddenIntent)
    failures.push(`forbidden intent ${test.forbiddenIntent}`);
  if (route.structured && widgetTypes.length === 0)
    failures.push("structured intent returned blank widgetType");
  if (
    test.expectedWidgets?.length &&
    !test.expectedWidgets.some((widget) => widgetTypes.includes(widget))
  ) {
    failures.push(
      `expected widget ${test.expectedWidgets.join(" or ")}, got ${widgetTypes.join(", ") || "none"}`,
    );
  }
  if (
    test.allowedWidgets?.length &&
    !test.allowedWidgets.some((widget) => widgetTypes.includes(widget))
  ) {
    failures.push(
      `allowed widgets ${test.allowedWidgets.join(", ")}, got ${widgetTypes.join(", ") || "none"}`,
    );
  }
  if (test.requiredWidget && !widgetTypes.includes(test.requiredWidget))
    failures.push(`missing required widget ${test.requiredWidget}`);
  if (test.requireRecords && !hasRecords(response))
    failures.push("expected actual records, got count-only response");
  if (
    test.expectedPhysicalCollections?.length &&
    !test.expectedPhysicalCollections.some((collection) =>
      physicalCollections.includes(collection),
    )
  ) {
    failures.push(
      `expected physical collection ${test.expectedPhysicalCollections.join(" or ")}, got ${physicalCollections.join(", ") || "none"}`,
    );
  }
  if (test.assert && !test.assert({ response, route, tool }))
    failures.push("custom assertion failed");

  return {
    query: test.query,
    intent: route.intent,
    entities: route.entities,
    selectedTool: tool?.intent || "generic_search",
    logicalAdapterUsed: tool?.collectionsUsed || route.collections || [],
    physicalCollectionUsed: physicalCollections,
    widgetType: widgetTypes[0] || "",
    widgetTypes,
    matchedCount: matchedCountFor(response),
    pass: failures.length === 0,
    failureReason: failures.join("; "),
  };
};

const selectedEntityTest = async () => {
  const loan = await Loan.findOne({
    $or: [
      { rc_redg_no: /7077$/i },
      { registrationNumber: /7077$/i },
      { vehicleRegNo: /7077$/i },
    ],
  }).lean();
  if (!loan) return null;
  return {
    query: "Loan closure 7077",
    expectedIntent: "loan_closure_pos",
    allowedWidgets: ["loan_closure_card", "unavailable_notice"],
    expectedPhysicalCollections: ["loans"],
    selectedEntity: {
      id: String(loan._id),
      entityType: "loan",
      customerName: loan.customerName,
      registrationNumber:
        loan.rc_redg_no || loan.registrationNumber || loan.vehicleRegNo,
      context: { loanId: loan.loanId },
    },
    assert: ({ response }) => {
      const data = response.widgets?.[0]?.data || {};
      return !data.id || data.id === String(loan._id);
    },
  };
};

const run = async () => {
  await connectDB();
  const dynamicTest = await selectedEntityTest();
  const tests = dynamicTest ? [...baseTests, dynamicTest] : baseTests;
  let failed = 0;
  for (const test of tests) {
    const route = routeAiAgentIntent({
      message: test.query,
      context: test.context || {},
      selectedEntity: test.selectedEntity,
    });
    const tool = getToolForIntent(route.intent);
    const response = await chatWithAgent({
      message: test.query,
      context: test.context || {},
      selectedEntity: test.selectedEntity,
      user: adminUser,
      debug: true,
    });
    const line = buildLine({ test, route, tool, response });
    if (!line.pass) failed += 1;
    console.log(JSON.stringify(line, null, 2));
  }
  await mongoose.connection.close();
  if (failed) {
    console.error(`ACI Assist harness failed ${failed} test(s).`);
    process.exit(1);
  }
  console.log("ACI Assist harness passed.");
};

run().catch(async (error) => {
  console.error("ACI Assist harness crashed:", error);
  await mongoose.connection.close().catch(() => {});
  process.exit(1);
});
