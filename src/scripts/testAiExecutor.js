import "dotenv/config";
import mongoose from "mongoose";

import { executeAciPlannerPlan } from "../services/aiAgent/aiAgent.executor.js";

/**
 * ACI Assist Executor Smoke Test
 *
 * Purpose:
 * - Test new planner -> executor -> responseTools -> sanitizer flow.
 * - Does not use old toolRegistry / old aiAgent.tools.
 * - DB connection is optional. If Mongo connects, runtime rows may come from DB.
 * - If Mongo does not connect, executor should still return valid response contract.
 */

const MONGO_URI =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DB_URI ||
  process.env.DATABASE_URL ||
  "";

const SHOULD_CONNECT_DB = process.env.ACI_EXECUTOR_SKIP_DB !== "true";

const safeJson = (value) => JSON.stringify(value, null, 2);

const connectMongoIfAvailable = async () => {
  if (!SHOULD_CONNECT_DB) {
    console.log("Skipping MongoDB connection because ACI_EXECUTOR_SKIP_DB=true");
    return;
  }

  if (!MONGO_URI) {
    console.log("No Mongo URI found. Running executor smoke without DB.");
    return;
  }

  if (mongoose.connection.readyState === 1) return;

  try {
    await mongoose.connect(MONGO_URI, {
      serverSelectionTimeoutMS: 6000,
    });
    console.log("MongoDB connected");
  } catch (error) {
    console.log("MongoDB not connected. Running executor smoke without DB.");
    console.log(error?.message || error);
  }
};

const closeMongo = async () => {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
};

const basePlan = ({
  tool,
  entities = {},
  filters = {},
  ranking = null,
  output = {},
  mode = "single_tool",
  conversationMode = "direct_answer",
  customerStage = "exploration",
  tools = null,
}) => ({
  mode,
  domain: "new_car",
  conversationMode,
  customerStage,
  confidence: 0.98,
  tools:
    tools ||
    [
      {
        tool,
        entities,
        filters: {
          city: "new-delhi",
          activeOnly: true,
          ...filters,
        },
        ranking,
        output,
      },
    ],
  nextSteps: [],
  clarification: null,
  reasoningSummary: "Executor smoke test plan.",
  unavailableReason: null,
});

const cases = [
  {
    id: "executor-price-001",
    query: "Verna pricelist",
    context: {},
    plan: basePlan({
      tool: "vehicle_pricelist",
      entities: {
        model: "Verna",
        primaryModel: "Verna",
      },
      filters: {
        priceBasis: "on_road",
      },
      output: {
        canvasType: "pricelist_canvas",
      },
    }),
  },
  {
    id: "executor-feature-001",
    query: "Does Verna SX have sunroof?",
    context: {},
    plan: basePlan({
      tool: "vehicle_feature_lookup",
      entities: {
        model: "Verna",
        primaryModel: "Verna",
        variant: "SX",
        primaryVariant: "SX",
        feature: "sunroof",
      },
      filters: {},
      output: {
        inlineType: "feature_answer_card",
      },
    }),
  },
  {
    id: "executor-emi-001",
    query: "EMI for Verna SX IVT with 2 lakh down payment",
    context: {},
    plan: basePlan({
      tool: "vehicle_emi",
      entities: {
        model: "Verna",
        primaryModel: "Verna",
        variant: "SX IVT",
        primaryVariant: "SX IVT",
      },
      filters: {
        priceBasis: "on_road",
        downPayment: 200000,
        tenureMonths: 60,
      },
      conversationMode: "calculation",
      customerStage: "consideration",
      output: {
        canvasType: "emi_calculator_canvas",
      },
    }),
  },
  {
    id: "executor-multi-001",
    query: "Show Verna price in Delhi, compare with City, tell EMI for 5 years and check offers",
    context: {},
    plan: basePlan({
      mode: "multi_tool",
      conversationMode: "comparison",
      customerStage: "consideration",
      tool: "vehicle_pricelist",
      tools: [
        {
          tool: "vehicle_pricelist",
          entities: {
            model: "Verna",
            primaryModel: "Verna",
          },
          filters: {
            city: "new-delhi",
            activeOnly: true,
            priceBasis: "on_road",
          },
          ranking: null,
          output: {
            canvasType: "pricelist_canvas",
          },
        },
        {
          tool: "vehicle_compare",
          entities: {
            model: "Verna",
            primaryModel: "Verna",
            models: ["Verna", "City"],
            comparisonModels: ["Verna", "City"],
          },
          filters: {
            city: "new-delhi",
            activeOnly: true,
            priceBasis: "on_road",
          },
          ranking: null,
          output: {
            canvasType: "comparison_canvas",
          },
        },
        {
          tool: "vehicle_emi",
          entities: {
            model: "Verna",
            primaryModel: "Verna",
          },
          filters: {
            city: "new-delhi",
            activeOnly: true,
            priceBasis: "on_road",
            tenureMonths: 60,
          },
          ranking: null,
          output: {
            canvasType: "emi_calculator_canvas",
          },
        },
        {
          tool: "aci_lead_capture",
          entities: {
            model: "Verna",
            primaryModel: "Verna",
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          },
          filters: {
            city: "new-delhi",
            activeOnly: true,
            leadType: "offer_enquiry",
            selectedServices: ["offer_enquiry", "quotation"],
          },
          ranking: null,
          output: {
            canvasType: "aci_quotation_canvas",
          },
        },
      ],
    }),
  },
  {
    id: "executor-context-001",
    query: "Compare with City",
    context: {
      selectedVehicle: {
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
      anchorModel: "Verna",
      anchorVariant: "SX IVT",
      anchorCity: "new-delhi",
    },
    plan: basePlan({
      tool: "vehicle_compare",
      entities: {
        model: "Verna",
        primaryModel: "Verna",
        variant: "SX IVT",
        primaryVariant: "SX IVT",
        models: ["Verna", "City"],
        comparisonModels: ["Verna", "City"],
      },
      filters: {
        priceBasis: "on_road",
      },
      conversationMode: "comparison",
      customerStage: "evaluation",
      output: {
        canvasType: "comparison_canvas",
      },
    }),
  },
  {
    id: "executor-lead-001",
    query: "Best price for black Verna SX automatic",
    context: {},
    plan: basePlan({
      tool: "aci_lead_capture",
      entities: {
        model: "Verna",
        primaryModel: "Verna",
        variant: "SX",
        primaryVariant: "SX",
        color: "black",
        transmission: "automatic",
        leadType: "quotation",
        selectedServices: ["quotation"],
      },
      filters: {
        leadType: "quotation",
        selectedServices: ["quotation"],
      },
      conversationMode: "lead_capture",
      customerStage: "closing",
      output: {
        canvasType: "aci_quotation_canvas",
      },
    }),
  },
  {
    id: "executor-unavailable-001",
    query: "black available?",
    context: {
      selectedVehicle: {
        model: "Verna",
        variant: "SX IVT",
        city: "new-delhi",
      },
    },
    plan: basePlan({
      tool: "unavailable",
      entities: {
        model: "Verna",
        primaryModel: "Verna",
        variant: "SX IVT",
        primaryVariant: "SX IVT",
      },
      filters: {
        unavailableReason: "variant_wise_color_not_available",
      },
      output: {
        inlineType: "unavailable_notice",
      },
      mode: "unavailable",
      conversationMode: "unavailable",
    }),
  },
  {
    id: "executor-internal-001",
    query: "Loan closure 7077",
    context: {},
    plan: {
      mode: "single_tool",
      domain: "internal",
      conversationMode: "internal_passthrough",
      customerStage: "unknown",
      confidence: 0.98,
      tools: [
        {
          tool: "internal_passthrough",
          entities: {},
          filters: {},
          ranking: null,
          output: {},
        },
      ],
      nextSteps: [],
      clarification: null,
      reasoningSummary: "Internal passthrough smoke test.",
      unavailableReason: null,
    },
    runtimeHints: {
      internalResult: {
        intent: "loan_closure_lookup",
        title: "Loan closure",
        answer: "I found internal CDrive records for loan closure 7077.",
        data: {
          loanId: "7077",
        },
        actions: [],
      },
    },
  },
];

const pickContractSummary = (response) => ({
  intent: response.intent,
  mode: response.mode,
  displayMode: response.displayMode,
  canvasType: response.canvasType,
  inlineType: response.inlineType,
  title: response.title,
  answer: response.answer,
  actions: (response.actions || []).map((item) => ({
    label: item.label,
    query: item.query,
    intent: item.intent,
  })),
  leadingQuestions: (response.leadingQuestions || []).map((item) => ({
    label: item.label,
    query: item.query,
  })),
  contextPatch: response.contextPatch,
  secondaryCount: response.secondaryResponses?.length || 0,
  contractValid: response.executor?.contractValidation?.valid,
  contractErrors: response.executor?.contractValidation?.errors || [],
  runtimeResultsMeta: response.executor?.runtimeResultsMeta || [],
});

const assertCase = ({ testCase, response }) => {
  const failures = [];

  if (!response.intent) failures.push("Missing intent");
  if (!response.displayMode) failures.push("Missing displayMode");
  if (!response.data || typeof response.data !== "object") failures.push("Missing data object");
  if (!Array.isArray(response.actions)) failures.push("actions is not array");
  if (!Array.isArray(response.leadingQuestions)) {
    failures.push("leadingQuestions is not array");
  }
  if (!response.contextPatch || typeof response.contextPatch !== "object") {
    failures.push("Missing contextPatch");
  }
  if (response.executor?.contractValidation?.valid === false) {
    failures.push(`Contract invalid: ${(response.executor.contractValidation.errors || []).join(", ")}`);
  }

  const blob = JSON.stringify(response).toLowerCase();

  if (blob.includes("test drive") || blob.includes("test_drive")) {
    failures.push("Test drive CTA/text should not appear");
  }

  if (testCase.id === "executor-internal-001") {
    if (blob.includes("show price") || blob.includes("calculate emi") || blob.includes("get quotation")) {
      failures.push("Internal result contains new-car CTA");
    }
  }

  if (testCase.id === "executor-multi-001") {
    if ((response.secondaryResponses || []).length < 3) {
      failures.push("Multi intent should have secondary responses");
    }
  }

  if (testCase.id === "executor-lead-001") {
    if (!blob.includes("exact automatic variant")) {
      failures.push("Loose automatic quote should ask exact automatic variant");
    }
  }

  return failures;
};

const main = async () => {
  await connectMongoIfAvailable();

  const results = [];

  for (const testCase of cases) {
    const started = Date.now();

    const response = await executeAciPlannerPlan({
      plan: testCase.plan,
      userMessage: testCase.query,
      context: testCase.context || {},
      runtimeHints: testCase.runtimeHints || {},
    });

    const failures = assertCase({ testCase, response });

    const result = {
      id: testCase.id,
      query: testCase.query,
      pass: failures.length === 0,
      durationMs: Date.now() - started,
      failures,
      summary: pickContractSummary(response),
    };

    results.push(result);
    console.log(safeJson(result));
  }

  const failed = results.filter((item) => !item.pass);

  console.log(
    safeJson({
      suite: "ACI Assist executor smoke",
      total: results.length,
      passed: results.length - failed.length,
      failed: failed.length,
      failedIds: failed.map((item) => item.id),
    }),
  );

  await closeMongo();

  if (failed.length) {
    process.exitCode = 1;
  }
};

main().catch(async (error) => {
  console.error(error);
  await closeMongo();
  process.exitCode = 1;
});
