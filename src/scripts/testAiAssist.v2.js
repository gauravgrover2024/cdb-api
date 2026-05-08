import "dotenv/config";
import mongoose from "mongoose";

import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

const MONGO_URI =
  process.env.MONGODB_URI ||
  process.env.MONGO_URI ||
  process.env.DB_URI ||
  process.env.DATABASE_URL ||
  "";

const SHOULD_CONNECT_DB = process.env.ACI_V2_SKIP_DB !== "true";

const connectMongoIfAvailable = async () => {
  if (!SHOULD_CONNECT_DB) {
    console.log("Skipping MongoDB connection because ACI_V2_SKIP_DB=true");
    return;
  }

  if (!MONGO_URI) {
    console.log("No Mongo URI found. Running V2 smoke without DB.");
    return;
  }

  try {
    if (mongoose.connection.readyState !== 1) {
      await mongoose.connect(MONGO_URI, {
        serverSelectionTimeoutMS: 6000,
      });
    }

    console.log("MongoDB connected");
  } catch (error) {
    console.log("MongoDB not connected. Running V2 smoke without DB.");
    console.log(error?.message || error);
  }
};

const closeMongo = async () => {
  if (mongoose.connection.readyState !== 0) {
    await mongoose.disconnect();
  }
};

const cases = [
  {
    id: "v2-price-001",
    message: "Verna pricelist",
    context: {},
  },
  {
    id: "v2-feature-001",
    message: "Does Verna SX have sunroof?",
    context: {},
  },
  {
    id: "v2-emi-001",
    message: "EMI for Verna SX IVT with 2 lakh down payment",
    context: {},
  },
  {
    id: "v2-multi-001",
    message:
      "Show Verna price in Delhi, compare with City, tell EMI for 5 years and check offers",
    context: {},
  },
  {
    id: "v2-context-001",
    message: "Compare with City",
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
  },
  {
    id: "v2-lead-001",
    message: "Best price for black Verna SX automatic",
    context: {},
  },
  {
    id: "v2-internal-001",
    message: "Loan closure 7077",
    context: {},
  },
];

const compact = (response) => ({
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
    intent: item.intent,
  })),
  contextPatch: response.contextPatch,
  secondaryCount: response.secondaryResponses?.length || 0,
  plannerTools: response.planner?.tools?.map((item) => item.tool) || [],
  contractValid: response.executor?.contractValidation?.valid,
  contractErrors: response.executor?.contractValidation?.errors || [],
  oldSystemUsed: response.service?.oldSystemUsed,
  plannerFallbackUsed: response.service?.planner?.fallbackUsed,
  plannerExport: response.service?.planner?.plannerExport,
  callShape: response.service?.planner?.callShape,
  runtimeResultsMeta: response.executor?.runtimeResultsMeta || [],
});

const assertCase = ({ testCase, response }) => {
  const failures = [];
  const blob = JSON.stringify(response).toLowerCase();

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

  if (response.service?.oldSystemUsed !== false) {
    failures.push("Old system should not be used");
  }

  if (response.executor?.contractValidation?.valid === false) {
    failures.push(
      `Contract invalid: ${(response.executor.contractValidation.errors || []).join(", ")}`,
    );
  }

  if (blob.includes("test drive") || blob.includes("test_drive")) {
    failures.push("Test drive should not appear");
  }

  if (testCase.id === "v2-internal-001") {
    if (
      blob.includes("show price") ||
      blob.includes("calculate emi") ||
      blob.includes("get quotation")
    ) {
      failures.push("Internal response contains new-car CTA");
    }
  }

  if (testCase.id === "v2-lead-001") {
    if (!blob.includes("exact automatic variant")) {
      failures.push("Loose automatic quote should ask exact automatic variant");
    }
  }

  if (testCase.id === "v2-multi-001") {
    if ((response.secondaryResponses || []).length < 2) {
      failures.push("Multi intent should include secondary responses");
    }
  }

  return failures;
};

const main = async () => {
  await connectMongoIfAvailable();

  const results = [];

  for (const testCase of cases) {
    const started = Date.now();

    const response = await chatWithAgent({
      message: testCase.message,
      context: testCase.context,
    });

    const failures = assertCase({ testCase, response });

    const result = {
      id: testCase.id,
      message: testCase.message,
      pass: failures.length === 0,
      durationMs: Date.now() - started,
      failures,
      summary: compact(response),
    };

    results.push(result);
    console.log(JSON.stringify(result, null, 2));
  }

  const failed = results.filter((item) => !item.pass);

  console.log(
    JSON.stringify(
      {
        suite: "ACI Assist V2 service smoke",
        total: results.length,
        passed: results.length - failed.length,
        failed: failed.length,
        failedIds: failed.map((item) => item.id),
      },
      null,
      2,
    ),
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
