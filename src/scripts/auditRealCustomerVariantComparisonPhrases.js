import dotenv from "dotenv";
import mongoose from "mongoose";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const uri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.DATABASE_URL;

if (!uri) {
  console.error("Missing Mongo URI in .env");
  process.exit(1);
}

if (mongoose.connection.readyState !== 1) {
  await mongoose.connect(uri);
  console.log("✅ MongoDB connected for real-customer variant comparison audit.");
}

const cases = [
  {
    message: "difference in creta e and ex",
    expectedModel: "Creta",
  },
  {
    message: "difference between creta e and ex",
    expectedModel: "Creta",
  },
  {
    message: "creta e vs ex features",
    expectedModel: "Creta",
  },
  {
    message: "compare creta e and ex",
    expectedModel: "Creta",
  },
  {
    message: "what extra features do i get in creta ex over e",
    expectedModel: "Creta",
  },
  {
    message: "difference in creta e and ex diesel",
    expectedModel: "Creta",
    note: "Should resolve sensibly, preferably E Diesel vs EX Diesel if fuel alignment is available.",
  },
];

const failures = [];

const getRows = (response) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.widget?.items ||
  [];

for (const test of cases) {
  const response = await chatWithAgent({
    message: test.message,
    sessionId: `real-customer-variant-comparison-${Date.now()}-${Math.random()}`,
    context: {
      anchorCity: "new-delhi",
    },
    user: null,
    debug: true,
  });

  const rows = getRows(response);

  const summary = {
    message: test.message,
    intent: response.intent,
    canvasType: response.canvasType,
    inlineType: response.inlineType,
    answer: response.answer,
    earlyFeatureGate: response.meta?.earlyFeatureGate || false,
    detectedModel: response.meta?.detectedModel || "",
    detectedFeature: response.meta?.detectedFeature || "",
    rowCount: rows.length,
    sampleRows: rows.slice(0, 5).map((row) => ({
      feature: row.displayName || row.feature || row.featureName || row.label,
      values: row.values || row.comparisonValues || row.variants,
    })),
    leadingQuestionCount:
      response.leadingQuestions?.length ||
      response.conversationSuggestions?.length ||
      0,
  };

  console.log("\n===", test.message, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (
    response.intent !== "vehicle_feature_comparison" &&
    response.intent !== "vehicle_model_features_explorer"
  ) {
    failures.push({
      message: test.message,
      reason: "intent mismatch",
      expected: "vehicle_feature_comparison",
      actual: response.intent,
      answer: response.answer,
    });
  }

  if (
    String(response.answer || "").includes("not available in the current ACI Assist backend") ||
    String(response.answer || "").includes("Are you asking about a new car")
  ) {
    failures.push({
      message: test.message,
      reason: "bad unavailable/clarification answer",
      answer: response.answer,
    });
  }

  if (!rows.length) {
    failures.push({
      message: test.message,
      reason: "no comparison rows returned",
      intent: response.intent,
      canvasType: response.canvasType,
      answer: response.answer,
    });
  }
}

console.log("\n=== FINAL RESULT ===");

await mongoose.disconnect();

if (failures.length) {
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: Real customer variant comparison phrases are handled.");
