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
  console.log("✅ MongoDB connected for real-customer feature audit.");
}

const cases = [
  {
    label: "Thar music system",
    message: "does thar has music system",
    shouldNotInclude: [
      "Are you asking about a new car",
      "something inside CDrive",
      "not available in the current ACI Assist backend",
    ],
  },
  {
    label: "Thar audio system",
    message: "does thar have audio system",
    shouldNotInclude: [
      "Are you asking about a new car",
      "something inside CDrive",
      "not available in the current ACI Assist backend",
    ],
  },
  {
    label: "Thar speakers",
    message: "does thar have speakers",
    shouldNotInclude: [
      "Are you asking about a new car",
      "something inside CDrive",
      "not available in the current ACI Assist backend",
    ],
  },
  {
    label: "Creta music system",
    message: "does creta have music system",
    shouldNotInclude: [
      "Are you asking about a new car",
      "something inside CDrive",
      "not available in the current ACI Assist backend",
    ],
  },
  {
    label: "Verna infotainment",
    message: "which verna variants have infotainment system",
    expectedIntent: "vehicle_feature_discovery",
    expectedCanvasType: "feature_match_builder_canvas",
    shouldNotInclude: [
      "Are you asking about a new car",
      "something inside CDrive",
      "not available in the current ACI Assist backend",
    ],
  },
];

const failures = [];

for (const test of cases) {
  const response = await chatWithAgent({
    message: test.message,
    sessionId: `real-customer-feature-${Date.now()}-${Math.random()}`,
    context: {
      anchorCity: "new-delhi",
    },
    user: null,
    debug: true,
  });

  const summary = {
    label: test.label,
    message: test.message,
    intent: response.intent,
    canvasType: response.canvasType,
    inlineType: response.inlineType,
    answer: response.answer,
    earlyFeatureGate: response.meta?.earlyFeatureGate || false,
    detectedModel: response.meta?.detectedModel || "",
    detectedFeature: response.meta?.detectedFeature || "",
    leadingQuestionCount:
      response.leadingQuestions?.length ||
      response.conversationSuggestions?.length ||
      0,
    rowCount:
      response.rows?.length ||
      response.data?.rows?.length ||
      response.widget?.rows?.length ||
      0,
  };

  console.log("\n===", test.label, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (test.expectedIntent && response.intent !== test.expectedIntent) {
    failures.push({
      label: test.label,
      reason: "intent mismatch",
      expected: test.expectedIntent,
      actual: response.intent,
    });
  }

  if (test.expectedCanvasType && response.canvasType !== test.expectedCanvasType) {
    failures.push({
      label: test.label,
      reason: "canvas mismatch",
      expected: test.expectedCanvasType,
      actual: response.canvasType,
    });
  }

  for (const banned of test.shouldNotInclude || []) {
    if (String(response.answer || "").includes(banned)) {
      failures.push({
        label: test.label,
        reason: "bad clarification/unavailable copy",
        banned,
        answer: response.answer,
      });
    }
  }

  if (!response.meta?.earlyFeatureGate) {
    failures.push({
      label: test.label,
      reason: "early feature gate was not used",
      meta: response.meta || {},
    });
  }
}

console.log("\n=== FINAL RESULT ===");

await mongoose.disconnect();

if (failures.length) {
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: Real customer feature phrases route to Feature Resolver V2.");
