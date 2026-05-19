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
  console.log("✅ MongoDB connected for dynamic resolver chat-path audit.");
}

const cases = [
  {
    message: "cretaa adas",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "hyundai vrna me adas hai kya",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Verna",
  },
  {
    message: "which hyundai vernaa variants have 360 camera",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedModel: "Verna",
  },
  {
    message: "cretaaa pricelist",
    expectedIntentAny: ["vehicle_pricelist"],
    expectedModel: "Creta",
  },
  {
    message: "cretta featuers",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedModel: "Creta",
  },
  {
    message: "kia seltoss price",
    expectedIntentAny: ["vehicle_pricelist"],
    expectedModel: "Seltos",
  },
  {
    message: "sonett features",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedModel: "Sonet",
  },
  {
    message: "mahindra thaar music system",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Thar",
  },
  {
    message: "difference in hyundai cretaa e and ex",
    expectedIntentAny: ["vehicle_feature_comparison"],
    expectedModel: "Creta",
  },
];

const failures = [];

const getRows = (response) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.widget?.items ||
  response.widget?.features ||
  response.features ||
  [];

for (const test of cases) {
  const response = await chatWithAgent({
    message: test.message,
    sessionId: `dynamic-resolver-chat-path-${Date.now()}-${Math.random()}`,
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
    detectedFullModel: response.meta?.detectedFullModel || "",
    detectedFeature: response.meta?.detectedFeature || "",
    modelMatchedText: response.meta?.modelMatchedText || "",
    modelCorrectionConfidence: response.meta?.modelCorrectionConfidence || null,
    rowCount: Array.isArray(rows) ? rows.length : 0,
  };

  console.log("\n===", test.message, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (!test.expectedIntentAny.includes(response.intent)) {
    failures.push({
      message: test.message,
      reason: "intent mismatch",
      expected: test.expectedIntentAny,
      actual: response.intent,
      answer: response.answer,
    });
  }

  if (summary.detectedModel !== test.expectedModel) {
    failures.push({
      message: test.message,
      reason: "model mismatch",
      expectedModel: test.expectedModel,
      detectedModel: summary.detectedModel,
      meta: response.meta || {},
    });
  }

  if (!summary.earlyFeatureGate) {
    failures.push({
      message: test.message,
      reason: "early feature gate was not used",
      meta: response.meta || {},
    });
  }

  if (
    String(response.answer || "").includes("not available in the current ACI Assist backend") ||
    String(response.answer || "").includes("Are you asking about a new car")
  ) {
    failures.push({
      message: test.message,
      reason: "bad unavailable/clarification copy",
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

console.log("PASSED: Dynamic make-aware resolver is integrated into chat early gate.");
