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
  console.log("✅ MongoDB connected for feature context carry-forward audit.");
}

const baseCretaContext = {
  anchorCity: "new-delhi",
  anchorMake: "Hyundai",
  anchorModel: "Creta",
  anchorFullModel: "Hyundai Creta",
  selectedVehicle: {
    make: "Hyundai",
    model: "Creta",
    fullModel: "Hyundai Creta",
    variant: "",
    city: "new-delhi",
  },
};

const baseSeltosContext = {
  anchorCity: "new-delhi",
  anchorMake: "Kia",
  anchorModel: "Seltos",
  anchorFullModel: "Kia Seltos",
  anchorVariant: "HTX iVT",
  selectedVehicle: {
    make: "Kia",
    model: "Seltos",
    fullModel: "Kia Seltos",
    variant: "HTX iVT",
    city: "new-delhi",
  },
};

const baseVernaContext = {
  anchorCity: "new-delhi",
  anchorMake: "Hyundai",
  anchorModel: "Verna",
  anchorFullModel: "Hyundai Verna",
  selectedVehicle: {
    make: "Hyundai",
    model: "Verna",
    fullModel: "Hyundai Verna",
    variant: "",
    city: "new-delhi",
  },
};

const cases = [
  {
    label: "Creta context: mileage of SX petrol",
    message: "Mileage of sx petrol",
    context: baseCretaContext,
    expectedModel: "Creta",
    expectedFeatureAny: ["ARAI Mileage", "Mileage"],
    requireRows: true,
    bannedAnswer: ["all 12 current", "get ARAI mileage", "gets ARAI mileage"],
  },
  {
    label: "Creta context: which variant has best mileage",
    message: "Which variant has best mileage?",
    context: baseCretaContext,
    expectedModel: "Creta",
    expectedFeatureAny: ["ARAI Mileage", "Mileage"],
    allowDiscovery: true,
    bannedAnswer: [
      "Chevrolet Beat",
      "from “Beat”",
      "could not identify the car model",
      "Which Best",
      "Which Best ?",
    ],
  },
  {
    label: "Creta context: does it have sunroof",
    message: "Does it have sunroof?",
    context: baseCretaContext,
    expectedModel: "Creta",
    expectedFeatureAny: ["Sunroof"],
    requireRows: true,
  },
  {
    label: "Seltos context: does it have ABS",
    message: "Does it have ABS?",
    context: baseSeltosContext,
    expectedModel: "Seltos",
    expectedFeatureAny: ["ABS", "Anti-lock Braking System (ABS)"],
    requireRows: true,
    bannedAnswer: ["all 1 current", "older Seltos variant"],
  },
  {
    label: "Seltos context: HTX iVT ABS direct",
    message: "Does htx ivt have abs?",
    context: baseSeltosContext,
    expectedModel: "Seltos",
    expectedFeatureAny: ["ABS", "Anti-lock Braking System (ABS)"],
    requireRows: true,
    bannedAnswer: ["all 1 current", "older Seltos variant"],
  },
  {
    label: "Verna context: does it have sunroof",
    message: "Does it have sunroof?",
    context: baseVernaContext,
    expectedModel: "Verna",
    expectedFeatureAny: ["Sunroof"],
    requireRows: true,
    bannedAnswer: ["HTX iVT", "older Verna variant"],
  },
];

const failures = [];

const rowsOf = (response) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.widget?.items ||
  response.widget?.matchedVariants ||
  [];

for (const test of cases) {
  const response = await chatWithAgent({
    message: test.message,
    sessionId: `feature-context-carry-${Date.now()}-${Math.random()}`,
    context: test.context,
    debug: true,
    user: null,
  });

  const rows = rowsOf(response);

  const summary = {
    label: test.label,
    message: test.message,
    intent: response.intent,
    canvasType: response.canvasType,
    inlineType: response.inlineType,
    answer: response.answer,
    detectedModel: response.meta?.detectedModel || "",
    detectedFullModel: response.meta?.detectedFullModel || "",
    detectedFeature: response.meta?.detectedFeature || "",
    rowCount: Array.isArray(rows) ? rows.length : 0,
    inactiveVariant: response.meta?.inactiveVariant || false,
    earlyFeatureGate: response.meta?.earlyFeatureGate || false,
  };

  console.log("\n===", test.label, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (["clarification", "unavailable", "vehicle_explainer"].includes(response.intent)) {
    failures.push({
      label: test.label,
      reason: "bad fallback intent",
      summary,
    });
  }

  if (test.expectedModel && summary.detectedModel && summary.detectedModel !== test.expectedModel) {
    failures.push({
      label: test.label,
      reason: "wrong detected model",
      expectedModel: test.expectedModel,
      actualModel: summary.detectedModel,
      summary,
    });
  }

  if (
    test.expectedFeatureAny &&
    summary.detectedFeature &&
    !test.expectedFeatureAny.includes(summary.detectedFeature)
  ) {
    failures.push({
      label: test.label,
      reason: "wrong detected feature",
      expectedFeatureAny: test.expectedFeatureAny,
      actualFeature: summary.detectedFeature,
      summary,
    });
  }

  if (test.requireRows && summary.rowCount < 1) {
    failures.push({
      label: test.label,
      reason: "expected rows",
      summary,
    });
  }

  if (summary.inactiveVariant) {
    failures.push({
      label: test.label,
      reason: "incorrect inactive variant detection",
      summary,
    });
  }

  for (const banned of test.bannedAnswer || []) {
    if (String(response.answer || "").includes(banned)) {
      failures.push({
        label: test.label,
        reason: "bad answer copy",
        banned,
        summary,
      });
    }
  }
}

console.log("\n=== FINAL RESULT ===");

await mongoose.disconnect();

if (failures.length) {
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: Feature context carry-forward is clean.");
