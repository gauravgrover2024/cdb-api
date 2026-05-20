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
  console.log("✅ MongoDB connected for visible screenshot bug audit.");
}

const cases = [
  {
    label: "Seltos HTX iVT ABS",
    message: "Does seltos has abs in htx ivt",
    context: {
      anchorMake: "Hyundai",
      anchorModel: "Verna",
      selectedVehicle: {
        make: "Hyundai",
        model: "Verna",
        variant: "",
      },
    },
    expectedModel: "Seltos",
    bannedAnswer: [
      "not available in the current ACI Assist backend",
      "Are you asking about a new car",
      "older Verna variant",
      "has abs htx ivt looks like",
      "has abs looks like",
      "looks like an older Seltos variant",
      "I can explain this in simple terms",
    ],
  },
  {
    label: "Seltos ABS model level",
    message: "Does seltos has abs",
    context: {
      anchorMake: "Hyundai",
      anchorModel: "Verna",
      anchorVariant: "HTX iVT",
      selectedVehicle: {
        make: "Hyundai",
        model: "Verna",
        variant: "HTX iVT",
      },
    },
    expectedModel: "Seltos",
    bannedAnswer: [
      "not available in the current ACI Assist backend",
      "Are you asking about a new car",
      "older Verna variant",
    ],
  },
  {
    label: "Verna sunroof must not reuse Seltos HTX iVT",
    message: "Does verna has sunroof",
    context: {
      anchorMake: "Kia",
      anchorModel: "Seltos",
      anchorVariant: "HTX iVT",
      selectedVehicle: {
        make: "Kia",
        model: "Seltos",
        variant: "HTX iVT",
      },
    },
    expectedModel: "Verna",
    bannedAnswer: [
      "HTX iVT looks like an older Verna variant",
      "older Verna variant",
      "not available in the current ACI Assist backend",
    ],
  },
];

const failures = [];

const rowsOf = (response) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.widget?.items ||
  [];

for (const test of cases) {
  const response = await chatWithAgent({
    message: test.message,
    sessionId: `visible-screenshot-bug-${Date.now()}-${Math.random()}`,
    context: {
      anchorCity: "new-delhi",
      ...(test.context || {}),
    },
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
    meta: response.meta || {},
  };

  console.log("\n===", test.label, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (
    test.expectedModel &&
    summary.detectedModel &&
    summary.detectedModel !== test.expectedModel
  ) {
    failures.push({
      label: test.label,
      reason: "wrong detected model",
      expectedModel: test.expectedModel,
      actualModel: summary.detectedModel,
    });
  }

  for (const banned of test.bannedAnswer) {
    if (String(response.answer || "").includes(banned)) {
      failures.push({
        label: test.label,
        reason: "bad answer copy",
        banned,
        answer: response.answer,
      });
    }
  }

  if (
    test.expectedModel &&
    ["Seltos", "Verna", "Creta"].includes(test.expectedModel) &&
    response.intent === "vehicle_feature_answer" &&
    summary.rowCount < 1
  ) {
    failures.push({
      label: test.label,
      reason: "feature answer returned zero rows",
      rowCount: summary.rowCount,
      answer: response.answer,
      meta: response.meta || {},
    });
  }

  if (response.meta?.inactiveVariant === true) {
    failures.push({
      label: test.label,
      reason: "incorrect inactive variant detection",
      answer: response.answer,
      meta: response.meta || {},
    });
  }

  if (["clarification", "unavailable", "vehicle_explainer"].includes(response.intent)) {
    failures.push({
      label: test.label,
      reason: "bad fallback intent",
      intent: response.intent,
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

console.log("PASSED: Visible screenshot bugs are clean.");
