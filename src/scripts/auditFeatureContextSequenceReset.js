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
  console.log("✅ MongoDB connected for feature context sequence reset audit.");
}

let context = {
  anchorCity: "new-delhi",
};

const cases = [
  {
    label: "Seltos variant-specific ABS",
    message: "Does seltos has abs in htx ivt",
    expectedModel: "Seltos",
    expectedVariant: "HTX iVT",
    bannedAnswer: ["older", "Verna"],
  },
  {
    label: "Seltos model-level ABS must clear HTX iVT",
    message: "Does seltos has abs",
    expectedModel: "Seltos",
    expectedVariant: "",
    bannedAnswer: ["HTX iVT", "older", "Verna"],
  },
  {
    label: "Verna model-level sunroof must not keep Seltos HTX iVT",
    message: "Does verna has sunroof",
    expectedModel: "Verna",
    expectedVariant: "",
    bannedAnswer: ["HTX iVT", "older Verna variant", "Seltos"],
  },
  {
    label: "Creta model-level mileage context",
    message: "Which variant has best mileage?",
    seedContext: {
      anchorMake: "Hyundai",
      anchorModel: "Creta",
      anchorFullModel: "Hyundai Creta",
      anchorVariant: "",
      selectedVehicle: {
        make: "Hyundai",
        brand: "Hyundai",
        model: "Creta",
        fullModel: "Hyundai Creta",
        variant: "",
        variantName: "",
        city: "new-delhi",
      },
      anchorCity: "new-delhi",
    },
    expectedModel: "Creta",
    expectedVariant: "",
    bannedAnswer: ["Chevrolet Beat", "Which Best", "older"],
  },
];

const failures = [];

for (const item of cases) {
  if (item.seedContext) {
    context = item.seedContext;
  }

  const response = await chatWithAgent({
    message: item.message,
    sessionId: "feature-context-sequence-reset",
    context,
    debug: true,
    user: null,
  });

  const patch = response.contextPatch || response.data?.contextPatch || response.widget?.contextPatch || {};
  context = {
    ...context,
    ...patch,
    selectedVehicle: {
      ...(context.selectedVehicle || {}),
      ...(patch.selectedVehicle || {}),
    },
  };

  const summary = {
    label: item.label,
    message: item.message,
    intent: response.intent,
    answer: response.answer,
    detectedModel: response.meta?.detectedModel || "",
    detectedFeature: response.meta?.detectedFeature || "",
    patchAnchorModel: patch.anchorModel || "",
    patchAnchorVariant: Object.prototype.hasOwnProperty.call(patch, "anchorVariant")
      ? patch.anchorVariant
      : "__missing__",
    selectedVehicleVariant: patch.selectedVehicle?.variant ?? "__missing__",
    fullContext: context,
  };

  console.log("\n===", item.label, "===");
  console.log(JSON.stringify(summary, null, 2));

  const actualModel = patch.anchorModel || response.meta?.detectedModel || context.anchorModel || "";

  if (item.expectedModel && actualModel !== item.expectedModel) {
    failures.push({
      label: item.label,
      reason: "wrong model context",
      expected: item.expectedModel,
      actual: actualModel,
      summary,
    });
  }

  if (Object.prototype.hasOwnProperty.call(item, "expectedVariant")) {
    const actualVariant = patch.anchorVariant ?? context.anchorVariant ?? "";

    if (actualVariant !== item.expectedVariant) {
      failures.push({
        label: item.label,
        reason: "wrong variant context",
        expected: item.expectedVariant,
        actual: actualVariant,
        summary,
      });
    }
  }

  for (const banned of item.bannedAnswer || []) {
    if (String(response.answer || "").includes(banned)) {
      failures.push({
        label: item.label,
        reason: "bad answer copy",
        banned,
        answer: response.answer,
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

console.log("PASSED: Feature context sequence reset is clean.");
