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
  console.log("✅ MongoDB connected for ACI V2 context authority flow audit.");
}

const hasOwn = (object, key) =>
  Object.prototype.hasOwnProperty.call(object || {}, key);

const mergeContext = (previous = {}, patch = {}) => {
  const selectedVehicle = {
    ...(previous.selectedVehicle || {}),
    ...(patch.selectedVehicle || {}),
  };

  if (hasOwn(patch, "anchorVariant")) {
    selectedVehicle.variant = String(patch.anchorVariant || "");
    selectedVehicle.variantName = String(patch.anchorVariant || "");
  }

  return {
    ...previous,
    ...patch,
    selectedVehicle,
  };
};

const cases = [
  {
    message: "does seltos has abs",
    expectedModel: "Seltos",
    expectedVariant: "",
    expectedIntent: "vehicle_feature_answer",
    bannedAnswer: ["Mercedes", "Rence", "HTX iVT"],
  },
  {
    message: "creta price",
    expectedModel: "Creta",
    expectedVariant: "",
    expectedIntent: "vehicle_pricelist",
    bannedAnswer: ["Seltos", "Mercedes", "Rence"],
  },
  {
    message: "sunroof?",
    expectedModel: "Creta",
    expectedVariant: "",
    expectedIntent: "vehicle_feature_answer",
    bannedAnswer: ["Seltos", "Mercedes", "Rence"],
  },
  {
    message: "difference between e and ex",
    expectedModel: "Creta",
    expectedVariant: "",
    expectedIntent: "vehicle_feature_comparison",
    bannedAnswer: ["Mercedes", "Rence"],
  },
];

let context = {
  anchorCity: "new-delhi",
};

const failures = [];

for (const item of cases) {
  const response = await chatWithAgent({
    message: item.message,
    sessionId: "aci-v2-context-authority-flow",
    context,
    debug: true,
    user: null,
  });

  const patch =
    response.contextPatch ||
    response.data?.contextPatch ||
    response.widget?.contextPatch ||
    {};

  context = mergeContext(context, patch);

  const actualModel =
    patch.anchorModel ||
    response.meta?.detectedModel ||
    context.anchorModel ||
    "";
  const actualVariant = hasOwn(patch, "anchorVariant")
    ? String(patch.anchorVariant || "")
    : String(context.anchorVariant || "");

  const summary = {
    message: item.message,
    intent: response.intent,
    canvasType: response.canvasType,
    answer: response.answer,
    detectedModel: response.meta?.detectedModel || "",
    patchAnchorModel: patch.anchorModel || "",
    patchAnchorVariant: hasOwn(patch, "anchorVariant")
      ? patch.anchorVariant
      : "__missing__",
    selectedVehicle: context.selectedVehicle || null,
    context,
  };

  console.log("\n===", item.message, "===");
  console.log(JSON.stringify(summary, null, 2));

  if (item.expectedIntent && response.intent !== item.expectedIntent) {
    failures.push({
      message: item.message,
      reason: "wrong intent",
      expected: item.expectedIntent,
      actual: response.intent,
      summary,
    });
  }

  if (actualModel !== item.expectedModel) {
    failures.push({
      message: item.message,
      reason: "wrong model context",
      expected: item.expectedModel,
      actual: actualModel,
      summary,
    });
  }

  if (actualVariant !== item.expectedVariant) {
    failures.push({
      message: item.message,
      reason: "wrong variant context",
      expected: item.expectedVariant,
      actual: actualVariant,
      summary,
    });
  }

  for (const banned of item.bannedAnswer || []) {
    if (String(response.answer || "").includes(banned)) {
      failures.push({
        message: item.message,
        reason: "bad answer copy",
        banned,
        answer: response.answer,
        summary,
      });
    }
  }
}

console.log("\n=== FINAL CONTEXT ===");
console.log(JSON.stringify(context, null, 2));

await mongoose.disconnect();

if (failures.length) {
  console.log("\n=== FAILURES ===");
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: ACI V2 context authority flow is clean.");
