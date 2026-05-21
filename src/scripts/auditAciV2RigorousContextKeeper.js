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
  console.log("✅ MongoDB connected for ACI V2 rigorous context audit.");
}

const hasOwn = (object, key) =>
  Object.prototype.hasOwnProperty.call(object || {}, key);

const norm = (value = "") =>
  String(value || "")
    .toLowerCase()
    .replace(/&/g, " and ")
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();

const compact = (value = "") => norm(value).replace(/\s+/g, "");

const mergeContext = (previous = {}, patch = {}) => {
  const incomingVehicle = patch.selectedVehicle || null;
  const previousVehicle = previous.selectedVehicle || {};
  const previousModelKey = compact(previousVehicle.model || previous.anchorModel || "");
  const incomingModelKey = compact(incomingVehicle?.model || patch.anchorModel || "");
  const sameVehicle =
    !incomingVehicle || !previousModelKey || !incomingModelKey || previousModelKey === incomingModelKey;

  const selectedVehicle = incomingVehicle
    ? {
        ...(sameVehicle ? previousVehicle : {}),
        ...incomingVehicle,
      }
    : {
        ...previousVehicle,
      };

  if (
    hasOwn(patch, "anchorModel") &&
    patch.anchorModel &&
    selectedVehicle.model &&
    compact(selectedVehicle.model) !== compact(patch.anchorModel)
  ) {
    selectedVehicle.model = patch.anchorModel;
    selectedVehicle.displayName = patch.anchorModel;
    delete selectedVehicle.id;
    delete selectedVehicle.imageUrl;
    delete selectedVehicle.normalizedImageUrl;
  }

  if (hasOwn(patch, "anchorVariant")) {
    selectedVehicle.variant = String(patch.anchorVariant || "");
    selectedVehicle.variantName = String(patch.anchorVariant || "");
    selectedVehicle.selectedVariant = String(patch.anchorVariant || "");
  }

  return {
    ...previous,
    ...patch,
    selectedVehicle,
  };
};

const modelSlugInUrl = (model = "") =>
  compact(model)
    .replace(/^xuv700$/, "xuv7xo")
    .replace(/^xuv7xo$/, "xuv7xo")
    .replace(/^xuv300$/, "xuv3xo")
    .replace(/^scorpion$/, "scorpion");

const modelsOverlap = (a = "", b = "") => {
  const left = compact(a);
  const right = compact(b);
  return Boolean(left && right && (left.includes(right) || right.includes(left)));
};

const expectedModels = [
  "Creta",
  "Seltos",
  "Verna",
  "Thar",
  "Thar Roxx",
  "Fortuner",
  "Fortuner Legender",
  "Hyryder",
  "Venue",
  "Sonet",
  "Nexon",
  "Harrier",
  "Safari",
  "Scorpio N",
  "XUV 7XO",
  "Brezza",
  "Fronx",
  "Baleno",
  "Swift",
  "City",
];

const cases = [];

for (const model of expectedModels) {
  cases.push({
    message: model,
    expectedModel: model,
    expectedIntent: "vehicle_overview",
    note: "model-only should open overview",
  });
  cases.push({
    message: `${model} price`,
    expectedModel: model,
    expectedIntent: "vehicle_pricelist",
    note: "explicit price should reset context to this model",
  });
  cases.push({
    message: "sunroof?",
    expectedModel: model,
    expectedIntent: "vehicle_feature_answer",
    note: "feature follow-up should use current model",
  });
}

cases.splice(2, 0, {
  message: "e vs ex",
  expectedModel: "Creta",
  expectedIntent: "vehicle_feature_comparison",
  note: "variant-only comparison should stay on Creta",
});

cases.push(
  {
    message: "Seltos price",
    expectedModel: "Seltos",
    expectedIntent: "vehicle_pricelist",
    note: "switch back to Seltos",
  },
  {
    message: "htx vs gtx",
    expectedModel: "Seltos",
    expectedIntent: "vehicle_feature_comparison",
    note: "variant-only comparison should stay on Seltos",
  },
  {
    message: "Fortuner price",
    expectedModel: "Fortuner",
    expectedIntent: "vehicle_pricelist",
    note: "switch to Fortuner",
  },
  {
    message: "4x2 at vs leader edition 4x2 diesel",
    expectedModel: "Fortuner",
    expectedIntent: "vehicle_feature_comparison",
    note: "variant-only comparison should stay on Fortuner",
  },
  {
    message: "which variant has best mileage?",
    expectedModel: "Fortuner",
    expectedIntents: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    bannedAnswer: ["Chevrolet Beat", "Beat"],
    note: "best-mileage follow-up must not fuzzy-match Beat",
  },
);

let context = {
  anchorCity: "new-delhi",
};
let previousExpectedModel = "";
const failures = [];

for (const [index, item] of cases.entries()) {
  const response = await chatWithAgent({
    message: item.message,
    sessionId: `aci-v2-rigorous-context-${Date.now()}`,
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
    patch.selectedVehicle?.model ||
    patch.anchorModel ||
    response.widget?.vehicle?.model ||
    response.vehicle?.model ||
    context.anchorModel ||
    "";
  const patchAnchorModel = patch.anchorModel || "";
  const selectedModel = patch.selectedVehicle?.model || "";
  const answer = String(response.answer || "");
  const imageUrl = String(
    patch.selectedVehicle?.imageUrl ||
      patch.selectedVehicle?.normalizedImageUrl ||
      response.widget?.vehicle?.imageUrl ||
      "",
  );
  const rows = response.rows || response.data?.rows || response.widget?.rows || [];

  const summary = {
    index: index + 1,
    message: item.message,
    note: item.note,
    intent: response.intent,
    canvasType: response.canvasType,
    expectedModel: item.expectedModel,
    actualModel,
    patchAnchorModel,
    selectedModel,
    anchorVariant: hasOwn(patch, "anchorVariant")
      ? patch.anchorVariant
      : "__missing__",
    imageUrl,
    answer,
  };

  console.log(`\n[${index + 1}/${cases.length}] ${item.message}`);
  console.log(JSON.stringify(summary, null, 2));

  const expectedIntents = item.expectedIntents || (item.expectedIntent ? [item.expectedIntent] : []);
  if (expectedIntents.length && !expectedIntents.includes(response.intent)) {
    failures.push({
      ...summary,
      reason: "wrong intent",
      expected: expectedIntents,
      actual: response.intent,
    });
  }

  if (compact(actualModel) !== compact(item.expectedModel)) {
    failures.push({
      ...summary,
      reason: "wrong active model",
    });
  }

  if (
    patchAnchorModel &&
    selectedModel &&
    compact(patchAnchorModel) !== compact(selectedModel)
  ) {
    failures.push({
      ...summary,
      reason: "contextPatch anchorModel conflicts with selectedVehicle.model",
    });
  }

  if (
    previousExpectedModel &&
    compact(previousExpectedModel) !== compact(item.expectedModel) &&
    !modelsOverlap(previousExpectedModel, item.expectedModel) &&
    answer.toLowerCase().includes(previousExpectedModel.toLowerCase())
  ) {
    failures.push({
      ...summary,
      reason: "answer leaked previous model",
      previousExpectedModel,
    });
  }

  for (const banned of item.bannedAnswer || []) {
    if (answer.toLowerCase().includes(String(banned).toLowerCase())) {
      failures.push({
        ...summary,
        reason: "banned answer text",
        banned,
      });
    }
  }

  if (
    item.expectedModel &&
    /could not identify the car model/i.test(answer)
  ) {
    failures.push({
      ...summary,
      reason: "model was in context but answer claimed model identification failed",
    });
  }

  if (
    response.intent === "vehicle_feature_answer" &&
    Array.isArray(rows) &&
    rows.length > 0 &&
    !imageUrl
  ) {
    failures.push({
      ...summary,
      reason: "feature answer has rows but no vehicle image",
    });
  }

  const previousSlug = modelSlugInUrl(previousExpectedModel);
  const expectedSlug = modelSlugInUrl(item.expectedModel);
  if (
    imageUrl &&
    previousSlug &&
    expectedSlug &&
    previousSlug !== expectedSlug &&
    !modelsOverlap(previousExpectedModel, item.expectedModel) &&
    imageUrl.toLowerCase().includes(previousSlug) &&
    !imageUrl.toLowerCase().includes(expectedSlug)
  ) {
    failures.push({
      ...summary,
      reason: "image leaked previous model",
      previousExpectedModel,
    });
  }

  previousExpectedModel = item.expectedModel;
}

console.log("\n=== FINAL CONTEXT ===");
console.log(JSON.stringify(context, null, 2));

await mongoose.disconnect();

if (failures.length) {
  console.log("\n=== FAILURES ===");
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: ACI V2 context stayed authoritative through rapid 20-model switches and follow-ups.");
