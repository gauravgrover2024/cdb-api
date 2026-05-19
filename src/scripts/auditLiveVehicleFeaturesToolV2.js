import dotenv from "dotenv";
import mongoose from "mongoose";
import { runVehicleFeaturesTool } from "../services/aiAgent/tools/newCars/vehicleFeatures.tool.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const failures = [];

const assert = (condition, label, details = {}) => {
  if (!condition) {
    failures.push({ label, details });
    console.error("❌", label, details);
  } else {
    console.log("✅", label);
  }
};

const expectedKingLabels = [
  "Open Car Overview",
  "Check Creta King on-road price",
  "Show all Creta King features",
  "Which colors are available in Creta?",
];

const labels = (response = {}) =>
  (response.leadingQuestions || []).map((item) => item.label || item.title || "");

const runCase = async ({ label, message, toolPlan }) => {
  console.log(`\n================ ${label} ================`);

  const response = await runVehicleFeaturesTool({
    userMessage: message,
    context: {},
    toolPlan,
  });

  console.dir(
    {
      intent: response.intent,
      canvasType: response.canvasType,
      inlineType: response.inlineType,
      title: response.title,
      answer: response.answer,
      model: response.contextPatch?.anchorModel,
      variant: response.contextPatch?.anchorVariant,
      leadingQuestions: response.leadingQuestions,
      rows: response.rows?.length || 0,
      variants: response.variants?.length || 0,
      features: response.features?.length || 0,
      selectedVariant: response.selectedVariant?.variant,
    },
    { depth: 8 },
  );

  return response;
};

const main = async () => {
  await mongoose.connect(mongoUri);

  const kingExplorer = await runCase({
    label: "Creta King explorer",
    message: "Show all Creta King features",
    toolPlan: {
      intent: "vehicle_model_features_explorer",
      entities: { model: "Creta", variant: "King" },
    },
  });

  assert(kingExplorer.intent === "vehicle_model_features_explorer", "King explorer intent ok");
  assert(kingExplorer.canvasType === "features_explorer_canvas", "King explorer canvas ok");
  assert(labels(kingExplorer).join(" | ") === expectedKingLabels.join(" | "), "King explorer leading questions exact", labels(kingExplorer));

  const kingAdas = await runCase({
    label: "Creta King ADAS",
    message: "Does Creta King have ADAS?",
    toolPlan: {
      intent: "vehicle_feature_answer",
      entities: { model: "Creta", variant: "King", feature: "ADAS" },
    },
  });

  assert(kingAdas.intent === "vehicle_feature_answer", "King ADAS intent ok");
  assert(/ADAS/i.test(kingAdas.answer || ""), "King ADAS answer mentions ADAS", kingAdas.answer);
  assert(labels(kingAdas).join(" | ") === expectedKingLabels.join(" | "), "King ADAS leading questions exact", labels(kingAdas));

  const kingSunroof = await runCase({
    label: "Creta King sunroof",
    message: "Does Creta King have sunroof?",
    toolPlan: {
      intent: "vehicle_feature_answer",
      entities: { model: "Creta", variant: "King", feature: "sunroof" },
    },
  });

  assert(kingSunroof.intent === "vehicle_feature_answer", "King sunroof intent ok");
  assert(/sunroof/i.test(kingSunroof.answer || ""), "King sunroof answer mentions sunroof", kingSunroof.answer);
  assert(labels(kingSunroof).join(" | ") === expectedKingLabels.join(" | "), "King sunroof leading questions exact", labels(kingSunroof));

  const sunroofDiscovery = await runCase({
    label: "Creta sunroof discovery",
    message: "Which Creta variants have sunroof?",
    toolPlan: {
      intent: "vehicle_feature_discovery",
      entities: { model: "Creta", feature: "sunroof" },
    },
  });

  assert(sunroofDiscovery.intent === "vehicle_feature_discovery", "Sunroof discovery intent ok");
  assert(sunroofDiscovery.canvasType === "feature_match_builder_canvas", "Sunroof discovery canvas ok");
  assert((sunroofDiscovery.rows || []).length === 43, "Sunroof discovery returns 43 active rows", sunroofDiscovery.rows?.length);

  const compare = await runCase({
    label: "Creta E vs EX Diesel features",
    message: "Compare Creta E vs EX Diesel features",
    toolPlan: {
      intent: "vehicle_feature_comparison",
      entities: { model: "Creta", variants: ["E", "EX Diesel"] },
    },
  });

  assert(compare.intent === "vehicle_feature_comparison", "Comparison intent ok");
  assert(
    compare.data?.variants?.map((item) => item.variant).join(" | ") === "E Diesel | EX Diesel",
    "E vs EX Diesel aligns diesel context",
    compare.data?.variants,
  );

  const oldVerna = await runCase({
    label: "Old Verna SX sunroof",
    message: "Verna SX sunroof",
    toolPlan: {
      intent: "vehicle_feature_answer",
      entities: { model: "Verna", variant: "SX", feature: "sunroof" },
    },
  });

  assert(oldVerna.intent === "vehicle_feature_answer", "Old Verna SX answer intent ok");
  assert(/older|current new-car/i.test(oldVerna.answer || ""), "Old Verna copy is safe", oldVerna.answer);

  const oldVernaLabels = labels(oldVerna);
  const expectedOldVernaLabels = [
    "Open Car Overview",
    "Check Verna on-road price",
    "Show all Verna features",
    "Which colors are available in Verna?",
  ];

  assert(
    oldVernaLabels.join(" | ") === expectedOldVernaLabels.join(" | "),
    "Old Verna SX uses current model-level leading questions",
    oldVernaLabels,
  );

  assert(
    !JSON.stringify(oldVerna.leadingQuestions || []).includes("Verna SX"),
    "Old Verna SX leading questions do not promote inactive SX",
    oldVerna.leadingQuestions,
  );

  console.log("\n=== FINAL RESULT ===");
  if (failures.length) {
    console.error(`FAILED: ${failures.length} live feature tool gate(s).`);
    console.dir(failures, { depth: 10 });
    await mongoose.disconnect();
    process.exit(1);
  }

  console.log("PASSED: Live vehicleFeatures.tool.js is wired to Feature Resolver V2.");
  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
