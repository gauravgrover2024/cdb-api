import dotenv from "dotenv";
import mongoose from "mongoose";
import { chatWithAgent } from "../services/aiAgent/aiAgent.service.js";

dotenv.config();

const uri = process.env.MONGO_URI || process.env.MONGODB_URI || process.env.DATABASE_URL;
if (!uri) {
  console.error("Missing Mongo URI in .env");
  process.exit(1);
}

if (mongoose.connection.readyState !== 1) {
  await mongoose.connect(uri);
  console.log("✅ MongoDB connected for final 21-bucket audit.");
}

const cases = [
  ["Creta LED headlamps variants", "vehicle_feature_discovery", "feature_match_builder_canvas"],
  ["cretaa LED headlights", ["vehicle_feature_answer", "vehicle_feature_discovery"]],
  ["cretaa me LED headlights hai kya", ["vehicle_feature_answer", "vehicle_feature_discovery"]],
  ["which cretaa variants have LED headlights", "vehicle_feature_discovery", "feature_match_builder_canvas"],

  ["Creta automatic climate control variants", "vehicle_feature_discovery", "feature_match_builder_canvas"],
  ["cretaa climate control", ["vehicle_feature_answer", "vehicle_feature_discovery"]],
  ["cretaa me climate control hai kya", ["vehicle_feature_answer", "vehicle_feature_discovery"]],
  ["which cretaa variants have climate control", "vehicle_feature_discovery", "feature_match_builder_canvas"],

  ["Creta hill hold variants", "vehicle_feature_discovery", "feature_match_builder_canvas"],
  ["cretaa hill assist", ["vehicle_feature_answer", "vehicle_feature_discovery"]],
  ["cretaa me hill assist hai kya", ["vehicle_feature_answer", "vehicle_feature_discovery"]],
  ["which cretaa variants have hill assist", "vehicle_feature_discovery", "feature_match_builder_canvas"],

  ["Show features of Creta", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Creta features", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Show all features of Creta", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Open feature explorer for Creta", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["List all features of Creta", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Show full feature list of Creta", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Show features of Creta EX (O)", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Creta EX (O) features", "vehicle_model_features_explorer", "features_explorer_canvas", 1],
  ["Show all features of Creta EX (O)", "vehicle_model_features_explorer", "features_explorer_canvas", 1],

  ["Creta infotainment features", ["vehicle_model_features_explorer", "vehicle_feature_discovery"], null, 1],
];

const failures = [];

const asArray = (v) => Array.isArray(v) ? v : [v].filter(Boolean);
const widgetOf = (r) => r.widget || (Array.isArray(r.widgets) ? r.widgets[0] : null) || {};
const groupsOf = (r) => {
  const w = widgetOf(r);
  return r.featureGroups || r.groups || r.data?.featureGroups || r.data?.groups || w.featureGroups || w.groups || [];
};
const rowsOf = (r) => {
  const w = widgetOf(r);
  return r.rows || r.data?.rows || w.rows || w.items || [];
};

for (const [message, expectedIntent, expectedCanvas, minGroups = 0] of cases) {
  const response = await chatWithAgent({
    message,
    sessionId: `final-21-${Date.now()}-${Math.random()}`,
    context: { anchorCity: "new-delhi" },
    debug: true,
    user: null,
  });

  const groups = asArray(groupsOf(response));
  const rows = asArray(rowsOf(response));

  const summary = {
    message,
    intent: response.intent,
    canvasType: response.canvasType,
    inlineType: response.inlineType,
    answer: response.answer,
    rows: rows.length,
    groups: groups.length,
    earlyFeatureGate: response.meta?.earlyFeatureGate || false,
    detectedModel: response.meta?.detectedModel || "",
    detectedFeature: response.meta?.detectedFeature || "",
    detectedCategory: response.meta?.detectedCategory || "",
  };

  console.log("\n===", message, "===");
  console.log(JSON.stringify(summary, null, 2));

  const expectedList = asArray(expectedIntent);
  if (!expectedList.includes(response.intent)) {
    failures.push({ message, reason: "intent mismatch", expected: expectedList, actual: response.intent });
  }

  if (expectedCanvas && response.canvasType !== expectedCanvas) {
    failures.push({ message, reason: "canvas mismatch", expected: expectedCanvas, actual: response.canvasType });
  }

  if (minGroups && groups.length < minGroups) {
    failures.push({ message, reason: "groups too low", expectedMin: minGroups, actual: groups.length });
  }

  if (/clarification|unavailable/i.test(response.intent || "")) {
    failures.push({ message, reason: "bad fallback intent", intent: response.intent, answer: response.answer });
  }
}

console.log("\n=== FINAL RESULT ===");

await mongoose.disconnect();

if (failures.length) {
  console.log(JSON.stringify(failures, null, 2));
  process.exit(1);
}

console.log("PASSED: Final 21 failure buckets are handled.");
