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
  console.log("✅ MongoDB connected for remaining mega-bucket audit.");
}

const cases = [
  // Feature discovery phrasing
  {
    message: "Creta sunroof variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
  },
  {
    message: "Creta ADAS variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
  },
  {
    message: "Creta 6 airbags variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
  },
  {
    message: "Creta 360 camera variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
  },
  {
    message: "Creta wireless charger variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
  },
  {
    message: "Verna sunroof variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
  },

  // Dynamic typo feature phrases
  {
    message: "cretaa adas",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "cretaa me adas hai kya",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "cretaa reverse camera",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "cretaa ventilated seat",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "vrna six airbags",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Verna",
  },
  {
    message: "vernaa six airbags",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Verna",
  },

  // Connected-car phrases
  {
    message: "Creta connected car",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "Does Creta have connected car?",
    expectedIntentAny: ["vehicle_feature_answer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
  },
  {
    message: "Which Creta variants have connected car?",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
    expectedModel: "Creta",
  },
  {
    message: "Creta connected car variants",
    expectedIntentAny: ["vehicle_feature_discovery"],
    expectedCanvasType: "feature_match_builder_canvas",
    expectedModel: "Creta",
  },

  // Feature explorer phrases
  {
    message: "Show features of Creta",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Creta",
  },
  {
    message: "Creta features",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Creta",
  },
  {
    message: "Show all features of Creta",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Creta",
  },
  {
    message: "Creta safety features",
    expectedIntentAny: ["vehicle_model_features_explorer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
    expectedCategory: "safety",
    shouldNotSayFullExplorer: true,
  },
  {
    message: "Show dimensions and capacity of Creta",
    expectedIntentAny: ["vehicle_model_features_explorer", "vehicle_feature_discovery"],
    expectedModel: "Creta",
    expectedCategory: "dimensions",
    shouldNotSayFullExplorer: true,
  },

  // Typo pricelist / typo explorer
  {
    message: "cretaa pricelist",
    expectedIntentAny: ["vehicle_pricelist"],
    expectedCanvasType: "pricelist_canvas",
    expectedModel: "Creta",
  },
  {
    message: "cretaaa pricelist",
    expectedIntentAny: ["vehicle_pricelist"],
    expectedCanvasType: "pricelist_canvas",
    expectedModel: "Creta",
  },
  {
    message: "cretta pricelist",
    expectedIntentAny: ["vehicle_pricelist"],
    expectedCanvasType: "pricelist_canvas",
    expectedModel: "Creta",
  },
  {
    message: "kia seltoss price",
    expectedIntentAny: ["vehicle_pricelist"],
    expectedCanvasType: "pricelist_canvas",
    expectedModel: "Seltos",
  },
  {
    message: "creta featuers",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Creta",
  },
  {
    message: "cretaa featuers",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Creta",
  },
  {
    message: "cretta featuers",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Creta",
  },
  {
    message: "sonett features",
    expectedIntentAny: ["vehicle_model_features_explorer"],
    expectedCanvasTypeAny: ["features_explorer_canvas", "feature_explorer_canvas"],
    expectedModel: "Sonet",
  },
];

const failures = [];

const badCopySnippets = [
  "not available in the current ACI Assist backend",
  "Are you asking about a new car",
  "something inside CDrive",
  "looks like an older",
];

const getRows = (response) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.widget?.items ||
  response.widget?.features ||
  response.features ||
  [];

const getGroups = (response) =>
  response.groups ||
  response.widget?.groups ||
  response.data?.groups ||
  response.featureGroups ||
  [];

for (const test of cases) {
  const response = await chatWithAgent({
    message: test.message,
    sessionId: `mega-bucket-${Date.now()}-${Math.random()}`,
    context: {
      anchorCity: "new-delhi",
    },
    user: null,
    debug: true,
  });

  const rows = getRows(response);
  const groups = getGroups(response);

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
    groupCount: Array.isArray(groups) ? groups.length : 0,
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

  if (
    test.expectedCanvasType &&
    response.canvasType !== test.expectedCanvasType
  ) {
    failures.push({
      message: test.message,
      reason: "canvas mismatch",
      expected: test.expectedCanvasType,
      actual: response.canvasType,
    });
  }

  if (
    test.expectedCanvasTypeAny &&
    !test.expectedCanvasTypeAny.includes(response.canvasType)
  ) {
    failures.push({
      message: test.message,
      reason: "canvas mismatch",
      expected: test.expectedCanvasTypeAny,
      actual: response.canvasType,
    });
  }

  if (
    test.expectedModel &&
    summary.detectedModel &&
    summary.detectedModel !== test.expectedModel
  ) {
    failures.push({
      message: test.message,
      reason: "model mismatch",
      expectedModel: test.expectedModel,
      detectedModel: summary.detectedModel,
      meta: response.meta || {},
    });
  }

  if (
    test.expectedCategory &&
    response.meta?.detectedCategory &&
    response.meta.detectedCategory !== test.expectedCategory
  ) {
    failures.push({
      message: test.message,
      reason: "category mismatch",
      expectedCategory: test.expectedCategory,
      detectedCategory: response.meta.detectedCategory,
      meta: response.meta || {},
    });
  }

  if (
    test.shouldNotSayFullExplorer &&
    /searchable features, grouped so you can compare quickly/i.test(String(response.answer || ""))
  ) {
    failures.push({
      message: test.message,
      reason: "category query returned generic full-explorer copy",
      answer: response.answer,
    });
  }

  for (const snippet of badCopySnippets) {
    if (String(response.answer || "").includes(snippet)) {
      failures.push({
        message: test.message,
        reason: "bad customer copy",
        snippet,
        answer: response.answer,
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

console.log("PASSED: Remaining mega-regression buckets are handled.");
