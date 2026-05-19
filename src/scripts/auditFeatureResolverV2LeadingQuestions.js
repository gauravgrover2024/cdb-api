import dotenv from "dotenv";
import mongoose from "mongoose";
import {
  getModelFeatureExplorerV2,
  answerModelFeatureV2,
  compareVariantFeaturesV2,
} from "../services/aiAgent/aiAgent.featureResolverV2.js";

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

const getQuestions = (result = {}) =>
  result.leadingQuestions ||
  result.conversationSuggestions ||
  result.data?.leadingQuestions ||
  [];

const labelsOf = (result = {}) =>
  getQuestions(result).map((item) => item.label || item.title || "");

const queriesOf = (result = {}) =>
  getQuestions(result).map((item) => item.query || "");

const expectedCretaKingLabels = [
  "Open Car Overview",
  "Check Creta King on-road price",
  "Show all Creta King features",
  "Which colors are available in Creta?",
];

const assertExactCretaKingQuestions = (label, result) => {
  const questions = getQuestions(result);
  const labels = labelsOf(result);
  const queries = queriesOf(result);

  assert(
    questions.length === expectedCretaKingLabels.length,
    `${label}: has exactly ${expectedCretaKingLabels.length} leading questions`,
    { labels, expectedCretaKingLabels, questions },
  );

  assert(
    labels.join(" | ") === expectedCretaKingLabels.join(" | "),
    `${label}: exact Creta King leading-question labels`,
    { labels, expectedCretaKingLabels, questions },
  );

  assert(
    questions.every((item) => item.intent && item.canvasType && item.query),
    `${label}: every question has intent, canvasType and query`,
    questions,
  );

  assert(
    questions[0]?.intent === "vehicle_overview" &&
      questions[0]?.canvasType === "car_overview_canvas",
    `${label}: overview question opens car overview`,
    questions[0],
  );

  assert(
    questions[1]?.canvasType === "pricelist_canvas" &&
      /on-road price/i.test(questions[1]?.query || ""),
    `${label}: price question opens pricelist/on-road flow`,
    questions[1],
  );

  assert(
    questions[2]?.intent === "vehicle_model_features_explorer" &&
      questions[2]?.canvasType === "features_explorer_canvas",
    `${label}: features question opens feature explorer`,
    questions[2],
  );

  assert(
    questions[3]?.intent === "vehicle_colors" &&
      questions[3]?.canvasType === "color_gallery_canvas",
    `${label}: colors question opens color gallery`,
    questions[3],
  );

  assert(
    queries.some((query) => /Creta King/i.test(query)),
    `${label}: keeps Creta King variant context where needed`,
    queries,
  );
};

const main = async () => {
  await mongoose.connect(mongoUri);

  console.log("\n=== CRETA KING EXACT LEADING QUESTION GATES ===");

  const kingExplorer = await getModelFeatureExplorerV2({
    model: "Creta",
    variant: "King",
  });

  assert(kingExplorer.ok, "Creta King explorer works");
  assertExactCretaKingQuestions("Creta King explorer", kingExplorer);

  const kingAdas = await answerModelFeatureV2({
    model: "Creta",
    variant: "King",
    feature: "ADAS",
  });

  assert(kingAdas.ok, "Creta King ADAS answer works");
  assertExactCretaKingQuestions("Creta King ADAS answer", kingAdas);

  const kingSunroof = await answerModelFeatureV2({
    model: "Creta",
    variant: "King",
    feature: "sunroof",
  });

  assert(kingSunroof.ok, "Creta King sunroof answer works");
  assertExactCretaKingQuestions("Creta King sunroof answer", kingSunroof);

  const kingCompare = await compareVariantFeaturesV2({
    model: "Creta",
    variants: ["King", "King iVT"],
  });

  assert(kingCompare.ok, "Creta King vs King iVT comparison works");
  assertExactCretaKingQuestions("Creta King comparison", kingCompare);

  console.log("\n=== FINAL RESULT ===");
  if (failures.length) {
    console.error(`FAILED: ${failures.length} leading-question gate(s).`);
    console.dir(failures, { depth: 10 });
    await mongoose.disconnect();
    process.exit(1);
  }

  console.log("PASSED: Creta King exact leading questions are clean.");
  await mongoose.disconnect();
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
