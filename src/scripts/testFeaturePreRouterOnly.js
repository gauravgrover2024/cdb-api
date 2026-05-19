import dotenv from "dotenv";
import mongoose from "mongoose";
import {
  buildFeatureIntelligenceLexicon,
  resolveFeatureQueryDeterministically,
  runDeterministicFeaturePreRouter,
} from "../services/aiAgent/aiAgent.featureIntelligence.js";

dotenv.config();

const mongoUri =
  process.env.MONGO_URI ||
  process.env.MONGODB_URI ||
  process.env.MONGO_URL ||
  process.env.DB_URI ||
  process.env.ATLAS_URI ||
  "";

const QUERIES = [
  "Creta sunroof",
  "cretaa sunroof",
  "thar sunroof",
  "Thar sunrrof",
  "Which Creta variants have sunroof?",
  "sunroof available in which Creta variant?",
  "Creta sunroof variants",
  "Cheapest Creta variant with sunroof",
  "Does Creta EX (O) have sunroof?",
  "Creta EX (O) sunroof",

  "Creta rear camera",
  "creta reverse camera",
  "which creta variants have reverse camera",
  "rear camera available in which Creta variant?",

  "Creta ventilated seats",
  "creta ventilated seat",
  "creta ventillated seets",
  "which creta variants have ventilated seats",

  "Creta wireless charger",
  "creta wireless charging",
  "which creta variants have wireless charging",

  "Creta alloy wheels",
  "creta alloys",
  "which creta variants have alloys",

  "Creta LED headlamps",
  "creta headlights",
  "which creta variants have led headlights",

  "vrna pricelist",
  "vern apricelist",
  "verna sunroof",
  "vrna sunrrof",
  "which vrna variants have sunroof",

  "Seltos bose speakers",
  "seltos premium speakers",
  "sletos ventillated seets",
  "which seltos variants have ventilated seats",

  "Does it have sunroof?",
  "which variants have it?",
];

const main = async () => {
  if (mongoUri && mongoose.connection.readyState !== 1) {
    await mongoose.connect(mongoUri);
    console.log("MongoDB connected");
  }

  const lexicon = await buildFeatureIntelligenceLexicon({ force: true });
  console.log("Lexicon stats:", lexicon.stats);

  let handled = 0;

  for (const q of QUERIES) {
    const context = /does it|which variants have it/i.test(q)
      ? {
          selectedVehicle: { make: "Hyundai", brand: "Hyundai", model: "Creta" },
          anchorMake: "Hyundai",
          anchorModel: "Creta",
          anchorFeature: "sunroof",
          anchorCity: "new-delhi",
        }
      : { anchorCity: "new-delhi" };

    const resolution = await resolveFeatureQueryDeterministically({
      message: q,
      context,
    });

    const route = await runDeterministicFeaturePreRouter({
      message: q,
      context,
      debug: true,
    });

    if (route.handled) handled += 1;

    const response = route.response || {};
    const rows = response.rows || response.widget?.rows || [];

    console.log(
      JSON.stringify({
        q,
        handled: route.handled,
        routeType: resolution.routeType,
        confidence: resolution.confidence,
        correctedQuery: resolution.correctedQuery,
        model: resolution.model?.model,
        modelScore: resolution.model?.score,
        variant: resolution.variant?.label,
        variantScore: resolution.variant?.score,
        feature: resolution.feature?.canonical,
        featureAlias: resolution.feature?.matchedAlias,
        featureScore: resolution.feature?.score,
        intent: response.intent,
        canvasType: response.canvasType,
        inlineType: response.inlineType,
        answer: response.answer,
        rows: rows.length,
        firstRows: rows.slice(0, 3).map((row) => ({
          variant: row.variant,
          value: row.displayValue,
          price: row.priceLabel,
        })),
      }),
    );
  }

  console.log(`\nHandled ${handled}/${QUERIES.length} before Gemini.`);

  if (mongoose.connection.readyState === 1) {
    await mongoose.disconnect();
  }
};

main().catch(async (error) => {
  console.error(error);
  if (mongoose.connection.readyState === 1) await mongoose.disconnect();
  process.exit(1);
});
