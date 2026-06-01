import "dotenv/config";
import mongoose from "mongoose";
import { performance } from "node:perf_hooks";

import connectDB from "../../config/db.js";
import { prewarmAciCoreRuntime } from "../../services/aciCore/aciCore.prewarm.js";
import { chatWithAgent } from "../../services/aiAgent/aiAgent.service.js";

const rowsOf = (response = {}) =>
  response.rows ||
  response.data?.rows ||
  response.widget?.rows ||
  response.records ||
  response.variants ||
  [];

const bridgeOf = (response = {}) =>
  response.aciCoreBridge || response.meta?.aciCoreBridge || {};

const contextPatchOf = (response = {}) =>
  response.contextPatch ||
  response.data?.contextPatch ||
  response.widget?.contextPatch ||
  {};

const summarize = (response = {}) => {
  const rows = rowsOf(response);

  return {
    intent: response.intent,
    canvasType: response.canvasType,
    title: response.title,
    answer: response.answer,
    rowCount: rows.length,
    firstRows: rows.slice(0, 6).map((row) => ({
      make: row.make || row.brand || "",
      model: row.model || row.fullModel || row.displayName || "",
      variant: row.variant || row.variantName || "",
      fuel: row.fuel || row.fuelType || "",
      transmission: row.transmission || "",
      price:
        row.onRoadPriceLabel ||
        row.onRoadPriceWithoutOptionalLabel ||
        row.priceLabel ||
        row.exShowroomPriceLabel ||
        "",
      rawModel: row.model,
      rawVariant: row.variant,
    })),
    requested: response.requested || response.data?.requested || null,
    activeComparison:
      response.activeComparison ||
      response.contextPatch?.activeComparison ||
      response.data?.activeComparison ||
      response.data?.contextPatch?.activeComparison ||
      null,
    selectedVehicle:
      response.selectedVehicle ||
      response.contextPatch?.selectedVehicle ||
      response.data?.selectedVehicle ||
      response.data?.contextPatch?.selectedVehicle ||
      null,
    contextPatch: contextPatchOf(response),
    bridge: bridgeOf(response),
    modulesChecked: response.modulesChecked || response.data?.modulesChecked || [],
  };
};

async function runSingle(message, context = {}) {
  const start = performance.now();
  const response = await chatWithAgent({ message, context });
  const durationMs = Math.round(performance.now() - start);

  console.log("\n==============================");
  console.log(message);
  console.log(JSON.stringify(
    {
      durationMs,
      ...summarize(response),
    },
    null,
    2,
  ));

  return response;
}

async function runFollowup(setupMessage, followupMessage) {
  console.log("\n==============================");
  console.log(`FOLLOW-UP SETUP: ${setupMessage}`);
  const setup = await runSingle(setupMessage, {});
  const context = contextPatchOf(setup);

  console.log("\n==============================");
  console.log(`FOLLOW-UP ASK: ${followupMessage}`);
  await runSingle(followupMessage, context);
}

await connectDB();
await prewarmAciCoreRuntime({ force: true, mode: "light", background: false });

await runSingle("cheapest CNG cars in Delhi");
await runSingle("Creta S(O) IVT vs Seltos HTX IVT");
await runSingle("Nexon Fearless Plus S vs Brezza ZXI Plus AT");
await runSingle("Punch Adventure S CNG vs Nexon Smart Plus S CNG");
await runFollowup("Verna HX8 iVT vs City ZX CVT", "Which one gives more features?");
await runFollowup("Punch and Nexon CNG sunroof ABS ADAS", "Which one is better?");

await mongoose.disconnect();
