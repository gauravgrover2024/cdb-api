import assert from "node:assert/strict";
import { compactAciClientResponse } from "../../services/aiAgent/aiAgent.clientResponse.js";

const vehicle = (make, model) => ({
  make,
  model,
  fullModel: `${make} ${model}`,
  displayName: `${make} ${model}`,
  imageUrl: `https://images.example/${model.toLowerCase()}.webp`,
});

const priceResponse = (make, model, price) => ({
  answer: `${make} ${model} price list`,
  intent: "vehicle_pricelist",
  canvasType: "pricelist_canvas",
  widget: {
    title: `${make} ${model} price list`,
    vehicle: vehicle(make, model),
    rows: [{
      variant: "Base",
      exShowroomPrice: price,
      onRoadPrice: price + 180000,
      raw: { shouldNotReachClient: "x".repeat(20000) },
    }],
  },
});

const raw = {
  answer: "Creta and Seltos comparison",
  intent: "vehicle_feature_comparison",
  canvasType: "feature_comparison_canvas",
  widget: {
    title: "Creta vs Seltos feature comparison",
    rows: [{
      feature: "Sunroof",
      models: [
        { ...vehicle("Hyundai", "Creta"), availableCount: 43, totalVariants: 50 },
        { ...vehicle("Kia", "Seltos"), availableCount: 32, totalVariants: 44 },
      ],
    }],
  },
  secondaryResponses: [
    priceResponse("Hyundai", "Creta", 1079000),
    priceResponse("Kia", "Seltos", 1099000),
  ],
  contextPatch: {
    selectedVehicle: vehicle("Kia", "Seltos"),
    anchorModel: "Seltos",
    compoundRequest: { version: "aci_compound_request_v2", modelCount: 2 },
    activeComparison: {
      vehicles: [vehicle("Hyundai", "Creta"), vehicle("Kia", "Seltos")],
      features: ["sunroof"],
    },
  },
};

const compact = compactAciClientResponse(raw);
const serialized = JSON.stringify(compact);

assert.equal(compact.answerBlocks.length, 3, "all answer blocks must survive");
assert.equal(compact.contextPatch.selectedVehicle, null, "comparison must not select the last car");
assert.equal(compact.contextPatch.anchorModel, "", "comparison must remain unanchored");
assert.equal(compact.contextPatch.activeComparison.vehicles.length, 2);
assert.equal(serialized.includes("shouldNotReachClient"), false, "raw database payload leaked");
assert.ok(serialized.length < 25000, `fixture response is too large: ${serialized.length}`);

console.log(JSON.stringify({
  status: "ok",
  bytes: serialized.length,
  answerBlocks: compact.answerBlocks.map((block) => block.intent),
  comparisonVehicles: compact.contextPatch.activeComparison.vehicles.map((item) => item.fullModel),
}, null, 2));
