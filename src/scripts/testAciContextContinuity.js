import assert from "node:assert/strict";

import {
  buildAciTurnContext,
  buildAciContextPatch,
  mergeAciContextPatch,
  sanitizeAciContext,
} from "../services/aiAgent/aciAssistContextManager.js";

const logPass = (name) => console.log(`✅ ${name}`);

const expectContext = (name, actual, expected = {}) => {
  for (const [key, value] of Object.entries(expected)) {
    assert.equal(
      actual[key],
      value,
      `${name}: expected ${key}=${value}, got ${actual[key]}`
    );
  }
  logPass(name);
};

let sessionContext = sanitizeAciContext({});

// 1. Explicit model + variant should set both.
let turn = await buildAciTurnContext({
  message: "Show Creta SX(O) features",
  previousContext: sessionContext,
});

expectContext("explicit Creta SX(O) context", turn, {
  anchorModel: "Creta",
  anchorVariant: "SX(O)",
  anchorCity: "new-delhi",
});

sessionContext = mergeAciContextPatch({
  previousContext: sessionContext,
  patch: buildAciContextPatch({
    resolvedContext: turn,
    intent: "vehicle_model_features_explorer",
  }),
});

// 2. Follow-up should preserve selected vehicle.
turn = await buildAciTurnContext({
  message: "Does it have sunroof?",
  previousContext: sessionContext,
});

expectContext("follow-up preserves Creta SX(O)", turn, {
  anchorModel: "Creta",
  anchorVariant: "SX(O)",
});

// 3. Model-level query should clear stale variant.
turn = await buildAciTurnContext({
  message: "Show Verna features",
  previousContext: sessionContext,
});

expectContext("new model clears stale Creta variant", turn, {
  anchorModel: "Verna",
  anchorVariant: "",
});

sessionContext = mergeAciContextPatch({
  previousContext: sessionContext,
  patch: buildAciContextPatch({
    resolvedContext: turn,
    intent: "vehicle_model_features_explorer",
  }),
});

// 4. Feature discovery should keep model but clear variant.
turn = await buildAciTurnContext({
  message: "Which variants have ADAS?",
  previousContext: sessionContext,
});

expectContext("feature discovery uses Verna and no stale variant", turn, {
  anchorModel: "Verna",
  anchorVariant: "",
});

// 5. Explicit variant should set selected variant again.
turn = await buildAciTurnContext({
  message: "Does Verna SX IVT have ADAS?",
  previousContext: sessionContext,
});

expectContext("explicit Verna SX IVT variant", turn, {
  anchorModel: "Verna",
  anchorVariant: "SX IVT",
});

sessionContext = mergeAciContextPatch({
  previousContext: sessionContext,
  patch: buildAciContextPatch({
    resolvedContext: turn,
    intent: "vehicle_feature_answer",
  }),
});

// 6. EMI follow-up should preserve variant.
turn = await buildAciTurnContext({
  message: "Check EMI",
  previousContext: sessionContext,
});

expectContext("EMI follow-up preserves Verna SX IVT", turn, {
  anchorModel: "Verna",
  anchorVariant: "SX IVT",
});

// 7. Model switch should again clear stale variant.
turn = await buildAciTurnContext({
  message: "Show Creta price list",
  previousContext: sessionContext,
});

expectContext("Creta pricelist clears stale Verna variant", turn, {
  anchorModel: "Creta",
  anchorVariant: "",
});

// 8. Typo model should normalize.
turn = await buildAciTurnContext({
  message: "cretaa sunroof variants",
  previousContext: sessionContext,
});

expectContext("cretaa typo normalizes to Creta", turn, {
  anchorModel: "Creta",
  anchorVariant: "",
});

console.log("");
console.log("✅ All ACI context continuity tests passed.");
